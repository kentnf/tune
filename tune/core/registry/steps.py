"""Step type registry — defines all supported bioinformatics step types.

Each entry maps a dotted step_type string to a StepTypeDefinition describing
its input/output slots, parameter schema, repair policy, safety policy, Pixi
packages, and the renderer callable.  This is the single source of truth for:

- LLM plan generation (valid step_type values)
- Input binding slot UI / pre-flight
- Command renderer dispatch
- Repair engine retry limits and escalation rules
- Authorization / safety sandbox
- Environment pre-building
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional


# ---------------------------------------------------------------------------
# Supporting dataclasses (Phase 3)
# ---------------------------------------------------------------------------


@dataclass
class RepairPolicy:
    """Controls how the repair engine handles failures for this step type.

    Attributes:
        max_l1_retries: Maximum Level-1 (deterministic rule) repair attempts.
        max_l2_retries: Maximum Level-2 (constrained LLM) repair attempts.
        allow_l2_llm:   If False, skip Level-2 entirely and go straight to L3.
        l3_escalate:    If False, skip Level-3 escalation and mark step failed.
    """
    max_l1_retries: int = 2
    max_l2_retries: int = 1
    allow_l2_llm: bool = True
    l3_escalate: bool = True


class FanoutMode:
    """Fan-out execution mode constants for a step.

    NONE       — runs once for the whole job
    PER_SAMPLE — runs once per sample (typical for QC / alignment)
    PER_FILE   — runs once per individual input file
    """
    NONE = "none"
    PER_SAMPLE = "per_sample"
    PER_FILE = "per_file"


@dataclass
class SafetyPolicy:
    """Controls command authorization and safety flags for this step type.

    Attributes:
        require_authorization: If True, prompt the user to approve the
            command before execution (default True for all steps).
        command_type: Auth-cache key used by _authorized_types; defaults
            to the step's primary CLI tool name (e.g. ``"fastqc"``).
            Leave empty to fall back to get_command_type() heuristic.
        safety_flags: Informational tags forwarded to RenderedCommand,
            e.g. ``["write_to_disk"]``, ``["network_access"]``.
    """
    require_authorization: bool = True
    command_type: str = ""
    safety_flags: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Slot + Step type definitions
# ---------------------------------------------------------------------------


@dataclass
class SlotDefinition:
    name: str
    description: str
    file_types: list[str]  # e.g. ["fastq", "fastq.gz"]
    required: bool = True
    multiple: bool = False  # True if slot accepts multiple files
    from_upstream_dir: bool = False  # True if slot expects the upstream dep's output *directory* (e.g. MultiQC input_dir)
    artifact_role: str | None = None
    accepted_roles: list[str] = field(default_factory=list)
    artifact_scope: str = "job_global"


@dataclass
class StepTypeDefinition:
    """Full specification for a registered bioinformatics step type.

    Phase-3 structured fields replace the earlier flat booleans/ints:
        fanout_mode    → replaces supports_fan_out: bool
        repair_policy  → replaces max_level_1_retries / max_level_2_retries
        safety_policy  → new (command type hint + safety flags)
        renderer       → callable linked by renderer/__init__.py at import time

    Backwards-compatible properties provide the old names for any code that
    still uses them (repair/engine.py, plan_validator.py, etc.).
    """
    step_type: str          # dotted identifier, e.g. "qc.fastqc"
    display_name: str
    input_slots: list[SlotDefinition]
    output_slots: list[SlotDefinition]
    params_schema: dict     # JSON Schema for the params dict

    # Phase-3 structured fields
    fanout_mode: str = FanoutMode.NONE
    repair_policy: RepairPolicy = field(default_factory=RepairPolicy)
    safety_policy: SafetyPolicy = field(default_factory=SafetyPolicy)
    renderer: Optional[Callable] = field(default=None, repr=False)
    renderer_spec: dict[str, Any] = field(default_factory=dict, repr=False)

    pixi_packages: list[str] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Backwards-compatible read-only properties
    # ------------------------------------------------------------------

    @property
    def supports_fan_out(self) -> bool:
        """True if this step runs once per sample (per_sample or per_file)."""
        return self.fanout_mode != FanoutMode.NONE

    @property
    def max_level_1_retries(self) -> int:
        return self.repair_policy.max_l1_retries

    @property
    def max_level_2_retries(self) -> int:
        return self.repair_policy.max_l2_retries

    @property
    def requires_reference(self) -> bool:
        """True if any required input slot needs a reference/index file."""
        ref_names = {"index_prefix", "genome_dir", "reference_fasta"}
        return any(s.name in ref_names and s.required for s in self.input_slots)

    @property
    def requires_annotation(self) -> bool:
        """True if any required input slot needs an annotation file."""
        return any(s.name == "annotation_gtf" and s.required
                   for s in self.input_slots)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_REGISTRY: dict[str, StepTypeDefinition] = {}
_CUSTOM_STEP_TYPES: set[str] = set()


def register(defn: StepTypeDefinition) -> StepTypeDefinition:
    _REGISTRY[defn.step_type] = defn
    return defn


def register_custom(defn: StepTypeDefinition) -> StepTypeDefinition:
    if defn.step_type in _REGISTRY and defn.step_type not in _CUSTOM_STEP_TYPES:
        raise ValueError(f"Custom step_type '{defn.step_type}' conflicts with a built-in step")
    _CUSTOM_STEP_TYPES.add(defn.step_type)
    _REGISTRY[defn.step_type] = defn
    return defn


def reset_custom() -> None:
    for step_type in list(_CUSTOM_STEP_TYPES):
        _REGISTRY.pop(step_type, None)
    _CUSTOM_STEP_TYPES.clear()


def get_step_type(step_type: str) -> StepTypeDefinition | None:
    return _REGISTRY.get(step_type)


def all_step_types() -> list[str]:
    return list(_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Step type definitions
# ---------------------------------------------------------------------------

register(StepTypeDefinition(
    step_type="qc.fastqc",
    display_name="FastQC Quality Control",
    input_slots=[
        SlotDefinition("reads", "FASTQ files to QC", ["fastq", "fastq.gz", "fq", "fq.gz"],
                       required=True, multiple=True,
                       accepted_roles=[
                           "trimmed_reads",
                           "trimmed_reads_read1",
                           "trimmed_reads_read2",
                           "raw_reads",
                           "raw_reads_read1",
                           "raw_reads_read2",
                       ],
                       artifact_scope="per_sample"),
    ],
    output_slots=[
        SlotDefinition("qc_html", "HTML report(s)", ["html"], required=False, multiple=True,
                       artifact_role="qc_fastqc_html", artifact_scope="per_sample"),
        SlotDefinition("qc_zip", "ZIP data(s)", ["zip"], required=False, multiple=True,
                       artifact_role="qc_fastqc_zip", artifact_scope="per_sample"),
    ],
    params_schema={
        "type": "object",
        "properties": {
            "threads": {"type": "integer", "default": 4, "minimum": 1},
        },
    },
    fanout_mode=FanoutMode.PER_SAMPLE,
    repair_policy=RepairPolicy(max_l1_retries=3, max_l2_retries=1),
    safety_policy=SafetyPolicy(command_type="fastqc", safety_flags=["write_to_disk"]),
    pixi_packages=["fastqc"],
))

register(StepTypeDefinition(
    step_type="qc.multiqc",
    display_name="MultiQC Summary Report",
    input_slots=[
        SlotDefinition("input_dir", "Directory containing QC reports", ["*"],
                       required=True, from_upstream_dir=True,
                       accepted_roles=["qc_fastqc_html", "qc_fastqc_zip", "qc_multiqc_input_dir"]),
    ],
    output_slots=[
        SlotDefinition("report_html", "MultiQC HTML report", ["html"], required=False,
                       artifact_role="qc_multiqc_html"),
        SlotDefinition("report_data", "MultiQC data directory", ["*"], required=False,
                       artifact_role="qc_multiqc_data_dir"),
    ],
    params_schema={
        "type": "object",
        "properties": {
            "report_name": {"type": "string", "default": "multiqc_report"},
        },
    },
    fanout_mode=FanoutMode.NONE,
    repair_policy=RepairPolicy(max_l1_retries=2, max_l2_retries=1),
    safety_policy=SafetyPolicy(command_type="multiqc", safety_flags=["write_to_disk"]),
    pixi_packages=["multiqc"],
))

register(StepTypeDefinition(
    step_type="trim.fastp",
    display_name="Adapter Trimming (fastp)",
    input_slots=[
        SlotDefinition("read1", "R1 FASTQ file", ["fastq", "fastq.gz", "fq", "fq.gz"],
                       accepted_roles=["raw_reads_read1", "trimmed_reads_read1"],
                       artifact_scope="per_sample"),
        SlotDefinition("read2", "R2 FASTQ file (paired-end only)",
                       ["fastq", "fastq.gz", "fq", "fq.gz"], required=False,
                       accepted_roles=["raw_reads_read2", "trimmed_reads_read2"],
                       artifact_scope="per_sample"),
    ],
    output_slots=[
        SlotDefinition("trimmed_read1", "Trimmed R1", ["fastq.gz"],
                       artifact_role="trimmed_reads_read1", artifact_scope="per_sample"),
        SlotDefinition("trimmed_read2", "Trimmed R2 (PE only)", ["fastq.gz"], required=False,
                       artifact_role="trimmed_reads_read2", artifact_scope="per_sample"),
        SlotDefinition("json_report", "fastp JSON report", ["json"],
                       artifact_role="qc_fastp_json", artifact_scope="per_sample"),
        SlotDefinition("html_report", "fastp HTML report", ["html"],
                       artifact_role="qc_fastp_html", artifact_scope="per_sample"),
    ],
    params_schema={
        "type": "object",
        "properties": {
            "threads": {"type": "integer", "default": 4, "minimum": 1},
            "paired_end": {"type": "boolean", "default": False},
            "quality": {"type": "integer", "default": 20, "minimum": 1},
            "length_required": {"type": "integer", "default": 36},
        },
    },
    fanout_mode=FanoutMode.PER_SAMPLE,
    repair_policy=RepairPolicy(max_l1_retries=2, max_l2_retries=1),
    safety_policy=SafetyPolicy(command_type="fastp", safety_flags=["write_to_disk"]),
    pixi_packages=["fastp"],
))

register(StepTypeDefinition(
    step_type="align.bwa",
    display_name="DNA Alignment (BWA-MEM)",
    input_slots=[
        SlotDefinition("read1", "R1 FASTQ", ["fastq", "fastq.gz", "fq", "fq.gz"],
                       accepted_roles=["trimmed_reads_read1", "raw_reads_read1"],
                       artifact_scope="per_sample"),
        SlotDefinition("read2", "R2 FASTQ (PE only)",
                       ["fastq", "fastq.gz", "fq", "fq.gz"], required=False,
                       accepted_roles=["trimmed_reads_read2", "raw_reads_read2"],
                       artifact_scope="per_sample"),
        SlotDefinition("index_prefix", "BWA index prefix",
                       ["*"], accepted_roles=["bwa_index"]),
    ],
    output_slots=[
        SlotDefinition("sam", "Output SAM file", ["sam"],
                       artifact_role="alignment_sam", artifact_scope="per_sample"),
    ],
    params_schema={
        "type": "object",
        "properties": {
            "threads": {"type": "integer", "default": 8, "minimum": 1},
            "paired_end": {"type": "boolean", "default": False},
            "mark_shorter_split_hits_as_secondary": {"type": "boolean", "default": True},
        },
    },
    fanout_mode=FanoutMode.PER_SAMPLE,
    repair_policy=RepairPolicy(max_l1_retries=3, max_l2_retries=1),
    safety_policy=SafetyPolicy(command_type="bwa", safety_flags=["write_to_disk"]),
    pixi_packages=["bwa", "samtools"],
))

register(StepTypeDefinition(
    step_type="align.hisat2",
    display_name="RNA-seq Alignment (HISAT2)",
    input_slots=[
        SlotDefinition("read1", "R1 FASTQ", ["fastq", "fastq.gz", "fq", "fq.gz"],
                       accepted_roles=["trimmed_reads_read1", "raw_reads_read1"],
                       artifact_scope="per_sample"),
        SlotDefinition("read2", "R2 FASTQ (PE only)",
                       ["fastq", "fastq.gz", "fq", "fq.gz"], required=False,
                       accepted_roles=["trimmed_reads_read2", "raw_reads_read2"],
                       artifact_scope="per_sample"),
        SlotDefinition("index_prefix", "HISAT2 index prefix (or genome FASTA to build from)",
                       ["*"], accepted_roles=["hisat2_index"]),
    ],
    output_slots=[
        SlotDefinition("sam", "Output SAM file", ["sam"],
                       artifact_role="alignment_sam", artifact_scope="per_sample"),
        SlotDefinition("alignment_summary", "Alignment summary text", ["txt"], required=False,
                       artifact_role="alignment_summary", artifact_scope="per_sample"),
    ],
    params_schema={
        "type": "object",
        "properties": {
            "threads": {"type": "integer", "default": 4, "minimum": 1},
            "paired_end": {"type": "boolean", "default": False},
            "strandness": {"type": "string", "enum": ["unstranded", "FR", "RF"],
                           "default": "unstranded"},
        },
    },
    fanout_mode=FanoutMode.PER_SAMPLE,
    repair_policy=RepairPolicy(max_l1_retries=3, max_l2_retries=1),
    safety_policy=SafetyPolicy(command_type="hisat2", safety_flags=["write_to_disk"]),
    pixi_packages=["hisat2", "samtools"],
))

register(StepTypeDefinition(
    step_type="align.star",
    display_name="RNA-seq Alignment (STAR)",
    input_slots=[
        SlotDefinition("read1", "R1 FASTQ", ["fastq", "fastq.gz", "fq", "fq.gz"],
                       accepted_roles=["trimmed_reads_read1", "raw_reads_read1"],
                       artifact_scope="per_sample"),
        SlotDefinition("read2", "R2 FASTQ (PE only)",
                       ["fastq", "fastq.gz", "fq", "fq.gz"], required=False,
                       accepted_roles=["trimmed_reads_read2", "raw_reads_read2"],
                       artifact_scope="per_sample"),
        SlotDefinition("genome_dir", "STAR genome directory", ["*"],
                       accepted_roles=["star_genome_dir"]),
        SlotDefinition("annotation_gtf", "Gene annotation GTF", ["gtf"], required=False),
    ],
    output_slots=[
        SlotDefinition("bam", "Aligned BAM", ["bam"],
                       artifact_role="alignment_bam", artifact_scope="per_sample"),
        SlotDefinition("log_final", "STAR final log", ["out"], required=False,
                       artifact_role="alignment_log", artifact_scope="per_sample"),
    ],
    params_schema={
        "type": "object",
        "properties": {
            "threads": {"type": "integer", "default": 8, "minimum": 1},
            "paired_end": {"type": "boolean", "default": False},
            "two_pass_mode": {"type": "boolean", "default": False},
        },
    },
    fanout_mode=FanoutMode.PER_SAMPLE,
    repair_policy=RepairPolicy(max_l1_retries=3, max_l2_retries=1),
    safety_policy=SafetyPolicy(command_type="star", safety_flags=["write_to_disk"]),
    pixi_packages=["star", "samtools"],
))

register(StepTypeDefinition(
    step_type="quant.featurecounts",
    display_name="Gene Quantification (featureCounts)",
    input_slots=[
        SlotDefinition("aligned_bam", "Aligned BAM file(s)", ["bam"], multiple=True,
                       accepted_roles=["sorted_bam", "alignment_bam"],
                       artifact_scope="per_sample"),
        SlotDefinition("annotation_gtf", "Gene annotation GTF", ["gtf"]),
    ],
    output_slots=[
        SlotDefinition("counts_matrix", "Gene count matrix", ["txt"],
                       artifact_role="counts_matrix"),
        SlotDefinition("summary", "featureCounts summary", ["summary"], required=False,
                       artifact_role="counts_summary"),
    ],
    params_schema={
        "type": "object",
        "properties": {
            "threads": {"type": "integer", "default": 4, "minimum": 1},
            "strandness": {"type": "integer", "enum": [0, 1, 2], "default": 0},
            "paired_end": {"type": "boolean", "default": False},
            "feature_type": {"type": "string", "default": "exon"},
            "attribute_type": {"type": "string", "default": "gene_id"},
        },
    },
    fanout_mode=FanoutMode.NONE,
    repair_policy=RepairPolicy(max_l1_retries=3, max_l2_retries=1),
    safety_policy=SafetyPolicy(command_type="featurecounts", safety_flags=["write_to_disk"]),
    pixi_packages=["subread"],
))

register(StepTypeDefinition(
    step_type="stats.deseq2",
    display_name="Differential Expression (DESeq2)",
    input_slots=[
        SlotDefinition("counts_matrix", "featureCounts output matrix", ["txt", "csv", "tsv"],
                       accepted_roles=["counts_matrix"]),
    ],
    output_slots=[
        SlotDefinition("differential_expression_results", "Differential expression results", ["csv"],
                       artifact_role="differential_expression_results"),
    ],
    params_schema={
        "type": "object",
        "properties": {
            "alpha": {"type": "number", "default": 0.05, "minimum": 0.0, "maximum": 1.0},
            "min_count": {"type": "integer", "default": 10, "minimum": 0},
            "design_factors": {
                "type": "array",
                "items": {"type": "string"},
                "default": [],
            },
            "contrast_factor": {"type": "string", "default": "condition"},
            "contrast_pairs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "numerator": {"type": "string"},
                        "denominator": {"type": "string"},
                    },
                    "required": ["numerator", "denominator"],
                },
                "default": [],
            },
        },
    },
    fanout_mode=FanoutMode.NONE,
    repair_policy=RepairPolicy(max_l1_retries=2, max_l2_retries=1),
    safety_policy=SafetyPolicy(command_type="rscript", safety_flags=["write_to_disk"]),
    pixi_packages=["r-base", "bioconductor-deseq2", "bioconductor-genomeinfodbdata"],
))

register(StepTypeDefinition(
    step_type="util.bwa_index",
    display_name="Build BWA Index",
    input_slots=[
        SlotDefinition("reference_fasta", "Genome FASTA", ["fa", "fasta", "fna"],
                       accepted_roles=["reference_fasta"]),
    ],
    output_slots=[
        SlotDefinition("index_prefix", "BWA index prefix", ["*"],
                       artifact_role="bwa_index"),
    ],
    params_schema={
        "type": "object",
        "properties": {
            "prefix_name": {"type": "string", "default": "genome"},
        },
    },
    fanout_mode=FanoutMode.NONE,
    repair_policy=RepairPolicy(max_l1_retries=2, max_l2_retries=0, allow_l2_llm=False),
    safety_policy=SafetyPolicy(command_type="bwa", safety_flags=["write_to_disk"]),
    pixi_packages=["bwa"],
))

register(StepTypeDefinition(
    step_type="util.hisat2_build",
    display_name="Build HISAT2 Index",
    input_slots=[
        SlotDefinition("reference_fasta", "Genome FASTA", ["fa", "fasta", "fna"],
                       accepted_roles=["reference_fasta"]),
    ],
    output_slots=[
        SlotDefinition("index_prefix", "HISAT2 index prefix", ["*"],
                       artifact_role="hisat2_index"),
    ],
    params_schema={
        "type": "object",
        "properties": {
            "threads": {"type": "integer", "default": 4, "minimum": 1},
        },
    },
    fanout_mode=FanoutMode.NONE,
    repair_policy=RepairPolicy(max_l1_retries=2, max_l2_retries=0, allow_l2_llm=False),
    safety_policy=SafetyPolicy(command_type="hisat2-build", safety_flags=["write_to_disk"]),
    pixi_packages=["hisat2"],
))

register(StepTypeDefinition(
    step_type="util.star_genome_generate",
    display_name="Build STAR Genome Index",
    input_slots=[
        SlotDefinition("reference_fasta", "Genome FASTA", ["fa", "fasta", "fna"],
                       accepted_roles=["reference_fasta"]),
        SlotDefinition("annotation_gtf", "Gene annotation GTF/GFF", ["gtf", "gff", "gff3", "gtf.gz", "gff.gz"], required=False),
    ],
    output_slots=[
        SlotDefinition("genome_dir", "STAR genome directory", ["*"],
                       artifact_role="star_genome_dir"),
    ],
    params_schema={
        "type": "object",
        "properties": {
            "threads":              {"type": "integer", "default": 4,  "minimum": 1},
            "genome_sa_index_nbases": {"type": "integer", "default": 14, "minimum": 1},
        },
    },
    fanout_mode=FanoutMode.NONE,
    repair_policy=RepairPolicy(max_l1_retries=2, max_l2_retries=0, allow_l2_llm=False),
    safety_policy=SafetyPolicy(command_type="star-genome", safety_flags=["write_to_disk"]),
    pixi_packages=["star"],
))

register(StepTypeDefinition(
    step_type="util.samtools_sort",
    display_name="Sort BAM (samtools)",
    input_slots=[
        SlotDefinition("input_sam_or_bam", "Input SAM or BAM", ["sam", "bam"],
                       accepted_roles=["alignment_sam", "alignment_bam", "sorted_bam"],
                       artifact_scope="per_sample"),
    ],
    output_slots=[
        SlotDefinition("sorted_bam", "Sorted BAM", ["bam"],
                       artifact_role="sorted_bam", artifact_scope="per_sample"),
    ],
    params_schema={
        "type": "object",
        "properties": {
            "threads": {"type": "integer", "default": 4, "minimum": 1},
            "memory_per_thread": {"type": "string", "default": "768M"},
        },
    },
    fanout_mode=FanoutMode.PER_SAMPLE,
    repair_policy=RepairPolicy(max_l1_retries=3, max_l2_retries=1),
    safety_policy=SafetyPolicy(command_type="samtools", safety_flags=["write_to_disk"]),
    pixi_packages=["samtools"],
))

register(StepTypeDefinition(
    step_type="util.samtools_index",
    display_name="Index BAM (samtools)",
    input_slots=[
        SlotDefinition("sorted_bam", "Sorted BAM to index", ["bam"],
                       accepted_roles=["sorted_bam"], artifact_scope="per_sample"),
    ],
    output_slots=[
        SlotDefinition("bai", "BAM index", ["bai"],
                       artifact_role="bam_index", artifact_scope="per_sample"),
    ],
    params_schema={
        "type": "object",
        "properties": {
            "threads": {"type": "integer", "default": 4, "minimum": 1},
        },
    },
    fanout_mode=FanoutMode.PER_SAMPLE,
    repair_policy=RepairPolicy(max_l1_retries=2, max_l2_retries=1),
    safety_policy=SafetyPolicy(command_type="samtools", safety_flags=["write_to_disk"]),
    pixi_packages=["samtools"],
))
