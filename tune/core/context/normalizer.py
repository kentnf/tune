"""MetadataNormalizer — derives AnalysisSummary from relational model data."""
from __future__ import annotations

from tune.core.context.models import (
    AnalysisSummary,
    ExperimentPlannerInfo,
    FilePlannerInfo,
    SamplePlannerInfo,
)
from tune.core.models import KnownPath

# Canonical mapping from (normalised) library_strategy → suggested analysis type
_STRATEGY_TO_TYPE: dict[str, str] = {
    "rna-seq": "rna_seq",
    "rna_seq": "rna_seq",
    "chip-seq": "chip_seq",
    "chip_seq": "chip_seq",
    "atac-seq": "atac_seq",
    "atac_seq": "atac_seq",
    "gs": "wgs",
    "wgs": "wgs",
    "wes": "wes",
    "amplicon": "amplicon",
    "mirna-seq": "mirna_seq",
    "mirna_seq": "mirna_seq",
}

_TYPE_TO_FAMILY: dict[str, str] = {
    "rna_seq": "transcriptomics",
    "mirna_seq": "small_rna",
    "chip_seq": "epigenomics",
    "atac_seq": "epigenomics",
    "wgs": "genomics",
    "wes": "genomics",
    "amplicon": "targeted_genomics",
}

_TYPE_TO_RESOURCE_ROLES: dict[str, list[str]] = {
    "rna_seq": ["reference_fasta", "annotation_gtf", "spliced_aligner_index"],
    "mirna_seq": ["reference_fasta", "annotation_gtf_or_mirna_reference"],
    "chip_seq": ["reference_fasta", "aligner_index"],
    "atac_seq": ["reference_fasta", "aligner_index"],
    "wgs": ["reference_fasta", "aligner_index"],
    "wes": ["reference_fasta", "aligner_index", "target_regions_optional"],
    "amplicon": ["reference_fasta", "target_regions_or_amplicon_manifest"],
}

_TYPE_TO_PLANNING_HINTS: dict[str, list[str]] = {
    "rna_seq": [
        "Prefer splice-aware alignment or transcript quantification; avoid DNA-style aligners unless the user explicitly asks for them.",
        "Quantification and differential expression usually require an annotation GTF/GFF in addition to the reference FASTA.",
    ],
    "mirna_seq": [
        "Small-RNA analysis often needs adapter trimming and should avoid assuming standard mRNA fragment lengths.",
        "Do not default to whole-transcript differential expression tools unless the user explicitly describes mRNA-style RNA-seq.",
    ],
    "chip_seq": [
        "Peak-calling workflows usually require alignment, duplicate-aware filtering, and a peak caller such as MACS2.",
        "If the user mentions control/input samples, preserve that relationship in the abstract plan rather than flattening all samples equally.",
    ],
    "atac_seq": [
        "ATAC-seq plans should account for chromatin-accessibility QC and peak-centric outputs rather than RNA quantification steps.",
        "Do not add annotation-dependent counting by default unless the user explicitly asks for peak annotation or gene-level summaries.",
    ],
    "wgs": [
        "Whole-genome DNA analysis usually centers on alignment, post-alignment cleanup, variant calling, and variant filtering.",
        "Do not default to RNA-specific QC, splice-aware alignment, or annotation-dependent counting for WGS.",
    ],
    "wes": [
        "Whole-exome analysis usually centers on DNA alignment and variant calling; capture target metadata can be relevant but is not always mandatory.",
        "Avoid RNA-specific quantification or splice-aware alignment unless the user explicitly asks for transcriptomic analysis.",
    ],
    "amplicon": [
        "Amplicon workflows usually focus on targeted alignment/consensus/variant calling rather than genome-wide quantification.",
        "Preserve primer/panel-specific constraints when present instead of assuming generic WGS or RNA-seq defaults.",
    ],
}

# KnownPath keys that indicate a reference genome / index is registered
_REFERENCE_KEYS: frozenset[str] = frozenset(
    {
        "reference_fasta",
        "hisat2_index",
        "star_genome_dir",
        "bwa_index",
        "bowtie2_index",
        "bowtie_index",
    }
)


def build_summary(
    samples: list[SamplePlannerInfo],
    experiments: list[ExperimentPlannerInfo],
    files: list[FilePlannerInfo],
    known_paths: list[KnownPath],
) -> AnalysisSummary:
    """Derive AnalysisSummary from assembled relational data."""

    # ── Files by type ──────────────────────────────────────────────────
    files_by_type: dict[str, int] = {}
    for f in files:
        files_by_type[f.file_type] = files_by_type.get(f.file_type, 0) + 1

    # ── Organisms (deduplicated, sorted) ───────────────────────────────
    organisms = sorted({s.organism for s in samples if s.organism})

    # ── Library strategies (deduplicated, sorted) ──────────────────────
    strategies = sorted({e.library_strategy for e in experiments if e.library_strategy})

    # ── Library layout → paired-end flag ──────────────────────────────
    layouts = {e.library_layout for e in experiments if e.library_layout}
    if not layouts:
        is_paired_end: bool | None = None
    elif layouts == {"PAIRED"}:
        is_paired_end = True
    elif layouts == {"SINGLE"}:
        is_paired_end = False
    else:
        is_paired_end = None  # mixed or unknown

    # ── Reference genome availability ─────────────────────────────────
    kp_keys = {kp.key for kp in known_paths}
    has_reference_genome = bool(kp_keys & _REFERENCE_KEYS)

    # ── Files without sample linkage ───────────────────────────────────
    files_without_samples = sum(1 for f in files if f.linked_sample_id is None)

    # ── Metadata completeness ─────────────────────────────────────────
    if not samples:
        completeness: str = "missing"
    elif files_without_samples > 0:
        completeness = "partial"
    else:
        completeness = "complete"

    # ── Suggested analysis type ────────────────────────────────────────
    suggested: str | None = None
    if len(strategies) == 1:
        norm = strategies[0].lower().replace("-", "_")
        suggested = _STRATEGY_TO_TYPE.get(norm)
    analysis_family: str | None = None
    required_resource_roles: list[str] = []
    planning_hints: list[str] = []
    if suggested:
        analysis_family = _TYPE_TO_FAMILY.get(suggested)
        required_resource_roles = list(_TYPE_TO_RESOURCE_ROLES.get(suggested, []))
        planning_hints = list(_TYPE_TO_PLANNING_HINTS.get(suggested, []))
    elif len(strategies) > 1:
        analysis_family = "multiomics"
        planning_hints = [
            "Multiple library strategies were detected; keep the plan modular and avoid collapsing mixed omics into one flat RNA-seq-style pipeline.",
        ]

    # ── Potential issues ───────────────────────────────────────────────
    issues: list[str] = []

    if not samples:
        issues.append(
            "No samples registered — biological context is unavailable; "
            "consider completing metadata before analysis"
        )
    elif files_without_samples > 0:
        issues.append(
            f"{files_without_samples} file(s) not linked to any sample — "
            "R1/R2 assignment may be incomplete"
        )

    if not has_reference_genome:
        issues.append(
            "No reference genome registered "
            "(use 'add reference genome' to register one)"
        )

    if len(strategies) > 1:
        issues.append(
            f"Mixed library strategies detected: {', '.join(strategies)} — "
            "verify this is intentional"
        )

    if is_paired_end is None and experiments and "PAIRED" in layouts and "SINGLE" in layouts:
        issues.append("Mixed PAIRED and SINGLE library layouts detected")

    return AnalysisSummary(
        total_files=len(files),
        files_by_type=files_by_type,
        sample_count=len(samples),
        experiment_count=len(experiments),
        library_strategies=strategies,
        organisms=organisms,
        is_paired_end=is_paired_end,
        has_reference_genome=has_reference_genome,
        files_without_samples=files_without_samples,
        metadata_completeness=completeness,  # type: ignore[arg-type]
        suggested_analysis_type=suggested,
        analysis_family=analysis_family,
        required_resource_roles=required_resource_roles,
        planning_hints=planning_hints,
        potential_issues=issues,
    )
