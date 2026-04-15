"""Stateless metadata task functions — produce proposal payloads without side effects.

Each function:
- Takes project_id + AsyncSession + LLM gateway + optional instruction text
- Calls the LLM to extract metadata
- Returns a proposal payload dict (no DB writes)
- Moves custom field suggestions to custom_fields_to_register instead of auto-registering
"""
from __future__ import annotations

import difflib
import re
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tune.core.llm.gateway import LLMMessage, get_gateway
from tune.core.metadata.manager import score_project_metadata_health
from tune.core.metadata.sample_inference import infer_samples_from_filenames
from tune.core.models import Experiment, File, FileRun, Sample


# ---------------------------------------------------------------------------
# Normalisation helpers (moved from engine.py)
# ---------------------------------------------------------------------------

_STRATEGY_MAP = {
    "rna-seq": "RNA-Seq", "rnaseq": "RNA-Seq", "mrna": "RNA-Seq",
    "gs": "WGS", "wgs": "WGS", "whole genome": "WGS",
    "wes": "WXS", "whole exome": "WXS", "wxs": "WXS",
    "chip-seq": "ChIP-Seq", "chipseq": "ChIP-Seq",
    "atac-seq": "ATAC-seq", "atacseq": "ATAC-seq",
    "bisulfite": "Bisulfite-Seq",
    "amplicon": "AMPLICON",
    "16s": "AMPLICON",
    "metagenomics": "WGS",
}

_SOURCE_MAP = {
    "RNA-Seq": "TRANSCRIPTOMIC",
    "WGS": "GENOMIC",
    "WXS": "GENOMIC",
    "ChIP-Seq": "GENOMIC",
    "ATAC-seq": "GENOMIC",
    "Bisulfite-Seq": "GENOMIC",
    "AMPLICON": "GENOMIC",
}

_SELECTION_MAP = {
    "RNA-Seq": "cDNA",
    "WGS": "RANDOM",
    "WXS": "Hybrid Selection",
    "ChIP-Seq": "ChIP",
    "ATAC-seq": "RANDOM",
    "Bisulfite-Seq": "RANDOM",
    "AMPLICON": "PCR",
}

_PLATFORM_MAP = {
    "ILLUMINA": "ILLUMINA", "illumina": "ILLUMINA",
    "PACBIO": "PACBIO_SMRT", "pacbio": "PACBIO_SMRT",
    "OXFORD NANOPORE": "OXFORD_NANOPORE", "nanopore": "OXFORD_NANOPORE", "ont": "OXFORD_NANOPORE",
    "ION TORRENT": "ION_TORRENT", "ion torrent": "ION_TORRENT",
    "BGISEQ": "BGISEQ", "bgi": "BGISEQ",
}

_INSTRUMENT_MAP = {
    "hiseq x ten": "HiSeq X Ten", "hiseq x 10": "HiSeq X Ten",
    "hiseq x five": "HiSeq X Five",
    "hiseq 6000": "HiSeq 6000", "hiseq6000": "HiSeq 6000",
    "hiseq 4000": "HiSeq 4000", "hiseq4000": "HiSeq 4000",
    "hiseq 3000": "HiSeq 3000", "hiseq 2500": "HiSeq 2500", "hiseq 2000": "HiSeq 2000",
    "novaseq 6000": "NovaSeq 6000", "novaseq6000": "NovaSeq 6000",
    "novaseq x plus": "NovaSeq X Plus", "novaseq x": "NovaSeq X",
    "nextseq 2000": "NextSeq 2000", "nextseq2000": "NextSeq 2000",
    "nextseq 500": "NextSeq 500", "nextseq500": "NextSeq 500",
    "nextseq 550": "NextSeq 550", "nextseq550": "NextSeq 550",
    "miseq": "MiSeq",
}


def normalise_library_strategy(text: str) -> tuple[str | None, str | None, str | None]:
    """Return (library_strategy, library_source, library_selection) from free text."""
    lower = text.lower()
    for key, val in _STRATEGY_MAP.items():
        if key in lower:
            return val, _SOURCE_MAP.get(val), _SELECTION_MAP.get(val)
    return None, None, None


def normalise_platform(text: str) -> str | None:
    lower = text.lower().replace("-", "").replace(" ", "")
    for key, val in _PLATFORM_MAP.items():
        if key.replace(" ", "") in lower:
            return val
    return None


def normalise_instrument(text: str) -> str | None:
    lower = text.lower().replace("-", "").replace(" ", "")
    for key, val in _INSTRUMENT_MAP.items():
        if key.replace(" ", "") in lower:
            return val
    return None


# ---------------------------------------------------------------------------
# Standard fields that don't need schema_extensions registration
# ---------------------------------------------------------------------------

_STANDARD_SAMPLE_KEYS = {
    "sample_name", "organism", "tissue", "treatment", "replicate",
    "sex", "age", "developmental_stage", "genotype", "cell_type",
    "package", "collection_date", "geo_loc_name",
}

# Maps common LLM-generated key variants → canonical standard key
_SAMPLE_KEY_ALIASES: dict[str, str] = {
    "tissue_type": "tissue",
    "tissue_source": "tissue",
    "experimental_treatment": "treatment",
    "condition": "treatment",
    "biological_replicate": "replicate",
    "bio_rep": "replicate",
    "rep": "replicate",
    "dev_stage": "developmental_stage",
    "development_stage": "developmental_stage",
    "gender": "sex",
    "cell_line": "cell_type",
}


def _norm_sample_key(key: str) -> str:
    """Normalise a field key to its canonical standard form.

    Steps (in order):
    1. Strip whitespace, lowercase, spaces/hyphens → underscores
    2. Exact alias lookup  (e.g. tissue_type → tissue)
    3. Already a known standard key → return as-is
    4. De-pluralise: strip trailing 's' or 'es', then retry steps 2-3
    5. Fuzzy match against all standard keys (cutoff 0.82)
    6. Fall back to the cleaned key unchanged
    """
    k = key.strip().lower().replace(" ", "_").replace("-", "_")

    # 1. Exact alias
    if k in _SAMPLE_KEY_ALIASES:
        return _SAMPLE_KEY_ALIASES[k]

    # 2. Already standard
    _all_standard = _STANDARD_SAMPLE_KEYS | _STANDARD_EXPERIMENT_KEYS
    if k in _all_standard:
        return k

    # 3. De-pluralise (try stripping 'es' then 's'; guard against very short stems)
    for suffix in ("es", "s"):
        if k.endswith(suffix) and len(k) > len(suffix) + 2:
            stem = k[: -len(suffix)]
            if stem in _SAMPLE_KEY_ALIASES:
                return _SAMPLE_KEY_ALIASES[stem]
            if stem in _all_standard:
                return stem

    # 4. Fuzzy match — only against the known standard target set
    targets = sorted(_all_standard)   # sorted for stable results
    matches = difflib.get_close_matches(k, targets, n=1, cutoff=0.82)
    if matches:
        return matches[0]

    return k

_STANDARD_EXPERIMENT_KEYS = {
    "library_strategy", "library_source", "library_selection",
    "library_layout", "platform", "instrument_model",
    "read_length", "library_prep_kit", "design_description",
    "library_name",
}

_MAX_ITEMS_PER_LIST = 500


# ---------------------------------------------------------------------------
# Task 1: Infer samples from FASTQ filenames
# ---------------------------------------------------------------------------


async def infer_samples_task(
    project_id: str,
    session: AsyncSession,
    instruction: str | None = None,
) -> dict[str, Any]:
    """Return proposal payload with samples_to_create inferred from FASTQ filenames."""
    files = (
        await session.execute(
            select(File).where(
                File.project_id == project_id,
                File.file_type.in_(["fastq", "fq"]),
            )
        )
    ).scalars().all()

    if not files:
        return {"error": "No FASTQ files found in this project.", "samples_to_create": []}

    # Get existing sample names so we don't duplicate
    existing_names = set(
        (
            await session.execute(
                select(Sample.sample_name).where(Sample.project_id == project_id)
            )
        ).scalars().all()
    )

    result = await infer_samples_from_filenames(list(files))

    samples_to_create = []
    file_links_preview = []
    for c in result.candidates:
        if c.sample_name in existing_names:
            continue
        if len(samples_to_create) >= _MAX_ITEMS_PER_LIST:
            break
        samples_to_create.append({
            "key": f"sample:new:{c.sample_name}",
            "sample_name": c.sample_name,
            "organism": None,
            "attrs": {},
            "is_paired": c.is_paired,
            "file_ids": c.file_ids,
            "filenames": c.filenames,
            "read_numbers": c.read_numbers,
        })
        for fid, fname, rn in zip(c.file_ids, c.filenames, c.read_numbers):
            file_links_preview.append({
                "key": f"link:{fid}:{c.sample_name}",
                "filename": fname,
                "file_id": fid,
                "inferred_sample_name": c.sample_name,
                "read_number": rn,
                "confidence": 0.9,
            })

    return {
        "samples_to_create": samples_to_create,
        "file_links_preview": file_links_preview,
        "library_layout": result.library_layout,
        "grouping_hint": result.grouping_hint,
        "truncated": len(result.candidates) > len(samples_to_create),
    }


# ---------------------------------------------------------------------------
# Task 2: Fill sample attributes
# ---------------------------------------------------------------------------


async def fill_samples_task(
    project_id: str,
    session: AsyncSession,
    instruction: str | None = None,
) -> dict[str, Any]:
    """Return proposal payload with samples_to_update and custom_fields_to_register."""
    samples = (
        await session.execute(
            select(Sample).where(Sample.project_id == project_id)
        )
    ).scalars().all()

    if not samples:
        return {"error": "No samples found. Infer or create samples first.", "samples_to_update": []}

    gw = get_gateway()

    sample_summaries = [
        {
            "id": s.id,
            "sample_name": s.sample_name,
            "organism": s.organism,
            "attrs": s.attrs or {},
        }
        for s in samples
    ]

    extraction = await gw.structured_output(
        messages=[LLMMessage("user", instruction or "Fill in missing sample attributes for all samples.")],
        schema={
            "type": "object",
            "properties": {
                "shared_attrs": {
                    "type": "object",
                    "description": "Attributes that apply to ALL samples (e.g. organism, tissue)",
                },
                "per_sample_attrs": {
                    "type": "array",
                    "description": "Per-sample overrides or additions",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sample_name_prefix": {"type": "string"},
                            "attrs": {"type": "object"},
                        },
                    },
                },
                "custom_fields": {
                    "type": "array",
                    "description": "Non-standard field names that may be project-specific",
                    "items": {
                        "type": "object",
                        "properties": {
                            "field_name": {"type": "string"},
                            "object_type": {"type": "string", "enum": ["Sample", "Experiment"]},
                            "inferred_type": {"type": "string"},
                            "example_value": {"type": "string"},
                        },
                    },
                },
            },
        },
        system=(
            "You are a bioinformatics metadata assistant. "
            f"Here are the current samples (with existing attrs): {sample_summaries[:50]}. "
            "The user wants to fill in MISSING metadata only — do NOT re-suggest fields that already have a value. "
            "IMPORTANT: Use ONLY these exact key names for standard fields — do not invent variations:\n"
            "  organism, tissue, treatment, replicate, sex, age, developmental_stage, genotype, cell_type, "
            "  collection_date, geo_loc_name\n"
            "Return shared_attrs (applies to all samples), per_sample_attrs (keyed by sample_name prefix for per-sample values), "
            "and custom_fields ONLY for truly project-specific fields that are NOT in the list above. "
            "Never put standard fields into custom_fields."
        ),
    )

    shared_attrs: dict = extraction.get("shared_attrs") or {}
    per_sample: list = extraction.get("per_sample_attrs") or []
    custom_fields_raw: list = extraction.get("custom_fields") or []

    samples_to_update = []
    for s in samples[:_MAX_ITEMS_PER_LIST]:
        current_attrs = dict(s.attrs or {})
        # Build a normalized key set for "already has value" checks
        # e.g. existing key "Tissue" or "tissue_type" both normalise to "tissue"
        normalized_existing: dict[str, str] = {
            _norm_sample_key(k): str(v)
            for k, v in current_attrs.items()
            if v not in (None, "", [])
        }
        changes: dict = {}

        def _should_add(raw_key: str, value: object) -> tuple[bool, str]:
            """Return (should_add, canonical_key). Skip if normalized key already has a value."""
            canon = _norm_sample_key(raw_key)
            if canon == "organism":
                return (not s.organism), "organism"
            if canon in normalized_existing:
                return False, canon
            return True, canon

        for k, v in shared_attrs.items():
            ok, canon = _should_add(k, v)
            if ok and v not in (None, ""):
                changes[canon] = v

        for entry in per_sample:
            prefix = entry.get("sample_name_prefix", "")
            if s.sample_name.startswith(prefix) or s.sample_name == prefix:
                for k, v in (entry.get("attrs") or {}).items():
                    ok, canon = _should_add(k, v)
                    if ok and v not in (None, ""):
                        changes[canon] = v

        if changes:
            samples_to_update.append({
                "key": f"sample:{s.id}",
                "sample_id": s.id,
                "sample_name": s.sample_name,
                "changes": changes,
            })

    # Custom fields: only suggest truly non-standard ones
    # Normalize the field name first — if it resolves to a standard key, skip it
    custom_fields_to_register = []
    for cf in custom_fields_raw:
        fn = cf.get("field_name", "")
        if not fn:
            continue
        canonical = _norm_sample_key(fn)
        if canonical in _STANDARD_SAMPLE_KEYS or canonical in _STANDARD_EXPERIMENT_KEYS:
            continue  # LLM put a standard field into custom_fields — ignore
        custom_fields_to_register.append({
            "key": f"custom:{cf.get('object_type', 'Sample').lower()}:{canonical}",
            "field_name": canonical,
            "object_type": cf.get("object_type", "Sample"),
            "inferred_type": cf.get("inferred_type", "string"),
            "example_value": cf.get("example_value", ""),
        })

    return {
        "samples_to_update": samples_to_update,
        "custom_fields_to_register": custom_fields_to_register,
        "truncated": len(samples) > _MAX_ITEMS_PER_LIST,
    }


# ---------------------------------------------------------------------------
# Task 3: Fill experiment attributes
# ---------------------------------------------------------------------------


async def fill_experiments_task(
    project_id: str,
    session: AsyncSession,
    instruction: str | None = None,
) -> dict[str, Any]:
    """Return proposal payload with experiments_to_update."""
    experiments = (
        await session.execute(
            select(Experiment).where(Experiment.project_id == project_id)
        )
    ).scalars().all()

    if not experiments:
        return {"error": "No experiments found. Create experiments first.", "experiments_to_update": []}

    # Fetch sample names so the LLM can generate per-experiment values like library_name
    sample_ids = list({e.sample_id for e in experiments})
    samples = (
        await session.execute(select(Sample).where(Sample.id.in_(sample_ids)))
    ).scalars().all()
    sample_id_to_name = {s.id: s.sample_name for s in samples}

    gw = get_gateway()

    exp_summaries = [
        {
            "id": e.id,
            "sample_name": sample_id_to_name.get(e.sample_id, "unknown"),
            "library_strategy": e.library_strategy,
            "library_source": e.library_source,
            "library_selection": e.library_selection,
            "library_layout": e.library_layout,
            "platform": e.platform,
            "instrument_model": e.instrument_model,
            "attrs": e.attrs or {},
        }
        for e in experiments[:50]
    ]

    extraction = await gw.structured_output(
        messages=[LLMMessage("user", instruction or "Fill in missing experiment metadata.")],
        schema={
            "type": "object",
            "properties": {
                "platform_text": {"type": "string"},
                "instrument_text": {"type": "string"},
                "strategy_text": {"type": "string"},
                "layout": {"type": "string"},
                "read_length": {"type": "integer"},
                "library_prep_kit": {"type": "string"},
                "design_description": {"type": "string"},
                "per_experiment": {
                    "type": "array",
                    "description": "Per-experiment attribute values. Use when values differ per experiment (e.g. library_name = sample_name + strategy).",
                    "items": {
                        "type": "object",
                        "properties": {
                            "experiment_id": {"type": "string"},
                            "library_name": {"type": "string"},
                        },
                        "required": ["experiment_id"],
                    },
                },
            },
        },
        system=(
            "You are a bioinformatics metadata assistant. "
            f"Here are the current experiments (with sample_name included): {exp_summaries}. "
            "The user wants to fill in missing experiment fields. "
            "Return platform_text, instrument_text, strategy_text, layout (PAIRED/SINGLE), "
            "read_length (int bp), library_prep_kit, design_description. "
            "For fields that differ per experiment (such as library_name), "
            "use the per_experiment array with experiment_id and the field value. "
            "library_name is typically '{sample_name}_{library_strategy}' or as the user specifies. "
            "Return empty string for fields not inferable."
        ),
    )

    strategy, source, selection = normalise_library_strategy(extraction.get("strategy_text", ""))
    platform = normalise_platform(
        extraction.get("platform_text", "") + " " + extraction.get("instrument_text", "")
    )
    instrument = normalise_instrument(extraction.get("instrument_text", ""))

    layout_raw = extraction.get("layout", "")
    if layout_raw.upper() in ("PAIRED", "PE"):
        layout = "PAIRED"
    elif layout_raw.upper() in ("SINGLE", "SE"):
        layout = "SINGLE"
    else:
        layout = None

    read_length = extraction.get("read_length") or None
    library_prep_kit = extraction.get("library_prep_kit") or None
    design_description = extraction.get("design_description") or None

    # Build per-experiment library_name lookup
    per_exp_map: dict[str, dict] = {}
    for entry in (extraction.get("per_experiment") or []):
        eid = entry.get("experiment_id")
        if eid:
            per_exp_map[eid] = entry

    experiments_to_update = []
    for e in experiments[:_MAX_ITEMS_PER_LIST]:
        changes: dict = {}
        if strategy and not e.library_strategy:
            changes["library_strategy"] = strategy
        if source and not e.library_source:
            changes["library_source"] = source
        if selection and not e.library_selection:
            changes["library_selection"] = selection
        if layout and not e.library_layout:
            changes["library_layout"] = layout
        if platform and not e.platform:
            changes["platform"] = platform
        if instrument and not e.instrument_model:
            changes["instrument_model"] = instrument

        attrs_changes: dict = {}
        if read_length and not (e.attrs or {}).get("read_length"):
            attrs_changes["read_length"] = read_length
        if library_prep_kit and not (e.attrs or {}).get("library_prep_kit"):
            attrs_changes["library_prep_kit"] = library_prep_kit
        if design_description and not (e.attrs or {}).get("design_description"):
            attrs_changes["design_description"] = design_description

        # Per-experiment fields (library_name etc.)
        per = per_exp_map.get(e.id, {})
        if per.get("library_name") and not (e.attrs or {}).get("library_name"):
            attrs_changes["library_name"] = per["library_name"]

        if attrs_changes:
            changes["attrs"] = attrs_changes

        if changes:
            experiments_to_update.append({
                "key": f"experiment:{e.id}",
                "experiment_id": e.id,
                "sample_id": e.sample_id,
                "sample_name": sample_id_to_name.get(e.sample_id, ""),
                "changes": changes,
            })

    return {
        "experiments_to_update": experiments_to_update,
        "truncated": len(experiments) > _MAX_ITEMS_PER_LIST,
    }


# ---------------------------------------------------------------------------
# Task 4: Link files to experiments
# ---------------------------------------------------------------------------


async def link_files_task(
    project_id: str,
    session: AsyncSession,
    instruction: str | None = None,
) -> dict[str, Any]:
    """Return proposal payload with file_links_to_create."""
    files = (
        await session.execute(
            select(File).where(
                File.project_id == project_id,
                File.file_type.in_(["fastq", "fq"]),
            )
        )
    ).scalars().all()

    if not files:
        return {"error": "No FASTQ files found.", "file_links_to_create": []}

    # Get already-linked file IDs
    linked_file_ids = set(
        (
            await session.execute(
                select(FileRun.file_id).join(
                    Experiment, FileRun.experiment_id == Experiment.id
                ).where(Experiment.project_id == project_id)
            )
        ).scalars().all()
    )

    # Get experiments with their samples
    experiments = (
        await session.execute(
            select(Experiment).where(Experiment.project_id == project_id)
        )
    ).scalars().all()

    if not experiments:
        return {"error": "No experiments found. Create experiments first.", "file_links_to_create": []}

    # Get sample names for experiments
    sample_ids = list({e.sample_id for e in experiments})
    samples = (
        await session.execute(
            select(Sample).where(Sample.id.in_(sample_ids))
        )
    ).scalars().all()
    sample_id_to_name = {s.id: s.sample_name for s in samples}
    sample_name_to_exp_id = {sample_id_to_name[e.sample_id]: e.id for e in experiments if e.sample_id in sample_id_to_name}

    # Re-run filename inference to get file → sample mapping
    inference = await infer_samples_from_filenames(list(files))

    file_links_to_create = []
    for fname, sample_name in inference.file_to_sample.items():
        file_obj = next((f for f in files if f.filename == fname), None)
        if not file_obj or file_obj.id in linked_file_ids:
            continue
        exp_id = sample_name_to_exp_id.get(sample_name)
        if not exp_id:
            continue

        # Find read number
        candidate = next(
            (c for c in inference.candidates if c.sample_name == sample_name),
            None,
        )
        read_number = None
        if candidate:
            try:
                idx = candidate.filenames.index(fname)
                read_number = candidate.read_numbers[idx]
            except (ValueError, IndexError):
                pass

        if len(file_links_to_create) >= _MAX_ITEMS_PER_LIST:
            break

        file_links_to_create.append({
            "key": f"link:{file_obj.id}:{exp_id}",
            "file_id": file_obj.id,
            "filename": fname,
            "experiment_id": exp_id,
            "inferred_sample_name": sample_name,
            "read_number": read_number,
            "confidence": 0.9,
        })

    return {
        "file_links_to_create": file_links_to_create,
        "truncated": False,
    }


# ---------------------------------------------------------------------------
# Task 5: Check gaps
# ---------------------------------------------------------------------------


async def check_gaps_task(
    project_id: str,
    session: AsyncSession,
    instruction: str | None = None,
) -> dict[str, Any]:
    """Return a structured gap report — no DB writes."""
    health = await score_project_metadata_health(project_id, session)

    actions = []
    if health["files_total_fastq"] > 0 and health["sample_count"] == 0:
        actions.append({
            "task_type": "infer-samples",
            "label": f"{health['files_total_fastq']} FASTQ files found, no samples yet",
        })
    if health["sample_count"] > 0 and health["sample_missing"] > 0:
        actions.append({
            "task_type": "fill-samples",
            "label": f"{health['sample_missing']} samples missing required fields",
        })
    if health["experiment_count"] > 0 and health["experiment_partial"] > 0:
        actions.append({
            "task_type": "fill-experiments",
            "label": f"{health['experiment_partial']} experiments with incomplete fields",
        })
    if health["files_unlinked"] > 0:
        actions.append({
            "task_type": "link-files",
            "label": f"{health['files_unlinked']} FASTQ files not linked to any experiment",
        })

    return {
        "gap_report": {
            **health,
            "suggested_actions": actions,
        },
    }


# ---------------------------------------------------------------------------
# Apply helper — called by the API router after user confirms
# ---------------------------------------------------------------------------


async def apply_proposal_payload(
    payload: dict[str, Any],
    accepted_keys: list[str] | None,
    project_id: str,
    session: AsyncSession,
) -> dict[str, int]:
    """Write accepted proposal items to the DB. Returns counts of applied changes."""
    key_set = set(accepted_keys) if accepted_keys is not None else None

    def _accepted(key: str) -> bool:
        return key_set is None or key in key_set

    counts: dict[str, int] = {
        "samples_created": 0,
        "samples_updated": 0,
        "experiments_updated": 0,
        "links_created": 0,
        "custom_fields_registered": 0,
    }

    # --- Create samples ---
    for item in payload.get("samples_to_create") or []:
        if not _accepted(item["key"]):
            continue
        s = Sample(
            id=str(uuid.uuid4()),
            project_id=project_id,
            sample_name=item["sample_name"],
            organism=item.get("organism"),
            attrs=item.get("attrs") or {},
        )
        session.add(s)
        counts["samples_created"] += 1

    await session.flush()  # Ensure IDs are available for experiments below

    # --- Update samples ---
    for item in payload.get("samples_to_update") or []:
        if not _accepted(item["key"]):
            continue
        s = (
            await session.execute(select(Sample).where(Sample.id == item["sample_id"]))
        ).scalar_one_or_none()
        if not s:
            continue
        changes = item.get("changes") or {}
        if "organism" in changes:
            s.organism = changes["organism"]
        # Remaining changes go to attrs
        attr_changes = {k: v for k, v in changes.items() if k != "organism"}
        if attr_changes:
            current = dict(s.attrs or {})
            current.update(attr_changes)
            s.attrs = current
        counts["samples_updated"] += 1

    # --- Update experiments ---
    for item in payload.get("experiments_to_update") or []:
        if not _accepted(item["key"]):
            continue
        e = (
            await session.execute(select(Experiment).where(Experiment.id == item["experiment_id"]))
        ).scalar_one_or_none()
        if not e:
            continue
        changes = item.get("changes") or {}
        for col in ("library_strategy", "library_source", "library_selection",
                    "library_layout", "platform", "instrument_model"):
            if col in changes:
                setattr(e, col, changes[col])
        if "attrs" in changes:
            current = dict(e.attrs or {})
            current.update(changes["attrs"])
            e.attrs = current
        counts["experiments_updated"] += 1

    # --- Create file links ---
    for item in payload.get("file_links_to_create") or []:
        if not _accepted(item["key"]):
            continue
        fr = FileRun(
            id=str(uuid.uuid4()),
            experiment_id=item["experiment_id"],
            file_id=item["file_id"],
            read_number=item.get("read_number"),
            filename=item.get("filename"),
        )
        session.add(fr)
        counts["links_created"] += 1

    # --- Register custom fields ---
    if payload.get("custom_fields_to_register"):
        from tune.core.models import Project

        proj = (
            await session.execute(select(Project).where(Project.id == project_id))
        ).scalar_one_or_none()
        if proj:
            extensions = dict(proj.schema_extensions or {})
            for cf in payload["custom_fields_to_register"]:
                if not _accepted(cf["key"]):
                    continue
                obj_type = cf.get("object_type", "Sample").lower()
                field_key = f"{obj_type}_fields"
                bucket = dict(extensions.get(field_key) or {})
                fn = cf["field_name"]
                if fn not in bucket:
                    bucket[fn] = {
                        "label": fn.replace("_", " ").title(),
                        "type": cf.get("inferred_type", "string"),
                    }
                    extensions[field_key] = bucket
                    counts["custom_fields_registered"] += 1
            proj.schema_extensions = extensions

    return counts
