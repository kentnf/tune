"""Pre-flight validation for analysis jobs.

Runs before execution begins to catch all binding/path/parameter errors
up front and (optionally) auto-inject missing build steps (e.g. HISAT2 index).
"""
from __future__ import annotations

import logging
import os
import uuid as _uuid_mod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from tune.core.resources.models import ReadinessIssue, ResourceCandidate

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

log = logging.getLogger(__name__)

_FASTA_SUFFIXES = (".fa", ".fasta", ".fna")
_GTF_GZ_SUFFIXES = (".gtf.gz", ".gff.gz", ".gff3.gz")


@dataclass
class PreflightResult:
    ok: bool
    amended_plan: list[dict]          # plan with auto-injected steps
    resolved_bindings: dict[str, dict]  # {step_key: {slot_name: path|[path, ...]}}
    issues: list[ReadinessIssue]      # blocking problems
    warnings: list[ReadinessIssue]    # non-blocking notices


async def run_preflight(
    plan: list[dict],
    project_id: str,
    job_id: str,
    output_dir: str,
    db: "AsyncSession",
) -> PreflightResult:
    """Run pre-flight checks on the plan before execution.

    Steps:
    1. Resolve bindings using 4-tier resolver (reads from DB).
    2. Detect FASTA-registered-but-no-index → inject util.hisat2_build step.
    3. Detect .gtf.gz annotation → add warning (featureCounts handles gz natively).
    4. Dry-run render each step to catch RendererError.
    5. Check all resolved input paths exist on disk.

    Returns PreflightResult with ok=True if no blocking issues found.
    """
    from tune.core.binding.resolver import (
        load_registered_resource_bindings,
    )
    from tune.core.binding.semantic_retrieval import (
        retrieve_best_semantic_candidate,
        retrieve_semantic_candidates,
        summarize_candidate_ambiguity,
    )
    from tune.core.models import KnownPath, File, InputBinding
    from tune.core.registry import ensure_registry_loaded, get_step_type
    from tune.core.renderer import render_step, RendererError
    from sqlalchemy import select

    ensure_registry_loaded()

    issues: list[ReadinessIssue] = []
    warnings: list[ReadinessIssue] = []
    amended_plan = list(plan)  # we may prepend steps

    # ------------------------------------------------------------------
    # Load project files and KnownPaths
    # ------------------------------------------------------------------
    project_files: list[dict] = []
    try:
        files = (await db.execute(
            select(File).where(File.project_id == project_id).limit(500)
        )).scalars().all()
        project_files = [
            {"id": f.id, "path": f.path, "filename": f.filename, "file_type": f.file_type}
            for f in files
        ]
    except Exception:
        log.exception("run_preflight: failed to load project files")

    kp_bindings = await load_registered_resource_bindings(project_id, db)

    # ------------------------------------------------------------------
    # Detect FASTA-registered-but-no-index case → inject hisat2_build
    # ------------------------------------------------------------------
    # Skip if ResourceGraph (PlannerAdapter) already injected prepare steps.
    # Preflight serves as safety-net only when the new ResourceGraph path failed.
    _rr_already_injected = any(s.get("_rr_injected") for s in amended_plan)

    # Check: any plan step needs index_prefix but only a FASTA is registered
    needs_index_steps = [
        s for s in amended_plan
        if _step_needs_slot(s, "index_prefix")
    ]

    hisat2_build_injected = False
    build_index_prefix: str | None = None

    if needs_index_steps and not _rr_already_injected:
        has_index = _kp_has_index(kp_bindings)
        reference_fasta = kp_bindings.get("reference_fasta")

        # Fallback: scan project files for a FASTA if no KnownPath is registered.
        # This is the common case where the user has the genome in the data folder
        # but hasn't explicitly registered it as a KnownPath.
        if not reference_fasta:
            for pf in project_files:
                pf_path = pf.get("path", "")
                if pf_path and any(pf_path.lower().endswith(s) for s in _FASTA_SUFFIXES):
                    reference_fasta = pf_path
                    log.info(
                        "run_preflight: no reference_fasta KnownPath; "
                        "auto-discovered FASTA from project files: '%s'",
                        reference_fasta,
                    )
                    break

        # Check if a hisat2_build step is already present in the plan (e.g. from a
        # previous pre-flight run on an already-amended plan).
        existing_build = next(
            (s for s in amended_plan if s.get("step_type") == "util.hisat2_build"),
            None,
        )

        if existing_build:
            # Already injected — compute the expected output prefix and set the flag
            # so downstream resolution uses it without inserting a duplicate step.
            build_out_dir = existing_build.get("_output_dir") or os.path.join(output_dir, "00_hisat2_build")
            build_index_prefix = os.path.join(build_out_dir, "hisat2_index", "genome")
            hisat2_build_injected = True
            log.debug("run_preflight: hisat2_build already in plan, reusing prefix %s", build_index_prefix)
        elif not has_index and reference_fasta:
            # Inject util.hisat2_build before the first step that needs index_prefix
            build_step_key = "_preflight_hisat2_build"
            build_out_dir = os.path.join(output_dir, "00_hisat2_build")
            build_index_prefix = os.path.join(build_out_dir, "hisat2_index", "genome")

            build_step: dict = {
                "step_key": build_step_key,
                "step_type": "util.hisat2_build",
                "display_name": "Build HISAT2 Index",
                "params": {"threads": 4},
                "depends_on": [],
                "_preflight_injected": True,
                "_output_dir": build_out_dir,
                # Store the resolved binding so tasks.py can use it
                "_resolved_bindings": {"reference_fasta": reference_fasta},
            }

            # Insert build step before the first step needing the index
            first_idx = next(
                (i for i, s in enumerate(amended_plan) if _step_needs_slot(s, "index_prefix")),
                0,
            )
            amended_plan.insert(first_idx, build_step)

            # Update downstream steps to depend on the build step
            for s in amended_plan:
                if _step_needs_slot(s, "index_prefix") and s is not build_step:
                    deps = list(s.get("depends_on") or [])
                    if build_step_key not in deps:
                        deps.append(build_step_key)
                    s["depends_on"] = deps

            hisat2_build_injected = True
            warnings.append(
                _issue_for_auto_build(
                    title="HISAT2 index will be built automatically",
                    description=(
                        f"No HISAT2 index was found. Tune will build one from FASTA "
                        f"'{reference_fasta}' before alignment."
                    ),
                    suggestion="No user action needed unless you want to provide a prebuilt HISAT2 index.",
                    affected_step_keys=[
                        s.get("step_key", "")
                        for s in amended_plan
                        if _step_needs_slot(s, "index_prefix") and s is not build_step
                    ],
                )
            )

    # ------------------------------------------------------------------
    # Detect FASTA-registered-but-no-STAR-genome case → inject star_genome_generate
    # ------------------------------------------------------------------
    needs_genome_dir_steps = [
        s for s in amended_plan
        if _step_needs_slot(s, "genome_dir")
    ]

    star_genome_injected = False
    star_genome_dir_path: str | None = None

    if needs_genome_dir_steps and not _rr_already_injected:
        # Check if genome_dir is already registered as a KnownPath
        has_star_genome = _kp_has_star_genome(kp_bindings)

        # Resolve reference FASTA: KnownPath first, then project file scan
        _star_reference_fasta = kp_bindings.get("reference_fasta")
        if not _star_reference_fasta:
            for pf in project_files:
                pf_path = pf.get("path", "")
                if pf_path and any(pf_path.lower().endswith(s) for s in _FASTA_SUFFIXES):
                    _star_reference_fasta = pf_path
                    break

        # Check if star_genome_generate already present (idempotent re-run)
        existing_star_build = next(
            (s for s in amended_plan if s.get("step_type") == "util.star_genome_generate"),
            None,
        )

        if existing_star_build:
            star_genome_dir_path = (
                existing_star_build.get("_output_dir")
                or os.path.join(output_dir, "00_star_genome")
            )
            star_genome_dir_path = os.path.join(star_genome_dir_path, "star_genome")
            star_genome_injected = True
            log.debug("run_preflight: star_genome_generate already in plan, reusing dir %s", star_genome_dir_path)
        elif not has_star_genome and _star_reference_fasta:
            build_out_dir = os.path.join(output_dir, "00_star_genome")
            star_genome_dir_path = os.path.join(build_out_dir, "star_genome")

            # Discover annotation GTF for STAR (improves splice-site accuracy)
            _star_gtf = kp_bindings.get("annotation_gtf")
            if not _star_gtf:
                _GTF_ALL = (".gtf", ".gff", ".gff3", ".gtf.gz", ".gff.gz", ".gff3.gz")
                for pf in project_files:
                    pf_path = pf.get("path", "")
                    if pf_path and any(pf_path.lower().endswith(s) for s in _GTF_ALL):
                        _star_gtf = pf_path
                        break

            build_star_step: dict = {
                "step_key": "_preflight_star_genome",
                "step_type": "util.star_genome_generate",
                "display_name": "Build STAR Genome Index",
                "params": {"threads": 4, "genome_sa_index_nbases": 11},
                "depends_on": [],
                "_preflight_injected": True,
                "_output_dir": build_out_dir,
                "_resolved_bindings": {
                    "reference_fasta": _star_reference_fasta,
                    **({"annotation_gtf": _star_gtf} if _star_gtf else {}),
                },
            }

            # Insert before the first step needing genome_dir
            first_idx = next(
                (i for i, s in enumerate(amended_plan) if _step_needs_slot(s, "genome_dir")),
                0,
            )
            amended_plan.insert(first_idx, build_star_step)

            # Update downstream steps to depend on the build step
            for s in amended_plan:
                if _step_needs_slot(s, "genome_dir") and s is not build_star_step:
                    deps = list(s.get("depends_on") or [])
                    if "_preflight_star_genome" not in deps:
                        deps.append("_preflight_star_genome")
                    s["depends_on"] = deps

            star_genome_injected = True
            warnings.append(
                _issue_for_auto_build(
                    title="STAR genome index will be built automatically",
                    description=(
                        f"No STAR genome index was found. Tune will build one from FASTA "
                        f"'{_star_reference_fasta}' before alignment."
                    ),
                    suggestion="No user action needed unless you want to provide a prebuilt STAR genome directory.",
                    affected_step_keys=[
                        s.get("step_key", "")
                        for s in amended_plan
                        if _step_needs_slot(s, "genome_dir") and s is not build_star_step
                    ],
                )
            )

    # ------------------------------------------------------------------
    # Detect .gtf.gz annotation
    # ------------------------------------------------------------------
    annotation_gtf = kp_bindings.get("annotation_gtf")

    # Fallback: scan project files for GTF/GFF if no KnownPath registered.
    # Also picks up .gtf.gz which _file_matches_types misses (it checks .gtf only).
    if not annotation_gtf:
        _GTF_ALL = (".gtf", ".gff", ".gff3", ".gtf.gz", ".gff.gz", ".gff3.gz")
        for pf in project_files:
            pf_path = pf.get("path", "")
            if pf_path and any(pf_path.lower().endswith(s) for s in _GTF_ALL):
                annotation_gtf = pf_path
                log.info(
                    "run_preflight: no annotation_gtf KnownPath; "
                    "auto-discovered annotation from project files: '%s'",
                    annotation_gtf,
                )
                # Also inject into kp_bindings so the per-slot loop below can use it
                kp_bindings["annotation_gtf"] = annotation_gtf
                break

    if annotation_gtf and any(annotation_gtf.lower().endswith(s) for s in _GTF_GZ_SUFFIXES):
        warnings.append(
            _issue_for_compressed_annotation(annotation_gtf)
        )

    # ------------------------------------------------------------------
    # Resolve bindings for each step and collect resolved_bindings map
    # ------------------------------------------------------------------
    resolved_bindings: dict[str, dict] = {}

    for step in amended_plan:
        step_key = step.get("step_key", "")
        step_type = step.get("step_type", "")

        # Preflight-injected steps already have _resolved_bindings
        if step.get("_preflight_injected"):
            resolved_bindings[step_key] = dict(step.get("_resolved_bindings") or {})
            continue

        defn = get_step_type(step_type)
        if defn is None:
            issues.append(_issue_for_unknown_step_type(step_key, step_type))
            continue

        step_resolved: dict[str, str | list[str]] = {}
        preferred_lineage = dict(step.get("_preferred_lineage") or {}) or None

        for slot in defn.input_slots:
            resolved_value: str | list[str] | None = None

            # Already bound via InputBinding DB records?
            try:
                run_id = step.get("_run_id") or step.get("run_id") or ""
                if run_id:
                    existing_rows = (await db.execute(
                        select(InputBinding).where(
                            InputBinding.job_id == job_id,
                            InputBinding.step_id == run_id,
                            InputBinding.slot_name == slot.name,
                            InputBinding.status == "resolved",
                        )
                    )).scalars().all()
                    existing_paths = [
                        row.resolved_path
                        for row in existing_rows
                        if getattr(row, "resolved_path", None)
                    ]
                    if existing_paths:
                        resolved_value = (
                            existing_paths
                            if slot.multiple
                            else existing_paths[0]
                        )
            except Exception:
                pass  # Best-effort

            # Injected build step provides index_prefix for downstream steps
            if not resolved_value and slot.name == "index_prefix" and hisat2_build_injected:
                resolved_value = build_index_prefix

            # Injected star_genome_generate provides genome_dir for downstream steps
            if not resolved_value and slot.name == "genome_dir" and star_genome_injected:
                resolved_value = star_genome_dir_path

            # KnownPath
            if not resolved_value:
                kp = kp_bindings.get(slot.name)
                if kp:
                    resolved_value = kp

            # Semantic resolver: respects fan-out lineage and existing role rules.
            if not resolved_value:
                dep_keys = list(step.get("depends_on") or [])
                if slot.multiple:
                    candidates = await retrieve_semantic_candidates(
                        job_id=job_id,
                        dep_keys=dep_keys,
                        slot=slot,
                        project_id=project_id,
                        project_files=project_files,
                        kp_bindings=kp_bindings,
                        db=db,
                        preferred_lineage=preferred_lineage,
                    )
                    resolved_paths = []
                    for candidate in candidates:
                        path_value = candidate.get("file_path")
                        if path_value and path_value not in resolved_paths:
                            resolved_paths.append(path_value)
                    if resolved_paths:
                        resolved_value = resolved_paths
                else:
                    candidates = await retrieve_semantic_candidates(
                        job_id=job_id,
                        dep_keys=dep_keys,
                        slot=slot,
                        project_id=project_id,
                        project_files=project_files,
                        kp_bindings=kp_bindings,
                        db=db,
                        preferred_lineage=preferred_lineage,
                    )
                    ambiguity = summarize_candidate_ambiguity(candidates[:3])
                    if ambiguity:
                        issues.append(
                            _issue_for_ambiguous_slot(
                                step_key=step_key,
                                step_type=step_type,
                                slot_name=slot.name,
                                candidates=candidates[:6],
                                ambiguity=ambiguity,
                            )
                        )
                        continue
                    candidate = candidates[0] if candidates else None
                    if candidate and candidate.get("file_path"):
                        resolved_value = candidate["file_path"]

            if resolved_value:
                step_resolved[slot.name] = resolved_value
            elif slot.required:
                # If the step has upstream deps that can provide this slot's file type,
                # defer to runtime — don't flag as a blocking issue.
                if _upstream_can_provide(step, slot, amended_plan):
                    log.debug(
                        "run_preflight: slot '%s.%s' deferred to runtime (upstream dep will provide)",
                        step_key, slot.name,
                    )
                else:
                    issues.append(_issue_for_missing_slot(step_key, step_type, slot.name))

        resolved_bindings[step_key] = step_resolved

    # ------------------------------------------------------------------
    # Dry-run render each step
    # ------------------------------------------------------------------
    for step in amended_plan:
        step_key = step.get("step_key", "")
        step_type = step.get("step_type", "")
        if not step_type:
            continue

        # Skip render for steps that have upstream deps providing missing slots —
        # those bindings will be resolved at runtime when output dirs exist.
        defn = get_step_type(step_type)
        if defn and step.get("depends_on"):
            deferred_slots = {
                slot.name for slot in defn.input_slots
                if slot.required
                   and slot.name not in resolved_bindings.get(step_key, {})
                   and _upstream_can_provide(step, slot, amended_plan)
            }
            if deferred_slots:
                log.debug(
                    "run_preflight: skipping dry-run render for '%s' (deferred slots: %s)",
                    step_key, deferred_slots,
                )
                continue

        step_bindings = resolved_bindings.get(step_key, {})
        dummy_out = os.path.join(output_dir, f"_preflight_{step_key}")

        try:
            render_step(step_type, step.get("params") or {}, step_bindings, dummy_out)
        except RendererError as exc:
            issues.append(_issue_for_render_error(step_key, step_type, str(exc)))
        except Exception as exc:
            log.debug("run_preflight: unexpected render error for %s: %s", step_key, exc)

    # ------------------------------------------------------------------
    # Path existence checks
    # ------------------------------------------------------------------
    for step in amended_plan:
        step_key = step.get("step_key", "")
        step_bindings = resolved_bindings.get(step_key, {})

        # Skip the build step output path — it doesn't exist yet (will be created)
        skip_slots = set()
        if step.get("_preflight_injected"):
            continue  # Its output will be created at runtime
        if hisat2_build_injected:
            skip_slots.add("index_prefix")  # Doesn't exist yet
        if star_genome_injected:
            skip_slots.add("genome_dir")    # Doesn't exist yet

        for slot_name, path_value in step_bindings.items():
            if slot_name in skip_slots:
                continue
            paths = path_value if isinstance(path_value, list) else [path_value]
            paths = [path for path in paths if path]
            if not paths:
                continue

            for path in paths:
                # index_prefix is a path prefix (e.g. /ref/genome), not a single file.
                # Check for any *.ht2 or *.bt2 files matching the prefix instead.
                if slot_name == "index_prefix":
                    import glob as _glob
                    if not (_glob.glob(path + "*.ht2") or _glob.glob(path + "*.bt2")):
                        # Only fail if there's no injected build step that will create it
                        if not hisat2_build_injected:
                            issues.append(
                                _issue_for_missing_concrete_path(
                                    step_key=step_key,
                                    step_type=step.get("step_type", ""),
                                    slot_name="index_prefix",
                                    path=path,
                                    detail="No index files found at the provided prefix.",
                                )
                            )
                    continue
                if not os.path.exists(path):
                    issues.append(
                        _issue_for_missing_concrete_path(
                            step_key=step_key,
                            step_type=step.get("step_type", ""),
                            slot_name=slot_name,
                            path=path,
                        )
                    )

    return PreflightResult(
        ok=len(issues) == 0,
        amended_plan=amended_plan,
        resolved_bindings=resolved_bindings,
        issues=issues,
        warnings=warnings,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _step_needs_slot(step: dict, slot_name: str) -> bool:
    """Return True if the step's step_type has a slot named slot_name."""
    from tune.core.registry import ensure_registry_loaded, get_step_type
    ensure_registry_loaded()
    defn = get_step_type(step.get("step_type", ""))
    if defn is None:
        return False
    return any(s.name == slot_name for s in defn.input_slots)


def _upstream_can_provide(step: dict, slot, amended_plan: list[dict]) -> bool:
    """Return True if any ancestor step (direct or transitive) can produce the slot's file type.

    Uses BFS over the full dependency DAG so that indirect dependencies
    (e.g. sort_samtools two levels above featurecounts) are also considered.
    """
    from tune.core.registry import ensure_registry_loaded, get_step_type
    from tune.core.binding.resolver import _file_matches_types

    ensure_registry_loaded()

    # Special case: slots that expect the upstream dep's output *directory* (e.g.
    # qc.multiqc's input_dir).  These are always deferred to runtime as long as
    # the step has at least one declared dependency.
    if getattr(slot, "from_upstream_dir", False):
        return bool(step.get("depends_on"))

    step_map = {s.get("step_key"): s for s in amended_plan}

    visited: set[str] = set()
    queue: list[str] = list(step.get("depends_on") or [])

    while queue:
        dep_key = queue.pop(0)
        if dep_key in visited:
            continue
        visited.add(dep_key)

        dep_step = step_map.get(dep_key)
        if dep_step is None:
            continue
        dep_defn = get_step_type(dep_step.get("step_type", ""))
        if dep_defn is None:
            continue
        for out_slot in dep_defn.output_slots:
            # Slot-name match: if upstream output slot name equals the required slot name,
            # the upstream step can directly provide it (e.g. hisat2_build outputs
            # "index_prefix" → satisfies align.hisat2's "index_prefix" input).
            if out_slot.name == slot.name:
                return True
            # If the dep produces any file type that this slot accepts, defer.
            # BUT: skip wildcard slots ('*') on EITHER side — a '*' slot
            # (e.g. genome_dir) should not be considered satisfiable by an
            # upstream step just because the slot has no type constraint.
            # Wildcard slots must be explicitly bound via KnownPath/InputBinding.
            if out_slot.file_types == ["*"] or slot.file_types == ["*"]:
                continue  # do NOT short-circuit for wildcard; keep looking
            for ft in out_slot.file_types:
                if any(_file_matches_types(f"dummy.{ft}", [t]) for t in slot.file_types):
                    return True
        # Traverse this dep's own dependencies (transitive)
        queue.extend(dep_step.get("depends_on") or [])

    return False


def _kp_has_index(kp_bindings: dict[str, str]) -> bool:
    """Return True if KnownPath bindings include a proper HISAT2 index prefix
    AND the index files actually exist on disk.

    A proper index is a path that is NOT a raw FASTA file AND has .ht2/.bt2 files.
    """
    import glob as _glob
    index_prefix = kp_bindings.get("index_prefix")
    if not index_prefix:
        return False
    # If the value looks like a FASTA file, it's not a built index
    lower = index_prefix.lower()
    if any(lower.endswith(s) for s in (".fa", ".fasta", ".fna")):
        return False
    # Verify index files actually exist — a stale KnownPath pointing to a
    # deleted job directory should not block hisat2_build injection.
    if not (_glob.glob(index_prefix + "*.ht2") or _glob.glob(index_prefix + "*.bt2")):
        log.debug(
            "_kp_has_index: KnownPath index_prefix '%s' has no .ht2/.bt2 files "
            "(stale entry?) — treating as no-index",
            index_prefix,
        )
        return False
    return True


def _kp_has_star_genome(kp_bindings: dict[str, str]) -> bool:
    """Return True if KnownPath bindings include a valid STAR genome directory
    that actually exists on disk (contains the 'SA' file created by genomeGenerate).
    """
    genome_dir = kp_bindings.get("star_genome_dir") or kp_bindings.get("genome_dir")
    if not genome_dir:
        return False
    # Must be a directory, not a file
    if not os.path.isdir(genome_dir):
        log.debug(
            "_kp_has_star_genome: genome_dir '%s' is not a directory — treating as no-genome",
            genome_dir,
        )
        return False
    # Verify the STAR SA index file exists
    sa_file = os.path.join(genome_dir, "SA")
    if not os.path.exists(sa_file):
        log.debug(
            "_kp_has_star_genome: genome_dir '%s' has no SA file "
            "(stale or incomplete?) — treating as no-genome",
            genome_dir,
        )
        return False
    return True


def _slot_issue_fields(slot_name: str) -> tuple[str, str, str, str]:
    if slot_name in {"read1", "read2", "reads"}:
        return (
            "missing_reads",
            "Input read files not resolved",
            "The required sequencing read inputs could not be resolved for this step.",
            "Link FASTQ files to experiments or provide the correct read paths.",
        )
    if slot_name == "annotation_gtf":
        return (
            "missing_annotation",
            "Annotation input not resolved",
            "The required annotation GTF/GFF file could not be resolved for this step.",
            "Register an annotation file via known paths or add it to the project.",
        )
    if slot_name in {"index_prefix", "genome_dir"}:
        return (
            "missing_index",
            "Reference index input not resolved",
            "The required aligner index input could not be resolved for this step.",
            "Provide a compatible prebuilt index or reference resource.",
        )
    if slot_name == "reference_fasta":
        return (
            "missing_reference",
            "Reference FASTA input not resolved",
            "The required reference FASTA could not be resolved for this step.",
            "Register a reference FASTA via known paths or add it to the project.",
        )
    return (
        "missing_input_slot",
        f"Required input '{slot_name}' not resolved",
        f"The required input slot '{slot_name}' could not be resolved for this step.",
        "Provide the missing input or revise the plan so the required artifact is produced upstream.",
    )


def _slot_binding_key(slot_name: str, *, step_type: str | None = None) -> str | None:
    if slot_name == "annotation_gtf":
        return "annotation_gtf"
    if slot_name == "reference_fasta":
        return "reference_fasta"
    if slot_name == "genome_dir":
        return "star_genome_dir"
    if slot_name == "index_prefix":
        mapping = {
            "align.hisat2": "hisat2_index",
            "align.bwa": "bwa_index",
            "align.bowtie2": "bowtie2_index",
        }
        if step_type:
            return mapping.get(step_type)
    return None


def _issue_for_auto_build(
    *,
    title: str,
    description: str,
    suggestion: str,
    affected_step_keys: list[str],
) -> ReadinessIssue:
    return ReadinessIssue(
        kind="missing_index",
        severity="warning",
        title=title,
        description=description,
        suggestion=suggestion,
        affected_step_keys=[key for key in affected_step_keys if key],
        resolution_type="confirm_auto_build",
    )


def _issue_for_compressed_annotation(path: str) -> ReadinessIssue:
    return ReadinessIssue(
        kind="incomplete_metadata",
        severity="warning",
        title="Annotation file is gzip-compressed",
        description=(
            f"Annotation file '{path}' is gzip-compressed. featureCounts can consume "
            "compressed annotation files directly."
        ),
        suggestion="No decompression needed unless a downstream tool explicitly requires an uncompressed file.",
        resolution_type="provide_path",
    )


def _issue_for_unknown_step_type(step_key: str, step_type: str) -> ReadinessIssue:
    return ReadinessIssue(
        kind="unknown_step_type",
        severity="blocking",
        title="Unknown step type in plan",
        description=f"Step '{step_key}' references unknown step_type '{step_type}'.",
        suggestion="Revise the plan to use a registered step type.",
        affected_step_keys=[step_key] if step_key else [],
    )


def _issue_for_missing_slot(step_key: str, step_type: str, slot_name: str) -> ReadinessIssue:
    kind, title, description, suggestion = _slot_issue_fields(slot_name)
    issue = ReadinessIssue(
        kind=kind,  # type: ignore[arg-type]
        severity="blocking",
        title=title,
        description=f"{description} Step '{step_key}' ({step_type}) needs slot '{slot_name}'.",
        suggestion=suggestion,
        affected_step_keys=[step_key] if step_key else [],
        resolution_type="provide_path",
    )
    binding_key = _slot_binding_key(slot_name, step_type=step_type)
    if binding_key:
        setattr(issue, "binding_key", binding_key)
    return issue


def _issue_for_ambiguous_slot(
    *,
    step_key: str,
    step_type: str,
    slot_name: str,
    candidates: list[dict],
    ambiguity: dict,
) -> ReadinessIssue:
    if slot_name == "reference_fasta":
        issue_kind = "ambiguous_reference"
        title = "Reference FASTA input is ambiguous"
    elif slot_name == "annotation_gtf":
        issue_kind = "ambiguous_annotation"
        title = "Annotation input is ambiguous"
    elif slot_name in {"index_prefix", "genome_dir"}:
        issue_kind = "ambiguous_index"
        title = "Reference index input is ambiguous"
    else:
        issue_kind = "missing_concrete_path"
        title = f"Required input '{slot_name}' is ambiguous"

    issue = ReadinessIssue(
        kind=issue_kind,  # type: ignore[arg-type]
        severity="blocking",
        title=title,
        description=(
            f"Step '{step_key}' ({step_type}) has multiple close candidates for slot '{slot_name}'. "
            f"Top candidates are '{ambiguity.get('primary_path')}' and '{ambiguity.get('secondary_path')}', "
            f"with score gap {ambiguity.get('score_gap')}."
        ),
        suggestion="Select the correct candidate before execution continues.",
        affected_step_keys=[step_key] if step_key else [],
        resolution_type="select_candidate",
        candidates=[],
        details={
            "slot_name": slot_name,
            "semantic_candidates": [
                {
                    "path": candidate.get("file_path"),
                    "source_type": candidate.get("source_type"),
                    "score": candidate.get("score"),
                    "organism": candidate.get("organism"),
                    "genome_build": candidate.get("genome_build"),
                }
                for candidate in candidates
            ],
            "ambiguity_summary": dict(ambiguity),
        },
    )
    issue.candidates = [
        ResourceCandidate(
            path=str(candidate.get("file_path") or ""),
            organism=candidate.get("organism"),
            genome_build=candidate.get("genome_build"),
            source_type=candidate.get("source_type"),
            confidence=max(min(float(candidate.get("score", 0)) / 100.0, 1.0), 0.0),
        )
        for candidate in candidates
        if candidate.get("file_path")
    ]
    binding_key = _slot_binding_key(slot_name, step_type=step_type)
    if binding_key:
        setattr(issue, "binding_key", binding_key)
    return issue


def _issue_for_render_error(step_key: str, step_type: str, detail: str) -> ReadinessIssue:
    return ReadinessIssue(
        kind="render_error",
        severity="blocking",
        title="Step command could not be rendered",
        description=(
            f"Step '{step_key}' ({step_type}) could not be rendered during preflight: {detail}"
        ),
        suggestion="Provide the missing required inputs or revise the step parameters.",
        affected_step_keys=[step_key] if step_key else [],
        resolution_type="provide_path",
    )


def _issue_for_missing_concrete_path(
    *,
    step_key: str,
    step_type: str,
    slot_name: str,
    path: str,
    detail: str | None = None,
) -> ReadinessIssue:
    kind, title, description, suggestion = _slot_issue_fields(slot_name)
    issue_kind = kind if kind != "missing_input_slot" else "missing_concrete_path"
    issue = ReadinessIssue(
        kind=issue_kind,  # type: ignore[arg-type]
        severity="blocking",
        title=title,
        description=(
            f"{description} Step '{step_key}' ({step_type}) resolved slot '{slot_name}' "
            f"to '{path}', but that path is not usable. {detail or 'Resolved path does not exist.'}"
        ),
        suggestion=suggestion,
        affected_step_keys=[step_key] if step_key else [],
        resolution_type="provide_path",
    )
    binding_key = _slot_binding_key(slot_name, step_type=step_type)
    if binding_key:
        setattr(issue, "binding_key", binding_key)
    return issue
