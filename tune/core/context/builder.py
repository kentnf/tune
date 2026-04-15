"""PlannerContextBuilder — assembles PlannerContext from the project-state view.

Usage:
    async with get_session_factory()() as session:
        ctx = await PlannerContextBuilder(session).build(
            ContextScope(project_id="...")
        )
"""
from __future__ import annotations

import logging
from types import SimpleNamespace

from sqlalchemy.ext.asyncio import AsyncSession

from tune.core.context.graph_loader import ProjectGraphLoader
from tune.core.context.models import (
    AnalysisSummary,
    ContextScope,
    ExperimentPlannerInfo,
    FilePlannerInfo,
    PlannerContext,
    ProjectPlannerInfo,
    SamplePlannerInfo,
)
from tune.core.context.semantic_dossier import build_semantic_memory_dossier
from tune.core.context.normalizer import build_summary
from tune.core.context.resolver import (
    resolve_files_from_file_runs,
    resolve_files_from_project,
)
from tune.core.memory.project_memory import (
    build_project_memory_profile,
    query_project_memory_facts,
    query_project_memory_patterns,
    query_project_memory_preferences,
    query_recent_project_episodes,
)
from tune.core.models import File, KnownPath, ResourceEntity
from tune.core.project_state import build_project_state
from tune.core.registry.steps import SlotDefinition
from tune.core.resources.entities import sync_project_resource_entities
from tune.core.resources.graph_builder import ResourceGraphBuilder

log = logging.getLogger(__name__)


class PlannerContextBuilder:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._loader = ProjectGraphLoader()

    async def build(self, scope: ContextScope) -> PlannerContext:
        """Assemble PlannerContext from the given scope."""
        if scope.file_ids is not None:
            return await self._build_file_set(scope)
        if scope.project_id:
            return await self._build_project(scope)
        return self._empty_context()

    async def _load_project_resource_entities(
        self,
        project_id: str,
        file_map: dict[str, File],
        known_paths: list[KnownPath],
    ) -> list[ResourceEntity]:
        try:
            await sync_project_resource_entities(
                self._session,
                project_id,
                file_map,
                known_paths,
            )
        except Exception:
            log.exception(
                "PlannerContextBuilder: resource entity sync failed for project %s; continuing without sync",
                project_id,
            )
        try:
            return await self._loader.load_resource_entities(project_id, self._session)
        except Exception:
            log.exception(
                "PlannerContextBuilder: resource entity load failed for project %s; continuing without entities",
                project_id,
            )
            return []

    @staticmethod
    def _serialize_resource_entities(resource_entities: list[ResourceEntity]) -> list[dict]:
        return [
            {
                "id": entity.id,
                "resource_role": entity.resource_role,
                "display_name": entity.display_name,
                "organism": entity.organism,
                "genome_build": entity.genome_build,
                "status": entity.status,
                "source_type": entity.source_type,
                "components": [
                    {
                        "file_id": rf.file_id,
                        "path": rf.file.path if rf.file is not None else None,
                        "file_role": rf.file_role,
                        "is_primary": rf.is_primary,
                    }
                    for rf in (entity.resource_files or [])
                ],
            }
            for entity in resource_entities
        ]

    @staticmethod
    def _project_info_from_project_state(project_state: dict) -> ProjectPlannerInfo:
        project = dict(project_state.get("project") or {})
        return ProjectPlannerInfo(
            id=str(project.get("id") or ""),
            name=str(project.get("name") or ""),
            project_dir=str(project.get("project_dir") or ""),
            project_goal=str(project_state.get("project_brief_md") or "").strip() or None,
            project_info=dict(project.get("project_info") or {}),
            known_paths=[],
            resource_entities=[],
        )

    @staticmethod
    def _samples_from_project_state(project_state: dict) -> list[SamplePlannerInfo]:
        lineage = dict(project_state.get("lineage") or {})
        linked_groups = list(lineage.get("linked_groups") or [])
        if linked_groups:
            samples: list[SamplePlannerInfo] = []
            seen: set[str] = set()
            for group in linked_groups:
                if not isinstance(group, dict):
                    continue
                item = dict(group.get("sample") or {})
                sample_id = str(item.get("id") or "").strip()
                if not sample_id or sample_id in seen:
                    continue
                seen.add(sample_id)
                attrs = dict(item.get("biological_context") or {})
                if item.get("description") and "description" not in attrs:
                    attrs["description"] = item.get("description")
                if item.get("sample_title") and "sample_title" not in attrs:
                    attrs["sample_title"] = item.get("sample_title")
                samples.append(
                    SamplePlannerInfo(
                        id=sample_id,
                        sample_name=str(item.get("sample_name") or ""),
                        organism=item.get("organism"),
                        attrs=attrs,
                    )
                )
            return samples
        samples: list[SamplePlannerInfo] = []
        for item in list(project_state.get("samples") or []):
            if not isinstance(item, dict):
                continue
            samples.append(
                SamplePlannerInfo(
                    id=str(item.get("id") or ""),
                    sample_name=str(item.get("sample_name") or ""),
                    organism=item.get("organism"),
                    attrs=dict(item.get("attrs") or {}),
                )
            )
        return samples

    @staticmethod
    def _experiments_from_project_state(project_state: dict) -> list[ExperimentPlannerInfo]:
        lineage = dict(project_state.get("lineage") or {})
        linked_groups = list(lineage.get("linked_groups") or [])
        if linked_groups:
            experiments: list[ExperimentPlannerInfo] = []
            seen: set[str] = set()
            for group in linked_groups:
                if not isinstance(group, dict):
                    continue
                item = dict(group.get("experiment") or {})
                sample = dict(group.get("sample") or {})
                experiment_id = str(item.get("id") or "").strip()
                if not experiment_id or experiment_id in seen:
                    continue
                seen.add(experiment_id)
                file_ids = [
                    str(file_info.get("file_id") or "")
                    for file_info in list(group.get("files") or [])
                    if isinstance(file_info, dict) and str(file_info.get("file_id") or "").strip()
                ]
                experiments.append(
                    ExperimentPlannerInfo(
                        id=experiment_id,
                        sample_id=str(sample.get("id") or ""),
                        library_strategy=item.get("library_strategy"),
                        library_layout=item.get("library_layout"),
                        platform=item.get("platform"),
                        instrument_model=item.get("instrument_model"),
                        file_ids=file_ids,
                    )
                )
            return experiments
        experiments: list[ExperimentPlannerInfo] = []
        for item in list(project_state.get("experiments") or []):
            if not isinstance(item, dict):
                continue
            file_ids = [
                str(file_id)
                for file_id in list(item.get("file_ids") or [])
                if str(file_id or "").strip()
            ]
            experiments.append(
                ExperimentPlannerInfo(
                    id=str(item.get("id") or ""),
                    sample_id=str(item.get("sample_id") or ""),
                    library_strategy=item.get("library_strategy"),
                    library_layout=item.get("library_layout"),
                    platform=item.get("platform"),
                    instrument_model=item.get("instrument_model"),
                    file_ids=file_ids,
                )
            )
        return experiments

    @staticmethod
    def _files_from_project_state(project_state: dict) -> list[FilePlannerInfo]:
        lineage = dict(project_state.get("lineage") or {})
        linked_groups = list(lineage.get("linked_groups") or [])
        if linked_groups:
            files: list[FilePlannerInfo] = []
            seen: set[str] = set()
            for group in linked_groups:
                if not isinstance(group, dict):
                    continue
                sample = dict(group.get("sample") or {})
                experiment = dict(group.get("experiment") or {})
                for item in list(group.get("files") or []):
                    if not isinstance(item, dict):
                        continue
                    file_id = str(item.get("file_id") or "").strip()
                    if not file_id or file_id in seen:
                        continue
                    seen.add(file_id)
                    files.append(
                        FilePlannerInfo(
                            id=file_id,
                            path=str(item.get("path") or ""),
                            filename=str(item.get("filename") or ""),
                            file_type=str(item.get("file_type") or ""),
                            read_number=item.get("read_number"),
                            linked_sample_id=str(sample.get("id") or "") or None,
                            linked_experiment_id=str(experiment.get("id") or "") or None,
                            intrinsic={},
                        )
                    )
            return files
        files: list[FilePlannerInfo] = []
        for item in list(project_state.get("files") or []):
            if not isinstance(item, dict):
                continue
            files.append(
                FilePlannerInfo(
                    id=str(item.get("id") or ""),
                    path=str(item.get("path") or ""),
                    filename=str(item.get("filename") or ""),
                    file_type=str(item.get("file_type") or ""),
                    read_number=item.get("read_number"),
                    linked_sample_id=item.get("linked_sample_id"),
                    linked_experiment_id=item.get("linked_experiment_id"),
                    intrinsic=dict(item.get("intrinsic") or {}),
                )
            )
        return files

    @staticmethod
    def _summary_from_project_state(project_state: dict) -> AnalysisSummary:
        payload = dict(project_state.get("summary") or {})
        return AnalysisSummary(
            total_files=int(payload.get("total_files") or 0),
            files_by_type=dict(payload.get("files_by_type") or {}),
            sample_count=int(payload.get("sample_count") or 0),
            experiment_count=int(payload.get("experiment_count") or 0),
            library_strategies=list(payload.get("library_strategies") or []),
            organisms=list(payload.get("organisms") or []),
            is_paired_end=payload.get("is_paired_end"),
            has_reference_genome=bool(payload.get("has_reference_genome", False)),
            files_without_samples=int(payload.get("files_without_samples") or 0),
            metadata_completeness=payload.get("metadata_completeness") or "missing",
            suggested_analysis_type=payload.get("suggested_analysis_type"),
            analysis_family=payload.get("analysis_family"),
            required_resource_roles=list(payload.get("required_resource_roles") or []),
            planning_hints=list(payload.get("planning_hints") or []),
            potential_issues=list(payload.get("potential_issues") or []),
            resource_candidates=[],
            memory_hints=[],
            stable_facts=[],
            semantic_hints=[],
            ambiguity_hints=[],
        )

    @staticmethod
    def _planner_context_from_project_state(project_state: dict) -> PlannerContext:
        project = PlannerContextBuilder._project_info_from_project_state(project_state)
        samples = PlannerContextBuilder._samples_from_project_state(project_state)
        experiments = PlannerContextBuilder._experiments_from_project_state(project_state)
        files = PlannerContextBuilder._files_from_project_state(project_state)
        summary = PlannerContextBuilder._summary_from_project_state(project_state)
        file_to_sample = {
            file_info.id: str(file_info.linked_sample_id or "")
            for file_info in files
            if str(file_info.linked_sample_id or "").strip()
        }
        file_to_experiment = {
            file_info.id: str(file_info.linked_experiment_id or "")
            for file_info in files
            if str(file_info.linked_experiment_id or "").strip()
        }
        return PlannerContext(
            context_mode="project",
            project=project,
            samples=samples,
            experiments=experiments,
            files=files,
            file_to_sample=file_to_sample,
            file_to_experiment=file_to_experiment,
            summary=summary,
            project_state=dict(project_state),
        )

    @staticmethod
    def _project_file_payloads(file_infos, samples) -> list[dict]:
        sample_by_id = {sample.id: sample for sample in samples}
        payloads = []
        for file_info in file_infos:
            sample = sample_by_id.get(file_info.linked_sample_id or "")
            payloads.append(
                {
                    "id": file_info.id,
                    "path": file_info.path,
                    "filename": file_info.filename,
                    "file_type": file_info.file_type,
                    "linked_sample_id": file_info.linked_sample_id,
                    "linked_experiment_id": file_info.linked_experiment_id,
                    "sample_name": sample.sample_name if sample is not None else None,
                    "read_number": file_info.read_number,
                }
            )
        return payloads

    @staticmethod
    def _known_path_bindings(known_paths: list[KnownPath]) -> dict[str, str]:
        return {
            kp.key: kp.path
            for kp in known_paths
            if kp.key and kp.path
        }

    @staticmethod
    def _slot_for_binding_key(binding_key: str) -> SlotDefinition | None:
        if binding_key == "reference_fasta":
            return SlotDefinition(
                "reference_fasta",
                "Reference FASTA",
                ["fa", "fasta", "fna", "fa.gz", "fasta.gz"],
                accepted_roles=["reference_fasta"],
            )
        if binding_key == "annotation_gtf":
            return SlotDefinition(
                "annotation_gtf",
                "Annotation GTF/GFF",
                ["gtf", "gff", "gff3", "gtf.gz", "gff.gz", "gff3.gz"],
                accepted_roles=["annotation_gtf"],
            )
        if binding_key in {"hisat2_index", "bwa_index", "bowtie2_index"}:
            return SlotDefinition(
                "index_prefix",
                "Aligner index prefix",
                ["*"],
                accepted_roles=[binding_key],
            )
        if binding_key == "star_genome_dir":
            return SlotDefinition(
                "genome_dir",
                "STAR genome directory",
                ["*"],
                accepted_roles=["star_genome_dir"],
            )
        return None

    @staticmethod
    def _summary_binding_keys(summary: AnalysisSummary) -> list[str]:
        binding_keys: list[str] = []
        for role in summary.required_resource_roles:
            if role == "reference_fasta":
                binding_keys.append("reference_fasta")
            elif role == "annotation_gtf":
                binding_keys.append("annotation_gtf")
            elif role == "spliced_aligner_index":
                binding_keys.extend(["hisat2_index", "star_genome_dir"])
            elif role == "aligner_index":
                binding_keys.extend(["bwa_index", "bowtie2_index", "hisat2_index", "star_genome_dir"])
        seen: set[str] = set()
        ordered: list[str] = []
        for binding_key in binding_keys:
            if binding_key in seen:
                continue
            seen.add(binding_key)
            ordered.append(binding_key)
        return ordered

    @staticmethod
    def _ambiguity_hint_for_candidates(binding_key: str, candidates: list[dict]) -> str | None:
        from tune.core.binding.semantic_retrieval import summarize_candidate_ambiguity

        summary = summarize_candidate_ambiguity(candidates)
        if not summary:
            return None
        return (
            f"{binding_key} has multiple close candidates: "
            f"{summary['primary_path']} ({summary.get('primary_source_type') or 'unknown'}, score={summary['primary_score']}) vs "
            f"{summary['secondary_path']} ({summary.get('secondary_source_type') or 'unknown'}, score={summary['secondary_score']})."
        )

    async def _build_resource_candidate_summaries(
        self,
        *,
        project_id: str,
        summary: AnalysisSummary,
        file_infos,
        samples,
        known_paths: list[KnownPath],
    ) -> tuple[list[dict], list[str]]:
        from tune.core.binding.semantic_retrieval import retrieve_semantic_candidates

        project_files = self._project_file_payloads(file_infos, samples)
        kp_bindings = self._known_path_bindings(known_paths)
        summaries: list[dict] = []
        ambiguity_hints: list[str] = []

        for binding_key in self._summary_binding_keys(summary):
            slot = self._slot_for_binding_key(binding_key)
            if slot is None:
                continue
            candidates = await retrieve_semantic_candidates(
                job_id="",
                dep_keys=[],
                slot=slot,
                project_id=project_id,
                project_files=project_files,
                kp_bindings=kp_bindings,
                db=self._session,
            )
            if not candidates:
                continue
            ambiguity_hint = self._ambiguity_hint_for_candidates(binding_key, candidates[:3])
            if ambiguity_hint:
                ambiguity_hints.append(ambiguity_hint)
            top = candidates[0]
            summaries.append(
                {
                    "binding_key": binding_key,
                    "slot_name": slot.name,
                    "path": top.get("file_path"),
                    "source_type": top.get("source_type"),
                    "score": top.get("score"),
                    "organism": top.get("organism"),
                    "genome_build": top.get("genome_build"),
                }
            )
        return summaries, ambiguity_hints[:4]

    @staticmethod
    def _memory_hints_from_memory_layers(
        profile: dict | None,
        *,
        memory_patterns: list[dict] | None = None,
        memory_preferences: list[dict] | None = None,
    ) -> list[str]:
        payload = dict(profile or {})
        preferences = dict(payload.get("preferences") or {})
        preference_entries = [
            item for item in (memory_preferences or [])
            if isinstance(item, dict)
        ]
        pattern_entries = [
            item for item in (memory_patterns or [])
            if isinstance(item, dict)
        ]
        hints: list[str] = []

        safe_action = None
        safe_action_basis = None
        for entry in preference_entries:
            if str(entry.get("preference_key") or "").strip() == "preferred_safe_action":
                safe_action = str(entry.get("value") or "").strip() or None
                safe_action_basis = str(entry.get("basis") or "").strip() or None
                break
        if not safe_action:
            safe_action = preferences.get("preferred_safe_action")
            safe_action_basis = preferences.get("preferred_safe_action_basis")
        if safe_action:
            suffix = f" (basis: {safe_action_basis})" if safe_action_basis else ""
            hints.append(
                f"Project memory prefers safe action '{safe_action}' when execution repair is needed{suffix}."
            )

        rollback_level = None
        rollback_basis = None
        for entry in preference_entries:
            if str(entry.get("preference_key") or "").strip() == "preferred_rollback_level":
                rollback_level = str(entry.get("value") or "").strip() or None
                rollback_basis = str(entry.get("basis") or "").strip() or None
                break
        if not rollback_level:
            rollback_level = preferences.get("preferred_rollback_level")
            rollback_basis = preferences.get("preferred_rollback_level_basis")
        if rollback_level:
            suffix = f" (basis: {rollback_basis})" if rollback_basis else ""
            hints.append(
                f"Project memory prefers rollback level '{rollback_level}' for recovery decisions{suffix}."
            )

        analysis_family = None
        for entry in preference_entries:
            if str(entry.get("preference_key") or "").strip() == "preferred_analysis_family":
                analysis_family = str(entry.get("value") or "").strip() or None
                break
        if not analysis_family:
            analysis_family = preferences.get("preferred_analysis_family")
        if analysis_family:
            hints.append(
                f"Project history is concentrated in analysis family '{analysis_family}'; prefer plans consistent with that family unless the user asks otherwise."
            )

        top_safe_action = next(
            (
                item
                for item in pattern_entries
                if str(item.get("pattern_type") or "").strip() == "safe_action"
            ),
            None,
        )
        if not top_safe_action:
            top_safe_action = next(iter(payload.get("safe_action_patterns") or []), None)
            safe_action_value = str((top_safe_action or {}).get("safe_action") or "").strip() or None
        else:
            safe_action_value = str(top_safe_action.get("recommended_value") or "").strip() or None
        if isinstance(top_safe_action, dict) and safe_action_value:
            incident_types = ", ".join(top_safe_action.get("incident_types") or [])
            if incident_types:
                hints.append(
                    f"Observed repair pattern: safe action '{safe_action_value}' often resolves incidents such as {incident_types}."
                )

        return hints[:4]

    @staticmethod
    def _stable_fact_summaries(facts: list[dict] | None) -> list[dict]:
        summaries: list[dict] = []
        for fact in facts or []:
            summaries.append(
                {
                    "fact_key": fact.get("fact_key"),
                    "fact_type": fact.get("fact_type"),
                    "title": fact.get("title"),
                    "statement": fact.get("statement"),
                    "binding_key": fact.get("binding_key"),
                    "path": fact.get("path"),
                    "file_id": fact.get("file_id"),
                    "experiment_id": fact.get("experiment_id"),
                    "read_number": fact.get("read_number"),
                }
            )
        return summaries[:6]

    @staticmethod
    def _semantic_query_text(project_info: ProjectPlannerInfo | None, summary: AnalysisSummary) -> str:
        parts: list[str] = []
        if project_info is not None and project_info.project_goal:
            parts.append(project_info.project_goal)
        if summary.suggested_analysis_type:
            parts.append(summary.suggested_analysis_type)
        if summary.analysis_family:
            parts.append(summary.analysis_family)
        if summary.library_strategies:
            parts.extend(summary.library_strategies[:2])
        if summary.organisms:
            parts.extend(summary.organisms[:2])
        return " ".join(part.strip() for part in parts if str(part or "").strip())

    @staticmethod
    async def _semantic_hint_summaries(
        *,
        ctx: PlannerContext,
        memory_profile: dict | None,
        stable_facts: list[dict] | None,
        recent_events,
        memory_patterns: list[dict] | None = None,
        memory_preferences: list[dict] | None = None,
    ) -> list[str]:
        from tune.core.semantic_document_service import (
            format_semantic_match_summary,
            query_semantic_documents,
        )
        from tune.core.semantic_documents import (
            documents_from_memory_facts,
            documents_from_project_events,
            documents_from_project_memory_profile,
            documents_from_structured_project_memory_layers,
            project_semantic_documents_from_context,
        )

        query_text = PlannerContextBuilder._semantic_query_text(ctx.project, ctx.summary)
        if not query_text:
            return []

        documents = list(project_semantic_documents_from_context(ctx))
        documents.extend(
            documents_from_memory_facts(stable_facts or [], project_id=ctx.project.id if ctx.project else "")
        )
        documents.extend(
            documents_from_project_events(recent_events or [], project_id=ctx.project.id if ctx.project else "")
        )
        documents.extend(
            documents_from_project_memory_profile(memory_profile or {}, project_id=ctx.project.id if ctx.project else "")
        )
        documents.extend(
            documents_from_structured_project_memory_layers(
                memory_patterns=memory_patterns or [],
                memory_preferences=memory_preferences or [],
                project_id=ctx.project.id if ctx.project else "",
            )
        )
        matches = await query_semantic_documents(documents, query_text, top_k=4)
        return [format_semantic_match_summary(match) for match in matches[:4]]

    async def _load_project_memory_bundle(
        self,
        project_id: str,
    ) -> tuple[list[dict], dict, list[dict], list[dict], list[dict]]:
        episodes = await query_recent_project_episodes(self._session, project_id, limit=20)
        profile = build_project_memory_profile(episodes)
        facts = await query_project_memory_facts(self._session, project_id, limit=20)
        patterns = await query_project_memory_patterns(self._session, project_id, limit=10)
        preferences = await query_project_memory_preferences(self._session, project_id, limit=10)
        return episodes, profile, facts, patterns, preferences

    # ------------------------------------------------------------------
    # Project-scope build
    # ------------------------------------------------------------------

    async def _build_project(self, scope: ContextScope) -> PlannerContext:
        project_state = await build_project_state(self._session, scope.project_id)
        if project_state is None:
            return self._empty_context()
        payload = project_state.model_dump(mode="json", exclude_none=True)
        return PlannerContext(
            context_mode="project",
            project_state=payload,
        )

    # ------------------------------------------------------------------
    # File-set-scope build
    # ------------------------------------------------------------------

    async def _build_file_set(self, scope: ContextScope) -> PlannerContext:
        file_ids = scope.file_ids or []
        file_map = await self._loader.load_files(file_ids, self._session)
        file_runs = await self._loader.load_file_runs_for_files(
            file_ids, self._session
        )

        # Collect unique samples and experiments from the file_runs
        seen_samples: dict[str, SamplePlannerInfo] = {}
        seen_experiments: dict[str, ExperimentPlannerInfo] = {}
        for fr in file_runs:
            exp = fr.experiment
            smp = exp.sample
            if smp.id not in seen_samples:
                seen_samples[smp.id] = SamplePlannerInfo(
                    id=smp.id,
                    sample_name=smp.sample_name,
                    organism=smp.organism,
                    attrs=smp.attrs or {},
                )
            if exp.id not in seen_experiments:
                seen_experiments[exp.id] = ExperimentPlannerInfo(
                    id=exp.id,
                    sample_id=smp.id,
                    library_strategy=exp.library_strategy,
                    library_layout=exp.library_layout,
                    platform=exp.platform,
                    instrument_model=exp.instrument_model,
                    file_ids=[
                        fr2.file_id
                        for fr2 in (exp.file_runs or [])
                        if fr2.file_id in file_map
                    ],
                )

        # Try to load project info if all files share one project
        project_info: ProjectPlannerInfo | None = None
        known_paths: list[KnownPath] = []
        project_ids = {f.project_id for f in file_map.values() if f.project_id}
        if len(project_ids) == 1:
            pid = next(iter(project_ids))
            project = await self._loader.load(pid, self._session)
            known_paths = await self._loader.load_known_paths(pid, self._session)
            project_file_map = await self._loader.load_files_for_project(
                pid, self._session
            )
            resource_entities = await self._load_project_resource_entities(
                pid,
                project_file_map,
                known_paths,
            )
            if project:
                project_info = ProjectPlannerInfo(
                    id=project.id,
                    name=project.name,
                    project_dir=project.project_dir,
                    project_goal=project.project_goal,
                    project_info=project.project_info or {},
                    known_paths=[
                        {
                            "key": kp.key,
                            "path": kp.path,
                            "description": kp.description,
                        }
                        for kp in known_paths
                    ],
                    resource_entities=self._serialize_resource_entities(resource_entities),
                )

        samples = list(seen_samples.values())
        experiments = list(seen_experiments.values())
        file_infos, file_to_sample, file_to_experiment = resolve_files_from_file_runs(
            file_runs, file_map
        )
        summary = build_summary(samples, experiments, file_infos, known_paths)
        ctx = PlannerContext(
            context_mode="file_set",
            project=project_info,
            samples=samples,
            experiments=experiments,
            files=file_infos,
            file_to_sample=file_to_sample,
            file_to_experiment=file_to_experiment,
            summary=summary,
        )
        if project_info is not None:
            (
                summary.resource_candidates,
                summary.ambiguity_hints,
            ) = await self._build_resource_candidate_summaries(
                project_id=project_info.id,
                summary=summary,
                file_infos=file_infos,
                samples=samples,
                known_paths=known_paths,
            )
            (
                recent_events,
                memory_profile,
                stable_facts,
                memory_patterns,
                memory_preferences,
            ) = await self._load_project_memory_bundle(project_info.id)
            summary.memory_hints = self._memory_hints_from_memory_layers(
                memory_profile,
                memory_patterns=memory_patterns,
                memory_preferences=memory_preferences,
            )
            summary.stable_facts = self._stable_fact_summaries(stable_facts)
            summary.semantic_hints = await self._semantic_hint_summaries(
                ctx=ctx,
                memory_profile=memory_profile,
                stable_facts=stable_facts,
                recent_events=recent_events,
                memory_patterns=memory_patterns,
                memory_preferences=memory_preferences,
            )
            ctx.semantic_memory_dossier = build_semantic_memory_dossier(
                summary,
                project_id=project_info.id,
                memory_patterns=memory_patterns,
                memory_preferences=memory_preferences,
            ) or {}

        return ctx

    # ------------------------------------------------------------------
    # Fallback
    # ------------------------------------------------------------------

    @staticmethod
    def _empty_context() -> PlannerContext:
        summary = AnalysisSummary(
            total_files=0,
            files_by_type={},
            sample_count=0,
            experiment_count=0,
            library_strategies=[],
            organisms=[],
            is_paired_end=None,
            has_reference_genome=False,
            files_without_samples=0,
            metadata_completeness="missing",
            suggested_analysis_type=None,
            analysis_family=None,
            required_resource_roles=[],
            planning_hints=[],
            potential_issues=["No project or files selected"],
            memory_hints=[],
            stable_facts=[],
            semantic_hints=[],
            ambiguity_hints=[],
        )
        return PlannerContext(
            context_mode="global",
            project=None,
            samples=[],
            experiments=[],
            files=[],
            file_to_sample={},
            file_to_experiment={},
            summary=summary,
        )
