"""Lightweight ProjectState projection built on demand from relational sources."""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict
import re
from types import SimpleNamespace
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import select

from tune.core.context.normalizer import build_summary
from tune.core.context.graph_loader import ProjectGraphLoader
from tune.core.context.resolver import resolve_files_from_project
from tune.core.models import AnalysisJob, ArtifactRecord

_GOAL_SECTION_TITLES = {
    "project goal",
    "goal",
    "项目目标",
}
_QUESTION_SECTION_TITLES = {
    "scientific question",
    "question",
    "current scientific question",
    "科学问题",
    "当前科学问题",
}


class ProjectState(BaseModel):
    project: dict[str, Any]
    project_brief_md: str = ""
    summary: dict[str, Any] = Field(default_factory=dict)
    planner_summary: dict[str, Any] = Field(default_factory=dict, exclude=True)
    samples: list[dict[str, Any]] | None = None
    experiments: list[dict[str, Any]] | None = None
    file_runs: list[dict[str, Any]] | None = None
    files: list[dict[str, Any]] | None = None
    lineage: dict[str, Any] = Field(default_factory=dict)


def parse_project_brief_md(markdown: str | None) -> dict[str, str | None]:
    raw_text = str(markdown or "").strip()
    if not raw_text:
        return {
            "project_brief_md": "",
            "project_goal": None,
            "scientific_question": None,
        }

    sections = _split_markdown_sections(raw_text)
    if not sections:
        return {
            "project_brief_md": raw_text,
            "project_goal": raw_text,
            "scientific_question": None,
        }

    project_goal = None
    scientific_question = None
    for title, body in sections:
        normalized = title.strip().lower()
        if normalized in _GOAL_SECTION_TITLES and not project_goal:
            project_goal = body
        elif normalized in _QUESTION_SECTION_TITLES and not scientific_question:
            scientific_question = body

    if not project_goal:
        project_goal = raw_text

    return {
        "project_brief_md": raw_text,
        "project_goal": project_goal or None,
        "scientific_question": scientific_question or None,
    }


def build_project_state_payload(
    *,
    project,
    samples: list[Any],
    experiments: list[Any],
    file_runs: list[Any],
    files: list[Any],
    known_paths: list[Any],
    resource_entities: list[Any],
    artifacts: list[Any],
) -> ProjectState:
    parsed_brief = parse_project_brief_md(getattr(project, "project_goal", None))

    file_map = {str(item.id): item for item in files if getattr(item, "id", None)}
    project_for_resolution = SimpleNamespace(samples=samples)
    file_infos, file_to_sample, file_to_experiment = resolve_files_from_project(
        project_for_resolution,
        file_map,
    )
    summary = build_summary(samples, experiments, file_infos, known_paths)
    sample_by_id = {str(sample.id): sample for sample in samples if getattr(sample, "id", None)}
    experiment_by_id = {
        str(experiment.id): experiment for experiment in experiments if getattr(experiment, "id", None)
    }
    file_info_by_id = {str(info.id): info for info in file_infos if getattr(info, "id", None)}
    file_run_by_file_id = {
        str(run.file_id): run for run in file_runs if getattr(run, "file_id", None)
    }

    def _sample_context(attrs: dict[str, Any]) -> dict[str, Any]:
        keys = (
            "description",
            "sample_title",
            "tissue",
            "treatment",
            "cultivar",
            "dev_stage",
            "replicate",
            "genotype",
            "sex",
            "age",
            "package",
        )
        return {
            key: attrs.get(key)
            for key in keys
            if attrs.get(key) not in (None, "", [], {})
        }

    sample_payloads = []
    for sample in samples:
        attrs = dict(sample.attrs or {})
        sample_payloads.append(
            {
                "id": sample.id,
                "sample_name": sample.sample_name,
                "organism": sample.organism,
                "description": attrs.get("description"),
                "sample_title": attrs.get("sample_title"),
                "biological_context": _sample_context(attrs),
                "attrs": attrs,
            }
        )

    experiment_payloads = []
    for experiment in experiments:
        attrs = dict(experiment.attrs or {})
        sample = sample_by_id.get(str(experiment.sample_id))
        experiment_runs = [
            run for run in file_runs if getattr(run, "experiment_id", None) == experiment.id
        ]
        experiment_payloads.append(
            {
                "id": experiment.id,
                "sample_id": experiment.sample_id,
                "sample_name": getattr(sample, "sample_name", None),
                "organism": getattr(sample, "organism", None),
                "library_strategy": experiment.library_strategy,
                "library_source": getattr(experiment, "library_source", None),
                "library_selection": getattr(experiment, "library_selection", None),
                "library_layout": experiment.library_layout,
                "platform": experiment.platform,
                "instrument_model": experiment.instrument_model,
                "design_description": attrs.get("design_description"),
                "library_name": attrs.get("library_name"),
                "read_length": attrs.get("read_length"),
                "file_ids": [run.file_id for run in experiment_runs],
                "file_run_ids": [run.id for run in experiment_runs],
                "attrs": attrs,
            }
        )

    file_run_payloads = []
    for run in file_runs:
        experiment = experiment_by_id.get(str(run.experiment_id))
        sample = sample_by_id.get(str(getattr(experiment, "sample_id", "")))
        info = file_info_by_id.get(str(run.file_id))
        file_obj = file_map.get(str(run.file_id))
        file_run_payloads.append(
            {
                "id": run.id,
                "experiment_id": run.experiment_id,
                "sample_id": getattr(experiment, "sample_id", None),
                "sample_name": getattr(sample, "sample_name", None),
                "library_strategy": getattr(experiment, "library_strategy", None),
                "library_layout": getattr(experiment, "library_layout", None),
                "file_id": run.file_id,
                "file_path": getattr(info, "path", None),
                "file_type": getattr(info, "file_type", None),
                "size_bytes": getattr(file_obj, "size_bytes", None),
                "md5": getattr(file_obj, "md5", None),
                "read_number": run.read_number,
                "filename": run.filename,
                "attrs": dict(run.attrs or {}),
            }
        )

    file_payloads = []
    for info in file_infos:
        file_obj = file_map.get(str(info.id))
        sample = sample_by_id.get(str(info.linked_sample_id or ""))
        experiment = experiment_by_id.get(str(info.linked_experiment_id or ""))
        file_payloads.append(
            {
                "id": info.id,
                "path": info.path,
                "filename": info.filename,
                "file_type": info.file_type,
                "size_bytes": getattr(file_obj, "size_bytes", None),
                "md5": getattr(file_obj, "md5", None),
                "read_number": info.read_number,
                "linked_sample_id": info.linked_sample_id,
                "linked_sample_name": getattr(sample, "sample_name", None),
                "linked_experiment_id": info.linked_experiment_id,
                "linked_library_strategy": getattr(experiment, "library_strategy", None),
                "intrinsic": dict(info.intrinsic or {}),
            }
        )

    known_path_keys = sorted(
        {
            str(path.key).strip()
            for path in known_paths
            if str(getattr(path, "key", "") or "").strip()
        }
    )
    resource_role_counts = Counter(
        str(getattr(entity, "resource_role", "") or "").strip()
        for entity in resource_entities
        if str(getattr(entity, "resource_role", "") or "").strip()
    )
    available_index_aligners = sorted(
        {
            str((getattr(entity, "metadata_json", {}) or {}).get("aligner") or "").strip()
            for entity in resource_entities
            if str(getattr(entity, "resource_role", "") or "").strip() == "aligner_index"
            and str((getattr(entity, "metadata_json", {}) or {}).get("aligner") or "").strip()
        }
    )
    summary_payload = asdict(summary)
    summary_payload.update(
        {
            "known_path_keys": known_path_keys,
            "resource_role_counts": dict(resource_role_counts),
            "available_index_aligners": available_index_aligners,
        }
    )
    public_summary = {
        "total_files": summary_payload["total_files"],
        "files_by_type": summary_payload["files_by_type"],
        "sample_count": summary_payload["sample_count"],
        "experiment_count": summary_payload["experiment_count"],
        "library_strategies": summary_payload["library_strategies"],
        "organisms": summary_payload["organisms"],
        "is_paired_end": summary_payload["is_paired_end"],
    }

    lineage_edges = []
    for info in file_infos:
        sample_id = file_to_sample.get(info.id)
        experiment_id = file_to_experiment.get(info.id)
        sample = sample_by_id.get(str(sample_id or ""))
        experiment = experiment_by_id.get(str(experiment_id or ""))
        run = file_run_by_file_id.get(str(info.id))
        lineage_edges.append(
            {
                "sample_id": sample_id,
                "sample_name": getattr(sample, "sample_name", None),
                "experiment_id": experiment_id,
                "library_strategy": getattr(experiment, "library_strategy", None),
                "file_run_id": getattr(run, "id", None),
                "file_id": info.id,
                "filename": info.filename,
                "path": info.path,
                "file_type": info.file_type,
                "read_number": info.read_number,
            }
        )
    linked_file_count = sum(1 for edge in lineage_edges if edge["sample_id"] or edge["experiment_id"])
    linked_groups = []
    for experiment_payload in experiment_payloads:
        sample_payload = next(
            (
                item
                for item in sample_payloads
                if item.get("id") == experiment_payload.get("sample_id")
            ),
            None,
        )
        runs = [
            item
            for item in file_run_payloads
            if item.get("experiment_id") == experiment_payload.get("id")
        ]
        runs.sort(
            key=lambda item: (
                item.get("read_number") if item.get("read_number") is not None else 99,
                str(item.get("filename") or ""),
            )
        )
        linked_groups.append(
            {
                "sample": {
                    "id": sample_payload.get("id") if sample_payload else None,
                    "sample_name": sample_payload.get("sample_name") if sample_payload else None,
                    "organism": sample_payload.get("organism") if sample_payload else None,
                    "description": sample_payload.get("description") if sample_payload else None,
                    "sample_title": sample_payload.get("sample_title") if sample_payload else None,
                    "biological_context": (
                        sample_payload.get("biological_context") if sample_payload else {}
                    ),
                },
                "experiment": {
                    "id": experiment_payload.get("id"),
                    "library_strategy": experiment_payload.get("library_strategy"),
                    "library_source": experiment_payload.get("library_source"),
                    "library_selection": experiment_payload.get("library_selection"),
                    "library_layout": experiment_payload.get("library_layout"),
                    "platform": experiment_payload.get("platform"),
                    "instrument_model": experiment_payload.get("instrument_model"),
                    "design_description": experiment_payload.get("design_description"),
                    "library_name": experiment_payload.get("library_name"),
                    "read_length": experiment_payload.get("read_length"),
                },
                "files": [
                    {
                        "file_run_id": run.get("id"),
                        "file_id": run.get("file_id"),
                        "filename": run.get("filename"),
                        "path": run.get("file_path"),
                        "file_type": run.get("file_type"),
                        "read_number": run.get("read_number"),
                        "size_bytes": run.get("size_bytes"),
                    }
                    for run in runs
                ],
            }
        )
    lineage_payload = {
        "linked_groups": linked_groups,
        "summary": {
            "sample_count": len(sample_payloads),
            "experiment_count": len(experiment_payloads),
            "file_run_count": len(file_run_payloads),
            "file_count": len(file_payloads),
            "linked_file_count": linked_file_count,
        },
    }
    if not linked_groups:
        lineage_payload["edges"] = lineage_edges
    include_top_level_entities = not linked_groups

    return ProjectState(
        project={
            "id": project.id,
            "name": project.name,
            "project_dir": project.project_dir,
            "description": project.description,
            "project_info": dict(project.project_info or {}),
        },
        project_brief_md=str(parsed_brief["project_brief_md"] or ""),
        summary=public_summary,
        planner_summary=summary_payload,
        samples=sample_payloads if include_top_level_entities else None,
        experiments=experiment_payloads if include_top_level_entities else None,
        file_runs=file_run_payloads if include_top_level_entities else None,
        files=file_payloads if include_top_level_entities else None,
        lineage=lineage_payload,
    )


async def build_project_state(
    session,
    project_id: str,
    *,
    artifact_limit: int = 50,
) -> ProjectState | None:
    loader = ProjectGraphLoader()
    project = await loader.load(project_id, session)
    if project is None:
        return None

    file_map = await loader.load_files_for_project(project_id, session)
    known_paths = await loader.load_known_paths(project_id, session)
    resource_entities = await loader.load_resource_entities(project_id, session)

    samples = list(project.samples or [])
    experiments = [
        experiment
        for sample in samples
        for experiment in (sample.experiments or [])
    ]
    file_runs = [
        run
        for experiment in experiments
        for run in (experiment.file_runs or [])
    ]

    artifacts = (
        await session.execute(
            select(ArtifactRecord)
            .join(AnalysisJob, AnalysisJob.id == ArtifactRecord.job_id)
            .where(AnalysisJob.project_id == project_id)
            .order_by(ArtifactRecord.created_at.desc())
            .limit(artifact_limit)
        )
    ).scalars().all()

    return build_project_state_payload(
        project=project,
        samples=samples,
        experiments=experiments,
        file_runs=file_runs,
        files=list(file_map.values()),
        known_paths=known_paths,
        resource_entities=resource_entities,
        artifacts=list(artifacts),
    )


def _split_markdown_sections(markdown: str) -> list[tuple[str, str]]:
    sections: list[tuple[str, str]] = []
    current_title: str | None = None
    current_lines: list[str] = []

    for raw_line in markdown.splitlines():
        line = raw_line.rstrip()
        match = re.match(r"^\s{0,3}#{1,6}\s+(.*\S)\s*$", line)
        if match:
            if current_title is not None:
                sections.append((current_title, "\n".join(current_lines).strip()))
            current_title = match.group(1).strip()
            current_lines = []
            continue
        if current_title is not None:
            current_lines.append(line)

    if current_title is not None:
        sections.append((current_title, "\n".join(current_lines).strip()))
    return sections
