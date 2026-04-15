"""Projects API routes — full CRUD + file assignment."""
from __future__ import annotations

import itertools
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from tune.core.config import get_config
from tune.core.database import get_session
from tune.core.models import Conversation, EnhancedMetadata, File, Project, ResourceEntity, ResourceFile, Thread, ThreadMessage, Sample, Experiment, FileRun
from tune.core.project_state import build_project_state
from tune.core.resources.entities import sync_project_resource_entities_by_id

router = APIRouter()

_PROJECT_DIR_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]*$")


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class ProjectCreate(BaseModel):
    name: str
    project_dir: str
    description: str | None = None
    dir_path: str | None = None


class ProjectUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    dir_path: str | None = None
    project_info: dict | None = None
    schema_extensions: dict | None = None
    project_goal: str | None = None


class AssignFilesRequest(BaseModel):
    file_ids: list[str]


# ---------------------------------------------------------------------------
# Resource entity sync response
# ---------------------------------------------------------------------------


class ResourceEntitySyncResponse(BaseModel):
    project_id: str
    project_name: str | None = None
    file_count: int
    known_path_count: int
    changes: int
    resource_entity_count: int


class ResourceEntityListItem(BaseModel):
    id: str
    resource_role: str
    display_name: str
    organism: str | None = None
    genome_build: str | None = None
    status: str | None = None
    source_type: str | None = None
    source_uri: str | None = None
    metadata_json: dict[str, Any] | None = None
    components: list[dict]


class ResourceEntityDecisionRequest(BaseModel):
    known_path_key: str
    decision: Literal["keep_registered"]
    recognized_path: str | None = None
    registered_path: str | None = None


# ---------------------------------------------------------------------------
# File preview helpers
# ---------------------------------------------------------------------------

BINARY_EXTENSIONS: frozenset[str] = frozenset({
    # Compression / archives
    ".gz", ".bz2", ".xz", ".zip", ".tar", ".7z", ".rar", ".zst",
    # Bioinformatics binary formats
    ".bam", ".cram", ".bcf", ".sra", ".hdf5", ".h5", ".hdf",
    # Genomics index / sorted outputs
    ".bai", ".csi", ".tbi", ".fai",
    # Sequence databases
    ".db", ".sqlite", ".sqlite3",
    # Image formats
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".tif", ".ico", ".webp", ".svg",
    # Document / office
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    # Data / columnar formats
    ".parquet", ".orc", ".feather", ".avro", ".npy", ".npz", ".pkl", ".pickle",
    # Compiled / binary executables
    ".so", ".dylib", ".dll", ".exe", ".pyc", ".pyo",
})

_PREVIEW_LINES = 200
_BINARY_SCAN_BYTES = 8192


def _is_binary(path: str) -> bool:
    """Return True if the file appears to be binary.

    Stage 1: fast extension check against BINARY_EXTENSIONS.
    Stage 2: scan first 8 KB for null bytes.
    """
    ext = os.path.splitext(path)[1].lower()
    if ext in BINARY_EXTENSIONS:
        return True
    try:
        with open(path, "rb") as fh:
            chunk = fh.read(_BINARY_SCAN_BYTES)
        return b"\x00" in chunk
    except OSError:
        return False


class FilePreviewResponse(BaseModel):
    success: bool
    file_name: str
    file_path: str
    file_type: str | None = None
    file_size: int | None = None
    preview_type: str  # "text" | "unsupported"
    content: str | None = None
    line_count: int | None = None
    shown_line_count: int | None = None
    truncated: bool | None = None
    message: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _validate_project_dir(value: str) -> None:
    """Raise 422 if project_dir does not match the naming rule."""
    if not _PROJECT_DIR_RE.match(value):
        raise HTTPException(
            422,
            "project_dir must start with a letter or digit and contain only "
            "letters, digits, hyphens, and underscores (no spaces or special characters)",
        )


def _resource_entity_dict(entity: ResourceEntity) -> dict:
    return {
        "id": entity.id,
        "resource_role": entity.resource_role,
        "display_name": entity.display_name,
        "organism": entity.organism,
        "genome_build": entity.genome_build,
        "status": entity.status,
        "source_type": entity.source_type,
        "source_uri": entity.source_uri,
        "metadata_json": entity.metadata_json or {},
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


async def _project_summary(session: AsyncSession, proj: Project) -> dict:
    """Return project dict with file count and metadata completeness summary."""
    from tune.core.metadata.schemas import all_required_fields
    from tune.core.metadata.manager import score_completeness, score_project_metadata_health
    from sqlalchemy.orm import selectinload

    files = (
        await session.execute(
            select(File)
            .options(selectinload(File.enhanced_metadata))
            .where(File.project_id == proj.id)
        )
    ).scalars().all()

    total = len(files)
    complete = partial = missing = 0
    for f in files:
        required = all_required_fields(f.file_type)
        s = score_completeness(f, required)
        if s == "complete":
            complete += 1
        elif s == "partial":
            partial += 1
        else:
            missing += 1

    health = await score_project_metadata_health(proj.id, session)

    # Ensure schema_extensions is in nested form
    se = proj.schema_extensions or {}
    if se and "sample_fields" not in se:
        # Legacy flat structure — wrap it
        se = {"project_fields": {}, "sample_fields": se, "experiment_fields": {}}
    elif not se:
        se = {"project_fields": {}, "sample_fields": {}, "experiment_fields": {}}

    # Compute directory paths (only use explicitly set fields, never fall back to name)
    try:
        cfg = get_config()
        data_path = str(cfg.data_dir / proj.dir_path) if proj.dir_path else None
        analysis_path = str(cfg.analysis_dir / proj.project_dir) if proj.project_dir else None
    except Exception:
        data_path = None
        analysis_path = None

    resource_entities = (
        await session.execute(
            select(ResourceEntity.resource_role).where(ResourceEntity.project_id == proj.id)
        )
    ).scalars().all()
    resource_summary = {
        "total": len(resource_entities),
        "reference_count": sum(1 for role in resource_entities if role in {"reference", "reference_bundle", "reference_fasta"}),
        "annotation_count": sum(1 for role in resource_entities if role in {"annotation", "annotation_bundle", "annotation_gtf"}),
        "index_count": sum(1 for role in resource_entities if role == "aligner_index"),
    }

    return {
        "id": proj.id,
        "name": proj.name,
        "project_dir": proj.project_dir,
        "description": proj.description,
        "dir_path": proj.dir_path,
        "inferred": proj.inferred,
        "created_at": proj.created_at,
        "file_count": total,
        "metadata_complete": complete,
        "metadata_partial": partial,
        "metadata_missing": missing,
        "project_info": proj.project_info or {},
        "schema_extensions": se,
        "project_goal": proj.project_goal,
        "health": health,
        "data_path": data_path,
        "analysis_path": analysis_path,
        "resource_entity_summary": resource_summary,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/")
async def list_projects(session: AsyncSession = Depends(get_session)):
    projects = (await session.execute(select(Project).order_by(Project.name))).scalars().all()
    return [await _project_summary(session, p) for p in projects]


@router.get("/stats")
async def project_stats(session: AsyncSession = Depends(get_session)):
    """Return aggregate counts for the global data overview."""
    from sqlalchemy import func
    from tune.core.models import Sample, Experiment, File as FileModel

    project_count = (await session.execute(select(func.count()).select_from(Project))).scalar() or 0
    sample_count = (await session.execute(select(func.count()).select_from(Sample))).scalar() or 0
    experiment_count = (await session.execute(select(func.count()).select_from(Experiment))).scalar() or 0
    fastq_count = (
        await session.execute(
            select(func.count()).select_from(FileModel).where(FileModel.file_type == "fastq")
        )
    ).scalar() or 0
    return {
        "project_count": project_count,
        "sample_count": sample_count,
        "experiment_count": experiment_count,
        "fastq_count": fastq_count,
    }


@router.get("/{project_id}/health")
async def project_health(project_id: str, session: AsyncSession = Depends(get_session)):
    from tune.core.metadata.manager import score_project_metadata_health
    proj = (
        await session.execute(select(Project).where(Project.id == project_id))
    ).scalar_one_or_none()
    if not proj:
        raise HTTPException(404, "Project not found")
    return await score_project_metadata_health(project_id, session)


@router.get("/{project_id}/files/preview")
async def project_file_preview(
    project_id: str,
    path: str = Query(..., description="Absolute path to the file to preview"),
    session: AsyncSession = Depends(get_session),
) -> FilePreviewResponse:
    """Return up to 200 lines of a text file, or an unsupported marker for binary files.

    Security: path is validated against the project's data_path and analysis_path via
    os.path.realpath() to prevent path traversal attacks.
    """
    proj = (
        await session.execute(select(Project).where(Project.id == project_id))
    ).scalar_one_or_none()
    if not proj:
        raise HTTPException(404, "Project not found")

    try:
        cfg = get_config()
    except Exception:
        raise HTTPException(500, "Server config not available")

    # Resolve the allowed roots for this project
    allowed_roots: list[str] = []
    if proj.dir_path:
        allowed_roots.append(os.path.realpath(str(cfg.data_dir / proj.dir_path)))
    if proj.project_dir:
        allowed_roots.append(os.path.realpath(str(cfg.analysis_dir / proj.project_dir)))

    # Resolve requested path and validate it is within an allowed root
    if not allowed_roots:
        raise HTTPException(403, "Project has no configured directories")
    real_path = os.path.realpath(path)
    if not any(
        real_path == root or real_path.startswith(root + os.sep)
        for root in allowed_roots
    ):
        raise HTTPException(403, "Path is outside the project's allowed directories")

    if not os.path.isfile(real_path):
        raise HTTPException(404, "File not found")

    file_name = os.path.basename(real_path)
    ext = os.path.splitext(file_name)[1].lower()
    file_size = os.path.getsize(real_path)

    # Binary branch
    if _is_binary(real_path):
        label = ext.lstrip(".").upper() if ext else "binary"
        return FilePreviewResponse(
            success=True,
            file_name=file_name,
            file_path=real_path,
            file_type=ext or None,
            file_size=file_size,
            preview_type="unsupported",
            message=f"This file appears to be a binary file ({label}) and cannot be previewed as text.",
        )

    # Text branch: read up to _PREVIEW_LINES lines
    try:
        with open(real_path, encoding="utf-8", errors="replace") as fh:
            head_lines = list(itertools.islice(fh, _PREVIEW_LINES))
            # Count remaining lines for total
            remaining = sum(1 for _ in fh)
    except OSError as exc:
        raise HTTPException(500, f"Failed to read file: {exc}") from exc

    shown = len(head_lines)
    total = shown + remaining
    truncated = remaining > 0

    return FilePreviewResponse(
        success=True,
        file_name=file_name,
        file_path=real_path,
        file_type=ext or None,
        file_size=file_size,
        preview_type="text",
        content="".join(head_lines),
        line_count=total,
        shown_line_count=shown,
        truncated=truncated,
    )


@router.post("/", status_code=201)
async def create_project(body: ProjectCreate, session: AsyncSession = Depends(get_session)):
    _validate_project_dir(body.project_dir)

    existing_name = (
        await session.execute(select(Project).where(Project.name == body.name))
    ).scalar_one_or_none()
    if existing_name:
        raise HTTPException(409, f"Project with name '{body.name}' already exists")

    existing_dir = (
        await session.execute(select(Project).where(Project.project_dir == body.project_dir))
    ).scalar_one_or_none()
    if existing_dir:
        raise HTTPException(409, f"project_dir '{body.project_dir}' is already in use")

    proj = Project(
        id=str(uuid.uuid4()),
        name=body.name,
        project_dir=body.project_dir,
        description=body.description,
        dir_path=body.dir_path,
        inferred=False,
        schema_extensions={"project_fields": {}, "sample_fields": {}, "experiment_fields": {}},
    )
    session.add(proj)
    await session.commit()

    # Create analysis_dir/{project_dir}/ on disk
    try:
        cfg = get_config()
        analysis_subdir = cfg.analysis_dir / body.project_dir
        os.makedirs(analysis_subdir, exist_ok=True)
    except Exception:
        pass  # non-fatal if directory creation fails (e.g. read-only filesystem in tests)

    return await _project_summary(session, proj)


@router.get("/{project_id}/resource-entities", response_model=list[ResourceEntityListItem])
async def list_project_resource_entities(
    project_id: str,
    session: AsyncSession = Depends(get_session),
):
    proj = (
        await session.execute(select(Project).where(Project.id == project_id))
    ).scalar_one_or_none()
    if not proj:
        raise HTTPException(404, "Project not found")

    entities = (
        await session.execute(
            select(ResourceEntity)
            .where(ResourceEntity.project_id == project_id)
            .options(selectinload(ResourceEntity.resource_files).selectinload(ResourceFile.file))
            .order_by(ResourceEntity.resource_role, ResourceEntity.display_name, ResourceEntity.id)
        )
    ).scalars().all()
    return [_resource_entity_dict(entity) for entity in entities]


@router.post("/{project_id}/resource-entities/sync", response_model=ResourceEntitySyncResponse)
async def sync_project_resource_entities_endpoint(
    project_id: str,
    session: AsyncSession = Depends(get_session),
):
    try:
        result = await sync_project_resource_entities_by_id(session, project_id)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc
    return result


@router.post(
    "/{project_id}/resource-entities/{entity_id}/decision",
    response_model=ResourceEntityListItem,
)
async def persist_project_resource_entity_decision(
    project_id: str,
    entity_id: str,
    body: ResourceEntityDecisionRequest,
    session: AsyncSession = Depends(get_session),
):
    entity = (
        await session.execute(
            select(ResourceEntity)
            .where(
                ResourceEntity.id == entity_id,
                ResourceEntity.project_id == project_id,
            )
            .options(selectinload(ResourceEntity.resource_files).selectinload(ResourceFile.file))
        )
    ).scalar_one_or_none()
    if not entity:
        raise HTTPException(404, "Resource entity not found")

    metadata = dict(entity.metadata_json or {})
    known_path_decisions = dict(metadata.get("known_path_decisions") or {})
    known_path_decisions[body.known_path_key] = {
        "decision": body.decision,
        "recognized_path": body.recognized_path,
        "registered_path": body.registered_path,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    metadata["known_path_decisions"] = known_path_decisions
    entity.metadata_json = metadata

    await session.commit()
    await session.refresh(entity)
    return _resource_entity_dict(entity)


@router.get("/{project_id}")
async def get_project(project_id: str, session: AsyncSession = Depends(get_session)):
    proj = (
        await session.execute(select(Project).where(Project.id == project_id))
    ).scalar_one_or_none()
    if not proj:
        raise HTTPException(404, "Project not found")
    return await _project_summary(session, proj)


@router.get("/{project_id}/state")
async def get_project_state(project_id: str, session: AsyncSession = Depends(get_session)):
    state = await build_project_state(session, project_id)
    if state is None:
        raise HTTPException(404, "Project not found")
    return state.model_dump(mode="json", exclude_none=True)


@router.patch("/{project_id}")
async def update_project(
    project_id: str,
    body: ProjectUpdate,
    session: AsyncSession = Depends(get_session),
):
    proj = (
        await session.execute(select(Project).where(Project.id == project_id))
    ).scalar_one_or_none()
    if not proj:
        raise HTTPException(404, "Project not found")

    if body.name is not None:
        existing = (
            await session.execute(
                select(Project).where(Project.name == body.name, Project.id != project_id)
            )
        ).scalar_one_or_none()
        if existing:
            raise HTTPException(409, f"Project with name '{body.name}' already exists")
        proj.name = body.name
    if body.description is not None:
        proj.description = body.description
    # dir_path can be set to None (clear) or a new string
    if "dir_path" in body.model_fields_set:
        proj.dir_path = body.dir_path
    if body.project_info is not None:
        proj.project_info = body.project_info
    if body.schema_extensions is not None:
        proj.schema_extensions = body.schema_extensions
    if "project_goal" in body.model_fields_set:
        proj.project_goal = body.project_goal

    await session.commit()
    await session.refresh(proj)
    return await _project_summary(session, proj)


@router.delete("/{project_id}", status_code=200)
async def delete_project(project_id: str, session: AsyncSession = Depends(get_session)):
    proj = (
        await session.execute(select(Project).where(Project.id == project_id))
    ).scalar_one_or_none()
    if not proj:
        raise HTTPException(404, "Project not found")

    from sqlalchemy import delete as sa_delete

    # Count files and metadata before deletion for the response summary
    file_ids_result = await session.execute(
        select(File.id).where(File.project_id == project_id)
    )
    file_ids = [row[0] for row in file_ids_result]
    deleted_files = len(file_ids)

    # Delete EnhancedMetadata for all files in this project
    deleted_metadata = 0
    if file_ids:
        meta_result = await session.execute(
            sa_delete(EnhancedMetadata).where(EnhancedMetadata.file_id.in_(file_ids))
        )
        deleted_metadata = meta_result.rowcount

        # Delete the File records
        await session.execute(
            sa_delete(File).where(File.id.in_(file_ids))
        )

    # Delete Thread records (and their messages) for this project
    thread_ids_result = await session.execute(
        select(Thread.id).where(Thread.project_id == project_id)
    )
    thread_ids = [row[0] for row in thread_ids_result]
    deleted_threads = len(thread_ids)
    if thread_ids:
        await session.execute(
            sa_delete(ThreadMessage).where(ThreadMessage.thread_id.in_(thread_ids))
        )
        await session.execute(
            sa_delete(Thread).where(Thread.id.in_(thread_ids))
        )

    # Delete SRA three-tier metadata: FileRun → Experiment → Sample (CASCADE handles children)
    sample_ids_result = await session.execute(
        select(Sample.id).where(Sample.project_id == project_id)
    )
    sample_ids = [row[0] for row in sample_ids_result]
    if sample_ids:
        exp_ids_result = await session.execute(
            select(Experiment.id).where(Experiment.sample_id.in_(sample_ids))
        )
        exp_ids = [row[0] for row in exp_ids_result]
        if exp_ids:
            await session.execute(
                sa_delete(FileRun).where(FileRun.experiment_id.in_(exp_ids))
            )
            await session.execute(
                sa_delete(Experiment).where(Experiment.id.in_(exp_ids))
            )
        await session.execute(
            sa_delete(Sample).where(Sample.id.in_(sample_ids))
        )

    # Null out project_id on legacy Conversation records so they are preserved
    await session.execute(
        update(Conversation).where(Conversation.project_id == project_id).values(project_id=None)
    )

    await session.delete(proj)
    await session.commit()

    return {"deleted_files": deleted_files, "deleted_metadata": deleted_metadata, "deleted_threads": deleted_threads}


@router.post("/{project_id}/assign-files")
async def assign_files(
    project_id: str,
    body: AssignFilesRequest,
    session: AsyncSession = Depends(get_session),
):
    proj = (
        await session.execute(select(Project).where(Project.id == project_id))
    ).scalar_one_or_none()
    if not proj:
        raise HTTPException(404, "Project not found")

    if body.file_ids:
        await session.execute(
            update(File)
            .where(File.id.in_(body.file_ids))
            .values(project_id=project_id)
        )
    await session.commit()
    return {"ok": True, "assigned": len(body.file_ids)}


@router.post("/{project_id}/unassign-files")
async def unassign_files(
    project_id: str,
    body: AssignFilesRequest,
    session: AsyncSession = Depends(get_session),
):
    """Set project_id = NULL for the given file IDs."""
    if body.file_ids:
        await session.execute(
            update(File)
            .where(File.id.in_(body.file_ids), File.project_id == project_id)
            .values(project_id=None)
        )
    await session.commit()
    return {"ok": True, "unassigned": len(body.file_ids)}


# ---------------------------------------------------------------------------
# Directory tree endpoint
# ---------------------------------------------------------------------------

_MAX_DEPTH = 5
_MAX_NODES_PER_DIR = 500


def _build_tree(root: str, max_depth: int = _MAX_DEPTH) -> list[dict]:
    """Walk root directory and return a nested tree (list of nodes)."""
    import os
    from pathlib import Path

    def _walk(path: str, depth: int) -> list[dict]:
        if depth > max_depth:
            return []
        try:
            entries = sorted(os.scandir(path), key=lambda e: (not e.is_dir(), e.name))
        except PermissionError:
            return []
        nodes: list[dict] = []
        for i, entry in enumerate(entries):
            if i >= _MAX_NODES_PER_DIR:
                break
            if entry.is_dir(follow_symlinks=False):
                nodes.append({
                    "name": entry.name,
                    "path": entry.path,
                    "type": "dir",
                    "children": _walk(entry.path, depth + 1),
                })
            else:
                try:
                    stat = entry.stat()
                    nodes.append({
                        "name": entry.name,
                        "path": entry.path,
                        "type": "file",
                        "size_bytes": stat.st_size,
                        "mtime": stat.st_mtime,
                    })
                except OSError:
                    pass
        return nodes

    return _walk(root, 1)


@router.get("/{project_id}/files/tree")
async def project_files_tree(
    project_id: str,
    source: str = "data",
    session: AsyncSession = Depends(get_session),
):
    """Return recursive directory tree for data_dir or analysis_dir."""
    proj = (
        await session.execute(select(Project).where(Project.id == project_id))
    ).scalar_one_or_none()
    if not proj:
        raise HTTPException(404, "Project not found")

    try:
        cfg = get_config()
    except Exception:
        raise HTTPException(500, "Server config not available")

    if source == "data":
        if not proj.dir_path:
            return {"exists": False, "root": None, "read_only": True, "children": []}
        root_path = str(cfg.data_dir / proj.dir_path)
        read_only = True
    elif source == "analysis":
        if not proj.project_dir:
            return {"exists": False, "root": None, "read_only": False, "children": []}
        root_path = str(cfg.analysis_dir / proj.project_dir)
        read_only = False
    else:
        raise HTTPException(422, "source must be 'data' or 'analysis'")

    if not os.path.isdir(root_path):
        return {"exists": False, "root": root_path, "read_only": read_only, "children": []}

    # Enrich file nodes with db_id if the file exists in DB
    import os as _os
    from pathlib import Path as _Path

    tree = _build_tree(root_path)

    # Build path→id map for files in this project
    db_files_result = await session.execute(
        select(File.path, File.id).where(File.project_id == project_id)
    )
    path_to_id: dict[str, str] = {row[0]: row[1] for row in db_files_result}

    def _enrich(nodes: list[dict]) -> None:
        for node in nodes:
            if node["type"] == "file":
                node["db_id"] = path_to_id.get(node["path"])
            elif node["type"] == "dir":
                _enrich(node.get("children", []))

    _enrich(tree)

    return {"exists": True, "root": root_path, "read_only": read_only, "children": tree}


# ---------------------------------------------------------------------------
# Lineage endpoint
# ---------------------------------------------------------------------------


@router.get("/{project_id}/lineage")
async def project_lineage(
    project_id: str,
    session: AsyncSession = Depends(get_session),
):
    """Return lineage graph for a project: Sample→Experiment→FileRun→File→Job→Step→Result."""
    import os
    from itertools import groupby
    from tune.core.models import AnalysisJob

    proj = (
        await session.execute(select(Project).where(Project.id == project_id))
    ).scalar_one_or_none()
    if not proj:
        raise HTTPException(404, "Project not found")

    nodes: list[dict] = []
    seen_node_ids: set[str] = set()
    edge_set: set[tuple[str, str]] = set()
    edges: list[dict] = []

    def add_node(node: dict) -> None:
        if node["id"] not in seen_node_ids:
            seen_node_ids.add(node["id"])
            nodes.append(node)

    def add_edge(source: str, target: str) -> None:
        if (source, target) not in edge_set:
            edge_set.add((source, target))
            edges.append({"source": source, "target": target})

    # ── SRA side: Sample → Experiment → File ──────────────────────────────────

    samples = (
        await session.execute(select(Sample).where(Sample.project_id == project_id))
    ).scalars().all()
    for s in samples:
        add_node({"id": f"sample-{s.id}", "type": "sample", "label": s.sample_name,
                  "attrs": {"organism": s.organism}})

    sample_ids = [s.id for s in samples]
    experiments: list[Experiment] = []
    if sample_ids:
        experiments = (
            await session.execute(select(Experiment).where(Experiment.sample_id.in_(sample_ids)))
        ).scalars().all()
    for exp in experiments:
        label = (exp.attrs or {}).get("library_name") or exp.library_strategy or "Experiment"
        add_node({"id": f"exp-{exp.id}", "type": "experiment", "label": label,
                  "attrs": {"library_strategy": exp.library_strategy, "platform": exp.platform}})
        if exp.sample_id:
            add_edge(f"sample-{exp.sample_id}", f"exp-{exp.id}")

    # FileRuns → Files (batch fetch)
    exp_ids = [e.id for e in experiments]
    file_runs: list[FileRun] = []
    if exp_ids:
        file_runs = (
            await session.execute(select(FileRun).where(FileRun.experiment_id.in_(exp_ids)))
        ).scalars().all()

    file_ids = list({fr.file_id for fr in file_runs})
    file_map: dict[str, File] = {}
    if file_ids:
        file_objs = (
            await session.execute(select(File).where(File.id.in_(file_ids)))
        ).scalars().all()
        file_map = {f.id: f for f in file_objs}

    fastq_node_ids: list[str] = []  # FASTQ file nodes already in the graph
    for fr in file_runs:
        file_obj = file_map.get(fr.file_id)
        if file_obj:
            add_node({"id": f"file-{file_obj.id}", "type": "file",
                      "label": file_obj.filename,
                      "attrs": {"size_bytes": file_obj.size_bytes, "type": file_obj.file_type}})
            add_edge(f"exp-{fr.experiment_id}", f"file-{file_obj.id}")
            if file_obj.file_type == "fastq":
                fastq_node_ids.append(f"file-{file_obj.id}")

    # ── Analysis side: per-job subgraphs  Job → Step → Result ─────────────────

    result_extensions = {".png", ".csv", ".html", ".tsv", ".bed", ".bam", ".vcf"}

    jobs = (
        await session.execute(
            select(AnalysisJob)
            .where(AnalysisJob.project_id == project_id)
            .order_by(AnalysisJob.created_at)
        )
    ).scalars().all()

    for job in jobs:
        output_dir = job.output_dir
        if not output_dir or not os.path.isdir(output_dir):
            continue

        # Collect (step_dir_name_or_None, file_path, file_name) for every result
        step_results: list[tuple] = []
        try:
            step_dirs = sorted(
                [e for e in os.scandir(output_dir) if e.is_dir()],
                key=lambda e: e.name,
            )
            for step_entry in step_dirs:
                for root, _, fnames in os.walk(step_entry.path):
                    for fname in fnames:
                        if any(fname.lower().endswith(ext) for ext in result_extensions):
                            step_results.append(
                                (step_entry.name, os.path.join(root, fname), fname)
                            )
            # Results directly in output_dir (no step subdirs)
            for fname in os.listdir(output_dir):
                fpath = os.path.join(output_dir, fname)
                if os.path.isfile(fpath) and any(fname.lower().endswith(ext) for ext in result_extensions):
                    step_results.append((None, fpath, fname))
        except Exception:
            continue

        if not step_results:
            continue  # job produced nothing displayable

        # Add job node
        job_nid = f"job-{job.id}"
        ts = job.created_at.strftime("%Y-%m-%d %H:%M") if job.created_at else ""
        add_node({"id": job_nid, "type": "job",
                  "label": job.name or f"Job {job.id[:8]}",
                  "attrs": {"status": job.status, "created_at": ts}})

        # Connect FASTQ file nodes already in graph → this job
        for fnode_id in fastq_node_ids:
            add_edge(fnode_id, job_nid)

        # Emit step nodes then result nodes, grouped by step
        for step_name, group in groupby(step_results, key=lambda x: x[0]):
            group_list = list(group)
            if step_name:
                step_nid = f"step-{job.id}-{step_name}"
                add_node({"id": step_nid, "type": "step",
                          "label": step_name, "attrs": {}})
                add_edge(job_nid, step_nid)
                parent_nid = step_nid
            else:
                parent_nid = job_nid

            for _, fpath, fname in group_list:
                result_nid = f"result-{fpath}"
                add_node({"id": result_nid, "type": "result",
                          "label": fname, "attrs": {"path": fpath}})
                add_edge(parent_nid, result_nid)

    return {"nodes": nodes, "edges": edges}
