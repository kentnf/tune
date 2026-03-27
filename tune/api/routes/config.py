"""Config and workspace settings API routes."""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from tune.core.config import TuneConfig, get_config, save_config, validate_config
from tune.core.llm.gateway import reset_gateway

router = APIRouter()


@router.get("/")
async def get_config_endpoint():
    cfg = get_config()
    return {
        "workspace_root": str(cfg.workspace_root) if cfg.workspace_root else None,
        "data_dir": str(cfg.data_dir),
        "analysis_dir": str(cfg.analysis_dir),
        "pixi_path": cfg.pixi_path,
        "host": cfg.host,
        "port": cfg.port,
        "active_llm_config_id": cfg.active_llm_config_id,
        "auto_authorize_commands": cfg.auto_authorize_commands,
    }


class ConfigUpdate(BaseModel):
    workspace_root: str | None = None
    data_dir: str | None = None
    analysis_dir: str | None = None
    pixi_path: str | None = None
    auto_authorize_commands: bool | None = None


@router.put("/")
async def update_config(body: ConfigUpdate):
    cfg = get_config()
    restart_required = False

    # --- directories ---
    if body.workspace_root and (body.data_dir or body.analysis_dir):
        raise HTTPException(400, "workspace_root cannot be combined with data_dir or analysis_dir")

    new_workspace_root = (
        Path(body.workspace_root).expanduser().resolve()
        if body.workspace_root
        else cfg.workspace_root
    )
    if body.workspace_root:
        from tune.core.config import derive_workspace_dirs

        new_data_dir, new_analysis_dir = derive_workspace_dirs(new_workspace_root)
    else:
        new_data_dir = Path(body.data_dir).expanduser().resolve() if body.data_dir else cfg.data_dir
        new_analysis_dir = Path(body.analysis_dir).expanduser().resolve() if body.analysis_dir else cfg.analysis_dir

    if new_data_dir != cfg.data_dir or new_analysis_dir != cfg.analysis_dir:
        if body.workspace_root:
            new_data_dir.mkdir(parents=True, exist_ok=True)
            new_analysis_dir.mkdir(parents=True, exist_ok=True)
        else:
            if not new_data_dir.exists():
                raise HTTPException(400, f"data_dir does not exist: {new_data_dir}")
            if not new_analysis_dir.exists() and body.analysis_dir:
                new_analysis_dir.mkdir(parents=True, exist_ok=True)
        if str(new_analysis_dir).startswith(str(new_data_dir) + "/"):
            raise HTTPException(400, "analysis_dir must not be inside data_dir")
        if new_data_dir == new_analysis_dir:
            raise HTTPException(400, "data_dir and analysis_dir must differ")
        restart_required = True

    new_cfg = TuneConfig(
        workspace_root=new_workspace_root,
        data_dir=new_data_dir,
        analysis_dir=new_analysis_dir,
        pixi_path=body.pixi_path or cfg.pixi_path,
        database_url=cfg.database_url,
        host=cfg.host,
        port=cfg.port,
        llm_configs=cfg.llm_configs,
        active_llm_config_id=cfg.active_llm_config_id,
        auto_authorize_commands=(
            body.auto_authorize_commands
            if body.auto_authorize_commands is not None
            else cfg.auto_authorize_commands
        ),
    )

    errors = validate_config(new_cfg)
    if errors:
        raise HTTPException(400, "; ".join(errors))

    save_config(new_cfg)
    from tune.core.config import set_config
    set_config(new_cfg)

    return {"ok": True, "restart_required": restart_required}
