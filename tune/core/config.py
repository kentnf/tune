"""Configuration model and persistence."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, field_validator, model_validator


class ApiConfig(BaseModel):
    """A single named LLM API configuration entry."""
    id: str
    name: str
    provider: str                         # e.g. openai / anthropic / qwen / deepseek / proxy / custom
    api_style: str                        # openai_compatible | openai | anthropic
    base_url: Optional[str] = None        # None for native Anthropic/OpenAI endpoints
    model_name: str
    api_key: str
    enabled: bool = True
    timeout: int = 120
    max_retries: int = 2
    endpoint_path: Optional[str] = None   # e.g. /v1/chat/completions
    extra_headers: dict[str, str] = {}
    extra_params: dict[str, Any] = {}
    remark: Optional[str] = None
    created_at: str = ""
    updated_at: str = ""

    @staticmethod
    def new(
        name: str,
        provider: str,
        api_style: str,
        model_name: str,
        api_key: str,
        base_url: Optional[str] = None,
        **kwargs,
    ) -> "ApiConfig":
        now = datetime.now(timezone.utc).isoformat()
        return ApiConfig(
            id=str(uuid.uuid4()),
            name=name,
            provider=provider,
            api_style=api_style,
            base_url=base_url,
            model_name=model_name,
            api_key=api_key,
            created_at=now,
            updated_at=now,
            **kwargs,
        )


class TuneConfig(BaseModel):
    workspace_root: Optional[Path] = None
    data_dir: Path
    analysis_dir: Path
    llm_configs: list[ApiConfig] = []
    active_llm_config_id: Optional[str] = None
    auto_authorize_commands: bool = False
    developer_show_llm_io_in_chat: bool = False
    database_url: str = "postgresql+psycopg://tune:tune@localhost:5432/tune"
    pixi_path: str = "pixi"
    host: str = "0.0.0.0"
    port: int = 8000

    @field_validator("workspace_root", "data_dir", "analysis_dir", mode="before")
    @classmethod
    def expand_path(cls, v):
        if v is None or v == "":
            return None
        return Path(v).expanduser().resolve()

    @model_validator(mode="after")
    def dirs_must_differ(self):
        if self.data_dir == self.analysis_dir:
            raise ValueError("data_dir and analysis_dir must be different paths")
        if str(self.analysis_dir).startswith(str(self.data_dir) + os.sep):
            raise ValueError("analysis_dir must not be inside data_dir")
        inferred_root = self.workspace_root
        if inferred_root is None:
            inferred_root = infer_workspace_root(self.data_dir, self.analysis_dir)

        if inferred_root is not None:
            expected_data, expected_analysis = derive_workspace_dirs(inferred_root)
            legacy_data, legacy_analysis = derive_legacy_workspace_dirs(inferred_root)
            if (self.data_dir, self.analysis_dir) not in {
                (expected_data, expected_analysis),
                (legacy_data, legacy_analysis),
            }:
                raise ValueError(
                    "workspace_root requires either "
                    "data_dir=<workspace_root>/data and analysis_dir=<workspace_root>/analysis "
                    "or the legacy layout data_dir=<workspace_root>/data and analysis_dir=<workspace_root>/workspace"
                )
            self.workspace_root = inferred_root
        return self

    @property
    def active_llm(self) -> Optional[ApiConfig]:
        """Return the active ApiConfig, or None if not configured."""
        if not self.active_llm_config_id:
            return None
        for cfg in self.llm_configs:
            if cfg.id == self.active_llm_config_id:
                return cfg
        return None


def _migrate_legacy_llm_config(data: dict) -> dict:
    """Migrate old primary_llm / fallback_llm YAML structure to llm_configs list.

    This is idempotent: if llm_configs already exists, the function is a no-op.
    """
    if "llm_configs" in data:
        return data

    now = datetime.now(timezone.utc).isoformat()
    configs = []

    def _to_api_config(src: dict, name: str) -> dict:
        provider = src.get("provider", "openai_compatible")
        # Map old provider value to api_style
        if provider == "anthropic":
            api_style = "anthropic"
        else:
            api_style = "openai_compatible"
        return {
            "id": str(uuid.uuid4()),
            "name": name,
            "provider": provider,
            "api_style": api_style,
            "base_url": src.get("base_url"),
            "model_name": src.get("model", ""),
            "api_key": src.get("api_key", ""),
            "enabled": True,
            "timeout": src.get("timeout", 120),
            "max_retries": 2,
            "created_at": now,
            "updated_at": now,
        }

    primary = data.pop("primary_llm", None)
    fallback = data.pop("fallback_llm", None)

    if primary:
        entry = _to_api_config(primary, "主模型（已迁移）")
        configs.append(entry)

    if fallback:
        entry = _to_api_config(fallback, "备用模型（已迁移）")
        configs.append(entry)

    data["llm_configs"] = configs
    data["active_llm_config_id"] = configs[0]["id"] if configs else None
    return data


def derive_workspace_dirs(workspace_root: Path) -> tuple[Path, Path]:
    root = Path(workspace_root).expanduser().resolve()
    return root / "data", root / "analysis"


def derive_legacy_workspace_dirs(workspace_root: Path) -> tuple[Path, Path]:
    root = Path(workspace_root).expanduser().resolve()
    return root / "data", root / "workspace"


def infer_workspace_root(data_dir: Path, analysis_dir: Path) -> Path | None:
    if data_dir.parent != analysis_dir.parent:
        return None
    parent = data_dir.parent
    current_expected = derive_workspace_dirs(parent)
    legacy_expected = derive_legacy_workspace_dirs(parent)
    if (data_dir, analysis_dir) in {current_expected, legacy_expected}:
        return parent
    return None


def _config_path(config_root: Path) -> Path:
    return config_root / ".tune" / "config.yaml"


def _resolve_config_path(path: Path) -> Path:
    resolved = Path(path).expanduser().resolve()
    direct = _config_path(resolved)
    if direct.exists():
        return direct

    for child_name in ("analysis", "workspace"):
        child = resolved / child_name
        candidate = _config_path(child)
        if candidate.exists():
            return candidate

    if resolved.name in {"analysis", "workspace"}:
        parent_candidate = _config_path(resolved.parent)
        if parent_candidate.exists():
            return parent_candidate

    return direct


def load_config(config_root_or_analysis_dir: Path) -> TuneConfig:
    path = _resolve_config_path(config_root_or_analysis_dir)
    if not path.exists():
        raise FileNotFoundError(f"Config not found at {path}. Run 'tune init' first.")
    with open(path) as f:
        data = yaml.safe_load(f) or {}

    # Migrate legacy primary_llm / fallback_llm on first load
    if "primary_llm" in data and "llm_configs" not in data:
        data = _migrate_legacy_llm_config(data)

    return TuneConfig(**data)


def save_config(cfg: TuneConfig) -> None:
    config_root = cfg.workspace_root or cfg.analysis_dir
    path = _config_path(config_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump(cfg.model_dump(mode="json"), f, default_flow_style=False)


# Runtime cache — populated by CLI on startup
_runtime_config: Optional[TuneConfig] = None


def set_config(cfg: TuneConfig) -> None:
    global _runtime_config
    _runtime_config = cfg


def resolve_runtime_analysis_dir_from_env() -> Path | None:
    # Prefer the explicit analysis-dir override when both vars are present.
    # This keeps tests and one-off tooling able to redirect runtime artifacts
    # without having to also mutate the workspace-root env var.
    for env_name in ("TUNE_ANALYSIS_DIR", "TUNE_WORKSPACE_ROOT"):
        raw = os.environ.get(env_name)
        if not raw:
            continue
        raw_path = Path(raw).expanduser().resolve()
        try:
            return load_config(raw_path).analysis_dir
        except Exception:
            if env_name == "TUNE_WORKSPACE_ROOT":
                return derive_workspace_dirs(raw_path)[1]
            return raw_path
    return None


def get_config() -> TuneConfig:
    global _runtime_config
    if _runtime_config is None:
        # Fallback: load from env vars set by CLI (supports uvicorn --reload child processes)
        config_input = os.environ.get("TUNE_WORKSPACE_ROOT") or os.environ.get("TUNE_ANALYSIS_DIR")
        if config_input:
            _runtime_config = load_config(Path(config_input))
        else:
            raise RuntimeError("Config not loaded. Start the server with 'tune start'.")
    return _runtime_config


def validate_config(cfg: TuneConfig) -> list[str]:
    """Return list of error strings; empty list means valid."""
    errors = []
    if not cfg.data_dir.exists():
        errors.append(f"data_dir does not exist: {cfg.data_dir}")
    elif not os.access(cfg.data_dir, os.R_OK):
        errors.append(f"data_dir is not readable: {cfg.data_dir}")

    cfg.analysis_dir.mkdir(parents=True, exist_ok=True)
    if not os.access(cfg.analysis_dir, os.W_OK):
        errors.append(f"analysis_dir is not writable: {cfg.analysis_dir}")

    if str(cfg.analysis_dir).startswith(str(cfg.data_dir) + os.sep):
        errors.append("analysis_dir must not be inside data_dir")

    return errors
