"""Step type registry package."""
from __future__ import annotations

import importlib
import os
from pathlib import Path

_REQUIRED_STEP_TYPES = {
    "align.hisat2",
    "align.star",
    "qc.fastqc",
    "qc.multiqc",
    "quant.featurecounts",
    "stats.deseq2",
    "trim.fastp",
    "util.hisat2_build",
    "util.samtools_index",
    "util.samtools_sort",
    "util.star_genome_generate",
}

_steps_module = None


def _bind_exports(steps_module) -> None:
    global _steps_module
    _steps_module = steps_module
    globals().update(
        {
            "StepTypeDefinition": steps_module.StepTypeDefinition,
            "SlotDefinition": steps_module.SlotDefinition,
            "RepairPolicy": steps_module.RepairPolicy,
            "FanoutMode": steps_module.FanoutMode,
            "SafetyPolicy": steps_module.SafetyPolicy,
        }
    )


def _resolve_runtime_analysis_dir() -> Path | None:
    analysis_dir_env = os.environ.get("TUNE_ANALYSIS_DIR")
    if analysis_dir_env:
        return Path(analysis_dir_env).expanduser().resolve()
    try:
        from tune.core.config import get_config
        return get_config().analysis_dir
    except Exception:
        return None


def ensure_registry_loaded() -> None:
    """Ensure built-in step types are present even after unusual import ordering."""
    steps_module = importlib.import_module("tune.core.registry.steps")
    if not _REQUIRED_STEP_TYPES.issubset(set(steps_module.all_step_types())):
        steps_module = importlib.reload(steps_module)
    _bind_exports(steps_module)
    from tune.core.registry.custom_steps import sync_runtime_custom_step_registry
    sync_runtime_custom_step_registry(_resolve_runtime_analysis_dir(), steps_module)


def get_step_type(step_type: str):
    ensure_registry_loaded()
    return _steps_module.get_step_type(step_type)


def all_step_types() -> list[str]:
    ensure_registry_loaded()
    return _steps_module.all_step_types()


def register(defn):
    ensure_registry_loaded()
    return _steps_module.register(defn)


_bind_exports(importlib.import_module("tune.core.registry.steps"))

__all__ = [
    "StepTypeDefinition",
    "SlotDefinition",
    "RepairPolicy",
    "FanoutMode",
    "SafetyPolicy",
    "get_step_type",
    "all_step_types",
    "register",
    "ensure_registry_loaded",
]
