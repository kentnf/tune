"""Environment planner — derive the Pixi package set for a full analysis plan.

Workflow:
  1. build_env_spec(steps, registry) — collect pixi_packages from each step type,
     deduplicate, compute a deterministic hash, detect version conflicts.
  2. check_env_cache(env_dir, env_spec_hash) — returns True if an env with that
     hash already exists and is ready (skip pixi install).
  3. write_env_cache(env_dir, env_spec_hash) — persist hash after successful install.
  4. format_env_spec_summary(spec) — human-readable summary for log messages.

Phase 7 additions to EnvSpec:
  - conflicts: list of detected version-pin collisions across steps
  - step_package_map: {step_key: [package, ...]} for debug tracing
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger(__name__)

_ENV_HASH_FILE = ".env_spec_hash"
_ENV_ORIGIN_FILE = ".env_origin_path"

_PACKAGE_ALIASES: dict[str, str] = {
    "featurecounts": "subread",
    "feature-counts": "subread",
    "feature_counts": "subread",
    "rscript": "r-base",
    "r-script": "r-base",
    "r_script": "r-base",
    "hisat2-build": "hisat2",
    "hisat2_build": "hisat2",
    "hisat2.build": "hisat2",
    "star-genome": "star",
    "star_genome": "star",
    "star.genome": "star",
    "starlong": "star",
    "bowtie2-build": "bowtie2",
    "bowtie2_build": "bowtie2",
    "bowtie2.build": "bowtie2",
}

_PACKAGE_SUFFIX_REWRITES = (
    "-build",
    "_build",
    ".build",
    "-genome",
    "_genome",
    ".genome",
)


@dataclass
class EnvSpec:
    """Fully-resolved specification for a Pixi environment."""
    packages: list[str]                        # deduplicated, sorted list of packages
    hash: str                                  # sha256[:16] of the sorted package list
    # Phase 7: richer metadata
    conflicts: list[str] = field(default_factory=list)
    # {step_key_or_type: [package, ...]} — which step needs which package
    step_package_map: dict[str, list[str]] = field(default_factory=dict)


def _parse_package_name(package: str) -> tuple[str, str]:
    """Return (name, version_spec) from a package string like 'hisat2==2.2.1'.

    For packages without a version pin the version_spec is ''.
    """
    m = re.match(r"^([A-Za-z0-9_.\-]+)(.*)", package)
    if m:
        return m.group(1).lower(), m.group(2).strip()
    return package.lower(), ""


def _candidate_package_names(name: str) -> list[str]:
    base = name.lower().strip()
    if not base:
        return []

    raw_forms = list(
        dict.fromkeys(
            [
                base,
                base.replace("_", "-"),
                base.replace(".", "-"),
                base.replace("_", "-").replace(".", "-"),
            ]
        )
    )

    preferred: list[str] = []
    for form in raw_forms:
        alias = _PACKAGE_ALIASES.get(form)
        if alias:
            preferred.append(alias)
        for suffix in _PACKAGE_SUFFIX_REWRITES:
            if form.endswith(suffix):
                stripped = form[: -len(suffix)].strip("._-")
                if stripped:
                    preferred.append(_PACKAGE_ALIASES.get(stripped, stripped))

    return list(dict.fromkeys([item for item in preferred + raw_forms if item]))


def candidate_package_specs(package: str) -> list[str]:
    """Return ordered safe package candidates for installation retries."""
    name, ver = _parse_package_name(package)
    return [f"{candidate}{ver}" for candidate in _candidate_package_names(name)]


def normalize_package_spec(package: str) -> str:
    """Normalize a requested package name to the Pixi/conda package we should install."""
    candidates = candidate_package_specs(package)
    return candidates[0] if candidates else package.lower()


def _detect_conflicts(packages: list[str]) -> list[str]:
    """Return a list of human-readable conflict descriptions.

    A conflict occurs when the same package is required with two different
    explicit version pins (e.g. 'hisat2==2.2.1' vs 'hisat2==2.1.0').
    Packages without pins are always compatible.
    """
    seen: dict[str, str] = {}   # name → first version_spec seen
    conflicts: list[str] = []

    for pkg in packages:
        name, ver = _parse_package_name(pkg)
        if not ver:
            continue  # unversioned — always ok
        if name in seen:
            if seen[name] != ver:
                conflicts.append(
                    f"Package '{name}' required with conflicting versions: "
                    f"'{seen[name]}' and '{ver}'"
                )
        else:
            seen[name] = ver

    return conflicts


def build_env_spec(
    steps: list[dict],
    registry=None,
) -> EnvSpec:
    """Derive the Pixi package set for all steps in a plan.

    Args:
        steps: list of step dicts (each may have ``step_type`` and/or ``tool``)
        registry: optional pre-imported registry module; imported lazily if None

    Returns:
        EnvSpec with deduplicated package list, hash, conflict warnings, and
        step_package_map for tracing which step needs which packages.
    """
    if registry is None:
        from tune.core import registry as _reg
        registry = _reg

    packages: set[str] = set()
    step_package_map: dict[str, list[str]] = {}

    for step in steps:
        step_type = step.get("step_type")
        step_key = step.get("step_key") or step_type or step.get("name", "unknown")
        step_pkgs: list[str] = []

        if step_type:
            defn = registry.get_step_type(step_type)
            if defn:
                step_pkgs = [normalize_package_spec(pkg) for pkg in defn.pixi_packages]
                packages.update(step_pkgs)
            else:
                log.debug("Unknown step_type '%s' — no packages added", step_type)
        else:
            # Legacy plan: map tool name heuristically
            tool = step.get("tool", "").lower()
            legacy_map = {
                "fastqc":        ["fastqc"],
                "multiqc":       ["multiqc"],
                "fastp":         ["fastp"],
                "hisat2":        ["hisat2", "samtools"],
                "star":          ["star", "samtools"],
                "featurecounts": ["subread"],
                "samtools":      ["samtools"],
            }
            for key, pkgs in legacy_map.items():
                if key in tool:
                    normalized = [normalize_package_spec(pkg) for pkg in pkgs]
                    step_pkgs.extend(normalized)
                    packages.update(normalized)

        if step_pkgs:
            step_package_map[step_key] = sorted(set(step_pkgs))

    sorted_packages = sorted(packages)
    pkg_hash = hashlib.sha256(json.dumps(sorted_packages).encode()).hexdigest()[:16]
    conflicts = _detect_conflicts(sorted_packages)

    if conflicts:
        log.warning("build_env_spec: detected conflicts: %s", conflicts)

    return EnvSpec(
        packages=sorted_packages,
        hash=pkg_hash,
        conflicts=conflicts,
        step_package_map=step_package_map,
    )


def check_env_cache(env_dir: Path, env_spec_hash: str) -> bool:
    """Return True if a Pixi environment with the given hash already exists.

    The hash is stored in ``{env_dir}/.env_spec_hash`` and the resolved env path
    is stored in ``{env_dir}/.env_origin_path``. Both must match the current
    location. This avoids reusing moved environments whose binary link paths
    still point at an older workspace path.
    """
    hash_file = env_dir / _ENV_HASH_FILE
    origin_file = env_dir / _ENV_ORIGIN_FILE
    if not hash_file.exists() or not origin_file.exists():
        return False
    stored = hash_file.read_text().strip()
    stored_origin = origin_file.read_text().strip()
    return stored == env_spec_hash and stored_origin == str(env_dir.resolve())


def write_env_cache(env_dir: Path, env_spec_hash: str) -> None:
    """Persist the env spec hash after a successful install."""
    env_dir.mkdir(parents=True, exist_ok=True)
    (env_dir / _ENV_HASH_FILE).write_text(env_spec_hash)
    (env_dir / _ENV_ORIGIN_FILE).write_text(str(env_dir.resolve()))


def format_env_spec_summary(spec: EnvSpec) -> str:
    """Return a compact human-readable summary of an EnvSpec for log messages."""
    parts = [f"packages={spec.packages}", f"hash={spec.hash}"]
    if spec.conflicts:
        parts.append(f"conflicts={spec.conflicts}")
    if spec.step_package_map:
        steps = list(spec.step_package_map.keys())
        parts.append(f"steps={steps}")
    return "EnvSpec(" + ", ".join(parts) + ")"
