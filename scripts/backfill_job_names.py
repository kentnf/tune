"""One-time backfill: update existing AnalysisJob.name to short slugs
and rename the corresponding output directories on disk.

Usage:
    python scripts/backfill_job_names.py --analysis-dir /path/to/analysis [--dry-run]

Only jobs whose name does not already match the slug pattern ([a-z0-9][a-z0-9-]{0,39})
will be updated. The new name is derived from _slugify(goal or name).
The output_dir is renamed from {timestamp}_{old_safe_name} → {timestamp}_{new_slug}.
"""
from __future__ import annotations

import argparse
import asyncio
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,39}$")
_TS_RE = re.compile(r"^(\d{8}_\d{6})_")  # matches "20260313_055344_"


def _slugify(text: str, max_len: int = 32) -> str:
    s = text.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s[:max_len].rstrip("-") or "analysis"


def _new_output_dir(output_dir: str, new_slug: str) -> str | None:
    """Return the new output_dir path with slug replacing the name part, or None if not parseable."""
    p = Path(output_dir)
    m = _TS_RE.match(p.name)
    if not m:
        return None
    new_name = f"{m.group(1)}_{new_slug}"
    return str(p.parent / new_name)


async def run(analysis_dir: str, dry_run: bool) -> None:
    from tune.core.config import load_config, set_config
    from tune.core.database import get_session_factory
    from sqlalchemy import select
    from tune.core.models import AnalysisJob

    cfg = load_config(Path(analysis_dir))
    set_config(cfg)

    async with get_session_factory()() as session:
        jobs = (await session.execute(select(AnalysisJob))).scalars().all()

        to_update: list[tuple[AnalysisJob, str, str | None, str | None]] = []
        for job in jobs:
            if _SLUG_RE.match(job.name or ""):
                continue  # already a valid slug — skip
            new_name = _slugify(job.goal or job.name or "analysis")
            new_dir = _new_output_dir(job.output_dir, new_name) if job.output_dir else None
            to_update.append((job, new_name, job.output_dir, new_dir))

        if not to_update:
            print("All jobs already have slug names. Nothing to update.")
            return

        prefix = "[DRY RUN] " if dry_run else ""
        print(f"{prefix}Jobs to update: {len(to_update)}\n")
        for job, new_name, old_dir, new_dir in to_update:
            print(f"  {job.id[:8]}…")
            print(f"    name:      {job.name!r:55s} → {new_name!r}")
            if old_dir and new_dir:
                exists = Path(old_dir).exists()
                print(f"    dir:       {Path(old_dir).name}")
                print(f"           →   {Path(new_dir).name}  {'(exists on disk)' if exists else '(directory not found — will skip rename)'}")
            elif old_dir:
                print(f"    dir:       {Path(old_dir).name}  (cannot parse timestamp — skipping rename)")
            print()

        if dry_run:
            print("Dry run complete — no changes written.")
            return

        errors: list[str] = []
        for job, new_name, old_dir, new_dir in to_update:
            # Rename directory if possible
            if old_dir and new_dir:
                old_path = Path(old_dir)
                new_path = Path(new_dir)
                if old_path.exists():
                    if new_path.exists():
                        print(f"  WARN: {new_path.name} already exists — skipping rename for job {job.id[:8]}")
                        errors.append(job.id)
                    else:
                        old_path.rename(new_path)
                        print(f"  Renamed: {old_path.name} → {new_path.name}")
                        job.output_dir = str(new_path)
                else:
                    print(f"  WARN: {old_path.name} not found on disk — updating DB only")

            job.name = new_name

        await session.commit()
        updated = len(to_update) - len(errors)
        print(f"\nDone. Updated {updated}/{len(to_update)} job(s).")
        if errors:
            print(f"Skipped {len(errors)} due to directory conflicts: {errors}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill job names to short slugs")
    parser.add_argument("--analysis-dir", required=True, help="Path to analysis directory")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without writing")
    args = parser.parse_args()
    asyncio.run(run(args.analysis_dir, args.dry_run))


if __name__ == "__main__":
    main()
