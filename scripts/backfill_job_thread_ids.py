from __future__ import annotations

import argparse
import asyncio
import re
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select

from tune.core.config import load_config, set_config
from tune.core.database import get_session_factory
from tune.core.models import AnalysisJob, Thread, ThreadMessage


_NORMALIZE_RE = re.compile(r"[\W_]+", re.UNICODE)


def _normalize(text: str | None) -> str:
    if not text:
        return ""
    return _NORMALIZE_RE.sub("", text).casefold()


def _texts_match(job_goal: str | None, message_content: str | None) -> bool:
    goal_norm = _normalize(job_goal)
    message_norm = _normalize(message_content)
    if not goal_norm or not message_norm:
        return False
    return goal_norm in message_norm or message_norm in goal_norm


@dataclass
class CandidateMatch:
    job_id: str
    thread_id: str
    thread_title: str | None
    message_at: object
    reason: str


async def _select_backfill_matches(*, max_gap_minutes: int) -> tuple[list[CandidateMatch], list[str]]:
    matches: list[CandidateMatch] = []
    skipped: list[str] = []

    async with get_session_factory()() as session:
        jobs = (
            await session.execute(
                select(AnalysisJob)
                .where(AnalysisJob.thread_id.is_(None))
                .order_by(AnalysisJob.created_at)
            )
        ).scalars().all()

        for job in jobs:
            if not job.project_id or not job.goal or not job.created_at:
                skipped.append(f"{job.id}: missing project_id/goal/created_at")
                continue

            candidate_threads = (
                await session.execute(
                    select(Thread)
                    .where(
                        Thread.project_id == job.project_id,
                        Thread.created_at <= job.created_at,
                    )
                    .order_by(Thread.created_at)
                )
            ).scalars().all()
            if not candidate_threads:
                skipped.append(f"{job.id}: no thread existed before job creation")
                continue

            valid_candidates: list[CandidateMatch] = []
            for thread in candidate_threads:
                recent_user_msg = (
                    await session.execute(
                        select(ThreadMessage)
                        .where(
                            ThreadMessage.thread_id == thread.id,
                            ThreadMessage.role == "user",
                            ThreadMessage.created_at <= job.created_at,
                        )
                        .order_by(ThreadMessage.created_at.desc())
                        .limit(1)
                    )
                ).scalars().first()
                if not recent_user_msg:
                    continue

                gap = job.created_at - recent_user_msg.created_at
                if gap.total_seconds() < 0 or gap.total_seconds() > max_gap_minutes * 60:
                    continue

                if not _texts_match(job.goal, recent_user_msg.content):
                    continue

                valid_candidates.append(
                    CandidateMatch(
                        job_id=job.id,
                        thread_id=thread.id,
                        thread_title=thread.title,
                        message_at=recent_user_msg.created_at,
                        reason="goal matches recent user message",
                    )
                )

            if len(valid_candidates) == 1:
                matches.append(valid_candidates[0])
            elif not valid_candidates:
                skipped.append(f"{job.id}: no deterministic thread match")
            else:
                thread_ids = ", ".join(c.thread_id for c in valid_candidates)
                skipped.append(f"{job.id}: ambiguous across threads {thread_ids}")

    return matches, skipped


async def _apply_backfill(matches: list[CandidateMatch]) -> int:
    if not matches:
        return 0

    async with get_session_factory()() as session:
        for match in matches:
            job = (
                await session.execute(
                    select(AnalysisJob).where(AnalysisJob.id == match.job_id)
                )
            ).scalar_one_or_none()
            if job and job.thread_id is None:
                job.thread_id = match.thread_id
        await session.commit()
    return len(matches)


async def _main() -> int:
    parser = argparse.ArgumentParser(description="Backfill analysis_jobs.thread_id conservatively.")
    parser.add_argument(
        "--analysis-dir",
        default="/Users/kentnf/projects/tune/analysis",
        help="Tune workspace root (or legacy analysis/workspace path) containing .tune/config.yaml",
    )
    parser.add_argument(
        "--max-gap-minutes",
        type=int,
        default=15,
        help="Maximum allowed gap between matched user message and job creation",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist the backfill instead of printing a dry run",
    )
    args = parser.parse_args()

    cfg = load_config(Path(args.analysis_dir))
    set_config(cfg)

    matches, skipped = await _select_backfill_matches(max_gap_minutes=args.max_gap_minutes)

    print(f"candidate_matches={len(matches)} skipped={len(skipped)}")
    for match in matches:
        print(
            f"MATCH job={match.job_id} -> thread={match.thread_id} "
            f"title={match.thread_title!r} at={match.message_at} reason={match.reason}"
        )
    for item in skipped:
        print(f"SKIP {item}")

    if args.apply:
        updated = await _apply_backfill(matches)
        print(f"updated={updated}")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
