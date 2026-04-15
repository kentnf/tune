"""PlannerPromptAdapter — serializes project planning context to LLM-readable text.

The formatted block is injected into the planner system prompt in place of
the old _summarize_files() file-count-only summary.

Output example (en):
    ## Project Context
    Project Goal: differential expression under heat stress
    Files: fastq: 12, gtf: 1, fa: 1
    Samples (6): apple-leaf-R1 (Malus domestica), apple-leaf-R2 (Malus domestica), ...
    Organism: Malus domestica
    Library Strategy: RNA-Seq
    Platform: ILLUMINA
    Paired-end: yes
    Reference genome: yes

    ⚠ Notes:
      - No reference genome registered (use 'add reference genome' to register one)
"""
from __future__ import annotations

import json
from typing import Any

from tune.core.context.models import (
    PlannerContext,
    _experiments_from_project_state,
    _files_from_project_state,
    _project_info_from_project_state,
    _samples_from_project_state,
    _summary_from_project_state,
)

_LABELS_EN = {
    "bio_context": "## Project Context",
    "project_goal": "Project Goal",
    "samples": "Samples",
    "organisms_label": "Organism",
    "strategy": "Library Strategy",
    "platform": "Platform",
    "paired": "Paired-end",
    "reference": "Reference genome",
    "files": "Files",
    "yes": "yes",
    "no": "no",
    "none_registered": "none registered",
    "issues": "⚠ Notes",
    "fastq_reads": "FASTQ read assignments",
    "r1": "R1",
    "r2": "R2",
    "resource_readiness": "Resource readiness",
    "analysis_family": "Suggested analysis family",
    "required_resources": "Required resource roles",
    "resource_candidates": "Candidate resources",
    "memory_hints": "Project memory hints",
    "stable_facts": "Stable project facts",
    "semantic_hints": "Semantic corpus hints",
    "ambiguity_hints": "Ambiguity warnings",
    "planning_hints": "Planning hints",
}

_LABELS_ZH = {
    "bio_context": "## 项目上下文",
    "project_goal": "项目目标",
    "samples": "样本",
    "organisms_label": "物种",
    "strategy": "文库策略",
    "platform": "测序平台",
    "paired": "双端测序",
    "reference": "参考基因组",
    "files": "文件",
    "yes": "是",
    "no": "否",
    "none_registered": "未注册",
    "issues": "⚠ 注意事项",
    "fastq_reads": "FASTQ 读端分配",
    "r1": "R1",
    "r2": "R2",
    "resource_readiness": "资源就绪状态",
    "analysis_family": "建议分析家族",
    "required_resources": "关键资源角色",
    "resource_candidates": "候选资源",
    "memory_hints": "项目记忆提示",
    "stable_facts": "稳定项目事实",
    "semantic_hints": "语义语料提示",
    "ambiguity_hints": "候选冲突提示",
    "planning_hints": "规划提示",
}

_SAMPLE_CAP = 20
_FILE_CAP = 30


class PlannerPromptAdapter:
    @staticmethod
    def format(
        context: PlannerContext | dict[str, Any],
        language: str = "en",
        *,
        semantic_memory_dossier: dict[str, Any] | None = None,
        resource_summary=None,
    ) -> str:
        """Return a structured text block for injection into the planner prompt."""
        if isinstance(context, dict):
            return json.dumps(context, ensure_ascii=False, indent=2)

        L = _LABELS_ZH if language == "zh" else _LABELS_EN
        project, samples, experiments, files, s, inferred_resource_summary, inferred_dossier = (
            PlannerPromptAdapter._normalize_context_payload(context)
        )
        semantic_memory = semantic_memory_dossier or inferred_dossier
        rs = resource_summary if resource_summary is not None else inferred_resource_summary
        lines: list[str] = [L["bio_context"]]

        # Project goal
        if project and project.project_goal:
            lines.append(f"{L['project_goal']}: {project.project_goal}")

        # Files by type
        if s.files_by_type:
            ft_str = ", ".join(
                f"{ft}: {n}" for ft, n in sorted(s.files_by_type.items())
            )
            lines.append(f"{L['files']}: {ft_str}")

        # Samples list (capped)
        if samples:
            sample_list = samples[:_SAMPLE_CAP]
            overflow = len(samples) - _SAMPLE_CAP
            suffix = f" (+{overflow} more)" if overflow > 0 else ""
            sample_strs = [
                sm.sample_name + (f" ({sm.organism})" if sm.organism else "")
                for sm in sample_list
            ]
            lines.append(
                f"{L['samples']} ({s.sample_count}){suffix}: {', '.join(sample_strs)}"
            )
        else:
            lines.append(f"{L['samples']}: {L['none_registered']}")

        # Organisms
        if s.organisms:
            lines.append(f"{L['organisms_label']}: {', '.join(s.organisms)}")

        # Library strategy
        if s.library_strategies:
            lines.append(f"{L['strategy']}: {', '.join(s.library_strategies)}")

        # Platform
        if experiments:
            platforms = sorted(
                {e.platform for e in experiments if e.platform}
            )
            if platforms:
                lines.append(f"{L['platform']}: {', '.join(platforms)}")

        # Paired-end
        if s.is_paired_end is True:
            lines.append(f"{L['paired']}: {L['yes']}")
        elif s.is_paired_end is False:
            lines.append(f"{L['paired']}: {L['no']}")

        # Reference genome
        if s.has_reference_genome is not None:
            ref_status = L["yes"] if s.has_reference_genome else L["no"]
            lines.append(f"{L['reference']}: {ref_status}")

        if s.analysis_family:
            family_suffix = (
                f" ({s.suggested_analysis_type})"
                if s.suggested_analysis_type
                else ""
            )
            lines.append(f"{L['analysis_family']}: {s.analysis_family}{family_suffix}")
        if s.required_resource_roles:
            lines.append(
                f"{L['required_resources']}: {', '.join(s.required_resource_roles)}"
            )
        resource_candidates = semantic_memory.get("resource_candidates") or s.resource_candidates
        memory_hints = semantic_memory.get("memory_hints") or s.memory_hints
        stable_facts = semantic_memory.get("stable_facts") or s.stable_facts
        semantic_hints = semantic_memory.get("semantic_hints") or s.semantic_hints
        ambiguity_hints = semantic_memory.get("ambiguity_hints") or s.ambiguity_hints
        if resource_candidates:
            lines.append(f"{L['resource_candidates']}:")
            for candidate in resource_candidates[:6]:
                meta_parts = [
                    part
                    for part in (
                        candidate.get("organism"),
                        candidate.get("genome_build"),
                        candidate.get("source_type"),
                    )
                    if part
                ]
                meta_suffix = f" [{' | '.join(meta_parts)}]" if meta_parts else ""
                lines.append(
                    f"  - {candidate.get('binding_key')}: {candidate.get('path')}{meta_suffix}"
                )
        if memory_hints:
            lines.append(f"{L['memory_hints']}:")
            for hint in memory_hints[:4]:
                lines.append(f"  - {hint}")
        if stable_facts:
            lines.append(f"{L['stable_facts']}:")
            for fact in stable_facts[:4]:
                statement = fact.get("statement") or fact.get("title")
                if statement:
                    lines.append(f"  - {statement}")
        if semantic_hints:
            lines.append(f"{L['semantic_hints']}:")
            for hint in semantic_hints[:4]:
                lines.append(f"  - {hint}")
        if ambiguity_hints:
            lines.append(f"{L['ambiguity_hints']}:")
            for hint in ambiguity_hints[:4]:
                lines.append(f"  - {hint}")

        # FASTQ R1/R2 read assignments
        _RN_CAP = 5
        fastq_files = [f for f in files if f.file_type == "fastq"]
        r1_files = [f for f in fastq_files if f.read_number == 1]
        r2_files = [f for f in fastq_files if f.read_number == 2]
        if r1_files or r2_files:
            def _rn_str(flist: list) -> str:
                shown = flist[:_RN_CAP]
                overflow = len(flist) - _RN_CAP
                names = ", ".join(f.filename for f in shown)
                return f"{names} (+{overflow} more)" if overflow > 0 else names
            rn_parts = []
            if r1_files:
                rn_parts.append(f"{L['r1']} ({len(r1_files)}): {_rn_str(r1_files)}")
            if r2_files:
                rn_parts.append(f"{L['r2']} ({len(r2_files)}): {_rn_str(r2_files)}")
            lines.append(f"{L['fastq_reads']}: " + " | ".join(rn_parts))

        # Resource readiness section (from ResourceSummary)
        if rs is not None:
            reads_val = "ready" if rs.reads_ready else "missing"
            ref_val = rs.reference_status
            ann_val = rs.annotation_status
            idx_val = rs.index_status
            if rs.prepare_steps_needed:
                idx_val = f"{idx_val} (will build: {', '.join(rs.prepare_steps_needed)})"
            lines.append(
                f"{L['resource_readiness']}: reads={reads_val}, "
                f"reference={ref_val}, annotation={ann_val}, index={idx_val}"
            )
            hard_constraints = []
            if not rs.reads_ready:
                hard_constraints.append("avoid steps that require sequencing reads unless they can be provided upstream")
            if rs.reference_status not in {"ready", "derivable", "stale"}:
                hard_constraints.append("do not plan reference-dependent alignment without a resolvable reference")
            if rs.annotation_status != "ready":
                hard_constraints.append("do not plan annotation-dependent quantification unless annotation is available")
            if rs.prepare_steps_needed:
                hard_constraints.append(
                    "include explicit prepare steps for: " + ", ".join(rs.prepare_steps_needed)
                )
            if hard_constraints:
                lines.append("Hard planning constraints:")
                for constraint in hard_constraints:
                    lines.append(f"  - {constraint}")

        if s.planning_hints:
            lines.append(f"\n{L['planning_hints']}:")
            for hint in s.planning_hints:
                lines.append(f"  - {hint}")

        # Potential issues / notes
        if s.potential_issues:
            lines.append(f"\n{L['issues']}:")
            for issue in s.potential_issues:
                lines.append(f"  - {issue}")

        return "\n".join(lines)

    @staticmethod
    def _normalize_context_payload(
        context: PlannerContext | dict[str, Any],
    ):
        if isinstance(context, PlannerContext):
            return (
                context.project,
                list(context.samples),
                list(context.experiments),
                list(context.files),
                context.summary,
                getattr(context, "resource_summary", None),
                getattr(context, "semantic_memory_dossier", None) or {},
            )

        project_state = dict(context or {})
        return (
            _project_info_from_project_state(project_state),
            _samples_from_project_state(project_state),
            _experiments_from_project_state(project_state),
            _files_from_project_state(project_state),
            _summary_from_project_state(project_state),
            None,
            {},
        )
