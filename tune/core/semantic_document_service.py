"""Project-level semantic document refresh service.

This is the deterministic projection layer that turns current project facts and
project memory summaries into one unified semantic document corpus. It is kept
logic-only for now: no persistence, no embeddings, no schema changes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import math
import re

from tune.core.context.builder import PlannerContextBuilder
from tune.core.context.models import ContextScope
from tune.core.memory.project_memory import (
    query_project_memory_facts,
    query_project_memory_patterns,
    query_project_memory_preferences,
    query_recent_project_episodes,
    summarize_project_memory,
)
from tune.core.semantic_documents import (
    SemanticDocument,
    documents_from_memory_facts,
    documents_from_project_events,
    documents_from_project_memory_profile,
    documents_from_structured_project_memory_layers,
    project_semantic_documents_from_context,
)


@dataclass
class ProjectSemanticCorpus:
    project_id: str
    documents: list[SemanticDocument] = field(default_factory=list)
    memory_profile: dict = field(default_factory=dict)

    @property
    def counts_by_type(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for document in self.documents:
            counts[document.doc_type] = counts.get(document.doc_type, 0) + 1
        return counts


@dataclass(frozen=True)
class SemanticDocumentMatch:
    document: SemanticDocument
    score: int
    lexical_score: int | None = None
    semantic_score: float | None = None
    matched_terms: list[str] = field(default_factory=list)
    rationale_codes: list[str] = field(default_factory=list)


async def build_project_semantic_corpus(
    session,
    project_id: str,
    *,
    include_memory_profile: bool = True,
    include_recent_events: bool = True,
) -> ProjectSemanticCorpus:
    if not project_id:
        return ProjectSemanticCorpus(project_id="", documents=[], memory_profile={})

    ctx = await PlannerContextBuilder(session).build(ContextScope(project_id=project_id))
    documents = list(project_semantic_documents_from_context(ctx))

    memory_profile: dict = {}
    if include_recent_events:
        recent_events = await query_recent_project_episodes(session, project_id, limit=20)
        derived_facts = await query_project_memory_facts(session, project_id, limit=20)
        memory_patterns = await query_project_memory_patterns(session, project_id, limit=10)
        memory_preferences = await query_project_memory_preferences(session, project_id, limit=10)
        documents.extend(
            documents_from_memory_facts(derived_facts, project_id=project_id)
        )
        documents.extend(
            documents_from_project_events(recent_events, project_id=project_id)
        )
        documents.extend(
            documents_from_structured_project_memory_layers(
                memory_patterns=memory_patterns,
                memory_preferences=memory_preferences,
                project_id=project_id,
            )
        )
    if include_memory_profile:
        memory_profile = await summarize_project_memory(session, project_id, limit=20)
        documents.extend(
            documents_from_project_memory_profile(memory_profile, project_id=project_id)
        )

    documents = _dedupe_documents(documents)
    return ProjectSemanticCorpus(
        project_id=project_id,
        documents=documents,
        memory_profile=memory_profile,
    )


async def query_project_semantic_corpus(
    session,
    project_id: str,
    query_text: str,
    *,
    top_k: int = 5,
    include_memory_profile: bool = True,
    include_recent_events: bool = True,
) -> list[SemanticDocumentMatch]:
    corpus = await build_project_semantic_corpus(
        session,
        project_id,
        include_memory_profile=include_memory_profile,
        include_recent_events=include_recent_events,
    )
    return await query_semantic_documents(
        corpus.documents,
        query_text,
        top_k=top_k,
    )


async def query_semantic_documents(
    documents: list[SemanticDocument],
    query_text: str,
    *,
    top_k: int = 5,
) -> list[SemanticDocumentMatch]:
    query_terms = _tokenize(query_text)
    if not query_terms:
        return []

    matches: list[SemanticDocumentMatch] = []
    for document in documents:
        score, matched_terms = _score_document_match(document, query_terms)
        if score <= 0:
            continue
        matches.append(
            SemanticDocumentMatch(
                document=document,
                score=score,
                lexical_score=score,
                matched_terms=matched_terms,
                rationale_codes=_rationale_codes_for_match(
                    document=document,
                    matched_terms=matched_terms,
                    lexical_score=score,
                    semantic_score=None,
                ),
            )
        )

    matches.sort(
        key=lambda item: (
            item.score,
            item.document.doc_type,
            item.document.title,
        ),
        reverse=True,
    )
    matches = await _semantic_rerank_matches(query_text, matches)
    return matches[:top_k]


def _dedupe_documents(documents: list[SemanticDocument]) -> list[SemanticDocument]:
    by_id: dict[str, SemanticDocument] = {}
    for document in documents:
        by_id[document.doc_id] = document
    return sorted(by_id.values(), key=lambda item: (item.doc_type, item.title, item.doc_id))


def _score_document_match(
    document: SemanticDocument,
    query_terms: list[str],
) -> tuple[int, list[str]]:
    haystacks = [
        document.title.lower(),
        document.text.lower(),
        " ".join(
            str(value).lower()
            for value in document.metadata.values()
            if value not in (None, "")
        ),
    ]
    score = 0
    matched_terms: list[str] = []
    for term in query_terms:
        matched = False
        if term in haystacks[0]:
            score += 8
            matched = True
        if term in haystacks[1]:
            score += 5
            matched = True
        if term in haystacks[2]:
            score += 3
            matched = True
        if matched:
            matched_terms.append(term)

    score += _doc_type_bonus(document)
    return score, matched_terms


async def _semantic_rerank_matches(
    query_text: str,
    matches: list[SemanticDocumentMatch],
    *,
    semantic_pool_size: int = 8,
) -> list[SemanticDocumentMatch]:
    if not matches:
        return matches

    try:
        from tune.core.memory.global_memory import embed_text

        query_embedding = await embed_text(query_text)
        if not query_embedding:
            return matches

        reranked: list[SemanticDocumentMatch] = []
        for match in matches[:semantic_pool_size]:
            doc_embedding = await embed_text(
                f"{match.document.title}\n{match.document.text}"
            )
            semantic_score = _cosine_similarity(query_embedding, doc_embedding)
            if semantic_score is None:
                reranked.append(match)
                continue
            semantic_bonus = max(int(round(semantic_score * 10)), 0)
            reranked.append(
                SemanticDocumentMatch(
                    document=match.document,
                    score=match.score + semantic_bonus,
                    lexical_score=match.lexical_score or match.score,
                    semantic_score=semantic_score,
                    matched_terms=match.matched_terms,
                    rationale_codes=_rationale_codes_for_match(
                        document=match.document,
                        matched_terms=match.matched_terms,
                        lexical_score=match.lexical_score or match.score,
                        semantic_score=semantic_score,
                    ),
                )
            )

        tail = matches[semantic_pool_size:]
        reranked.sort(
            key=lambda item: (
                item.score,
                item.semantic_score or 0.0,
                item.document.doc_type,
                item.document.title,
            ),
            reverse=True,
        )
        return reranked + tail
    except Exception:
        return matches


def _cosine_similarity(vec_a: list[float] | None, vec_b: list[float] | None) -> float | None:
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return None
    dot = sum(a * b for a, b in zip(vec_a, vec_b, strict=False))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0 or norm_b == 0:
        return None
    return dot / (norm_a * norm_b)


def _doc_type_bonus(document: SemanticDocument) -> int:
    return {
        "resource_entity": 4,
        "memory_fact": 4,
        "known_path": 3,
        "artifact_record": 2,
        "project_file": 1,
        "memory_episode": 0,
    }.get(document.doc_type, 0)


def _tokenize(text: str) -> list[str]:
    tokens = [
        token.lower()
        for token in re.split(r"[^a-zA-Z0-9_.-]+", str(text or ""))
        if len(token.strip()) >= 2
    ]
    seen: set[str] = set()
    ordered: list[str] = []
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        ordered.append(token)
    return ordered


def format_semantic_match_summary(match: SemanticDocumentMatch) -> str:
    document = getattr(match, "document", None)
    doc_type = getattr(document, "doc_type", "semantic_document")
    title = getattr(document, "title", "")
    reasons = _human_rationales(match)
    reason_suffix = f" [{'; '.join(reasons)}]" if reasons else ""
    return f"{doc_type}: {title}{reason_suffix}"


def _rationale_codes_for_match(
    *,
    document: SemanticDocument,
    matched_terms: list[str],
    lexical_score: int | None,
    semantic_score: float | None,
) -> list[str]:
    codes: list[str] = []
    if matched_terms:
        codes.append("matched_terms")
    if document.doc_type == "memory_fact":
        codes.append("stable_fact")
    elif document.doc_type == "memory_episode":
        codes.append("historical_episode")
    elif document.doc_type == "resource_entity":
        codes.append("resource_entity")
    elif document.doc_type == "known_path":
        codes.append("known_path")
    elif document.doc_type == "artifact_record":
        codes.append("artifact_record")
    if lexical_score and lexical_score >= 12:
        codes.append("strong_lexical_match")
    if semantic_score is not None and semantic_score >= 0.75:
        codes.append("strong_semantic_match")
    elif semantic_score is not None and semantic_score >= 0.4:
        codes.append("semantic_support")
    return codes


def _human_rationales(match: SemanticDocumentMatch) -> list[str]:
    document = getattr(match, "document", None)
    doc_type = getattr(document, "doc_type", "")
    matched_terms = list(getattr(match, "matched_terms", []) or [])
    rationale_codes = list(getattr(match, "rationale_codes", []) or [])

    if not rationale_codes:
        if doc_type == "memory_fact":
            rationale_codes.append("stable_fact")
        elif doc_type == "memory_episode":
            rationale_codes.append("historical_episode")
        elif doc_type == "resource_entity":
            rationale_codes.append("resource_entity")
        elif doc_type == "known_path":
            rationale_codes.append("known_path")
        elif doc_type == "artifact_record":
            rationale_codes.append("artifact_record")

    reasons: list[str] = []
    if matched_terms:
        reasons.append("matched=" + ", ".join(matched_terms[:3]))
    if "stable_fact" in rationale_codes:
        reasons.append("stable fact")
    elif "historical_episode" in rationale_codes:
        reasons.append("historical episode")
    elif "resource_entity" in rationale_codes:
        reasons.append("resource entity")
    elif "known_path" in rationale_codes:
        reasons.append("known path")
    elif "artifact_record" in rationale_codes:
        reasons.append("artifact record")
    if "strong_semantic_match" in rationale_codes:
        reasons.append("strong semantic match")
    elif "semantic_support" in rationale_codes:
        reasons.append("semantic support")
    elif "strong_lexical_match" in rationale_codes:
        reasons.append("strong lexical match")
    return reasons


__all__ = [
    "ProjectSemanticCorpus",
    "SemanticDocumentMatch",
    "build_project_semantic_corpus",
    "format_semantic_match_summary",
    "query_semantic_documents",
    "query_project_semantic_corpus",
]
