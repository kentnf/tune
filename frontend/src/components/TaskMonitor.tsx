import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { ChevronDown, ChevronRight, PlayCircle, Trash2 } from 'lucide-react'
import type { ResourceWorkspaceRequest } from './DataBrowser'
import { PROJECT_TASK_PAGE_SIZE, useProjectTaskFeed, type ProjectTaskJob } from '../hooks/useProjectTaskFeed'
import { useLanguage } from '../i18n/LanguageContext'
import type { TranslationKey } from '../i18n/translations'
import {
  formatOperatorChatActionCta,
  formatOperatorChatActionHint,
  resolveOperatorChatActionCode,
} from '../lib/taskAttention'

type Job = ProjectTaskJob

interface SupervisorRecommendation {
  priority: number
  job_id: string
  job_name: string
  thread_id?: string
  incident_type: string
  severity: string
  owner: string
  diagnosis: string
  rollback_level?: string
  rollback_target: string
  safe_action?: string | null
  safe_action_eligibility?: {
    eligible?: boolean
    current_job_status?: string | null
    retryable_job_statuses?: string[] | null
    has_resolved_pending_signal?: boolean
    has_pending_request_reference?: boolean
    resolved_pending_types?: string[] | null
    pending_reference_types?: string[] | null
    blocking_reasons?: string[] | null
  } | null
  historical_policy?: {
    preferred_safe_action?: string | null
    support_count?: number | null
    total_matches?: number | null
    confidence?: string | null
    current_safe_action?: string | null
    current_supported_count?: number | null
    aligns_with_current?: boolean | null
    preferred_rollback_level?: string | null
    current_rollback_level?: string | null
    rollback_level_supported_count?: number | null
    rollback_level_aligns_with_current?: boolean | null
    preferred_rollback_target?: string | null
    current_rollback_target?: string | null
    rollback_target_supported_count?: number | null
    rollback_target_aligns_with_current?: boolean | null
  } | null
  auto_recoverable?: boolean
  auto_recovery_kind?: string | null
  recommended_action_confidence?: string | null
  recommended_action_basis?: string[] | null
  safe_action_note?: string | null
  historical_guidance?: string | null
  immediate_action: string
  why_now: string
  dossier_summary?: string
  recovery_playbook?: {
    goal?: string | null
    rollback_target?: string | null
    step_codes?: string[] | null
  } | null
}

interface SupervisorDossierDecision {
  decision_type: string
  created_at?: string | null
}

interface SupervisorDossierLog {
  stream?: string | null
  line?: string | null
  ts?: string | null
}

interface RuntimeDiagnostic {
  kind?: string | null
  request_type?: string | null
  request_id?: string | null
  request_status?: string | null
  resolved_at?: string | null
  stage?: string | null
  failure_kind?: string | null
  retryable?: boolean | null
  failed_packages?: string[] | null
  package_candidates?: Record<string, string[] | null> | null
  implicated_steps?: Array<{
    step_key?: string | null
    step_type?: string | null
    display_name?: string | null
    packages?: string[] | null
    matched_failed_packages?: string[] | null
  }> | null
}

interface AutoRecoveryEvent {
  source?: string | null
  issue_kind?: string | null
  safe_action?: string | null
  resulting_status?: string | null
  pending_types?: string | null
  line?: string | null
  ts?: string | null
}

interface TimelineEvent {
  ts?: string | null
  kind?: string | null
  source?: string | null
  category?: string | null
  result_kind?: string | null
  title?: string | null
  detail?: string | null
}

interface RollbackGuidance {
  level?: string | null
  target?: string | null
  reconfirmation_required?: boolean | null
  reason?: string | null
  historical_matches?: number | null
  historical_same_level_count?: number | null
  historical_same_target_count?: number | null
  summary?: string | null
}

type TimelineCategory = 'all' | 'step' | 'result' | 'recovery' | 'confirmation'

interface SupervisorDossierResourceNode {
  id: string
  kind?: string | null
  label?: string | null
  status?: string | null
  cause?: string | null
  source_type?: string | null
  organism?: string | null
  genome_build?: string | null
  candidate_count?: number | null
  candidate_preview?: string[] | null
  derived_from_count?: number | null
}

interface SupervisorDossierResourceCandidate {
  path: string
  organism?: string | null
  genome_build?: string | null
  source_type?: string | null
  confidence?: number | null
  recommended?: boolean | null
  rationale?: string | null
}

interface SupervisorDossierResourceSummary {
  id: string
  label?: string | null
  kind?: string | null
  status?: string | null
  cause?: string | null
  source_type?: string | null
  organism?: string | null
  genome_build?: string | null
  why_blocked?: string | null
  operator_hint?: string | null
  recommended_action?: string | null
  registry_key?: string | null
  workspace_section?: 'recognized' | 'registry' | 'files' | null
  candidate_choices?: SupervisorDossierResourceCandidate[] | null
  preferred_candidate?: SupervisorDossierResourceCandidate | null
  derived_from_preview?: string[] | null
}

interface SupervisorDossier {
  job_id: string
  summary?: string
  current_step?: {
    step_key?: string | null
    display_name?: string | null
  } | null
  impacted_step_keys?: string[]
  resource_graph?: {
    available?: boolean
    total_nodes?: number
    blocking_total?: number
    status_counts?: Record<string, number>
    blocking_kind_counts?: Record<string, number>
    blocking_cause_counts?: Record<string, number>
    blocking_nodes?: SupervisorDossierResourceNode[]
    blocking_summary?: SupervisorDossierResourceSummary[]
    dominant_blocker?: SupervisorDossierResourceSummary | null
  } | null
  recent_logs?: SupervisorDossierLog[]
  recent_decisions?: SupervisorDossierDecision[]
  pending_requests?: {
    active_type?: string | null
    auth_request_id?: string | null
    repair_request_id?: string | null
    has_payload?: boolean
    diagnostic_kinds?: string[]
    diagnostic_types?: string[]
    recent_authorizations?: Array<{
      id?: string | null
      status?: string | null
      command_type?: string | null
      requested_at?: string | null
      resolved_at?: string | null
    }>
    recent_repairs?: Array<{
      id?: string | null
      status?: string | null
      created_at?: string | null
      resolved_at?: string | null
    }>
  } | null
  runtime_diagnostics?: RuntimeDiagnostic[]
  auto_recovery_events?: AutoRecoveryEvent[]
  rollback_hint?: {
    suggested_level?: string | null
    reason?: string | null
  } | null
  execution_confirmation_overview?: ExecutionConfirmationOverview | null
  execution_ir_review?: ExecutionIrReviewItem[] | null
  execution_plan_delta?: ExecutionPlanDelta | null
  execution_plan_changes?: ExecutionPlanChangeItem[] | null
  similar_resolutions?: Array<{
    event_type?: string | null
    description?: string | null
    resolution?: string | null
    safe_action?: string | null
    user_contributed?: boolean
    created_at?: string | null
  }>
}

interface SupervisorReview {
  mode: 'llm' | 'heuristic'
  generated_at: string
  overview: string
  supervisor_message: string
  focus_summary?: {
    top_owner?: string | null
    top_incident_type?: string | null
    top_blocker_cause?: string | null
    high_confidence_total?: number | null
    auto_recoverable_total?: number | null
    user_wait_total?: number | null
    top_failure_layer?: string | null
    top_safe_action?: string | null
    top_rollback_level?: string | null
    top_rollback_target?: string | null
    top_historical_rollback_level?: string | null
    top_historical_rollback_alignment?: boolean | null
    top_historical_rollback_target?: string | null
    top_historical_rollback_target_alignment?: boolean | null
    primary_lane?: string | null
    lane_reason?: string | null
    next_best_operator_move?: string | null
    next_best_operator_reason?: string | null
    latest_auto_recovery_issue?: string | null
    latest_auto_recovery_action?: string | null
    latest_auto_recovery_status?: string | null
    latest_auto_recovery_pending_types?: string | null
    latest_auto_recovery_job_id?: string | null
  } | null
  project_playbook?: {
    goal?: string | null
    next_move?: string | null
    step_codes?: string[] | null
  } | null
  recommendations: SupervisorRecommendation[]
  dossiers?: SupervisorDossier[]
}

interface ExecutionPlanSummary {
  has_execution_ir?: boolean
  has_expanded_dag?: boolean
  node_count?: number
  group_count?: number
}

interface ExecutionConfirmationOverview {
  abstract_step_count?: number | null
  execution_ir_step_count?: number | null
  execution_group_count?: number | null
  unchanged_group_count?: number | null
  changed_group_count?: number | null
  added_group_count?: number | null
  per_sample_step_count?: number | null
  aggregate_step_count?: number | null
  global_step_count?: number | null
  fan_out_change_count?: number | null
  aggregate_change_count?: number | null
  auto_injected_change_count?: number | null
}

interface ExecutionIrReviewItem {
  step_key?: string | null
  step_type?: string | null
  display_name?: string | null
  description?: string | null
  scope?: string | null
  execution_kind?: string | null
  aggregation_mode?: string | null
  input_semantics?: string[] | null
  depends_on?: string[] | null
}

interface ExecutionPlanDeltaGroupItem {
  group_key?: string | null
  display_name?: string | null
  step_type?: string | null
  change_kinds?: string[] | null
}

interface ExecutionPlanDelta {
  abstract_step_count?: number | null
  execution_group_count?: number | null
  added_group_count?: number | null
  changed_group_count?: number | null
  unchanged_group_count?: number | null
  added_groups?: ExecutionPlanDeltaGroupItem[] | null
  changed_groups?: ExecutionPlanDeltaGroupItem[] | null
}

interface ExecutionPlanChangeItem {
  group_key?: string | null
  step_type?: string | null
  display_name?: string | null
  change_kinds?: string[] | null
  summary?: string | null
  depends_on?: string[] | null
  node_count?: number | null
  scope?: string | null
  fan_out_mode?: string | null
  aggregate_mode?: string | null
  auto_injected_reasons?: string[] | null
  auto_injected_cause?: string | null
}

interface ConfirmationPlanItem {
  step_key?: string | null
  step_type?: string | null
  display_name?: string | null
  name?: string | null
  description?: string | null
}

interface ConfirmationLayerItem {
  key: string
  label: TranslationKey
  stateLabel: TranslationKey
  tone: 'ready' | 'pending' | 'waiting' | 'missing'
}

function formatRuntimeDiagnostic(item: RuntimeDiagnostic, t: (k: TranslationKey) => string): string {
  const kind = item.kind || 'unknown'
  const requestType = item.request_type || 'request'
  const requestStatus = item.request_status ? ` (${item.request_status})` : ''
  const labelMap: Record<string, TranslationKey> = {
    resolved_pending_request: 'tasks_runtime_diag_resolved_pending_request',
    orphan_pending_request: 'tasks_runtime_diag_orphan_pending_request',
    environment_prepare_failed: 'tasks_runtime_diag_environment_prepare_failed',
  }
  const label = t(labelMap[kind] || 'tasks_runtime_diag_unknown')
  if (kind === 'environment_prepare_failed') {
    const detailParts: string[] = []
    if (item.stage) {
      detailParts.push(`stage=${item.stage}`)
    }
    const failedPackages = (item.failed_packages ?? []).filter(Boolean)
    if (failedPackages.length > 0) {
      detailParts.push(`packages=${failedPackages.join(', ')}`)
    }
    const candidateEntries = Object.entries(item.package_candidates ?? {})
      .map(([pkg, candidates]) => {
        const cleaned = (candidates ?? []).filter(Boolean)
        if (!pkg || cleaned.length === 0) return null
        const alternatives = cleaned.filter((candidate) => candidate !== pkg)
        if (alternatives.length === 0) return null
        return `${pkg} -> ${alternatives.join(', ')}`
      })
      .filter(Boolean) as string[]
    if (candidateEntries.length > 0) {
      detailParts.push(`candidates=${candidateEntries.join('; ')}`)
    }
    const implicatedSteps = (item.implicated_steps ?? [])
      .map((step) => step.display_name || step.step_key || '')
      .filter(Boolean)
    if (implicatedSteps.length > 0) {
      detailParts.push(`steps=${implicatedSteps.join(', ')}`)
    }
    return detailParts.length > 0 ? `${label}: ${detailParts.join(' · ')}` : label
  }
  return `${label}: ${requestType}${requestStatus}`
}

function buildConfirmationLayers(
  phase: 'abstract' | 'execution' | null,
  summary: ExecutionPlanSummary | null,
): ConfirmationLayerItem[] {
  if (phase === 'execution') {
    return [
      {
        key: 'abstract_plan',
        label: 'tasks_confirmation_layer_abstract',
        stateLabel: 'tasks_confirmation_state_ready',
        tone: 'ready',
      },
      {
        key: 'execution_ir',
        label: 'tasks_confirmation_layer_execution_ir',
        stateLabel: summary?.has_execution_ir
          ? 'tasks_confirmation_state_ready'
          : 'tasks_confirmation_state_missing',
        tone: summary?.has_execution_ir ? 'ready' : 'missing',
      },
      {
        key: 'expanded_dag',
        label: 'tasks_confirmation_layer_expanded_dag',
        stateLabel: summary?.has_expanded_dag
          ? 'tasks_confirmation_state_pending'
          : 'tasks_confirmation_state_missing',
        tone: summary?.has_expanded_dag ? 'pending' : 'missing',
      },
    ]
  }
  return [
    {
      key: 'abstract_plan',
      label: 'tasks_confirmation_layer_abstract',
      stateLabel: 'tasks_confirmation_state_pending',
      tone: 'pending',
    },
    {
      key: 'execution_ir',
      label: 'tasks_confirmation_layer_execution_ir',
      stateLabel: 'tasks_confirmation_state_waiting',
      tone: 'waiting',
    },
    {
      key: 'expanded_dag',
      label: 'tasks_confirmation_layer_expanded_dag',
      stateLabel: 'tasks_confirmation_state_waiting',
      tone: 'waiting',
    },
  ]
}

function formatAttentionReasonLabel(
  reason: 'authorization' | 'repair' | 'confirmation' | 'clarification' | 'rollback_review' | 'warning',
  t: (key: TranslationKey) => string,
): string {
  switch (reason) {
    case 'authorization':
      return t('status_waiting_for_authorization')
    case 'repair':
      return t('status_waiting_for_repair')
    case 'confirmation':
      return t('status_awaiting_plan_confirmation')
    case 'clarification':
      return t('tasks_attention_reason_clarification')
    case 'rollback_review':
      return t('tasks_attention_reason_rollback_review')
    case 'warning':
    default:
      return t('tasks_tray_warning')
  }
}

function pendingOperatorPriority(reason: 'confirmation' | 'clarification' | 'rollback_review' | string): number {
  if (reason === 'rollback_review') return 0
  if (reason === 'confirmation') return 1
  if (reason === 'clarification') return 2
  return 3
}

interface BindingMatchMetadata {
  candidate_source?: string
  score?: number
  reason_codes?: string[]
  source_step_key?: string
  source_step_type?: string
  source_slot_name?: string
  artifact_role?: string
  expected_roles?: string[]
  lineage?: {
    sample_id?: string | null
    experiment_id?: string | null
    sample_name?: string | null
    read_number?: number | null
  }
}

interface JobBinding {
  id: string
  slot_name: string
  source_type?: string | null
  source_ref?: string | null
  resolved_path?: string | null
  status: string
  match_metadata?: BindingMatchMetadata | null
}

interface JobBindingStep {
  step_id: string
  step_key?: string | null
  step_type?: string | null
  display_name?: string | null
  status?: string | null
  bindings: JobBinding[]
}

interface PendingInteractionPayload {
  auth_request_id?: string | null
  repair_request_id?: string | null
  command?: string | null
  command_type?: string | null
  step_key?: string | null
  failed_command?: string | null
  stderr_excerpt?: string | null
  prompt_text?: string | null
  issues?: Array<{
    title?: string | null
    description?: string | null
  }>
}

interface JobBindingResponse {
  job_status?: string
  error_message?: string | null
  pending_interaction_type?: string | null
  pending_interaction_payload?: PendingInteractionPayload | null
  runtime_diagnostics?: RuntimeDiagnostic[]
  rollback_guidance?: RollbackGuidance | null
  auto_recovery_events?: AutoRecoveryEvent[]
  timeline?: TimelineEvent[]
  confirmation_phase?: 'abstract' | 'execution' | null
  confirmation_plan?: ConfirmationPlanItem[]
  execution_plan_summary?: ExecutionPlanSummary | null
  execution_confirmation_overview?: ExecutionConfirmationOverview | null
  execution_ir_review?: ExecutionIrReviewItem[] | null
  execution_plan_delta?: ExecutionPlanDelta | null
  execution_plan_changes?: ExecutionPlanChangeItem[] | null
  steps?: JobBindingStep[]
}

function formatExecutionPlanChangeSummary(
  item: ExecutionPlanChangeItem,
  t: (k: TranslationKey) => string,
): string {
  const changeKinds = new Set((item.change_kinds ?? []).filter(Boolean))
  const detailParts: string[] = []

  if (changeKinds.has('fan_out')) {
    detailParts.push(
      (
        item.fan_out_mode === 'per_sample'
          ? t('tasks_confirmation_change_detail_fan_out_per_sample')
          : t('tasks_confirmation_change_detail_fan_out')
      ).replace('{count}', String(item.node_count ?? 0)),
    )
  }

  if (changeKinds.has('aggregate')) {
    const sources = (item.depends_on ?? []).filter(Boolean)
    detailParts.push(
      sources.length > 0
        ? t('tasks_confirmation_change_detail_aggregate_with_sources').replace('{sources}', sources.join(', '))
        : t('tasks_confirmation_change_detail_aggregate'),
    )
  }

  if (changeKinds.has('auto_injected')) {
    const causeMap: Record<string, TranslationKey> = {
      missing_hisat2_index: 'tasks_confirmation_change_detail_auto_injected_missing_hisat2_index',
      missing_star_genome: 'tasks_confirmation_change_detail_auto_injected_missing_star_genome',
      derivable_hisat2_index: 'tasks_confirmation_change_detail_auto_injected_derivable_hisat2_index',
      derivable_star_genome: 'tasks_confirmation_change_detail_auto_injected_derivable_star_genome',
      stale_derived_resource: 'tasks_confirmation_change_detail_auto_injected_stale_derived_resource',
    }
    if (item.auto_injected_cause && causeMap[item.auto_injected_cause]) {
      detailParts.push(t(causeMap[item.auto_injected_cause]))
      return detailParts.join(' · ') || item.summary || ''
    }
    const sourceLabels = (item.auto_injected_reasons ?? [])
      .filter(Boolean)
      .map((reason) => (
        reason === 'preflight'
          ? t('tasks_confirmation_change_source_preflight')
          : reason === 'resource_readiness'
            ? t('tasks_confirmation_change_source_resource_readiness')
            : reason
      ))
    detailParts.push(
      sourceLabels.length > 0
        ? t('tasks_confirmation_change_detail_auto_injected_with_sources').replace('{sources}', sourceLabels.join(', '))
        : t('tasks_confirmation_change_detail_auto_injected'),
    )
  }

  return detailParts.join(' · ') || item.summary || ''
}

function formatExecutionPlanChangeKind(
  kind: string,
  t: (k: TranslationKey) => string,
): string {
  const keyMap: Record<string, TranslationKey> = {
    fan_out: 'tasks_confirmation_change_fan_out',
    aggregate: 'tasks_confirmation_change_aggregate',
    auto_injected: 'tasks_confirmation_change_auto_injected',
  }
  return t(keyMap[kind] || 'tasks_confirmation_change_auto_injected')
}

function buildExecutionPlanDeltaStatusMap(delta: ExecutionPlanDelta | null | undefined): Record<string, 'added' | 'changed'> {
  const statusMap: Record<string, 'added' | 'changed'> = {}
  for (const item of delta?.added_groups ?? []) {
    if (item.group_key) statusMap[item.group_key] = 'added'
  }
  for (const item of delta?.changed_groups ?? []) {
    if (item.group_key && !statusMap[item.group_key]) statusMap[item.group_key] = 'changed'
  }
  return statusMap
}

function formatTaskStatusLabel(status: string | null | undefined, t: (k: TranslationKey) => string): string {
  const keyMap: Record<string, TranslationKey> = {
    running: 'status_running',
    completed: 'status_completed',
    failed: 'status_failed',
    cancelled: 'status_cancelled',
    interrupted: 'status_interrupted',
    queued: 'status_queued',
    binding_required: 'status_binding_required',
    resource_clarification_required: 'status_binding_required',
    waiting_for_authorization: 'status_waiting_for_authorization',
    waiting_for_repair: 'status_waiting_for_repair',
    awaiting_plan_confirmation: 'status_awaiting_plan_confirmation',
  }
  const key = keyMap[status || '']
  return key ? t(key) : (status || 'unknown')
}

function formatSafeActionLabel(action: string | null | undefined, t: (k: TranslationKey) => string): string {
  const keyMap: Record<string, TranslationKey> = {
    step_reenter: 'tasks_supervisor_retry_from_step',
    refresh_execution_graph: 'tasks_supervisor_refresh_execution_graph',
    refresh_execution_plan: 'tasks_supervisor_refresh_execution_plan',
    revalidate_abstract_plan: 'tasks_supervisor_revalidate_abstract_plan',
    normalize_orphan_pending_state: 'tasks_supervisor_normalize_orphan_pending_state',
    normalize_terminal_state: 'tasks_supervisor_normalize_terminal_state',
    retry_resume_chain: 'tasks_supervisor_retry_resume_chain',
  }
  const key = keyMap[action || '']
  return key ? t(key) : (action || 'unknown')
}

function formatAutoRecoveryIssueKind(issueKind: string | null | undefined, t: (k: TranslationKey) => string): string {
  const keyMap: Record<string, TranslationKey> = {
    resume_failed: 'tasks_auto_recovery_issue_resume_failed',
    orphan_pending_request: 'tasks_auto_recovery_issue_orphan_pending_request',
    job_status_mismatch: 'tasks_auto_recovery_issue_job_status_mismatch',
  }
  return t(keyMap[issueKind || ''] || 'tasks_auto_recovery_issue_unknown')
}

function formatAutoRecoveryEvent(item: AutoRecoveryEvent, t: (k: TranslationKey) => string): string {
  const safeAction = formatSafeActionLabel(item.safe_action, t)
  const issueKind = formatAutoRecoveryIssueKind(item.issue_kind, t)
  const resultingStatus = formatTaskStatusLabel(item.resulting_status, t)
  const pendingTypes = item.pending_types ? ` · pending=${item.pending_types}` : ''
  return t('tasks_auto_recovery_entry')
    .replace('{issue}', issueKind)
    .replace('{action}', safeAction)
    .replace('{status}', resultingStatus) + pendingTypes
}

function formatAutoRecoveryKind(kind: string | null | undefined, t: (k: TranslationKey) => string): string {
  const keyMap: Record<string, TranslationKey> = {
    metadata_normalization: 'tasks_supervisor_auto_recovery_kind_metadata_normalization',
  }
  return t(keyMap[kind || ''] || 'tasks_supervisor_auto_recovery_kind_unknown')
}

function formatFocusSummaryAutoRecovery(
  focusSummary: SupervisorReview['focus_summary'] | null | undefined,
  t: (k: TranslationKey) => string,
): string | null {
  if (!focusSummary?.latest_auto_recovery_issue || !focusSummary.latest_auto_recovery_action || !focusSummary.latest_auto_recovery_status) {
    return null
  }
  return formatAutoRecoveryEvent(
    {
      source: 'watchdog',
      issue_kind: focusSummary.latest_auto_recovery_issue,
      safe_action: focusSummary.latest_auto_recovery_action,
      resulting_status: focusSummary.latest_auto_recovery_status,
      pending_types: focusSummary.latest_auto_recovery_pending_types ?? undefined,
    },
    t,
  )
}

function formatResourceBlocker(item: SupervisorDossierResourceNode): string {
  const label = item.label || item.kind || item.id
  const parts = [
    item.status || null,
    item.cause ? `cause=${item.cause}` : null,
    item.kind || null,
    item.source_type || null,
    item.organism || null,
    item.genome_build || null,
    typeof item.candidate_count === 'number' && item.candidate_count > 1 ? `candidates=${item.candidate_count}` : null,
    item.candidate_preview && item.candidate_preview.length > 0
      ? `preview=${item.candidate_preview.join('|')}`
      : null,
    typeof item.derived_from_count === 'number' && item.derived_from_count > 0 ? `derived_from=${item.derived_from_count}` : null,
  ].filter(Boolean)
  return parts.length > 0 ? `${label} [${parts.join(' · ')}]` : label
}

function formatResourceCandidate(item: SupervisorDossierResourceCandidate): string {
  const parts = [
    item.path,
    typeof item.confidence === 'number' ? `confidence=${item.confidence.toFixed(2)}` : null,
    item.source_type ? `source=${item.source_type}` : null,
    item.organism || null,
    item.genome_build || null,
    item.rationale || null,
  ].filter(Boolean)
  return parts.join(' · ')
}

function buildResourceWorkspaceRequest(item: SupervisorDossierResourceSummary): ResourceWorkspaceRequest {
  const section = item.workspace_section || 'recognized'
  if (section === 'files') {
    return {
      nonce: Date.now(),
      tab: 'files',
      description: item.label || item.kind || item.id,
    }
  }
  return {
    nonce: Date.now(),
    tab: 'project-info',
    focusSection: section === 'registry' ? 'registry' : 'recognized',
    key: item.registry_key || undefined,
    path: item.preferred_candidate?.path || undefined,
    description: item.label || item.kind || item.id,
  }
}

function buildRegistryWorkspaceRequest(item: SupervisorDossierResourceSummary): ResourceWorkspaceRequest | null {
  if (!item.registry_key) return null
  return {
    nonce: Date.now() + 1,
    tab: 'project-info',
    focusSection: 'registry',
    key: item.registry_key,
    path: item.preferred_candidate?.path || undefined,
    description: item.label || item.kind || item.id,
  }
}

function formatEligibilityBool(value: boolean | null | undefined, t: (k: TranslationKey) => string): string {
  if (value === true) return t('tasks_supervisor_eligibility_yes')
  if (value === false) return t('tasks_supervisor_eligibility_no')
  return t('tasks_supervisor_eligibility_unknown')
}

function formatResumeRetryBlocker(code: string, t: (k: TranslationKey) => string): string {
  const keyMap: Record<string, TranslationKey> = {
    job_status_not_retryable: 'tasks_supervisor_resume_retry_blocker_job_status_not_retryable',
    pending_request_reference_missing: 'tasks_supervisor_resume_retry_blocker_pending_reference_missing',
    resolved_pending_signal_missing: 'tasks_supervisor_resume_retry_blocker_resolved_signal_missing',
    pending_request_type_mismatch: 'tasks_supervisor_resume_retry_blocker_pending_request_type_mismatch',
  }
  return t(keyMap[code] || 'tasks_supervisor_resume_retry_blocker_unknown')
}

function formatHistoricalConfidence(value: string | null | undefined, t: (k: TranslationKey) => string): string {
  const keyMap: Record<string, TranslationKey> = {
    high: 'tasks_supervisor_historical_confidence_high',
    medium: 'tasks_supervisor_historical_confidence_medium',
    low: 'tasks_supervisor_historical_confidence_low',
  }
  return t(keyMap[value || ''] || 'tasks_supervisor_eligibility_unknown')
}

function formatHistoricalAlignment(value: boolean | null | undefined, t: (k: TranslationKey) => string): string {
  if (value === true) return t('tasks_supervisor_historical_alignment_aligned')
  if (value === false) return t('tasks_supervisor_historical_alignment_diverges')
  return t('tasks_supervisor_historical_alignment_unknown')
}

function formatRecommendationConfidence(value: string | null | undefined, t: (k: TranslationKey) => string): string {
  const keyMap: Record<string, TranslationKey> = {
    high: 'tasks_supervisor_historical_confidence_high',
    medium: 'tasks_supervisor_historical_confidence_medium',
    low: 'tasks_supervisor_historical_confidence_low',
  }
  return t(keyMap[value || ''] || 'tasks_supervisor_eligibility_unknown')
}

function formatRecommendationBasis(code: string, t: (k: TranslationKey) => string): string {
  const keyMap: Record<string, TranslationKey> = {
    no_safe_action: 'tasks_supervisor_recommendation_basis_no_safe_action',
    auto_recoverable: 'tasks_supervisor_recommendation_basis_auto_recoverable',
    eligibility_passed: 'tasks_supervisor_recommendation_basis_eligibility_passed',
    eligibility_blocked: 'tasks_supervisor_recommendation_basis_eligibility_blocked',
    historical_alignment: 'tasks_supervisor_recommendation_basis_historical_alignment',
    historical_divergence: 'tasks_supervisor_recommendation_basis_historical_divergence',
    historical_rollback_alignment: 'tasks_supervisor_recommendation_basis_historical_rollback_alignment',
    historical_rollback_divergence: 'tasks_supervisor_recommendation_basis_historical_rollback_divergence',
    historical_target_alignment: 'tasks_supervisor_recommendation_basis_historical_target_alignment',
    historical_target_divergence: 'tasks_supervisor_recommendation_basis_historical_target_divergence',
    historical_confidence_high: 'tasks_supervisor_recommendation_basis_historical_confidence_high',
    historical_confidence_medium: 'tasks_supervisor_recommendation_basis_historical_confidence_medium',
    historical_confidence_low: 'tasks_supervisor_recommendation_basis_historical_confidence_low',
  }
  return t(keyMap[code] || 'tasks_supervisor_resume_retry_blocker_unknown')
}

function recommendationConfidenceBadgeClass(value: string | null | undefined): string {
  if (value === 'high') return 'bg-emerald-500/15 text-emerald-300'
  if (value === 'medium') return 'bg-amber-500/15 text-amber-300'
  return 'bg-slate-500/15 text-slate-300'
}

function humanizeFocusToken(value: string | null | undefined): string {
  const raw = String(value || '').trim()
  if (!raw) return ''
  return raw.split('_').join(' ')
}

function formatFocusOwner(value: string | null | undefined, t: (k: TranslationKey) => string): string {
  if (value === 'user') return t('tasks_incident_owner_user')
  if (value === 'system') return t('tasks_incident_owner_system')
  return humanizeFocusToken(value)
}

function formatPrimaryLane(value: string | null | undefined, t: (k: TranslationKey) => string): string {
  const keyMap: Record<string, TranslationKey> = {
    operator_review: 'tasks_supervisor_lane_operator_review',
    confirmation_gates: 'tasks_supervisor_lane_confirmation_gates',
    user_intervention: 'tasks_supervisor_lane_user_intervention',
    resource_readiness: 'tasks_supervisor_lane_resource_readiness',
    environment_readiness: 'tasks_supervisor_lane_environment_readiness',
    runtime_recovery: 'tasks_supervisor_lane_runtime_recovery',
    rollback_review: 'tasks_supervisor_lane_rollback_review',
  }
  return t(keyMap[value || ''] || 'tasks_supervisor_lane_unknown') || humanizeFocusToken(value)
}

function formatNextBestMove(value: string | null | undefined, t: (k: TranslationKey) => string): string {
  const keyMap: Record<string, TranslationKey> = {
    inspect_top_incident: 'tasks_supervisor_move_inspect_top_incident',
    review_confirmation_gate: 'tasks_supervisor_move_review_confirmation_gate',
    resolve_authorization_request: 'tasks_supervisor_move_resolve_authorization_request',
    resolve_repair_request: 'tasks_supervisor_move_resolve_repair_request',
    resolve_resource_clarification: 'tasks_supervisor_move_resolve_resource_clarification',
    resolve_resource_readiness: 'tasks_supervisor_move_resolve_resource_readiness',
    resolve_resource_registration_mismatch: 'tasks_supervisor_move_resolve_resource_registration_mismatch',
    register_primary_resource: 'tasks_supervisor_move_register_primary_resource',
    resolve_ambiguous_resource_candidates: 'tasks_supervisor_move_resolve_ambiguous_resource_candidates',
    review_stale_resource_decision: 'tasks_supervisor_move_review_stale_resource_decision',
    refresh_stale_derived_resource: 'tasks_supervisor_move_refresh_stale_derived_resource',
    restore_missing_runtime_resource: 'tasks_supervisor_move_restore_missing_runtime_resource',
    inspect_environment_failure: 'tasks_supervisor_move_inspect_environment_failure',
    apply_runtime_recovery: 'tasks_supervisor_move_apply_runtime_recovery',
    review_rollback_scope: 'tasks_supervisor_move_review_rollback_scope',
  }
  return t(keyMap[value || ''] || 'tasks_supervisor_move_unknown') || humanizeFocusToken(value)
}

function formatFocusCause(value: string | null | undefined, t: (k: TranslationKey) => string): string {
  const keyMap: Record<string, TranslationKey> = {
    missing_primary_resource: 'tasks_supervisor_cause_missing_primary_resource',
    ambiguous_candidates: 'tasks_supervisor_cause_ambiguous_candidates',
    registered_path_mismatch: 'tasks_supervisor_cause_registered_path_mismatch',
    stale_resource_decision: 'tasks_supervisor_cause_stale_resource_decision',
    stale_derived_resource: 'tasks_supervisor_cause_stale_derived_resource',
    missing_runtime_resource: 'tasks_supervisor_cause_missing_runtime_resource',
  }
  return t(keyMap[value || ''] || 'tasks_supervisor_cause_unknown') || humanizeFocusToken(value)
}

function formatFailureLayer(value: string | null | undefined, t: (k: TranslationKey) => string): string {
  const keyMap: Record<string, TranslationKey> = {
    abstract_plan: 'tasks_supervisor_failure_layer_abstract_plan',
    execution_ir: 'tasks_supervisor_failure_layer_execution_ir',
    expanded_dag: 'tasks_supervisor_failure_layer_expanded_dag',
    resource_binding: 'tasks_supervisor_failure_layer_resource_binding',
    step_execution: 'tasks_supervisor_failure_layer_step_execution',
  }
  return t(keyMap[value || ''] || 'tasks_supervisor_failure_layer_unknown') || humanizeFocusToken(value)
}

function formatRollbackTarget(value: string | null | undefined): string {
  const raw = String(value || '').trim()
  if (!raw) return ''
  return humanizeFocusToken(raw)
}

function formatPendingTypeLabel(value: string | null | undefined, t: (k: TranslationKey) => string): string {
  if (value === 'authorization') return t('tasks_tray_metric_authorization')
  if (value === 'repair') return t('tasks_tray_metric_repair')
  return humanizeFocusToken(value)
}

function formatRequestStatusLabel(value: string | null | undefined): string {
  return humanizeFocusToken(value) || 'unknown'
}

function formatTimelineTimestamp(ts: string | null | undefined, lang: 'en' | 'zh'): string {
  if (!ts) return ''
  const date = new Date(ts)
  if (Number.isNaN(date.getTime())) return ts
  return date.toLocaleString(lang === 'zh' ? 'zh-CN' : 'en-US', {
    hour12: false,
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function classifyTimelineEvent(item: TimelineEvent): TimelineCategory {
  const category = String(item.category || '').trim().toLowerCase()
  if (category === 'step' || category === 'result' || category === 'recovery' || category === 'confirmation') {
    return category
  }
  const raw = `${item.kind || ''} ${item.source || ''} ${item.title || ''}`.toLowerCase()
  if (
    raw.includes('artifact') ||
    raw.includes('result') ||
    raw.includes('output') ||
    raw.includes('complete')
  ) {
    return 'result'
  }
  if (
    raw.includes('recovery') ||
    raw.includes('repair') ||
    raw.includes('watchdog') ||
    raw.includes('supervisor') ||
    raw.includes('incident')
  ) {
    return 'recovery'
  }
  if (
    raw.includes('confirm') ||
    raw.includes('authorization') ||
    raw.includes('decision') ||
    raw.includes('approval') ||
    raw.includes('clarification')
  ) {
    return 'confirmation'
  }
  if (
    raw.includes('step') ||
    raw.includes('run') ||
    raw.includes('job') ||
    raw.includes('queue') ||
    raw.includes('binding')
  ) {
    return 'step'
  }
  return 'step'
}

function timelineCategoryLabel(category: TimelineCategory, t: (k: TranslationKey) => string): string {
  const keyMap: Record<TimelineCategory, TranslationKey> = {
    all: 'tasks_timeline_filter_all',
    step: 'tasks_timeline_filter_step',
    result: 'tasks_timeline_filter_result',
    recovery: 'tasks_timeline_filter_recovery',
    confirmation: 'tasks_timeline_filter_confirmation',
  }
  return t(keyMap[category])
}

function timelineCategoryBadgeClass(category: TimelineCategory): string {
  const classMap: Record<TimelineCategory, string> = {
    all: 'border-border-subtle bg-surface-raised/70 text-text-muted',
    step: 'border-slate-500/20 bg-slate-500/10 text-slate-200',
    result: 'border-emerald-500/20 bg-emerald-500/10 text-emerald-200',
    recovery: 'border-amber-500/20 bg-amber-500/10 text-amber-200',
    confirmation: 'border-sky-500/20 bg-sky-500/10 text-sky-200',
  }
  return classMap[category]
}

function timelineCardClass(category: TimelineCategory): string {
  const classMap: Record<TimelineCategory, string> = {
    all: 'border-border-subtle bg-surface-raised/70',
    step: 'border-border-subtle bg-surface-raised/70',
    result: 'border-emerald-500/20 bg-emerald-500/8',
    recovery: 'border-amber-500/20 bg-amber-500/8',
    confirmation: 'border-sky-500/20 bg-sky-500/8',
  }
  return classMap[category]
}

interface Props {
  compact?: boolean
  projectId?: string | null
  autoSelectJobId?: string | null
  onAutoSelectConsumed?: () => void
  onOpenThread?: (threadId: string | null, jobId?: string | null) => void
  onOpenResourceWorkspace?: (request: ResourceWorkspaceRequest) => void
  onOpenSettingsDiagnostics?: () => void
}

const TERMINAL_STATUSES = new Set(['completed', 'failed', 'cancelled', 'interrupted'])
const PAGE_SIZE = PROJECT_TASK_PAGE_SIZE

function StatusBadge({ status }: { status: string }) {
  const { t } = useLanguage()
  const styles: Record<string, string> = {
    running:          'bg-indigo-500/15 text-indigo-400',
    completed:        'bg-emerald-500/12 text-emerald-400',
    failed:           'bg-red-500/12 text-red-400',
    cancelled:        'bg-amber-500/12 text-amber-400',
    interrupted:      'bg-amber-500/12 text-amber-400',
    queued:           'bg-surface-overlay text-text-muted',
    binding_required: 'bg-orange-500/15 text-orange-400',
    resource_clarification_required: 'bg-orange-500/15 text-orange-400',
    waiting_for_authorization: 'bg-sky-500/15 text-sky-300',
    waiting_for_repair: 'bg-rose-500/15 text-rose-300',
    awaiting_plan_confirmation: 'bg-amber-500/15 text-amber-300',
  }
  const labelMap: Record<string, string> = {
    running:          t('status_running'),
    completed:        t('status_completed'),
    failed:           t('status_failed'),
    cancelled:        t('status_cancelled'),
    interrupted:      t('status_interrupted'),
    queued:           t('status_queued'),
    binding_required: t('status_binding_required'),
    resource_clarification_required: t('status_binding_required'),
    waiting_for_authorization: t('status_waiting_for_authorization'),
    waiting_for_repair: t('status_waiting_for_repair'),
    awaiting_plan_confirmation: t('status_awaiting_plan_confirmation'),
  }
  return (
    <span className={`rounded-full px-2.5 py-0.5 text-xs font-medium ${styles[status] ?? 'bg-surface-overlay text-text-muted'}`}>
      {labelMap[status] ?? status}
    </span>
  )
}

function timeAgo(dateStr: string, t: (k: TranslationKey) => string): string {
  const diff = Date.now() - new Date(dateStr).getTime()
  const mins = Math.floor(diff / 60000)
  if (mins < 1) return t('time_just_now')
  if (mins < 60) return `${mins}${t('time_ago_m')}`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}${t('time_ago_h')}`
  return `${Math.floor(hrs / 24)}${t('time_ago_d')}`
}

function formatBindingSource(sourceType: string | null | undefined, lang: 'en' | 'zh'): string {
  const labels: Record<string, { en: string; zh: string }> = {
    artifact_record: { en: 'upstream artifact', zh: '上游步骤产物' },
    filerun: { en: 'experiment-linked reads', zh: '实验关联 reads' },
    known_path: { en: 'registered resource', zh: '已注册资源' },
    project_file: { en: 'project file', zh: '项目文件' },
    step_output: { en: 'filesystem fallback', zh: '文件系统回退匹配' },
    user_provided: { en: 'manual override', zh: '手动覆盖' },
  }
  if (!sourceType) return lang === 'zh' ? '未知来源' : 'unknown source'
  return labels[sourceType]?.[lang] ?? sourceType
}

function formatReasonCode(reasonCode: string, lang: 'en' | 'zh'): string {
  const labels: Record<string, { en: string; zh: string }> = {
    role_exact: { en: 'exact semantic role match', zh: '语义角色精确匹配' },
    role_compatible: { en: 'compatible semantic role', zh: '语义角色兼容' },
    slot_name_exact: { en: 'same slot semantics', zh: 'slot 语义直接匹配' },
    file_type_match: { en: 'file type matched', zh: '文件类型匹配' },
    sample_match: { en: 'same sample lineage', zh: '样本 lineage 一致' },
    experiment_match: { en: 'same experiment lineage', zh: '实验 lineage 一致' },
    read_number_match: { en: 'same read direction', zh: 'read 方向一致' },
    dependency_proximity: { en: 'closer upstream dependency', zh: '更近的上游依赖' },
    source_artifact_record: { en: 'preferred upstream artifact source', zh: '优先使用上游产物记录' },
    source_filerun: { en: 'preferred experiment-linked source', zh: '优先使用实验关联来源' },
    source_known_path: { en: 'explicit registered resource', zh: '显式注册资源' },
    source_project_file: { en: 'project file fallback', zh: '项目文件回退候选' },
  }
  return labels[reasonCode]?.[lang] ?? reasonCode
}

function formatRollbackLevel(level: string | undefined, t: (k: TranslationKey) => string): string {
  const keyMap: Record<string, TranslationKey> = {
    step: 'tasks_supervisor_level_step',
    dag: 'tasks_supervisor_level_dag',
    execution_ir: 'tasks_supervisor_level_execution_ir',
    abstract_plan: 'tasks_supervisor_level_abstract_plan',
  }
  const key = keyMap[level || ''] ?? 'tasks_supervisor_level_step'
  return t(key)
}

function formatSupervisorAction(action: string | undefined, t: (k: TranslationKey) => string): string {
  const key = `tasks_incident_action_${action || 'inspect_task'}` as TranslationKey
  const label = t(key)
  return label || action || 'inspect_task'
}

function formatSupervisorPlaybookStep(code: string, t: (k: TranslationKey) => string): string {
  if (code === 'open_task') return t('tasks_supervisor_open_task')
  if (code === 'open_chat') return t('tasks_supervisor_open_chat')
  if (code === 'resume_job') return t('tasks_supervisor_resume_job')
  if (code === 'review_historical_policy') return t('tasks_incident_action_review_historical_policy')
  if (code === 'apply_safe_action') return t('tasks_supervisor_playbook_apply_safe_action')
  if (code === 'recheck_task_state') return t('tasks_supervisor_playbook_recheck_task_state')
  return formatSupervisorAction(code, t)
}

function safeActionNextStatus(recommendation: SupervisorRecommendation): string | undefined {
  if (recommendation.safe_action === 'step_reenter') return 'queued'
  if (recommendation.safe_action === 'normalize_orphan_pending_state') return 'interrupted'
  if (recommendation.safe_action === 'normalize_terminal_state') return 'completed'
  if (recommendation.safe_action === 'retry_resume_chain') {
    return recommendation.safe_action_eligibility?.current_job_status || 'interrupted'
  }
  return undefined
}

function formatStatusList(
  values: Array<string | null | undefined> | null | undefined,
  t: (k: TranslationKey) => string,
): string {
  const items = (values || [])
    .map((item) => formatTaskStatusLabel(item, t))
    .filter(Boolean)
  return items.join(', ')
}

function formatSimilarResolution(item: NonNullable<SupervisorDossier['similar_resolutions']>[number], t: (k: TranslationKey) => string): string {
  if (item.safe_action) return formatSafeActionLabel(item.safe_action, t)
  if (item.description) return item.description
  if (item.resolution) return item.resolution
  if (item.event_type) return humanizeFocusToken(item.event_type)
  return 'memory'
}

function splitIssueText(errorMessage: string | null | undefined): string[] {
  if (!errorMessage) return []
  return errorMessage
    .split(/;\s+/)
    .map((item) => item.trim())
    .filter(Boolean)
}

function CommandPreview({
  command,
  className = '',
}: {
  command: string
  className?: string
}) {
  return (
    <div
      className={`rounded-md border border-border-subtle bg-surface-raised/80 p-2 text-xs font-mono text-text-primary whitespace-pre ${className}`}
      style={{ maxHeight: 224, overflowY: 'auto', overflowX: 'auto' }}
    >
      {command}
    </div>
  )
}

function AuthorizationPromptCard({
  title,
  subtitle,
  command,
  onAuthorize,
  onReject,
  authActionLoading,
  actionRow,
}: {
  title: string
  subtitle?: string | null
  command: string
  onAuthorize: () => void
  onReject: () => void
  authActionLoading: string | null
  actionRow?: ReactNode
}) {
  const { t } = useLanguage()

  return (
    <div className="rounded-lg border border-sky-500/20 bg-sky-500/8 p-3">
      <div className="text-[11px] font-semibold uppercase tracking-wide text-sky-300 mb-2">
        {title}
      </div>
      {subtitle && (
        <div className="text-xs text-text-muted mb-2">
          {subtitle}
        </div>
      )}
      <CommandPreview command={command} />
      <div className="mt-3 flex flex-wrap gap-2">
        <button
          type="button"
          onClick={onAuthorize}
          disabled={authActionLoading !== null}
          className="px-3 py-1.5 rounded-lg bg-emerald-700/70 hover:bg-emerald-600/70 text-xs text-white disabled:opacity-50"
        >
          {authActionLoading === 'approved' ? '…' : t('auth_authorize')}
        </button>
        <button
          type="button"
          onClick={onReject}
          disabled={authActionLoading !== null}
          className="px-3 py-1.5 rounded-lg bg-red-700/70 hover:bg-red-600/70 text-xs text-white disabled:opacity-50"
        >
          {authActionLoading === 'rejected' ? '…' : t('auth_reject')}
        </button>
      </div>
      {actionRow && (
        <div className="mt-3 flex flex-wrap gap-2">
          {actionRow}
        </div>
      )}
    </div>
  )
}

function RepairPromptCard({
  title,
  subtitle,
  failedCommand,
  stderrExcerpt,
  repairCommand,
  onRepairCommandChange,
  onSendRetry,
  onRetryOriginal,
  onCancelJob,
  repairActionLoading,
  actionRow,
}: {
  title: string
  subtitle?: string | null
  failedCommand?: string | null
  stderrExcerpt?: string | null
  repairCommand: string
  onRepairCommandChange: (value: string) => void
  onSendRetry: () => void
  onRetryOriginal: () => void
  onCancelJob: () => void
  repairActionLoading: string | null
  actionRow?: ReactNode
}) {
  const { t } = useLanguage()

  return (
    <div className="rounded-lg border border-rose-500/20 bg-rose-500/8 p-3">
      <div className="text-[11px] font-semibold uppercase tracking-wide text-rose-300 mb-2">
        {title}
      </div>
      {subtitle && (
        <div className="text-xs text-text-muted mb-2">{subtitle}</div>
      )}
      {failedCommand && (
        <>
          <div className="text-xs text-text-muted mb-2">{t('recovery_failing_command')}</div>
          <div className="mb-3">
            <CommandPreview command={failedCommand} />
          </div>
        </>
      )}
      {stderrExcerpt && (
        <>
          <div className="text-xs text-text-muted mb-2">{t('recovery_stderr')}</div>
          <div className="mb-3">
            <CommandPreview command={stderrExcerpt} className="text-rose-100" />
          </div>
        </>
      )}
      <div className="text-xs text-text-muted mb-2">{t('recovery_prompt')}</div>
      <textarea
        value={repairCommand}
        onChange={(e) => onRepairCommandChange(e.target.value)}
        rows={5}
        className="w-full rounded-md bg-surface-raised/80 border border-border-subtle p-2 text-xs font-mono text-text-primary resize-y"
        placeholder={t('recovery_input_placeholder')}
      />
      <div className="mt-3 flex gap-2 flex-wrap">
        <button
          type="button"
          onClick={onSendRetry}
          disabled={repairActionLoading !== null || !repairCommand.trim()}
          className="px-3 py-1.5 rounded-lg bg-emerald-700/70 hover:bg-emerald-600/70 text-xs text-white disabled:opacity-50"
        >
          {repairActionLoading === 'modify_params' ? '…' : t('recovery_send_retry')}
        </button>
        <button
          type="button"
          onClick={onRetryOriginal}
          disabled={repairActionLoading !== null}
          className="px-3 py-1.5 rounded-lg bg-surface-overlay hover:bg-surface-hover text-xs text-text-primary disabled:opacity-50"
        >
          {repairActionLoading === 'retry_original' ? '…' : t('recovery_retry_original')}
        </button>
        <button
          type="button"
          onClick={onCancelJob}
          disabled={repairActionLoading !== null}
          className="px-3 py-1.5 rounded-lg bg-red-700/70 hover:bg-red-600/70 text-xs text-white disabled:opacity-50"
        >
          {repairActionLoading === 'cancel_job' ? '…' : t('recovery_stop_job')}
        </button>
      </div>
      {actionRow && (
        <div className="mt-3 flex flex-wrap gap-2">
          {actionRow}
        </div>
      )}
    </div>
  )
}

function JobCard({
  job,
  autoExpand,
  detailRefreshNonce,
  onAutoExpandConsumed,
  onDelete,
  onJobStateSync,
  onResume,
  onOpenThread,
}: {
  job: Job
  autoExpand: boolean
  detailRefreshNonce: number
  onAutoExpandConsumed: () => void
  onDelete: (job: Job) => void
  onJobStateSync: (jobId: string, patch: Partial<Job>) => void
  onResume: (job: Job) => void
  onOpenThread?: (threadId: string | null, jobId?: string | null) => void
}) {
  const { t, lang } = useLanguage()
  const [logsOpen, setLogsOpen] = useState(false)
  const [logs, setLogs] = useState<string[]>([])
  const [resources, setResources] = useState<{ cpu: number; mem: number } | null>(null)
  const [bindingSteps, setBindingSteps] = useState<JobBindingStep[]>([])
  const [bindingsLoading, setBindingsLoading] = useState(false)
  const [blockingIssues, setBlockingIssues] = useState<string[]>([])
  const [blockingPrompt, setBlockingPrompt] = useState<string | null>(null)
  const [pendingInteraction, setPendingInteraction] = useState<PendingInteractionPayload | null>(null)
  const [pendingInteractionType, setPendingInteractionType] = useState<string | null>(null)
  const [runtimeDiagnostics, setRuntimeDiagnostics] = useState<RuntimeDiagnostic[]>([])
  const [rollbackGuidance, setRollbackGuidance] = useState<RollbackGuidance | null>(null)
  const [autoRecoveryEvents, setAutoRecoveryEvents] = useState<AutoRecoveryEvent[]>([])
  const [timeline, setTimeline] = useState<TimelineEvent[]>([])
  const [timelineFilter, setTimelineFilter] = useState<TimelineCategory>('all')
  const [authActionLoading, setAuthActionLoading] = useState<string | null>(null)
  const [repairCommand, setRepairCommand] = useState('')
  const [repairActionLoading, setRepairActionLoading] = useState<string | null>(null)
  const [confirmationPhase, setConfirmationPhase] = useState<'abstract' | 'execution' | null>(null)
  const [confirmationPlan, setConfirmationPlan] = useState<ConfirmationPlanItem[]>([])
  const [executionPlanSummary, setExecutionPlanSummary] = useState<ExecutionPlanSummary | null>(null)
  const [executionConfirmationOverview, setExecutionConfirmationOverview] = useState<ExecutionConfirmationOverview | null>(null)
  const [executionIrReview, setExecutionIrReview] = useState<ExecutionIrReviewItem[]>([])
  const [executionPlanDelta, setExecutionPlanDelta] = useState<ExecutionPlanDelta | null>(null)
  const [executionPlanChanges, setExecutionPlanChanges] = useState<ExecutionPlanChangeItem[]>([])
  const logRef = useRef<HTMLDivElement>(null)
  const wsRef = useRef<WebSocket | null>(null)
  const executionPlanStatusMap = useMemo(
    () => buildExecutionPlanDeltaStatusMap(executionPlanDelta),
    [executionPlanDelta],
  )

  const filteredTimeline = useMemo(() => (
    timeline.filter((item) => timelineFilter === 'all' || classifyTimelineEvent(item) === timelineFilter)
  ), [timeline, timelineFilter])

  const loadBindings = useCallback(() => {
    setBindingsLoading(true)
    fetch(`/api/jobs/${job.id}/bindings?detailed=1`)
      .then((r) => r.json())
      .then((data: JobBindingResponse) => {
        onJobStateSync(job.id, {
          status: data.job_status ?? job.status,
          error_message: data.error_message ?? null,
          pending_interaction_type: data.pending_interaction_type ?? null,
        })
        setBindingSteps(Array.isArray(data.steps) ? data.steps : [])
        const payloadIssues = (data.pending_interaction_payload?.issues ?? [])
          .map((issue) => [issue.title, issue.description].filter(Boolean).join(': ').trim())
          .filter(Boolean)
        setBlockingIssues(payloadIssues.length > 0 ? payloadIssues : splitIssueText(data.error_message))
        setBlockingPrompt(data.pending_interaction_payload?.prompt_text ?? null)
        setPendingInteraction(data.pending_interaction_payload ?? null)
        setPendingInteractionType(data.pending_interaction_type ?? null)
        setRuntimeDiagnostics(Array.isArray(data.runtime_diagnostics) ? data.runtime_diagnostics : [])
        setRollbackGuidance(data.rollback_guidance ?? null)
        setAutoRecoveryEvents(Array.isArray(data.auto_recovery_events) ? data.auto_recovery_events : [])
        setTimeline(Array.isArray(data.timeline) ? data.timeline : [])
        setTimelineFilter('all')
        setRepairCommand(data.pending_interaction_payload?.failed_command ?? '')
        setConfirmationPhase(data.confirmation_phase ?? null)
        setConfirmationPlan(Array.isArray(data.confirmation_plan) ? data.confirmation_plan : [])
        setExecutionPlanSummary(data.execution_plan_summary ?? null)
        setExecutionConfirmationOverview(data.execution_confirmation_overview ?? null)
        setExecutionIrReview(Array.isArray(data.execution_ir_review) ? data.execution_ir_review : [])
        setExecutionPlanDelta(data.execution_plan_delta ?? null)
        setExecutionPlanChanges(Array.isArray(data.execution_plan_changes) ? data.execution_plan_changes : [])
      })
      .catch(() => {
        setBindingSteps([])
        setBlockingIssues(splitIssueText(job.error_message))
        setBlockingPrompt(null)
        setPendingInteraction(null)
        setPendingInteractionType(null)
        setRuntimeDiagnostics([])
        setRollbackGuidance(null)
        setAutoRecoveryEvents([])
        setTimeline([])
        setTimelineFilter('all')
        setRepairCommand('')
        setConfirmationPhase(null)
        setConfirmationPlan([])
        setExecutionPlanSummary(null)
        setExecutionConfirmationOverview(null)
        setExecutionIrReview([])
        setExecutionPlanDelta(null)
        setExecutionPlanChanges([])
      })
      .finally(() => setBindingsLoading(false))
  }, [job.id, job.error_message, job.status, job.pending_interaction_type, detailRefreshNonce, onJobStateSync])

  // Auto-expand when triggered externally (job just started)
  useEffect(() => {
    if (autoExpand) {
      setLogsOpen(true)
      onAutoExpandConsumed()
    }
  }, [autoExpand])

  // WS subscription when logs are expanded
  useEffect(() => {
    wsRef.current?.close()
    setLogs([])
    setResources(null)
    if (!logsOpen) return

    const wsUrl = `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws/jobs/${job.id}`
    const ws = new WebSocket(wsUrl)
    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data)
      if (msg.type === 'log') {
        setLogs((prev) => [...prev.slice(-499), `[${msg.stream}] ${msg.line}`])
        setTimeout(() => logRef.current?.scrollTo(0, logRef.current.scrollHeight), 50)
      } else if (msg.type === 'resources') {
        setResources({ cpu: msg.cpu_pct, mem: msg.mem_mb })
      } else if (msg.type === 'status') {
        // status updates handled by parent polling
      }
    }
    wsRef.current = ws
    return () => ws.close()
  }, [logsOpen, job.id])

  useEffect(() => {
    setBindingSteps([])
    setBlockingIssues([])
    setBlockingPrompt(null)
    setPendingInteraction(null)
    setPendingInteractionType(null)
    setRuntimeDiagnostics([])
    setAutoRecoveryEvents([])
    setTimeline([])
    setRepairCommand('')
    setConfirmationPhase(null)
    setConfirmationPlan([])
    setExecutionPlanSummary(null)
    setExecutionConfirmationOverview(null)
    setExecutionIrReview([])
    setExecutionPlanDelta(null)
    setExecutionPlanChanges([])
    if (!logsOpen) return
    loadBindings()
  }, [logsOpen, loadBindings])

  const resolveAuthorization = (action: 'approved' | 'rejected') => {
    const authRequestId = pendingInteraction?.auth_request_id
    if (!authRequestId || authActionLoading) return
    setAuthActionLoading(action)
    fetch(`/api/jobs/${job.id}/authorization-requests/${authRequestId}/resolve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action }),
    })
      .then(async (response) => {
        if (!response.ok) {
          const data = await response.json().catch(() => ({}))
          throw new Error(data.detail ?? action)
        }
        loadBindings()
      })
      .catch(() => {})
      .finally(() => setAuthActionLoading(null))
  }

  const resolveRepair = (choice: 'retry_original' | 'modify_params' | 'cancel_job') => {
    const repairRequestId = pendingInteraction?.repair_request_id
    if (!repairRequestId || repairActionLoading) return
    setRepairActionLoading(choice)
    fetch(`/api/jobs/${job.id}/repair-requests/${repairRequestId}/resolve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        choice,
        params: choice === 'modify_params' ? { command: repairCommand } : undefined,
      }),
    })
      .then(async (response) => {
        if (!response.ok) {
          const data = await response.json().catch(() => ({}))
          throw new Error(data.detail ?? choice)
        }
        loadBindings()
      })
      .catch(() => {})
      .finally(() => setRepairActionLoading(null))
  }

  const isTerminal = TERMINAL_STATUSES.has(job.status)
  const isResumable = [
    'queued',
    'binding_required',
    'resource_clarification_required',
    'interrupted',
    'failed',
  ].includes(job.status)

  return (
    <div className="bg-surface-raised rounded-xl overflow-hidden">
      {/* Card header */}
      <div className="flex items-center gap-4 px-5 py-4">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-3 flex-wrap">
            <span className="text-sm font-semibold text-text-primary truncate">{job.name ?? job.id}</span>
            <StatusBadge status={job.status} />
          </div>
          {job.goal && (
            <div className="text-xs text-text-muted truncate mt-0.5">{job.goal}</div>
          )}
          {(job.status === 'binding_required' || job.status === 'resource_clarification_required') && job.error_message && (
            <div className="text-xs text-orange-300/90 truncate mt-1" title={job.error_message}>
              {job.error_message}
            </div>
          )}
          <div className="flex items-center gap-3 mt-1 text-xs text-text-muted flex-wrap">
            <span>{job.created_at ? timeAgo(job.created_at, t) : t('time_just_now')}</span>
            {resources && job.status === 'running' && (
              <span>CPU {resources.cpu.toFixed(0)}% · {resources.mem.toFixed(0)} MB</span>
            )}
            {isTerminal && job.peak_cpu_pct != null && (
              <span>Peak CPU {job.peak_cpu_pct.toFixed(0)}% · {job.peak_mem_mb?.toFixed(0)} MB</span>
            )}
          </div>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          {isResumable && (
            <button
              onClick={() => onResume(job)}
              className="p-1.5 rounded-lg text-text-muted hover:text-indigo-400 hover:bg-indigo-500/10 transition-colors"
              title={t('tasks_resume_job_title')}
            >
              <PlayCircle size={13} />
            </button>
          )}
          {isTerminal && (
            <button
              onClick={() => onDelete(job)}
              className="p-1.5 rounded-lg text-text-muted hover:text-red-400 hover:bg-red-500/10 transition-colors"
              title={t('tasks_delete_job_title')}
            >
              <Trash2 size={13} />
            </button>
          )}
          <button
            onClick={() => setLogsOpen((o) => !o)}
            className="flex items-center gap-1 text-xs text-text-muted hover:text-text-primary transition-colors px-2 py-1 rounded-lg hover:bg-surface-hover"
          >
            {logsOpen ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
            {t('tasks_logs')}
          </button>
        </div>
      </div>

      {/* Collapsible log section */}
      <AnimatePresence initial={false}>
        {logsOpen && (
          <motion.div
            key="logs"
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2, ease: 'easeOut' }}
            className="overflow-hidden"
          >
            <div className="border-t border-border-subtle mx-5" />
            <div className="px-4 pt-4 pb-3 bg-surface-base">
              {(blockingPrompt || blockingIssues.length > 0 || job.error_message) && (
                <div className="mb-4 rounded-lg border border-orange-500/20 bg-orange-500/8 p-3">
                  <div className="text-[11px] font-semibold uppercase tracking-wide text-orange-300 mb-2">
                    {t('tasks_blocked_reason')}
                  </div>
                  {blockingPrompt && (
                    <div className="text-xs text-orange-100 whitespace-pre-wrap mb-2">{blockingPrompt}</div>
                  )}
                  {blockingIssues.length > 0 ? (
                    <div className="space-y-1">
                      {blockingIssues.map((issue, index) => (
                        <div key={`${job.id}-issue-${index}`} className="text-xs text-orange-100 break-words">
                          {issue}
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="text-xs text-orange-100">
                      {job.error_message || t('tasks_blocked_details_empty')}
                    </div>
                  )}
                </div>
              )}
              {runtimeDiagnostics.length > 0 && (
                <div className="mb-4 rounded-lg border border-indigo-500/20 bg-indigo-500/8 p-3">
                  <div className="text-[11px] font-semibold uppercase tracking-wide text-indigo-300 mb-2">
                    {t('tasks_runtime_diag_heading')}
                  </div>
                  <div className="space-y-1">
                    {runtimeDiagnostics.map((item, index) => (
                      <div key={`${job.id}-runtime-diagnostic-${index}`} className="text-xs text-indigo-100 break-words">
                        {formatRuntimeDiagnostic(item, t)}
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {autoRecoveryEvents.length > 0 && (
                <div className="mb-4 rounded-lg border border-emerald-500/20 bg-emerald-500/8 p-3">
                  <div className="text-[11px] font-semibold uppercase tracking-wide text-emerald-300 mb-2">
                    {t('tasks_auto_recovery_heading')}
                  </div>
                  <div className="space-y-1">
                    {autoRecoveryEvents.map((item, index) => (
                      <div key={`${job.id}-auto-recovery-${index}`} className="text-xs text-emerald-100 break-words">
                        {formatAutoRecoveryEvent(item, t)}
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {rollbackGuidance && (
                <div className="mb-4 rounded-lg border border-sky-500/20 bg-sky-500/8 p-3">
                  <div className="text-[11px] font-semibold uppercase tracking-wide text-sky-300 mb-2">
                    {t('tasks_rollback_guidance_heading')}
                  </div>
                  <div className="text-xs text-sky-100 break-words">
                    {[
                      `${t('tasks_supervisor_rollback_level')} ${formatRollbackLevel(rollbackGuidance.level ?? undefined, t)}`,
                      rollbackGuidance.target
                        ? `${t('tasks_rollback_guidance_target')} ${rollbackGuidance.target}`
                        : null,
                      `${t('tasks_rollback_guidance_reconfirm')} ${formatEligibilityBool(rollbackGuidance.reconfirmation_required, t)}`,
                    ].filter(Boolean).join(' · ')}
                  </div>
                  {rollbackGuidance.reason && (
                    <div className="mt-1 text-xs text-sky-100 break-words">
                      {rollbackGuidance.reason}
                    </div>
                  )}
                  {(rollbackGuidance.historical_matches || 0) > 0 && (
                    <div className="mt-1 text-xs text-sky-100 break-words">
                      {t('tasks_rollback_guidance_history')}{' '}
                      {[
                        t('tasks_rollback_guidance_matches').replace('{count}', String(rollbackGuidance.historical_matches ?? 0)),
                        typeof rollbackGuidance.historical_same_level_count === 'number'
                          ? t('tasks_rollback_guidance_same_level').replace('{count}', String(rollbackGuidance.historical_same_level_count))
                          : null,
                        typeof rollbackGuidance.historical_same_target_count === 'number'
                          ? t('tasks_rollback_guidance_same_target').replace('{count}', String(rollbackGuidance.historical_same_target_count))
                          : null,
                      ].filter(Boolean).join(' · ')}
                    </div>
                  )}
                </div>
              )}
              {timeline.length > 0 && (
                <div className="mb-4 rounded-lg border border-border-subtle bg-surface-overlay/60 p-3">
                  <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                    <div className="text-[11px] font-semibold uppercase tracking-wide text-text-muted">
                      {t('tasks_timeline_heading')}
                    </div>
                    <div className="text-[11px] text-text-muted">
                      {t('tasks_timeline_count')
                        .replace('{visible}', String(filteredTimeline.length))
                        .replace('{total}', String(timeline.length))}
                    </div>
                  </div>
                  <div className="mb-3 flex flex-wrap gap-2">
                    {(['all', 'step', 'result', 'recovery', 'confirmation'] as TimelineCategory[]).map((category) => {
                      const active = timelineFilter === category
                      return (
                        <button
                          key={`${job.id}-timeline-filter-${category}`}
                          type="button"
                          onClick={() => setTimelineFilter(category)}
                          className={`rounded-full border px-2.5 py-1 text-[11px] transition ${
                            active
                              ? `${timelineCategoryBadgeClass(category)} shadow-sm`
                              : 'border-border-subtle bg-surface-raised/40 text-text-muted hover:bg-surface-raised/70'
                          }`}
                        >
                          {timelineCategoryLabel(category, t)}
                        </button>
                      )
                    })}
                  </div>
                  <div className="space-y-2">
                    {filteredTimeline.length > 0 ? (
                      filteredTimeline.map((item, index) => {
                        const category = classifyTimelineEvent(item)
                        return (
                          <div
                            key={`${job.id}-timeline-${index}`}
                            className={`rounded-md border px-3 py-2 ${timelineCardClass(category)}`}
                          >
                            <div className="flex flex-wrap items-center justify-between gap-2">
                              <div className="flex flex-wrap items-center gap-2">
                                <div className="text-xs font-medium text-text-primary">{item.title || item.kind || 'event'}</div>
                                <span className={`rounded-full border px-2 py-0.5 text-[10px] ${timelineCategoryBadgeClass(category)}`}>
                                  {timelineCategoryLabel(category, t)}
                                </span>
                              </div>
                              <div className="text-[11px] text-text-muted">{formatTimelineTimestamp(item.ts, lang)}</div>
                            </div>
                            {item.detail && (
                              <div className="mt-1 text-[11px] text-text-muted break-words">{item.detail}</div>
                            )}
                          </div>
                        )
                      })
                    ) : (
                      <div className="rounded-md border border-dashed border-border-subtle px-3 py-2 text-[11px] text-text-muted">
                        {t('tasks_timeline_empty_filter')}
                      </div>
                    )}
                  </div>
                </div>
              )}
              {(pendingInteractionType === 'plan_confirmation' || pendingInteractionType === 'execution_confirmation') && (
                <div className="mb-4 rounded-lg border border-amber-500/20 bg-amber-500/8 p-3">
                  {(() => {
                    const confirmationLayers = buildConfirmationLayers(confirmationPhase, executionPlanSummary)
                    const layerToneClass: Record<ConfirmationLayerItem['tone'], string> = {
                      ready: 'border-emerald-500/25 bg-emerald-500/10 text-emerald-200',
                      pending: 'border-amber-500/25 bg-amber-500/10 text-amber-100',
                      waiting: 'border-slate-500/25 bg-slate-500/10 text-slate-300',
                      missing: 'border-red-500/25 bg-red-500/10 text-red-200',
                    }
                    return (
                      <>
                  <div className="text-[11px] font-semibold uppercase tracking-wide text-amber-300 mb-2">
                    {t('tasks_confirmation_heading')}
                  </div>
                  <div className="mb-2 flex items-center gap-2 flex-wrap">
                    <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-[11px] font-medium text-amber-200">
                      {confirmationPhase === 'execution'
                        ? t('tasks_confirmation_execution')
                        : t('tasks_confirmation_abstract')}
                    </span>
                    {confirmationPhase === 'execution' && executionPlanSummary && (
                      <span className="text-[11px] text-amber-100/90">
                        {t('tasks_confirmation_summary')
                          .replace('{groups}', String(executionPlanSummary.group_count ?? 0))
                          .replace('{nodes}', String(executionPlanSummary.node_count ?? 0))}
                      </span>
                    )}
                    <span className="text-[11px] text-text-muted">
                      {t('tasks_confirmation_scope').replace('{items}', String(confirmationPlan.length))}
                    </span>
                  </div>
                  {confirmationPhase === 'execution' && executionConfirmationOverview && (
                    <div className="mb-3 rounded-md border border-cyan-500/15 bg-cyan-500/6 px-3 py-2 text-[11px] text-cyan-100/90">
                      {t('tasks_confirmation_overview_summary')
                        .replace('{abstract}', String(executionConfirmationOverview.abstract_step_count ?? 0))
                        .replace('{ir}', String(executionConfirmationOverview.execution_ir_step_count ?? 0))
                        .replace('{groups}', String(executionConfirmationOverview.execution_group_count ?? 0))
                        .replace('{per_sample}', String(executionConfirmationOverview.per_sample_step_count ?? 0))
                        .replace('{aggregate}', String(executionConfirmationOverview.aggregate_step_count ?? 0))
                        .replace('{added}', String(executionConfirmationOverview.added_group_count ?? 0))
                        .replace('{changed}', String(executionConfirmationOverview.changed_group_count ?? 0))}
                    </div>
                  )}
                  {confirmationPhase === 'execution' && executionPlanDelta && (
                    <div className="mb-3 rounded-md border border-border-subtle/70 bg-surface-base/30 px-3 py-2 text-[11px] text-text-muted">
                      {t('tasks_confirmation_delta_summary')
                        .replace('{abstract}', String(executionPlanDelta.abstract_step_count ?? 0))
                        .replace('{execution}', String(executionPlanDelta.execution_group_count ?? 0))
                        .replace('{unchanged}', String(executionPlanDelta.unchanged_group_count ?? 0))
                        .replace('{changed}', String(executionPlanDelta.changed_group_count ?? 0))
                        .replace('{added}', String(executionPlanDelta.added_group_count ?? 0))}
                    </div>
                  )}
                  {confirmationPhase === 'execution' && executionIrReview.length > 0 && (
                    <div className="space-y-2 mb-3">
                      <div className="text-[11px] font-semibold uppercase tracking-wide text-text-muted">
                        {t('tasks_confirmation_execution_ir_review')}
                      </div>
                      {executionIrReview.map((item, index) => {
                        const title = item.display_name || item.step_key || item.step_type || t('chat_plan_step_fallback')
                        const meta = [item.step_type, item.description].filter(Boolean).join(' · ')
                        return (
                          <div
                            key={`${job.id}-execution-ir-${item.step_key ?? item.step_type ?? index}`}
                            className="rounded-md border border-cyan-500/10 bg-surface-raised/80 p-2"
                          >
                            <div className="text-xs font-medium text-text-primary">
                              {index + 1}. {title}
                            </div>
                            {meta && (
                              <div className="mt-1 text-[11px] text-text-muted break-words">
                                {meta}
                              </div>
                            )}
                          </div>
                        )
                      })}
                    </div>
                  )}
                  <div className="mb-3 rounded-md border border-border-subtle/70 bg-surface-base/30 px-3 py-2">
                    <div className="mb-2 text-[11px] font-semibold uppercase tracking-wide text-text-muted">
                      {t('tasks_confirmation_layers')}
                    </div>
                    <div className="grid gap-2 md:grid-cols-3">
                      {confirmationLayers.map((layer) => (
                        <div
                          key={`${job.id}-confirmation-layer-${layer.key}`}
                          className={`rounded-md border px-3 py-2 ${layerToneClass[layer.tone]}`}
                        >
                          <div className="text-[10px] uppercase tracking-wide opacity-80">
                            {t(layer.label)}
                          </div>
                          <div className="mt-1 text-xs font-medium">
                            {t(layer.stateLabel)}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="text-xs text-amber-100 whitespace-pre-wrap mb-3">
                    {pendingInteraction?.prompt_text || t('tasks_confirmation_waiting')}
                  </div>
                  {confirmationPhase === 'execution' && executionPlanChanges.length > 0 && (
                    <div className="space-y-2 mb-3">
                      <div className="text-[11px] font-semibold uppercase tracking-wide text-text-muted">
                        {t('tasks_confirmation_changes')}
                      </div>
                      {executionPlanChanges.map((item, index) => {
                        const title = item.display_name || item.group_key || item.step_type || t('chat_plan_step_fallback')
                        const summary = formatExecutionPlanChangeSummary(item, t)
                        const changeKinds = (item.change_kinds ?? []).filter(Boolean)
                        return (
                          <div
                            key={`${job.id}-confirmation-change-${item.group_key ?? item.step_type ?? index}`}
                            className="rounded-md border border-sky-500/10 bg-surface-raised/80 p-2"
                          >
                            <div className="flex flex-wrap items-center gap-2">
                              <div className="text-xs font-medium text-text-primary">
                                {title}
                              </div>
                              {changeKinds.map((kind) => (
                                <span
                                  key={`${job.id}-confirmation-change-kind-${item.group_key ?? index}-${kind}`}
                                  className="rounded-full border border-sky-500/20 bg-sky-500/10 px-2 py-0.5 text-[10px] font-medium text-sky-200"
                                >
                                  {formatExecutionPlanChangeKind(kind, t)}
                                </span>
                              ))}
                            </div>
                            {summary && (
                              <div className="mt-1 text-[11px] text-text-muted break-words">
                                {summary}
                              </div>
                            )}
                          </div>
                        )
                      })}
                    </div>
                  )}
                  {confirmationPlan.length > 0 && (
                    <div className="space-y-2 mb-3">
                      <div className="text-[11px] font-semibold uppercase tracking-wide text-text-muted">
                        {t('tasks_confirmation_review_items')}
                      </div>
                      {confirmationPlan.map((item, index) => {
                        const title = item.display_name || item.name || item.step_key || item.step_type || t('chat_plan_step_fallback')
                        const meta = [item.step_type, item.description].filter(Boolean).join(' · ')
                        const status = item.step_key ? executionPlanStatusMap[item.step_key] : undefined
                        return (
                          <div
                            key={`${job.id}-confirmation-${item.step_key ?? item.step_type ?? index}`}
                            className="rounded-md border border-amber-500/10 bg-surface-raised/80 p-2"
                          >
                            <div className="flex flex-wrap items-center gap-2">
                              <div className="text-xs font-medium text-text-primary">
                                {index + 1}. {title}
                              </div>
                              {status === 'added' && (
                                <span className="rounded-full border border-sky-500/20 bg-sky-500/10 px-2 py-0.5 text-[10px] font-medium text-sky-200">
                                  {t('tasks_confirmation_status_added')}
                                </span>
                              )}
                              {status === 'changed' && (
                                <span className="rounded-full border border-amber-500/20 bg-amber-500/10 px-2 py-0.5 text-[10px] font-medium text-amber-200">
                                  {t('tasks_confirmation_status_changed')}
                                </span>
                              )}
                            </div>
                            {meta && (
                              <div className="mt-1 text-[11px] text-text-muted break-words">
                                {meta}
                              </div>
                            )}
                          </div>
                        )
                      })}
                    </div>
                  )}
                  <div className="flex flex-wrap items-center gap-2">
                    <div className="text-xs text-text-muted">
                      {t('tasks_confirmation_open_chat')}
                    </div>
                    {job.thread_id && onOpenThread && (
                      <button
                        type="button"
                        onClick={() => onOpenThread(job.thread_id ?? null, job.id)}
                        className="rounded-lg border border-amber-500/25 px-3 py-1.5 text-xs text-amber-100 transition-colors hover:bg-amber-500/10"
                      >
                        {t('tasks_supervisor_open_chat')}
                      </button>
                    )}
                  </div>
                      </>
                    )
                  })()}
                </div>
              )}
              {pendingInteractionType === 'authorization' && pendingInteraction?.command && (
                <div className="mb-4">
                  <AuthorizationPromptCard
                    title={t('status_waiting_for_authorization')}
                    subtitle={pendingInteraction.command_type || pendingInteraction.step_key || 'command'}
                    command={pendingInteraction.command}
                    onAuthorize={() => resolveAuthorization('approved')}
                    onReject={() => resolveAuthorization('rejected')}
                    authActionLoading={authActionLoading}
                  />
                </div>
              )}
              {pendingInteractionType === 'repair' && pendingInteraction?.repair_request_id && (
                <div className="mb-4">
                  <RepairPromptCard
                    title={t('status_waiting_for_repair')}
                    subtitle={pendingInteraction.step_key}
                    failedCommand={pendingInteraction.failed_command}
                    stderrExcerpt={pendingInteraction.stderr_excerpt}
                    repairCommand={repairCommand}
                    onRepairCommandChange={setRepairCommand}
                    onSendRetry={() => resolveRepair('modify_params')}
                    onRetryOriginal={() => resolveRepair('retry_original')}
                    onCancelJob={() => resolveRepair('cancel_job')}
                    repairActionLoading={repairActionLoading}
                  />
                </div>
              )}
              <div className="text-[11px] font-semibold uppercase tracking-wide text-text-muted mb-2">
                {t('tasks_bindings_heading')}
              </div>
              {bindingsLoading ? (
                <div className="text-xs text-text-muted">{t('settings_loading')}</div>
              ) : bindingSteps.length === 0 ? (
                <div className="text-xs text-text-muted">{t('tasks_bindings_empty')}</div>
              ) : (
                <div className="space-y-3">
                  {bindingSteps.map((step) => (
                    <div key={step.step_id} className="rounded-lg border border-border-subtle bg-surface-overlay/60 p-3">
                      <div className="flex flex-wrap items-center gap-2 mb-2">
                        <span className="text-xs font-semibold text-text-primary">
                          {step.display_name || step.step_key || step.step_type || step.step_id}
                        </span>
                        {step.step_key && (
                          <code className="text-[10px] text-text-muted">{step.step_key}</code>
                        )}
                      </div>
                      <div className="space-y-2">
                        {step.bindings.map((binding) => {
                          const reasons = binding.match_metadata?.reason_codes || []
                          return (
                            <div key={binding.id} className="rounded-md border border-border-subtle/70 bg-surface-raised/70 p-2">
                              <div className="flex flex-wrap items-center gap-2 mb-1">
                                <span className="text-xs font-medium text-text-primary">{binding.slot_name}</span>
                                <span className={`rounded-full px-2 py-0.5 text-[10px] ${
                                  binding.status === 'resolved'
                                    ? 'bg-emerald-500/15 text-emerald-300'
                                    : 'bg-amber-500/15 text-amber-300'
                                }`}>
                                  {binding.status === 'resolved' ? t('tasks_binding_resolved') : t('tasks_binding_missing')}
                                </span>
                                <span className="rounded-full px-2 py-0.5 text-[10px] bg-sky-500/10 text-sky-300">
                                  {formatBindingSource(binding.source_type, lang)}
                                </span>
                              </div>
                              <div className="text-xs font-mono text-text-primary break-all">
                                {binding.resolved_path || '—'}
                              </div>
                              {reasons.length > 0 && (
                                <div className="mt-2">
                                  <div className="text-[10px] uppercase tracking-wide text-text-muted mb-1">
                                    {t('tasks_binding_reasons')}
                                  </div>
                                  <div className="flex flex-wrap gap-1">
                                    {reasons.map((reason) => (
                                      <span
                                        key={`${binding.id}-${reason}`}
                                        className="rounded-full px-2 py-0.5 text-[10px] bg-violet-500/10 text-violet-200"
                                      >
                                        {formatReasonCode(reason, lang)}
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              )}
                              {binding.match_metadata?.source_step_key && (
                                <div className="mt-1 text-[11px] text-text-muted">
                                  {t('tasks_binding_selected_via')} {binding.match_metadata.source_step_key}
                                  {binding.match_metadata.source_slot_name ? `.${binding.match_metadata.source_slot_name}` : ''}
                                </div>
                              )}
                            </div>
                          )
                        })}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
            <div
              ref={logRef}
              className="overflow-y-auto p-4 font-mono text-xs text-emerald-300 bg-surface-base whitespace-pre-wrap"
              style={{ maxHeight: 240 }}
            >
              {logs.length > 0
                ? logs.join('\n')
                : <span className="text-text-muted">{t('tasks_waiting_logs')}</span>
              }
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

export default function TaskMonitor({
  compact: _compact,
  projectId,
  autoSelectJobId,
  onAutoSelectConsumed,
  onOpenThread,
  onOpenResourceWorkspace,
  onOpenSettingsDiagnostics,
}: Props) {
  const { t, lang } = useLanguage()
  const {
    jobs: recentJobs,
    getJobsPage,
    getPageHasMore,
    incidents,
    attentionSummary,
    locateJobPage,
    patchJob,
    refreshJobPage,
    refreshJobs,
    refreshAll,
    eventVersion,
    totalCount,
    refreshIncidents,
  } = useProjectTaskFeed()
  const [supervisorReview, setSupervisorReview] = useState<SupervisorReview | null>(null)
  const [supervisorLoading, setSupervisorLoading] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState<{ jobId: string; jobName: string; outputDir?: string | null } | null>(null)
  const [autoExpandId, setAutoExpandId] = useState<string | null>(null)
  const [page, setPage] = useState(1)
  const [jobsLoading, setJobsLoading] = useState(false)
  const [detailRefreshNonce, setDetailRefreshNonce] = useState(0)
  const [authorizationSnapshots, setAuthorizationSnapshots] = useState<Record<string, PendingInteractionPayload>>({})
  const [authorizationSnapshotLoading, setAuthorizationSnapshotLoading] = useState<Record<string, boolean>>({})
  const [authorizationActionLoading, setAuthorizationActionLoading] = useState<Record<string, string | null>>({})
  const [repairSnapshots, setRepairSnapshots] = useState<Record<string, PendingInteractionPayload>>({})
  const [repairSnapshotLoading, setRepairSnapshotLoading] = useState<Record<string, boolean>>({})
  const [repairActionLoadingByJob, setRepairActionLoadingByJob] = useState<Record<string, string | null>>({})
  const [repairDrafts, setRepairDrafts] = useState<Record<string, string>>({})
  const listRef = useRef<HTMLDivElement>(null)
  const projectEventRefreshTimerRef = useRef<number | null>(null)
  const jobs = getJobsPage(page) as Job[]
  const knownJobsById = useMemo(
    () => Object.fromEntries([...recentJobs, ...jobs].map((job) => [job.id, job])),
    [jobs, recentJobs],
  )
  const incidentsByKey = useMemo(
    () => Object.fromEntries(incidents.map((incident) => [`${incident.job_id}:${incident.incident_type}`, incident])),
    [incidents],
  )
  const attentionNeedsInput = attentionSummary?.needs_input ?? []
  const attentionNeedsReview = attentionSummary?.needs_review ?? []
  const pendingAuthorizationEntries = useMemo(
    () => attentionNeedsInput
      .filter((item) => item.reason === 'authorization')
      .map((item) => ({
        item,
        job: knownJobsById[item.job_id] ?? null,
      })),
    [attentionNeedsInput, knownJobsById],
  )
  const pendingRepairEntries = useMemo(
    () => attentionNeedsInput
      .filter((item) => item.reason === 'repair')
      .map((item) => ({
        item,
        job: knownJobsById[item.job_id] ?? null,
      })),
    [attentionNeedsInput, knownJobsById],
  )
  const pendingOperatorEntries = useMemo(
    () => attentionNeedsInput
      .filter((item) => item.reason === 'confirmation' || item.reason === 'clarification' || item.reason === 'rollback_review')
      .map((item) => ({
        item,
        job: knownJobsById[item.job_id] ?? null,
        incident: incidentsByKey[`${item.job_id}:${item.incident_type}`],
      }))
      .sort((left, right) => {
        const reasonDelta = pendingOperatorPriority(left.item.reason) - pendingOperatorPriority(right.item.reason)
        if (reasonDelta !== 0) return reasonDelta
        return (right.item.age_seconds ?? 0) - (left.item.age_seconds ?? 0)
      }),
    [attentionNeedsInput, incidentsByKey, knownJobsById],
  )
  const reviewEntries = useMemo(
    () => attentionNeedsReview.map((item) => ({
      item,
      job: knownJobsById[item.job_id] ?? null,
      incident: incidentsByKey[`${item.job_id}:${item.incident_type}`],
    })),
    [attentionNeedsReview, incidentsByKey, knownJobsById],
  )
  const autoAuthorizeCommands = attentionSummary?.auto_authorize_commands ?? false
  const attentionCounts = attentionSummary?.counts ?? {
    running: 0,
    authorization: 0,
    repair: 0,
    confirmation: 0,
    clarification: 0,
    rollback_review: 0,
    warning: 0,
    needs_input: 0,
    needs_review: 0,
  }
  const reviewSummary = useMemo(
    () => reviewEntries.reduce((acc, { item }) => {
      acc.total += 1
      if (item.severity === 'critical') acc.critical += 1
      else if (item.severity === 'warning') acc.warning += 1
      else acc.info += 1
      return acc
    }, { total: 0, critical: 0, warning: 0, info: 0 }),
    [reviewEntries],
  )
  const supervisorCandidateTotal = (
    reviewEntries.length
    + pendingAuthorizationEntries.length
    + pendingRepairEntries.length
    + pendingOperatorEntries.length
  )

  const loadJobsPage = useCallback((pageNumber: number, options?: { force?: boolean }) => {
    setJobsLoading(true)
    return refreshJobPage(pageNumber, options)
      .then((data) => {
        if (pageNumber > 1 && data.length === 0) {
          setPage((prev) => Math.max(1, prev - 1))
        }
        return data
      })
      .catch(() => [] as Job[])
      .finally(() => setJobsLoading(false))
  }, [refreshJobPage])

  const syncJobState = useCallback((jobId: string, patch: Partial<Job>) => {
    patchJob(jobId, patch)
  }, [patchJob])

  const loadSupervisorReview = useCallback(() => {
    setSupervisorLoading(true)
    const params = new URLSearchParams()
    if (projectId) params.set('project', projectId)
    return fetch(`/api/jobs/supervisor-review?${params.toString()}`)
      .then((r) => r.json())
      .then((data: SupervisorReview) => setSupervisorReview(data))
      .catch(() => setSupervisorReview(null))
      .finally(() => setSupervisorLoading(false))
  }, [projectId])

  useEffect(() => {
    setPage(1)
  }, [projectId])

  useEffect(() => {
    setSupervisorReview(null)
  }, [projectId])

  useEffect(() => {
    if (supervisorCandidateTotal === 0) {
      setSupervisorReview(null)
    }
  }, [supervisorCandidateTotal])

  useEffect(() => {
    void loadJobsPage(page)
    const interval = setInterval(() => {
      void loadJobsPage(page, { force: true })
    }, 30000)
    return () => clearInterval(interval)
  }, [loadJobsPage, page])

  useEffect(() => () => {
    if (projectEventRefreshTimerRef.current !== null) {
      window.clearTimeout(projectEventRefreshTimerRef.current)
      projectEventRefreshTimerRef.current = null
    }
  }, [])

  useEffect(() => {
    if (eventVersion === 0) return

    const shouldRefreshSupervisor = Boolean(supervisorReview) || supervisorCandidateTotal > 0

    if (projectEventRefreshTimerRef.current !== null) {
      window.clearTimeout(projectEventRefreshTimerRef.current)
    }
    projectEventRefreshTimerRef.current = window.setTimeout(() => {
      void loadJobsPage(page, { force: true })
      void refreshIncidents()
      if (shouldRefreshSupervisor) {
        void loadSupervisorReview()
      }
      projectEventRefreshTimerRef.current = null
    }, 250)
  }, [eventVersion, loadJobsPage, loadSupervisorReview, page, refreshIncidents, supervisorCandidateTotal, supervisorReview])

  useEffect(() => {
    if (!autoSelectJobId) return
    locateJobPage(autoSelectJobId)
      .then((targetPage) => {
        const nextPage = targetPage ?? 1
        setPage(nextPage)
        return loadJobsPage(nextPage, { force: true }).then(() => {
          setAutoExpandId(autoSelectJobId)
          onAutoSelectConsumed?.()
        })
      })
      .catch(() => {
        setAutoExpandId(autoSelectJobId)
        onAutoSelectConsumed?.()
      })
  }, [autoSelectJobId, loadJobsPage, locateJobPage, onAutoSelectConsumed])

  useEffect(() => {
    listRef.current?.scrollTo({ top: 0, behavior: 'smooth' })
  }, [page])

  const executeDelete = () => {
    if (!confirmDelete) return
    fetch(`/api/jobs/${confirmDelete.jobId}/purge`, { method: 'DELETE' })
      .then(async () => {
        await Promise.all([
          refreshJobs(),
          loadJobsPage(page, { force: true }),
          refreshIncidents(),
        ])
      })
      .catch(() => {})
      .finally(() => setConfirmDelete(null))
  }

  const loadAuthorizationSnapshot = useCallback((jobId: string) => {
    setAuthorizationSnapshotLoading((prev) => ({ ...prev, [jobId]: true }))
    return fetch(`/api/jobs/${jobId}/bindings?detailed=1`)
      .then((r) => r.json())
      .then((data: JobBindingResponse) => {
        if (data.pending_interaction_type === 'authorization' && data.pending_interaction_payload?.command) {
          setAuthorizationSnapshots((prev) => ({
            ...prev,
            [jobId]: data.pending_interaction_payload ?? {},
          }))
        } else {
          setAuthorizationSnapshots((prev) => {
            const next = { ...prev }
            delete next[jobId]
            return next
          })
        }
      })
      .catch(() => {})
      .finally(() => {
        setAuthorizationSnapshotLoading((prev) => ({ ...prev, [jobId]: false }))
      })
  }, [])

  useEffect(() => {
    pendingAuthorizationEntries.forEach(({ item }) => {
      if (!authorizationSnapshots[item.job_id] && !authorizationSnapshotLoading[item.job_id]) {
        void loadAuthorizationSnapshot(item.job_id)
      }
    })
  }, [authorizationSnapshotLoading, authorizationSnapshots, loadAuthorizationSnapshot, pendingAuthorizationEntries])

  const loadRepairSnapshot = useCallback((jobId: string) => {
    setRepairSnapshotLoading((prev) => ({ ...prev, [jobId]: true }))
    return fetch(`/api/jobs/${jobId}/bindings?detailed=1`)
      .then((r) => r.json())
      .then((data: JobBindingResponse) => {
        if (data.pending_interaction_type === 'repair' && data.pending_interaction_payload?.repair_request_id) {
          setRepairSnapshots((prev) => ({
            ...prev,
            [jobId]: data.pending_interaction_payload ?? {},
          }))
          setRepairDrafts((prev) => ({
            ...prev,
            [jobId]: prev[jobId] ?? (data.pending_interaction_payload?.failed_command ?? ''),
          }))
        } else {
          setRepairSnapshots((prev) => {
            const next = { ...prev }
            delete next[jobId]
            return next
          })
        }
      })
      .catch(() => {})
      .finally(() => {
        setRepairSnapshotLoading((prev) => ({ ...prev, [jobId]: false }))
      })
  }, [])

  useEffect(() => {
    pendingRepairEntries.forEach(({ item }) => {
      if (!repairSnapshots[item.job_id] && !repairSnapshotLoading[item.job_id]) {
        void loadRepairSnapshot(item.job_id)
      }
    })
  }, [loadRepairSnapshot, pendingRepairEntries, repairSnapshotLoading, repairSnapshots])

  const resolveAuthorizationFromQueue = useCallback((jobId: string, authRequestId: string, action: 'approved' | 'rejected') => {
    setAuthorizationActionLoading((prev) => ({ ...prev, [jobId]: action }))
    return fetch(`/api/jobs/${jobId}/authorization-requests/${authRequestId}/resolve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action }),
    })
      .then(async (response) => {
        if (!response.ok) {
          const data = await response.json().catch(() => ({}))
          throw new Error(data.detail ?? action)
        }
        await Promise.all([
          refreshAll(),
          loadJobsPage(page, { force: true }),
        ])
        setDetailRefreshNonce((prev) => prev + 1)
      })
      .catch(() => {})
      .finally(() => {
        setAuthorizationActionLoading((prev) => ({ ...prev, [jobId]: null }))
      })
  }, [loadJobsPage, page, refreshAll])

  const resolveRepairFromQueue = useCallback((
    jobId: string,
    repairRequestId: string,
    choice: 'retry_original' | 'modify_params' | 'cancel_job',
  ) => {
    setRepairActionLoadingByJob((prev) => ({ ...prev, [jobId]: choice }))
    return fetch(`/api/jobs/${jobId}/repair-requests/${repairRequestId}/resolve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        choice,
        params: choice === 'modify_params' ? { command: repairDrafts[jobId] ?? '' } : undefined,
      }),
    })
      .then(async (response) => {
        if (!response.ok) {
          const data = await response.json().catch(() => ({}))
          throw new Error(data.detail ?? choice)
        }
        await Promise.all([
          refreshAll(),
          loadJobsPage(page, { force: true }),
        ])
        setDetailRefreshNonce((prev) => prev + 1)
      })
      .catch(() => {})
      .finally(() => {
        setRepairActionLoadingByJob((prev) => ({ ...prev, [jobId]: null }))
      })
  }, [loadJobsPage, page, refreshAll, repairDrafts])

  const executeResume = (job: Job) => {
    fetch(`/api/jobs/${job.id}/resume`, { method: 'POST' })
      .then((r) => r.json())
      .then(() => {
        patchJob(job.id, { status: 'queued' })
        setDetailRefreshNonce((prev) => prev + 1)
      })
      .catch(() => {})
  }

  const executeResumeById = (jobId: string) => {
    fetch(`/api/jobs/${jobId}/resume`, { method: 'POST' })
      .then((r) => r.json())
      .then(() => {
        patchJob(jobId, { status: 'queued' })
        setDetailRefreshNonce((prev) => prev + 1)
        void refreshJobs()
        void loadJobsPage(page, { force: true })
        void refreshIncidents()
        setSupervisorReview(null)
      })
      .catch(() => {})
  }

  const executeSupervisorSafeAction = (jobId: string, safeAction: string, nextStatus?: string) => {
    fetch(`/api/jobs/${jobId}/supervisor-actions/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ safe_action: safeAction }),
    })
      .then((r) => r.json())
      .then(() => {
        if (nextStatus) {
          patchJob(jobId, { status: nextStatus })
        }
        setDetailRefreshNonce((prev) => prev + 1)
        void refreshJobs()
        void loadJobsPage(page, { force: true })
        void refreshIncidents()
        void loadSupervisorReview()
      })
      .catch(() => {})
  }

  const focusJob = (jobId: string) => {
    locateJobPage(jobId)
      .then((targetPage) => {
        const nextPage = targetPage ?? 1
        setPage(nextPage)
        return loadJobsPage(nextPage, { force: true }).then(() => {
          setAutoExpandId(jobId)
          listRef.current?.scrollTo({ top: 0, behavior: 'smooth' })
        })
      })
      .catch(() => {
        setAutoExpandId(jobId)
      })
  }

  const totalPages = Math.max(1, Math.ceil(totalCount / PAGE_SIZE))
  const rangeStart = totalCount === 0 ? 0 : (page - 1) * PAGE_SIZE + 1
  const rangeEnd = totalCount === 0 ? 0 : Math.min(page * PAGE_SIZE, totalCount)
  const prevDisabled = page <= 1
  const nextDisabled = !getPageHasMore(page)
  const summaryText = lang === 'zh'
    ? `${t('tasks_showing_label')} ${rangeStart}-${rangeEnd} / ${totalCount} ${t('tasks_total_jobs')}`
    : `${t('tasks_showing_label')} ${rangeStart}-${rangeEnd} of ${totalCount} ${t('tasks_total_jobs')}`
  const pageText = lang === 'zh'
    ? `${t('tasks_page_label')} ${page} / ${totalPages} 页`
    : `${t('tasks_page_label')} ${page} / ${totalPages}`

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="shrink-0 px-5 pt-5">
        <h2 className="text-xs font-semibold text-text-muted uppercase tracking-wider">{t('tasks_heading')}</h2>
      </div>

      <div ref={listRef} className="flex-1 min-h-0 overflow-y-auto p-5 space-y-3">
        {(attentionSummary?.signal ?? 'idle') !== 'idle' && (
          <div className={`rounded-xl border p-4 ${
            attentionSummary?.signal === 'attention'
              ? 'border-rose-500/20 bg-rose-500/8'
              : attentionSummary?.signal === 'warning'
                ? 'border-amber-500/20 bg-amber-500/8'
                : 'border-emerald-500/20 bg-emerald-500/8'
          }`}>
            <div className={`text-xs font-semibold uppercase tracking-wide ${
              attentionSummary?.signal === 'attention'
                ? 'text-rose-300'
                : attentionSummary?.signal === 'warning'
                  ? 'text-amber-300'
                  : 'text-emerald-300'
            }`}>
              {t('tasks_attention_heading')}
            </div>
            <div className="mt-1 text-xs text-text-muted">
              {t('tasks_attention_hint')}
            </div>
            <div className="mt-3 flex flex-wrap gap-2">
              {attentionCounts.needs_input > 0 && (
                <span className="rounded-full bg-rose-500/12 px-2 py-1 text-[11px] font-medium text-rose-200">
                  {t('tasks_tray_attention')} {attentionCounts.needs_input}
                </span>
              )}
              {attentionCounts.needs_review > 0 && (
                <span className="rounded-full bg-amber-500/12 px-2 py-1 text-[11px] font-medium text-amber-200">
                  {t('tasks_tray_warning')} {attentionCounts.needs_review}
                </span>
              )}
              {attentionCounts.running > 0 && (
                <span className="rounded-full bg-emerald-500/12 px-2 py-1 text-[11px] font-medium text-emerald-200">
                  {t('tasks_tray_running')} {attentionCounts.running}
                </span>
              )}
            </div>
          </div>
        )}
        {autoAuthorizeCommands && (
          <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/8 p-4">
            <div className="text-xs font-semibold uppercase tracking-wide text-emerald-300">
              {t('tasks_auto_authorize_heading')}
            </div>
            <div className="mt-1 text-sm text-text-primary">
              {t('tasks_auto_authorize_summary')}
            </div>
            <div className="mt-1 text-xs text-text-muted">
              {pendingAuthorizationEntries.length > 0
                ? t('tasks_auto_authorize_legacy_hint').replace('{count}', String(pendingAuthorizationEntries.length))
                : t('tasks_auto_authorize_hint')}
            </div>
          </div>
        )}
        {pendingOperatorEntries.length > 0 && (
          <div className="rounded-xl border border-violet-500/20 bg-violet-500/8 p-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <div className="text-xs font-semibold uppercase tracking-wide text-violet-300">
                  {t('tasks_pending_inputs_heading')}
                </div>
                <div className="mt-1 text-sm text-text-primary">
                  {t('tasks_pending_inputs_summary').replace('{count}', String(pendingOperatorEntries.length))}
                </div>
                <div className="mt-1 text-xs text-text-muted">
                  {t('tasks_pending_inputs_hint')}
                </div>
              </div>
            </div>
            <div className="mt-3 space-y-3">
              {pendingOperatorEntries.map(({ item, job, incident }) => (
                (() => {
                  const chatActionCode = resolveOperatorChatActionCode({
                    reason: item.reason,
                    rollbackLevel: item.rollback_level,
                    nextAction: incident?.next_action ?? item.next_action,
                  })
                  const chatHint = formatOperatorChatActionHint(chatActionCode, t)
                  return (
                    <div
                      key={`pending-input-${item.key}`}
                      className="rounded-lg border border-violet-500/10 bg-surface-raised/80 p-3"
                    >
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="text-xs font-semibold text-text-primary">{job?.name ?? item.job_name ?? item.job_id}</span>
                        {job?.status && <StatusBadge status={job.status} />}
                        <span className="rounded-full bg-violet-500/12 px-2 py-0.5 text-[10px] text-violet-200">
                          {formatAttentionReasonLabel(item.reason, t)}
                        </span>
                      </div>
                      {job?.goal && (
                        <div className="mt-1 text-[11px] text-text-muted break-words">
                          {job.goal}
                        </div>
                      )}
                      <div className="mt-2 text-xs text-text-primary">
                        {item.summary}
                      </div>
                      {item.reason === 'rollback_review' && (
                        <div className="mt-1 space-y-1 text-[11px] text-text-muted">
                          <div>
                            {t('tasks_attention_rollback_scope')}{' '}
                            {[
                              item.rollback_level
                                ? `${t('tasks_supervisor_rollback_level')} ${formatRollbackLevel(item.rollback_level, t)}`
                                : null,
                              item.rollback_target ? `target=${item.rollback_target}` : null,
                              item.reconfirmation_required !== undefined && item.reconfirmation_required !== null
                                ? `${t('tasks_rollback_guidance_reconfirm')} ${formatEligibilityBool(item.reconfirmation_required, t)}`
                                : null,
                            ].filter(Boolean).join(' · ')}
                          </div>
                          {item.rollback_reason && (
                            <div>
                              {t('tasks_attention_rollback_reason')}{' '}
                              {item.rollback_reason}
                            </div>
                          )}
                        </div>
                      )}
                      {incident?.current_step_name && (
                        <div className="mt-1 text-[11px] text-text-muted">
                          {t('tasks_incident_current_step').replace('{step}', incident.current_step_name)}
                        </div>
                      )}
                      {incident?.next_action && (
                        <div className="mt-1 text-[11px] text-text-muted">
                          {t(`tasks_incident_action_${incident.next_action}` as TranslationKey)}
                        </div>
                      )}
                      {chatHint && (
                        <div className="mt-1 text-[11px] text-violet-100/90">
                          {chatHint}
                        </div>
                      )}
                      <div className="mt-3 flex flex-wrap gap-2">
                        <button
                          type="button"
                          onClick={() => focusJob(item.job_id)}
                          className="rounded-lg border border-border-subtle px-3 py-1.5 text-xs text-text-primary transition-colors hover:bg-surface-hover"
                        >
                          {t('tasks_supervisor_open_task')}
                        </button>
                        {(job?.thread_id || item.thread_id || incident?.thread_id) && onOpenThread && (
                          <button
                            type="button"
                            onClick={() => onOpenThread((job?.thread_id ?? item.thread_id ?? incident?.thread_id) || null, item.job_id)}
                            className="rounded-lg border border-violet-500/25 px-3 py-1.5 text-xs text-violet-100 transition-colors hover:bg-violet-500/10"
                          >
                            {formatOperatorChatActionCta(chatActionCode, t)}
                          </button>
                        )}
                      </div>
                    </div>
                  )
                })()
              ))}
            </div>
          </div>
        )}
        {pendingAuthorizationEntries.length > 0 && (
          <div className="rounded-xl border border-sky-500/20 bg-sky-500/8 p-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <div className="text-xs font-semibold uppercase tracking-wide text-sky-300">
                  {t('tasks_pending_authorizations_heading')}
                </div>
                <div className="mt-1 text-sm text-text-primary">
                  {t('tasks_pending_authorizations_summary').replace('{count}', String(pendingAuthorizationEntries.length))}
                </div>
                <div className="mt-1 text-xs text-text-muted">
                  {t('tasks_pending_authorizations_hint')}
                </div>
              </div>
            </div>
            <div className="mt-3 space-y-3">
              {pendingAuthorizationEntries.map(({ item, job }) => {
                const snapshot = authorizationSnapshots[item.job_id]
                const authRequestId = snapshot?.auth_request_id
                return (
                  <div
                    key={`pending-auth-${item.job_id}`}
                    className="rounded-lg border border-sky-500/10 bg-surface-raised/80 p-3"
                  >
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-xs font-semibold text-text-primary">{job?.name ?? item.job_name ?? item.job_id}</span>
                      {job?.status && <StatusBadge status={job.status} />}
                    </div>
                    {job?.goal && (
                      <div className="mt-1 text-[11px] text-text-muted break-words">
                        {job.goal}
                      </div>
                    )}
                    <div className="mt-3">
                      {snapshot?.command && authRequestId ? (
                        <AuthorizationPromptCard
                          title={t('status_waiting_for_authorization')}
                          subtitle={snapshot.command_type || snapshot.step_key || 'command'}
                          command={snapshot.command}
                          onAuthorize={() => {
                            void resolveAuthorizationFromQueue(item.job_id, authRequestId, 'approved')
                          }}
                          onReject={() => {
                            void resolveAuthorizationFromQueue(item.job_id, authRequestId, 'rejected')
                          }}
                          authActionLoading={authorizationActionLoading[item.job_id] ?? null}
                          actionRow={(
                            <>
                              <button
                                type="button"
                                onClick={() => focusJob(item.job_id)}
                                className="rounded-lg border border-border-subtle px-3 py-1.5 text-xs text-text-primary transition-colors hover:bg-surface-hover"
                              >
                                {t('tasks_supervisor_open_task')}
                              </button>
                              {job?.thread_id && onOpenThread && (
                                <button
                                  type="button"
                                  onClick={() => onOpenThread(job.thread_id ?? null, item.job_id)}
                                  className="rounded-lg border border-sky-500/25 px-3 py-1.5 text-xs text-sky-100 transition-colors hover:bg-sky-500/10"
                                >
                                  {t('tasks_supervisor_open_chat')}
                                </button>
                              )}
                            </>
                          )}
                        />
                      ) : (
                        <div className="rounded-lg border border-border-subtle/70 bg-surface-base/40 px-3 py-2 text-xs text-text-muted">
                          {authorizationSnapshotLoading[item.job_id]
                            ? t('settings_loading')
                            : t('tasks_pending_authorizations_loading_command')}
                        </div>
                      )}
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        )}
        {pendingRepairEntries.length > 0 && (
          <div className="rounded-xl border border-rose-500/20 bg-rose-500/8 p-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <div className="text-xs font-semibold uppercase tracking-wide text-rose-300">
                  {t('tasks_pending_repairs_heading')}
                </div>
                <div className="mt-1 text-sm text-text-primary">
                  {t('tasks_pending_repairs_summary').replace('{count}', String(pendingRepairEntries.length))}
                </div>
                <div className="mt-1 text-xs text-text-muted">
                  {t('tasks_pending_repairs_hint')}
                </div>
              </div>
            </div>
            <div className="mt-3 space-y-3">
              {pendingRepairEntries.map(({ item, job }) => {
                const snapshot = repairSnapshots[item.job_id]
                const repairRequestId = snapshot?.repair_request_id
                return (
                  <div
                    key={`pending-repair-${item.job_id}`}
                    className="rounded-lg border border-rose-500/10 bg-surface-raised/80 p-3"
                  >
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-xs font-semibold text-text-primary">{job?.name ?? item.job_name ?? item.job_id}</span>
                      {job?.status && <StatusBadge status={job.status} />}
                    </div>
                    {job?.goal && (
                      <div className="mt-1 text-[11px] text-text-muted break-words">
                        {job.goal}
                      </div>
                    )}
                    <div className="mt-3">
                      {repairRequestId ? (
                        <RepairPromptCard
                          title={t('status_waiting_for_repair')}
                          subtitle={snapshot?.step_key}
                          failedCommand={snapshot?.failed_command}
                          stderrExcerpt={snapshot?.stderr_excerpt}
                          repairCommand={repairDrafts[item.job_id] ?? ''}
                          onRepairCommandChange={(value) => {
                            setRepairDrafts((prev) => ({ ...prev, [item.job_id]: value }))
                          }}
                          onSendRetry={() => {
                            void resolveRepairFromQueue(item.job_id, repairRequestId, 'modify_params')
                          }}
                          onRetryOriginal={() => {
                            void resolveRepairFromQueue(item.job_id, repairRequestId, 'retry_original')
                          }}
                          onCancelJob={() => {
                            void resolveRepairFromQueue(item.job_id, repairRequestId, 'cancel_job')
                          }}
                          repairActionLoading={repairActionLoadingByJob[item.job_id] ?? null}
                          actionRow={(
                            <>
                              <button
                                type="button"
                                onClick={() => focusJob(item.job_id)}
                                className="rounded-lg border border-border-subtle px-3 py-1.5 text-xs text-text-primary transition-colors hover:bg-surface-hover"
                              >
                                {t('tasks_supervisor_open_task')}
                              </button>
                              {job?.thread_id && onOpenThread && (
                                <button
                                  type="button"
                                  onClick={() => onOpenThread(job.thread_id ?? null, item.job_id)}
                                  className="rounded-lg border border-rose-500/25 px-3 py-1.5 text-xs text-rose-100 transition-colors hover:bg-rose-500/10"
                                >
                                  {t('tasks_supervisor_open_chat')}
                                </button>
                              )}
                            </>
                          )}
                        />
                      ) : (
                        <div className="rounded-lg border border-border-subtle/70 bg-surface-base/40 px-3 py-2 text-xs text-text-muted">
                          {repairSnapshotLoading[item.job_id]
                            ? t('settings_loading')
                            : t('tasks_pending_repairs_loading_payload')}
                        </div>
                      )}
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        )}
        {supervisorCandidateTotal > 0 && (
          <div className="rounded-xl border border-amber-500/20 bg-amber-500/8 p-4">
            <div className="flex flex-wrap items-center gap-2 justify-between">
              <div>
                <div className="text-xs font-semibold uppercase tracking-wide text-amber-300">
                  {reviewEntries.length > 0 ? t('tasks_incident_heading') : t('tasks_supervisor_heading')}
                </div>
                {reviewEntries.length > 0 ? (
                  <div className="mt-1 text-sm text-text-primary">
                    {t('tasks_incident_summary')
                      .replace('{total}', String(reviewSummary.total))
                      .replace('{critical}', String(reviewSummary.critical))
                      .replace('{warning}', String(reviewSummary.warning))
                      .replace('{info}', String(reviewSummary.info))}
                  </div>
                ) : (
                  <div className="mt-1 text-xs text-text-muted">
                    {t('tasks_attention_hint')}
                  </div>
                )}
              </div>
              <button
                type="button"
                onClick={() => void loadSupervisorReview()}
                disabled={supervisorLoading}
                className="rounded-lg border border-border-subtle px-3 py-1.5 text-xs text-text-primary transition-colors hover:bg-surface-hover disabled:opacity-40"
              >
                {t(supervisorLoading ? 'tasks_supervisor_reviewing' : 'tasks_supervisor_review')}
              </button>
            </div>
            {reviewEntries.length > 0 && (
              <div className="mt-3 space-y-2">
                {reviewEntries.slice(0, 5).map(({ item, job, incident }) => {
                  const ageMinutes = item.age_seconds != null ? Math.max(1, Math.floor(item.age_seconds / 60)) : null
                  const severityClass = item.severity === 'critical'
                    ? 'border-red-500/20 bg-red-500/8'
                    : item.severity === 'warning'
                      ? 'border-amber-500/20 bg-amber-500/8'
                      : 'border-sky-500/20 bg-sky-500/8'
                  const badgeClass = item.owner === 'user'
                    ? 'bg-indigo-500/15 text-indigo-300'
                    : 'bg-surface-overlay text-text-muted'
                  return (
                    <div
                      key={item.key}
                      className={`rounded-lg border p-3 ${severityClass}`}
                    >
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="text-xs font-semibold text-text-primary">{job?.name ?? item.job_name}</span>
                        {job?.status && <StatusBadge status={job.status} />}
                        <span className={`rounded-full px-2 py-0.5 text-[10px] ${badgeClass}`}>
                          {item.owner === 'user' ? t('tasks_incident_owner_user') : t('tasks_incident_owner_system')}
                        </span>
                        {ageMinutes != null && (
                          <span className="text-[11px] text-text-muted">
                            {t('tasks_incident_age_minutes').replace('{minutes}', String(ageMinutes))}
                          </span>
                        )}
                      </div>
                      <div className="mt-2 text-xs text-text-primary">{item.summary}</div>
                      {incident?.current_step_name && (
                        <div className="mt-1 text-[11px] text-text-muted">
                          {t('tasks_incident_current_step').replace('{step}', incident.current_step_name)}
                        </div>
                      )}
                      {incident?.next_action && (
                        <div className="mt-1 text-[11px] text-text-muted">
                          {t(`tasks_incident_action_${incident.next_action}` as TranslationKey)}
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            )}
            {supervisorReview && (
              <div className="mt-4 rounded-lg border border-indigo-500/20 bg-indigo-500/8 p-4">
                <div className="flex flex-wrap items-center gap-2 justify-between">
                  <div className="text-xs font-semibold uppercase tracking-wide text-indigo-300">
                    {t('tasks_supervisor_heading')}
                  </div>
                  <span className="rounded-full bg-indigo-500/15 px-2 py-0.5 text-[10px] text-indigo-200">
                    {supervisorReview.mode === 'llm' ? t('tasks_supervisor_mode_llm') : t('tasks_supervisor_mode_heuristic')}
                  </span>
                </div>
                <div className="mt-2 text-sm text-text-primary">{supervisorReview.overview}</div>
                <div className="mt-2 text-xs text-text-muted">{supervisorReview.supervisor_message}</div>
                {supervisorReview.focus_summary && (
                  <div className="mt-2 text-[11px] text-text-muted">
                    {(() => {
                      const focusSummaryAutoRecovery = formatFocusSummaryAutoRecovery(supervisorReview.focus_summary, t)
                      return (
                        <>
                    {t('tasks_supervisor_focus_summary')}{' '}
                    {[
                      supervisorReview.focus_summary.primary_lane
                        ? `lane=${formatPrimaryLane(supervisorReview.focus_summary.primary_lane, t)}`
                        : null,
                      supervisorReview.focus_summary.top_owner
                        ? `owner=${formatFocusOwner(supervisorReview.focus_summary.top_owner, t)}`
                        : null,
                      supervisorReview.focus_summary.top_incident_type
                        ? `type=${humanizeFocusToken(supervisorReview.focus_summary.top_incident_type)}`
                        : null,
                      supervisorReview.focus_summary.top_blocker_cause
                        ? `cause=${formatFocusCause(supervisorReview.focus_summary.top_blocker_cause, t)}`
                        : null,
                      typeof supervisorReview.focus_summary.high_confidence_total === 'number'
                        ? `high=${supervisorReview.focus_summary.high_confidence_total}`
                        : null,
                      typeof supervisorReview.focus_summary.auto_recoverable_total === 'number'
                        ? `auto=${supervisorReview.focus_summary.auto_recoverable_total}`
                        : null,
                      typeof supervisorReview.focus_summary.user_wait_total === 'number'
                        ? `user_wait=${supervisorReview.focus_summary.user_wait_total}`
                        : null,
                      supervisorReview.focus_summary.top_failure_layer
                        ? `layer=${formatFailureLayer(supervisorReview.focus_summary.top_failure_layer, t)}`
                        : null,
                      supervisorReview.focus_summary.top_safe_action
                        ? `safe_action=${formatSafeActionLabel(supervisorReview.focus_summary.top_safe_action, t)}`
                        : null,
                      supervisorReview.focus_summary.top_rollback_level
                        ? `rollback=${formatRollbackLevel(supervisorReview.focus_summary.top_rollback_level, t)}`
                        : null,
                      supervisorReview.focus_summary.top_historical_rollback_level
                        ? `history_rollback=${formatRollbackLevel(supervisorReview.focus_summary.top_historical_rollback_level, t)}`
                        : null,
                      supervisorReview.focus_summary.top_historical_rollback_alignment !== undefined
                        && supervisorReview.focus_summary.top_historical_rollback_alignment !== null
                        ? `history_align=${formatHistoricalAlignment(supervisorReview.focus_summary.top_historical_rollback_alignment, t)}`
                        : null,
                      supervisorReview.focus_summary.top_rollback_target
                        ? `target=${formatRollbackTarget(supervisorReview.focus_summary.top_rollback_target)}`
                        : null,
                      supervisorReview.focus_summary.top_historical_rollback_target
                        ? `history_target=${formatRollbackTarget(supervisorReview.focus_summary.top_historical_rollback_target)}`
                        : null,
                      supervisorReview.focus_summary.top_historical_rollback_target_alignment !== undefined
                        && supervisorReview.focus_summary.top_historical_rollback_target_alignment !== null
                        ? `target_align=${formatHistoricalAlignment(supervisorReview.focus_summary.top_historical_rollback_target_alignment, t)}`
                        : null,
                      supervisorReview.focus_summary.next_best_operator_move
                        ? `next=${formatNextBestMove(supervisorReview.focus_summary.next_best_operator_move, t)}`
                        : null,
                    ].filter(Boolean).join(' · ')}
                    {focusSummaryAutoRecovery && (
                      <>
                        {' '}· {t('tasks_supervisor_auto_recovery')} {focusSummaryAutoRecovery}
                      </>
                    )}
                    {supervisorReview.focus_summary.lane_reason && (
                      <>
                        {' '}· {supervisorReview.focus_summary.lane_reason}
                      </>
                    )}
                    {supervisorReview.focus_summary.next_best_operator_reason && (
                      <>
                        {' '}· {supervisorReview.focus_summary.next_best_operator_reason}
                      </>
                    )}
                        </>
                      )
                    })()}
                  </div>
                )}
                {supervisorReview.project_playbook?.step_codes && supervisorReview.project_playbook.step_codes.length > 0 && (
                  (() => {
                    const topRecommendation = supervisorReview.recommendations[0]
                    const topJob = recentJobs.find((item) => item.id === topRecommendation?.job_id) ?? null
                    const topDossier = supervisorReview.dossiers?.find((item) => item.job_id === topRecommendation?.job_id)
                    const topPendingAuthorizationEntry = (
                      topRecommendation?.job_id
                        ? pendingAuthorizationEntries.find(({ item }) => item.job_id === topRecommendation.job_id)
                        : null
                    ) ?? pendingAuthorizationEntries[0] ?? null
                    const topAuthorizationSnapshot = topPendingAuthorizationEntry ? authorizationSnapshots[topPendingAuthorizationEntry.item.job_id] : null
                    const topAuthorizationRequestId = topAuthorizationSnapshot?.auth_request_id ?? null
                    const topPendingRepairEntry = (
                      topRecommendation?.job_id
                        ? pendingRepairEntries.find(({ item }) => item.job_id === topRecommendation.job_id)
                        : null
                    ) ?? pendingRepairEntries[0] ?? null
                    const topRepairSnapshot = topPendingRepairEntry ? repairSnapshots[topPendingRepairEntry.item.job_id] : null
                    const topRepairRequestId = topRepairSnapshot?.repair_request_id ?? null
                    const topBlockingSummary = topDossier?.resource_graph?.blocking_summary ?? []
                    const topDominantBlocker = topDossier?.resource_graph?.dominant_blocker ?? topBlockingSummary[0] ?? null
                    const topWorkspaceRequest = topDominantBlocker ? buildResourceWorkspaceRequest(topDominantBlocker) : null
                    const topRegistryRequest = topDominantBlocker ? buildRegistryWorkspaceRequest(topDominantBlocker) : null
                    const topThreadId = topRecommendation?.thread_id ?? topJob?.thread_id ?? null
                    const showOpenTaskShortcut = Boolean(topRecommendation?.job_id)
                    const showOpenChatShortcut = supervisorReview.project_playbook?.step_codes?.includes('open_chat') && Boolean(topThreadId) && Boolean(onOpenThread)
                    const showApplySafeActionShortcut = supervisorReview.project_playbook?.step_codes?.includes('apply_safe_action') && Boolean(topRecommendation?.job_id) && Boolean(topRecommendation?.safe_action)
                    const showResumeJobShortcut = supervisorReview.project_playbook?.step_codes?.includes('resume_job') && Boolean(topRecommendation?.job_id) && topRecommendation?.immediate_action === 'resume_job'
                    const showAuthorizationPromptShortcut = supervisorReview.project_playbook?.step_codes?.includes('review_and_authorize_command') && Boolean(topPendingAuthorizationEntry)
                    const showRepairPromptShortcut = supervisorReview.project_playbook?.step_codes?.includes('review_failure_and_choose_repair') && Boolean(topPendingRepairEntry)
                    const showEnvironmentDiagnosticsShortcut = (
                      supervisorReview.focus_summary?.primary_lane === 'environment_readiness'
                      || supervisorReview.focus_summary?.next_best_operator_move === 'inspect_environment_failure'
                      || supervisorReview.project_playbook?.goal === 'environment_readiness'
                      || supervisorReview.project_playbook?.next_move === 'inspect_environment_failure'
                    )

                    return (
                      <div className="mt-3 rounded-md border border-border-subtle/70 bg-surface-base/40 px-3 py-2">
                        <div className="mb-2 text-[11px] font-semibold uppercase tracking-wide text-text-muted">
                          {t('tasks_supervisor_project_playbook')}
                        </div>
                        {(supervisorReview.project_playbook.goal || supervisorReview.project_playbook.next_move) && (
                          <div className="mb-2 text-[11px] text-text-muted">
                            {[
                              supervisorReview.project_playbook.goal
                                ? `${t('tasks_supervisor_project_goal')} ${formatPrimaryLane(supervisorReview.project_playbook.goal, t)}`
                                : null,
                              supervisorReview.project_playbook.next_move
                                ? `${t('tasks_supervisor_project_next_move')} ${formatNextBestMove(supervisorReview.project_playbook.next_move, t)}`
                                : null,
                            ].filter(Boolean).join(' · ')}
                          </div>
                        )}
                        <div className="space-y-1">
                          {supervisorReview.project_playbook.step_codes.map((stepCode, index) => (
                            <div key={`project-playbook-${stepCode}-${index}`} className="text-[11px] text-text-muted">
                              {index + 1}. {formatSupervisorPlaybookStep(stepCode, t)}
                            </div>
                          ))}
                        </div>
                        {showAuthorizationPromptShortcut && topPendingAuthorizationEntry && (
                          <div className="mt-3">
                            {topAuthorizationSnapshot?.command && topAuthorizationRequestId ? (
                              <AuthorizationPromptCard
                                title={t('status_waiting_for_authorization')}
                                subtitle={topAuthorizationSnapshot.command_type || topAuthorizationSnapshot.step_key || 'command'}
                                command={topAuthorizationSnapshot.command}
                                onAuthorize={() => {
                                  void resolveAuthorizationFromQueue(topPendingAuthorizationEntry.item.job_id, topAuthorizationRequestId, 'approved')
                                }}
                                onReject={() => {
                                  void resolveAuthorizationFromQueue(topPendingAuthorizationEntry.item.job_id, topAuthorizationRequestId, 'rejected')
                                }}
                                authActionLoading={authorizationActionLoading[topPendingAuthorizationEntry.item.job_id] ?? null}
                                actionRow={(
                                  <>
                                    <button
                                      type="button"
                                      onClick={() => focusJob(topPendingAuthorizationEntry.item.job_id)}
                                      className="rounded-lg border border-border-subtle px-3 py-1.5 text-xs text-text-primary transition-colors hover:bg-surface-hover"
                                    >
                                      {t('tasks_supervisor_open_task')}
                                    </button>
                                    {(topPendingAuthorizationEntry.job?.thread_id || topThreadId) && onOpenThread && (
                                      <button
                                        type="button"
                                        onClick={() => onOpenThread(topPendingAuthorizationEntry.job?.thread_id ?? topThreadId ?? null, topPendingAuthorizationEntry.item.job_id)}
                                        className="rounded-lg border border-sky-500/25 px-3 py-1.5 text-xs text-sky-100 transition-colors hover:bg-sky-500/10"
                                      >
                                        {t('tasks_supervisor_open_chat')}
                                      </button>
                                    )}
                                  </>
                                )}
                              />
                            ) : (
                              <div className="rounded-lg border border-border-subtle/70 bg-surface-base/40 px-3 py-2 text-xs text-text-muted">
                                {authorizationSnapshotLoading[topPendingAuthorizationEntry.item.job_id]
                                  ? t('settings_loading')
                                  : t('tasks_pending_authorizations_loading_command')}
                              </div>
                            )}
                          </div>
                        )}
                        {showRepairPromptShortcut && topPendingRepairEntry && (
                          <div className="mt-3">
                            {topRepairRequestId ? (
                              <RepairPromptCard
                                title={t('status_waiting_for_repair')}
                                subtitle={topRepairSnapshot?.step_key}
                                failedCommand={topRepairSnapshot?.failed_command}
                                stderrExcerpt={topRepairSnapshot?.stderr_excerpt}
                                repairCommand={repairDrafts[topPendingRepairEntry.item.job_id] ?? ''}
                                onRepairCommandChange={(value) => {
                                  setRepairDrafts((prev) => ({ ...prev, [topPendingRepairEntry.item.job_id]: value }))
                                }}
                                onSendRetry={() => {
                                  void resolveRepairFromQueue(topPendingRepairEntry.item.job_id, topRepairRequestId, 'modify_params')
                                }}
                                onRetryOriginal={() => {
                                  void resolveRepairFromQueue(topPendingRepairEntry.item.job_id, topRepairRequestId, 'retry_original')
                                }}
                                onCancelJob={() => {
                                  void resolveRepairFromQueue(topPendingRepairEntry.item.job_id, topRepairRequestId, 'cancel_job')
                                }}
                                repairActionLoading={repairActionLoadingByJob[topPendingRepairEntry.item.job_id] ?? null}
                                actionRow={(
                                  <>
                                    <button
                                      type="button"
                                      onClick={() => focusJob(topPendingRepairEntry.item.job_id)}
                                      className="rounded-lg border border-border-subtle px-3 py-1.5 text-xs text-text-primary transition-colors hover:bg-surface-hover"
                                    >
                                      {t('tasks_supervisor_open_task')}
                                    </button>
                                    {(topPendingRepairEntry.job?.thread_id || topThreadId) && onOpenThread && (
                                      <button
                                        type="button"
                                        onClick={() => onOpenThread(topPendingRepairEntry.job?.thread_id ?? topThreadId ?? null, topPendingRepairEntry.item.job_id)}
                                        className="rounded-lg border border-rose-500/25 px-3 py-1.5 text-xs text-rose-100 transition-colors hover:bg-rose-500/10"
                                      >
                                        {t('tasks_supervisor_open_chat')}
                                      </button>
                                    )}
                                  </>
                                )}
                              />
                            ) : (
                              <div className="rounded-lg border border-border-subtle/70 bg-surface-base/40 px-3 py-2 text-xs text-text-muted">
                                {repairSnapshotLoading[topPendingRepairEntry.item.job_id]
                                  ? t('settings_loading')
                                  : t('tasks_pending_repairs_loading_payload')}
                              </div>
                            )}
                          </div>
                        )}
                        {showOpenTaskShortcut || showOpenChatShortcut || showApplySafeActionShortcut || showResumeJobShortcut || (showEnvironmentDiagnosticsShortcut && onOpenSettingsDiagnostics) || (topDominantBlocker && onOpenResourceWorkspace) ? (
                          <div className="mt-3 flex flex-wrap gap-2">
                            {showOpenTaskShortcut && topRecommendation?.job_id && (
                              <button
                                type="button"
                                onClick={() => focusJob(topRecommendation.job_id)}
                                className="rounded-lg border border-border-subtle px-3 py-1.5 text-xs text-text-primary transition-colors hover:bg-surface-hover"
                              >
                                {t('tasks_supervisor_open_task')}
                              </button>
                            )}
                            {showOpenChatShortcut && topRecommendation?.job_id && topThreadId && onOpenThread && (
                              <button
                                type="button"
                                onClick={() => onOpenThread(topThreadId, topRecommendation.job_id)}
                                className="rounded-lg border border-violet-500/25 px-3 py-1.5 text-xs text-violet-100 transition-colors hover:bg-violet-500/10"
                              >
                                {t('tasks_supervisor_open_chat')}
                              </button>
                            )}
                            {showApplySafeActionShortcut && topRecommendation?.job_id && topRecommendation?.safe_action && (
                              <button
                                type="button"
                                onClick={() => executeSupervisorSafeAction(topRecommendation.job_id, topRecommendation.safe_action as string, safeActionNextStatus(topRecommendation))}
                                className="rounded-lg bg-violet-700/80 px-3 py-1.5 text-xs text-white transition-colors hover:bg-violet-600/80"
                              >
                                {formatSafeActionLabel(topRecommendation.safe_action, t)}
                              </button>
                            )}
                            {showResumeJobShortcut && topRecommendation?.job_id && (
                              <button
                                type="button"
                                onClick={() => executeResumeById(topRecommendation.job_id)}
                                className="rounded-lg bg-emerald-700/70 px-3 py-1.5 text-xs text-white transition-colors hover:bg-emerald-600/70"
                              >
                                {t('tasks_supervisor_resume_job')}
                              </button>
                            )}
                            {showEnvironmentDiagnosticsShortcut && onOpenSettingsDiagnostics && (
                              <button
                                type="button"
                                onClick={onOpenSettingsDiagnostics}
                                className="rounded-lg border border-violet-500/25 px-3 py-1.5 text-xs text-violet-200 transition-colors hover:bg-violet-500/10"
                              >
                                {t('tasks_supervisor_open_settings')}
                              </button>
                            )}
                            {topWorkspaceRequest && (
                              <button
                                type="button"
                                onClick={() => onOpenResourceWorkspace(topWorkspaceRequest)}
                                className="rounded-lg border border-emerald-500/25 px-3 py-1.5 text-xs text-emerald-200 transition-colors hover:bg-emerald-500/10"
                              >
                                {topDominantBlocker.workspace_section === 'files'
                                  ? t('tasks_supervisor_open_data_files')
                                  : topDominantBlocker.workspace_section === 'registry'
                                    ? t('tasks_supervisor_open_resource_registry')
                                    : t('tasks_supervisor_review_recognized_resources')}
                              </button>
                            )}
                            {topRegistryRequest && topDominantBlocker.workspace_section !== 'registry' && (
                              <button
                                type="button"
                                onClick={() => onOpenResourceWorkspace(topRegistryRequest)}
                                className="rounded-lg border border-sky-500/25 px-3 py-1.5 text-xs text-sky-200 transition-colors hover:bg-sky-500/10"
                                >
                                  {t('tasks_supervisor_open_resource_registry')}
                                </button>
                              )}
                          </div>
                        ) : null}
                      </div>
                    )
                  })()
                )}
                <div className="mt-3 space-y-2">
                  {supervisorReview.recommendations.length > 0 ? supervisorReview.recommendations.map((rec) => (
                    (() => {
                      const dossier = supervisorReview.dossiers?.find((item) => item.job_id === rec.job_id)
                      const latestLog = dossier?.recent_logs && dossier.recent_logs.length > 0
                        ? dossier.recent_logs[dossier.recent_logs.length - 1]
                        : null
                      const decisionTypes = (dossier?.recent_decisions ?? []).map((item) => item.decision_type)
                      const blockingNodes = dossier?.resource_graph?.blocking_nodes ?? []
                      const blockingSummary = dossier?.resource_graph?.blocking_summary ?? []
                      const dominantBlocker = dossier?.resource_graph?.dominant_blocker ?? blockingSummary[0] ?? null
                      const dominantWorkspaceRequest = dominantBlocker ? buildResourceWorkspaceRequest(dominantBlocker) : null
                      const directRegistryRequest = dominantBlocker ? buildRegistryWorkspaceRequest(dominantBlocker) : null
                      const resourceStatusCounts = dossier?.resource_graph?.status_counts ?? {}
                      const blockingKindCounts = dossier?.resource_graph?.blocking_kind_counts ?? {}
                      const blockingCauseCounts = dossier?.resource_graph?.blocking_cause_counts ?? {}
                      const pendingSnapshot = dossier?.pending_requests
                      const latestAuthRequest = pendingSnapshot?.recent_authorizations && pendingSnapshot.recent_authorizations.length > 0
                        ? pendingSnapshot.recent_authorizations[pendingSnapshot.recent_authorizations.length - 1]
                        : null
                      const latestRepairRequest = pendingSnapshot?.recent_repairs && pendingSnapshot.recent_repairs.length > 0
                        ? pendingSnapshot.recent_repairs[pendingSnapshot.recent_repairs.length - 1]
                        : null
                      const runtimeSignals = dossier?.runtime_diagnostics ?? []
                      const latestAutoRecovery = dossier?.auto_recovery_events && dossier.auto_recovery_events.length > 0
                        ? dossier.auto_recovery_events[dossier.auto_recovery_events.length - 1]
                        : null
                      return (
                        <div
                          key={`${rec.job_id}-${rec.priority}-${rec.rollback_target}`}
                          className="rounded-md border border-indigo-500/10 bg-surface-raised/80 p-3"
                        >
                          <div className="flex flex-wrap items-center gap-2">
                            <span className="text-xs font-semibold text-text-primary">
                              {rec.priority}. {rec.job_name}
                            </span>
                            <span className="rounded-full bg-surface-overlay px-2 py-0.5 text-[10px] text-text-muted">
                              {rec.rollback_target}
                            </span>
                            {rec.recommended_action_confidence && (
                              <span className={`rounded-full px-2 py-0.5 text-[10px] ${recommendationConfidenceBadgeClass(rec.recommended_action_confidence)}`}>
                                {formatRecommendationConfidence(rec.recommended_action_confidence, t)}
                              </span>
                            )}
                            {rec.auto_recoverable && (
                              <span className="rounded-full bg-emerald-500/15 px-2 py-0.5 text-[10px] text-emerald-300">
                                {t('tasks_supervisor_auto_recoverable')}
                              </span>
                            )}
                          </div>
                          <div className="mt-2 text-xs text-text-primary">{rec.diagnosis}</div>
                          {rec.failure_layer && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_failure_layer')} {formatFailureLayer(rec.failure_layer, t)}
                            </div>
                          )}
                          {rec.rollback_level && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_rollback_level')} {formatRollbackLevel(rec.rollback_level, t)}
                            </div>
                          )}
                          {rec.reconfirmation_required !== undefined && rec.reconfirmation_required !== null && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_rollback_guidance_reconfirm')} {formatEligibilityBool(rec.reconfirmation_required, t)}
                              {(rec.historical_matches || 0) > 0 && (
                                <>
                                  {' '}· {t('tasks_rollback_guidance_history')}{' '}
                                  {t('tasks_rollback_guidance_matches').replace('{count}', String(rec.historical_matches ?? 0))}
                                </>
                              )}
                            </div>
                          )}
                          {rec.auto_recovery_kind && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_auto_recovery_kind')} {formatAutoRecoveryKind(rec.auto_recovery_kind, t)}
                            </div>
                          )}
                          {rec.recommended_action_confidence && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_recommendation_confidence')} {formatRecommendationConfidence(rec.recommended_action_confidence, t)}
                              {(rec.recommended_action_basis || []).length > 0 && (
                                <>
                                  {' '}· {t('tasks_supervisor_recommendation_basis')}{' '}
                                  {(rec.recommended_action_basis || []).map((item) => formatRecommendationBasis(item, t)).join(' · ')}
                                </>
                              )}
                            </div>
                          )}
                          {rec.safe_action_note && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_safe_action_note')} {rec.safe_action_note}
                            </div>
                          )}
                          {rec.historical_guidance && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_historical_guidance')} {rec.historical_guidance}
                            </div>
                          )}
                          {rec.historical_policy && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_historical_policy')}{' '}
                              {[
                                rec.historical_policy.preferred_safe_action
                                  ? `prefer=${formatSafeActionLabel(rec.historical_policy.preferred_safe_action, t)}`
                                  : null,
                                rec.historical_policy.current_safe_action
                                  ? `${t('tasks_supervisor_historical_policy_current_action')}=${formatSafeActionLabel(rec.historical_policy.current_safe_action, t)}`
                                  : null,
                                typeof rec.historical_policy.support_count === 'number'
                                  && typeof rec.historical_policy.total_matches === 'number'
                                  ? `support=${rec.historical_policy.support_count}/${rec.historical_policy.total_matches}`
                                  : null,
                                typeof rec.historical_policy.current_supported_count === 'number'
                                  ? `${t('tasks_supervisor_historical_policy_current_support')}=${rec.historical_policy.current_supported_count}`
                                  : null,
                                rec.historical_policy.confidence
                                  ? `confidence=${formatHistoricalConfidence(rec.historical_policy.confidence, t)}`
                                  : null,
                                `alignment=${formatHistoricalAlignment(rec.historical_policy.aligns_with_current, t)}`,
                                rec.historical_policy.preferred_rollback_level
                                  ? `${t('tasks_supervisor_historical_policy_rollback')}=${formatRollbackLevel(rec.historical_policy.preferred_rollback_level, t)}`
                                  : null,
                                rec.historical_policy.current_rollback_level
                                  ? `${t('tasks_supervisor_historical_policy_current_rollback')}=${formatRollbackLevel(rec.historical_policy.current_rollback_level, t)}`
                                  : null,
                                typeof rec.historical_policy.rollback_level_supported_count === 'number'
                                  ? `${t('tasks_supervisor_historical_policy_rollback_support')}=${rec.historical_policy.rollback_level_supported_count}`
                                  : null,
                                rec.historical_policy.rollback_level_aligns_with_current !== undefined
                                  && rec.historical_policy.rollback_level_aligns_with_current !== null
                                  ? `${t('tasks_supervisor_historical_policy_rollback_alignment')}=${formatHistoricalAlignment(rec.historical_policy.rollback_level_aligns_with_current, t)}`
                                  : null,
                                rec.historical_policy.preferred_rollback_target
                                  ? `${t('tasks_supervisor_historical_policy_target')}=${formatRollbackTarget(rec.historical_policy.preferred_rollback_target)}`
                                  : null,
                                rec.historical_policy.current_rollback_target
                                  ? `${t('tasks_supervisor_historical_policy_current_target')}=${formatRollbackTarget(rec.historical_policy.current_rollback_target)}`
                                  : null,
                                typeof rec.historical_policy.rollback_target_supported_count === 'number'
                                  ? `${t('tasks_supervisor_historical_policy_target_support')}=${rec.historical_policy.rollback_target_supported_count}`
                                  : null,
                                rec.historical_policy.rollback_target_aligns_with_current !== undefined
                                  && rec.historical_policy.rollback_target_aligns_with_current !== null
                                  ? `${t('tasks_supervisor_historical_policy_target_alignment')}=${formatHistoricalAlignment(rec.historical_policy.rollback_target_aligns_with_current, t)}`
                                  : null,
                              ].filter(Boolean).join(' · ')}
                            </div>
                          )}
                          <div className="mt-1 text-[11px] text-text-muted">
                            {t('tasks_supervisor_next_action')} {formatSupervisorAction(rec.immediate_action, t)}
                          </div>
                          <div className="mt-1 text-[11px] text-text-muted">
                            {t('tasks_supervisor_why_now')} {rec.why_now}
                          </div>
                          {rec.recovery_playbook?.step_codes && rec.recovery_playbook.step_codes.length > 0 && (
                            <div className="mt-2 rounded-md border border-border-subtle/70 bg-surface-base/40 px-3 py-2">
                              <div className="mb-2 text-[11px] font-semibold uppercase tracking-wide text-text-muted">
                                {t('tasks_supervisor_playbook')}
                              </div>
                              <div className="space-y-1">
                                {rec.recovery_playbook.step_codes.map((stepCode, index) => (
                                  <div key={`${rec.job_id}-playbook-${stepCode}-${index}`} className="text-[11px] text-text-muted">
                                    {index + 1}. {formatSupervisorPlaybookStep(stepCode, t)}
                                  </div>
                                ))}
                              </div>
                            </div>
                          )}
                          {rec.safe_action_eligibility && rec.incident_type === 'resume_failed' && (
                            <div className="mt-2 rounded-md border border-border-subtle/70 bg-surface-base/40 px-3 py-2">
                              <div className="mb-2 text-[11px] font-semibold uppercase tracking-wide text-text-muted">
                                {t('tasks_supervisor_eligibility')}
                              </div>
                              <div className="space-y-1 text-[11px] text-text-muted">
                                <div>
                                  {t('tasks_supervisor_eligibility_status')}{' '}
                                  {formatEligibilityBool(rec.safe_action_eligibility.eligible, t)}
                                </div>
                                <div>
                                  {t('tasks_supervisor_eligibility_current_job_status')}{' '}
                                  {formatTaskStatusLabel(rec.safe_action_eligibility.current_job_status, t)}
                                </div>
                                <div>
                                  {t('tasks_supervisor_eligibility_retryable_statuses')}{' '}
                                  {formatStatusList(rec.safe_action_eligibility.retryable_job_statuses, t) || 'none'}
                                </div>
                                <div>
                                  {t('tasks_supervisor_eligibility_resolved_signal')}{' '}
                                  {formatEligibilityBool(rec.safe_action_eligibility.has_resolved_pending_signal, t)}
                                </div>
                                <div>
                                  {t('tasks_supervisor_eligibility_pending_reference')}{' '}
                                  {formatEligibilityBool(rec.safe_action_eligibility.has_pending_request_reference, t)}
                                </div>
                                <div>
                                  {t('tasks_supervisor_eligibility_resolved_types')}{' '}
                                  {(rec.safe_action_eligibility.resolved_pending_types || [])
                                    .map((item) => formatPendingTypeLabel(item, t))
                                    .join(', ') || 'none'}
                                </div>
                                <div>
                                  {t('tasks_supervisor_eligibility_pending_types')}{' '}
                                  {(rec.safe_action_eligibility.pending_reference_types || [])
                                    .map((item) => formatPendingTypeLabel(item, t))
                                    .join(', ') || 'none'}
                                </div>
                                {(rec.safe_action_eligibility.blocking_reasons || []).length > 0 && (
                                  <div>
                                    {t('tasks_supervisor_eligibility_blockers')}{' '}
                                    {(rec.safe_action_eligibility.blocking_reasons || [])
                                      .map((item) => formatResumeRetryBlocker(item, t))
                                      .join(' · ')}
                                  </div>
                                )}
                              </div>
                            </div>
                          )}
                          {rec.dossier_summary && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_dossier')} {rec.dossier_summary}
                            </div>
                          )}
                          {dossier?.current_step?.display_name && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_current_step')} {dossier.current_step.display_name}
                            </div>
                          )}
                          {dossier?.impacted_step_keys && dossier.impacted_step_keys.length > 0 && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_impacted_steps')} {dossier.impacted_step_keys.join(', ')}
                            </div>
                          )}
                          {Object.keys(resourceStatusCounts).length > 0 && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_resource_status_counts')} {Object.entries(resourceStatusCounts)
                                .map(([key, value]) => `${key}=${value}`)
                                .join(' · ')}
                            </div>
                          )}
                          {Object.keys(blockingKindCounts).length > 0 && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_resource_blocker_kinds')} {Object.entries(blockingKindCounts)
                                .map(([key, value]) => `${key}=${value}`)
                                .join(' · ')}
                            </div>
                          )}
                          {Object.keys(blockingCauseCounts).length > 0 && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_resource_blocker_causes')} {Object.entries(blockingCauseCounts)
                                .map(([key, value]) => `${key}=${value}`)
                                .join(' · ')}
                            </div>
                          )}
                          {blockingNodes.length > 0 && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_resource_blockers')} {blockingNodes.map((item) => formatResourceBlocker(item)).join(', ')}
                            </div>
                          )}
                          {dominantBlocker && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_resource_focus')}{' '}
                              {[
                                dominantBlocker.label || dominantBlocker.kind || dominantBlocker.id,
                                dominantBlocker.cause ? `cause=${formatFocusCause(dominantBlocker.cause, t)}` : null,
                                dominantBlocker.recommended_action
                                  ? `next=${formatNextBestMove(dominantBlocker.recommended_action, t)}`
                                  : null,
                              ].filter(Boolean).join(' · ')}
                              {dominantBlocker.why_blocked ? ` · ${dominantBlocker.why_blocked}` : ''}
                              {dominantBlocker.operator_hint ? ` · ${dominantBlocker.operator_hint}` : ''}
                            </div>
                          )}
                          {dominantBlocker && onOpenResourceWorkspace && (
                            <div className="mt-2 flex flex-wrap gap-2">
                              {dominantWorkspaceRequest && (
                                <button
                                  type="button"
                                  onClick={() => onOpenResourceWorkspace(dominantWorkspaceRequest)}
                                  className="rounded-lg border border-emerald-500/25 px-3 py-1.5 text-xs text-emerald-200 transition-colors hover:bg-emerald-500/10"
                                >
                                  {dominantBlocker.workspace_section === 'files'
                                    ? t('tasks_supervisor_open_data_files')
                                    : dominantBlocker.workspace_section === 'registry'
                                      ? t('tasks_supervisor_open_resource_registry')
                                      : t('tasks_supervisor_review_recognized_resources')}
                                </button>
                              )}
                              {directRegistryRequest && dominantBlocker.workspace_section !== 'registry' && (
                                <button
                                  type="button"
                                  onClick={() => onOpenResourceWorkspace(directRegistryRequest)}
                                  className="rounded-lg border border-sky-500/25 px-3 py-1.5 text-xs text-sky-200 transition-colors hover:bg-sky-500/10"
                                >
                                  {t('tasks_supervisor_open_resource_registry')}
                                </button>
                              )}
                            </div>
                          )}
                          {blockingSummary.length > 0 && (
                            <div className="mt-1 space-y-1 text-[11px] text-text-muted">
                              {blockingSummary.slice(0, 3).map((item, index) => (
                                <div key={`${rec.job_id}-resource-summary-${item.id}-${index}`}>
                                  {index + 1}. {[
                                    item.label || item.kind || item.id,
                                    item.cause ? `cause=${formatFocusCause(item.cause, t)}` : null,
                                    item.status || null,
                                  ].filter(Boolean).join(' · ')}
                                  {item.preferred_candidate && (
                                    <>
                                      {' '}· {t('tasks_supervisor_resource_preferred_candidate')} {formatResourceCandidate(item.preferred_candidate)}
                                    </>
                                  )}
                                  {item.derived_from_preview && item.derived_from_preview.length > 0 && (
                                    <>
                                      {' '}· {t('tasks_supervisor_resource_upstream')} {item.derived_from_preview.join(' | ')}
                                    </>
                                  )}
                                </div>
                              ))}
                            </div>
                          )}
                          {pendingSnapshot && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_pending_requests')} {[
                                pendingSnapshot.active_type ? `active=${formatPendingTypeLabel(pendingSnapshot.active_type, t)}` : null,
                                pendingSnapshot.auth_request_id ? `auth=${pendingSnapshot.auth_request_id}` : null,
                                pendingSnapshot.repair_request_id ? `repair=${pendingSnapshot.repair_request_id}` : null,
                                pendingSnapshot.diagnostic_kinds && pendingSnapshot.diagnostic_kinds.length > 0
                                  ? `signals=${pendingSnapshot.diagnostic_kinds.map((item) => humanizeFocusToken(item)).join(', ')}`
                                  : null,
                              ].filter(Boolean).join(' · ')}
                            </div>
                          )}
                          {(latestAuthRequest || latestRepairRequest) && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_recent_requests')} {[
                                latestAuthRequest
                                  ? `auth:${latestAuthRequest.command_type || latestAuthRequest.id || 'request'}=${formatRequestStatusLabel(latestAuthRequest.status)}`
                                  : null,
                                latestRepairRequest
                                  ? `repair:${latestRepairRequest.id || 'request'}=${formatRequestStatusLabel(latestRepairRequest.status)}`
                                  : null,
                              ].filter(Boolean).join(' · ')}
                            </div>
                          )}
                          {decisionTypes.length > 0 && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_recent_decisions')} {decisionTypes.map((item) => humanizeFocusToken(item)).join(', ')}
                            </div>
                          )}
                          {runtimeSignals.length > 0 && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_runtime_diagnostics')} {runtimeSignals.map((item) => formatRuntimeDiagnostic(item, t)).join(', ')}
                            </div>
                          )}
                          {dossier?.rollback_hint?.suggested_level && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_rollback_hint')}{' '}
                              {[
                                formatRollbackLevel(dossier.rollback_hint.suggested_level, t),
                                dossier.rollback_hint.reason || null,
                              ].filter(Boolean).join(' · ')}
                            </div>
                          )}
                          {dossier?.execution_confirmation_overview && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_execution_overview')}{' '}
                              {t('tasks_confirmation_overview_summary')
                                .replace('{abstract}', String(dossier.execution_confirmation_overview.abstract_step_count ?? 0))
                                .replace('{ir}', String(dossier.execution_confirmation_overview.execution_ir_step_count ?? 0))
                                .replace('{groups}', String(dossier.execution_confirmation_overview.execution_group_count ?? 0))
                                .replace('{per_sample}', String(dossier.execution_confirmation_overview.per_sample_step_count ?? 0))
                                .replace('{aggregate}', String(dossier.execution_confirmation_overview.aggregate_step_count ?? 0))
                                .replace('{added}', String(dossier.execution_confirmation_overview.added_group_count ?? 0))
                                .replace('{changed}', String(dossier.execution_confirmation_overview.changed_group_count ?? 0))}
                            </div>
                          )}
                          {dossier?.execution_plan_changes && dossier.execution_plan_changes.length > 0 && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_execution_change_counts')}{' '}
                              {[
                                `fan_out=${dossier.execution_plan_changes.filter((item) => (item.change_kinds ?? []).includes('fan_out')).length}`,
                                `aggregate=${dossier.execution_plan_changes.filter((item) => (item.change_kinds ?? []).includes('aggregate')).length}`,
                                `auto_injected=${dossier.execution_plan_changes.filter((item) => (item.change_kinds ?? []).includes('auto_injected')).length}`,
                              ].join(' · ')}
                            </div>
                          )}
                          {latestAutoRecovery && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_auto_recovery')} {formatAutoRecoveryEvent(latestAutoRecovery, t)}
                            </div>
                          )}
                          {dossier?.similar_resolutions && dossier.similar_resolutions.length > 0 && (
                            <div className="mt-1 text-[11px] text-text-muted">
                              {t('tasks_supervisor_similar_resolutions')} {dossier.similar_resolutions
                                .slice(0, 2)
                                .map((item) => formatSimilarResolution(item, t))
                                .join(', ')}
                            </div>
                          )}
                          {latestLog?.line && (
                            <div className="mt-1 line-clamp-2 text-[11px] text-text-muted">
                              {t('tasks_supervisor_latest_log')} {latestLog.line}
                            </div>
                          )}
                          <div className="mt-3 flex flex-wrap gap-2">
                            <button
                              type="button"
                              onClick={() => focusJob(rec.job_id)}
                              className="rounded-lg border border-border-subtle px-3 py-1.5 text-xs text-text-primary transition-colors hover:bg-surface-hover"
                            >
                              {t('tasks_supervisor_open_task')}
                            </button>
                            {rec.safe_action === 'step_reenter' && (
                              <button
                                type="button"
                                onClick={() => executeSupervisorSafeAction(rec.job_id, 'step_reenter', 'queued')}
                                className="rounded-lg bg-indigo-700/70 px-3 py-1.5 text-xs text-white transition-colors hover:bg-indigo-600/70"
                              >
                                {t('tasks_supervisor_retry_from_step')}
                              </button>
                            )}
                            {rec.safe_action === 'refresh_execution_graph' && (
                              <button
                                type="button"
                                onClick={() => executeSupervisorSafeAction(rec.job_id, 'refresh_execution_graph')}
                                className="rounded-lg bg-sky-700/70 px-3 py-1.5 text-xs text-white transition-colors hover:bg-sky-600/70"
                              >
                                {t('tasks_supervisor_refresh_execution_graph')}
                              </button>
                            )}
                            {rec.safe_action === 'refresh_execution_plan' && (
                              <button
                                type="button"
                                onClick={() => executeSupervisorSafeAction(rec.job_id, 'refresh_execution_plan')}
                                className="rounded-lg bg-cyan-700/70 px-3 py-1.5 text-xs text-white transition-colors hover:bg-cyan-600/70"
                              >
                                {t('tasks_supervisor_refresh_execution_plan')}
                              </button>
                            )}
                            {rec.safe_action === 'revalidate_abstract_plan' && (
                              <button
                                type="button"
                                onClick={() => executeSupervisorSafeAction(rec.job_id, 'revalidate_abstract_plan')}
                                className="rounded-lg bg-amber-700/80 px-3 py-1.5 text-xs text-white transition-colors hover:bg-amber-600/80"
                              >
                                {t('tasks_supervisor_revalidate_abstract_plan')}
                              </button>
                            )}
                            {rec.safe_action === 'normalize_orphan_pending_state' && (
                              <button
                                type="button"
                                onClick={() => executeSupervisorSafeAction(rec.job_id, 'normalize_orphan_pending_state', 'interrupted')}
                                className="rounded-lg bg-orange-700/80 px-3 py-1.5 text-xs text-white transition-colors hover:bg-orange-600/80"
                              >
                                {t('tasks_supervisor_normalize_orphan_pending_state')}
                              </button>
                            )}
                            {rec.safe_action === 'normalize_terminal_state' && (
                              <button
                                type="button"
                                onClick={() => executeSupervisorSafeAction(rec.job_id, 'normalize_terminal_state', 'completed')}
                                className="rounded-lg bg-emerald-700/70 px-3 py-1.5 text-xs text-white transition-colors hover:bg-emerald-600/70"
                              >
                                {t('tasks_supervisor_normalize_terminal_state')}
                              </button>
                            )}
                            {rec.safe_action === 'retry_resume_chain' && (
                              <button
                                type="button"
                                onClick={() => executeSupervisorSafeAction(rec.job_id, 'retry_resume_chain', rec.safe_action_eligibility?.current_job_status || 'interrupted')}
                                className="rounded-lg bg-violet-700/80 px-3 py-1.5 text-xs text-white transition-colors hover:bg-violet-600/80"
                              >
                                {t('tasks_supervisor_retry_resume_chain')}
                              </button>
                            )}
                            {rec.immediate_action === 'resume_job' && (
                              <button
                                type="button"
                                onClick={() => executeResumeById(rec.job_id)}
                                className="rounded-lg bg-emerald-700/70 px-3 py-1.5 text-xs text-white transition-colors hover:bg-emerald-600/70"
                              >
                                {t('tasks_supervisor_resume_job')}
                              </button>
                            )}
                            {rec.owner === 'user' && rec.thread_id && onOpenThread && (
                              <button
                                type="button"
                                onClick={() => onOpenThread(rec.thread_id ?? null, rec.job_id)}
                                className="rounded-lg border border-indigo-500/20 px-3 py-1.5 text-xs text-indigo-200 transition-colors hover:bg-indigo-500/10"
                              >
                                {t('tasks_supervisor_open_chat')}
                              </button>
                            )}
                          </div>
                        </div>
                      )
                    })()
                  )) : (
                    <div className="text-xs text-text-muted">{t('tasks_supervisor_empty')}</div>
                  )}
                </div>
              </div>
            )}
          </div>
        )}
        {jobs.length === 0 ? (
          <div className="bg-surface-raised rounded-xl p-8 text-center">
            <p className="text-sm text-text-muted">{t('tasks_empty')}</p>
          </div>
        ) : (
          jobs.map((job) => (
            <JobCard
              key={job.id}
              job={job}
              autoExpand={autoExpandId === job.id}
              detailRefreshNonce={detailRefreshNonce}
              onAutoExpandConsumed={() => setAutoExpandId(null)}
              onDelete={(j) => setConfirmDelete({ jobId: j.id, jobName: j.name ?? j.id, outputDir: j.output_dir })}
              onJobStateSync={syncJobState}
              onResume={executeResume}
              onOpenThread={onOpenThread}
            />
          ))
        )}
      </div>

      <div className="shrink-0 border-t border-border-subtle px-5 py-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="text-xs text-text-muted">
            {summaryText}
          </div>
          <div className="flex items-center gap-2">
            <span className="text-xs text-text-muted">{pageText}</span>
            <button
              type="button"
              onClick={() => setPage((prev) => Math.max(1, prev - 1))}
              disabled={prevDisabled || jobsLoading}
              className="rounded-lg border border-border-subtle px-3 py-1.5 text-xs text-text-primary transition-colors hover:bg-surface-hover disabled:cursor-not-allowed disabled:opacity-40"
            >
              {t('tasks_prev_page')}
            </button>
            <button
              type="button"
              onClick={() => setPage((prev) => prev + 1)}
              disabled={nextDisabled || jobsLoading}
              className="rounded-lg border border-border-subtle px-3 py-1.5 text-xs text-text-primary transition-colors hover:bg-surface-hover disabled:cursor-not-allowed disabled:opacity-40"
            >
              {t('tasks_next_page')}
            </button>
          </div>
        </div>
      </div>

      {/* Delete confirmation dialog */}
      {confirmDelete && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
          <div className="bg-surface-raised border border-border-subtle rounded-xl p-6 max-w-md w-full mx-4 shadow-xl">
            <h3 className="text-sm font-semibold text-text-primary mb-2">{t('tasks_delete_confirm_title')}</h3>
            <p className="text-xs text-text-muted mb-3">{t('tasks_delete_confirm_body')}</p>
            <div className="text-xs text-text-primary mb-1 font-medium truncate">{confirmDelete.jobName}</div>
            {confirmDelete.outputDir && (
              <div className="text-xs text-text-muted mb-4">
                <span className="font-medium">{t('tasks_delete_output_dir')}:</span>{' '}
                <span className="font-mono break-all">{confirmDelete.outputDir}</span>
              </div>
            )}
            <div className="flex justify-end gap-2">
              <button
                onClick={() => setConfirmDelete(null)}
                className="px-3 py-1.5 text-xs rounded-lg border border-border-subtle text-text-muted hover:bg-surface-hover transition-colors"
              >
                {t('tasks_cancel')}
              </button>
              <button
                onClick={executeDelete}
                className="px-3 py-1.5 text-xs rounded-lg bg-red-600/80 hover:bg-red-600 text-white transition-colors"
              >
                {t('tasks_delete')}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
