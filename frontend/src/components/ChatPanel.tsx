import { useEffect, useRef, useState } from 'react'
import { ChevronsUpDown } from 'lucide-react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import rehypeHighlight from 'rehype-highlight'
import type { WSMessage } from '../hooks/useWebSocket'
import ErrorRecoveryPanel from './ErrorRecoveryPanel'
import ResultViewer, { type ResultItem } from './ResultViewer'
import { useLanguage } from '../i18n/LanguageContext'
import type { Lang } from '../i18n/translations'
import { buildTaskAttentionReminderMessage, type TaskAttentionReminder } from '../lib/taskAttention'

type ConfirmationPhase = 'abstract' | 'execution'

interface PlanSummary {
  has_execution_ir?: boolean
  has_expanded_dag?: boolean
  node_count?: number
  group_count?: number
  ambiguity_count?: number
  memory_review_count?: number
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
  step_key?: string
  step_type?: string
  display_name?: string
  description?: string | null
  scope?: string | null
  execution_kind?: string | null
  aggregation_mode?: string | null
  input_semantics?: string[] | null
  depends_on?: string[] | null
}

interface ExecutionPlanDeltaGroupItem {
  group_key?: string
  display_name?: string
  step_type?: string
  change_kinds?: string[]
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

interface PlanItem {
  step_key?: string
  step_type?: string
  display_name?: string
  name?: string
  description?: string
}

interface ExecutionPlanChangeItem {
  group_key?: string
  step_type?: string
  display_name?: string
  change_kinds?: string[]
  summary?: string | null
  depends_on?: string[]
  node_count?: number | null
  scope?: string | null
  fan_out_mode?: string | null
  aggregate_mode?: string | null
  auto_injected_reasons?: string[]
  auto_injected_cause?: string | null
}

interface ExecutionAmbiguityReviewItem {
  step_key?: string
  step_type?: string
  display_name?: string
  slot_name?: string | null
  binding_key?: string | null
  primary_path?: string | null
  secondary_path?: string | null
  score_gap?: number | null
  candidate_count?: number | null
  description?: string | null
}

interface ExecutionMemoryBindingReviewItem {
  step_key?: string
  step_type?: string
  display_name?: string
  slot_name?: string | null
  binding_key?: string | null
  fact_key?: string | null
  confirmed_path?: string | null
  candidate_path?: string | null
  candidate_count?: number | null
  description?: string | null
}

interface ExecutionSemanticGuardrails {
  ambiguity_count?: number | null
  ambiguity_reviews?: ExecutionAmbiguityReviewItem[] | null
  memory_review_count?: number | null
  memory_binding_reviews?: ExecutionMemoryBindingReviewItem[] | null
  project_memory_summary?: {
    stable_fact_count?: number | null
    memory_pattern_count?: number | null
    memory_preference_count?: number | null
    memory_link_count?: number | null
    resource_link_count?: number | null
    artifact_link_count?: number | null
    runtime_link_count?: number | null
  } | null
}

interface Message {
  id: string
  role: 'user' | 'assistant' | 'system'
  content: string
  plan?: PlanItem[]
  requiresConfirmation?: boolean
  confirmationPhase?: ConfirmationPhase
  executionPlanSummary?: PlanSummary | null
  executionConfirmationOverview?: ExecutionConfirmationOverview | null
  executionIrReview?: ExecutionIrReviewItem[] | null
  executionPlanDelta?: ExecutionPlanDelta | null
  executionPlanChanges?: ExecutionPlanChangeItem[] | null
  executionSemanticGuardrails?: ExecutionSemanticGuardrails | null
  results?: ResultItem[]
  newFilesEvent?: { count: number; types: Record<string, number> }
}

interface Props {
  ws: ReturnType<typeof import('../hooks/useWebSocket').useWebSocket>
  projectId: string | null
  projectName: string | null
  lang: Lang
  threadTitle?: string | null
  llmReachable?: boolean
  onJobStarted?: (jobId: string) => void
  onAnalysisResult?: () => void
  onNavigateToSettings?: () => void
  onNavigateToData?: () => void
  onOpenThreadDrawer?: () => void
  taskAttentionReminders?: TaskAttentionReminder[]
}

export default function ChatPanel({ ws, projectId, lang, threadTitle, llmReachable = true, onJobStarted, onAnalysisResult, onNavigateToSettings, onNavigateToData, onOpenThreadDrawer, taskAttentionReminders = [] }: Props) {
  const { t } = useLanguage()
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [streaming, setStreaming] = useState(false)
  const [skillSaveOffer, setSkillSaveOffer] = useState<{ jobId: string; jobName: string } | null>(null)
  const [errorRecovery, setErrorRecovery] = useState<{
    jobId: string
    step: string
    command: string
    stderr: string
    attemptHistory: { command: string; stderr: string }[]
  } | null>(null)
  const [memorySavePrompt, setMemorySavePrompt] = useState<{
    jobId: string
    trigger: string
    approach: string
  } | null>(null)
  const bottomRef = useRef<HTMLDivElement>(null)
  const currentAssistant = useRef('')
  const redirectTimerRef = useRef<number | null>(null)
  const seenCommandAuthNoticesRef = useRef<Set<string>>(new Set())
  const seenTaskReminderKeysRef = useRef<Set<string>>(new Set())

  const upsertLastAssistant = (
    prev: Message[],
    updater: (message: Message) => void,
  ) => {
    const updated = [...prev]
    const last = updated[updated.length - 1]
    if (last?.role === 'assistant') {
      updater(last)
      return updated
    }
    const nextMessage: Message = {
      id: Date.now().toString(),
      role: 'assistant',
      content: '',
    }
    updater(nextMessage)
    updated.push(nextMessage)
    return updated
  }

  const clearConfirmationCards = (prev: Message[]) =>
    prev.map((message) => (
      message.requiresConfirmation
        ? {
            ...message,
            requiresConfirmation: false,
          }
        : message
    ))

  const formatPlanTitle = (item: PlanItem) =>
    item.display_name || item.name || item.step_key || item.step_type || t('chat_plan_step_fallback')

  const formatPlanMeta = (item: PlanItem) => {
    const parts = [item.step_type, item.description].filter(Boolean)
    return parts.join(' · ')
  }

  const formatExecutionPlanChangeKind = (kind: string) => {
    const keyMap = {
      fan_out: 'tasks_confirmation_change_fan_out',
      aggregate: 'tasks_confirmation_change_aggregate',
      auto_injected: 'tasks_confirmation_change_auto_injected',
    } as const
    return t(keyMap[kind as keyof typeof keyMap] ?? 'tasks_confirmation_change_auto_injected')
  }

  const formatExecutionPlanChangeSummary = (item: ExecutionPlanChangeItem) => {
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
      const causeMap = {
        missing_hisat2_index: 'tasks_confirmation_change_detail_auto_injected_missing_hisat2_index',
        missing_star_genome: 'tasks_confirmation_change_detail_auto_injected_missing_star_genome',
        derivable_hisat2_index: 'tasks_confirmation_change_detail_auto_injected_derivable_hisat2_index',
        derivable_star_genome: 'tasks_confirmation_change_detail_auto_injected_derivable_star_genome',
        stale_derived_resource: 'tasks_confirmation_change_detail_auto_injected_stale_derived_resource',
      } as const
      if (item.auto_injected_cause && causeMap[item.auto_injected_cause as keyof typeof causeMap]) {
        detailParts.push(t(causeMap[item.auto_injected_cause as keyof typeof causeMap]))
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

  const formatExecutionSemanticReviewTitle = (
    item: { display_name?: string | null; step_key?: string | null; step_type?: string | null },
  ) => item.display_name || item.step_key || item.step_type || t('chat_plan_step_fallback')

  const buildExecutionPlanDeltaStatusMap = (delta: ExecutionPlanDelta | null | undefined) => {
    const statusMap: Record<string, 'added' | 'changed'> = {}
    for (const item of delta?.added_groups ?? []) {
      if (item.group_key) statusMap[item.group_key] = 'added'
    }
    for (const item of delta?.changed_groups ?? []) {
      if (item.group_key && !statusMap[item.group_key]) statusMap[item.group_key] = 'changed'
    }
    return statusMap
  }

  useEffect(() => {
    // subscribe is a stable useCallback ref — this effect runs once
    const unsub = ws.subscribe((msg: WSMessage) => {
      if (msg.type === 'history') {
        // Pre-populate messages from thread history
        const hist = (msg.messages as Array<{ role: string; content: string }>) || []
        setMessages(hist.map((m, i) => ({
          id: `hist-${i}`,
          role: m.role as Message['role'],
          content: m.content,
        })))
      } else if (msg.type === 'start') {
        currentAssistant.current = ''
        setMessages((prev) => [
          ...prev,
          { id: Date.now().toString(), role: 'assistant', content: '' },
        ])
        setStreaming(true)
      } else if (msg.type === 'token') {
        currentAssistant.current += (msg.content as string) || ''
        setMessages((prev) => {
          const updated = [...prev]
          const last = updated[updated.length - 1]
          if (last?.role === 'assistant') last.content = currentAssistant.current
          return updated
        })
      } else if (msg.type === 'end') {
        setStreaming(false)
      } else if (msg.type === 'analysis_result') {
        const item: ResultItem = {
          kind: msg.kind as ResultItem['kind'],
          path: msg.path as string,
          filename: msg.filename as string,
          step: msg.step as string,
        }
        onAnalysisResult?.()
        setMessages((prev) => {
          const updated = [...prev]
          const last = updated[updated.length - 1]
          if (last?.role === 'assistant') {
            last.results = [...(last.results ?? []), item]
          } else {
            updated.push({ id: Date.now().toString(), role: 'assistant', content: '', results: [item] })
          }
          return [...updated]
        })
      } else if (msg.type === 'command_auth') {
        const authKey = String((msg.auth_request_id as string) || (msg.job_id as string) || (msg.command as string) || Date.now())
        if (!seenCommandAuthNoticesRef.current.has(authKey)) {
          seenCommandAuthNoticesRef.current.add(authKey)
          setMessages((prev) => [
            ...prev,
            {
              id: `command-auth-${authKey}`,
              role: 'system',
              content: lang === 'zh'
                ? '任务需要命令授权。请在右侧任务面板中处理，主聊天窗口可继续对话。'
                : 'A task is waiting for command authorization. Use the task tray on the right to respond; chat can continue here.',
            },
          ])
        }
      } else if (msg.type === 'pending_state_cleared') {
        const fields = Array.isArray(msg.fields) ? msg.fields.map((item) => String(item)) : []
        if (fields.includes('error_recovery')) {
          setErrorRecovery(null)
        }
        if (fields.includes('analysis_plan')) {
          setMessages((prev) => clearConfirmationCards(prev))
        }
      } else if (msg.type === 'error_recovery_human') {
        setErrorRecovery({
          jobId: msg.job_id as string,
          step: msg.step as string,
          command: msg.command as string,
          stderr: msg.stderr as string,
          attemptHistory: (msg.attempt_history as { command: string; stderr: string }[]) || [],
        })
      } else if (msg.type === 'suggest_memory_save') {
        setMemorySavePrompt({
          jobId: msg.job_id as string,
          trigger: msg.trigger_suggestion as string,
          approach: msg.approach_suggestion as string,
        })
      } else if (msg.type === 'offer_skill_save') {
        setSkillSaveOffer({ jobId: msg.job_id as string, jobName: msg.job_name as string })
      } else if (msg.type === 'job_started') {
        setMessages((prev) => clearConfirmationCards(prev))
        onJobStarted?.(msg.job_id as string)
      } else if (msg.type === 'analysis_complete') {
        setMessages((prev) => clearConfirmationCards(prev))
        const status = msg.status as string
        const steps = msg.steps_total as number
        const outDir = msg.output_dir as string | null
        const err = msg.error as string | null
        const jobName = msg.job_name as string
        let summary: string
        if (status === 'completed') {
          summary = `✅ **${jobName}** complete — ${steps} step${steps !== 1 ? 's' : ''} ran.${outDir ? `\n📁 Outputs: \`${outDir}\`` : ''}`
        } else if (status === 'failed') {
          summary = `❌ **${jobName}** failed after ${steps} step${steps !== 1 ? 's' : ''}.${err ? `\n${err}` : ''}`
        } else {
          summary = `⚠️ **${jobName}** ended with status: ${status}.`
        }
        setMessages((prev) => [
          ...prev,
          { id: Date.now().toString(), role: 'assistant', content: summary },
        ])
      } else if (msg.type === 'plan') {
        setMessages((prev) => upsertLastAssistant(clearConfirmationCards(prev), (last) => {
          last.plan = ((msg.plan as PlanItem[]) || []).filter((item): item is PlanItem => Boolean(item))
          last.requiresConfirmation = true
          last.confirmationPhase = last.confirmationPhase || 'abstract'
          if (last.confirmationPhase !== 'execution') {
            last.executionPlanSummary = null
            last.executionConfirmationOverview = null
            last.executionIrReview = null
            last.executionPlanDelta = null
            last.executionPlanChanges = null
            last.executionSemanticGuardrails = null
          }
        }))
      } else if (msg.type === 'execution_plan') {
        setMessages((prev) => upsertLastAssistant(clearConfirmationCards(prev), (last) => {
          last.requiresConfirmation = true
          last.confirmationPhase = 'execution'
          last.executionPlanSummary = (
            (msg.execution_plan_summary as PlanSummary | undefined)
            ?? ((msg.execution_plan as { summary?: PlanSummary } | undefined)?.summary)
            ?? null
          )
          last.executionConfirmationOverview = (
            (msg.execution_confirmation_overview as ExecutionConfirmationOverview | undefined)
            ?? ((msg.execution_plan as { review_overview?: ExecutionConfirmationOverview } | undefined)?.review_overview)
            ?? null
          )
          last.executionIrReview = (
            (msg.execution_ir_review as ExecutionIrReviewItem[] | undefined)
            ?? ((msg.execution_plan as { review_ir?: ExecutionIrReviewItem[] } | undefined)?.review_ir)
            ?? null
          )
          last.executionPlanDelta = (
            (msg.execution_plan_delta as ExecutionPlanDelta | undefined)
            ?? ((msg.execution_plan as { review_delta?: ExecutionPlanDelta } | undefined)?.review_delta)
            ?? null
          )
          last.executionPlanChanges = (
            (msg.execution_plan_changes as ExecutionPlanChangeItem[] | undefined)
            ?? ((msg.execution_plan as { review_changes?: ExecutionPlanChangeItem[] } | undefined)?.review_changes)
            ?? null
          )
          last.executionSemanticGuardrails = (
            (msg.execution_semantic_guardrails as ExecutionSemanticGuardrails | undefined)
            ?? ((msg.execution_plan as { semantic_guardrails?: ExecutionSemanticGuardrails } | undefined)?.semantic_guardrails)
            ?? null
          )
        }))
      } else if (msg.type === 'new_files_discovered') {
        // New file notification bubble (task 9.1 + 9.2)
        const count = msg.count as number
        const types = msg.types as Record<string, number>
        setMessages((prev) => [
          ...prev,
          {
            id: Date.now().toString(),
            role: 'system',
            content: '',
            newFilesEvent: { count, types },
          },
        ])
      } else if (msg.type === 'ui_redirect') {
        if (redirectTimerRef.current !== null) {
          window.clearTimeout(redirectTimerRef.current)
          redirectTimerRef.current = null
        }

        if (msg.target === 'data') {
          const surface = (msg.surface as string | undefined) ?? ''
          const delayMs = surface === 'metadata' ? 3200 : 1500

          if (surface === 'metadata') {
            setMessages((prev) => [
              ...prev,
              {
                id: Date.now().toString(),
                role: 'system',
                content: lang === 'zh'
                  ? '请到数据页面使用「元数据助手」或相关元数据面板完成填写与管理。即将自动跳转。'
                  : "Please use the Data page's Metadata Assistant or metadata panels to manage metadata. Redirecting shortly.",
              },
            ])
          }

          redirectTimerRef.current = window.setTimeout(() => {
            onNavigateToData?.()
            redirectTimerRef.current = null
          }, delayMs)
        }
      } else if (msg.type === 'binding_required' || msg.type === 'resource_clarification_required') {
        const issues = (msg.issues as string[]) || []
        setMessages((prev) => [
          ...prev,
          {
            id: Date.now().toString(),
            role: 'assistant',
            content: `⚠️ **${t('chat_binding_required_title')}**\n${issues.map((s) => `- ${s}`).join('\n')}\n\n${t('chat_binding_required_resume')}`,
          },
        ])
      }
    })
    return () => { unsub() }
  }, [ws.subscribe])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  useEffect(() => {
    const activeKeys = new Set(taskAttentionReminders.map((item) => item.key))
    for (const key of Array.from(seenTaskReminderKeysRef.current)) {
      if (!activeKeys.has(key)) {
        seenTaskReminderKeysRef.current.delete(key)
      }
    }

    if (taskAttentionReminders.length === 0) return

    const nextMessages: Message[] = []
    for (const reminder of taskAttentionReminders) {
      if (seenTaskReminderKeysRef.current.has(reminder.key)) continue
      seenTaskReminderKeysRef.current.add(reminder.key)
      nextMessages.push({
        id: `task-reminder-${reminder.key}`,
        role: 'system',
        content: buildTaskAttentionReminderMessage(reminder, lang, t),
      })
    }

    if (nextMessages.length > 0) {
      setMessages((prev) => [...prev, ...nextMessages])
    }
  }, [lang, t, taskAttentionReminders])

  useEffect(() => () => {
    if (redirectTimerRef.current !== null) {
      window.clearTimeout(redirectTimerRef.current)
    }
  }, [])

  const send = () => {
    if (!input.trim() || streaming) return
    const userMsg: Message = { id: Date.now().toString(), role: 'user', content: input }
    setMessages((prev) => [...prev, userMsg])
    ws.send({ type: 'chat', content: input, project_id: projectId, language: lang })
    setInput('')
  }

  return (
    <div className="flex flex-col h-full bg-surface-base">
      {/* Thread title strip (only when a thread is active) */}
      {threadTitle && (
        <div className="px-4 py-1.5 border-b border-border-subtle flex items-center gap-2 bg-surface-raised">
          <span className="text-xs text-text-muted truncate flex-1">{threadTitle}</span>
          {onOpenThreadDrawer && (
            <button
              onClick={onOpenThreadDrawer}
              className="p-1 rounded text-text-muted hover:text-text-primary hover:bg-surface-hover transition-colors shrink-0"
              title={t('switch_thread_title')}
            >
              <ChevronsUpDown size={12} />
            </button>
          )}
        </div>
      )}

      {/* Messages */}
      <div className="flex-1 overflow-y-auto px-5 py-5 space-y-4">
        {messages.map((m) => {
          if (m.role === 'system' && m.newFilesEvent) {
            const { count, types } = m.newFilesEvent
            const typeSummary = Object.entries(types).map(([k, v]) => `${v} ${k}`).join(', ')
            return (
              <div key={m.id} className="flex justify-center">
                <div className="bg-indigo-500/8 border border-indigo-500/20 rounded-xl px-4 py-3 text-xs text-indigo-300 max-w-[90%]">
                  <p>
                    {t('chat_new_files')
                      .replace('{count}', String(count))
                      .replace('{types}', typeSummary)}
                  </p>
                  {onNavigateToData && (
                    <button
                      onClick={onNavigateToData}
                      className="mt-2 text-xs px-2 py-1 bg-accent hover:bg-accent-hover rounded-md transition-colors"
                    >
                      {t('chat_open_data')}
                    </button>
                  )}
                </div>
              </div>
            )
          }

          return (
            <div key={m.id} className={`flex ${m.role === 'user' ? 'justify-end' : 'justify-start'}`}>
              {(() => {
                const executionPlanStatusMap = buildExecutionPlanDeltaStatusMap(m.executionPlanDelta)
                return (
              <div className={`max-w-[85%] rounded-2xl px-4 py-3 text-sm ${
                m.role === 'user'
                  ? 'bg-accent/15 text-text-primary'
                  : 'bg-surface-raised text-text-primary'
              }`}>
                {m.role === 'assistant' ? (
                  <div className="prose-chat">
                    {!m.content && streaming ? (
                      <span className="flex items-center gap-1.5 text-text-muted text-xs">
                        <span className="inline-flex gap-1">
                          <span className="w-1.5 h-1.5 rounded-full bg-text-muted animate-bounce" style={{ animationDelay: '0ms' }} />
                          <span className="w-1.5 h-1.5 rounded-full bg-text-muted animate-bounce" style={{ animationDelay: '150ms' }} />
                          <span className="w-1.5 h-1.5 rounded-full bg-text-muted animate-bounce" style={{ animationDelay: '300ms' }} />
                        </span>
                        {t('chat_thinking')}
                      </span>
                    ) : (
                      <ReactMarkdown
                        remarkPlugins={[remarkGfm]}
                        rehypePlugins={[rehypeHighlight]}
                      >
                        {m.content}
                      </ReactMarkdown>
                    )}
                  </div>
                ) : (
                  <span className="whitespace-pre-wrap">{m.content}</span>
                )}
                {m.results && m.results.map((r, i) => <ResultViewer key={i} {...r} />)}
                {m.requiresConfirmation && (
                  <div className="mt-3 rounded-xl border border-amber-500/20 bg-amber-500/8 px-3 py-3">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-[11px] font-medium text-amber-200">
                        {m.confirmationPhase === 'execution'
                          ? t('chat_plan_phase_execution')
                          : t('chat_plan_phase_abstract')}
                      </span>
                      {m.confirmationPhase === 'execution' && m.executionPlanSummary && (
                        <span className="text-[11px] text-amber-100/90">
                          {t('chat_execution_summary')
                            .replace('{groups}', String(m.executionPlanSummary.group_count ?? 0))
                            .replace('{nodes}', String(m.executionPlanSummary.node_count ?? 0))}
                        </span>
                      )}
                    </div>
                    {m.confirmationPhase === 'execution' && m.executionConfirmationOverview && (
                      <div className="mt-3 rounded-lg border border-cyan-500/15 bg-cyan-500/6 px-3 py-2 text-[11px] text-cyan-100/90">
                        {t('tasks_confirmation_overview_summary')
                          .replace('{abstract}', String(m.executionConfirmationOverview.abstract_step_count ?? 0))
                          .replace('{ir}', String(m.executionConfirmationOverview.execution_ir_step_count ?? 0))
                          .replace('{groups}', String(m.executionConfirmationOverview.execution_group_count ?? 0))
                          .replace('{per_sample}', String(m.executionConfirmationOverview.per_sample_step_count ?? 0))
                          .replace('{aggregate}', String(m.executionConfirmationOverview.aggregate_step_count ?? 0))
                          .replace('{added}', String(m.executionConfirmationOverview.added_group_count ?? 0))
                          .replace('{changed}', String(m.executionConfirmationOverview.changed_group_count ?? 0))}
                      </div>
                    )}
                    {m.confirmationPhase === 'execution' && m.executionPlanDelta && (
                      <div className="mt-3 rounded-lg border border-amber-500/10 bg-surface-base/70 px-3 py-2 text-[11px] text-text-muted">
                        {t('tasks_confirmation_delta_summary')
                          .replace('{abstract}', String(m.executionPlanDelta.abstract_step_count ?? 0))
                          .replace('{execution}', String(m.executionPlanDelta.execution_group_count ?? 0))
                          .replace('{unchanged}', String(m.executionPlanDelta.unchanged_group_count ?? 0))
                          .replace('{changed}', String(m.executionPlanDelta.changed_group_count ?? 0))
                          .replace('{added}', String(m.executionPlanDelta.added_group_count ?? 0))}
                      </div>
                    )}
                    {m.confirmationPhase === 'execution' && m.executionSemanticGuardrails && (
                      ((m.executionSemanticGuardrails.ambiguity_count ?? 0) > 0 || (m.executionSemanticGuardrails.memory_review_count ?? 0) > 0)
                    ) && (
                      <div className="mt-3 rounded-lg border border-rose-500/20 bg-rose-500/8 px-3 py-3">
                        <div className="mb-2 text-[11px] font-semibold uppercase tracking-wide text-rose-200">
                          {t('tasks_confirmation_semantic_guardrails')}
                        </div>
                        <div className="mb-3 flex flex-wrap gap-2">
                          {(m.executionSemanticGuardrails.memory_review_count ?? 0) > 0 && (
                            <span className="rounded-full border border-rose-500/25 bg-rose-500/12 px-2 py-0.5 text-[10px] font-medium text-rose-200">
                              {t('tasks_confirmation_guardrail_memory_conflicts').replace('{count}', String(m.executionSemanticGuardrails.memory_review_count ?? 0))}
                            </span>
                          )}
                          {(m.executionSemanticGuardrails.ambiguity_count ?? 0) > 0 && (
                            <span className="rounded-full border border-amber-500/25 bg-amber-500/12 px-2 py-0.5 text-[10px] font-medium text-amber-200">
                              {t('tasks_confirmation_guardrail_ambiguities').replace('{count}', String(m.executionSemanticGuardrails.ambiguity_count ?? 0))}
                            </span>
                          )}
                        </div>
                        {m.executionSemanticGuardrails.project_memory_summary && (
                          <div className="mb-3 text-[11px] text-rose-100/85 break-words">
                            {t('tasks_confirmation_guardrail_memory_summary')}{' '}
                            {[
                              t('tasks_confirmation_guardrail_memory_facts').replace('{count}', String(m.executionSemanticGuardrails.project_memory_summary.stable_fact_count ?? 0)),
                              t('tasks_confirmation_guardrail_memory_patterns').replace('{count}', String(m.executionSemanticGuardrails.project_memory_summary.memory_pattern_count ?? 0)),
                              t('tasks_confirmation_guardrail_memory_preferences').replace('{count}', String(m.executionSemanticGuardrails.project_memory_summary.memory_preference_count ?? 0)),
                              t('tasks_confirmation_guardrail_memory_resources').replace('{count}', String(m.executionSemanticGuardrails.project_memory_summary.resource_link_count ?? 0)),
                              t('tasks_confirmation_guardrail_memory_artifacts').replace('{count}', String(m.executionSemanticGuardrails.project_memory_summary.artifact_link_count ?? 0)),
                              t('tasks_confirmation_guardrail_memory_runtime').replace('{count}', String(m.executionSemanticGuardrails.project_memory_summary.runtime_link_count ?? 0)),
                            ].join(' · ')}
                          </div>
                        )}
                        {(m.executionSemanticGuardrails.memory_binding_reviews?.length ?? 0) > 0 && (
                          <div className="space-y-2">
                            {m.executionSemanticGuardrails.memory_binding_reviews?.map((item, index) => (
                              <div
                                key={`${m.id}-memory-guardrail-${item.binding_key ?? item.step_key ?? index}`}
                                className="rounded-lg border border-rose-500/15 bg-surface-base/70 px-3 py-2"
                              >
                                <div className="text-xs font-medium text-text-primary">
                                  {formatExecutionSemanticReviewTitle(item)}
                                </div>
                                {item.description && (
                                  <div className="mt-1 text-[11px] text-text-muted break-words">
                                    {item.description}
                                  </div>
                                )}
                                <div className="mt-1 text-[11px] text-rose-100/90 break-words">
                                  {[
                                    item.slot_name ? `${t('tasks_confirmation_guardrail_slot')} ${item.slot_name}` : null,
                                    item.candidate_path ? `${t('tasks_confirmation_guardrail_candidate')} ${item.candidate_path}` : null,
                                    item.confirmed_path ? `${t('tasks_confirmation_guardrail_confirmed')} ${item.confirmed_path}` : null,
                                  ].filter(Boolean).join(' · ')}
                                </div>
                              </div>
                            ))}
                          </div>
                        )}
                        {(m.executionSemanticGuardrails.ambiguity_reviews?.length ?? 0) > 0 && (
                          <div className="mt-3 space-y-2">
                            {m.executionSemanticGuardrails.ambiguity_reviews?.map((item, index) => (
                              <div
                                key={`${m.id}-ambiguity-guardrail-${item.binding_key ?? item.step_key ?? index}`}
                                className="rounded-lg border border-amber-500/15 bg-surface-base/70 px-3 py-2"
                              >
                                <div className="text-xs font-medium text-text-primary">
                                  {formatExecutionSemanticReviewTitle(item)}
                                </div>
                                {item.description && (
                                  <div className="mt-1 text-[11px] text-text-muted break-words">
                                    {item.description}
                                  </div>
                                )}
                                <div className="mt-1 text-[11px] text-amber-100/90 break-words">
                                  {[
                                    item.slot_name ? `${t('tasks_confirmation_guardrail_slot')} ${item.slot_name}` : null,
                                    item.primary_path ? `${t('tasks_confirmation_guardrail_primary')} ${item.primary_path}` : null,
                                    item.secondary_path ? `${t('tasks_confirmation_guardrail_secondary')} ${item.secondary_path}` : null,
                                  ].filter(Boolean).join(' · ')}
                                </div>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    )}
                    {m.confirmationPhase === 'execution' && (m.executionIrReview?.length ?? 0) > 0 && (
                      <div className="mt-3 space-y-2">
                        <div className="text-[11px] font-semibold uppercase tracking-wide text-amber-200/85">
                          {t('tasks_confirmation_execution_ir_review')}
                        </div>
                        {m.executionIrReview?.map((item, index) => {
                          const title = item.display_name || item.step_key || item.step_type || t('chat_plan_step_fallback')
                          const meta = [item.step_type, item.description].filter(Boolean).join(' · ')
                          return (
                            <div
                              key={`${m.id}-execution-ir-${item.step_key ?? item.step_type ?? index}`}
                              className="rounded-lg border border-cyan-500/15 bg-surface-base/70 px-3 py-2"
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
                    {m.plan && m.plan.length > 0 && (
                      <div className="mt-3 space-y-2">
                        {m.plan.map((item, index) => {
                          const meta = formatPlanMeta(item)
                          const status = item.step_key ? executionPlanStatusMap[item.step_key] : undefined
                          return (
                            <div
                              key={`${m.id}-plan-${item.step_key ?? item.step_type ?? index}`}
                              className="rounded-lg border border-amber-500/10 bg-surface-base/70 px-3 py-2"
                            >
                              <div className="flex items-center gap-2 flex-wrap">
                                <div className="text-xs font-medium text-text-primary">
                                  {index + 1}. {formatPlanTitle(item)}
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
                    {m.confirmationPhase === 'execution' && (m.executionPlanChanges?.length ?? 0) > 0 && (
                      <div className="mt-3 space-y-2">
                        <div className="text-[11px] font-semibold uppercase tracking-wide text-amber-200/85">
                          {t('tasks_confirmation_changes')}
                        </div>
                        {m.executionPlanChanges?.map((item, index) => {
                          const title = item.display_name || item.group_key || item.step_type || t('chat_plan_step_fallback')
                          const summary = formatExecutionPlanChangeSummary(item)
                          const changeKinds = (item.change_kinds ?? []).filter(Boolean)
                          return (
                            <div
                              key={`${m.id}-execution-change-${item.group_key ?? item.step_type ?? index}`}
                              className="rounded-lg border border-sky-500/15 bg-surface-base/70 px-3 py-2"
                            >
                              <div className="flex items-center gap-2 flex-wrap">
                                <div className="text-xs font-medium text-text-primary">
                                  {title}
                                </div>
                                {changeKinds.map((kind) => (
                                  <span
                                    key={`${m.id}-execution-change-kind-${item.group_key ?? index}-${kind}`}
                                    className="rounded-full border border-sky-500/20 bg-sky-500/10 px-2 py-0.5 text-[10px] font-medium text-sky-200"
                                  >
                                    {formatExecutionPlanChangeKind(kind)}
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
                  </div>
                )}
                {m.requiresConfirmation && (
                  <div className="mt-3 flex gap-2">
                    <button
                      onClick={() => ws.send({ type: 'confirm_plan', confirm: true })}
                      className="px-3 py-1 bg-emerald-700/60 hover:bg-emerald-600/60 rounded-lg text-xs transition-colors"
                    >
                      {t('chat_proceed')}
                    </button>
                    <button
                      onClick={() => ws.send({ type: 'confirm_plan', confirm: false })}
                      className="px-3 py-1 bg-surface-overlay hover:bg-surface-hover rounded-lg text-xs transition-colors"
                    >
                      {t('chat_cancel')}
                    </button>
                  </div>
                )}
              </div>
                )
              })()}
            </div>
          )
        })}
        <div ref={bottomRef} />
      </div>

      {/* Human error recovery panel */}
      {errorRecovery && (
        <ErrorRecoveryPanel
          jobId={errorRecovery.jobId}
          step={errorRecovery.step}
          command={errorRecovery.command}
          stderr={errorRecovery.stderr}
          attemptHistory={errorRecovery.attemptHistory}
          onSendRetry={(_jobId, text) => {
            ws.send({ type: 'chat', content: text, project_id: projectId, language: lang })
            setErrorRecovery(null)
          }}
          onStop={(jobId) => {
            ws.send({ type: 'terminate_error_recovery', job_id: jobId })
            setErrorRecovery(null)
          }}
        />
      )}

      {/* Memory save prompt (after human-assisted recovery) */}
      {memorySavePrompt && (
        <div className="mx-4 mb-3 border border-purple-500/30 rounded-xl p-4 bg-purple-500/8">
          <p className="text-purple-300 text-xs font-semibold mb-2">{t('memory_save_heading')}</p>
          <p className="text-text-muted text-xs mb-0.5">{t('memory_save_trigger_label')}</p>
          <p className="text-text-secondary text-xs mb-2 font-mono">{memorySavePrompt.trigger}</p>
          <p className="text-text-muted text-xs mb-0.5">{t('memory_save_approach_label')}</p>
          <p className="text-text-secondary text-xs mb-3 font-mono">{memorySavePrompt.approach}</p>
          <div className="flex gap-2">
            <button
              onClick={() => {
                ws.send({
                  type: 'save_memory',
                  trigger: memorySavePrompt.trigger,
                  approach: memorySavePrompt.approach,
                })
                setMemorySavePrompt(null)
              }}
              className="px-3 py-1.5 bg-purple-600/60 hover:bg-purple-500/60 rounded-lg text-xs font-medium transition-colors"
            >
              {t('memory_save_confirm')}
            </button>
            <button
              onClick={() => setMemorySavePrompt(null)}
              className="px-3 py-1.5 bg-surface-overlay hover:bg-surface-hover rounded-lg text-xs font-medium transition-colors"
            >
              {t('chat_dismiss')}
            </button>
          </div>
        </div>
      )}

      {/* Skill save offer */}
      {skillSaveOffer && (
        <div className="mx-4 mb-3 border border-emerald-500/30 rounded-xl p-4 bg-emerald-500/8">
          <p className="text-emerald-300 text-sm mb-2">
            ✓ {t('chat_skill_save_offer').replace('{name}', skillSaveOffer.jobName)}
          </p>
          <div className="flex gap-2">
            <button
              onClick={async () => {
                const name = window.prompt(t('chat_skill_name_prompt'), skillSaveOffer.jobName) ?? skillSaveOffer.jobName
                await fetch(`/api/skills/from-job/${skillSaveOffer.jobId}`, {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({ name }),
                })
                setSkillSaveOffer(null)
                setMessages((prev) => [
                  ...prev,
                  { id: Date.now().toString(), role: 'assistant', content: t('chat_skill_saved').replace('{name}', name) },
                ])
              }}
              className="px-3 py-1.5 bg-emerald-700/60 hover:bg-emerald-600/60 rounded-lg text-xs font-medium transition-colors"
            >
              {t('chat_save_as_skill')}
            </button>
            <button
              onClick={() => setSkillSaveOffer(null)}
              className="px-3 py-1.5 bg-surface-overlay hover:bg-surface-hover rounded-lg text-xs font-medium transition-colors"
            >
              {t('chat_dismiss')}
            </button>
          </div>
        </div>
      )}

      {/* LLM not configured callout */}
      {!llmReachable && (
        <div className="mx-4 mb-3 bg-red-500/8 border border-red-500/20 rounded-xl px-4 py-3 flex items-center gap-3">
          <div className="flex-1">
            <p className="text-sm font-medium text-red-300">{t('chat_llm_not_configured')}</p>
            <p className="text-xs text-text-muted mt-0.5">{t('chat_llm_not_configured_desc')}</p>
          </div>
          {onNavigateToSettings && (
            <button
              onClick={onNavigateToSettings}
              className="text-xs text-accent hover:text-accent-hover transition-colors shrink-0"
            >
              {t('chat_go_settings')}
            </button>
          )}
        </div>
      )}

      {/* Input area */}
      <div className="border-t border-border-subtle px-4 py-3 flex gap-2">
        <input
          className="flex-1 bg-surface-overlay rounded-xl px-4 py-2.5 text-sm text-text-primary outline-none focus:ring-1 focus:ring-accent placeholder-text-muted"
          placeholder={!llmReachable ? t('chat_placeholder_llm_disabled') : streaming ? t('chat_placeholder_idle') : t('chat_placeholder_idle')}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && !e.shiftKey && (e.preventDefault(), send())}
          disabled={streaming || !llmReachable}
        />
        <button
          onClick={send}
          disabled={streaming || !input.trim() || !llmReachable}
          className="px-4 py-2 bg-accent hover:bg-accent-hover rounded-xl text-sm font-medium disabled:opacity-40 transition-colors text-white"
        >
          {t('chat_send')}
        </button>
      </div>
    </div>
  )
}
