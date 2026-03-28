import type { Lang, TranslationKey } from '../i18n/translations'

export type TaskAttentionSignal = 'idle' | 'running' | 'warning' | 'attention'
export type TaskAttentionReason = 'authorization' | 'repair' | 'confirmation' | 'clarification' | 'rollback_review' | 'warning'
export type OperatorChatActionCode = 'confirm_or_edit_plan' | 'provide_missing_resource_clarification' | 'confirm_or_edit_execution' | null

type TranslateFn = (key: TranslationKey) => string

export interface TaskAttentionReminder {
  key: string
  jobId: string
  jobName: string
  threadId?: string | null
  incidentType: string
  reason: TaskAttentionReason
  ageSeconds: number
  summary: string
  severity: 'info' | 'warning' | 'critical'
  owner: 'user' | 'system'
  nextAction?: string | null
  rollbackLevel?: string | null
}

export function formatTaskAttentionReason(reason: TaskAttentionReason, lang: Lang): string {
  const labels: Record<TaskAttentionReason, { zh: string; en: string }> = {
    authorization: { zh: '命令授权', en: 'authorization' },
    repair: { zh: '人工修复', en: 'repair' },
    confirmation: { zh: '确认', en: 'confirmation' },
    clarification: { zh: '资源澄清', en: 'resource clarification' },
    rollback_review: { zh: '回退复核', en: 'rollback review' },
    warning: { zh: '检查', en: 'review' },
  }
  return labels[reason][lang]
}

export function resolveOperatorChatActionCode(input: {
  reason?: string | null
  rollbackLevel?: string | null
  nextAction?: string | null
}): OperatorChatActionCode {
  if (input.reason === 'rollback_review') {
    if (input.rollbackLevel === 'abstract_plan') return 'confirm_or_edit_plan'
    if (input.rollbackLevel === 'execution_ir') return 'provide_missing_resource_clarification'
    return 'confirm_or_edit_execution'
  }
  if (input.nextAction === 'confirm_or_edit_plan') return 'confirm_or_edit_plan'
  if (input.nextAction === 'confirm_or_edit_execution') return 'confirm_or_edit_execution'
  if (input.nextAction === 'provide_missing_resource_clarification') return 'provide_missing_resource_clarification'
  return null
}

export function formatOperatorChatActionCta(actionCode: OperatorChatActionCode, t: TranslateFn): string {
  if (actionCode === 'confirm_or_edit_plan') return t('tasks_pending_inputs_open_plan_chat')
  if (actionCode === 'provide_missing_resource_clarification') return t('tasks_pending_inputs_open_execution_ir_chat')
  if (actionCode === 'confirm_or_edit_execution') return t('tasks_pending_inputs_open_dag_chat')
  return t('tasks_supervisor_open_chat')
}

export function formatOperatorChatActionHint(actionCode: OperatorChatActionCode, t: TranslateFn): string | null {
  if (actionCode === 'confirm_or_edit_plan') return t('tasks_pending_inputs_hint_plan_chat')
  if (actionCode === 'provide_missing_resource_clarification') return t('tasks_pending_inputs_hint_execution_ir_chat')
  if (actionCode === 'confirm_or_edit_execution') return t('tasks_pending_inputs_hint_dag_chat')
  return null
}

export function buildTaskAttentionReminderMessage(
  reminder: TaskAttentionReminder,
  lang: Lang,
  t: TranslateFn,
): string {
  const waitMinutes = Math.max(1, Math.floor(reminder.ageSeconds / 60))
  const actionCode = resolveOperatorChatActionCode({
    reason: reminder.reason,
    rollbackLevel: reminder.rollbackLevel,
    nextAction: reminder.nextAction,
  })
  const actionHint = formatOperatorChatActionHint(actionCode, t)

  if (lang === 'zh') {
    return [
      `任务“${reminder.jobName}”仍在等待${formatTaskAttentionReason(reminder.reason, lang)}，已超过 ${waitMinutes} 分钟。`,
      actionHint ?? '请打开右侧任务面板处理。',
    ].join('')
  }

  return [
    `Task "${reminder.jobName}" is still waiting on ${formatTaskAttentionReason(reminder.reason, lang)} after ${waitMinutes} minute(s).`,
    actionHint ?? 'Open the task tray on the right to continue.',
  ].join(' ')
}
