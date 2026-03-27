import type React from 'react'
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import TaskMonitor from './TaskMonitor'
import { LanguageProvider } from '../i18n/LanguageContext'
import { useProjectTaskFeed } from '../hooks/useProjectTaskFeed'

vi.mock('framer-motion', () => ({
  AnimatePresence: ({ children }: { children: React.ReactNode }) => children,
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => <div {...props}>{children}</div>,
  },
}))

vi.mock('../hooks/useProjectTaskFeed', () => ({
  PROJECT_TASK_PAGE_SIZE: 20,
  useProjectTaskFeed: vi.fn(),
}))

const mockUseProjectTaskFeed = vi.mocked(useProjectTaskFeed)

function renderTaskMonitor(props: React.ComponentProps<typeof TaskMonitor>) {
  return render(
    <LanguageProvider>
      <TaskMonitor {...props} />
    </LanguageProvider>,
  )
}

describe('TaskMonitor', () => {
  afterEach(() => {
    cleanup()
  })

  beforeEach(() => {
    localStorage.clear()
    vi.clearAllMocks()

    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [],
      attentionSummary: {
        signal: 'attention',
        count: 1,
        counts: {
          running: 0,
          authorization: 0,
          repair: 0,
          confirmation: 1,
          clarification: 0,
          warning: 0,
          needs_input: 1,
          needs_review: 0,
        },
        needs_input: [
          {
            key: 'job-1:confirmation',
            job_id: 'job-1',
            job_name: 'RNA-seq confirmation',
            incident_type: 'execution_confirmation',
            reason: 'confirmation',
            age_seconds: 120,
            summary: 'Execution graph is waiting for final confirmation.',
            severity: 'info',
            owner: 'user',
          },
        ],
        needs_review: [],
        reminders: [
          {
            key: 'job-1:confirmation',
            job_id: 'job-1',
            job_name: 'RNA-seq confirmation',
            incident_type: 'execution_confirmation',
            reason: 'confirmation',
            age_seconds: 120,
            summary: 'Execution graph is waiting for final confirmation.',
            severity: 'info',
            owner: 'user',
          },
        ],
        auto_authorize_commands: false,
      },
      incidents: [
        {
          job_id: 'job-1',
          job_name: 'RNA-seq confirmation',
          job_status: 'awaiting_plan_confirmation',
          incident_type: 'execution_confirmation',
          severity: 'info',
          owner: 'user',
          summary: 'Execution graph is waiting for final confirmation.',
          next_action: 'confirm_or_edit_execution',
          age_seconds: 120,
        },
      ],
      incidentSummary: { total_open: 1, critical: 0, warning: 0, info: 1 },
      overview: { total: 1, active: 1, by_status: { awaiting_plan_confirmation: 1 } },
      eventVersion: 0,
      totalCount: 1,
      getJobsPage: () => [
        {
          id: 'job-1',
          name: 'RNA-seq confirmation',
          status: 'awaiting_plan_confirmation',
          goal: 'Analyze apple RNA-seq data',
          thread_id: 'thread-1',
          created_at: '2026-03-26T10:00:00Z',
        },
      ],
      getPageHasMore: () => false,
      patchJob: vi.fn(),
      locateJobPage: vi.fn().mockResolvedValue(1),
      refreshJobPage: vi.fn().mockResolvedValue([
        {
          id: 'job-1',
          name: 'RNA-seq confirmation',
          status: 'awaiting_plan_confirmation',
          goal: 'Analyze apple RNA-seq data',
          thread_id: 'thread-1',
          created_at: '2026-03-26T10:00:00Z',
        },
      ]),
      refreshJobs: vi.fn().mockResolvedValue([]),
      refreshAttentionSummary: vi.fn().mockResolvedValue(undefined),
      refreshIncidents: vi.fn().mockResolvedValue(undefined),
      refreshAll: vi.fn().mockResolvedValue(undefined),
    })

    vi.stubGlobal(
      'WebSocket',
      class {
        onmessage: ((event: MessageEvent) => void) | null = null
        close() {}
      } as unknown as typeof WebSocket,
    )
  })

  it('renders layered confirmation details for execution confirmation', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input)
        if (url.includes('/api/jobs/job-1/bindings?detailed=1')) {
          return Promise.resolve({
            ok: true,
            json: async () => ({
              job_status: 'awaiting_plan_confirmation',
              error_message: 'Execution graph is ready for final confirmation.',
              pending_interaction_type: 'execution_confirmation',
              pending_interaction_payload: {
                prompt_text: 'Execution graph is ready for final confirmation.',
              },
              runtime_diagnostics: [],
              auto_recovery_events: [],
              timeline: [],
              confirmation_phase: 'execution',
              confirmation_plan: [
                { step_key: 'align', display_name: 'HISAT2 align', step_type: 'align.hisat2' },
                { step_key: 'count', display_name: 'featureCounts', step_type: 'quant.featurecounts' },
              ],
              execution_plan_summary: {
                has_execution_ir: true,
                has_expanded_dag: true,
                group_count: 4,
                node_count: 9,
              },
              steps: [],
            }),
          })
        }
        return Promise.resolve({
          ok: true,
          json: async () => ({}),
        })
      }),
    )

    renderTaskMonitor({
      projectId: 'proj-1',
      onOpenThread: vi.fn(),
    })

    fireEvent.click(screen.getByRole('button', { name: /logs/i }))

    expect(await screen.findByText('Layer readiness')).toBeInTheDocument()
    expect(screen.getByText('Abstract Plan')).toBeInTheDocument()
    expect(screen.getByText('Execution IR')).toBeInTheDocument()
    expect(screen.getByText('Expanded DAG')).toBeInTheDocument()
    expect(screen.getByText('4 groups · 9 executable nodes')).toBeInTheDocument()
    expect(screen.getByText('2 review items')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Open Chat' })).toBeInTheDocument()
  })

  it('shows a unified attention summary banner for normalized task input states', async () => {
    vi.stubGlobal('fetch', vi.fn(() => Promise.resolve({ ok: true, json: async () => ({}) })))

    renderTaskMonitor({
      projectId: 'proj-1',
      onOpenThread: vi.fn(),
    })

    expect(await screen.findByText('Attention Queue')).toBeInTheDocument()
    expect(screen.getByText(/Needs input\s+1/)).toBeInTheDocument()
  })

  it('surfaces confirmation work in the unified pending input section', async () => {
    vi.stubGlobal('fetch', vi.fn(() => Promise.resolve({ ok: true, json: async () => ({}) })))

    renderTaskMonitor({
      projectId: 'proj-1',
      onOpenThread: vi.fn(),
    })

    expect(await screen.findByText('Pending Operator Input')).toBeInTheDocument()
    expect(screen.getByText('1 task(s) are waiting for confirmation or clarification.')).toBeInTheDocument()
    expect(screen.getAllByText('Awaiting Confirmation')).toHaveLength(2)
    expect(screen.getByText('Execution graph is waiting for final confirmation.')).toBeInTheDocument()
  })

  it('renders environment failure diagnostics with package candidates in the task panel', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input)
        if (url.includes('/api/jobs/job-1/bindings?detailed=1')) {
          return Promise.resolve({
            ok: true,
            json: async () => ({
              job_status: 'queued',
              error_message: 'Pixi install failed for package(s) [featureCounts]: PackagesNotFoundError: featureCounts',
              pending_interaction_type: null,
              pending_interaction_payload: null,
              runtime_diagnostics: [
                {
                  kind: 'environment_prepare_failed',
                  stage: 'pixi_install',
                  failure_kind: 'missing_package',
                  retryable: true,
                  failed_packages: ['featureCounts'],
                  package_candidates: {
                    featureCounts: ['subread', 'featurecounts'],
                  },
                  implicated_steps: [
                    {
                      step_key: 'featurecounts_quant',
                      step_type: 'quant.featurecounts',
                      display_name: 'featureCounts quantification',
                    },
                  ],
                },
              ],
              auto_recovery_events: [],
              timeline: [],
              confirmation_phase: null,
              confirmation_plan: [],
              execution_plan_summary: null,
              steps: [],
            }),
          })
        }
        return Promise.resolve({
          ok: true,
          json: async () => ({}),
        })
      }),
    )

    renderTaskMonitor({
      projectId: 'proj-1',
      onOpenThread: vi.fn(),
    })

    fireEvent.click(screen.getByRole('button', { name: /logs/i }))

    expect(await screen.findByText('Runtime Diagnostics')).toBeInTheDocument()
    expect(
      screen.getByText(
        'Environment preparation failed: stage=pixi_install · packages=featureCounts · candidates=featureCounts -> subread, featurecounts · steps=featureCounts quantification',
      ),
    ).toBeInTheDocument()
  })

  it('emits resource workspace navigation requests from supervisor review', async () => {
    const onOpenResourceWorkspace = vi.fn()

    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-1',
          name: 'RNA-seq confirmation',
          status: 'awaiting_plan_confirmation',
          goal: 'Analyze apple RNA-seq data',
          thread_id: 'thread-1',
          created_at: '2026-03-26T10:00:00Z',
        },
      ],
      attentionSummary: {
        signal: 'warning',
        count: 1,
        counts: {
          running: 0,
          authorization: 0,
          repair: 0,
          confirmation: 0,
          clarification: 0,
          warning: 1,
          needs_input: 0,
          needs_review: 1,
        },
        needs_input: [],
        needs_review: [
          {
            key: 'job-1:binding',
            job_id: 'job-1',
            job_name: 'RNA-seq confirmation',
            incident_type: 'binding',
            reason: 'warning',
            age_seconds: 180,
            summary: 'Reference FASTA is missing.',
            severity: 'warning',
            owner: 'system',
          },
        ],
        reminders: [],
        auto_authorize_commands: false,
      },
      incidents: [
        {
          job_id: 'job-1',
          job_name: 'RNA-seq confirmation',
          job_status: 'awaiting_plan_confirmation',
          incident_type: 'binding',
          severity: 'warning',
          owner: 'system',
          summary: 'Reference FASTA is missing.',
          next_action: 'inspect_bindings_and_resume',
          age_seconds: 180,
          thread_id: 'thread-1',
        },
      ],
      incidentSummary: { total_open: 4, critical: 1, warning: 2, info: 1 },
      overview: { total: 1, active: 1, by_status: { awaiting_plan_confirmation: 1 } },
      eventVersion: 0,
      totalCount: 1,
      getJobsPage: () => [
        {
          id: 'job-1',
          name: 'RNA-seq confirmation',
          status: 'awaiting_plan_confirmation',
          goal: 'Analyze apple RNA-seq data',
          thread_id: 'thread-1',
          created_at: '2026-03-26T10:00:00Z',
        },
      ],
      getPageHasMore: () => false,
      patchJob: vi.fn(),
      locateJobPage: vi.fn().mockResolvedValue(1),
      refreshJobPage: vi.fn().mockResolvedValue([
        {
          id: 'job-1',
          name: 'RNA-seq confirmation',
          status: 'awaiting_plan_confirmation',
          goal: 'Analyze apple RNA-seq data',
          thread_id: 'thread-1',
          created_at: '2026-03-26T10:00:00Z',
        },
      ]),
      refreshJobs: vi.fn().mockResolvedValue([]),
      refreshAttentionSummary: vi.fn().mockResolvedValue(undefined),
      refreshIncidents: vi.fn().mockResolvedValue(undefined),
      refreshAll: vi.fn().mockResolvedValue(undefined),
    })

    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input)
        if (url.includes('/api/jobs/supervisor-review?project=proj-1')) {
          return Promise.resolve({
            ok: true,
            json: async () => ({
              mode: 'heuristic',
              generated_at: '2026-03-26T12:00:00Z',
              overview: '1 open incident.',
              supervisor_message: 'Register the missing primary resource.',
              focus_summary: {
                primary_lane: 'resource_readiness',
                next_best_operator_move: 'register_primary_resource',
              },
              project_playbook: {
                goal: 'resource_readiness',
                next_move: 'register_primary_resource',
                step_codes: ['open_task', 'inspect_resource_blockers'],
              },
              recommendations: [
                {
                  priority: 1,
                  job_id: 'job-1',
                  job_name: 'RNA-seq confirmation',
                  incident_type: 'binding',
                  severity: 'warning',
                  owner: 'system',
                  diagnosis: 'Reference FASTA is missing.',
                  immediate_action: 'inspect_bindings_and_resume',
                  why_now: 'Alignment cannot start.',
                  rollback_target: 'align',
                },
              ],
              dossiers: [
                {
                  job_id: 'job-1',
                  resource_graph: {
                    blocking_nodes: [],
                    blocking_summary: [
                      {
                        id: 'ref',
                        label: 'GDDH13 reference',
                        status: 'missing',
                        cause: 'missing_primary_resource',
                        recommended_action: 'register_primary_resource',
                        registry_key: 'reference_fasta',
                        workspace_section: 'registry',
                      },
                    ],
                    dominant_blocker: {
                      id: 'ref',
                      label: 'GDDH13 reference',
                      status: 'missing',
                      cause: 'missing_primary_resource',
                      why_blocked: 'A required primary reference/annotation resource is missing.',
                      operator_hint: 'Register or select the matching reference FASTA / annotation GTF for this project.',
                      recommended_action: 'register_primary_resource',
                      registry_key: 'reference_fasta',
                      workspace_section: 'registry',
                    },
                  },
                },
              ],
            }),
          })
        }
        return Promise.resolve({
          ok: true,
          json: async () => ({}),
        })
      }),
    )

    renderTaskMonitor({
      projectId: 'proj-1',
      onOpenThread: vi.fn(),
      onOpenResourceWorkspace,
    })

    expect(await screen.findByText('1 open · 0 critical · 1 warning · 0 info')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Supervisor Review' }))

    const button = await screen.findByRole('button', { name: 'Open Resource Registry' })
    fireEvent.click(button)

    await waitFor(() => {
      expect(onOpenResourceWorkspace).toHaveBeenCalledWith(
        expect.objectContaining({
          tab: 'project-info',
          focusSection: 'registry',
          key: 'reference_fasta',
          description: 'GDDH13 reference',
        }),
      )
    })
  })

  it('shows resolved and pending request types when resume retry is blocked by type mismatch', async () => {
    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-resume',
          name: 'Resume auth chain',
          status: 'waiting_for_authorization',
          goal: 'Resume RNA-seq pipeline',
          thread_id: 'thread-resume',
          created_at: '2026-03-27T09:00:00Z',
        },
      ],
      attentionSummary: {
        signal: 'warning',
        count: 1,
        counts: {
          running: 0,
          authorization: 0,
          repair: 0,
          confirmation: 0,
          clarification: 0,
          warning: 1,
          needs_input: 0,
          needs_review: 1,
        },
        needs_input: [],
        needs_review: [
          {
            key: 'job-resume:resume_failed',
            job_id: 'job-resume',
            job_name: 'Resume auth chain',
            incident_type: 'resume_failed',
            reason: 'warning',
            age_seconds: 200,
            summary: 'Resume chain stopped after a resolved decision.',
            severity: 'warning',
            owner: 'system',
          },
        ],
        reminders: [],
        auto_authorize_commands: false,
      },
      incidents: [
        {
          job_id: 'job-resume',
          job_name: 'Resume auth chain',
          job_status: 'waiting_for_authorization',
          incident_type: 'resume_failed',
          severity: 'warning',
          owner: 'system',
          summary: 'Resume chain stopped after a resolved decision.',
          next_action: 'inspect_resume_chain',
          age_seconds: 200,
          thread_id: 'thread-resume',
        },
      ],
      incidentSummary: { total_open: 1, critical: 0, warning: 1, info: 0 },
      overview: { total: 1, active: 1, by_status: { waiting_for_authorization: 1 } },
      eventVersion: 0,
      totalCount: 1,
      getJobsPage: () => [
        {
          id: 'job-resume',
          name: 'Resume auth chain',
          status: 'waiting_for_authorization',
          goal: 'Resume RNA-seq pipeline',
          thread_id: 'thread-resume',
          created_at: '2026-03-27T09:00:00Z',
        },
      ],
      getPageHasMore: () => false,
      patchJob: vi.fn(),
      locateJobPage: vi.fn().mockResolvedValue(1),
      refreshJobPage: vi.fn().mockResolvedValue([
        {
          id: 'job-resume',
          name: 'Resume auth chain',
          status: 'waiting_for_authorization',
          goal: 'Resume RNA-seq pipeline',
          thread_id: 'thread-resume',
          created_at: '2026-03-27T09:00:00Z',
        },
      ]),
      refreshJobs: vi.fn().mockResolvedValue([]),
      refreshAttentionSummary: vi.fn().mockResolvedValue(undefined),
      refreshIncidents: vi.fn().mockResolvedValue(undefined),
      refreshAll: vi.fn().mockResolvedValue(undefined),
    })

    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input)
        if (url.includes('/api/jobs/supervisor-review?project=proj-1')) {
          return Promise.resolve({
            ok: true,
            json: async () => ({
              mode: 'heuristic',
              generated_at: '2026-03-27T12:00:00Z',
              overview: '1 open incident.',
              supervisor_message: 'Retry resume chain is blocked until the pending reference is reconciled.',
              recommendations: [
                {
                  priority: 1,
                  job_id: 'job-resume',
                  job_name: 'Resume auth chain',
                  incident_type: 'resume_failed',
                  severity: 'warning',
                  owner: 'system',
                  diagnosis: 'The resolved decision type does not match the attached pending request.',
                  immediate_action: 'inspect_resume_chain',
                  why_now: 'Blind retry would apply the wrong decision chain.',
                  rollback_target: 'resume_chain',
                  safe_action: null,
                  safe_action_note:
                    'Resume-chain retry is withheld because the resolved decision type does not match the remaining pending request reference.',
                  safe_action_eligibility: {
                    eligible: false,
                    current_job_status: 'waiting_for_authorization',
                    retryable_job_statuses: ['interrupted', 'waiting_for_authorization', 'waiting_for_repair'],
                    has_resolved_pending_signal: true,
                    has_pending_request_reference: true,
                    resolved_pending_types: ['repair'],
                    pending_reference_types: ['authorization'],
                    blocking_reasons: ['pending_request_type_mismatch'],
                  },
                },
              ],
              dossiers: [],
            }),
          })
        }
        return Promise.resolve({
          ok: true,
          json: async () => ({}),
        })
      }),
    )

    renderTaskMonitor({
      projectId: 'proj-1',
      onOpenThread: vi.fn(),
    })

    fireEvent.click(await screen.findByRole('button', { name: 'Supervisor Review' }))

    expect(await screen.findByText('Resolved decision types: repair')).toBeInTheDocument()
    expect(screen.getByText('Pending reference types: authorization')).toBeInTheDocument()
    expect(
      screen.getByText('Blockers: resolved decision type does not match pending request reference'),
    ).toBeInTheDocument()
  })

  it('renders environment readiness focus summary and project playbook from supervisor review', async () => {
    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-env',
          name: 'Prepare fastqc runtime',
          status: 'queued',
          goal: 'Run QC on apple RNA-seq data',
          thread_id: 'thread-env',
          created_at: '2026-03-27T10:00:00Z',
        },
      ],
      attentionSummary: {
        signal: 'warning',
        count: 1,
        counts: {
          running: 0,
          authorization: 0,
          repair: 0,
          confirmation: 0,
          clarification: 0,
          warning: 1,
          needs_input: 0,
          needs_review: 1,
        },
        needs_input: [],
        needs_review: [
          {
            key: 'job-env:failed',
            job_id: 'job-env',
            job_name: 'Prepare fastqc runtime',
            incident_type: 'failed',
            reason: 'warning',
            age_seconds: 120,
            summary: 'Environment preparation failed.',
            severity: 'warning',
            owner: 'system',
          },
        ],
        reminders: [],
        auto_authorize_commands: false,
      },
      incidents: [
        {
          job_id: 'job-env',
          job_name: 'Prepare fastqc runtime',
          job_status: 'queued',
          incident_type: 'failed',
          severity: 'warning',
          owner: 'system',
          summary: 'Environment preparation failed.',
          next_action: 'inspect_failure_and_retry',
          age_seconds: 120,
          thread_id: 'thread-env',
        },
      ],
      incidentSummary: { total_open: 1, critical: 0, warning: 1, info: 0 },
      overview: { total: 1, active: 1, by_status: { queued: 1 } },
      eventVersion: 0,
      totalCount: 1,
      getJobsPage: () => [
        {
          id: 'job-env',
          name: 'Prepare fastqc runtime',
          status: 'queued',
          goal: 'Run QC on apple RNA-seq data',
          thread_id: 'thread-env',
          created_at: '2026-03-27T10:00:00Z',
        },
      ],
      getPageHasMore: () => false,
      patchJob: vi.fn(),
      locateJobPage: vi.fn().mockResolvedValue(1),
      refreshJobPage: vi.fn().mockResolvedValue([
        {
          id: 'job-env',
          name: 'Prepare fastqc runtime',
          status: 'queued',
          goal: 'Run QC on apple RNA-seq data',
          thread_id: 'thread-env',
          created_at: '2026-03-27T10:00:00Z',
        },
      ]),
      refreshJobs: vi.fn().mockResolvedValue([]),
      refreshAttentionSummary: vi.fn().mockResolvedValue(undefined),
      refreshIncidents: vi.fn().mockResolvedValue(undefined),
      refreshAll: vi.fn().mockResolvedValue(undefined),
    })

    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input)
        if (url.includes('/api/jobs/supervisor-review?project=proj-1')) {
          return Promise.resolve({
            ok: true,
            json: async () => ({
              mode: 'heuristic',
              generated_at: '2026-03-27T13:00:00Z',
              overview: '1 open incident.',
              supervisor_message: 'Inspect the environment preparation failure before retrying.',
              focus_summary: {
                primary_lane: 'environment_readiness',
                top_owner: 'system',
                top_incident_type: 'failed',
                high_confidence_total: 0,
                auto_recoverable_total: 0,
                user_wait_total: 0,
                next_best_operator_move: 'inspect_environment_failure',
                lane_reason: 'Environment readiness is dominated by missing or mismatched runtime packages.',
                next_best_operator_reason:
                  'Inspect the failed package mapping, candidate aliases, and implicated step requirements for package(s) fastqc before retrying environment preparation.',
              },
              project_playbook: {
                goal: 'environment_readiness',
                next_move: 'inspect_environment_failure',
                step_codes: ['open_task', 'inspect_environment_failure', 'recheck_task_state'],
              },
              recommendations: [
                {
                  priority: 1,
                  job_id: 'job-env',
                  job_name: 'Prepare fastqc runtime',
                  incident_type: 'failed',
                  severity: 'warning',
                  owner: 'system',
                  diagnosis: 'Environment preparation failed before execution could start.',
                  immediate_action: 'inspect_failure_and_retry',
                  why_now: 'QC cannot begin until the runtime packages are resolved.',
                  rollback_target: 'runtime_environment',
                },
              ],
              dossiers: [],
            }),
          })
        }
        return Promise.resolve({
          ok: true,
          json: async () => ({}),
        })
      }),
    )

    renderTaskMonitor({
      projectId: 'proj-1',
      onOpenThread: vi.fn(),
    })

    fireEvent.click(await screen.findByRole('button', { name: 'Supervisor Review' }))

    expect(await screen.findByText(/lane=environment readiness/)).toBeInTheDocument()
    expect(screen.getByText(/next=inspect environment failure/)).toBeInTheDocument()
    expect(screen.getByText('Project Playbook')).toBeInTheDocument()
    expect(screen.getByText('2. Next: inspect environment diagnostics, package mapping, and implicated steps.')).toBeInTheDocument()
  })

  it('surfaces pending command authorization at the top of the task panel with a scrollable command box', async () => {
    const refreshAll = vi.fn().mockResolvedValue(undefined)
    const refreshJobPage = vi.fn().mockResolvedValue([
      {
        id: 'job-auth',
        name: 'DESeq2 run',
        status: 'waiting_for_authorization',
        goal: 'Run differential expression on apple RNA-seq data',
        thread_id: 'thread-auth',
        pending_interaction_type: 'authorization',
        created_at: '2026-03-27T08:00:00Z',
      },
    ])

    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-auth',
          name: 'DESeq2 run',
          status: 'waiting_for_authorization',
          goal: 'Run differential expression on apple RNA-seq data',
          thread_id: 'thread-auth',
          pending_interaction_type: 'authorization',
          created_at: '2026-03-27T08:00:00Z',
        },
      ],
      attentionSummary: {
        signal: 'attention',
        count: 1,
        counts: {
          running: 0,
          authorization: 1,
          repair: 0,
          confirmation: 0,
          clarification: 0,
          warning: 0,
          needs_input: 1,
          needs_review: 0,
        },
        needs_input: [
          {
            key: 'job-auth:authorization',
            job_id: 'job-auth',
            job_name: 'DESeq2 run',
            incident_type: 'authorization',
            reason: 'authorization',
            age_seconds: 60,
            summary: 'Authorization is pending.',
            severity: 'info',
            owner: 'user',
          },
        ],
        needs_review: [],
        reminders: [],
        auto_authorize_commands: false,
      },
      incidents: [],
      incidentSummary: { total_open: 0, critical: 0, warning: 0, info: 0 },
      overview: { total: 1, active: 1, by_status: { waiting_for_authorization: 1 } },
      eventVersion: 0,
      totalCount: 1,
      getJobsPage: () => [
        {
          id: 'job-auth',
          name: 'DESeq2 run',
          status: 'waiting_for_authorization',
          goal: 'Run differential expression on apple RNA-seq data',
          thread_id: 'thread-auth',
          pending_interaction_type: 'authorization',
          created_at: '2026-03-27T08:00:00Z',
        },
      ],
      getPageHasMore: () => false,
      patchJob: vi.fn(),
      locateJobPage: vi.fn().mockResolvedValue(1),
      refreshJobPage,
      refreshJobs: vi.fn().mockResolvedValue([]),
      refreshAttentionSummary: vi.fn().mockResolvedValue(undefined),
      refreshIncidents: vi.fn().mockResolvedValue(undefined),
      refreshAll,
    })

    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input)
      if (url.includes('/api/jobs/job-auth/bindings?detailed=1')) {
        return Promise.resolve({
          ok: true,
          json: async () => ({
            job_status: 'waiting_for_authorization',
            pending_interaction_type: 'authorization',
            pending_interaction_payload: {
              auth_request_id: 'auth-1',
              command_type: 'rscript',
              step_key: 'stats.deseq2',
              command: 'Rscript /tmp/run_deseq2.R\n# lots of code\nprint(\"hello\")',
            },
            steps: [],
          }),
        })
      }
      if (url.includes('/api/jobs/job-auth/authorization-requests/auth-1/resolve')) {
        return Promise.resolve({
          ok: true,
          json: async () => ({ ok: true }),
        })
      }
      return Promise.resolve({
        ok: true,
        json: async () => ({}),
      })
    })

    vi.stubGlobal('fetch', fetchMock)

    renderTaskMonitor({
      projectId: 'proj-1',
      onOpenThread: vi.fn(),
    })

    expect(await screen.findByText('Pending Command Authorization')).toBeInTheDocument()
    expect(screen.getByText('1 task(s) are waiting for command authorization.')).toBeInTheDocument()
    const commandPreview = await screen.findByText(/Rscript \/tmp\/run_deseq2\.R/)
    const scrollBox = commandPreview.closest('[style]')
    expect(scrollBox).not.toBeNull()
    expect(scrollBox?.getAttribute('style')).toContain('max-height: 224px')
    expect(scrollBox?.getAttribute('style')).toContain('overflow-y: auto')

    fireEvent.click(screen.getByRole('button', { name: 'Authorize & Run' }))

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        '/api/jobs/job-auth/authorization-requests/auth-1/resolve',
        expect.objectContaining({
          method: 'POST',
        }),
      )
    })
    await waitFor(() => {
      expect(refreshAll).toHaveBeenCalled()
      expect(refreshJobPage).toHaveBeenCalledWith(1, { force: true })
    })
  })

  it('surfaces pending repair requests at the top of the task panel and submits repair actions', async () => {
    const refreshAll = vi.fn().mockResolvedValue(undefined)
    const refreshJobPage = vi.fn().mockResolvedValue([
      {
        id: 'job-repair',
        name: 'featureCounts repair',
        status: 'waiting_for_repair',
        goal: 'Repair featureCounts run on apple RNA-seq data',
        thread_id: 'thread-repair',
        pending_interaction_type: 'repair',
        created_at: '2026-03-27T09:00:00Z',
      },
    ])

    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-repair',
          name: 'featureCounts repair',
          status: 'waiting_for_repair',
          goal: 'Repair featureCounts run on apple RNA-seq data',
          thread_id: 'thread-repair',
          pending_interaction_type: 'repair',
          created_at: '2026-03-27T09:00:00Z',
        },
      ],
      attentionSummary: {
        signal: 'attention',
        count: 1,
        counts: {
          running: 0,
          authorization: 0,
          repair: 1,
          confirmation: 0,
          clarification: 0,
          warning: 0,
          needs_input: 1,
          needs_review: 0,
        },
        needs_input: [
          {
            key: 'job-repair:repair',
            job_id: 'job-repair',
            job_name: 'featureCounts repair',
            incident_type: 'repair',
            reason: 'repair',
            age_seconds: 60,
            summary: 'Repair is pending.',
            severity: 'info',
            owner: 'user',
          },
        ],
        needs_review: [],
        reminders: [],
        auto_authorize_commands: false,
      },
      incidents: [],
      incidentSummary: { total_open: 0, critical: 0, warning: 0, info: 0 },
      overview: { total: 1, active: 1, by_status: { waiting_for_repair: 1 } },
      eventVersion: 0,
      totalCount: 1,
      getJobsPage: () => [
        {
          id: 'job-repair',
          name: 'featureCounts repair',
          status: 'waiting_for_repair',
          goal: 'Repair featureCounts run on apple RNA-seq data',
          thread_id: 'thread-repair',
          pending_interaction_type: 'repair',
          created_at: '2026-03-27T09:00:00Z',
        },
      ],
      getPageHasMore: () => false,
      patchJob: vi.fn(),
      locateJobPage: vi.fn().mockResolvedValue(1),
      refreshJobPage,
      refreshJobs: vi.fn().mockResolvedValue([]),
      refreshAttentionSummary: vi.fn().mockResolvedValue(undefined),
      refreshIncidents: vi.fn().mockResolvedValue(undefined),
      refreshAll,
    })

    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input)
      if (url.includes('/api/jobs/job-repair/bindings?detailed=1')) {
        return Promise.resolve({
          ok: true,
          json: async () => ({
            job_status: 'waiting_for_repair',
            pending_interaction_type: 'repair',
            pending_interaction_payload: {
              repair_request_id: 'repair-1',
              step_key: 'quant.featurecounts',
              failed_command: 'featureCounts -a genes.gtf -o counts.txt sample.bam',
              stderr_excerpt: 'featureCounts: failed to open annotation file',
            },
            steps: [],
          }),
        })
      }
      if (url.includes('/api/jobs/job-repair/repair-requests/repair-1/resolve')) {
        return Promise.resolve({
          ok: true,
          json: async () => ({ ok: true }),
        })
      }
      return Promise.resolve({
        ok: true,
        json: async () => ({}),
      })
    })

    vi.stubGlobal('fetch', fetchMock)

    renderTaskMonitor({
      projectId: 'proj-1',
      onOpenThread: vi.fn(),
    })

    expect(await screen.findByText('Pending Repair Requests')).toBeInTheDocument()
    expect(screen.getByText('1 task(s) are waiting for repair input.')).toBeInTheDocument()
    const commandMatches = await screen.findAllByText(/featureCounts -a genes\.gtf/)
    expect(commandMatches.length).toBeGreaterThan(0)
    const repairInput = screen.getByDisplayValue('featureCounts -a genes.gtf -o counts.txt sample.bam')
    expect(repairInput.tagName).toBe('TEXTAREA')
    const previewScrollBox = commandMatches
      .map((node) => node.closest('[style]'))
      .find((node) => node?.getAttribute('style')?.includes('max-height: 224px'))
    expect(previewScrollBox).not.toBeNull()
    expect(screen.getByText(/featureCounts: failed to open annotation file/)).toBeInTheDocument()

    fireEvent.change(screen.getByPlaceholderText('e.g. "the index needs to be rebuilt with the correct GTF file"'), {
      target: { value: 'use the correct genes.gtf path' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Send & Retry' }))

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        '/api/jobs/job-repair/repair-requests/repair-1/resolve',
        expect.objectContaining({
          method: 'POST',
        }),
      )
    })
    await waitFor(() => {
      expect(refreshAll).toHaveBeenCalled()
      expect(refreshJobPage).toHaveBeenCalledWith(1, { force: true })
    })
  })

  it('shows an auto-authorization status banner when workspace auto-approve is enabled', async () => {
    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [],
      attentionSummary: {
        signal: 'idle',
        count: 0,
        counts: {
          running: 0,
          authorization: 0,
          repair: 0,
          confirmation: 0,
          clarification: 0,
          warning: 0,
          needs_input: 0,
          needs_review: 0,
        },
        needs_input: [],
        needs_review: [],
        reminders: [],
        auto_authorize_commands: true,
      },
      incidents: [],
      incidentSummary: { total_open: 0, critical: 0, warning: 0, info: 0 },
      overview: { total: 0, active: 0, by_status: {} },
      eventVersion: 0,
      totalCount: 0,
      getJobsPage: () => [],
      getPageHasMore: () => false,
      patchJob: vi.fn(),
      locateJobPage: vi.fn().mockResolvedValue(1),
      refreshJobPage: vi.fn().mockResolvedValue([]),
      refreshJobs: vi.fn().mockResolvedValue([]),
      refreshAttentionSummary: vi.fn().mockResolvedValue(undefined),
      refreshIncidents: vi.fn().mockResolvedValue(undefined),
      refreshAll: vi.fn().mockResolvedValue(undefined),
    })

    vi.stubGlobal('fetch', vi.fn(() => Promise.resolve({ ok: true, json: async () => ({}) })))

    renderTaskMonitor({
      projectId: 'proj-1',
      onOpenThread: vi.fn(),
    })

    expect(await screen.findByText('Automatic Command Authorization')).toBeInTheDocument()
    expect(screen.getByText('New analysis commands are currently being auto-authorized from workspace settings.')).toBeInTheDocument()
    expect(screen.getByText('Per-task authorization prompts are suppressed until you disable the setting in Settings -> Execution.')).toBeInTheDocument()
  })
})
