import type React from 'react'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
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
            thread_id: 'thread-1',
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
          thread_id: 'thread-1',
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
                { step_key: 'build', display_name: 'Build HISAT2 Index', step_type: 'util.hisat2_build' },
                { step_key: 'align', display_name: 'HISAT2 align', step_type: 'align.hisat2' },
                { step_key: 'count', display_name: 'featureCounts', step_type: 'quant.featurecounts' },
              ],
              execution_plan_summary: {
                has_execution_ir: true,
                has_expanded_dag: true,
                group_count: 4,
                node_count: 9,
              },
              execution_confirmation_overview: {
                abstract_step_count: 3,
                execution_ir_step_count: 3,
                execution_group_count: 4,
                per_sample_step_count: 1,
                aggregate_step_count: 1,
                added_group_count: 1,
                changed_group_count: 2,
              },
              execution_ir_review: [
                {
                  step_key: 'align',
                  display_name: 'HISAT2 align',
                  step_type: 'align.hisat2',
                  description: 'per_sample | align | inputs=trimmed_paired_reads, aligner_index | aggregate=same_lineage',
                },
                {
                  step_key: 'count',
                  display_name: 'featureCounts',
                  step_type: 'quant.featurecounts',
                  description: 'aggregate | quantify | inputs=aligned_bam_collection, annotation_gtf | aggregate=all_upstream | depends_on=align',
                },
              ],
              execution_plan_delta: {
                abstract_step_count: 3,
                execution_group_count: 4,
                unchanged_group_count: 1,
                changed_group_count: 2,
                added_group_count: 1,
                added_groups: [
                  { group_key: 'build', display_name: 'Build HISAT2 Index' },
                ],
                changed_groups: [
                  { group_key: 'align', display_name: 'HISAT2 align', change_kinds: ['fan_out'] },
                  { group_key: 'count', display_name: 'featureCounts', change_kinds: ['aggregate'] },
                ],
              },
              execution_plan_changes: [
                {
                  group_key: 'build',
                  display_name: 'Build HISAT2 Index',
                  change_kinds: ['auto_injected'],
                  auto_injected_cause: 'missing_hisat2_index',
                  node_count: 1,
                },
                {
                  group_key: 'align',
                  display_name: 'HISAT2 align',
                  change_kinds: ['fan_out'],
                  fan_out_mode: 'per_sample',
                  node_count: 2,
                },
                {
                  group_key: 'count',
                  display_name: 'featureCounts',
                  change_kinds: ['aggregate'],
                  depends_on: ['align'],
                  node_count: 1,
                },
              ],
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
    expect(screen.getByText('3 abstract steps become 3 execution-semantics steps and 4 execution groups; 1 run per-sample, 1 aggregate upstream outputs, 1 system-added, 2 orchestrated changes.')).toBeInTheDocument()
    expect(screen.getByText('Compared with the abstract plan: 3 abstract steps -> 4 execution groups · 1 unchanged · 2 changed · 1 added')).toBeInTheDocument()
    expect(screen.getByText('Execution semantics')).toBeInTheDocument()
    expect(screen.getByText('align.hisat2 · per_sample | align | inputs=trimmed_paired_reads, aligner_index | aggregate=same_lineage')).toBeInTheDocument()
    expect(screen.getByText('Orchestration changes')).toBeInTheDocument()
    expect(screen.getByText('Fan-out')).toBeInTheDocument()
    expect(screen.getByText('Aggregate')).toBeInTheDocument()
    expect(screen.getByText('Auto-injected')).toBeInTheDocument()
    expect(screen.getByText('Auto-injected to build a missing HISAT2 index from the registered reference FASTA')).toBeInTheDocument()
    expect(screen.getByText('Expanded into 2 per-sample execution nodes')).toBeInTheDocument()
    expect(screen.getByText('Aggregates upstream outputs from align into one execution step')).toBeInTheDocument()
    expect(screen.getByText('Added by orchestration')).toBeInTheDocument()
    expect(screen.getAllByText('Changed by orchestration').length).toBeGreaterThan(0)
    expect(screen.getByText('3 review items')).toBeInTheDocument()
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

    expect(await screen.findByText('Pending Operator Review')).toBeInTheDocument()
    expect(screen.getByText('1 task(s) are waiting for confirmation, clarification, or rollback review.')).toBeInTheDocument()
    expect(screen.getAllByText('Awaiting Confirmation').length).toBeGreaterThanOrEqual(2)
    expect(screen.getByText('Execution graph is waiting for final confirmation.')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Open Execution Review' })).toBeInTheDocument()
    expect(screen.getByText('Return to chat and confirm the execution graph.')).toBeInTheDocument()
  })

  it('allows project-level authorization directly from the supervisor playbook', async () => {
    const refreshAll = vi.fn().mockResolvedValue(undefined)
    const refreshJobPage = vi.fn().mockResolvedValue([])
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input)
      if (url.includes('/api/jobs/supervisor-review?project=proj-1')) {
        return Promise.resolve({
          ok: true,
          json: async () => ({
            mode: 'heuristic',
            generated_at: '2026-03-29T12:00:00Z',
            overview: '1 open incident.',
            supervisor_message: 'A generated command is waiting for authorization.',
            focus_summary: {
              primary_lane: 'operator_review',
              top_owner: 'user',
              top_incident_type: 'authorization',
              high_confidence_total: 0,
              auto_recoverable_total: 0,
              user_wait_total: 1,
              next_best_operator_move: 'resolve_authorization_request',
              lane_reason: 'A command is blocked on explicit user approval.',
              next_best_operator_reason: 'Review the generated command and authorize it if it matches the approved plan.',
            },
            project_playbook: {
              goal: 'operator_review',
              next_move: 'resolve_authorization_request',
              step_codes: ['open_task', 'review_and_authorize_command', 'recheck_task_state'],
            },
            recommendations: [
              {
                priority: 1,
                job_id: 'job-auth',
                job_name: 'FastQC command review',
                thread_id: 'thread-auth',
                incident_type: 'authorization',
                severity: 'info',
                owner: 'user',
                diagnosis: 'A generated command is waiting for authorization.',
                immediate_action: 'review_and_authorize_command',
                why_now: 'Execution cannot continue until the command is approved or rejected.',
                rollback_target: 'qc_fastqc',
              },
            ],
            dossiers: [],
          }),
        })
      }
      if (url.includes('/api/jobs/job-auth/bindings?detailed=1')) {
        return Promise.resolve({
          ok: true,
          json: async () => ({
            pending_interaction_type: 'authorization',
            pending_interaction_payload: {
              auth_request_id: 'auth-1',
              command_type: 'shell',
              step_key: 'qc_fastqc',
              command: 'fastqc sample_R1.fastq.gz -o qc/',
            },
          }),
        })
      }
      if (url === '/api/jobs/job-auth/authorization-requests/auth-1/resolve') {
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

    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-auth',
          name: 'FastQC command review',
          status: 'waiting_for_authorization',
          goal: 'Run FastQC on apple RNA-seq data',
          thread_id: 'thread-auth',
          created_at: '2026-03-29T10:00:00Z',
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
          needs_review: 1,
        },
        needs_input: [
          {
            key: 'job-auth:authorization',
            job_id: 'job-auth',
            job_name: 'FastQC command review',
            thread_id: 'thread-auth',
            incident_type: 'authorization',
            reason: 'authorization',
            age_seconds: 120,
            summary: 'A generated command is waiting for authorization.',
            severity: 'info',
            owner: 'user',
            next_action: 'review_and_authorize_command',
          },
        ],
        needs_review: [
          {
            key: 'job-auth:authorization',
            job_id: 'job-auth',
            job_name: 'FastQC command review',
            thread_id: 'thread-auth',
            incident_type: 'authorization',
            reason: 'warning',
            age_seconds: 120,
            summary: 'A generated command is waiting for authorization.',
            severity: 'info',
            owner: 'user',
          },
        ],
        reminders: [],
        auto_authorize_commands: false,
      },
      incidents: [
        {
          job_id: 'job-auth',
          job_name: 'FastQC command review',
          job_status: 'waiting_for_authorization',
          thread_id: 'thread-auth',
          incident_type: 'authorization',
          severity: 'info',
          owner: 'user',
          summary: 'A generated command is waiting for authorization.',
          next_action: 'review_and_authorize_command',
          age_seconds: 120,
        },
      ],
      incidentSummary: { total_open: 1, critical: 0, warning: 0, info: 1 },
      overview: { total: 1, active: 1, by_status: { waiting_for_authorization: 1 } },
      eventVersion: 0,
      totalCount: 1,
      getJobsPage: () => [
        {
          id: 'job-auth',
          name: 'FastQC command review',
          status: 'waiting_for_authorization',
          goal: 'Run FastQC on apple RNA-seq data',
          thread_id: 'thread-auth',
          created_at: '2026-03-29T10:00:00Z',
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

    vi.stubGlobal('fetch', fetchMock)

    renderTaskMonitor({
      projectId: 'proj-1',
      onOpenThread: vi.fn(),
    })

    fireEvent.click(screen.getByRole('button', { name: /logs/i }))
    fireEvent.click(await screen.findByRole('button', { name: 'Supervisor Review' }))

    const projectPlaybook = (await screen.findByText('Project Playbook')).closest('div')?.parentElement
    expect(projectPlaybook).not.toBeNull()
    expect(within(projectPlaybook as HTMLElement).getByText('fastqc sample_R1.fastq.gz -o qc/')).toBeInTheDocument()

    fireEvent.click(within(projectPlaybook as HTMLElement).getByRole('button', { name: 'Authorize & Run' }))

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        '/api/jobs/job-auth/authorization-requests/auth-1/resolve',
        expect.objectContaining({
          method: 'POST',
        }),
      )
    })
    expect(refreshAll).toHaveBeenCalled()
    expect(refreshJobPage).toHaveBeenCalled()
  })

  it('allows project-level repair submission directly from the supervisor playbook', async () => {
    const refreshAll = vi.fn().mockResolvedValue(undefined)
    const refreshJobPage = vi.fn().mockResolvedValue([])
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input)
      if (url.includes('/api/jobs/supervisor-review?project=proj-1')) {
        return Promise.resolve({
          ok: true,
          json: async () => ({
            mode: 'heuristic',
            generated_at: '2026-03-29T12:10:00Z',
            overview: '1 open incident.',
            supervisor_message: 'A failed command is waiting for repair input.',
            focus_summary: {
              primary_lane: 'operator_review',
              top_owner: 'user',
              top_incident_type: 'repair',
              high_confidence_total: 0,
              auto_recoverable_total: 0,
              user_wait_total: 1,
              next_best_operator_move: 'resolve_repair_request',
              lane_reason: 'A command failed and is waiting for an operator repair decision.',
              next_best_operator_reason: 'Inspect the failing command and submit a repair directly if the fix is clear.',
            },
            project_playbook: {
              goal: 'operator_review',
              next_move: 'resolve_repair_request',
              step_codes: ['open_task', 'review_failure_and_choose_repair', 'recheck_task_state'],
            },
            recommendations: [
              {
                priority: 1,
                job_id: 'job-repair',
                job_name: 'featureCounts repair',
                thread_id: 'thread-repair',
                incident_type: 'repair',
                severity: 'warning',
                owner: 'user',
                diagnosis: 'featureCounts failed and is waiting for a repair command.',
                immediate_action: 'review_failure_and_choose_repair',
                why_now: 'Execution cannot continue until the repair is submitted or cancelled.',
                rollback_target: 'quant_featurecounts',
              },
            ],
            dossiers: [],
          }),
        })
      }
      if (url.includes('/api/jobs/job-repair/bindings?detailed=1')) {
        return Promise.resolve({
          ok: true,
          json: async () => ({
            pending_interaction_type: 'repair',
            pending_interaction_payload: {
              repair_request_id: 'repair-1',
              step_key: 'quant_featurecounts',
              failed_command: 'featureCounts -a genes.gtf -o counts.txt sample.bam',
              stderr_excerpt: 'ERROR: failed to open annotation file genes.gtf',
            },
          }),
        })
      }
      if (url === '/api/jobs/job-repair/repair-requests/repair-1/resolve') {
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

    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-repair',
          name: 'featureCounts repair',
          status: 'waiting_for_repair',
          goal: 'Repair featureCounts on apple RNA-seq data',
          thread_id: 'thread-repair',
          created_at: '2026-03-29T10:30:00Z',
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
          needs_review: 1,
        },
        needs_input: [
          {
            key: 'job-repair:repair',
            job_id: 'job-repair',
            job_name: 'featureCounts repair',
            thread_id: 'thread-repair',
            incident_type: 'repair',
            reason: 'repair',
            age_seconds: 180,
            summary: 'A failed command is waiting for repair input.',
            severity: 'warning',
            owner: 'user',
            next_action: 'review_failure_and_choose_repair',
          },
        ],
        needs_review: [
          {
            key: 'job-repair:repair',
            job_id: 'job-repair',
            job_name: 'featureCounts repair',
            thread_id: 'thread-repair',
            incident_type: 'repair',
            reason: 'warning',
            age_seconds: 180,
            summary: 'A failed command is waiting for repair input.',
            severity: 'warning',
            owner: 'user',
          },
        ],
        reminders: [],
        auto_authorize_commands: false,
      },
      incidents: [
        {
          job_id: 'job-repair',
          job_name: 'featureCounts repair',
          job_status: 'waiting_for_repair',
          thread_id: 'thread-repair',
          incident_type: 'repair',
          severity: 'warning',
          owner: 'user',
          summary: 'A failed command is waiting for repair input.',
          next_action: 'review_failure_and_choose_repair',
          age_seconds: 180,
        },
      ],
      incidentSummary: { total_open: 1, critical: 0, warning: 1, info: 0 },
      overview: { total: 1, active: 1, by_status: { waiting_for_repair: 1 } },
      eventVersion: 0,
      totalCount: 1,
      getJobsPage: () => [
        {
          id: 'job-repair',
          name: 'featureCounts repair',
          status: 'waiting_for_repair',
          goal: 'Repair featureCounts on apple RNA-seq data',
          thread_id: 'thread-repair',
          created_at: '2026-03-29T10:30:00Z',
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

    vi.stubGlobal('fetch', fetchMock)

    renderTaskMonitor({
      projectId: 'proj-1',
      onOpenThread: vi.fn(),
    })

    fireEvent.click(await screen.findByRole('button', { name: 'Supervisor Review' }))

    const projectPlaybook = (await screen.findByText('Project Playbook')).closest('div')?.parentElement
    expect(projectPlaybook).not.toBeNull()
    expect(within(projectPlaybook as HTMLElement).getAllByText('featureCounts -a genes.gtf -o counts.txt sample.bam').length).toBeGreaterThan(0)
    expect(within(projectPlaybook as HTMLElement).getByText('ERROR: failed to open annotation file genes.gtf')).toBeInTheDocument()
    expect(within(projectPlaybook as HTMLElement).getByDisplayValue('featureCounts -a genes.gtf -o counts.txt sample.bam')).toBeInTheDocument()

    fireEvent.change(within(projectPlaybook as HTMLElement).getByRole('textbox'), {
      target: { value: 'featureCounts -a /refs/genes.gtf -o counts.txt sample.bam' },
    })
    fireEvent.click(within(projectPlaybook as HTMLElement).getByRole('button', { name: 'Send & Retry' }))

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        '/api/jobs/job-repair/repair-requests/repair-1/resolve',
        expect.objectContaining({
          method: 'POST',
          body: JSON.stringify({
            choice: 'modify_params',
            params: { command: 'featureCounts -a /refs/genes.gtf -o counts.txt sample.bam' },
          }),
        }),
      )
    })
    expect(refreshAll).toHaveBeenCalled()
    expect(refreshJobPage).toHaveBeenCalled()
  })

  it('routes resource clarification playbooks directly to the chat thread', async () => {
    const onOpenThread = vi.fn()

    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-clarify',
          name: 'Reference clarification',
          status: 'resource_clarification_required',
          goal: 'Clarify which reference resource should be used for apple RNA-seq analysis',
          thread_id: 'thread-clarify',
          created_at: '2026-03-29T11:00:00Z',
        },
      ],
      attentionSummary: {
        signal: 'attention',
        count: 1,
        counts: {
          running: 0,
          authorization: 0,
          repair: 0,
          confirmation: 0,
          clarification: 1,
          warning: 0,
          needs_input: 1,
          needs_review: 0,
        },
        needs_input: [
          {
            key: 'job-clarify:clarification',
            job_id: 'job-clarify',
            job_name: 'Reference clarification',
            thread_id: 'thread-clarify',
            incident_type: 'resource_clarification',
            reason: 'clarification',
            age_seconds: 150,
            summary: 'The workflow needs a clarification about the correct reference resource before execution can continue.',
            severity: 'info',
            owner: 'user',
            next_action: 'provide_missing_resource_clarification',
            rollback_level: 'execution_ir',
          },
        ],
        needs_review: [],
        reminders: [],
        auto_authorize_commands: false,
      },
      incidents: [
        {
          job_id: 'job-clarify',
          job_name: 'Reference clarification',
          job_status: 'resource_clarification_required',
          thread_id: 'thread-clarify',
          incident_type: 'resource_clarification',
          severity: 'info',
          owner: 'user',
          summary: 'The workflow needs a clarification about the correct reference resource before execution can continue.',
          next_action: 'provide_missing_resource_clarification',
          age_seconds: 150,
        },
      ],
      incidentSummary: { total_open: 1, critical: 0, warning: 0, info: 1 },
      overview: { total: 1, active: 1, by_status: { resource_clarification_required: 1 } },
      eventVersion: 0,
      totalCount: 1,
      getJobsPage: () => [
        {
          id: 'job-clarify',
          name: 'Reference clarification',
          status: 'resource_clarification_required',
          goal: 'Clarify which reference resource should be used for apple RNA-seq analysis',
          thread_id: 'thread-clarify',
          created_at: '2026-03-29T11:00:00Z',
        },
      ],
      getPageHasMore: () => false,
      patchJob: vi.fn(),
      locateJobPage: vi.fn().mockResolvedValue(1),
      refreshJobPage: vi.fn().mockResolvedValue([]),
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
              generated_at: '2026-03-29T12:20:00Z',
              overview: '1 open incident.',
              supervisor_message: 'Execution is paused until the missing resource clarification is provided.',
              focus_summary: {
                primary_lane: 'operator_review',
                top_owner: 'user',
                top_incident_type: 'resource_clarification',
                high_confidence_total: 0,
                auto_recoverable_total: 0,
                user_wait_total: 1,
                next_best_operator_move: 'resolve_resource_clarification',
                lane_reason: 'The current project needs a user clarification before orchestration can continue safely.',
                next_best_operator_reason: 'Return to chat, clarify the resource choice, then rebuild the execution graph if needed.',
              },
              project_playbook: {
                goal: 'operator_review',
                next_move: 'resolve_resource_clarification',
                step_codes: ['open_chat', 'provide_missing_resource_clarification', 'recheck_task_state'],
              },
              recommendations: [
                {
                  priority: 1,
                  job_id: 'job-clarify',
                  job_name: 'Reference clarification',
                  thread_id: 'thread-clarify',
                  incident_type: 'resource_clarification',
                  severity: 'info',
                  owner: 'user',
                  diagnosis: 'A resource clarification is required before execution can continue.',
                  immediate_action: 'provide_missing_resource_clarification',
                  why_now: 'The orchestrator cannot safely proceed without this clarification.',
                  rollback_target: 'resource_clarification_gate',
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
      onOpenThread,
    })

    fireEvent.click(await screen.findByRole('button', { name: 'Supervisor Review' }))

    const projectPlaybook = (await screen.findByText('Project Playbook')).closest('div')?.parentElement
    expect(projectPlaybook).not.toBeNull()
    expect(within(projectPlaybook as HTMLElement).getByText('Goal: operator review · Next move: resolve resource clarification')).toBeInTheDocument()
    expect(within(projectPlaybook as HTMLElement).getByText('2. Next: provide the missing resource clarification in chat.')).toBeInTheDocument()
    fireEvent.click(within(projectPlaybook as HTMLElement).getByRole('button', { name: 'Open Chat' }))
    expect(onOpenThread).toHaveBeenCalledWith('thread-clarify', 'job-clarify')
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

    const projectPlaybook = (await screen.findByText('Project Playbook')).closest('div')?.parentElement
    expect(projectPlaybook).not.toBeNull()
    const button = within(projectPlaybook as HTMLElement).getByRole('button', { name: 'Open Resource Registry' })
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

    expect(await screen.findByText('Resolved decision types: Repair')).toBeInTheDocument()
    expect(screen.getByText('Pending reference types: Auth')).toBeInTheDocument()
    expect(
      screen.getByText('Blockers: resolved decision type does not match pending request reference'),
    ).toBeInTheDocument()
  })

  it('falls back to blocking summary workspace actions when no dominant blocker is provided', async () => {
    const onOpenResourceWorkspace = vi.fn()

    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-1',
          name: 'RNA-seq confirmation',
          status: 'binding_required',
          goal: 'Analyze apple RNA-seq data',
          thread_id: 'thread-1',
          created_at: '2026-03-28T10:00:00Z',
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
            key: 'job-1:warning',
            job_id: 'job-1',
            job_name: 'RNA-seq confirmation',
            thread_id: 'thread-1',
            incident_type: 'binding',
            reason: 'warning',
            age_seconds: 240,
            summary: 'Binding is blocked because multiple reference candidates match.',
            severity: 'warning',
            owner: 'system',
            next_action: 'inspect_bindings_and_resume',
          },
        ],
        reminders: [],
        auto_authorize_commands: false,
      },
      incidents: [
        {
          job_id: 'job-1',
          job_name: 'RNA-seq confirmation',
          job_status: 'binding_required',
          thread_id: 'thread-1',
          incident_type: 'binding',
          severity: 'warning',
          owner: 'system',
          summary: 'Binding is blocked because multiple reference candidates match.',
          next_action: 'inspect_bindings_and_resume',
          age_seconds: 240,
        },
      ],
      incidentSummary: { total_open: 1, critical: 0, warning: 1, info: 0 },
      overview: { total: 1, active: 1, by_status: { binding_required: 1 } },
      eventVersion: 0,
      totalCount: 1,
      getJobsPage: () => [
        {
          id: 'job-1',
          name: 'RNA-seq confirmation',
          status: 'binding_required',
          goal: 'Analyze apple RNA-seq data',
          thread_id: 'thread-1',
          created_at: '2026-03-28T10:00:00Z',
        },
      ],
      getPageHasMore: () => false,
      patchJob: vi.fn(),
      locateJobPage: vi.fn().mockResolvedValue(1),
      refreshJobPage: vi.fn().mockResolvedValue([]),
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
              generated_at: '2026-03-28T14:00:00Z',
              overview: '1 open incident.',
              supervisor_message: 'Resolve the ambiguous reference candidate set.',
              focus_summary: {
                primary_lane: 'resource_readiness',
                next_best_operator_move: 'resolve_ambiguous_resource_candidates',
              },
              project_playbook: {
                goal: 'resource_readiness',
                next_move: 'resolve_ambiguous_resource_candidates',
                step_codes: ['open_task', 'inspect_resource_candidates'],
              },
              recommendations: [
                {
                  priority: 1,
                  job_id: 'job-1',
                  job_name: 'RNA-seq confirmation',
                  incident_type: 'binding',
                  severity: 'warning',
                  owner: 'system',
                  diagnosis: 'Multiple reference candidates match the current project.',
                  immediate_action: 'inspect_bindings_and_resume',
                  why_now: 'Execution cannot continue until one candidate is selected.',
                  rollback_target: 'binding_resolution',
                },
              ],
              dossiers: [
                {
                  job_id: 'job-1',
                  resource_graph: {
                    blocking_nodes: [],
                    blocking_summary: [
                      {
                        id: 'ref-choice',
                        label: 'Reference candidates',
                        status: 'ambiguous',
                        cause: 'ambiguous_candidates',
                        recommended_action: 'resolve_ambiguous_resource_candidates',
                        registry_key: 'reference_fasta',
                        workspace_section: 'recognized',
                        preferred_candidate: {
                          path: '/refs/apple.fa',
                        },
                      },
                    ],
                    dominant_blocker: null,
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

    fireEvent.click(screen.getByRole('button', { name: /logs/i }))
    fireEvent.click(await screen.findByRole('button', { name: 'Supervisor Review' }))

    const projectPlaybook = (await screen.findByText('Project Playbook')).closest('div')?.parentElement
    expect(projectPlaybook).not.toBeNull()

    fireEvent.click(within(projectPlaybook as HTMLElement).getByRole('button', { name: 'Review Recognized Resources' }))
    await waitFor(() => {
      expect(onOpenResourceWorkspace).toHaveBeenCalledWith(
        expect.objectContaining({
          tab: 'project-info',
          focusSection: 'recognized',
          key: 'reference_fasta',
          path: '/refs/apple.fa',
          description: 'Reference candidates',
        }),
      )
    })

    fireEvent.click(within(projectPlaybook as HTMLElement).getByRole('button', { name: 'Open Resource Registry' }))
    await waitFor(() => {
      expect(onOpenResourceWorkspace).toHaveBeenCalledWith(
        expect.objectContaining({
          tab: 'project-info',
          focusSection: 'registry',
          key: 'reference_fasta',
          path: '/refs/apple.fa',
          description: 'Reference candidates',
        }),
      )
    })
  })

  it('renders environment readiness focus summary and project playbook from supervisor review', async () => {
    const onOpenSettingsDiagnostics = vi.fn()

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
      onOpenSettingsDiagnostics,
    })

    fireEvent.click(await screen.findByRole('button', { name: 'Supervisor Review' }))

    expect(await screen.findByText(/lane=environment readiness/)).toBeInTheDocument()
    expect(screen.getByText(/next=inspect environment failure/)).toBeInTheDocument()
    expect(screen.getByText('Project Playbook')).toBeInTheDocument()
    expect(screen.getByText('Goal: environment readiness · Next move: inspect environment failure')).toBeInTheDocument()
    expect(screen.getByText('2. Next: inspect environment diagnostics, package mapping, and implicated steps.')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Open Settings' }))
    expect(onOpenSettingsDiagnostics).toHaveBeenCalledTimes(1)
  })

  it('renders translated resource decision mismatch guidance in supervisor focus summary', async () => {
    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-1',
          name: 'RNA-seq bind review',
          status: 'binding_required',
          goal: 'Resolve resource mismatch',
          thread_id: 'thread-1',
          created_at: '2026-03-29T10:00:00Z',
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
            job_name: 'RNA-seq bind review',
            incident_type: 'binding',
            reason: 'warning',
            age_seconds: 90,
            summary: 'Recognized resource path disagrees with current registered path.',
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
          job_name: 'RNA-seq bind review',
          job_status: 'binding_required',
          incident_type: 'binding',
          severity: 'warning',
          owner: 'system',
          summary: 'Recognized resource path disagrees with current registered path.',
          next_action: 'inspect_bindings_and_resume',
          age_seconds: 90,
          thread_id: 'thread-1',
        },
      ],
      incidentSummary: { total_open: 1, critical: 0, warning: 1, info: 0 },
      overview: { total: 1, active: 1, by_status: { binding_required: 1 } },
      eventVersion: 0,
      totalCount: 1,
      getJobsPage: () => [
        {
          id: 'job-1',
          name: 'RNA-seq bind review',
          status: 'binding_required',
          goal: 'Resolve resource mismatch',
          thread_id: 'thread-1',
          created_at: '2026-03-29T10:00:00Z',
        },
      ],
      getPageHasMore: () => false,
      patchJob: vi.fn(),
      locateJobPage: vi.fn().mockResolvedValue(1),
      refreshJobPage: vi.fn().mockResolvedValue([
        {
          id: 'job-1',
          name: 'RNA-seq bind review',
          status: 'binding_required',
          goal: 'Resolve resource mismatch',
          thread_id: 'thread-1',
          created_at: '2026-03-29T10:00:00Z',
        },
      ]),
      refreshJobs: vi.fn().mockResolvedValue([
        {
          id: 'job-1',
          name: 'RNA-seq bind review',
          status: 'binding_required',
          goal: 'Resolve resource mismatch',
          thread_id: 'thread-1',
          created_at: '2026-03-29T10:00:00Z',
        },
      ]),
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
              generated_at: '2026-03-29T15:00:00Z',
              overview: '1 open incident.',
              supervisor_message: 'Resolve the resource registration mismatch.',
              focus_summary: {
                primary_lane: 'resource_readiness',
                top_blocker_cause: 'registered_path_mismatch',
                next_best_operator_move: 'resolve_resource_registration_mismatch',
                lane_reason: 'Resource readiness is dominated by recognized resources disagreeing with current registered paths.',
                next_best_operator_reason: 'Review the recognized resource mismatch and either replace the registered path or explicitly keep the current registration before resuming binding.',
              },
              project_playbook: {
                goal: 'resource_readiness',
                next_move: 'resolve_resource_registration_mismatch',
                step_codes: ['open_task', 'inspect_resource_candidates', 'resolve_resource_registration_mismatch', 'recheck_task_state'],
              },
              recommendations: [
                {
                  priority: 1,
                  job_id: 'job-1',
                  job_name: 'RNA-seq bind review',
                  incident_type: 'binding',
                  severity: 'warning',
                  owner: 'system',
                  diagnosis: 'Recognized resource path disagrees with the registered path.',
                  immediate_action: 'inspect_bindings_and_resume',
                  why_now: 'Binding should not continue until the resource registration is confirmed.',
                  rollback_target: 'binding_resolution',
                },
              ],
              dossiers: [
                {
                  job_id: 'job-1',
                  resource_decisions: {
                    available: true,
                    tracked_total: 1,
                    mismatch_total: 1,
                    stale_decision_total: 0,
                    entries: [],
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
    })

    fireEvent.click(await screen.findByRole('button', { name: 'Supervisor Review' }))

    expect(await screen.findByText(/cause=registered path mismatch/)).toBeInTheDocument()
    expect(screen.getByText(/next=resolve resource registration mismatch/)).toBeInTheDocument()
    expect(screen.getByText('Goal: resource readiness · Next move: resolve resource registration mismatch')).toBeInTheDocument()
    expect(screen.getByText('3. Next: inspect the recognized resource mismatch, then choose whether to use the detected path or keep the current registration.')).toBeInTheDocument()
  })

  it('renders latest watchdog auto recovery in supervisor focus summary', async () => {
    const patchJob = vi.fn()
    const refreshJobs = vi.fn().mockResolvedValue([])
    const refreshIncidents = vi.fn().mockResolvedValue(undefined)

    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-resume',
          name: 'Resume auth chain',
          status: 'waiting_for_authorization',
          goal: 'Resume RNA-seq pipeline',
          thread_id: 'thread-resume',
          created_at: '2026-03-28T10:00:00Z',
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
            age_seconds: 240,
            summary: 'Resume chain stalled after a resolved decision.',
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
          summary: 'Resume chain stalled after a resolved decision.',
          next_action: 'inspect_resume_chain',
          age_seconds: 240,
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
          created_at: '2026-03-28T10:00:00Z',
        },
      ],
      getPageHasMore: () => false,
      patchJob,
      locateJobPage: vi.fn().mockResolvedValue(1),
      refreshJobPage: vi.fn().mockResolvedValue([]),
      refreshJobs,
      refreshAttentionSummary: vi.fn().mockResolvedValue(undefined),
      refreshIncidents,
      refreshAll: vi.fn().mockResolvedValue(undefined),
    })

    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input)
      if (url.includes('/api/jobs/supervisor-review?project=proj-1')) {
        return Promise.resolve({
          ok: true,
          json: async () => ({
            mode: 'heuristic',
            generated_at: '2026-03-28T13:00:00Z',
            overview: '1 open incident.',
            supervisor_message: 'Watchdog recently retried the preserved resume chain.',
            focus_summary: {
              primary_lane: 'runtime_recovery',
              top_owner: 'system',
              top_incident_type: 'resume_failed',
              high_confidence_total: 0,
              auto_recoverable_total: 1,
              user_wait_total: 0,
              next_best_operator_move: 'apply_runtime_recovery',
              lane_reason: 'The current project is mainly in runtime recovery / normalization work.',
              next_best_operator_reason:
                'Apply the top safe action or normalization path, then recheck job state consistency.',
              latest_auto_recovery_issue: 'resume_failed',
              latest_auto_recovery_action: 'retry_resume_chain',
              latest_auto_recovery_status: 'waiting_for_authorization',
              latest_auto_recovery_pending_types: 'authorization',
              latest_auto_recovery_job_id: 'job-resume',
            },
            project_playbook: {
              goal: 'runtime_recovery',
              next_move: 'apply_runtime_recovery',
              step_codes: ['open_task', 'apply_safe_action', 'recheck_task_state'],
            },
            recommendations: [
              {
                priority: 1,
                job_id: 'job-resume',
                job_name: 'Resume auth chain',
                thread_id: 'thread-resume',
                incident_type: 'resume_failed',
                severity: 'warning',
                owner: 'system',
                diagnosis: 'Resume chain stalled after a resolved decision.',
                immediate_action: 'inspect_resume_chain',
                why_now: 'Authorization is still pending after watchdog retry.',
                safe_action: 'retry_resume_chain',
                safe_action_eligibility: {
                  current_job_status: 'waiting_for_authorization',
                },
                rollback_target: 'align',
              },
            ],
            dossiers: [],
          }),
        })
      }
      if (url === '/api/jobs/job-resume/supervisor-actions/execute') {
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

    vi.stubGlobal(
      'fetch',
      fetchMock,
    )

    renderTaskMonitor({
      projectId: 'proj-1',
      onOpenThread: vi.fn(),
    })

    fireEvent.click(await screen.findByRole('button', { name: 'Supervisor Review' }))

    expect(
      await screen.findByText(/Recent auto recovery: Resolved decision did not resume -> Retry Resume Chain -> Awaiting Authorization · pending=authorization/),
    ).toBeInTheDocument()
    const projectPlaybook = (await screen.findByText('Project Playbook')).closest('div')?.parentElement
    expect(projectPlaybook).not.toBeNull()
    fireEvent.click(within(projectPlaybook as HTMLElement).getByRole('button', { name: 'Retry Resume Chain' }))
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        '/api/jobs/job-resume/supervisor-actions/execute',
        expect.objectContaining({
          method: 'POST',
        }),
      )
    })
    expect(patchJob).toHaveBeenCalledWith('job-resume', { status: 'waiting_for_authorization' })
    expect(refreshJobs).toHaveBeenCalled()
    expect(refreshIncidents).toHaveBeenCalled()
  })

  it('renders rollback hint and recovery playbooks from supervisor review dossiers', async () => {
    const onOpenThread = vi.fn()

    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-resume',
          name: 'Resume auth chain',
          status: 'waiting_for_authorization',
          goal: 'Resume RNA-seq pipeline',
          thread_id: 'thread-resume',
          created_at: '2026-03-28T10:00:00Z',
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
            age_seconds: 240,
            summary: 'Resume chain stalled after a resolved decision.',
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
          summary: 'Resume chain stalled after a resolved decision.',
          next_action: 'inspect_resume_chain',
          age_seconds: 240,
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
          created_at: '2026-03-28T10:00:00Z',
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
          created_at: '2026-03-28T10:00:00Z',
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
              generated_at: '2026-03-28T13:00:00Z',
              overview: '1 open incident.',
              supervisor_message: 'Resume-chain retry is intentionally withheld until the expanded DAG is reviewed.',
              focus_summary: {
                primary_lane: 'rollback_review',
                top_owner: 'system',
                top_incident_type: 'resume_failed',
                high_confidence_total: 0,
                auto_recoverable_total: 0,
                user_wait_total: 0,
                top_failure_layer: 'step_execution',
                top_rollback_level: 'dag',
                top_rollback_target: 'align',
                top_historical_rollback_level: 'dag',
                top_historical_rollback_alignment: true,
                top_historical_rollback_target: 'align',
                top_historical_rollback_target_alignment: true,
                next_best_operator_move: 'review_rollback_scope',
                lane_reason: 'The current project is blocked on runtime recovery choices.',
                next_best_operator_reason:
                  'Review the rollback scope before retrying the preserved resume chain.',
              },
              project_playbook: {
                goal: 'rollback_review',
                next_move: 'review_rollback_scope',
                step_codes: ['inspect_resume_chain', 'open_chat', 'recheck_task_state'],
              },
              recommendations: [
                {
                  priority: 1,
                  job_id: 'job-resume',
                  job_name: 'Resume auth chain',
                  incident_type: 'resume_failed',
                  severity: 'warning',
                  owner: 'system',
                  diagnosis:
                    'A human decision was already resolved, but the resume chain did not complete. Rollback hint: The current failing / blocked step was materially re-orchestrated in the expanded DAG, so rollback should revisit the execution graph.',
                  immediate_action: 'inspect_resume_chain',
                  why_now: 'Blind retry would skip the orchestration review that changed the current lane.',
                  failure_layer: 'step_execution',
                  rollback_level: 'dag',
                  rollback_target: 'align',
                  reconfirmation_required: true,
                  historical_matches: 2,
                  safe_action: null,
                  safe_action_note:
                    'Resume-chain retry is intentionally withheld because the safer rollback scope is dag, so operator review should revisit that layer before resuming.',
                  historical_guidance:
                    'Project memory most often resolved similar incidents via retry_resume_chain (2/2), but the current recommendation intentionally withholds that path because the safer rollback scope is dag.',
                  recommended_action_confidence: 'low',
                  recommended_action_basis: ['no_safe_action', 'historical_rollback_divergence', 'historical_target_alignment'],
                  historical_policy: {
                    preferred_safe_action: 'retry_resume_chain',
                    support_count: 2,
                    total_matches: 2,
                    confidence: 'high',
                    current_safe_action: 'retry_resume_chain',
                    current_supported_count: 2,
                    aligns_with_current: null,
                    preferred_rollback_level: 'dag',
                    current_rollback_level: 'dag',
                    rollback_level_supported_count: 2,
                    rollback_level_aligns_with_current: true,
                    preferred_rollback_target: 'align',
                    current_rollback_target: 'align',
                    rollback_target_supported_count: 2,
                    rollback_target_aligns_with_current: true,
                  },
                  recovery_playbook: {
                    goal: 'restore_execution_progress',
                    rollback_target: 'align',
                    step_codes: ['inspect_resume_chain', 'open_chat', 'open_task', 'recheck_task_state'],
                  },
                },
              ],
              dossiers: [
                {
                  job_id: 'job-resume',
                  summary: 'exec_overview=abstract:3,ir:3,groups:4,added:1,changed:2 · rollback_hint=dag',
                  rollback_hint: {
                    suggested_level: 'dag',
                    reason:
                      'The current failing / blocked step was materially re-orchestrated in the expanded DAG, so rollback should revisit the execution graph.',
                  },
                  execution_confirmation_overview: {
                    abstract_step_count: 3,
                    execution_ir_step_count: 3,
                    execution_group_count: 4,
                    per_sample_step_count: 1,
                    aggregate_step_count: 1,
                    added_group_count: 1,
                    changed_group_count: 2,
                  },
                  runtime_diagnostics: [],
                  recent_decisions: [],
                  recent_logs: [],
                  auto_recovery_events: [],
                  pending_requests: {
                    active_type: null,
                    auth_request_id: 'auth-1',
                    repair_request_id: null,
                    has_payload: false,
                    diagnostic_kinds: ['resolved_pending_request'],
                    diagnostic_types: ['authorization'],
                    recent_authorizations: [],
                    recent_repairs: [],
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
      onOpenThread,
    })

    fireEvent.click(await screen.findByRole('button', { name: 'Supervisor Review' }))

    const projectPlaybook = (await screen.findByText('Project Playbook')).closest('div')?.parentElement
    expect(projectPlaybook).not.toBeNull()
    const focusLine = screen.getByText(/lane=rollback review/)
    expect(focusLine).toBeInTheDocument()
    expect(focusLine.textContent || '').toContain('layer=Step Execution')
    expect(focusLine.textContent || '').toContain('rollback=DAG')
    expect(focusLine.textContent || '').toContain('history_rollback=DAG')
    expect(focusLine.textContent || '').toContain('history_align=aligned')
    expect(focusLine.textContent || '').toContain('target=align')
    expect(focusLine.textContent || '').toContain('history_target=align')
    expect(focusLine.textContent || '').toContain('target_align=aligned')
    expect(focusLine.textContent || '').toContain('next=review rollback scope')
    expect(screen.getByText('Failure layer: Step Execution')).toBeInTheDocument()
    expect(screen.getByText('Rollback hint: DAG · The current failing / blocked step was materially re-orchestrated in the expanded DAG, so rollback should revisit the execution graph.')).toBeInTheDocument()
    expect(screen.getByText('Needs reconfirmation: Yes · History: matches=2')).toBeInTheDocument()
    expect(screen.getByText('Execution overview: 3 abstract steps become 3 execution-semantics steps and 4 execution groups; 1 run per-sample, 1 aggregate upstream outputs, 1 system-added, 2 orchestrated changes.')).toBeInTheDocument()
    expect(screen.getByText('Action boundary: Resume-chain retry is intentionally withheld because the safer rollback scope is dag, so operator review should revisit that layer before resuming.')).toBeInTheDocument()
    expect(screen.getByText('Historical guidance: Project memory most often resolved similar incidents via retry_resume_chain (2/2), but the current recommendation intentionally withholds that path because the safer rollback scope is dag.')).toBeInTheDocument()
    const historicalPolicyLine = screen.getByText(/Historical policy:/)
    expect(historicalPolicyLine).toBeInTheDocument()
    expect(historicalPolicyLine.textContent || '').toContain('prefer=Retry Resume Chain')
    expect(historicalPolicyLine.textContent || '').toContain('current action=Retry Resume Chain')
    expect(historicalPolicyLine.textContent || '').toContain('current support=2')
    expect(historicalPolicyLine.textContent || '').toContain('rollback=DAG')
    expect(historicalPolicyLine.textContent || '').toContain('current rollback')
    expect(historicalPolicyLine.textContent || '').toContain('rollback support=2')
    expect(historicalPolicyLine.textContent || '').toContain('rollback alignment')
    expect(historicalPolicyLine.textContent || '').toContain('target=align')
    expect(historicalPolicyLine.textContent || '').toContain('current target=align')
    expect(historicalPolicyLine.textContent || '').toContain('target support=2')
    expect(historicalPolicyLine.textContent || '').toContain('target alignment')
    expect(screen.getByText(/Basis: no safe action available · historical rollback scope diverges · historical rollback target aligns/)).toBeInTheDocument()
    expect(screen.getByText('Recovery Playbook')).toBeInTheDocument()
    expect(screen.getAllByText('2. Open Chat').length).toBeGreaterThan(0)
    expect(screen.getByText('3. Open Task')).toBeInTheDocument()
    fireEvent.click(within(projectPlaybook as HTMLElement).getByRole('button', { name: 'Open Chat' }))
    expect(onOpenThread).toHaveBeenCalledWith('thread-resume', 'job-resume')
    expect(within(projectPlaybook as HTMLElement).getByRole('button', { name: 'Open Task' })).toBeInTheDocument()
  })

  it('shows rollback guidance directly in task details and timeline', async () => {
    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-resume',
          name: 'Resume auth chain',
          status: 'waiting_for_authorization',
          goal: 'Resume RNA-seq pipeline',
          thread_id: 'thread-resume',
          created_at: '2026-03-28T10:00:00Z',
        },
      ],
      attentionSummary: {
        signal: 'warning',
        count: 1,
        counts: {
          running: 0,
          authorization: 1,
          repair: 0,
          confirmation: 0,
          clarification: 0,
          warning: 1,
          needs_input: 1,
          needs_review: 0,
        },
        needs_input: [],
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
          id: 'job-resume',
          name: 'Resume auth chain',
          status: 'waiting_for_authorization',
          goal: 'Resume RNA-seq pipeline',
          thread_id: 'thread-resume',
          created_at: '2026-03-28T10:00:00Z',
        },
      ],
      getPageHasMore: () => false,
      patchJob: vi.fn(),
      locateJobPage: vi.fn().mockResolvedValue(1),
      refreshJobPage: vi.fn().mockResolvedValue([]),
      refreshJobs: vi.fn().mockResolvedValue([]),
      refreshAttentionSummary: vi.fn().mockResolvedValue(undefined),
      refreshIncidents: vi.fn().mockResolvedValue(undefined),
      refreshAll: vi.fn().mockResolvedValue(undefined),
    })

    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input)
        if (url.includes('/api/jobs/job-resume/bindings?detailed=1')) {
          return Promise.resolve({
            ok: true,
            json: async () => ({
              job_status: 'waiting_for_authorization',
              pending_interaction_type: 'authorization',
              pending_interaction_payload: {
                auth_request_id: 'auth-1',
                command_type: 'fastqc',
              },
              rollback_guidance: {
                level: 'dag',
                target: 'align',
                reconfirmation_required: true,
                reason:
                  'The current failing / blocked step was materially re-orchestrated in the expanded DAG, so rollback should revisit the execution graph.',
                historical_matches: 2,
                historical_same_level_count: 2,
                historical_same_target_count: 2,
              },
              timeline: [
                {
                  ts: '2026-03-28T10:06:00Z',
                  kind: 'rollback_guidance',
                  source: 'supervisor',
                  category: 'recovery',
                  title: 'Rollback review recommended',
                  detail:
                    'level=dag · target=align · reconfirm=true · The current failing / blocked step was materially re-orchestrated in the expanded DAG, so rollback should revisit the execution graph.',
                },
              ],
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

    fireEvent.click(await screen.findByRole('button', { name: 'Logs' }))

    expect(await screen.findByText('Rollback Guidance')).toBeInTheDocument()
    expect(screen.getByText(/Rollback level: DAG/)).toBeInTheDocument()
    expect(screen.getByText(/Target: align/)).toBeInTheDocument()
    expect(screen.getByText(/Needs reconfirmation: Yes/)).toBeInTheDocument()
    expect(screen.getAllByText(/The current failing \/ blocked step was materially re-orchestrated/).length).toBeGreaterThan(0)
    expect(screen.getByText(/History: matches=2 · same level=2 · same target=2/)).toBeInTheDocument()
    expect(screen.getByText('Rollback review recommended')).toBeInTheDocument()
  })

  it('shows rollback scope directly in the pending input queue for rollback review items', async () => {
    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-rollback',
          name: 'Resume auth chain',
          status: 'waiting_for_authorization',
          goal: 'Resume RNA-seq pipeline',
          thread_id: 'thread-rollback',
          created_at: '2026-03-28T10:00:00Z',
        },
      ],
      attentionSummary: {
        signal: 'attention',
        count: 1,
        counts: {
          running: 0,
          authorization: 0,
          repair: 0,
          confirmation: 0,
          clarification: 0,
          rollback_review: 1,
          warning: 0,
          needs_input: 1,
          needs_review: 0,
        },
        needs_input: [
          {
            key: 'job-rollback:rollback_review',
            job_id: 'job-rollback',
            job_name: 'Resume auth chain',
            thread_id: 'thread-rollback',
            incident_type: 'resume_failed',
            reason: 'rollback_review',
            age_seconds: 420,
            summary: 'Resume chain stalled after a resolved decision.',
            severity: 'warning',
            owner: 'system',
            rollback_level: 'dag',
            rollback_target: 'align',
            rollback_reason: 'The current failing / blocked step was materially re-orchestrated in the expanded DAG, so rollback should revisit the execution graph.',
            reconfirmation_required: true,
          },
        ],
        needs_review: [],
        reminders: [],
        auto_authorize_commands: false,
      },
      incidents: [
        {
          job_id: 'job-rollback',
          job_name: 'Resume auth chain',
          job_status: 'waiting_for_authorization',
          incident_type: 'resume_failed',
          severity: 'warning',
          owner: 'system',
          summary: 'Resume chain stalled after a resolved decision.',
          next_action: 'inspect_resume_chain',
          age_seconds: 420,
          thread_id: 'thread-rollback',
        },
      ],
      incidentSummary: { total_open: 1, critical: 0, warning: 1, info: 0 },
      overview: { total: 1, active: 1, by_status: { waiting_for_authorization: 1 } },
      eventVersion: 0,
      totalCount: 1,
      getJobsPage: () => [
        {
          id: 'job-rollback',
          name: 'Resume auth chain',
          status: 'waiting_for_authorization',
          goal: 'Resume RNA-seq pipeline',
          thread_id: 'thread-rollback',
          created_at: '2026-03-28T10:00:00Z',
        },
      ],
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

    expect(await screen.findByText('Pending Operator Review')).toBeInTheDocument()
    expect(screen.getByText('Needs Rollback Review')).toBeInTheDocument()
    expect(screen.getByText(/Rollback scope: Rollback level: DAG · target=align · Needs reconfirmation: Yes/)).toBeInTheDocument()
    expect(screen.getByText(/Why rollback: The current failing \/ blocked step was materially re-orchestrated/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Open Execution Review' })).toBeInTheDocument()
    expect(screen.getByText('Return to chat and confirm the execution graph.')).toBeInTheDocument()
  })

  it('prioritizes rollback review items ahead of other pending operator work', async () => {
    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-confirm',
          name: 'RNA-seq confirmation',
          status: 'awaiting_plan_confirmation',
          goal: 'Analyze apple RNA-seq data',
          thread_id: 'thread-confirm',
          created_at: '2026-03-28T10:00:00Z',
        },
        {
          id: 'job-rollback',
          name: 'Resume auth chain',
          status: 'waiting_for_authorization',
          goal: 'Resume RNA-seq pipeline',
          thread_id: 'thread-rollback',
          created_at: '2026-03-28T11:00:00Z',
        },
      ],
      attentionSummary: {
        signal: 'attention',
        count: 2,
        counts: {
          running: 0,
          authorization: 0,
          repair: 0,
          confirmation: 1,
          clarification: 0,
          rollback_review: 1,
          warning: 0,
          needs_input: 2,
          needs_review: 0,
        },
        needs_input: [
          {
            key: 'job-confirm:confirmation',
            job_id: 'job-confirm',
            job_name: 'RNA-seq confirmation',
            thread_id: 'thread-confirm',
            incident_type: 'execution_confirmation',
            reason: 'confirmation',
            age_seconds: 120,
            summary: 'Execution graph is waiting for final confirmation.',
            severity: 'info',
            owner: 'user',
          },
          {
            key: 'job-rollback:rollback_review',
            job_id: 'job-rollback',
            job_name: 'Resume auth chain',
            thread_id: 'thread-rollback',
            incident_type: 'resume_failed',
            reason: 'rollback_review',
            age_seconds: 60,
            summary: 'Resume chain stalled after a resolved decision.',
            severity: 'warning',
            owner: 'system',
            rollback_level: 'dag',
            rollback_target: 'align',
            reconfirmation_required: true,
          },
        ],
        needs_review: [],
        reminders: [],
        auto_authorize_commands: false,
      },
      incidents: [
        {
          job_id: 'job-confirm',
          job_name: 'RNA-seq confirmation',
          job_status: 'awaiting_plan_confirmation',
          incident_type: 'execution_confirmation',
          severity: 'info',
          owner: 'user',
          summary: 'Execution graph is waiting for final confirmation.',
          next_action: 'confirm_or_edit_execution',
          age_seconds: 120,
          thread_id: 'thread-confirm',
        },
        {
          job_id: 'job-rollback',
          job_name: 'Resume auth chain',
          job_status: 'waiting_for_authorization',
          incident_type: 'resume_failed',
          severity: 'warning',
          owner: 'system',
          summary: 'Resume chain stalled after a resolved decision.',
          next_action: 'inspect_resume_chain',
          age_seconds: 60,
          thread_id: 'thread-rollback',
        },
      ],
      incidentSummary: { total_open: 2, critical: 0, warning: 1, info: 1 },
      overview: { total: 2, active: 2, by_status: { awaiting_plan_confirmation: 1, waiting_for_authorization: 1 } },
      eventVersion: 0,
      totalCount: 2,
      getJobsPage: () => [
        {
          id: 'job-confirm',
          name: 'RNA-seq confirmation',
          status: 'awaiting_plan_confirmation',
          goal: 'Analyze apple RNA-seq data',
          thread_id: 'thread-confirm',
          created_at: '2026-03-28T10:00:00Z',
        },
        {
          id: 'job-rollback',
          name: 'Resume auth chain',
          status: 'waiting_for_authorization',
          goal: 'Resume RNA-seq pipeline',
          thread_id: 'thread-rollback',
          created_at: '2026-03-28T11:00:00Z',
        },
      ],
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

    const rollbackBadge = await screen.findByText('Needs Rollback Review')
    const firstCard = rollbackBadge.closest('.rounded-lg')
    expect(firstCard?.textContent || '').toContain('Resume auth chain')
    expect(screen.getAllByRole('button', { name: 'Open Execution Review' }).length).toBeGreaterThan(0)
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

  it('humanizes watchdog auto retry resume events in task details', async () => {
    mockUseProjectTaskFeed.mockReturnValue({
      jobs: [
        {
          id: 'job-resume-auto',
          name: 'Resume auth chain',
          status: 'waiting_for_authorization',
          goal: 'Resume RNA-seq pipeline',
          thread_id: 'thread-resume-auto',
          created_at: '2026-03-28T10:00:00Z',
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
          id: 'job-resume-auto',
          name: 'Resume auth chain',
          status: 'waiting_for_authorization',
          goal: 'Resume RNA-seq pipeline',
          thread_id: 'thread-resume-auto',
          created_at: '2026-03-28T10:00:00Z',
        },
      ],
      getPageHasMore: () => false,
      patchJob: vi.fn(),
      locateJobPage: vi.fn().mockResolvedValue(1),
      refreshJobPage: vi.fn().mockResolvedValue([]),
      refreshJobs: vi.fn().mockResolvedValue([]),
      refreshAttentionSummary: vi.fn().mockResolvedValue(undefined),
      refreshIncidents: vi.fn().mockResolvedValue(undefined),
      refreshAll: vi.fn().mockResolvedValue(undefined),
    })

    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = String(input)
        if (url.includes('/api/jobs/job-resume-auto/bindings?detailed=1')) {
          return Promise.resolve({
            ok: true,
            json: async () => ({
              job_status: 'waiting_for_authorization',
              pending_interaction_type: null,
              pending_interaction_payload: null,
              runtime_diagnostics: [
                {
                  kind: 'resolved_pending_request',
                  request_type: 'authorization',
                  request_status: 'approved',
                },
              ],
              auto_recovery_events: [
                {
                  source: 'watchdog',
                  issue_kind: 'resume_failed',
                  safe_action: 'retry_resume_chain',
                  resulting_status: 'waiting_for_authorization',
                  pending_types: 'authorization',
                  line: '[watchdog] Auto-applied resume_failed via retry_resume_chain (status=waiting_for_authorization, pending=authorization).',
                  ts: '2026-03-28T10:11:00Z',
                },
              ],
              timeline: [],
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

    fireEvent.click(await screen.findByRole('button', { name: 'Logs' }))

    expect(await screen.findByText('Auto Recovery Audit')).toBeInTheDocument()
    expect(screen.getByText('Resolved decision did not resume -> Retry Resume Chain -> Awaiting Authorization · pending=authorization')).toBeInTheDocument()
  })
})
