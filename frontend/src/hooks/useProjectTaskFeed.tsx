import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import type { WSMessage } from './useWebSocket'

const RECENT_JOBS_LIMIT = 200
export const PROJECT_TASK_PAGE_SIZE = 20

export interface ProjectTaskJob {
  id: string
  name?: string | null
  status: string
  goal?: string | null
  output_dir?: string | null
  error_message?: string | null
  pending_interaction_type?: string | null
  thread_id?: string | null
  project_id?: string | null
  created_at?: string
  started_at?: string
  ended_at?: string
  peak_cpu_pct?: number
  peak_mem_mb?: number
}

export interface ProjectTaskIncidentSummary {
  total_open: number
  critical: number
  warning: number
  info: number
  by_type?: Record<string, number>
}

export interface ProjectTaskIncident {
  job_id: string
  job_name: string
  job_status: string
  project_id?: string | null
  thread_id?: string | null
  incident_type: string
  severity: 'info' | 'warning' | 'critical'
  owner: 'user' | 'system'
  summary: string
  detail?: string | null
  next_action: string
  age_seconds?: number | null
  pending_interaction_type?: string | null
  current_step_key?: string | null
  current_step_name?: string | null
}

export interface ProjectTaskOverview {
  total: number
  active: number
  by_status?: Record<string, number>
}

export interface ProjectTaskAttentionItem {
  key: string
  job_id: string
  job_name: string
  thread_id?: string | null
  incident_type: string
  reason: 'authorization' | 'repair' | 'confirmation' | 'clarification' | 'rollback_review' | 'warning'
  age_seconds: number
  summary: string
  severity: 'info' | 'warning' | 'critical'
  owner: 'user' | 'system'
  next_action?: string | null
  pending_interaction_type?: string | null
  rollback_level?: string | null
  rollback_target?: string | null
  rollback_reason?: string | null
  reconfirmation_required?: boolean | null
}

export interface ProjectTaskAttentionSummary {
  signal: 'idle' | 'running' | 'warning' | 'attention'
  count: number
  counts: {
    running: number
    authorization: number
    repair: number
    confirmation: number
    clarification: number
    rollback_review?: number
    warning: number
    needs_input: number
    needs_review: number
  }
  needs_input: ProjectTaskAttentionItem[]
  needs_review: ProjectTaskAttentionItem[]
  reminders: ProjectTaskAttentionItem[]
  auto_authorize_commands: boolean
}

interface ProjectTaskFeedContextValue {
  jobs: ProjectTaskJob[]
  incidents: ProjectTaskIncident[]
  incidentSummary: ProjectTaskIncidentSummary | null
  overview: ProjectTaskOverview | null
  attentionSummary: ProjectTaskAttentionSummary | null
  eventVersion: number
  totalCount: number
  getJobsPage: (pageNumber: number) => ProjectTaskJob[]
  getPageHasMore: (pageNumber: number) => boolean
  patchJob: (jobId: string, patch: Partial<ProjectTaskJob>) => void
  locateJobPage: (jobId: string) => Promise<number | null>
  refreshJobPage: (pageNumber: number, options?: { force?: boolean }) => Promise<ProjectTaskJob[]>
  refreshJobs: () => Promise<ProjectTaskJob[]>
  refreshAttentionSummary: () => Promise<void>
  refreshIncidents: () => Promise<void>
  refreshAll: () => Promise<void>
}

const ProjectTaskFeedContext = createContext<ProjectTaskFeedContextValue | null>(null)

interface ProjectTaskFeedProviderProps {
  children: ReactNode
  projectId: string | null
  ws: ReturnType<typeof import('./useWebSocket').useWebSocket>
  pollMs?: number
}

export function ProjectTaskFeedProvider({
  children,
  projectId,
  ws,
  pollMs = 60000,
}: ProjectTaskFeedProviderProps) {
  const [jobs, setJobs] = useState<ProjectTaskJob[]>([])
  const [jobPages, setJobPages] = useState<Record<number, ProjectTaskJob[]>>({})
  const [jobPageHasMore, setJobPageHasMore] = useState<Record<number, boolean>>({})
  const [incidents, setIncidents] = useState<ProjectTaskIncident[]>([])
  const [incidentSummary, setIncidentSummary] = useState<ProjectTaskIncidentSummary | null>(null)
  const [overview, setOverview] = useState<ProjectTaskOverview | null>(null)
  const [attentionSummary, setAttentionSummary] = useState<ProjectTaskAttentionSummary | null>(null)
  const [eventVersion, setEventVersion] = useState(0)
  const [totalCount, setTotalCount] = useState(0)
  const wasConnectedRef = useRef(false)
  const jobsRef = useRef<ProjectTaskJob[]>([])
  const jobPagesRef = useRef<Record<number, ProjectTaskJob[]>>({})
  const totalCountRef = useRef(0)

  const updateJobs = useCallback((updater: ProjectTaskJob[] | ((prev: ProjectTaskJob[]) => ProjectTaskJob[])) => {
    setJobs((prev) => {
      const next = typeof updater === 'function'
        ? (updater as (prev: ProjectTaskJob[]) => ProjectTaskJob[])(prev)
        : updater
      jobsRef.current = next
      return next
    })
  }, [])

  const updateJobPages = useCallback((updater: Record<number, ProjectTaskJob[]> | ((prev: Record<number, ProjectTaskJob[]>) => Record<number, ProjectTaskJob[]>)) => {
    setJobPages((prev) => {
      const next = typeof updater === 'function'
        ? (updater as (prev: Record<number, ProjectTaskJob[]>) => Record<number, ProjectTaskJob[]>)(prev)
        : updater
      jobPagesRef.current = next
      return next
    })
  }, [])

  const updateJobPageHasMore = useCallback((updater: Record<number, boolean> | ((prev: Record<number, boolean>) => Record<number, boolean>)) => {
    setJobPageHasMore((prev) => (
      typeof updater === 'function'
        ? (updater as (prev: Record<number, boolean>) => Record<number, boolean>)(prev)
        : updater
    ))
  }, [])

  const updateTotalCount = useCallback((next: number) => {
    totalCountRef.current = next
    setTotalCount(next)
  }, [])

  const fetchJobs = useCallback(async (limit: number, offset: number) => {
    const params = new URLSearchParams()
    if (projectId) params.set('project', projectId)
    params.set('limit', String(limit))
    params.set('offset', String(offset))
    const response = await fetch(`/api/jobs/?${params.toString()}`)
    const payload = await response.json().catch(() => [])
    const total = Number(response.headers.get('X-Total-Count') ?? '0')
    const hasMore = response.headers.get('X-Has-More') === '1'
    return {
      jobs: Array.isArray(payload) ? payload as ProjectTaskJob[] : [],
      total: Number.isFinite(total) ? total : 0,
      hasMore,
    }
  }, [projectId])

  const refreshJobs = useCallback(async () => {
    const result = await fetchJobs(RECENT_JOBS_LIMIT, 0)
    updateJobs(result.jobs)
    updateTotalCount(result.total)
    updateJobPages((prev) => ({
      ...prev,
      1: result.jobs.slice(0, PROJECT_TASK_PAGE_SIZE),
    }))
    updateJobPageHasMore((prev) => ({
      ...prev,
      1: result.total > PROJECT_TASK_PAGE_SIZE,
    }))
    return result.jobs
  }, [fetchJobs, updateJobPageHasMore, updateJobPages, updateJobs, updateTotalCount])

  const refreshJobPage = useCallback(async (pageNumber: number, options?: { force?: boolean }) => {
    if (pageNumber < 1) return []
    if (!options?.force) {
      const cached = jobPagesRef.current[pageNumber]
      if (cached) return cached
    }

    const result = await fetchJobs(PROJECT_TASK_PAGE_SIZE, (pageNumber - 1) * PROJECT_TASK_PAGE_SIZE)
    updateJobPages((prev) => ({
      ...prev,
      [pageNumber]: result.jobs,
    }))
    updateJobPageHasMore((prev) => ({
      ...prev,
      [pageNumber]: result.hasMore,
    }))
    updateTotalCount(result.total)

    if (pageNumber === 1) {
      updateJobs((prev) => {
        if (prev.length === 0) return result.jobs
        const tail = prev.slice(PROJECT_TASK_PAGE_SIZE)
        return [...result.jobs, ...tail].slice(0, RECENT_JOBS_LIMIT)
      })
    }

    return result.jobs
  }, [fetchJobs, updateJobPageHasMore, updateJobPages, updateJobs, updateTotalCount])

  const refreshAttentionSummary = useCallback(async () => {
    const params = new URLSearchParams()
    if (projectId) params.set('project', projectId)
    const response = await fetch(`/api/jobs/attention-summary?${params.toString()}`)
    const payload = await response.json().catch(() => ({}))
    setAttentionSummary((payload as ProjectTaskAttentionSummary | undefined) ?? null)
    setIncidentSummary((payload.summary as ProjectTaskIncidentSummary | undefined) ?? { total_open: 0, critical: 0, warning: 0, info: 0 })
    setIncidents(Array.isArray(payload.incidents) ? payload.incidents as ProjectTaskIncident[] : [])
    setOverview({
      total: typeof payload.overview?.total === 'number' ? payload.overview.total : 0,
      active: typeof payload.overview?.active === 'number' ? payload.overview.active : 0,
      by_status: typeof payload.overview?.by_status === 'object' && payload.overview?.by_status
        ? payload.overview.by_status as Record<string, number>
        : {},
    })
  }, [projectId])

  const refreshIncidents = useCallback(async () => {
    await refreshAttentionSummary()
  }, [refreshAttentionSummary])

  const refreshAll = useCallback(async () => {
    try {
      await Promise.all([refreshJobs(), refreshAttentionSummary()])
    } catch {
      updateJobs([])
      updateJobPages({})
      updateJobPageHasMore({})
      updateTotalCount(0)
      setIncidents([])
      setIncidentSummary(null)
      setOverview(null)
      setAttentionSummary(null)
    }
  }, [refreshAttentionSummary, refreshJobs, updateJobPageHasMore, updateJobPages, updateJobs, updateTotalCount])

  const patchJob = useCallback((jobId: string, patch: Partial<ProjectTaskJob>) => {
    updateJobs((prev) => prev.map((job) => (job.id === jobId ? { ...job, ...patch } : job)))
    updateJobPages((prev) => {
      let changed = false
      const next = Object.fromEntries(
        Object.entries(prev).map(([pageKey, pageJobs]) => [
          Number(pageKey),
          pageJobs.map((job) => {
            if (job.id !== jobId) return job
            changed = true
            return { ...job, ...patch }
          }),
        ]),
      ) as Record<number, ProjectTaskJob[]>
      return changed ? next : prev
    })
  }, [updateJobPages, updateJobs])

  const locateJobPage = useCallback(async (jobId: string) => {
    const cachedEntry = Object.entries(jobPagesRef.current).find(([, pageJobs]) => (
      pageJobs.some((job) => job.id === jobId)
    ))
    if (cachedEntry) return Number(cachedEntry[0])

    const recentIndex = jobsRef.current.findIndex((job) => job.id === jobId)
    if (recentIndex >= 0) {
      const pageNumber = Math.floor(recentIndex / PROJECT_TASK_PAGE_SIZE) + 1
      await refreshJobPage(pageNumber, { force: true })
      return pageNumber
    }

    if (totalCountRef.current === 0) {
      await refreshJobs()
    }

    const totalPages = Math.max(1, Math.ceil(totalCountRef.current / PROJECT_TASK_PAGE_SIZE))
    for (let pageNumber = 1; pageNumber <= totalPages; pageNumber += 1) {
      const pageJobs = pageNumber === 1 && jobsRef.current.length > 0
        ? jobsRef.current.slice(0, PROJECT_TASK_PAGE_SIZE)
        : await refreshJobPage(pageNumber, { force: !jobPagesRef.current[pageNumber] })
      if (pageJobs.some((job) => job.id === jobId)) {
        return pageNumber
      }
    }

    return null
  }, [refreshJobPage, refreshJobs])

  const getJobsPage = useCallback((pageNumber: number) => (
    jobPages[pageNumber] ?? []
  ), [jobPages])

  const getPageHasMore = useCallback((pageNumber: number) => (
    jobPageHasMore[pageNumber] ?? false
  ), [jobPageHasMore])

  useEffect(() => {
    updateJobs([])
    updateJobPages({})
    updateJobPageHasMore({})
    setIncidents([])
    setIncidentSummary(null)
    setOverview(null)
    setAttentionSummary(null)
    updateTotalCount(0)
  }, [projectId, updateJobPageHasMore, updateJobPages, updateJobs, updateTotalCount])

  useEffect(() => {
    void refreshAll()
    const interval = window.setInterval(() => {
      void refreshAll()
    }, pollMs)
    return () => window.clearInterval(interval)
  }, [pollMs, refreshAll])

  useEffect(() => {
    const wasConnected = wasConnectedRef.current
    wasConnectedRef.current = ws.connected
    if (ws.connected && !wasConnected) {
      void refreshAll()
    }
  }, [refreshAll, ws.connected])

  useEffect(() => {
    const unsub = ws.subscribe((msg: WSMessage) => {
      if (msg.type !== 'project_task_event') return
      const job = msg.job as ProjectTaskJob | undefined
      if (!job?.id) return
      if (projectId && job.project_id && job.project_id !== projectId) return

      if (msg.deleted === true) {
        updateJobs((prev) => prev.filter((item) => item.id !== job.id))
        updateJobPages((prev) => Object.fromEntries(
          Object.entries(prev).map(([pageKey, pageJobs]) => [
            Number(pageKey),
            pageJobs.filter((item) => item.id !== job.id),
          ]),
        ) as Record<number, ProjectTaskJob[]>)
      } else if (jobsRef.current.some((item) => item.id === job.id)) {
        patchJob(job.id, job)
      } else {
        void refreshJobs()
      }

      void refreshAttentionSummary()
      setEventVersion((prev) => prev + 1)
    })
    return () => { unsub() }
  }, [patchJob, projectId, refreshAttentionSummary, refreshJobs, updateJobPages, updateJobs, ws])

  const value = useMemo<ProjectTaskFeedContextValue>(() => ({
    jobs,
    incidents,
    incidentSummary,
    overview,
    attentionSummary,
    eventVersion,
    totalCount,
    getJobsPage,
    getPageHasMore,
    patchJob,
    locateJobPage,
    refreshJobPage,
    refreshJobs,
    refreshAttentionSummary,
    refreshIncidents,
    refreshAll,
  }), [
    attentionSummary,
    eventVersion,
    getJobsPage,
    getPageHasMore,
    incidentSummary,
    incidents,
    jobs,
    overview,
    locateJobPage,
    patchJob,
    refreshAll,
    refreshAttentionSummary,
    refreshIncidents,
    refreshJobPage,
    refreshJobs,
    totalCount,
  ])

  return (
    <ProjectTaskFeedContext.Provider value={value}>
      {children}
    </ProjectTaskFeedContext.Provider>
  )
}

export function useProjectTaskFeed() {
  const context = useContext(ProjectTaskFeedContext)
  if (!context) {
    throw new Error('useProjectTaskFeed must be used within ProjectTaskFeedProvider')
  }
  return context
}
