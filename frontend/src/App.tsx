import { useState, useCallback, useEffect, useMemo, type Dispatch, type ReactNode, type SetStateAction } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { ChevronLeft, X } from 'lucide-react'
import Sidebar from './components/Sidebar'
import TopHeader from './components/TopHeader'
import DashboardHome from './components/DashboardHome'
import ChatPanel from './components/ChatPanel'
import DataBrowser, { type ResourceWorkspaceRequest } from './components/DataBrowser'
import TaskMonitor from './components/TaskMonitor'
import SettingsPage from './components/SettingsPage'
import SkillLibrary from './components/SkillLibrary'
import { ProjectTaskFeedProvider, useProjectTaskFeed } from './hooks/useProjectTaskFeed'
import { useWebSocket } from './hooks/useWebSocket'
import { useSystemHealth } from './hooks/useSystemHealth'
import { useLanguage } from './i18n/LanguageContext'
import type { TaskAttentionSignal } from './lib/taskAttention'

type ActivePanel = 'data' | 'tasks' | 'skills' | 'settings' | null
type ActiveView = 'home' | 'chat' | 'data' | 'tasks' | 'skills' | 'settings'

interface Project {
  id: string
  name: string
}

interface Thread {
  id: string
  title: string | null
  project_id: string | null
  project_name: string | null
  created_at: string
  updated_at: string
}

interface TaskTraySummaryItem {
  key: string
  label: string
  count: number
  tone: 'running' | 'attention' | 'warning'
}

const STORAGE_KEYS = {
  activeThread: 'tune_active_thread',
  projectId: 'tune_active_project_id',
  projectName: 'tune_active_project_name',
  activePanel: 'tune_active_panel',
  activeView: 'tune_active_view',
  autoSelectJobId: 'tune_auto_select_job_id',
} as const

function readStoredString(key: string): string | null {
  const value = localStorage.getItem(key)
  return value && value.trim() ? value : null
}

function readStoredView(): ActiveView {
  const value = readStoredString(STORAGE_KEYS.activeView)
  if (value === 'home' || value === 'chat' || value === 'data' || value === 'tasks' || value === 'skills' || value === 'settings') {
    return value
  }
  return 'home'
}

function readStoredPanel(): ActivePanel {
  const value = readStoredString(STORAGE_KEYS.activePanel)
  if (value === 'data' || value === 'tasks' || value === 'skills' || value === 'settings') {
    return value
  }
  return null
}

function readStoredThread(): Thread | null {
  const raw = localStorage.getItem(STORAGE_KEYS.activeThread)
  if (!raw) return null
  try {
    const parsed = JSON.parse(raw) as Partial<Thread>
    if (!parsed || typeof parsed.id !== 'string' || !parsed.id.trim()) return null
    return {
      id: parsed.id,
      title: parsed.title ?? null,
      project_id: parsed.project_id ?? null,
      project_name: parsed.project_name ?? null,
      created_at: parsed.created_at ?? '',
      updated_at: parsed.updated_at ?? '',
    }
  } catch {
    return null
  }
}

function RightPanel({ children, onClose }: { children: ReactNode; onClose: () => void }) {
  const { t } = useLanguage()
  return (
    <motion.div
      key="right-panel"
      initial={{ opacity: 0, x: 40 }}
      animate={{ opacity: 1, x: 0 }}
      exit={{ opacity: 0, x: 40 }}
      transition={{ duration: 0.2, ease: 'easeOut' }}
      className="flex h-full min-h-0 flex-col border-l border-border-subtle bg-surface-raised relative"
    >
      <button
        onClick={onClose}
        className="absolute top-2 right-2 z-10 p-1 rounded-lg text-text-muted hover:text-text-primary hover:bg-surface-hover transition-colors"
        title={t('close_panel_title')}
      >
        <X size={14} />
      </button>
      <div className="min-w-0 flex-1 overflow-auto">
        {children}
      </div>
    </motion.div>
  )
}

function TaskTrayHandle({
  signal,
  count,
  summaryItems,
  onOpen,
}: {
  signal: TaskAttentionSignal
  count: number
  summaryItems: TaskTraySummaryItem[]
  onOpen: () => void
}) {
  const { t } = useLanguage()
  const labelMap: Record<TaskAttentionSignal, string> = {
    idle: t('tasks_tray_idle'),
    running: t('tasks_tray_running'),
    warning: t('tasks_tray_warning'),
    attention: t('tasks_tray_attention'),
  }

  const activeDotClass: Record<TaskAttentionSignal, string> = {
    idle: 'bg-slate-500/60 shadow-none',
    running: 'bg-emerald-400 shadow-[0_0_14px_rgba(74,222,128,0.45)]',
    warning: 'bg-amber-400 shadow-[0_0_14px_rgba(251,191,36,0.4)]',
    attention: 'bg-rose-400 shadow-[0_0_14px_rgba(251,113,133,0.45)]',
  }

  return (
    <button
      type="button"
      onClick={onOpen}
      className="absolute right-3 top-1/2 z-20 flex -translate-y-1/2 items-center gap-3 rounded-2xl border border-border-subtle bg-surface-raised/95 px-3 py-3 text-left shadow-lg backdrop-blur"
      title={t('tasks_tray_open')}
    >
      <div className="flex flex-col items-center gap-1">
        <span className={`h-2.5 w-2.5 rounded-full ${signal === 'attention' ? activeDotClass.attention : 'bg-rose-900/40'}`} />
        <span className={`h-2.5 w-2.5 rounded-full ${signal === 'warning' ? activeDotClass.warning : 'bg-amber-900/40'}`} />
        <span className={`h-2.5 w-2.5 rounded-full ${signal === 'running' ? activeDotClass.running : signal === 'idle' ? activeDotClass.idle : 'bg-emerald-900/40'}`} />
      </div>
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-[11px] font-semibold uppercase tracking-[0.18em] text-text-muted">
            {t('tasks_heading_compact')}
          </span>
          {count > 0 && (
            <span className="rounded-full bg-accent px-1.5 py-0.5 text-[10px] font-semibold text-white">
              {count}
            </span>
          )}
        </div>
        <div className="mt-1 flex items-center gap-2">
          <span className="text-xs text-text-primary">{labelMap[signal]}</span>
          <ChevronLeft size={12} className="text-text-muted" />
        </div>
        {summaryItems.length > 0 && (
          <div className="mt-2 flex flex-wrap gap-1.5">
            {summaryItems.map((item) => (
              <span
                key={item.key}
                className={`rounded-full px-2 py-0.5 text-[10px] font-medium ${
                  item.tone === 'attention'
                    ? 'bg-rose-500/12 text-rose-200'
                    : item.tone === 'warning'
                      ? 'bg-amber-500/12 text-amber-200'
                      : 'bg-emerald-500/12 text-emerald-200'
                }`}
              >
                {item.label} {item.count}
              </span>
            ))}
          </div>
        )}
      </div>
    </button>
  )
}

interface AppBodyProps {
  activeThread: Thread | null
  activePanel: ActivePanel
  activeView: ActiveView
  autoSelectJobId: string | null
  health: ReturnType<typeof useSystemHealth>['health']
  lang: ReturnType<typeof useLanguage>['lang']
  projectId: string | null
  projectName: string | null
  projects: Project[]
  refreshHealth: () => void
  sidebarCollapsed: boolean
  threadDrawerOpen: boolean
  ws: ReturnType<typeof import('./hooks/useWebSocket').useWebSocket>
  setActivePanel: Dispatch<SetStateAction<ActivePanel>>
  setActiveView: Dispatch<SetStateAction<ActiveView>>
  setAutoSelectJobId: Dispatch<SetStateAction<string | null>>
  setLang: ReturnType<typeof useLanguage>['setLang']
  setSidebarCollapsed: Dispatch<SetStateAction<boolean>>
  setThreadDrawerOpen: Dispatch<SetStateAction<boolean>>
  handleAnalysisResult: () => void
  handleJobStarted: (jobId: string) => void
  handleOpenThread: (threadId: string | null, jobId?: string | null) => void
  handleOpenResourceWorkspace: (request: ResourceWorkspaceRequest) => void
  handleOpenSettingsDiagnostics: () => void
  handleProjectSelect: (id: string | null, name: string | null) => void
  handleThreadSelect: (thread: Thread | null) => void
  resourceWorkspaceRequest: ResourceWorkspaceRequest | null
  clearResourceWorkspaceRequest: () => void
}

function AppBody({
  activeThread,
  activePanel,
  activeView,
  autoSelectJobId,
  health,
  lang,
  projectId,
  projectName,
  projects,
  refreshHealth,
  sidebarCollapsed,
  threadDrawerOpen,
  ws,
  setActivePanel,
  setActiveView,
  setAutoSelectJobId,
  setLang,
  setSidebarCollapsed,
  setThreadDrawerOpen,
  handleAnalysisResult,
  handleJobStarted,
  handleOpenThread,
  handleOpenResourceWorkspace,
  handleOpenSettingsDiagnostics,
  handleProjectSelect,
  handleThreadSelect,
  resourceWorkspaceRequest,
  clearResourceWorkspaceRequest,
}: AppBodyProps) {
  const { t } = useLanguage()
  const { attentionSummary } = useProjectTaskFeed()

  const taskTraySummaryItems = useMemo<TaskTraySummaryItem[]>(() => {
    const items: TaskTraySummaryItem[] = []
    if ((attentionSummary?.counts.running ?? 0) > 0) {
      items.push({ key: 'running', label: t('tasks_tray_metric_running'), count: attentionSummary?.counts.running ?? 0, tone: 'running' })
    }
    if ((attentionSummary?.counts.authorization ?? 0) > 0) {
      items.push({ key: 'authorization', label: t('tasks_tray_metric_authorization'), count: attentionSummary?.counts.authorization ?? 0, tone: 'attention' })
    }
    if ((attentionSummary?.counts.repair ?? 0) > 0) {
      items.push({ key: 'repair', label: t('tasks_tray_metric_repair'), count: attentionSummary?.counts.repair ?? 0, tone: 'attention' })
    }
    const otherAttentionCount = (
      (attentionSummary?.counts.confirmation ?? 0)
      + (attentionSummary?.counts.clarification ?? 0)
      + (attentionSummary?.counts.rollback_review ?? 0)
    )
    if (otherAttentionCount > 0) {
      items.push({ key: 'attention', label: t('tasks_tray_metric_attention'), count: otherAttentionCount, tone: 'attention' })
    }
    if ((attentionSummary?.counts.warning ?? 0) > 0) {
      items.push({ key: 'warning', label: t('tasks_tray_metric_warning'), count: attentionSummary?.counts.warning ?? 0, tone: 'warning' })
    }
    return items.slice(0, 3)
  }, [attentionSummary, t])

  const isSplit = activeView === 'chat' && activePanel !== null

  return (
    <div className="flex h-screen overflow-hidden bg-surface-base text-text-primary">
      {/* Sidebar navigation */}
      <Sidebar
        activeView={activeView}
        collapsed={sidebarCollapsed}
        projects={projects}
        currentProjectId={projectId}
        currentProjectName={projectName}
        onProjectSelect={handleProjectSelect}
        onNavigate={(view) => {
          setActiveView(view)
          // close right panel when navigating away from chat
          if (view !== 'chat') setActivePanel(null)
        }}
        onToggleCollapse={() => setSidebarCollapsed((c) => !c)}
      />

      {/* Main content area */}
      <div className="flex min-h-0 flex-col flex-1 overflow-hidden">
        {/* Top header */}
        <TopHeader
          activeView={activeView}
          threadTitle={activeThread?.title ?? null}
          wsConnected={ws.connected}
          lang={lang}
          onSetLang={setLang}
          activeThreadId={activeThread?.id ?? null}
          activeProjectId={activeThread?.project_id ?? projectId}
          onSelectThread={handleThreadSelect}
          threadDrawerOpen={threadDrawerOpen}
          onThreadDrawerChange={setThreadDrawerOpen}
        />

        {/* View area */}
        <div className="flex min-h-0 flex-1 overflow-hidden">

          {/* Home view */}
          {activeView === 'home' && (
            <DashboardHome
              selectedProject={projectId ?? activeThread?.project_id ?? null}
              onNavigate={(view) => setActiveView(view as ActiveView)}
              health={health}
            />
          )}

          {/* Chat view — always mounted to preserve message state */}
          <motion.div
            layout
            className="relative h-full min-w-0 overflow-hidden"
            style={{ flex: 1, display: activeView === 'chat' ? undefined : 'none' }}
            transition={{ duration: 0.25, ease: 'easeOut' }}
          >
            <ChatPanel
              key={activeThread?.id ?? 'default'}
              ws={ws}
              projectId={projectId}
              projectName={projectName}
              lang={lang}
              threadTitle={activeThread?.title ?? null}
              llmReachable={health.llm_reachable}
              onJobStarted={handleJobStarted}
              onAnalysisResult={handleAnalysisResult}
              onNavigateToSettings={() => setActiveView('settings')}
              onNavigateToData={() => setActiveView('data')}
              onOpenThreadDrawer={() => setThreadDrawerOpen(true)}
              taskAttentionReminders={
                activeView === 'chat' && activePanel !== 'tasks'
                  ? (attentionSummary?.reminders ?? []).map((item) => ({
                    key: item.key,
                    jobId: item.job_id,
                    jobName: item.job_name,
                    threadId: item.thread_id,
                    incidentType: item.incident_type,
                    reason: item.reason,
                    ageSeconds: item.age_seconds,
                    summary: item.summary,
                    severity: item.severity,
                    owner: item.owner,
                    nextAction: item.next_action,
                    rollbackLevel: item.rollback_level,
                  }))
                  : []
              }
            />
            {activeView === 'chat' && activePanel !== 'tasks' && (
              <TaskTrayHandle
                signal={attentionSummary?.signal ?? 'idle'}
                count={attentionSummary?.count ?? 0}
                summaryItems={taskTraySummaryItems}
                onOpen={() => setActivePanel('tasks')}
              />
            )}
          </motion.div>

          {/* Right panel (split-view within chat) */}
          <AnimatePresence initial={false}>
            {isSplit && (
              <motion.div
                key="right-panel-container"
                initial={{ width: 0, opacity: 0 }}
                animate={{ width: '45%', opacity: 1 }}
                exit={{ width: 0, opacity: 0 }}
                transition={{ duration: 0.25, ease: 'easeOut' }}
                className="h-full min-h-0 shrink-0 overflow-hidden border-l border-border-subtle"
              >
                <AnimatePresence mode="wait">
                  <RightPanel key={activePanel} onClose={() => setActivePanel(null)}>
                    {activePanel === 'data' && (
                      <DataBrowser
                        selectedProject={projectId}
                        ws={ws}
                        onHealthChange={refreshHealth}
                        onProjectDeselect={() => handleProjectSelect(null, null)}
                        onProjectSelect={(id, name) => handleProjectSelect(id, name)}
                        workspaceRequest={resourceWorkspaceRequest}
                        onWorkspaceRequestHandled={clearResourceWorkspaceRequest}
                      />
                    )}
                    {activePanel === 'tasks' && (
                      <TaskMonitor
                        projectId={projectId}
                        autoSelectJobId={autoSelectJobId}
                        onAutoSelectConsumed={() => setAutoSelectJobId(null)}
                        onOpenThread={handleOpenThread}
                        onOpenResourceWorkspace={handleOpenResourceWorkspace}
                        onOpenSettingsDiagnostics={handleOpenSettingsDiagnostics}
                      />
                    )}
                    {activePanel === 'skills' && <SkillLibrary ws={ws} />}
                    {activePanel === 'settings' && <SettingsPage />}
                  </RightPanel>
                </AnimatePresence>
              </motion.div>
            )}
          </AnimatePresence>

          {/* Full-page Data view */}
          {activeView === 'data' && (
            <div className="min-h-0 flex-1 overflow-hidden">
              <DataBrowser
                selectedProject={projectId}
                ws={ws}
                onHealthChange={refreshHealth}
                onProjectDeselect={() => handleProjectSelect(null, null)}
                onProjectSelect={(id, name) => handleProjectSelect(id, name)}
                workspaceRequest={resourceWorkspaceRequest}
                onWorkspaceRequestHandled={clearResourceWorkspaceRequest}
              />
            </div>
          )}

          {/* Full-page Tasks view */}
          {activeView === 'tasks' && (
            <div className="min-h-0 flex-1 overflow-hidden">
              <TaskMonitor
                projectId={projectId}
                autoSelectJobId={autoSelectJobId}
                onAutoSelectConsumed={() => setAutoSelectJobId(null)}
                onOpenThread={handleOpenThread}
                onOpenResourceWorkspace={handleOpenResourceWorkspace}
                onOpenSettingsDiagnostics={handleOpenSettingsDiagnostics}
              />
            </div>
          )}

          {/* Full-page Skills view */}
          {activeView === 'skills' && (
            <div className="min-h-0 flex-1 overflow-hidden">
              <SkillLibrary ws={ws} />
            </div>
          )}

          {/* Full-page Settings view */}
          {activeView === 'settings' && (
            <div className="flex-1 overflow-y-auto">
              <SettingsPage />
            </div>
          )}

        </div>
      </div>
    </div>
  )
}

export default function App() {
  const [activeThread, setActiveThread] = useState<Thread | null>(readStoredThread)
  const [projectId, setProjectId] = useState<string | null>(() => readStoredString(STORAGE_KEYS.projectId))
  const [projectName, setProjectName] = useState<string | null>(() => readStoredString(STORAGE_KEYS.projectName))
  const [projects, setProjects] = useState<Project[]>([])
  const [activePanel, setActivePanel] = useState<ActivePanel>(readStoredPanel)
  const [activeView, setActiveView] = useState<ActiveView>(readStoredView)
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const [threadDrawerOpen, setThreadDrawerOpen] = useState(false)
  const [autoSelectJobId, setAutoSelectJobId] = useState<string | null>(() => readStoredString(STORAGE_KEYS.autoSelectJobId))
  const [resourceWorkspaceRequest, setResourceWorkspaceRequest] = useState<ResourceWorkspaceRequest | null>(null)
  const ws = useWebSocket(activeThread?.id ?? null)
  const { lang, setLang } = useLanguage()
  const { health, refresh: refreshHealth } = useSystemHealth()

  const fetchProjects = useCallback(async () => {
    const data = await fetch('/api/projects/').then((r) => r.json()).catch(() => [])
    setProjects(data)
  }, [])

  useEffect(() => { fetchProjects() }, [fetchProjects])

  useEffect(() => {
    if (!activeThread?.id) return
    fetch('/api/threads/')
      .then((r) => r.json())
      .then((threads: Thread[]) => {
        const matched = Array.isArray(threads)
          ? threads.find((thread) => thread.id === activeThread.id)
          : null
        if (!matched) {
          setActiveThread(null)
          return
        }
        setActiveThread((prev) => {
          if (!prev || prev.id !== matched.id) return prev
          return JSON.stringify(prev) === JSON.stringify(matched) ? prev : matched
        })
        setProjectId(matched.project_id ?? null)
        setProjectName(matched.project_name ?? null)
      })
      .catch(() => {})
  }, [activeThread?.id])

  useEffect(() => {
    if (activeThread) localStorage.setItem(STORAGE_KEYS.activeThread, JSON.stringify(activeThread))
    else localStorage.removeItem(STORAGE_KEYS.activeThread)
  }, [activeThread])

  useEffect(() => {
    if (projectId) localStorage.setItem(STORAGE_KEYS.projectId, projectId)
    else localStorage.removeItem(STORAGE_KEYS.projectId)
  }, [projectId])

  useEffect(() => {
    if (projectName) localStorage.setItem(STORAGE_KEYS.projectName, projectName)
    else localStorage.removeItem(STORAGE_KEYS.projectName)
  }, [projectName])

  useEffect(() => {
    localStorage.setItem(STORAGE_KEYS.activeView, activeView)
  }, [activeView])

  useEffect(() => {
    if (activePanel) localStorage.setItem(STORAGE_KEYS.activePanel, activePanel)
    else localStorage.removeItem(STORAGE_KEYS.activePanel)
  }, [activePanel])

  useEffect(() => {
    if (autoSelectJobId) localStorage.setItem(STORAGE_KEYS.autoSelectJobId, autoSelectJobId)
    else localStorage.removeItem(STORAGE_KEYS.autoSelectJobId)
  }, [autoSelectJobId])

  // Sync project context to backend whenever project or WS connection changes
  useEffect(() => {
    if (ws.connected && projectId) {
      ws.send({ type: 'set_project', project_id: projectId, language: lang })
    }
  }, [projectId, ws.connected, lang])

  const handleProjectSelect = (id: string | null, name: string | null) => {
    setProjectId(id)
    setProjectName(name)
  }

  const handleThreadSelect = useCallback((thread: Thread | null) => {
    setActiveThread(thread)
    if (thread) {
      setProjectId(thread.project_id ?? null)
      setProjectName(thread.project_name ?? null)
    }
  }, [])

  const handleOpenThread = useCallback((threadId: string | null, jobId?: string | null) => {
    setActiveView('chat')
    setActivePanel('tasks')
    setAutoSelectJobId(jobId ?? null)
    if (!threadId) return
    fetch('/api/threads/')
      .then((r) => r.json())
      .then((threads: Thread[]) => {
        const matched = Array.isArray(threads)
          ? threads.find((thread) => thread.id === threadId)
          : null
        if (matched) {
          handleThreadSelect(matched)
        }
      })
      .catch(() => {})
  }, [handleThreadSelect])

  const handleJobStarted = useCallback((jobId: string) => {
    setActiveView('chat')
    setAutoSelectJobId(jobId)
  }, [])

  const handleAnalysisResult = useCallback(() => {
    setActivePanel((prev) => prev ?? 'data')
  }, [])

  const handleOpenResourceWorkspace = useCallback((request: ResourceWorkspaceRequest) => {
    if (activeView === 'chat') {
      setActivePanel('data')
    } else {
      setActiveView('data')
      setActivePanel(null)
    }
    setResourceWorkspaceRequest(request)
  }, [activeView])

  const handleOpenSettingsDiagnostics = useCallback(() => {
    if (activeView === 'chat') {
      setActivePanel('settings')
    } else {
      setActiveView('settings')
      setActivePanel(null)
    }
  }, [activeView])

  useEffect(() => {
    const unsub = ws.subscribe((msg) => {
      if (msg.type !== 'thread_bound') return
      const thread = msg.thread as Thread | undefined
      if (!thread?.id) return
      setActiveThread(thread)
      setProjectId(thread.project_id ?? null)
      setProjectName(thread.project_name ?? null)
    })
    return () => { unsub() }
  }, [ws.subscribe])

  return (
    <ProjectTaskFeedProvider projectId={projectId} ws={ws}>
      <AppBody
        activeThread={activeThread}
        activePanel={activePanel}
        activeView={activeView}
        autoSelectJobId={autoSelectJobId}
        health={health}
        lang={lang}
        projectId={projectId}
        projectName={projectName}
        projects={projects}
        refreshHealth={refreshHealth}
        sidebarCollapsed={sidebarCollapsed}
        threadDrawerOpen={threadDrawerOpen}
        ws={ws}
        setActivePanel={setActivePanel}
        setActiveView={setActiveView}
        setAutoSelectJobId={setAutoSelectJobId}
        setLang={setLang}
        setSidebarCollapsed={setSidebarCollapsed}
        setThreadDrawerOpen={setThreadDrawerOpen}
        handleAnalysisResult={handleAnalysisResult}
        handleJobStarted={handleJobStarted}
        handleOpenThread={handleOpenThread}
        handleOpenResourceWorkspace={handleOpenResourceWorkspace}
        handleOpenSettingsDiagnostics={handleOpenSettingsDiagnostics}
        handleProjectSelect={handleProjectSelect}
        handleThreadSelect={handleThreadSelect}
        resourceWorkspaceRequest={resourceWorkspaceRequest}
        clearResourceWorkspaceRequest={() => setResourceWorkspaceRequest(null)}
      />
    </ProjectTaskFeedProvider>
  )
}
