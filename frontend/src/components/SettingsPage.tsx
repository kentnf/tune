import { useEffect, useState } from 'react'
import DirectoryPicker from './settings/DirectoryPicker'
import { useLanguage } from '../i18n/LanguageContext'
import type { SystemHealth } from '../hooks/useSystemHealth'
import ApiConfigList from './settings/ApiConfigList'

interface Config {
  workspace_root: string | null
  data_dir: string
  analysis_dir: string
  pixi_path: string
  active_llm_config_id: string | null
  auto_authorize_commands: boolean
  developer_show_llm_io_in_chat: boolean
}

interface UserProfile {
  research_domain: string | null
  experience_level: string | null
  language_preference: string | null
  communication_style: string | null
  notes: string | null
}

type SaveState = 'idle' | 'saving' | 'saved' | 'error'

function SettingSection({
  title,
  description,
  children,
  saveState,
  saveError,
  onSave,
}: {
  title: string
  description: string
  children: React.ReactNode
  saveState: SaveState
  saveError?: string | null
  onSave: () => void
}) {
  const { t } = useLanguage()
  return (
    <div className="bg-surface-raised rounded-xl p-6">
      <div className="mb-5">
        <h2 className="text-sm font-semibold text-text-primary">{title}</h2>
        <p className="text-xs text-text-muted mt-1">{description}</p>
      </div>
      <div className="space-y-4">
        {children}
      </div>
      <div className="mt-5 flex items-center gap-3">
        <button
          onClick={onSave}
          disabled={saveState === 'saving'}
          className="px-4 py-1.5 bg-accent hover:bg-accent-hover text-white rounded-lg text-xs font-medium disabled:opacity-50 transition-colors"
        >
          {saveState === 'saving' ? t('settings_saving') : t('settings_save')}
        </button>
        {saveState === 'saved' && (
          <span className="text-xs text-emerald-400">✓ {t('settings_saved')}</span>
        )}
        {saveState === 'error' && saveError && (
          <span className="text-xs text-red-400">{saveError}</span>
        )}
      </div>
    </div>
  )
}

function FieldRow({
  label,
  children,
}: {
  label: string
  children: React.ReactNode
}) {
  return (
    <div>
      <label className="text-xs text-text-muted mb-1 block">{label}</label>
      {children}
    </div>
  )
}

const inputCls = 'w-full bg-surface-overlay border border-border-subtle rounded-lg px-3 py-2 text-sm text-text-primary focus:outline-none focus:ring-1 focus:ring-accent placeholder-text-muted'
const selectCls = 'w-full bg-surface-overlay border border-border-subtle rounded-lg px-3 py-2 text-sm text-text-primary focus:outline-none focus:ring-1 focus:ring-accent'

function joinWorkspacePath(root: string, child: string): string {
  const trimmed = root.trim()
  if (!trimmed) return ''
  const normalized = trimmed.replace(/[\\/]+$/, '')
  return `${normalized}/${child}`
}

export default function SettingsPage() {
  const { t } = useLanguage()
  const [config, setConfig] = useState<Config | null>(null)
  const [workspaceRoot, setWorkspaceRoot] = useState('')
  const [dataDir, setDataDir] = useState('')
  const [analysisDir, setAnalysisDir] = useState('')
  const [pixiPath, setPixiPath] = useState('')
  const [activeConfigId, setActiveConfigId] = useState<string | null>(null)
  const [autoAuthorizeCommands, setAutoAuthorizeCommands] = useState(false)
  const [developerShowLlmIoInChat, setDeveloperShowLlmIoInChat] = useState(false)

  const [workspaceState, setWorkspaceState] = useState<SaveState>('idle')
  const [profileState, setProfileState] = useState<SaveState>('idle')
  const [executionState, setExecutionState] = useState<SaveState>('idle')

  const [profile, setProfile] = useState<UserProfile>({
    research_domain: null, experience_level: null,
    language_preference: null, communication_style: null, notes: null,
  })
  const [llmHealth, setLlmHealth] = useState<SystemHealth | null>(null)

  useEffect(() => {
    fetch('/api/config/')
      .then((r) => r.json())
      .then((cfg: Config) => {
        setConfig(cfg)
        setWorkspaceRoot(cfg.workspace_root || '')
        setDataDir(cfg.data_dir)
        setAnalysisDir(cfg.analysis_dir)
        setPixiPath(cfg.pixi_path)
        setActiveConfigId(cfg.active_llm_config_id)
        setAutoAuthorizeCommands(Boolean(cfg.auto_authorize_commands))
        setDeveloperShowLlmIoInChat(Boolean(cfg.developer_show_llm_io_in_chat))
      })
      .catch(() => {})

    fetch('/api/profile')
      .then((r) => r.json())
      .then((p: UserProfile) => setProfile(p))
      .catch(() => {})

    fetch('/api/system/health')
      .then((r) => r.json())
      .then((h: SystemHealth) => setLlmHealth(h))
      .catch(() => {})
  }, [])

  const saveWorkspace = async () => {
    setWorkspaceState('saving')
    try {
      const body = workspaceRoot.trim()
        ? { workspace_root: workspaceRoot.trim() }
        : { data_dir: dataDir, analysis_dir: analysisDir }
      const res = await fetch('/api/config/', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      }).then((r) => r.json())
      if (res.ok) {
        setConfig((prev) => (prev ? {
          ...prev,
          workspace_root: workspaceRoot.trim() || null,
          data_dir: dataDir,
          analysis_dir: analysisDir,
        } : prev))
        setWorkspaceState('saved')
        setTimeout(() => setWorkspaceState('idle'), 2000)
      } else {
        setWorkspaceState('error')
      }
    } catch {
      setWorkspaceState('error')
    }
  }

  const saveProfile = async () => {
    setProfileState('saving')
    await fetch('/api/profile', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(profile),
    }).catch(() => {})
    setProfileState('saved')
    setTimeout(() => setProfileState('idle'), 2000)
  }

  const saveExecution = async () => {
    setExecutionState('saving')
    try {
      const res = await fetch('/api/config/', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          auto_authorize_commands: autoAuthorizeCommands,
          developer_show_llm_io_in_chat: developerShowLlmIoInChat,
        }),
      }).then((r) => r.json())
      if (res.ok) {
        setExecutionState('saved')
        setConfig((prev) => (
          prev
            ? {
                ...prev,
                auto_authorize_commands: autoAuthorizeCommands,
                developer_show_llm_io_in_chat: developerShowLlmIoInChat,
              }
            : prev
        ))
        setTimeout(() => setExecutionState('idle'), 2000)
      } else {
        setExecutionState('error')
      }
    } catch {
      setExecutionState('error')
    }
  }

  const saveDiagnostics = async () => {
    await fetch('/api/config/', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pixi_path: pixiPath }),
    }).catch(() => {})
  }
  const [diagState, setDiagState] = useState<SaveState>('idle')
  const handleSaveDiag = async () => {
    setDiagState('saving')
    await saveDiagnostics()
    setDiagState('saved')
    setTimeout(() => setDiagState('idle'), 2000)
  }

  if (!config) {
    return <div className="p-8 text-text-muted text-sm">{t('settings_loading')}</div>
  }

  return (
    <div className="max-w-2xl mx-auto p-8 space-y-5">
      <h1 className="text-xl font-semibold text-text-primary">{t('settings_title')}</h1>

      {/* Workspace */}
      <SettingSection
        title={t('settings_workspace')}
        description={t('settings_workspace_desc')}
        saveState={workspaceState}
        onSave={saveWorkspace}
      >
        <FieldRow label={t('settings_workspace_root')}>
          <DirectoryPicker
            label=""
            value={workspaceRoot}
            onChange={(path) => {
              setWorkspaceRoot(path)
              setDataDir(joinWorkspacePath(path, 'data'))
              setAnalysisDir(joinWorkspacePath(path, 'analysis'))
            }}
          />
        </FieldRow>
        <div className="rounded-lg border border-border-subtle bg-surface-overlay px-4 py-3">
          <div className="text-xs font-semibold uppercase tracking-wide text-text-muted">
            {t('settings_workspace_layout')}
          </div>
          <div className="mt-2 space-y-1 text-xs font-mono text-text-primary break-all">
            <div>{workspaceRoot || '…'}/.tune</div>
            <div>{workspaceRoot || '…'}/data</div>
            <div>{workspaceRoot || '…'}/analysis</div>
          </div>
          <div className="mt-2 text-xs text-text-muted">
            {t('settings_workspace_layout_hint')}
          </div>
        </div>
        <FieldRow label={t('settings_data_dir')}>
          <div className="bg-surface-overlay rounded p-3 text-sm text-white font-mono break-all">
            {dataDir || <span className="text-text-muted">—</span>}
          </div>
        </FieldRow>
        <FieldRow label={t('settings_analysis_dir')}>
          <div className="bg-surface-overlay rounded p-3 text-sm text-white font-mono break-all">
            {analysisDir || <span className="text-text-muted">—</span>}
          </div>
        </FieldRow>
      </SettingSection>

      {/* AI Models — multi-config list */}
      <div className="bg-surface-raised rounded-xl p-6">
        <div className="mb-5">
          <h2 className="text-sm font-semibold text-text-primary">{t('api_config_title')}</h2>
          <p className="text-xs text-text-muted mt-1">{t('api_config_desc')}</p>
        </div>
        <ApiConfigList
          activeConfigId={activeConfigId}
          onActiveChanged={setActiveConfigId}
        />
      </div>

      {/* Researcher Profile */}
      <SettingSection
        title={t('settings_profile')}
        description={t('settings_profile_desc')}
        saveState={profileState}
        onSave={saveProfile}
      >
        <FieldRow label={t('settings_research_domain')}>
          <input
            type="text"
            value={profile.research_domain ?? ''}
            onChange={(e) => setProfile((p) => ({ ...p, research_domain: e.target.value || null }))}
            placeholder={t('settings_research_domain_placeholder')}
            className={inputCls}
          />
        </FieldRow>
        <FieldRow label={t('settings_experience_level')}>
          <select
            value={profile.experience_level ?? ''}
            onChange={(e) => setProfile((p) => ({ ...p, experience_level: e.target.value || null }))}
            className={selectCls}
          >
            <option value="">{t('settings_not_set')}</option>
            <option value="novice">{t('settings_novice')}</option>
            <option value="intermediate">{t('settings_intermediate')}</option>
            <option value="expert">{t('settings_expert')}</option>
          </select>
        </FieldRow>
        <FieldRow label={t('settings_comm_style')}>
          <select
            value={profile.communication_style ?? ''}
            onChange={(e) => setProfile((p) => ({ ...p, communication_style: e.target.value || null }))}
            className={selectCls}
          >
            <option value="">{t('settings_not_set')}</option>
            <option value="brief">{t('settings_brief')}</option>
            <option value="detailed">{t('settings_detailed')}</option>
          </select>
        </FieldRow>
        <FieldRow label={t('settings_notes')}>
          <textarea
            value={profile.notes ?? ''}
            onChange={(e) => setProfile((p) => ({ ...p, notes: e.target.value || null }))}
            rows={3}
            placeholder={t('settings_notes_placeholder')}
            className={`${inputCls} resize-none`}
          />
        </FieldRow>
      </SettingSection>

      <SettingSection
        title={t('settings_execution')}
        description={t('settings_execution_desc')}
        saveState={executionState}
        onSave={saveExecution}
      >
        <label className="flex items-start gap-3 rounded-lg border border-border-subtle bg-surface-overlay px-4 py-3">
          <input
            type="checkbox"
            checked={autoAuthorizeCommands}
            onChange={(e) => setAutoAuthorizeCommands(e.target.checked)}
            className="mt-0.5 h-4 w-4 rounded border-border-subtle bg-surface-base text-accent focus:ring-accent"
          />
          <div>
            <div className="text-sm font-medium text-text-primary">
              {t('settings_auto_authorize_commands')}
            </div>
            <div className="mt-1 text-xs text-text-muted">
              {t('settings_auto_authorize_commands_hint')}
            </div>
          </div>
        </label>
        <label className="flex items-start gap-3 rounded-lg border border-border-subtle bg-surface-overlay px-4 py-3">
          <input
            type="checkbox"
            checked={developerShowLlmIoInChat}
            onChange={(e) => setDeveloperShowLlmIoInChat(e.target.checked)}
            className="mt-0.5 h-4 w-4 rounded border-border-subtle bg-surface-base text-accent focus:ring-accent"
          />
          <div>
            <div className="text-sm font-medium text-text-primary">
              {t('settings_developer_show_llm_io_in_chat')}
            </div>
            <div className="mt-1 text-xs text-text-muted">
              {t('settings_developer_show_llm_io_in_chat_hint')}
            </div>
          </div>
        </label>
      </SettingSection>

      {/* Diagnostics */}
      <SettingSection
        title={t('settings_diagnostics')}
        description={t('settings_diagnostics_desc')}
        saveState={diagState}
        onSave={handleSaveDiag}
      >
        <FieldRow label={t('settings_pixi_path')}>
          <input
            type="text"
            value={pixiPath}
            onChange={(e) => setPixiPath(e.target.value)}
            className={`${inputCls} font-mono`}
          />
        </FieldRow>
        <FieldRow label={t('settings_llm_connectivity')}>
          {llmHealth ? (
            <div className={`flex items-center gap-2 text-sm ${llmHealth.llm_reachable ? 'text-emerald-400' : 'text-red-400'}`}>
              <span className={`w-2 h-2 rounded-full ${llmHealth.llm_reachable ? 'bg-emerald-400' : 'bg-red-400'}`} />
              {llmHealth.llm_reachable ? t('settings_connected') : `${t('settings_unreachable')}${llmHealth.llm_error ? ` — ${llmHealth.llm_error}` : ''}`}
            </div>
          ) : (
            <span className="text-text-muted text-sm">{t('settings_checking')}</span>
          )}
        </FieldRow>
      </SettingSection>
    </div>
  )
}
