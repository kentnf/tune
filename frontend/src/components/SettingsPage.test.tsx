import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import SettingsPage from './SettingsPage'
import { LanguageProvider } from '../i18n/LanguageContext'

vi.mock('./settings/DirectoryPicker', () => ({
  default: ({ value }: { value: string }) => <div>DirectoryPicker:{value}</div>,
}))

vi.mock('./settings/ApiConfigList', () => ({
  default: () => <div>ApiConfigList</div>,
}))

function renderSettingsPage() {
  return render(
    <LanguageProvider>
      <SettingsPage />
    </LanguageProvider>,
  )
}

describe('SettingsPage execution preferences', () => {
  afterEach(() => {
    cleanup()
  })

  beforeEach(() => {
    localStorage.clear()
    vi.clearAllMocks()

    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input)
        if (url === '/api/config/' && (!init || !init.method || init.method === 'GET')) {
          return Promise.resolve({
            ok: true,
            json: async () => ({
              data_dir: '/tmp/data',
              analysis_dir: '/tmp/analysis',
              pixi_path: 'pixi',
              active_llm_config_id: null,
              auto_authorize_commands: false,
              developer_show_llm_io_in_chat: false,
            }),
          })
        }
        if (url === '/api/profile') {
          return Promise.resolve({
            ok: true,
            json: async () => ({
              research_domain: null,
              experience_level: null,
              language_preference: null,
              communication_style: null,
              notes: null,
            }),
          })
        }
        if (url === '/api/system/health') {
          return Promise.resolve({
            ok: true,
            json: async () => ({
              llm_reachable: true,
              llm_error: null,
            }),
          })
        }
        if (url === '/api/config/' && init?.method === 'PUT') {
          return Promise.resolve({
            ok: true,
            json: async () => ({ ok: true }),
          })
        }
        return Promise.resolve({
          ok: true,
          json: async () => ({}),
        })
      }),
    )
  })

  it('saves auto-authorize command preference through config api', async () => {
    renderSettingsPage()

    await screen.findByText('Execution')
    const autoAuthorizeCheckbox = screen.getByRole('checkbox', { name: /Auto-authorize analysis commands/i })
    const developerTraceCheckbox = screen.getByRole('checkbox', { name: /Show LLM input\/output in chat/i })
    fireEvent.click(autoAuthorizeCheckbox)
    fireEvent.click(developerTraceCheckbox)
    expect(autoAuthorizeCheckbox).toBeChecked()
    expect(developerTraceCheckbox).toBeChecked()

    const saveButtons = await screen.findAllByRole('button', { name: 'Save' })
    fireEvent.click(saveButtons[2])

    await waitFor(() => {
      expect(fetch).toHaveBeenCalledWith(
        '/api/config/',
        expect.objectContaining({
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            auto_authorize_commands: true,
            developer_show_llm_io_in_chat: true,
          }),
        }),
      )
    })
  })
})
