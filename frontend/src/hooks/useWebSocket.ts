import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

export type WSMessage = Record<string, unknown>

const BASE_URL = `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws/chat`

export function useWebSocket(threadId: string | null = null) {
  const ws = useRef<WebSocket | null>(null)
  const [connected, setConnected] = useState(false)
  const listeners = useRef<Set<(msg: WSMessage) => void>>(new Set())
  const threadIdRef = useRef(threadId)
  const socketThreadIdRef = useRef<string | null>(threadId)

  // Build the WS URL with optional thread_id param
  const buildUrl = (tid: string | null) =>
    tid ? `${BASE_URL}?thread_id=${encodeURIComponent(tid)}` : BASE_URL

  const connect = useCallback((tid: string | null) => {
    ws.current?.close()
    socketThreadIdRef.current = tid
    const url = buildUrl(tid)
    const socket = new WebSocket(url)
    socket.onopen = () => setConnected(true)
    socket.onclose = () => {
      setConnected(false)
      // Auto-reconnect only if threadId hasn't changed (avoid reconnect loops during intentional close)
      setTimeout(() => {
        if (threadIdRef.current === tid) {
          connect(tid)
        }
      }, 2000)
    }
    socket.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data) as WSMessage
        if (msg.type === 'thread_bound') {
          const boundThread = msg.thread as { id?: unknown } | undefined
          if (typeof boundThread?.id === 'string' && boundThread.id.length > 0) {
            socketThreadIdRef.current = boundThread.id
          }
        }
        listeners.current.forEach((fn) => fn(msg))
      } catch {}
    }
    ws.current = socket
  }, [])

  // Connect / reconnect when threadId changes
  useEffect(() => {
    threadIdRef.current = threadId
    const socket = ws.current
    if (!socket) {
      connect(threadId)
      return () => { ws.current?.close() }
    }

    if (socketThreadIdRef.current !== threadId) {
      connect(threadId)
    }

    return () => { ws.current?.close() }
  }, [threadId, connect])

  const send = useCallback((msg: WSMessage) => {
    ws.current?.send(JSON.stringify(msg))
  }, [])

  const subscribe = useCallback((fn: (msg: WSMessage) => void) => {
    listeners.current.add(fn)
    return () => listeners.current.delete(fn)
  }, [])

  // Memoize return value so the object reference only changes when `connected` changes.
  // Without this, ChatPanel's useEffect([ws]) would re-run on every render.
  return useMemo(() => ({ connected, send, subscribe }), [connected, send, subscribe])
}
