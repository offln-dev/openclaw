const DEFAULT_PORT = 18792

const BADGE = {
  on: { text: 'ON', color: '#FF5A36' },
  off: { text: '', color: '#000000' },
  connecting: { text: '…', color: '#F59E0B' },
  error: { text: '!', color: '#B91C1C' },
}

/** @type {WebSocket|null} */
let relayWs = null
/** @type {Promise<void>|null} */
let relayConnectPromise = null

let debuggerListenersInstalled = false

let nextSession = 1

/** @type {Map<number, {state:'connecting'|'connected', sessionId?:string, targetId?:string, attachOrder?:number}>} */
const tabs = new Map()
/** @type {Map<string, number>} */
const tabBySession = new Map()
/** @type {Map<string, number>} */
const childSessionToTab = new Map()

/** @type {Map<number, {resolve:(v:any)=>void, reject:(e:Error)=>void}>} */
const pending = new Map()

/** @type {{profileId: string, profileName: string} | null} */
let profileInfo = null

// Reconnect state — uses chrome.alarms API to survive MV3 service worker suspension
const RECONNECT_ALARM = 'relay-reconnect'
let reconnectAttempt = 0
const RECONNECT_MAX_ATTEMPTS = 20
const RECONNECT_BASE_MS = 5000
const RECONNECT_MAX_MS = 30000
/** @type {Set<number>} */
const previouslyAttachedTabs = new Set()
let reconnectEnabled = true

function nowStack() {
  try {
    return new Error().stack || ''
  } catch {
    return ''
  }
}

async function getRelayPort() {
  const stored = await chrome.storage.local.get(['relayPort'])
  const raw = stored.relayPort
  const n = Number.parseInt(String(raw || ''), 10)
  if (!Number.isFinite(n) || n <= 0 || n > 65535) return DEFAULT_PORT
  return n
}

/** Get or create profile identity */
async function getProfileInfo() {
  if (profileInfo) return profileInfo
  
  const stored = await chrome.storage.local.get(['profileId', 'profileName'])
  let { profileId, profileName } = stored
  
  // Generate profileId if not exists
  if (!profileId) {
    profileId = crypto.randomUUID()
    await chrome.storage.local.set({ profileId })
    console.log('Generated new profileId:', profileId)
  }
  
  // Default profileName to empty (user can set in options)
  profileName = profileName || ''
  
  profileInfo = { profileId, profileName }
  return profileInfo
}

/** Refresh profile info from storage (called when options change) */
async function refreshProfileInfo() {
  profileInfo = null
  return await getProfileInfo()
}

/** Check if auto-connect is enabled */
async function isAutoConnectEnabled() {
  const stored = await chrome.storage.local.get(['autoConnect'])
  return stored.autoConnect === true
}

/** Check if auto-attach is enabled */
async function isAutoAttachEnabled() {
  const stored = await chrome.storage.local.get(['autoAttach'])
  return stored.autoAttach === true
}

function setBadge(tabId, kind) {
  const cfg = BADGE[kind]
  void chrome.action.setBadgeText({ tabId, text: cfg.text })
  void chrome.action.setBadgeBackgroundColor({ tabId, color: cfg.color })
  void chrome.action.setBadgeTextColor({ tabId, color: '#FFFFFF' }).catch(() => {})
}

async function ensureRelayConnection() {
  if (relayWs && relayWs.readyState === WebSocket.OPEN) return
  if (relayConnectPromise) return await relayConnectPromise

  relayConnectPromise = (async () => {
    const port = await getRelayPort()
    const profile = await getProfileInfo()
    const httpBase = `http://127.0.0.1:${port}`
    const wsUrl = `ws://127.0.0.1:${port}/extension`

    // Fast preflight: is the relay server up?
    try {
      await fetch(`${httpBase}/`, { method: 'HEAD', signal: AbortSignal.timeout(2000) })
    } catch (err) {
      throw new Error(`Relay server not reachable at ${httpBase} (${String(err)})`)
    }

    const ws = new WebSocket(wsUrl)
    relayWs = ws

    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('WebSocket connect timeout')), 5000)
      ws.onopen = () => {
        clearTimeout(t)
        resolve()
      }
      ws.onerror = () => {
        clearTimeout(t)
        reject(new Error('WebSocket connect failed'))
      }
      ws.onclose = (ev) => {
        clearTimeout(t)
        reject(new Error(`WebSocket closed (${ev.code} ${ev.reason || 'no reason'})`))
      }
    })

    // Send profile registration immediately after connect
    ws.send(JSON.stringify({
      method: 'register',
      params: {
        profileId: profile.profileId,
        profileName: profile.profileName,
      }
    }))
    console.log('Registered with relay:', profile.profileId, profile.profileName || '(unnamed)')

    ws.onmessage = (event) => void onRelayMessage(String(event.data || ''))
    ws.onclose = () => onRelayClosed('closed')
    ws.onerror = () => onRelayClosed('error')

    if (!debuggerListenersInstalled) {
      debuggerListenersInstalled = true
      chrome.debugger.onEvent.addListener(onDebuggerEvent)
      chrome.debugger.onDetach.addListener(onDebuggerDetach)
    }
  })()

  try {
    await relayConnectPromise
  } finally {
    relayConnectPromise = null
  }
}

function onRelayClosed(reason) {
  relayWs = null
  for (const [id, p] of pending.entries()) {
    pending.delete(id)
    p.reject(new Error(`Relay disconnected (${reason})`))
  }

  // Remember which tabs were attached before disconnect for re-attach on reconnect
  for (const tabId of tabs.keys()) {
    previouslyAttachedTabs.add(tabId)
    void chrome.debugger.detach({ tabId }).catch(() => {})
    setBadge(tabId, 'connecting')
    void chrome.action.setTitle({
      tabId,
      title: 'OpenClaw Browser Relay: disconnected (reconnecting…)',
    })
  }
  tabs.clear()
  tabBySession.clear()
  childSessionToTab.clear()

  // Start reconnect loop
  scheduleReconnect()
}

function scheduleReconnect() {
  if (!reconnectEnabled) return
  if (reconnectAttempt >= RECONNECT_MAX_ATTEMPTS) {
    console.warn('Max reconnect attempts reached, giving up')
    for (const tabId of previouslyAttachedTabs) {
      setBadge(tabId, 'error')
      void chrome.action.setTitle({
        tabId,
        title: 'OpenClaw Browser Relay: failed to reconnect (click to retry)',
      })
    }
    previouslyAttachedTabs.clear()
    reconnectAttempt = 0
    return
  }

  const delayMs = Math.min(RECONNECT_BASE_MS * Math.pow(2, reconnectAttempt), RECONNECT_MAX_MS)
  reconnectAttempt++
  console.log(`Reconnect attempt ${reconnectAttempt}/${RECONNECT_MAX_ATTEMPTS} in ${delayMs}ms`)

  // Use chrome.alarms — survives MV3 service worker suspension
  // For sub-minute delays, we still use setTimeout but also set a 1-min alarm as backup
  void chrome.alarms.clear(RECONNECT_ALARM)
  if (delayMs < 60000) {
    const fallbackTimer = setTimeout(() => void doReconnect(), delayMs)
    scheduleReconnect._fallbackTimer = fallbackTimer
    scheduleReconnect._fallbackFired = false
    chrome.alarms.create(RECONNECT_ALARM, { delayInMinutes: delayMs / 60000 })
  } else {
    chrome.alarms.create(RECONNECT_ALARM, { delayInMinutes: delayMs / 60000 })
  }
}

// Shared reconnect logic
async function doReconnect() {
  // Prevent double-fire from both setTimeout and alarm
  if (scheduleReconnect._fallbackFired) return
  scheduleReconnect._fallbackFired = true
  if (scheduleReconnect._fallbackTimer) {
    clearTimeout(scheduleReconnect._fallbackTimer)
    scheduleReconnect._fallbackTimer = null
  }
  void chrome.alarms.clear(RECONNECT_ALARM)

  try {
    await ensureRelayConnection()
    console.log('Reconnected to relay successfully')
    reconnectAttempt = 0

    // Re-attach previously attached tabs
    const tabsToReattach = [...previouslyAttachedTabs]
    previouslyAttachedTabs.clear()
    for (const tabId of tabsToReattach) {
      try {
        const tab = await chrome.tabs.get(tabId)
        if (!tab || !tab.url || tab.url.startsWith('chrome://') || tab.url.startsWith('chrome-extension://')) {
          continue
        }
        if (tabs.has(tabId)) continue
        tabs.set(tabId, { state: 'connecting' })
        setBadge(tabId, 'connecting')
        await attachTab(tabId)
      } catch (err) {
        tabs.delete(tabId)
        setBadge(tabId, 'error')
        console.warn('Failed to re-attach tab', tabId, err instanceof Error ? err.message : String(err))
      }
    }
  } catch (err) {
    console.warn('Reconnect failed:', err instanceof Error ? err.message : String(err))
    scheduleReconnect._fallbackFired = false // allow next attempt
    scheduleReconnect()
  }
}

// Alarm handler — wakes service worker and triggers reconnect
chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === RECONNECT_ALARM) {
    void doReconnect()
  }
})

function cancelReconnect() {
  reconnectEnabled = false
  void chrome.alarms.clear(RECONNECT_ALARM)
  if (scheduleReconnect._fallbackTimer) {
    clearTimeout(scheduleReconnect._fallbackTimer)
    scheduleReconnect._fallbackTimer = null
  }
  reconnectAttempt = 0
  previouslyAttachedTabs.clear()
}

function sendToRelay(payload) {
  const ws = relayWs
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    throw new Error('Relay not connected')
  }
  ws.send(JSON.stringify(payload))
}

async function maybeOpenHelpOnce() {
  try {
    const stored = await chrome.storage.local.get(['helpOnErrorShown'])
    if (stored.helpOnErrorShown === true) return
    await chrome.storage.local.set({ helpOnErrorShown: true })
    await chrome.runtime.openOptionsPage()
  } catch {
    // ignore
  }
}

function requestFromRelay(command) {
  const id = command.id
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject })
    try {
      sendToRelay(command)
    } catch (err) {
      pending.delete(id)
      reject(err instanceof Error ? err : new Error(String(err)))
    }
  })
}

async function onRelayMessage(text) {
  /** @type {any} */
  let msg
  try {
    msg = JSON.parse(text)
  } catch {
    return
  }

  if (msg && msg.method === 'ping') {
    try {
      sendToRelay({ method: 'pong' })
    } catch {
      // ignore
    }
    return
  }

  if (msg && msg.method === 'reload') {
    console.log('Reload requested by relay, reloading extension...')
    chrome.runtime.reload()
    return
  }

  if (msg && msg.method === 'shutdown') {
    console.log('Relay shutting down (planned), disabling reconnect')
    cancelReconnect()
    return
  }

  if (msg && typeof msg.id === 'number' && (msg.result !== undefined || msg.error !== undefined)) {
    const p = pending.get(msg.id)
    if (!p) return
    pending.delete(msg.id)
    if (msg.error) p.reject(new Error(String(msg.error)))
    else p.resolve(msg.result)
    return
  }

  if (msg && typeof msg.id === 'number' && msg.method === 'forwardCDPCommand') {
    try {
      const result = await handleForwardCdpCommand(msg)
      sendToRelay({ id: msg.id, result })
    } catch (err) {
      sendToRelay({ id: msg.id, error: err instanceof Error ? err.message : String(err) })
    }
    return
  }

  // Simple tab management commands (no debugger attachment required)
  if (msg && typeof msg.id === 'number') {
    try {
      const result = await handleTabManagementCommand(msg)
      if (result !== undefined) {
        sendToRelay({ id: msg.id, result })
      }
    } catch (err) {
      sendToRelay({ id: msg.id, error: err instanceof Error ? err.message : String(err) })
    }
  }
}

/** Handle simple tab management commands that don't require debugger */
async function handleTabManagementCommand(msg) {
  const method = msg.method
  const params = msg.params || {}

  if (method === 'listTabs') {
    const allTabs = await chrome.tabs.query({})
    return allTabs.map(tab => ({
      tabId: tab.id,
      windowId: tab.windowId,
      title: tab.title || '',
      url: tab.url || '',
      active: tab.active,
      pinned: tab.pinned,
      attached: tab.id ? tabs.has(tab.id) : false,
    }))
  }

  if (method === 'openTab') {
    const url = typeof params.url === 'string' ? params.url : 'about:blank'
    const active = params.active !== false
    const tab = await chrome.tabs.create({ url, active })
    return { tabId: tab.id, windowId: tab.windowId }
  }

  if (method === 'closeTab') {
    const tabId = params.tabId
    if (typeof tabId !== 'number') throw new Error('tabId required')
    await chrome.tabs.remove(tabId)
    return { success: true }
  }

  if (method === 'activateTab') {
    const tabId = params.tabId
    if (typeof tabId !== 'number') throw new Error('tabId required')
    const tab = await chrome.tabs.get(tabId)
    if (tab.windowId) {
      await chrome.windows.update(tab.windowId, { focused: true })
    }
    await chrome.tabs.update(tabId, { active: true })
    return { success: true }
  }

  if (method === 'navigateTab') {
    const tabId = params.tabId
    const url = params.url
    if (typeof tabId !== 'number') throw new Error('tabId required')
    if (typeof url !== 'string') throw new Error('url required')
    await chrome.tabs.update(tabId, { url })
    return { success: true }
  }

  if (method === 'getProfileInfo') {
    const profile = await getProfileInfo()
    return profile
  }

  if (method === 'listWindows') {
    const allWindows = await chrome.windows.getAll({ populate: true })
    return allWindows.map(win => ({
      windowId: win.id,
      focused: win.focused,
      state: win.state,
      type: win.type,
      width: win.width,
      height: win.height,
      top: win.top,
      left: win.left,
      tabs: (win.tabs || []).map(tab => ({
        tabId: tab.id,
        title: tab.title || '',
        url: tab.url || '',
        active: tab.active,
        pinned: tab.pinned,
        attached: tab.id ? tabs.has(tab.id) : false,
      })),
    }))
  }

  if (method === 'openWindow') {
    const url = typeof params.url === 'string' ? params.url : undefined
    const urls = Array.isArray(params.urls) ? params.urls : (url ? [url] : undefined)
    const focused = params.focused !== false
    const state = params.state
    const win = await chrome.windows.create({ url: urls, focused, state })
    return {
      windowId: win.id,
      tabs: (win.tabs || []).map(tab => ({ tabId: tab.id, url: tab.url }))
    }
  }

  if (method === 'closeWindow') {
    const windowId = params.windowId
    if (typeof windowId !== 'number') throw new Error('windowId required')
    await chrome.windows.remove(windowId)
    return { success: true }
  }

  if (method === 'focusWindow') {
    const windowId = params.windowId
    if (typeof windowId !== 'number') throw new Error('windowId required')
    await chrome.windows.update(windowId, { focused: true })
    return { success: true }
  }

  // Unknown method - return undefined to skip response
  return undefined
}

function getTabBySessionId(sessionId) {
  const direct = tabBySession.get(sessionId)
  if (direct) return { tabId: direct, kind: 'main' }
  const child = childSessionToTab.get(sessionId)
  if (child) return { tabId: child, kind: 'child' }
  return null
}

function getTabByTargetId(targetId) {
  for (const [tabId, tab] of tabs.entries()) {
    if (tab.targetId === targetId) return tabId
  }
  return null
}

async function attachTab(tabId, opts = {}) {
  const debuggee = { tabId }
  await chrome.debugger.attach(debuggee, '1.3')
  await chrome.debugger.sendCommand(debuggee, 'Page.enable').catch(() => {})

  const info = /** @type {any} */ (await chrome.debugger.sendCommand(debuggee, 'Target.getTargetInfo'))
  const targetInfo = info?.targetInfo
  const targetId = String(targetInfo?.targetId || '').trim()
  if (!targetId) {
    throw new Error('Target.getTargetInfo returned no targetId')
  }

  const sessionId = `cb-tab-${nextSession++}`
  const attachOrder = nextSession

  tabs.set(tabId, { state: 'connected', sessionId, targetId, attachOrder })
  tabBySession.set(sessionId, tabId)
  void chrome.action.setTitle({
    tabId,
    title: 'OpenClaw Browser Relay: attached (click to detach)',
  })

  if (!opts.skipAttachedEvent) {
    sendToRelay({
      method: 'forwardCDPEvent',
      params: {
        method: 'Target.attachedToTarget',
        params: {
          sessionId,
          targetInfo: { ...targetInfo, attached: true },
          waitingForDebugger: false,
        },
      },
    })
  }

  setBadge(tabId, 'on')
  return { sessionId, targetId }
}

async function detachTab(tabId, reason) {
  const tab = tabs.get(tabId)
  if (tab?.sessionId && tab?.targetId) {
    try {
      sendToRelay({
        method: 'forwardCDPEvent',
        params: {
          method: 'Target.detachedFromTarget',
          params: { sessionId: tab.sessionId, targetId: tab.targetId, reason },
        },
      })
    } catch {
      // ignore
    }
  }

  if (tab?.sessionId) tabBySession.delete(tab.sessionId)
  tabs.delete(tabId)

  for (const [childSessionId, parentTabId] of childSessionToTab.entries()) {
    if (parentTabId === tabId) childSessionToTab.delete(childSessionId)
  }

  try {
    await chrome.debugger.detach({ tabId })
  } catch {
    // ignore
  }

  setBadge(tabId, 'off')
  void chrome.action.setTitle({
    tabId,
    title: 'OpenClaw Browser Relay (click to attach/detach)',
  })
}

async function connectOrToggleForActiveTab() {
  const [active] = await chrome.tabs.query({ active: true, currentWindow: true })
  const tabId = active?.id
  if (!tabId) return

  const existing = tabs.get(tabId)
  if (existing?.state === 'connected') {
    await detachTab(tabId, 'toggle')
    return
  }

  // Re-enable reconnect on manual action (in case it was cancelled or exhausted)
  reconnectEnabled = true

  tabs.set(tabId, { state: 'connecting' })
  setBadge(tabId, 'connecting')
  void chrome.action.setTitle({
    tabId,
    title: 'OpenClaw Browser Relay: connecting to local relay…',
  })

  try {
    await ensureRelayConnection()
    await attachTab(tabId)
  } catch (err) {
    tabs.delete(tabId)
    setBadge(tabId, 'error')
    void chrome.action.setTitle({
      tabId,
      title: 'OpenClaw Browser Relay: relay not running (open options for setup)',
    })
    void maybeOpenHelpOnce()
    // Extra breadcrumbs in chrome://extensions service worker logs.
    const message = err instanceof Error ? err.message : String(err)
    console.warn('attach failed', message, nowStack())
  }
}

async function handleForwardCdpCommand(msg) {
  const method = String(msg?.params?.method || '').trim()
  const params = msg?.params?.params || undefined
  const sessionId = typeof msg?.params?.sessionId === 'string' ? msg.params.sessionId : undefined

  // Map command to tab
  const bySession = sessionId ? getTabBySessionId(sessionId) : null
  const targetId = typeof params?.targetId === 'string' ? params.targetId : undefined
  const tabId =
    bySession?.tabId ||
    (targetId ? getTabByTargetId(targetId) : null) ||
    (() => {
      // No sessionId: pick the first connected tab (stable-ish).
      for (const [id, tab] of tabs.entries()) {
        if (tab.state === 'connected') return id
      }
      return null
    })()

  if (!tabId) throw new Error(`No attached tab for method ${method}`)

  /** @type {chrome.debugger.DebuggerSession} */
  const debuggee = { tabId }

  if (method === 'Runtime.enable') {
    try {
      await chrome.debugger.sendCommand(debuggee, 'Runtime.disable')
      await new Promise((r) => setTimeout(r, 50))
    } catch {
      // ignore
    }
    return await chrome.debugger.sendCommand(debuggee, 'Runtime.enable', params)
  }

  if (method === 'Target.createTarget') {
    const url = typeof params?.url === 'string' ? params.url : 'about:blank'
    const tab = await chrome.tabs.create({ url, active: false })
    if (!tab.id) throw new Error('Failed to create tab')
    await new Promise((r) => setTimeout(r, 100))
    const attached = await attachTab(tab.id)
    return { targetId: attached.targetId }
  }

  if (method === 'Target.closeTarget') {
    const target = typeof params?.targetId === 'string' ? params.targetId : ''
    const toClose = target ? getTabByTargetId(target) : tabId
    if (!toClose) return { success: false }
    try {
      await chrome.tabs.remove(toClose)
    } catch {
      return { success: false }
    }
    return { success: true }
  }

  if (method === 'Target.activateTarget') {
    const target = typeof params?.targetId === 'string' ? params.targetId : ''
    const toActivate = target ? getTabByTargetId(target) : tabId
    if (!toActivate) return {}
    const tab = await chrome.tabs.get(toActivate).catch(() => null)
    if (!tab) return {}
    if (tab.windowId) {
      await chrome.windows.update(tab.windowId, { focused: true }).catch(() => {})
    }
    await chrome.tabs.update(toActivate, { active: true }).catch(() => {})
    return {}
  }

  const tabState = tabs.get(tabId)
  const mainSessionId = tabState?.sessionId
  const debuggerSession =
    sessionId && mainSessionId && sessionId !== mainSessionId
      ? { ...debuggee, sessionId }
      : debuggee

  return await chrome.debugger.sendCommand(debuggerSession, method, params)
}

function onDebuggerEvent(source, method, params) {
  const tabId = source.tabId
  if (!tabId) return
  const tab = tabs.get(tabId)
  if (!tab?.sessionId) return

  if (method === 'Target.attachedToTarget' && params?.sessionId) {
    childSessionToTab.set(String(params.sessionId), tabId)
  }

  if (method === 'Target.detachedFromTarget' && params?.sessionId) {
    childSessionToTab.delete(String(params.sessionId))
  }

  try {
    sendToRelay({
      method: 'forwardCDPEvent',
      params: {
        sessionId: source.sessionId || tab.sessionId,
        method,
        params,
      },
    })
  } catch {
    // ignore
  }
}

function onDebuggerDetach(source, reason) {
  const tabId = source.tabId
  if (!tabId) return
  if (!tabs.has(tabId)) return

  // Normal detach reasons — clean up fully
  if (reason === 'target_closed' || reason === 'canceled_by_user') {
    void detachTab(tabId, reason)
    return
  }

  // Unexpected detach (crash, navigation to restricted page, etc.)
  // Send detach event immediately so relay cleans up stale state,
  // then attempt reattach (which sends a fresh attachedToTarget if successful)
  console.log(`Unexpected debugger detach for tab ${tabId}: "${reason}", will attempt reattach`)

  // Clean up via normal detachTab (sends detachedFromTarget to relay immediately)
  void detachTab(tabId, reason)

  // Then attempt reattach after a short delay
  setBadge(tabId, 'connecting')
  setTimeout(async () => {
    try {
      const tab = await chrome.tabs.get(tabId)
      if (!tab || !tab.url || tab.url.startsWith('chrome://') || tab.url.startsWith('chrome-extension://')) {
        throw new Error('Tab not available for reattach')
      }

      tabs.set(tabId, { state: 'connecting' })
      await attachTab(tabId)
      console.log(`Successfully reattached tab ${tabId} after unexpected detach`)
    } catch (err) {
      console.warn(`Failed to reattach tab ${tabId}:`, err instanceof Error ? err.message : String(err))
      tabs.delete(tabId)
      setBadge(tabId, 'off')
      void chrome.action.setTitle({
        tabId,
        title: 'OpenClaw Browser Relay (click to attach/detach)',
      })
    }
  }, 500)
}

chrome.action.onClicked.addListener(() => void connectOrToggleForActiveTab())

chrome.runtime.onInstalled.addListener((details) => {
  // Generate profileId on install
  if (details.reason === 'install') {
    void getProfileInfo().then((info) => {
      console.log('Extension installed, profileId:', info.profileId)
    })
  }
  // Useful: first-time instructions.
  void chrome.runtime.openOptionsPage()
})

// Listen for storage changes (profile name updated in options, or automation settings)
chrome.storage.onChanged.addListener((changes, areaName) => {
  if (areaName === 'local' && (changes.profileName || changes.profileId)) {
    void refreshProfileInfo()
  }
  // If auto-connect or auto-attach settings change, trigger startup logic
  if (areaName === 'local' && (changes.autoConnect || changes.autoAttach)) {
    void maybeAutoConnectAndAttach()
  }
})

// ── Auto-attach functionality (configurable via options) ────────────────
// Only runs when autoAttach is enabled in chrome.storage.local.
// Default is false — user must opt in via options page.

async function autoAttachToTab(tabId) {
  // Check setting each time (can be toggled at runtime)
  if (!(await isAutoAttachEnabled())) return

  // Skip if already attached or connecting
  if (tabs.has(tabId)) return

  // Skip chrome:// and extension pages
  try {
    const tab = await chrome.tabs.get(tabId)
    if (!tab.url || tab.url.startsWith('chrome://') || tab.url.startsWith('chrome-extension://')) {
      return
    }
  } catch {
    return
  }

  tabs.set(tabId, { state: 'connecting' })
  setBadge(tabId, 'connecting')

  try {
    await ensureRelayConnection()
    await attachTab(tabId)
  } catch (err) {
    tabs.delete(tabId)
    setBadge(tabId, 'error')
    console.warn('auto-attach failed for tab', tabId, err instanceof Error ? err.message : String(err))
  }
}

async function autoAttachAllTabs() {
  if (!(await isAutoAttachEnabled())) return

  const allTabs = await chrome.tabs.query({})
  for (const tab of allTabs) {
    if (tab.id) {
      void autoAttachToTab(tab.id)
    }
  }
}

// Track navigation on already-attached tabs: send targetInfoChanged so relay metadata stays fresh
// Also auto-attach unattached tabs when navigation completes (if autoAttach is enabled)
chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  if (changeInfo.status !== 'complete' || !tab.url) return

  const tracked = tabs.get(tabId)
  if (tracked && tracked.state === 'connected' && tracked.sessionId) {
    // Tab is already attached — re-query targetInfo and notify relay of URL/title change
    const debuggee = { tabId }
    chrome.debugger.sendCommand(debuggee, 'Target.getTargetInfo')
      .then((info) => {
        const targetInfo = /** @type {any} */ (info)?.targetInfo
        if (!targetInfo) return
        try {
          sendToRelay({
            method: 'forwardCDPEvent',
            params: {
              sessionId: tracked.sessionId,
              method: 'Target.targetInfoChanged',
              params: { targetInfo: { ...targetInfo, attached: true } },
            },
          })
        } catch {
          // relay not connected, ignore
        }
      })
      .catch(() => {
        // debugger may have detached, ignore
      })
  } else {
    // Not attached yet — try auto-attach (checks setting internally)
    void autoAttachToTab(tabId)
  }
})

// Auto-attach when new tabs are created (if enabled)
chrome.tabs.onCreated.addListener((tab) => {
  if (tab.id) {
    // Small delay to let the tab initialize
    setTimeout(() => void autoAttachToTab(tab.id), 500)
  }
})

// Auto-connect and auto-attach on startup (if enabled)
chrome.runtime.onStartup.addListener(() => {
  void maybeAutoConnectAndAttach()
})

// Also on service worker activation (covers first install and reloads)
void maybeAutoConnectAndAttach()

// Connect to relay with retry, then auto-attach all tabs (only when autoConnect is enabled)
async function maybeAutoConnectAndAttach() {
  if (!(await isAutoConnectEnabled())) return

  // Try to connect immediately
  try {
    await ensureRelayConnection()
    await autoAttachAllTabs()
    return
  } catch {
    // Relay not up yet — use alarm-based retry
  }

  // Schedule retries via alarm (survives SW suspension)
  console.log('Relay not available on startup, scheduling connection retry')
  reconnectEnabled = true
  reconnectAttempt = 0
  // Collect all eligible tabs as "previously attached" so reconnect logic re-attaches them
  if (await isAutoAttachEnabled()) {
    try {
      const allTabs = await chrome.tabs.query({})
      for (const tab of allTabs) {
        if (tab.id && tab.url && !tab.url.startsWith('chrome://') && !tab.url.startsWith('chrome-extension://')) {
          previouslyAttachedTabs.add(tab.id)
        }
      }
    } catch {
      // ignore
    }
  }
  scheduleReconnect()
}
