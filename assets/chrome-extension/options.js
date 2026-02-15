const DEFAULT_PORT = 18792

function clampPort(value) {
  const n = Number.parseInt(String(value || ''), 10)
  if (!Number.isFinite(n)) return DEFAULT_PORT
  if (n <= 0 || n > 65535) return DEFAULT_PORT
  return n
}

function updateRelayUrl(port) {
  const el = document.getElementById('relay-url')
  if (!el) return
  el.textContent = `http://127.0.0.1:${port}/`
}

function setStatus(kind, message) {
  const status = document.getElementById('status')
  if (!status) return
  status.dataset.kind = kind || ''
  status.textContent = message || ''
}

async function checkRelayReachable(port) {
  const url = `http://127.0.0.1:${port}/`
  const ctrl = new AbortController()
  const t = setTimeout(() => ctrl.abort(), 900)
  try {
    const res = await fetch(url, { method: 'HEAD', signal: ctrl.signal })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    setStatus('ok', `Relay reachable at ${url}`)
  } catch {
    setStatus(
      'error',
      `Relay not reachable at ${url}. Start OpenClaw's browser relay on this machine, then click the toolbar button again.`,
    )
  } finally {
    clearTimeout(t)
  }
}

async function load() {
  const stored = await chrome.storage.local.get([
    'relayPort',
    'profileId',
    'profileName',
    'autoConnect',
    'autoAttach',
  ])

  // Relay port
  const port = clampPort(stored.relayPort)
  document.getElementById('port').value = String(port)
  updateRelayUrl(port)
  await checkRelayReachable(port)

  // Profile
  document.getElementById('profileId').textContent = stored.profileId || '(not yet generated)'
  document.getElementById('profileName').value = stored.profileName || ''

  // Automation
  document.getElementById('autoConnect').checked = stored.autoConnect === true
  document.getElementById('autoAttach').checked = stored.autoAttach === true
}

async function savePort() {
  const input = document.getElementById('port')
  const port = clampPort(input.value)
  await chrome.storage.local.set({ relayPort: port })
  input.value = String(port)
  updateRelayUrl(port)
  await checkRelayReachable(port)
}

async function saveProfile() {
  const name = document.getElementById('profileName').value.trim()
  await chrome.storage.local.set({ profileName: name })
}

async function saveAutoConnect() {
  const checked = document.getElementById('autoConnect').checked
  await chrome.storage.local.set({ autoConnect: checked })
}

async function saveAutoAttach() {
  const checked = document.getElementById('autoAttach').checked
  await chrome.storage.local.set({ autoAttach: checked })
}

document.getElementById('save').addEventListener('click', () => void savePort())
document.getElementById('saveProfile').addEventListener('click', () => void saveProfile())
document.getElementById('autoConnect').addEventListener('change', () => void saveAutoConnect())
document.getElementById('autoAttach').addEventListener('change', () => void saveAutoAttach())
void load()
