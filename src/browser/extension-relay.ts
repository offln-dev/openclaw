import type { IncomingMessage } from "node:http";
import type { AddressInfo } from "node:net";
import type { Duplex } from "node:stream";
import { execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { copyFileSync, existsSync, readFileSync, readdirSync } from "node:fs";
import { createServer } from "node:http";
import { join, dirname } from "node:path";
import WebSocket, { WebSocketServer } from "ws";
import { isLoopbackAddress, isLoopbackHost } from "../gateway/net.js";
import { rawDataToString } from "../infra/ws.js";

type CdpCommand = {
  id: number;
  method: string;
  params?: unknown;
  sessionId?: string;
};

type CdpResponse = {
  id: number;
  result?: unknown;
  error?: { message: string };
  sessionId?: string;
};

type CdpEvent = {
  method: string;
  params?: unknown;
  sessionId?: string;
};

type ExtensionForwardCommandMessage = {
  id: number;
  method: "forwardCDPCommand";
  params: { method: string; params?: unknown; sessionId?: string };
};

type ExtensionResponseMessage = {
  id: number;
  result?: unknown;
  error?: string;
};

type ExtensionForwardEventMessage = {
  method: "forwardCDPEvent";
  params: { method: string; params?: unknown; sessionId?: string };
};

type ExtensionPingMessage = { method: "ping" };
type ExtensionPongMessage = { method: "pong" };

type ExtensionMessage =
  | ExtensionResponseMessage
  | ExtensionForwardEventMessage
  | ExtensionPongMessage;

type TargetInfo = {
  targetId: string;
  type?: string;
  title?: string;
  url?: string;
  attached?: boolean;
};

type AttachedToTargetEvent = {
  sessionId: string;
  targetInfo: TargetInfo;
  waitingForDebugger?: boolean;
};

type DetachedFromTargetEvent = {
  sessionId: string;
  targetId?: string;
};

type ConnectedTarget = {
  sessionId: string;
  targetId: string;
  targetInfo: TargetInfo;
};

const RELAY_AUTH_HEADER = "x-openclaw-relay-token";

function headerValue(value: string | string[] | undefined): string | undefined {
  if (!value) {
    return undefined;
  }
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}

function getHeader(req: IncomingMessage, name: string): string | undefined {
  return headerValue(req.headers[name.toLowerCase()]);
}

export type ChromeExtensionRelayServer = {
  host: string;
  port: number;
  baseUrl: string;
  cdpWsUrl: string;
  extensionConnected: () => boolean;
  stop: () => Promise<void>;
};

function parseBaseUrl(raw: string): {
  host: string;
  port: number;
  baseUrl: string;
} {
  const parsed = new URL(raw.trim().replace(/\/$/, ""));
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error(`extension relay cdpUrl must be http(s), got ${parsed.protocol}`);
  }
  const host = parsed.hostname;
  const port =
    parsed.port?.trim() !== "" ? Number(parsed.port) : parsed.protocol === "https:" ? 443 : 80;
  if (!Number.isFinite(port) || port <= 0 || port > 65535) {
    throw new Error(`extension relay cdpUrl has invalid port: ${parsed.port || "(empty)"}`);
  }
  return { host, port, baseUrl: parsed.toString().replace(/\/$/, "") };
}

function text(res: Duplex, status: number, bodyText: string) {
  const body = Buffer.from(bodyText);
  res.write(
    `HTTP/1.1 ${status} ${status === 200 ? "OK" : "ERR"}\r\n` +
      "Content-Type: text/plain; charset=utf-8\r\n" +
      `Content-Length: ${body.length}\r\n` +
      "Connection: close\r\n" +
      "\r\n",
  );
  res.write(body);
  res.end();
}

function rejectUpgrade(socket: Duplex, status: number, bodyText: string) {
  text(socket, status, bodyText);
  try {
    socket.destroy();
  } catch {
    // ignore
  }
}

const serversByPort = new Map<number, ChromeExtensionRelayServer>();
const relayAuthByPort = new Map<number, string>();

function relayAuthTokenForUrl(url: string): string | null {
  try {
    const parsed = new URL(url);
    if (!isLoopbackHost(parsed.hostname)) {
      return null;
    }
    const port =
      parsed.port?.trim() !== ""
        ? Number(parsed.port)
        : parsed.protocol === "https:" || parsed.protocol === "wss:"
          ? 443
          : 80;
    if (!Number.isFinite(port)) {
      return null;
    }
    return relayAuthByPort.get(port) ?? null;
  } catch {
    return null;
  }
}

export function getChromeExtensionRelayAuthHeaders(url: string): Record<string, string> {
  const token = relayAuthTokenForUrl(url);
  if (!token) {
    return {};
  }
  return { [RELAY_AUTH_HEADER]: token };
}

export async function ensureChromeExtensionRelayServer(opts: {
  cdpUrl: string;
}): Promise<ChromeExtensionRelayServer> {
  const info = parseBaseUrl(opts.cdpUrl);
  if (!isLoopbackHost(info.host)) {
    throw new Error(`extension relay requires loopback cdpUrl host (got ${info.host})`);
  }

  const existing = serversByPort.get(info.port);
  if (existing) {
    return existing;
  }

  let extensionWs: WebSocket | null = null;
  let extensionProfile: { profileId?: string; profileName?: string } | null = null;
  const cdpClients = new Set<WebSocket>();
  const connectedTargets = new Map<string, ConnectedTarget>();

  const pendingExtension = new Map<
    number,
    {
      resolve: (v: unknown) => void;
      reject: (e: Error) => void;
      timer: NodeJS.Timeout;
    }
  >();
  let nextExtensionId = 1;

  // ── Session aliasing ──────────────────────────────────────────────
  // When the extension reattaches the same tab (same targetId) with a
  // new Chrome-assigned sessionId, we keep the *original* sessionId
  // that Playwright already knows about.  All CDP traffic is silently
  // translated:
  //   Playwright → relay:  old sessionId → new real sessionId (to extension)
  //   Extension  → relay:  new real sessionId → old sessionId (to Playwright)
  //
  // playwrightToReal: sessionId Playwright uses → actual Chrome sessionId
  // realToPlaywright: actual Chrome sessionId   → sessionId Playwright uses
  const playwrightToReal = new Map<string, string>();
  const realToPlaywright = new Map<string, string>();

  /** Translate a sessionId from Playwright's view to the real Chrome sessionId */
  const toRealSession = (sid?: string): string | undefined => {
    if (!sid) return sid;
    return playwrightToReal.get(sid) ?? sid;
  };

  /** Translate a sessionId from Chrome/extension back to Playwright's view */
  const toPlaywrightSession = (sid?: string): string | undefined => {
    if (!sid) return sid;
    return realToPlaywright.get(sid) ?? sid;
  };

  const sendToExtension = async (payload: ExtensionForwardCommandMessage): Promise<unknown> => {
    const ws = extensionWs;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      throw new Error("Chrome extension not connected");
    }
    ws.send(JSON.stringify(payload));
    return await new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        pendingExtension.delete(payload.id);
        reject(new Error(`extension request timeout: ${payload.params.method}`));
      }, 30_000);
      pendingExtension.set(payload.id, { resolve, reject, timer });
    });
  };

  const broadcastToCdpClients = (evt: CdpEvent) => {
    const msg = JSON.stringify(evt);
    for (const ws of cdpClients) {
      if (ws.readyState !== WebSocket.OPEN) {
        continue;
      }
      ws.send(msg);
    }
  };

  const sendResponseToCdp = (ws: WebSocket, res: CdpResponse) => {
    if (ws.readyState !== WebSocket.OPEN) {
      return;
    }
    ws.send(JSON.stringify(res));
  };

  const ensureTargetEventsForClient = (ws: WebSocket, mode: "autoAttach" | "discover") => {
    for (const target of connectedTargets.values()) {
      if (mode === "autoAttach") {
        ws.send(
          JSON.stringify({
            method: "Target.attachedToTarget",
            params: {
              sessionId: target.sessionId,
              targetInfo: { ...target.targetInfo, attached: true },
              waitingForDebugger: false,
            },
          } satisfies CdpEvent),
        );
      } else {
        ws.send(
          JSON.stringify({
            method: "Target.targetCreated",
            params: { targetInfo: { ...target.targetInfo, attached: true } },
          } satisfies CdpEvent),
        );
      }
    }
  };

  const routeCdpCommand = async (cmd: CdpCommand): Promise<unknown> => {
    switch (cmd.method) {
      case "Browser.getVersion":
        return {
          protocolVersion: "1.3",
          product: "Chrome/OpenClaw-Extension-Relay",
          revision: "0",
          userAgent: "OpenClaw-Extension-Relay",
          jsVersion: "V8",
        };
      case "Browser.setDownloadBehavior":
        return {};
      case "Target.setAutoAttach":
      case "Target.setDiscoverTargets":
        return {};
      case "Target.getTargets":
        return {
          targetInfos: Array.from(connectedTargets.values()).map((t) => ({
            ...t.targetInfo,
            attached: true,
          })),
        };
      case "Target.getTargetInfo": {
        const params = (cmd.params ?? {}) as { targetId?: string };
        const targetId = typeof params.targetId === "string" ? params.targetId : undefined;
        if (targetId) {
          for (const t of connectedTargets.values()) {
            if (t.targetId === targetId) {
              return { targetInfo: t.targetInfo };
            }
          }
        }
        if (cmd.sessionId && connectedTargets.has(cmd.sessionId)) {
          const t = connectedTargets.get(cmd.sessionId);
          if (t) {
            return { targetInfo: t.targetInfo };
          }
        }
        const first = Array.from(connectedTargets.values())[0];
        return { targetInfo: first?.targetInfo };
      }
      case "Target.attachToTarget": {
        const params = (cmd.params ?? {}) as { targetId?: string };
        const targetId = typeof params.targetId === "string" ? params.targetId : undefined;
        if (!targetId) {
          throw new Error("targetId required");
        }
        for (const t of connectedTargets.values()) {
          if (t.targetId === targetId) {
            return { sessionId: t.sessionId };
          }
        }
        throw new Error("target not found");
      }
      case "Target.attachToBrowserTarget": {
        // Playwright calls this internally for newCDPSession(page).
        // Return the first available session — the extension relay is the "browser".
        const first = Array.from(connectedTargets.values())[0];
        if (first) return { sessionId: first.sessionId };
        throw new Error("No browser target available — no tabs attached to extension");
      }
      default: {
        const id = nextExtensionId++;
        return await sendToExtension({
          id,
          method: "forwardCDPCommand",
          params: {
            method: cmd.method,
            // Translate Playwright's sessionId to the real Chrome sessionId
            sessionId: toRealSession(cmd.sessionId),
            params: cmd.params,
          },
        });
      }
    }
  };

  const relayAuthToken = randomBytes(32).toString("base64url");

  const server = createServer((req, res) => {
    const url = new URL(req.url ?? "/", info.baseUrl);
    const path = url.pathname;

    if (path.startsWith("/json")) {
      const token = getHeader(req, RELAY_AUTH_HEADER);
      if (!token || token !== relayAuthToken) {
        res.writeHead(401);
        res.end("Unauthorized");
        return;
      }
    }

    if (req.method === "HEAD" && path === "/") {
      res.writeHead(200);
      res.end();
      return;
    }

    if (path === "/") {
      res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("OK");
      return;
    }

    if (path === "/extension/status") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ connected: Boolean(extensionWs) }));
      return;
    }

    if (path === "/extension/reload" && req.method === "POST") {
      if (!extensionWs || extensionWs.readyState !== 1) {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: "extension not connected" }));
        return;
      }

      // Find background.js and validate syntax before reloading.
      // Search: env override → Chrome profile unpacked extensions → container default
      const extDir = process.env.OPENCLAW_EXTENSION_DIR?.trim() || "";
      const candidatePaths = [extDir ? join(extDir, "background.js") : ""];
      // Discover unpacked extension paths from Chrome Default profile preferences
      try {
        const homes = [
          process.env.HOME || "/home/node",
          ...(() => {
            try {
              return readdirSync("/home")
                .map((d) => join("/home", d))
                .filter((d) => d !== (process.env.HOME || "/home/node"));
            } catch {
              return [];
            }
          })(),
        ];
        const prefsPath =
          homes
            .map((h) => join(h, ".config/google-chrome/Default/Preferences"))
            .find((p) => existsSync(p)) ??
          join(process.env.HOME || "/home/node", ".config/google-chrome/Default/Preferences");
        if (existsSync(prefsPath)) {
          const prefs = JSON.parse(readFileSync(prefsPath, "utf-8")) as {
            extensions?: {
              settings?: Record<string, { path?: string; manifest?: { name?: string } }>;
            };
          };
          const exts = prefs?.extensions?.settings ?? {};
          for (const ext of Object.values(exts)) {
            const p = ext?.path ?? "";
            const name = ext?.manifest?.name ?? "";
            if (
              (p.includes("openclaw") ||
                name.toLowerCase().includes("openclaw") ||
                name.toLowerCase().includes("browser relay")) &&
              existsSync(join(p, "background.js"))
            ) {
              candidatePaths.push(join(p, "background.js"));
            }
          }
        }
      } catch {
        // Chrome prefs not available, skip
      }
      candidatePaths.push("/app/assets/chrome-extension/background.js");
      const filteredPaths = candidatePaths.filter(Boolean);

      const bgPath = filteredPaths.find((p) => existsSync(p));
      if (bgPath) {
        // Validate JS syntax before reload
        try {
          execFileSync(process.execPath, ["--check", bgPath], {
            timeout: 5_000,
            stdio: "pipe",
          });
        } catch (syntaxErr) {
          const msg = syntaxErr instanceof Error ? syntaxErr.message : String(syntaxErr);
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(
            JSON.stringify({
              ok: false,
              error: `background.js syntax error — reload aborted`,
              details: msg.slice(0, 500),
              path: bgPath,
            }),
          );
          return;
        }
        // Backup to .last-good before reload
        const lastGood = join(dirname(bgPath), "background.last-good.js");
        try {
          copyFileSync(bgPath, lastGood);
        } catch {
          // non-fatal: best-effort backup
        }
      }

      try {
        const wasConnected = Boolean(extensionWs);
        extensionWs.send(JSON.stringify({ method: "reload" }));
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({ ok: true, validated: Boolean(bgPath), backed_up: Boolean(bgPath) }),
        );

        // Monitor reconnection: if extension doesn't reconnect within 30s,
        // log a warning (can't auto-restore without manual intervention)
        if (wasConnected && bgPath) {
          const lastGood = join(dirname(bgPath), "background.last-good.js");
          setTimeout(() => {
            if (!extensionWs || extensionWs.readyState !== 1) {
              // Extension didn't reconnect — restore backup
              if (existsSync(lastGood)) {
                try {
                  copyFileSync(lastGood, bgPath);
                  console.warn(
                    `[extension-relay] Extension failed to reconnect after reload. ` +
                      `Restored ${bgPath} from .last-good. ` +
                      `Manual reload in chrome://extensions may be needed.`,
                  );
                } catch {
                  console.warn(
                    `[extension-relay] Extension failed to reconnect and backup restore failed.`,
                  );
                }
              }
            }
          }, 30_000);
        }
      } catch (err) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: String(err) }));
      }
      return;
    }

    const hostHeader = req.headers.host?.trim() || `${info.host}:${info.port}`;
    const wsHost = `ws://${hostHeader}`;
    const cdpWsUrl = `${wsHost}/cdp`;

    if (
      (path === "/json/version" || path === "/json/version/") &&
      (req.method === "GET" || req.method === "PUT")
    ) {
      const payload: Record<string, unknown> = {
        Browser: "OpenClaw/extension-relay",
        "Protocol-Version": "1.3",
      };
      // Only advertise the WS URL if a real extension is connected.
      if (extensionWs) {
        payload.webSocketDebuggerUrl = cdpWsUrl;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(payload));
      return;
    }

    const listPaths = new Set(["/json", "/json/", "/json/list", "/json/list/"]);
    if (listPaths.has(path) && (req.method === "GET" || req.method === "PUT")) {
      const list = Array.from(connectedTargets.values()).map((t) => ({
        id: t.targetId,
        type: t.targetInfo.type ?? "page",
        title: t.targetInfo.title ?? "",
        description: t.targetInfo.title ?? "",
        url: t.targetInfo.url ?? "",
        webSocketDebuggerUrl: cdpWsUrl,
        devtoolsFrontendUrl: `/devtools/inspector.html?ws=${cdpWsUrl.replace("ws://", "")}`,
      }));
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(list));
      return;
    }

    const activateMatch = path.match(/^\/json\/activate\/(.+)$/);
    if (activateMatch && (req.method === "GET" || req.method === "PUT")) {
      const targetId = decodeURIComponent(activateMatch[1] ?? "").trim();
      if (!targetId) {
        res.writeHead(400);
        res.end("targetId required");
        return;
      }
      void (async () => {
        try {
          await sendToExtension({
            id: nextExtensionId++,
            method: "forwardCDPCommand",
            params: { method: "Target.activateTarget", params: { targetId } },
          });
        } catch {
          // ignore
        }
      })();
      res.writeHead(200);
      res.end("OK");
      return;
    }

    const closeMatch = path.match(/^\/json\/close\/(.+)$/);
    if (closeMatch && (req.method === "GET" || req.method === "PUT")) {
      const targetId = decodeURIComponent(closeMatch[1] ?? "").trim();
      if (!targetId) {
        res.writeHead(400);
        res.end("targetId required");
        return;
      }
      void (async () => {
        try {
          await sendToExtension({
            id: nextExtensionId++,
            method: "forwardCDPCommand",
            params: { method: "Target.closeTarget", params: { targetId } },
          });
        } catch {
          // ignore
        }
      })();
      res.writeHead(200);
      res.end("OK");
      return;
    }

    // /json/new?url=ENCODED_URL — create a new tab via extension
    if (
      (path === "/json/new" || path === "/json/new/") &&
      (req.method === "GET" || req.method === "PUT")
    ) {
      const targetUrl = url.searchParams.get("url") || "about:blank";
      if (!extensionWs || extensionWs.readyState !== WebSocket.OPEN) {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Extension not connected" }));
        return;
      }
      void (async () => {
        try {
          const result = (await sendToExtension({
            id: nextExtensionId++,
            method: "forwardCDPCommand",
            params: { method: "Target.createTarget", params: { url: targetUrl } },
          })) as { targetId?: string } | undefined;
          const targetId = result?.targetId ?? "";
          // Look up the target in connectedTargets (extension auto-attaches)
          // Give it a moment for the attachedToTarget event to arrive
          let target: ConnectedTarget | undefined;
          for (let i = 0; i < 10 && !target; i++) {
            for (const t of connectedTargets.values()) {
              if (t.targetId === targetId) {
                target = t;
                break;
              }
            }
            if (!target) await new Promise((r) => setTimeout(r, 200));
          }
          const payload = {
            id: targetId,
            type: target?.targetInfo?.type ?? "page",
            title: target?.targetInfo?.title ?? "",
            url: target?.targetInfo?.url ?? targetUrl,
            webSocketDebuggerUrl: cdpWsUrl,
          };
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify(payload));
        } catch (err) {
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(
            JSON.stringify({
              error: err instanceof Error ? err.message : String(err),
            }),
          );
        }
      })();
      return;
    }

    res.writeHead(404);
    res.end("not found");
  });

  const wssExtension = new WebSocketServer({ noServer: true });
  const wssCdp = new WebSocketServer({ noServer: true });

  server.on("upgrade", (req, socket, head) => {
    const url = new URL(req.url ?? "/", info.baseUrl);
    const pathname = url.pathname;
    const remote = req.socket.remoteAddress;

    if (!isLoopbackAddress(remote)) {
      rejectUpgrade(socket, 403, "Forbidden");
      return;
    }

    const origin = headerValue(req.headers.origin);
    if (origin && !origin.startsWith("chrome-extension://")) {
      rejectUpgrade(socket, 403, "Forbidden: invalid origin");
      return;
    }

    if (pathname === "/extension") {
      if (extensionWs) {
        rejectUpgrade(socket, 409, "Extension already connected");
        return;
      }
      wssExtension.handleUpgrade(req, socket, head, (ws) => {
        wssExtension.emit("connection", ws, req);
      });
      return;
    }

    if (pathname === "/cdp") {
      const token = getHeader(req, RELAY_AUTH_HEADER);
      if (!token || token !== relayAuthToken) {
        rejectUpgrade(socket, 401, "Unauthorized");
        return;
      }
      if (!extensionWs) {
        rejectUpgrade(socket, 503, "Extension not connected");
        return;
      }
      wssCdp.handleUpgrade(req, socket, head, (ws) => {
        wssCdp.emit("connection", ws, req);
      });
      return;
    }

    rejectUpgrade(socket, 404, "Not Found");
  });

  wssExtension.on("connection", (ws) => {
    extensionWs = ws;

    const ping = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) {
        return;
      }
      ws.send(JSON.stringify({ method: "ping" } satisfies ExtensionPingMessage));
    }, 5000);

    ws.on("message", (data) => {
      let parsed: ExtensionMessage | null = null;
      try {
        parsed = JSON.parse(rawDataToString(data)) as ExtensionMessage;
      } catch {
        return;
      }

      if (parsed && typeof parsed === "object" && "id" in parsed && typeof parsed.id === "number") {
        const pending = pendingExtension.get(parsed.id);
        if (!pending) {
          return;
        }
        pendingExtension.delete(parsed.id);
        clearTimeout(pending.timer);
        if ("error" in parsed && typeof parsed.error === "string" && parsed.error.trim()) {
          pending.reject(new Error(parsed.error));
        } else {
          pending.resolve(parsed.result);
        }
        return;
      }

      if (parsed && typeof parsed === "object" && "method" in parsed) {
        if ((parsed as ExtensionPongMessage).method === "pong") {
          return;
        }

        // Handle profile registration from extension
        if ((parsed as { method: string }).method === "register") {
          const regParams = (parsed as { params?: { profileId?: string; profileName?: string } })
            .params;
          if (regParams) {
            extensionProfile = {
              profileId: regParams.profileId,
              profileName: regParams.profileName,
            };
          }
          return;
        }

        if ((parsed as ExtensionForwardEventMessage).method !== "forwardCDPEvent") {
          return;
        }
        const evt = parsed as ExtensionForwardEventMessage;
        const method = evt.params?.method;
        const params = evt.params?.params;
        const sessionId = evt.params?.sessionId;
        if (!method || typeof method !== "string") {
          return;
        }

        if (method === "Target.attachedToTarget") {
          const attached = (params ?? {}) as AttachedToTargetEvent;
          const targetType = attached?.targetInfo?.type ?? "page";
          if (targetType !== "page") {
            return;
          }
          if (attached?.sessionId && attached?.targetInfo?.targetId) {
            const newRealSid = attached.sessionId;
            const nextTargetId = attached.targetInfo.targetId;

            // ── Session-aliasing for same-tab reattach ──────────────────
            // If this targetId is already tracked under a different session,
            // the tab was reattached (e.g. transient detach/reattach by the
            // extension).  Instead of tearing down and recreating the
            // Playwright CRPage, silently remap: Playwright keeps using the
            // old sessionId, we translate to/from the new real one.
            let existingPlaywrightSid: string | null = null;
            for (const [sid, target] of connectedTargets) {
              if (target.targetId === nextTargetId && sid !== newRealSid) {
                existingPlaywrightSid = sid;
                break;
              }
            }

            if (existingPlaywrightSid) {
              // Same tab, new Chrome sessionId → alias it
              // Clean up any old alias for this playwright session
              const oldReal = playwrightToReal.get(existingPlaywrightSid);
              if (oldReal) realToPlaywright.delete(oldReal);

              playwrightToReal.set(existingPlaywrightSid, newRealSid);
              realToPlaywright.set(newRealSid, existingPlaywrightSid);

              // Update connectedTargets to keep using the playwright sessionId
              connectedTargets.set(existingPlaywrightSid, {
                sessionId: existingPlaywrightSid,
                targetId: nextTargetId,
                targetInfo: attached.targetInfo,
              });
              // Don't broadcast anything — Playwright doesn't need to know
              return;
            }
            // ── End session-aliasing ────────────────────────────────────

            const prev = connectedTargets.get(newRealSid);
            const prevTargetId = prev?.targetId;
            const changedTarget = Boolean(prev && prevTargetId && prevTargetId !== nextTargetId);
            connectedTargets.set(newRealSid, {
              sessionId: newRealSid,
              targetId: nextTargetId,
              targetInfo: attached.targetInfo,
            });
            if (changedTarget && prevTargetId) {
              broadcastToCdpClients({
                method: "Target.detachedFromTarget",
                params: { sessionId: newRealSid, targetId: prevTargetId },
                sessionId: newRealSid,
              });
            }
            if (!prev || changedTarget) {
              broadcastToCdpClients({ method, params, sessionId });
            }
            return;
          }
        }

        if (method === "Target.detachedFromTarget") {
          const detached = (params ?? {}) as DetachedFromTargetEvent;
          const realSid = detached?.sessionId;
          if (realSid) {
            // Check if this real sessionId is aliased
            const pwSid = realToPlaywright.get(realSid);
            if (pwSid) {
              // Clean up aliases and use playwright's sessionId
              realToPlaywright.delete(realSid);
              playwrightToReal.delete(pwSid);
              connectedTargets.delete(pwSid);
              broadcastToCdpClients({
                method,
                params: { ...detached, sessionId: pwSid },
                sessionId: pwSid,
              });
            } else {
              connectedTargets.delete(realSid);
              broadcastToCdpClients({ method, params, sessionId });
            }
          } else {
            broadcastToCdpClients({ method, params, sessionId });
          }
          return;
        }

        // Keep cached tab metadata fresh for /json/list.
        // After navigation, Chrome updates URL/title via Target.targetInfoChanged.
        if (method === "Target.targetInfoChanged") {
          const changed = (params ?? {}) as { targetInfo?: { targetId?: string; type?: string } };
          const targetInfo = changed?.targetInfo;
          const targetId = targetInfo?.targetId;
          if (targetId && (targetInfo?.type ?? "page") === "page") {
            for (const [sid, target] of connectedTargets) {
              if (target.targetId !== targetId) {
                continue;
              }
              connectedTargets.set(sid, {
                ...target,
                targetInfo: { ...target.targetInfo, ...(targetInfo as object) },
              });
            }
          }
        }

        // Translate real Chrome sessionId → Playwright's sessionId for events
        const pwSessionId = toPlaywrightSession(sessionId);
        broadcastToCdpClients({ method, params, sessionId: pwSessionId });
      }
    });

    ws.on("close", () => {
      clearInterval(ping);
      extensionWs = null;
      extensionProfile = null;
      for (const [, pending] of pendingExtension) {
        clearTimeout(pending.timer);
        pending.reject(new Error("extension disconnected"));
      }
      pendingExtension.clear();
      connectedTargets.clear();
      playwrightToReal.clear();
      realToPlaywright.clear();

      for (const client of cdpClients) {
        try {
          client.close(1011, "extension disconnected");
        } catch {
          // ignore
        }
      }
      cdpClients.clear();
    });
  });

  wssCdp.on("connection", (ws) => {
    cdpClients.add(ws);

    ws.on("message", async (data) => {
      let cmd: CdpCommand | null = null;
      try {
        cmd = JSON.parse(rawDataToString(data)) as CdpCommand;
      } catch {
        return;
      }
      if (!cmd || typeof cmd !== "object") {
        return;
      }
      if (typeof cmd.id !== "number" || typeof cmd.method !== "string") {
        return;
      }

      if (!extensionWs) {
        sendResponseToCdp(ws, {
          id: cmd.id,
          sessionId: cmd.sessionId,
          error: { message: "Extension not connected" },
        });
        return;
      }

      try {
        const result = await routeCdpCommand(cmd);

        if (cmd.method === "Target.setAutoAttach" && !cmd.sessionId) {
          ensureTargetEventsForClient(ws, "autoAttach");
        }
        if (cmd.method === "Target.setDiscoverTargets") {
          const discover = (cmd.params ?? {}) as { discover?: boolean };
          if (discover.discover === true) {
            ensureTargetEventsForClient(ws, "discover");
          }
        }
        if (cmd.method === "Target.attachToTarget") {
          const params = (cmd.params ?? {}) as { targetId?: string };
          const targetId = typeof params.targetId === "string" ? params.targetId : undefined;
          if (targetId) {
            const target = Array.from(connectedTargets.values()).find(
              (t) => t.targetId === targetId,
            );
            if (target) {
              ws.send(
                JSON.stringify({
                  method: "Target.attachedToTarget",
                  params: {
                    sessionId: target.sessionId,
                    targetInfo: { ...target.targetInfo, attached: true },
                    waitingForDebugger: false,
                  },
                } satisfies CdpEvent),
              );
            }
          }
        }

        sendResponseToCdp(ws, { id: cmd.id, sessionId: cmd.sessionId, result });
      } catch (err) {
        sendResponseToCdp(ws, {
          id: cmd.id,
          sessionId: cmd.sessionId,
          error: { message: err instanceof Error ? err.message : String(err) },
        });
      }
    });

    ws.on("close", () => {
      cdpClients.delete(ws);
    });
  });

  await new Promise<void>((resolve, reject) => {
    server.listen(info.port, info.host, () => resolve());
    server.once("error", reject);
  });

  const addr = server.address() as AddressInfo | null;
  const port = addr?.port ?? info.port;
  const host = info.host;
  const baseUrl = `${new URL(info.baseUrl).protocol}//${host}:${port}`;

  const relay: ChromeExtensionRelayServer = {
    host,
    port,
    baseUrl,
    cdpWsUrl: `ws://${host}:${port}/cdp`,
    extensionConnected: () => Boolean(extensionWs),
    stop: async () => {
      serversByPort.delete(port);
      relayAuthByPort.delete(port);
      try {
        // Tell extension not to reconnect — this is a planned shutdown
        extensionWs?.send(JSON.stringify({ method: "shutdown" }));
        extensionWs?.close(1001, "server stopping");
      } catch {
        // ignore
      }
      for (const ws of cdpClients) {
        try {
          ws.close(1001, "server stopping");
        } catch {
          // ignore
        }
      }
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
      wssExtension.close();
      wssCdp.close();
    },
  };

  relayAuthByPort.set(port, relayAuthToken);
  serversByPort.set(port, relay);
  return relay;
}

export async function stopChromeExtensionRelayServer(opts: { cdpUrl: string }): Promise<boolean> {
  const info = parseBaseUrl(opts.cdpUrl);
  const existing = serversByPort.get(info.port);
  if (!existing) {
    return false;
  }
  await existing.stop();
  relayAuthByPort.delete(info.port);
  return true;
}
