/**
 * Always-on workspace bridge. Runs next to the workers (anywhere with the
 * workspace key + burn's ledger), holds one realtime event stream to Agent
 * Relay, and serves the site's wire shapes over HTTP:
 *
 *   GET /feed      -> FeedResponse        (latest messages + agent roster)
 *   GET /observer  -> ObserverResponse    (channels + agents + all messages)
 *   GET /spend     -> { spentUsd, inputTokens, outputTokens, generations }
 *   GET /healthz   -> { ok, agents, messages, connected }
 *
 * Env: RELAY_OBSERVER_TOKEN (ot_live_…, read-only) — the only relay credential
 *      needed: REST reads plus near-realtime events via the durable workspace
 *      event log (GET /v1/workspace/events cursor polling, relaycast v5.1+).
 *      RELAY_WORKSPACE_KEY (rk_live_…) works as a fallback but is admin-scoped;
 *      prefer the observer token. RELAY_BASE_URL, PORT (8390), BRIDGE_TOKEN
 *      (optional bearer auth), BURN_PROJECT (path filter), BURN_SINCE (e.g.
 *      "30d"; default all-time).
 *
 * Run: npm run bridge   (or: node bridge/server.mjs)
 */
import http from "node:http";
import { execFile } from "node:child_process";
import { AgentRelay } from "@agent-relay/sdk";

const KEY = process.env.RELAY_OBSERVER_TOKEN ?? process.env.RELAY_WORKSPACE_KEY;
if (!KEY) {
  console.error("RELAY_OBSERVER_TOKEN (or RELAY_WORKSPACE_KEY) is required");
  process.exit(1);
}
const BASE_URL = (process.env.RELAY_BASE_URL ?? "https://cast.agentrelay.com").replace(/\/+$/, "");
const EVENTS_POLL_MS = 2000;
const PORT = Number(process.env.PORT) || 8390;
const MAX_MESSAGES = 400;
const OFFLINE_AFTER_MS = 30 * 60 * 1000;
const WORK_STATUSES = new Set(["active", "idle", "blocked", "waiting"]);

const relay = new AgentRelay({
  workspaceKey: KEY,
  ...(process.env.RELAY_BASE_URL ? { baseUrl: process.env.RELAY_BASE_URL } : {}),
});

// ── state ─────────────────────────────────────────────────────────────────────
const agents = new Map(); // name -> CrewAgent
const agentNamesById = new Map();
const channels = new Map(); // name -> { name, topic }
let messages = []; // FeedMessage[], oldest first
let connected = false;
let spend = { spentUsd: 0, inputTokens: 0, outputTokens: 0, generations: 0 };

function toStatus(status, lastSeenAt) {
  if (WORK_STATUSES.has(status)) return status;
  if (status === "online") return "active";
  if (lastSeenAt && Date.now() - new Date(lastSeenAt).getTime() > OFFLINE_AFTER_MS) {
    return "offline";
  }
  return "active";
}

function mapMessage(m, channelName) {
  return {
    id: m.id ?? m.messageId,
    agent: m.from?.name ?? "unknown",
    channel: m.channel?.name ?? channelName ?? "",
    text: m.text ?? "",
    createdAt: m.createdAt ?? new Date().toISOString(),
    mentions: m.mentions ?? [],
    reactions: (m.reactions ?? []).map((r) => ({ emoji: r.emoji, count: r.count })),
    replyCount: m.replyCount ?? 0,
  };
}

function pushMessage(msg) {
  if (!msg.id || messages.some((m) => m.id === msg.id)) return;
  messages.push(msg);
  messages.sort((a, b) => a.createdAt.localeCompare(b.createdAt));
  if (messages.length > MAX_MESSAGES) messages = messages.slice(-MAX_MESSAGES);
  // a message is also a liveness signal
  const a = agents.get(msg.agent);
  if (a) {
    a.lastSeen = msg.createdAt;
    if (a.status === "offline") a.status = "active";
  }
}

// ── seeding + refresh over REST ───────────────────────────────────────────────
async function refreshAgents() {
  const list = await relay.agents.list();
  for (const a of list) {
    if (a.type === "system") continue;
    agentNamesById.set(a.id, a.name);
    const existing = agents.get(a.name);
    agents.set(a.name, {
      name: a.name,
      role:
        a.persona ?? (typeof a.metadata?.role === "string" ? a.metadata.role : undefined),
      // realtime status (set via events) wins over polled presence
      status: existing?.fromEvent ? existing.status : toStatus(a.status, a.lastSeenAt),
      fromEvent: existing?.fromEvent ?? false,
      lastSeen: a.lastSeenAt ?? existing?.lastSeen ?? new Date().toISOString(),
      currentAction: existing?.currentAction,
    });
  }
}

async function refreshChannels() {
  const list = await relay.channels.list();
  for (const c of list) {
    if (!c.archived) channels.set(c.name, { name: c.name, topic: c.topic });
  }
}

async function seedMessages() {
  for (const c of channels.values()) {
    try {
      const batch = await relay.messages.list(c.name, { limit: 30 });
      for (const m of batch) pushMessage(mapMessage(m, c.name));
    } catch {
      // channel may be empty or restricted; skip
    }
  }
}

// ── realtime events (durable workspace event log) ─────────────────────────────
// Relaycast v5.1 appends every workspace event to a durable, per-workspace
// `(seq)`-cursored log, readable with the observer token even where the push
// WebSocket stream is gated off. The bridge tails it: seed the cursor at boot
// (history comes from the REST seed instead), then poll for new rows every
// EVENTS_POLL_MS. `connected` now means the tail is healthy.
let eventsCursor = 0;
let polling = false;

async function fetchEvents(since, limit) {
  const res = await fetch(
    `${BASE_URL}/v1/workspace/events?since=${since}&limit=${limit}`,
    { headers: { authorization: `Bearer ${KEY}` } },
  );
  if (!res.ok) throw new Error(`events poll HTTP ${res.status}`);
  const body = await res.json();
  return body.data ?? body; // { events, latest_seq, next_since }
}

// Rows store the client-shaped WS frame: message events carry
// { channel, message: { id, agent_name, text } }, status events carry
// { agent: { name }, status }.
function handleEventRow(row) {
  const p = row.payload ?? {};
  if (row.type === "message.created" || row.type === "thread.reply") {
    if (!p.message?.id) return;
    pushMessage({
      id: p.message.id,
      agent: p.message.agent_name ?? "unknown",
      channel: p.channel ?? "",
      text: p.message.text ?? "",
      createdAt: p.created_at ?? row.created_at ?? new Date().toISOString(),
      mentions: [],
      reactions: [],
      replyCount: 0,
    });
  } else if (row.type?.startsWith("agent.status")) {
    const a = agents.get(p.agent?.name);
    if (!a) return;
    const status = p.status ?? row.type.split(".").pop();
    if (WORK_STATUSES.has(status) || status === "offline") {
      a.status = status;
      a.fromEvent = true;
      a.lastSeen = p.created_at ?? new Date().toISOString();
    }
  }
}

async function pollEvents() {
  if (polling) return;
  polling = true;
  try {
    // Catch up in pages in case more than one page accrued between polls.
    for (;;) {
      const data = await fetchEvents(eventsCursor, 500);
      for (const row of data.events ?? []) {
        try {
          handleEventRow(row);
        } catch (err) {
          console.error("event handling error", err);
        }
      }
      eventsCursor = data.next_since ?? data.latest_seq ?? eventsCursor;
      if (!data.events?.length || eventsCursor >= (data.latest_seq ?? 0)) break;
    }
    if (!connected) {
      connected = true;
      console.log("event log tail connected");
    }
  } catch (err) {
    if (connected) console.error("event log tail failed:", err.message);
    connected = false;
  } finally {
    polling = false;
  }
}

async function startEventTail() {
  // Start at the log head — history is already covered by the REST seed. If
  // the head fetch fails we still start the tail: it recovers on a later poll
  // (a cursor of 0 replays the retained log; pushMessage dedupes by id).
  try {
    const head = await fetchEvents(0, 1);
    eventsCursor = head.latest_seq ?? 0;
    connected = true;
    console.log(`event log tail started at seq ${eventsCursor}`);
  } catch (err) {
    console.error("event log head fetch failed:", err.message);
  }
  setInterval(pollEvents, EVENTS_POLL_MS);
}

// ── burn metering ─────────────────────────────────────────────────────────────
function refreshSpend() {
  const args = ["summary", "--json"];
  if (process.env.BURN_PROJECT) args.push("--project", process.env.BURN_PROJECT);
  if (process.env.BURN_SINCE) args.push("--since", process.env.BURN_SINCE);
  execFile("burn", args, { maxBuffer: 16 * 1024 * 1024 }, (err, stdout) => {
    if (err) {
      console.error("burn summary failed:", err.message);
      return;
    }
    try {
      const data = JSON.parse(stdout);
      const usage = (data.byModel ?? []).reduce(
        (acc, m) => ({
          input: acc.input + (m.usage?.input ?? 0),
          output: acc.output + (m.usage?.output ?? 0),
        }),
        { input: 0, output: 0 },
      );
      spend = {
        spentUsd: Math.round((data.totalCost?.total ?? 0) * 100) / 100,
        inputTokens: usage.input,
        outputTokens: usage.output,
        generations: data.turns ?? 0,
      };
    } catch (parseErr) {
      console.error("burn output parse failed:", parseErr.message);
    }
  });
}

// ── http ──────────────────────────────────────────────────────────────────────
function agentList() {
  return [...agents.values()].map(({ fromEvent: _ignored, ...a }) => a);
}

const server = http.createServer((req, res) => {
  const token = process.env.BRIDGE_TOKEN;
  if (token && req.headers.authorization !== `Bearer ${token}`) {
    res.writeHead(401).end();
    return;
  }
  const url = new URL(req.url ?? "/", "http://bridge");
  const now = new Date().toISOString();
  let body;
  if (url.pathname === "/healthz") {
    body = { ok: true, connected, agents: agents.size, messages: messages.length };
  } else if (url.pathname === "/feed") {
    body = { messages: messages.slice(-30), agents: agentList(), generatedAt: now };
  } else if (url.pathname === "/observer") {
    body = {
      channels: [...channels.values()].map((c) => ({
        ...c,
        messageCount: messages.filter((m) => m.channel === c.name).length,
      })),
      agents: agentList(),
      messages,
      generatedAt: now,
    };
  } else if (url.pathname === "/spend") {
    body = spend;
  } else {
    res.writeHead(404).end();
    return;
  }
  res.writeHead(200, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
});

// ── boot ──────────────────────────────────────────────────────────────────────
await refreshChannels().catch((e) => console.error("channels seed failed", e.message));
await refreshAgents().catch((e) => console.error("agents seed failed", e.message));
await seedMessages();
await startEventTail().catch((e) => console.error("event tail start failed", e.message));
refreshSpend();
setInterval(() => refreshAgents().catch(() => {}), 60_000);
setInterval(() => refreshChannels().catch(() => {}), 120_000);
setInterval(refreshSpend, 60_000);

server.listen(PORT, () => {
  console.log(
    `bridge up on :${PORT} — ${agents.size} agents, ${channels.size} channels, ${messages.length} messages`,
  );
});
