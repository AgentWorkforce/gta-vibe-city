import "server-only";
import { AgentRelay } from "@agent-relay/sdk";
import type {
  RelayAgent,
  RelayMessage,
} from "@agent-relay/sdk";
import type { AgentStatus, CrewAgent, FeedMessage, FeedResponse } from "./types";

let client: AgentRelay | undefined;

/**
 * Credential for the read-only site surface. Prefer a scoped observer token
 * (ot_live_…, relay v9+) over the workspace admin key (rk_live_…) — the site
 * only reads, and only the observer token can hold the realtime stream.
 */
export function relayKey(): string | undefined {
  return process.env.RELAY_OBSERVER_TOKEN ?? process.env.RELAY_WORKSPACE_KEY;
}

export function relay(): AgentRelay {
  return (client ??= new AgentRelay({
    workspaceKey: relayKey()!,
    ...(process.env.RELAY_BASE_URL ? { baseUrl: process.env.RELAY_BASE_URL } : {}),
  }));
}

const WORK_STATUSES: AgentStatus[] = ["active", "idle", "blocked", "waiting"];
const OFFLINE_AFTER_MS = 30 * 60 * 1000;

function toStatus(status: string | undefined, lastSeenAt?: string): AgentStatus {
  // Rich work states pass through when the workspace reports them.
  if (WORK_STATUSES.includes(status as AgentStatus)) return status as AgentStatus;
  if (status === "online") return "active";
  // "offline" here is socket presence, not work state — agents posting over
  // REST/MCP never hold a socket. Trust recent activity instead and only go
  // offline after a long silence.
  if (lastSeenAt && Date.now() - new Date(lastSeenAt).getTime() > OFFLINE_AFTER_MS) {
    return "offline";
  }
  return "active";
}

export function toFeedResponse(
  channel: string,
  messages: RelayMessage[],
  agents: RelayAgent[],
): FeedResponse {
  const feedMessages: FeedMessage[] = messages
    .map((m) => ({
      id: m.id,
      agent: m.from?.name ?? "unknown",
      channel: m.channel?.name ?? channel,
      text: m.text,
      createdAt: m.createdAt ?? new Date().toISOString(),
      mentions: m.mentions ?? [],
      reactions: (m.reactions ?? []).map((r) => ({ emoji: r.emoji, count: r.count })),
      replyCount: m.replyCount ?? 0,
    }))
    .sort((a, b) => a.createdAt.localeCompare(b.createdAt));

  const crew: CrewAgent[] = agents
    .filter((a) => a.type !== "system")
    .map((a) => ({
      name: a.name,
      role:
        a.persona ??
        (typeof a.metadata?.role === "string" ? a.metadata.role : undefined),
      status: toStatus(a.status, a.lastSeenAt),
      lastSeen: a.lastSeenAt ?? new Date().toISOString(),
    }));

  return {
    messages: feedMessages,
    agents: crew,
    generatedAt: new Date().toISOString(),
  };
}
