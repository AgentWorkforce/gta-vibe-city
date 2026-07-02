/**
 * Mint the read-only observer token (ot_live_…) the site + bridge use on
 * Agent Relay v9+, and append it to .env.local as RELAY_OBSERVER_TOKEN.
 *
 * Run: RELAY_WORKSPACE_KEY=rk_live_… node scripts/create-observer-token.mjs
 *
 * Requires the workspace ADMIN key (rk_live_…). The token secret is only
 * returned once, so it is written to .env.local and never printed.
 */
import { appendFileSync } from "node:fs";
import { RelayCast } from "@relaycast/sdk";

const KEY = process.env.RELAY_WORKSPACE_KEY;
if (!KEY?.startsWith("rk_")) {
  console.error("RELAY_WORKSPACE_KEY (rk_live_…) is required");
  process.exit(1);
}

const rc = new RelayCast({
  apiKey: KEY,
  ...(process.env.RELAY_BASE_URL ? { baseUrl: process.env.RELAY_BASE_URL } : {}),
});

const existing = await rc.observerTokens.list();
const clash = existing.find((t) => t.name === "vibe-city-site" && t.status === "active");
if (clash) {
  console.error(
    `An active "vibe-city-site" token already exists (${clash.id}). ` +
      "Rotate it to get a fresh secret: rc.observerTokens.rotate(id)",
  );
  process.exit(1);
}

const token = await rc.observerTokens.create({
  name: "vibe-city-site",
  description: "Read-only token for the vibe-city public site + bridge",
  scopes: [
    "stream:read",
    "messages:read",
    "threads:read",
    "channels:read",
    "agents:read",
    "reactions:read",
    "search:read",
    "activity:read",
  ],
});

appendFileSync(".env.local", `RELAY_OBSERVER_TOKEN=${token.token}\n`);
console.log(
  `Created observer token ${token.id} (${token.name}) — secret appended to .env.local`,
);
