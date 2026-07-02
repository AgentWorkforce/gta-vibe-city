import { unstable_cache } from "next/cache";
import { fromBridge } from "@/lib/bridge";
import { relay, relayKey, toFeedResponse } from "@/lib/relay";
import type { FeedResponse } from "@/lib/types";

export const dynamic = "force-dynamic";

const EMPTY: Omit<FeedResponse, "generatedAt"> = { messages: [], agents: [] };

const getLiveFeed = unstable_cache(
  async () => {
    const channel =
      process.env.RELAY_FEED_CHANNEL ??
      (await relay().channels.list()).find((c) => !c.archived)?.name ??
      "general";
    const [messages, agents] = await Promise.all([
      relay().messages.list(channel, { limit: 30 }),
      relay().agents.list(),
    ]);
    return toFeedResponse(channel, messages, agents);
  },
  ["relay-feed"],
  { revalidate: 4 },
);

const getBridgeFeed = unstable_cache(
  async () => fromBridge<FeedResponse>("/feed"),
  ["bridge-feed"],
  { revalidate: 2 },
);

export async function GET() {
  try {
    const body =
      (process.env.BRIDGE_URL ? await getBridgeFeed() : null) ??
      (relayKey()
        ? await getLiveFeed()
        : { ...EMPTY, generatedAt: new Date().toISOString() });
    return Response.json(body, {
      headers: { "Cache-Control": "s-maxage=3, stale-while-revalidate=10" },
    });
  } catch {
    // upstream unreachable: serve an empty feed rather than break the page
    return Response.json({ ...EMPTY, generatedAt: new Date().toISOString() });
  }
}
