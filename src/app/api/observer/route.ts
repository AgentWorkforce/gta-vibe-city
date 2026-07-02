import { unstable_cache } from "next/cache";
import { fromBridge } from "@/lib/bridge";
import { relay, relayKey, toFeedResponse } from "@/lib/relay";
import type { ObserverResponse } from "@/lib/types";

export const dynamic = "force-dynamic";

const MAX_CHANNELS = 8;
const PER_CHANNEL = 100; // upstream page-size cap; older history via /api/history

function empty(): ObserverResponse {
  return {
    channels: [],
    agents: [],
    messages: [],
    generatedAt: new Date().toISOString(),
  };
}

const getLiveObserver = unstable_cache(
  async (): Promise<ObserverResponse> => {
    const [allChannels, agents] = await Promise.all([
      relay().channels.list(),
      relay().agents.list(),
    ]);
    const channels = allChannels.filter((c) => !c.archived).slice(0, MAX_CHANNELS);
    const perChannel = await Promise.all(
      channels.map((c) =>
        relay()
          .messages.list(c.name, { limit: PER_CHANNEL })
          .catch(() => []),
      ),
    );
    const merged = toFeedResponse("", perChannel.flat(), agents);
    return {
      channels: channels.map((c, i) => ({
        name: c.name,
        topic: c.topic,
        messageCount: perChannel[i].length,
      })),
      agents: merged.agents,
      messages: merged.messages,
      generatedAt: merged.generatedAt,
    };
  },
  ["relay-observer"],
  { revalidate: 4 },
);

const getBridgeObserver = unstable_cache(
  async () => fromBridge<ObserverResponse>("/observer"),
  ["bridge-observer"],
  { revalidate: 2 },
);

export async function GET() {
  try {
    const body =
      (process.env.BRIDGE_URL ? await getBridgeObserver() : null) ??
      (relayKey() ? await getLiveObserver() : empty());
    return Response.json(body, {
      headers: { "Cache-Control": "s-maxage=3, stale-while-revalidate=10" },
    });
  } catch {
    return Response.json(empty());
  }
}
