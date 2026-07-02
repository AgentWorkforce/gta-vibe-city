import { unstable_cache } from "next/cache";
import { relay, relayKey, toFeedResponse } from "@/lib/relay";

export const dynamic = "force-dynamic";

// Older pages are immutable, so cache them long per (channel, before) cursor.
const getHistory = unstable_cache(
  async (channel: string, before: string) => {
    const messages = await relay().messages.list(channel, { limit: 100, before });
    return toFeedResponse(channel, messages, []).messages;
  },
  ["relay-history"],
  { revalidate: 300 },
);

export async function GET(req: Request) {
  const url = new URL(req.url);
  const channel = url.searchParams.get("channel");
  const before = url.searchParams.get("before");
  if (!channel || !before || !relayKey()) {
    return Response.json({ messages: [] });
  }
  try {
    const messages = await getHistory(channel, before);
    return Response.json(
      { messages },
      { headers: { "Cache-Control": "s-maxage=300, stale-while-revalidate=3600" } },
    );
  } catch {
    return Response.json({ messages: [] });
  }
}
