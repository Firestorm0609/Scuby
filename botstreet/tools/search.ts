import "dotenv/config";
import type { SearchResult } from "./types.js";

export async function webSearch(query: string): Promise<SearchResult[]> {
  const apiKey = process.env.BRAVE_API_KEY;
  if (!apiKey) {
    throw new Error("BRAVE_API_KEY not set");
  }

  const url = new URL("https://api.search.brave.com/res/v1/web/search");
  url.searchParams.set("q", query);
  url.searchParams.set("count", "10");

  const res = await fetch(url.toString(), {
    headers: {
      Accept: "application/json",
      "Accept-Encoding": "gzip",
      "X-Subscription-Token": apiKey,
    },
  });

  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = (await res.json()) as any;
      detail = body?.error?.detail ?? body?.error?.code ?? res.statusText;
    } catch {
      // keep the HTTP status text if the error payload is not JSON
    }
    throw new Error(`Brave Search API returned ${res.status}: ${detail}`);
  }

  const data = (await res.json()) as any;
  const results: SearchResult[] = (data.web?.results ?? []).map((r: any) => ({
    title: r.title ?? "",
    url: r.url ?? "",
    snippet: r.description ?? "",
  }));

  return results;
}

// ── CLI entry point ──

const query = process.argv.slice(2).join(" ");
if (!query) {
  console.log(JSON.stringify({ error: "usage: search.ts <query>" }));
  process.exit(1);
}

try {
  const results = await webSearch(query);
  console.log(JSON.stringify(results, null, 2));
} catch (e: any) {
  console.log(JSON.stringify({ error: e.message }));
  process.exit(1);
}
