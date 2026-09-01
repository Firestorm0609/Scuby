import { Box } from "@upstash/box";
import { env } from "$env/dynamic/private";
import { getOrSetCache } from "$lib/server/cache.js";

const BOX_CACHE_TTL_MS = 5 * 60 * 1000;

export async function getBoxByName(name: string): Promise<Box> {
  return getOrSetCache(`box:${name}`, BOX_CACHE_TTL_MS, async () => {
    const boxes = await Box.list({ apiKey: env.UPSTASH_BOX_API_KEY });
    const match = boxes.find((box: any) => box.name === name);

    if (!match) {
      throw new Error(`Box not found by name: ${name}`);
    }

    const box = await Box.get(match.id, { apiKey: env.UPSTASH_BOX_API_KEY });

    return box;
  });
}
