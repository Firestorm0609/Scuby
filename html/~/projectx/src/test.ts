import { PumpFunClient } from "./scanner/pumpfun.js";

async function test() {
  console.log("Testing Pump.fun API connection...");
  const client = new PumpFunClient();
  const tokens = await client.fetchLatestTokens(5);
  console.log("Tokens found:", tokens.length);
  if (tokens.length > 0) {
    console.log("First token:", JSON.stringify(tokens[0], null, 2));
  }
}

test().catch(console.error);
