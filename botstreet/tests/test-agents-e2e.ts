import "dotenv/config";
import { Agent, Box, BoxApiKey, ClaudeCode, OpenCodeModel } from "@upstash/box";
import { readdirSync } from "fs";
import path from "path";
import { fileURLToPath } from "url";
const __dirname = path.dirname(fileURLToPath(import.meta.url));

async function getBoxByName(name: string): Promise<InstanceType<typeof Box>> {
  const boxes = await Box.list();
  const match = boxes.find((b: any) => b.name === name);
  if (!match) throw new Error(`Box not found by name: ${name}`);
  return Box.get(match.id);
}
const ROOT = path.resolve(__dirname, "..");

const PROMPT = "trade";
const FORK_READY_TIMEOUT_MS = 5 * 60 * 1000;
const STATUS_POLL_MS = 2_000;
const FILE_READY_TIMEOUT_MS = 60_000;
const BOX_ROOT = "/workspace/home";
const DATA_DIR = `${BOX_ROOT}/data`;

const agents = [
  { name: "claude", boxName: "botstreet-claude-v10" },
  { name: "gemini", boxName: "botstreet-gemini-v10" },
  { name: "openai", boxName: "botstreet-openai-v10" },
] as const;

let passed = 0;
let failed = 0;

function assert(name: string, condition: boolean, detail?: string) {
  if (condition) {
    console.log(`    ✓ ${name}`);
    passed++;
  } else {
    console.log(`    ✗ ${name}${detail ? ` — ${detail}` : ""}`);
    failed++;
  }
}

function parse<T = any>(json: string): T | null {
  try {
    return JSON.parse(json) as T;
  } catch {
    return null;
  }
}

async function sleep(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function readFile(box: any, filePath: string): Promise<string> {
  try {
    return await box.files.read(filePath);
  } catch {
    return "";
  }
}

function agentPath(fileName: string) {
  return `${DATA_DIR}/${fileName}`;
}

async function readAgentFile(box: any, fileName: string): Promise<string> {
  return await readFile(box, agentPath(fileName));
}

async function waitUntilForkable(agentName: string, box: any) {
  const start = Date.now();

  while (Date.now() - start < FORK_READY_TIMEOUT_MS) {
    const status = await box.getStatus();
    if (status.status === "idle" || status.status === "paused") {
      return status.status;
    }
    if (status.status === "error") {
      throw new Error(`${agentName} source box is in error state`);
    }
    await sleep(STATUS_POLL_MS);
  }

  throw new Error(`${agentName} source box did not become idle/paused in time`);
}

async function waitForNonEmptyFile(box: any, filePath: string, timeoutMs = FILE_READY_TIMEOUT_MS) {
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    const content = await readFile(box, filePath);
    if (content) {
      return content;
    }
    await sleep(1_000);
  }

  return "";
}

function cloneSkill(agentName: string): string {
  return `# E2E Trade Skill

You are running an automated end-to-end validation for the ${agentName} agent.

	When you receive the prompt "trade", do exactly this:

1. Run \`npx tsx /workspace/home/tools/portfolio.ts get\`.
2. Run \`npx tsx /workspace/home/tools/prices.ts current AAPL\`.
3. Execute exactly one valid buy trade:
   \`npx tsx /workspace/home/tools/trade.ts execute AAPL buy 1000 e2e_validation_trade\`
4. Run \`npx tsx /workspace/home/tools/snapshot.ts save\`.
5. Write a short entry to \`/workspace/home/data/diary.md\` noting that the E2E validation trade completed today.
6. Write a short update to \`/workspace/home/data/memory.md\` noting that the validation trade succeeded.

Do not skip the trade. Do not choose hold. Do not ask follow-up questions.`;
}

async function syncCloneWorkspace(agentName: string, box: any) {
  await box.files.upload([
    { path: path.join(ROOT, "package.json"), destination: "/workspace/home/package.json" },
    { path: path.join(ROOT, "tsconfig.json"), destination: "/workspace/home/tsconfig.json" },
  ]);

  const toolsDir = path.join(ROOT, "tools");
  const toolFiles = readdirSync(toolsDir).filter((file) => file.endsWith(".ts"));
  await box.files.upload(
    toolFiles.map((file) => ({
      path: path.join(toolsDir, file),
      destination: `/workspace/home/tools/${file}`,
    })),
  );

  const skillContent = cloneSkill(agentName);
  await box.files.write({
    path: "/workspace/home/skills/trade/SKILL.md",
    content: skillContent,
  });
  await box.files.write({
    path: "/workspace/home/CLAUDE.md",
    content: skillContent,
  });
  await box.files.write({
    path: "/workspace/home/AGENTS.md",
    content: skillContent,
  });
  await box.files.write({
    path: "/workspace/home/SKILL.md",
    content: skillContent,
  });
}

async function prepareClone(agentName: string, box: any) {
  const portfolio = parse<any>(await readAgentFile(box, "portfolio.json"));
  if (!portfolio?.agent) {
    throw new Error(`Missing portfolio in clone for ${agentName}`);
  }

  portfolio.last_run_date = null;
  portfolio.last_action = null;

  const portfolioPath = agentPath("portfolio.json");
  const tradesPath = agentPath("today_trades.json");
  const diaryPath = agentPath("diary.md");
  const memoryPath = agentPath("memory.md");
  const today = new Date().toISOString().split("T")[0];
  const snapshotPath = `${DATA_DIR}/history/portfolio_${today.replace(/-/g, "_")}.json`;

  await box.files.write({
    path: portfolioPath,
    content: JSON.stringify(portfolio, null, 2),
  });
  await box.files.write({
    path: diaryPath,
    content: (await readAgentFile(box, "diary.md")) || "# Trading Diary\n",
  });
  await box.files.write({
    path: memoryPath,
    content: (await readAgentFile(box, "memory.md")) || "# Trading Memory\n",
  });
  await box.files.write({ path: tradesPath, content: "[]" });
  await box.files.write({
    path: snapshotPath,
    content: JSON.stringify({ sentinel: true, trades: [] }, null, 2),
  });

  return {
    portfolioPath,
    tradesPath,
    snapshotPath,
    before: portfolio,
  };
}

function getCloneConfig(agent: (typeof agents)[number]) {
  if (agent.name === "claude") {
    return {
      agent: {
        provider: Agent.ClaudeCode,
        model: ClaudeCode.Opus_4_6,
        apiKey: process.env.ANTHROPIC_API_KEY!,
      },
    };
  }

  if (agent.name === "gemini") {
    return {
      env: {
        GOOGLE_API_KEY: "",
        GEMINI_API_KEY: "",
      },
      agent: {
        provider: Agent.OpenCode,
        model: OpenCodeModel.Zen_Gemini_3_1_Pro,
        apiKey: BoxApiKey.UpstashKey,
      },
    };
  }

  return {
    agent: {
      provider: Agent.Codex,
      model: "openai/gpt-5.4",
      apiKey: process.env.OPENAI_API_KEY!,
    },
  };
}

async function runAgentE2E(agent: (typeof agents)[number], box: any) {
  console.log(`\n[${agent.name} clone]`);

  await waitUntilForkable(`${agent.name} clone`, box);
  await syncCloneWorkspace(agent.name, box);

  const skill = await waitForNonEmptyFile(box, "/workspace/home/SKILL.md");
  assert("Root SKILL.md present", skill.includes("Trading Skill"));

  const prepared = await prepareClone(agent.name, box);

  const run = await box.agent.run({ prompt: PROMPT, timeout: 600000 });
  assert("Agent run completed", run.status === "completed", `status=${run.status}`);

  const today = new Date().toISOString().split("T")[0];
  const after = parse<any>(await readFile(box, prepared.portfolioPath));
  const diary = await readFile(box, `${DATA_DIR}/diary.md`);
  const memory = await readFile(box, `${DATA_DIR}/memory.md`);
  const snapshot = parse<any>(await readFile(box, prepared.snapshotPath));

  assert("portfolio.json exists", !!after?.agent);
  assert("last_action exists", !!after?.last_action?.timestamp);
  assert("Diary updated", diary.length > 20);
  assert("Memory updated", memory.length > 20);
  assert("Snapshot exists for today", !!snapshot?.date);
  assert("Snapshot contains at least one trade", Array.isArray(snapshot?.trades) && snapshot.trades.length > 0);
  assert(
    "Portfolio changed after run",
    JSON.stringify(after?.holdings ?? []) !== JSON.stringify(prepared.before.holdings ?? []) ||
      after?.cash !== prepared.before.cash,
  );

  const holdingsSum = (after?.holdings ?? []).reduce((sum: number, holding: any) => sum + holding.dollars, 0);
  const expectedTotal = Math.round(((after?.cash ?? 0) + holdingsSum) * 100) / 100;
  assert(
    "Portfolio math holds",
    after?.total_value === expectedTotal,
    `total=${after?.total_value}, expected=${expectedTotal}`,
  );

  return {
    name: agent.name,
    totalValue: after?.total_value,
    cash: after?.cash,
    trades: snapshot?.trades?.length ?? 0,
  };
}

async function main() {
  const skipCleanup = process.argv.includes("--no-cleanup");

  console.log("=== BotStreet E2E — Three Agents On Clones ===\n");
  if (skipCleanup) console.log("[--no-cleanup] Clones and snapshots will be preserved for debugging.\n");

  const results: Array<{ name: string; totalValue: number; cash: number; trades: number }> = [];

  console.log("[Sequential clone runs]");
  for (const agent of agents) {
    let sourceBox: any;
    let snapshotId: string | undefined;
    let clone: any;

    try {
      sourceBox = await getBoxByName(agent.boxName);
      const sourceStatus = await waitUntilForkable(agent.name, sourceBox);
      assert(`${agent.name} source box reachable`, sourceStatus === "idle" || sourceStatus === "paused");

      const snapshot = await sourceBox.snapshot({
        name: `e2e-${agent.name}-${Date.now()}`,
      });
      snapshotId = snapshot.id;
      console.log(`  ${agent.name}: snapshot ${snapshotId}`);

      clone = await Box.fromSnapshot(snapshot.id, getCloneConfig(agent) as any);
      console.log(`  ${agent.name}: clone ${clone.id}`);

      results.push(await runAgentE2E(agent, clone));
    } catch (error) {
      console.log(`  FAIL ${agent.name}: ${String(error)}`);
      failed++;
    } finally {
      if (skipCleanup) {
        console.log(`  ${agent.name}: skipping cleanup (--no-cleanup)`);
        if (clone) console.log(`    clone preserved: ${clone.id}`);
        if (snapshotId) console.log(`    snapshot preserved: ${snapshotId}`);
      } else {
        console.log(`  ${agent.name}: cleanup`);

        if (clone) {
          try {
            await clone.delete();
            console.log(`    deleted clone ${clone.id}`);
          } catch (error) {
            console.error(`    failed to delete clone:`, error);
          }
        }

        if (sourceBox && snapshotId) {
          try {
            await sourceBox.deleteSnapshot(snapshotId);
            console.log(`    deleted snapshot ${snapshotId}`);
          } catch (error) {
            console.error(`    failed to delete snapshot:`, error);
          }
        }
      }
    }
  }

  console.log("\n[Summary]");
  for (const result of results) {
    console.log(`  ${result.name}: total=$${result.totalValue} cash=$${result.cash} trades=${result.trades}`);
  }

  console.log(`\n=== Results: ${passed} passed, ${failed} failed ===`);
  if (failed > 0) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("E2E test failed:", err);
  process.exit(1);
});
