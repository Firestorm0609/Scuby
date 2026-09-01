import "dotenv/config";
import { Box } from "@upstash/box";

const V2_AGENTS = [
  { name: "claude", boxName: "botstreet-claude-v2" },
  { name: "gemini", boxName: "botstreet-gemini-v2" },
  { name: "openai", boxName: "botstreet-openai-v2" },
];

const V10_AGENTS = [
  { name: "claude", boxName: "botstreet-claude-v10" },
  { name: "gemini", boxName: "botstreet-gemini-v10" },
  { name: "openai", boxName: "botstreet-openai-v10" },
];

const DATA_DIR = "/workspace/home/data";

async function getBoxByName(name: string): Promise<InstanceType<typeof Box>> {
  const boxes = await Box.list();
  const match = boxes.find((b: any) => b.name === name);
  if (!match) throw new Error(`Box not found: ${name}`);
  return Box.get(match.id);
}

async function listFilesRecursive(box: InstanceType<typeof Box>, dir: string): Promise<string[]> {
  const paths: string[] = [];
  const entries = await box.files.list(dir);

  for (const entry of entries) {
    const entryPath = entry.path ?? `${dir}/${entry.name}`;
    if (entry.is_dir) {
      paths.push(...(await listFilesRecursive(box, entryPath)));
    } else {
      paths.push(entryPath);
    }
  }

  return paths;
}

async function pullData(boxName: string): Promise<Map<string, string>> {
  const box = await getBoxByName(boxName);
  const files = await listFilesRecursive(box, DATA_DIR);
  const data = new Map<string, string>();

  for (const filePath of files) {
    try {
      const content = await box.files.read(filePath);
      const relativePath = filePath.replace(`${DATA_DIR}/`, "");
      // Skip directory entries that slipped through (no extension, empty content)
      if (!relativePath.includes(".")) continue;
      data.set(relativePath, content);
    } catch {
      console.log(`  Warning: could not read ${filePath}`);
    }
  }

  return data;
}

async function pushData(boxName: string, data: Map<string, string>) {
  const box = await getBoxByName(boxName);

  // Ensure data directory exists
  console.log(`  Creating data directories...`);
  await box.exec.command("mkdir -p /workspace/home/data/history");

  for (const [relativePath, content] of data) {
    const destPath = `${DATA_DIR}/${relativePath}`;
    console.log(`    writing ${destPath} (${content.length} bytes)`);
    await box.files.write({ path: destPath, content });
  }
}

async function main() {
  const mode = process.argv[2];

  if (mode === "pull") {
    console.log("=== Pulling data from v2 boxes ===\n");

    for (const agent of V2_AGENTS) {
      console.log(`Pulling ${agent.name} (${agent.boxName})...`);
      const data = await pullData(agent.boxName);
      console.log(`  ${data.size} files pulled`);

      for (const [path] of data) {
        console.log(`    ${path}`);
      }
    }

    console.log("\nPull complete. Run with 'migrate' to copy data to v10 boxes.");
    return;
  }

  if (mode === "migrate") {
    console.log("=== Migrating data from v2 → v10 ===\n");

    for (let i = 0; i < V2_AGENTS.length; i++) {
      const src = V2_AGENTS[i];
      const dst = V10_AGENTS[i];

      console.log(`\n[${src.name}] ${src.boxName} → ${dst.boxName}`);

      console.log(`  Pulling from ${src.boxName}...`);
      const data = await pullData(src.boxName);
      console.log(`  ${data.size} files pulled`);

      console.log(`  Pushing to ${dst.boxName}...`);
      await pushData(dst.boxName, data);
      console.log(`  ${data.size} files pushed`);

      console.log(`  Done.`);
    }

    console.log("\n=== Migration complete ===");
    return;
  }

  console.log("Usage:");
  console.log("  tsx migrate-v3.ts pull      # Preview: list files in v2 boxes");
  console.log("  tsx migrate-v3.ts migrate   # Copy data from v2 boxes to v10 boxes");
  console.log("");
  console.log("Before running 'migrate', create v10 boxes with: ahi apply");
}

main().catch((err) => {
  console.error("Migration failed:", err);
  process.exit(1);
});
