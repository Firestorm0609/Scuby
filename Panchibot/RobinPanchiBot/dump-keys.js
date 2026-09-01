// Run once on your VPS, in the bot's directory (needs .env + data/panchi.sqlite present):
//   node dump-keys.js
// Prints uid, wallet name, address, and decrypted private key for every wallet.
// Delete this file after use — do not leave it on the server.
import 'dotenv/config';
import Database from 'better-sqlite3';
import path from 'path';
import { decrypt } from './crypto.js';

const DB_PATH = path.join(process.cwd(), 'data', 'panchi.sqlite');
const db = new Database(DB_PATH, { readonly: true });

const rows = db.prepare('SELECT uid, id, name, address, private_key FROM wallets').all();

if (rows.length === 0) {
  console.log('No wallets found.');
  process.exit(0);
}

for (const w of rows) {
  let pk;
  try {
    pk = decrypt(w.private_key);
  } catch (err) {
    pk = `DECRYPT_FAILED: ${err.message}`;
  }
  console.log('----------------------------------------');
  console.log(`uid:        ${w.uid}`);
  console.log(`wallet id:  ${w.id}`);
  console.log(`name:       ${w.name}`);
  console.log(`address:    ${w.address}`);
  console.log(`privateKey: ${pk}`);
}
console.log('----------------------------------------');
console.log(`Total: ${rows.length} wallet(s). Delete this script now.`);
