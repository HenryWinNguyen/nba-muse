// src/scripts/query.ts
import { runQuery } from './lib/runQuery'; 
function main() {
  const text = process.argv.slice(2).join(' ').trim();
  if (!text) {
    console.log('Usage: npm run query -- "<your query>"');
    process.exit(1);
  }
  try {
    const out = runQuery(text);
    console.log('\n' + out + '\n');
  } catch (e: any) {
    console.error(e?.message ?? String(e));
    process.exit(1);
  }
}

main();
