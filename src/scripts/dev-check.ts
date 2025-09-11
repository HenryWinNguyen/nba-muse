// src/scripts/dev-check.ts
import duckdb from 'duckdb';
import { promisify } from 'node:util';

const db = new duckdb.Database('data/warehouse/warehouse.duckdb');
const conn = db.connect();

// Promisify the callback-style API so we can await it
const all = promisify(conn.all.bind(conn));

(async () => {
  try {
    const rows = await all('SELECT 1 AS ok');
    console.log(rows); // should log: [ { ok: 1 } ]
  } finally {
    conn.close();
    db.close();
  }
})();
