// src/scripts/etl.ts
import duckdb from 'duckdb';
import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';

// Paths
const WAREHOUSE = 'data/warehouse/warehouse.duckdb';
const RAW_DIR = path.join('data', 'raw');
const RAW_GLOB = path.join(RAW_DIR, '*.csv');
const SQLITE_OUT = 'data/serving/nba.sqlite';

// Ensure directories exist
function ensureDirs() {
  for (const d of ['data', 'data/warehouse', 'data/serving', RAW_DIR]) {
    if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
  }
}

// Promise helpers around DuckDB’s callback API (typed)
const promisifyRun = (conn: any) => (sql: string) =>
  new Promise<void>((resolve, reject) => conn.run(sql, (err: any) => (err ? reject(err) : resolve())));

const promisifyAll = (conn: any) => <T = any>(sql: string) =>
  new Promise<T[]>((resolve, reject) =>
    conn.all(sql, (err: any, rows: any[]) => (err ? reject(err) : resolve(rows as T[])))
  );

async function main() {
  ensureDirs();

  // 0) Check for CSVs
  const files = fs.readdirSync(RAW_DIR).filter((f) => f.toLowerCase().endsWith('.csv'));
  if (files.length === 0) {
    console.log('No CSVs in data/raw. Put your CSVs there, then run: npm run etl');
    return;
  }
  console.log(`Found ${files.length} CSV(s):`, files.join(', '));

  // 1) Open DuckDB + helpers
  const db = new duckdb.Database(WAREHOUSE);
  const conn = db.connect();
  const run = promisifyRun(conn);
  const all = promisifyAll(conn);

  // 2) Stage raw
  await run(`
    CREATE OR REPLACE TABLE stg_box AS
    SELECT * FROM read_csv_auto('${RAW_GLOB}', HEADER=TRUE, UNION_BY_NAME=TRUE);
  `);

  // 3) Normalize & derive
  // Your headers seen: gameid, date, type, teamid, team, home, away, MIN, PTS, FGM, FGA, FG%, 3PM, 3PA, 3P%, FTM, FTA, FT%, OREB, DREB, REB, AST, TOV, STL, BLK, PF, +/- , win, season, playerid, player
  // We’ll:
  //   - keep player rows only (playerid not null) for box scores
  //   - derive opponent_abbr (if team == home then opponent = away else team == away -> home)
  //   - create clean game + team + player dims

  await run(`
    CREATE OR REPLACE VIEW v_player_rows AS
    SELECT
      CAST(gameid AS VARCHAR)                          AS game_id,
      CAST(playerid AS BIGINT)                         AS player_id,
      player                                           AS player_name,
      team                                             AS team_abbr,
      home, away,
      CAST(date AS DATE)                               AS game_date,
      CAST(season AS INTEGER)                          AS season,
      type                                             AS season_type,
      MIN, PTS, FGM, FGA, "FG%" AS FG_PCT,
      "3PM" AS FG3M, "3PA" AS FG3A, "3P%" AS FG3_PCT,
      FTM, FTA, "FT%" AS FT_PCT,
      OREB, DREB, REB, AST, TOV, STL, BLK, PF, "+/-" AS PLUS_MINUS, win
    FROM stg_box
    WHERE playerid IS NOT NULL
  `);

  await run(`
    CREATE OR REPLACE VIEW v_box_enriched AS
    SELECT
      *,
      CASE
        WHEN team_abbr = home THEN away
        WHEN team_abbr = away THEN home
        ELSE NULL
      END AS opponent_abbr
    FROM v_player_rows
  `);

  // 4) Build serving tables in DuckDB first (drop/recreate)
  await run(`DROP TABLE IF EXISTS players`);
  await run(`DROP TABLE IF EXISTS teams`);
  await run(`DROP TABLE IF EXISTS games`);
  await run(`DROP TABLE IF EXISTS box_scores`);

  await run(`
    CREATE TABLE players AS
    SELECT DISTINCT player_id, player_name
    FROM v_box_enriched
    WHERE player_id IS NOT NULL AND player_name IS NOT NULL
  `);

  await run(`
    CREATE TABLE teams AS
    SELECT DISTINCT team_abbr AS abbr FROM v_box_enriched WHERE team_abbr IS NOT NULL
    UNION
    SELECT DISTINCT home FROM v_box_enriched WHERE home IS NOT NULL
    UNION
    SELECT DISTINCT away FROM v_box_enriched WHERE away IS NOT NULL
    UNION
    SELECT DISTINCT opponent_abbr FROM v_box_enriched WHERE opponent_abbr IS NOT NULL
  `);

  await run(`
    CREATE TABLE games AS
    SELECT
      game_id,
      MIN(game_date) AS game_date,
      ANY_VALUE(season) AS season,
      ANY_VALUE(season_type) AS season_type,
      -- The 'home' / 'away' columns are per-row but should be consistent per game
      ANY_VALUE(home) AS home_team_abbr,
      ANY_VALUE(away) AS away_team_abbr
    FROM v_box_enriched
    GROUP BY game_id
  `);

  await run(`
    CREATE TABLE box_scores AS
    SELECT
      game_id,
      player_id,
      player_name,
      team_abbr,
      opponent_abbr,
      game_date,
      season,
      season_type,
      MIN, PTS, FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT,
      OREB, DREB, REB, AST, STL, BLK, TOV, PF, PLUS_MINUS, win
    FROM v_box_enriched
  `);

  // Sanity prints
  const [pc] = await all<{ c: number }>(`SELECT COUNT(*) AS c FROM players`);
  const [tc] = await all<{ c: number }>(`SELECT COUNT(*) AS c FROM teams`);
  const [gc] = await all<{ c: number }>(`SELECT COUNT(*) AS c FROM games`);
  const [bc] = await all<{ c: number }>(`SELECT COUNT(*) AS c FROM box_scores`);
  console.log('\nCounts in DuckDB =>', { players: pc.c, teams: tc.c, games: gc.c, box_scores: bc.c });

  // 5) Export to SQLite for easy distribution
  if (fs.existsSync(SQLITE_OUT)) fs.unlinkSync(SQLITE_OUT);
  const sqlite = new Database(SQLITE_OUT);
  sqlite.pragma('journal_mode = WAL');

  sqlite.exec(`
    CREATE TABLE players(
      player_id INTEGER PRIMARY KEY,
      player_name TEXT NOT NULL
    );
    CREATE TABLE teams(
      abbr TEXT PRIMARY KEY
    );
    CREATE TABLE games(
      game_id TEXT PRIMARY KEY,
      game_date TEXT,
      season INTEGER,
      season_type TEXT,
      home_team_abbr TEXT,
      away_team_abbr TEXT
    );
    CREATE TABLE box_scores(
      game_id TEXT,
      player_id INTEGER,
      player_name TEXT,
      team_abbr TEXT,
      opponent_abbr TEXT,
      game_date TEXT,
      season INTEGER,
      season_type TEXT,
      MIN REAL,
      PTS REAL,
      FGM REAL, FGA REAL, FG_PCT REAL,
      FG3M REAL, FG3A REAL, FG3_PCT REAL,
      FTM REAL, FTA REAL, FT_PCT REAL,
      OREB REAL, DREB REAL, REB REAL,
      AST REAL, STL REAL, BLK REAL, TOV REAL,
      PF REAL, PLUS_MINUS REAL, win INTEGER
    );
    CREATE INDEX idx_box_player_date ON box_scores(player_id, game_date DESC);
    CREATE INDEX idx_box_player_opp_date ON box_scores(player_id, opponent_abbr, game_date DESC);
  `);

  // Helper to stream DuckDB -> SQLite in chunks, with date casting for SQLite
const copy = async <T extends object>(tab: 'players' | 'teams' | 'games' | 'box_scores', insertSQL: string) => {
  const [{ c }] = await all<{ c: bigint }>(`SELECT COUNT(*) AS c FROM ${tab}`);
  const total = Number(c);

  // Build a SELECT that casts game_date to VARCHAR where needed
  const baseSelect =
    tab === 'games'
      ? `SELECT game_id,
               CAST(game_date AS VARCHAR) AS game_date,
               season, season_type, home_team_abbr, away_team_abbr
         FROM games`
      : tab === 'box_scores'
      ? `SELECT game_id, player_id, player_name, team_abbr, opponent_abbr,
               CAST(game_date AS VARCHAR) AS game_date,
               season, season_type,
               MIN, PTS, FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT,
               FTM, FTA, FT_PCT,
               OREB, DREB, REB, AST, STL, BLK, TOV, PF, PLUS_MINUS, win
         FROM box_scores`
      : `SELECT * FROM ${tab}`;

  console.log(`Copy ${tab}: ${total} rows`);
  const CHUNK = 20_000;

  const tx = sqlite.transaction((rows: T[]) => {
    const stmt = sqlite.prepare(insertSQL);
    for (const r of rows) stmt.run(r as any);
  });

  for (let off = 0; off < total; off += CHUNK) {
    const rows = await all<T>(`SELECT * FROM (${baseSelect}) AS t LIMIT ${CHUNK} OFFSET ${off}`);
    tx(rows);
    process.stdout.write(`  … ${Math.min(off + CHUNK, total)}/${total}\r`);
  }
  process.stdout.write('\n');
};


  await copy('players', 'INSERT INTO players (player_id, player_name) VALUES (@player_id, @player_name)');
  await copy('teams',   'INSERT INTO teams (abbr) VALUES (@abbr)');
  await copy('games',   'INSERT INTO games (game_id, game_date, season, season_type, home_team_abbr, away_team_abbr) VALUES (@game_id, @game_date, @season, @season_type, @home_team_abbr, @away_team_abbr)');
  await copy('box_scores', `INSERT INTO box_scores
    (game_id, player_id, player_name, team_abbr, opponent_abbr, game_date, season, season_type,
     MIN, PTS, FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT,
     OREB, DREB, REB, AST, STL, BLK, TOV, PF, PLUS_MINUS, win)
    VALUES
    (@game_id, @player_id, @player_name, @team_abbr, @opponent_abbr, @game_date, @season, @season_type,
     @MIN, @PTS, @FGM, @FGA, @FG_PCT, @FG3M, @FG3A, @FG3_PCT, @FTM, @FTA, @FT_PCT,
     @OREB, @DREB, @REB, @AST, @STL, @BLK, @TOV, @PF, @PLUS_MINUS, @win)`);

  sqlite.close();
  conn.close();
  db.close();

  console.log(`\nExported to ${SQLITE_OUT} ✅`);
}

main().catch((err) => {
  console.error('ETL failed:', err);
  process.exit(1);
});
