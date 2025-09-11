// src/scripts/server.ts
import express from 'express';
import cors from 'cors';
import path from 'node:path';
import Database from 'better-sqlite3';
import { runQuery, runGames } from './lib/runQuery';

const app = express();
app.use(cors());

// Serve the static demo
const publicDir = path.resolve(__dirname, '../public');
app.use(express.static(publicDir));

// Summary text
app.get('/api/query', (req, res) => {
  const text = (req.query.text as string) || '';
  try {
    const result = runQuery(text);
    res.type('text/plain').send(result);
  } catch (e: any) {
    res.status(400).send(e?.message ?? 'Bad request');
  }
});

// Per-game table
app.get('/api/games', (req, res) => {
  const text = (req.query.text as string) || '';
  const limit = Number(req.query.limit ?? 25);
  try {
    const data = runGames(text, Math.max(1, Math.min(200, limit)));
    res.json(data);
  } catch (e: any) {
    res.status(400).json({ error: e?.message ?? 'Bad request' });
  }
});

// Player autocomplete
app.get('/api/suggest', (req, res) => {
  const q = String(req.query.player || '').trim().toLowerCase();
  if (!q) return res.json({ players: [], ideas: [] });

  const db = new Database('data/serving/nba.sqlite', { readonly: true });
  try {
    const players = db.prepare(
      `SELECT player_name FROM players
       WHERE lower(player_name) LIKE ?
       ORDER BY player_name
       LIMIT 8`
    ).all(`%${q}%`) as { player_name: string }[];

    // quick ideas based on top match
    const top = players[0]?.player_name;
    const ideas = top ? [
      `${top} career playoffs`,
      `${top} vs BOS last 10`,
      `${top} vs GSW in 2016 playoffs`,
      `${top} since 2018`,
    ] : [];

    res.json({ players: players.map(p => p.player_name), ideas });
  } catch (e: any) {
    res.status(400).json({ error: e?.message ?? 'Bad request' });
  } finally {
    db.close();
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API running on http://localhost:${PORT}`);
});
