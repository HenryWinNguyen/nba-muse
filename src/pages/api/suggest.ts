import type { NextApiRequest, NextApiResponse } from "next";
import { createClient } from "@libsql/client";

const db = createClient({
  url: process.env.TURSO_DATABASE_URL as string,
  authToken: process.env.TURSO_AUTH_TOKEN as string,
});

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    const q = String(req.query.player || "").trim().toLowerCase();
    if (!q) { res.status(200).json({ players: [], ideas: [] }); return; }

    const r = await db.execute({
      sql: `SELECT player_name FROM players
            WHERE lower(player_name) LIKE ?
            ORDER BY player_name
            LIMIT 8`,
      args: [`%${q}%`],
    });
    const players = r.rows.map((row: any) => String(row.player_name));
    const top = players[0];

    const ideas = top ? [
      `${top} career playoffs`,
      `${top} vs BOS last 10`,
      `${top} vs GSW in 2016 playoffs`,
      `${top} since 2018`,
    ] : [];

    res.status(200).json({ players, ideas });
  } catch (e: any) {
    res.status(500).json({ players: [], ideas: [], error: e?.message ?? "suggest failed" });
  }
}
