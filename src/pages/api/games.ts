import type { NextApiRequest, NextApiResponse } from "next";
import { runGames } from "../../scripts/lib/runQuery";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    const text = (req.query.text as string) || "";
    const limit = Math.max(1, Math.min(400, Number(req.query.limit ?? 25)));
    if (!text) { res.status(400).json({ rows: [], error: "Missing text" }); return; }

    const data = await runGames(text, limit); // same logic as localhost
    res.status(200).json({ rows: data.rows }); // front-end only reads .rows
  } catch (e: any) {
    res.status(500).json({ rows: [], error: e?.message ?? "games failed" });
  }
}
