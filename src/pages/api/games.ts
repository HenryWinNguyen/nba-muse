import type { NextApiRequest, NextApiResponse } from "next";
import { runGames } from "../../scripts/lib/runQuery";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    const text = String(req.query.text ?? (typeof req.body === "string" ? req.body : req.body?.text) ?? "").trim();
    const limit = Math.max(1, Math.min(400, Number(req.query.limit ?? (typeof req.body === "string" ? undefined : req.body?.limit) ?? 25)));
    if (!text) return res.status(400).json({ error: "Missing text" });

    const data = await runGames(text, limit);
    return res.status(200).json(data);
  } catch (e: any) {
    return res.status(500).json({ error: e?.message ?? "games failed" });
  }
}
