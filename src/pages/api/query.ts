import type { NextApiRequest, NextApiResponse } from "next";
// NOTE: use a RELATIVE import so we don't rely on path aliases:
import { runQuery } from "../../scripts/lib/runQuery";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    const text = String(req.query.text ?? (typeof req.body === "string" ? req.body : req.body?.text) ?? "").trim();
    if (!text) return res.status(400).send("Missing text");
    const out = await runQuery(text);
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    return res.status(200).send(out);
  } catch (e: any) {
    return res.status(500).send(e?.message ?? "query failed");
  }
}
