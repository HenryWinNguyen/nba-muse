import type { NextApiRequest, NextApiResponse } from "next";
import { createClient } from "@libsql/client";

const db = createClient({
  url: process.env.TURSO_DATABASE_URL as string,
  authToken: process.env.TURSO_AUTH_TOKEN as string,
});

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;
    const sql = body?.sql as string;
    const params = (body?.params ?? []) as any[];

    if (!sql) {
      return res.status(400).json({ error: "Missing sql" });
    }

    const result = await db.execute({ sql, args: params });
    return res.status(200).json({ rows: result.rows });
  } catch (e: any) {
    return res.status(500).json({ error: e?.message ?? "query failed" });
  }
}
