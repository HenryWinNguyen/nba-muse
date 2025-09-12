export async function runSQL<T = any>(sql: string, params: any[] = []): Promise<T[]> {
    const r = await fetch("/api/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql, params })
    });
    const j = await r.json();
    if (!r.ok) throw new Error(j.error || "query failed");
    return j.rows as T[];
  }
  