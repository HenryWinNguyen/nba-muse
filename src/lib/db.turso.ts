import { createClient } from "@libsql/client";

const db = createClient({
  url: process.env.TURSO_DATABASE_URL as string,
  authToken: process.env.TURSO_AUTH_TOKEN as string,
});

// Convert named params (:name / @name / $name) to positional "?" + args[]
function bindNamed(sql: string, params?: Record<string, any> | any[]) {
  if (!params || Array.isArray(params)) return { sql, args: params ?? [] };
  const names: string[] = [];
  const sql2 = sql.replace(/[@:$]([a-zA-Z_][a-zA-Z0-9_]*)/g, (_m, name) => {
    names.push(name);
    return "?";
  });
  const args = names.map((n) => (params as Record<string, any>)[n]);
  return { sql: sql2, args };
}

export async function sqlGet<T = any>(sql: string, params?: Record<string, any> | any[]) {
  const { sql: s, args } = bindNamed(sql, params);
  const r = await db.execute({ sql: s, args });
  return (r.rows[0] as T) ?? undefined;
}

export async function sqlAll<T = any>(sql: string, params?: Record<string, any> | any[]) {
  const { sql: s, args } = bindNamed(sql, params);
  const r = await db.execute({ sql: s, args });
  return r.rows as unknown as T[];
}
