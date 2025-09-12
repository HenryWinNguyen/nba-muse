import { useEffect, useState } from "react";
import { runSQL } from "../lib/sql";

export default function Home() {
  const [rows, setRows] = useState<any[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      try {
        // Adjust the table name if needed
        const r = await runSQL("SELECT * FROM players LIMIT 10");
        setRows(r);
      } catch (e: any) {
        setError(e?.message ?? "query failed");
      }
    })();
  }, []);

  const cols = rows && rows.length ? Object.keys(rows[0]) : [];

  return (
    <main style={{ padding: 24, fontFamily: "ui-sans-serif, system-ui" }}>
      <h1 style={{ fontSize: 28, marginBottom: 8 }}>NBA Muse</h1>
      <p style={{ marginBottom: 16, color: "#555" }}>
        Live data via Turso. Showing first 10 rows from <code>players</code>.
      </p>

      {error && <pre style={{ color: "#b91c1c" }}>{error}</pre>}

      {!rows && !error && <p>Loading…</p>}

      {rows && rows.length > 0 && (
        <div style={{ overflowX: "auto", border: "1px solid #e5e7eb", borderRadius: 10 }}>
          <table style={{ borderCollapse: "collapse", width: "100%" }}>
            <thead>
              <tr>
                {cols.map((c) => (
                  <th key={c} style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #e5e7eb" }}>
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={i} style={{ background: i % 2 ? "#fafafa" : "#fff" }}>
                  {cols.map((c) => (
                    <td key={c} style={{ padding: 8, borderBottom: "1px solid #f1f5f9", whiteSpace: "nowrap" }}>
                      {String(r[c])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </main>
  );
}
