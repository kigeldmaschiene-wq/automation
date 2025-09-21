// /api/db.js
import { Pool } from 'pg';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

export default async function handler(req, res) {
  try {
    if (req.method !== 'POST') {
      return res.status(405).json({ error: 'Only POST allowed' });
    }

    const { action, table, values, where, order, bulk } = req.body || {};
    if (!table) return res.status(400).json({ error: 'Missing table' });

    const client = await pool.connect();
    try {
      // SELECT
      if (action === 'select') {
        const clauses = [];
        const params = [];

        if (where && typeof where === 'object') {
          const w = Object.entries(where).map(([k, v], i) => {
            params.push(v);
            return `"${k}" = $${params.length}`;
          }).join(' AND ');
          if (w) clauses.push('WHERE ' + w);
        }

        if (order && order.column) {
          clauses.push(`ORDER BY "${order.column}" ${order.dir?.toUpperCase() === 'DESC' ? 'DESC' : 'ASC'}`);
        }

        const q = `SELECT * FROM ${table} ${clauses.join(' ')}`;
        const r = await client.query(q, params);
        return res.json({ data: r.rows });
      }

      // INSERT
      if (action === 'insert') {
        if (!values) return res.status(400).json({ error: 'Missing values' });
        if (bulk && Array.isArray(values)) {
          const cols = Object.keys(values[0]);
          const rows = values.map(v => cols.map(c => v[c] ?? null));
          const placeholders = rows.map((r, i) =>
            '(' + r.map((_, j) => `$${i * cols.length + j + 1}`).join(',') + ')'
          ).join(',');
          const flat = rows.flat();
          const q = `INSERT INTO ${table} ("${cols.join('","')}") VALUES ${placeholders} RETURNING *`;
          const r = await client.query(q, flat);
          return res.json({ data: r.rows });
        } else {
          const cols = Object.keys(values);
          const params = cols.map((_, i) => `$${i + 1}`);
          const q = `INSERT INTO ${table} ("${cols.join('","')}") VALUES (${params.join(',')}) RETURNING *`;
          const r = await client.query(q, cols.map(c => values[c] ?? null));
          return res.json({ data: r.rows[0] });
        }
      }

      // UPDATE
      if (action === 'update') {
        if (!values || !where) return res.status(400).json({ error: 'Missing values/where' });
        const setCols = Object.keys(values);
        const set = setCols.map((k, i) => `"${k}"=$${i + 1}`).join(',');
        const setVals = setCols.map(k => values[k] ?? null);

        const wCols = Object.keys(where);
        const w = wCols.map((k, i) => `"${k}"=$${setCols.length + i + 1}`).join(' AND ');
        const wVals = wCols.map(k => where[k]);

        const q = `UPDATE ${table} SET ${set} WHERE ${w} RETURNING *`;
        const r = await client.query(q, [...setVals, ...wVals]);
        return res.json({ data: r.rows[0] });
      }

      // DELETE
      if (action === 'delete') {
        if (!where) return res.status(400).json({ error: 'Missing where' });
        const wCols = Object.keys(where);
        const w = wCols.map((k, i) => `"${k}"=$${i + 1}`).join(' AND ');
        const q = `DELETE FROM ${table} WHERE ${w}`;
        await client.query(q, wCols.map(k => where[k]));
        return res.json({ ok: true });
      }

      return res.status(400).json({ error: 'Unknown action' });
    } finally {
      client.release();
    }
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e.message || e) });
  }
}
