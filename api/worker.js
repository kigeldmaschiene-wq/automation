// /api/worker.js – HeyGen Worker (QUEUED → RENDERING → READY)
import { Pool } from 'pg';
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function fetchAll(client, q, params = []) {
  const r = await client.query(q, params);
  return r.rows;
}

async function getHeyGenIntegration(client) {
  const r = await fetchAll(
    client,
    `SELECT * FROM integrations WHERE service='heygen' ORDER BY created_at DESC LIMIT 1`
  );
  if (!r.length) throw new Error('Keine HeyGen-Integration gefunden');
  return {
    apiKey: r[0].api_key,
    base: (r[0].base_url || 'https://api.heygen.com').replace(/\/$/, ''),
    avatarId: r[0].presenter_id,
    voiceId: r[0].voice_id || 'de-DE-Neural2-D',
  };
}

async function startHeyGenJob({ apiKey, base, avatarId, voiceId, text, captionsOn, hookText, hookOn, hookPos }) {
  const overlays = hookOn
    ? [{ text: hookText || '', start: 0, end: 3, position: hookPos || 'top' }]
    : [];

  const payload = {
    input_text: text,
    avatar_id: avatarId,
    voice: voiceId,
    background: '#000000',
    caption: !!captionsOn,
    overlays,
  };

  // Haupt-Pfad
  let res = await fetch(`${base}/v1/video.generate`, {
    method: 'POST',
    headers: { 'X-Api-Key': apiKey, 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  // Fallback-Pfad
  if (res.status === 404) {
    res = await fetch(`${base}/v1/videos/generate`, {
      method: 'POST',
      headers: { 'X-Api-Key': apiKey, 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
  }

  if (!res.ok) {
    const t = await res.text();
    throw new Error(`HeyGen start failed: ${res.status} ${t}`);
  }
  return res.json(); // { video_id | task_id }
}

async function pollHeyGen({ apiKey, base, video_id }) {
  for (let i = 0; i < 60; i++) {
    const r = await fetch(`${base}/v1/video.status?video_id=${encodeURIComponent(video_id)}`, {
      headers: { 'X-Api-Key': apiKey },
    });
    const j = await r.json().catch(() => ({}));
    if (j?.status === 'completed' && j?.video_url) return j.video_url;
    if (j?.status === 'failed') throw new Error('HeyGen job failed');
    await new Promise((s) => setTimeout(s, 5000));
  }
  throw new Error('HeyGen timeout');
}

export default async function handler(req, res) {
  if (req.method !== 'GET' && req.method !== 'POST') {
    return res.status(405).json({ error: 'Use GET/POST' });
  }

  const client = await pool.connect();
  try {
    const now = new Date().toISOString();

    // Jobs abholen
    const jobs = await fetchAll(
      client,
      `
      SELECT * FROM videos
      WHERE (
        status='QUEUED'
        OR (status='INIT' AND (scheduled_at IS NULL OR scheduled_at <= $1))
      )
      AND (render_service='heygen' OR render_service IS NULL)
      ORDER BY (status='QUEUED') DESC, scheduled_at NULLS FIRST
      LIMIT 3
    `,
      [now]
    );

    if (!jobs.length) return res.json({ ok: true, processed: 0 });

    const integ = await getHeyGenIntegration(client);
    let processed = 0;

    for (const v of jobs) {
      try {
        await client.query(`UPDATE videos SET status='RENDERING' WHERE id=$1`, [v.id]);

        const start = await startHeyGenJob({
          apiKey: integ.apiKey,
          base: integ.base,
          avatarId: integ.avatarId,
          voiceId: integ.voiceId,
          text: v.script || v.hook || '',
          captionsOn: v.captions_on ?? true,
          hookText: v.hook || '',
          hookOn: v.hook_overlay_on ?? true,
          hookPos: v.hook_pos || 'top',
        });

        const videoId = start.video_id || start.task_id;
        if (!videoId) throw new Error('Kein video_id erhalten');

        const url = await pollHeyGen({ apiKey: integ.apiKey, base: integ.base, video_id: videoId });

        await client.query(
          `UPDATE videos SET status='READY', file_url=$1, render_id=$2, link='' WHERE id=$3`,
          [url, videoId, v.id]
        );
        processed++;
      } catch (e) {
        await client.query(`UPDATE videos SET status='ERROR', link=$1 WHERE id=$2`, [
          String(e.message || e),
          v.id,
        ]);
      }
    }

    return res.json({ ok: true, processed });
  } catch (e) {
    return res.status(500).json({ error: String(e.message || e) });
  } finally {
    await client.release();
  }
}
