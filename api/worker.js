// /api/worker.js — HeyGen V2 Generate + V1 Status (READY mit file_url)
import { Pool } from 'pg';
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function qAll(c, sql, p = []) { const r = await c.query(sql, p); return r.rows; }
async function getHeyGen(c) {
  const [x] = await qAll(c, `select * from integrations where service='heygen' order by created_at desc limit 1`);
  if (!x) throw new Error('Keine HeyGen-Integration gefunden');
  return {
    apiKey: x.api_key,
    base: (x.base_url || 'https://api.heygen.com').replace(/\/$/, ''),
    avatarId: x.presenter_id,             // HeyGen Avatar-ID
    voiceId: x.voice_id || 'de-DE-Neural2-D' // HeyGen Voice-ID
  };
}

// V2: Create video
async function heygenCreate({ apiKey, base, avatarId, voiceId, text, captionsOn }) {
  const body = {
    caption: !!captionsOn,                      // eingebrannte Captions
    dimension: { width: 1080, height: 1920 },  // 9:16
    video_inputs: [{
      character: { type: 'avatar', avatar_id: avatarId },
      voice: { type: 'text', voice_id: voiceId, input_text: text },
      background: { type: 'color', value: '#000000' }
    }]
  };
  const r = await fetch(`${base}/v2/video/generate`, {
    method: 'POST',
    headers: { 'X-Api-Key': apiKey, 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!r.ok) throw new Error(`HeyGen start failed: ${r.status} ${await r.text()}`);
  const j = await r.json();
  const video_id = j?.data?.video_id || j?.video_id || j?.id;
  if (!video_id) throw new Error('Kein video_id erhalten');
  return video_id;
}

// V1: Poll status
async function heygenStatus({ apiKey, base, video_id }) {
  for (let i = 0; i < 60; i++) {
    const r = await fetch(`${base}/v1/video_status.get?video_id=${encodeURIComponent(video_id)}`, {
      headers: { 'X-Api-Key': apiKey }
    });
    const j = await r.json().catch(() => ({}));
    const d = j?.data || {};
    if (d.status === 'completed' && (d.video_url_caption || d.video_url)) {
      return d.video_url_caption || d.video_url; // bevorzugt: mit Captions
    }
    if (d.status === 'failed') throw new Error(d?.error?.message || 'HeyGen job failed');
    await new Promise(s => setTimeout(s, 5000));
  }
  throw new Error('HeyGen timeout');
}

export default async function handler(req, res) {
  if (req.method !== 'GET' && req.method !== 'POST') return res.status(405).json({ error: 'Use GET/POST' });
  const c = await pool.connect();
  try {
    const now = new Date().toISOString();

    // Jobs: QUEUED zuerst, dann fällige INIT
    const jobs = await qAll(c, `
      SELECT * FROM videos
      WHERE (
        status='QUEUED'
        OR (status='INIT' AND (scheduled_at IS NULL OR scheduled_at <= $1))
      )
      AND (render_service='heygen' OR render_service IS NULL)
      ORDER BY (status='QUEUED') DESC, scheduled_at NULLS FIRST
      LIMIT 3
    `, [now]);

    if (!jobs.length) return res.json({ ok: true, processed: 0 });

    const hg = await getHeyGen(c);
    let processed = 0;

    for (const v of jobs) {
      try {
        await c.query(`update videos set status='RENDERING' where id=$1`, [v.id]);

        // Hook in die ersten Sekunden: Hook + Script zusammengeben (Captions zeigen beides)
        const text = [v.hook, v.script].filter(Boolean).join('\n\n');

        const video_id = await heygenCreate({
          apiKey: hg.apiKey,
          base: hg.base,
          avatarId: hg.avatarId,
          voiceId: hg.voiceId,
          text,
          captionsOn: v.captions_on ?? true
        });

        const fileUrl = await heygenStatus({ apiKey: hg.apiKey, base: hg.base, video_id });

        await c.query(
          `update videos set status='READY', file_url=$1, render_id=$2, link='' where id=$3`,
          [fileUrl, video_id, v.id]
        );
        processed++;
      } catch (e) {
        await c.query(`update videos set status='ERROR', link=$1 where id=$2`, [String(e.message || e), v.id]);
      }
    }

    res.json({ ok: true, processed });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  } finally {
    await c.release();
  }
}
