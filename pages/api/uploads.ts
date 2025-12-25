// pages/api/uploads.ts
import type { NextApiRequest, NextApiResponse } from 'next';
import fs from 'fs/promises';
import path from 'path';

const MAX_BYTES = 10 * 1024 * 1024; // 10 MB

export const config = {
  api: {
    // allow larger JSON bodies since uploads are base64-encoded
    bodyParser: {
      sizeLimit: '12mb',
    },
  },
};

function sanitizeFileName(name: string) {
  // remove directory separators and suspicious chars, keep a simple safe subset
  return name.replace(/[^a-zA-Z0-9._-]/g, '_').replace(/^_+/, '').substring(0, 200) || 'file';
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const body = req.body;
    if (!body || typeof body !== 'object') {
      return res.status(400).json({ error: 'Missing JSON body' });
    }

    const { filename, content, contentType } = body as { filename?: string; content?: string; contentType?: string };

    if (!filename || typeof filename !== 'string') {
      return res.status(400).json({ error: 'filename required' });
    }
    if (!content || typeof content !== 'string') {
      return res.status(400).json({ error: 'content (base64) required' });
    }

    const safeName = sanitizeFileName(filename);
    // strip possible dataURI prefix if present
    const base64 = content.replace(/^data:[^;]+;base64,/, '').trim();
    const estimatedBytes = Math.floor((base64.length * 3) / 4);

    if (estimatedBytes > MAX_BYTES) {
      return res.status(413).json({ error: `Attachment too large (max ${MAX_BYTES} bytes)` });
    }

    // decode
    let buffer: Buffer;
    try {
      buffer = Buffer.from(base64, 'base64');
    } catch (e) {
      return res.status(400).json({ error: 'Invalid base64 content' });
    }

    const uploadsDir = path.join(process.cwd(), 'public', 'uploads');
    await fs.mkdir(uploadsDir, { recursive: true });

    const ts = Date.now();
    const outName = `${ts}-${safeName}`;
    const outPath = path.join(uploadsDir, outName);

    await fs.writeFile(outPath, buffer, { mode: 0o644 });

    const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || '').replace(/\/$/, '');
    const url = PUBLIC_BASE_URL ? `${PUBLIC_BASE_URL}/uploads/${encodeURIComponent(outName)}` : `/uploads/${encodeURIComponent(outName)}`;

    return res.status(201).json({
      url,
      path: `/uploads/${outName}`,
      filename: outName,
      contentType: contentType || null,
      size: buffer.length,
    });
  } catch (err: any) {
    console.error('Upload error', err);
    // return a helpful message (no stacktrace)
    return res.status(500).json({ error: 'Internal server error during upload' });
  }
}
