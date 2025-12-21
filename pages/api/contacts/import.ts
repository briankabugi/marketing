import type { NextApiRequest, NextApiResponse } from 'next';
import formidable, { File } from 'formidable';
import * as XLSX from 'xlsx';
import fs from 'fs';
import path from 'path';

export const config = {
  api: { bodyParser: false },
};

function normalizeHeader(key: string) {
  return key.replace(/^\uFEFF/, '').trim().toLowerCase();
}

export default function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method Not Allowed' });
  }

  const form = formidable({ multiples: false, keepExtensions: true });

  form.parse(req, (err, _fields, files) => {
    try {
      if (err) {
        return res.status(500).json({ error: err.message });
      }

      let file = files.file as File | File[] | undefined;
      if (Array.isArray(file)) file = file[0];

      if (!file || !file.filepath) {
        return res.status(400).json({
          error: 'Invalid file upload (missing filepath)',
        });
      }

      const originalName = file.originalFilename ?? '';
      const ext =
        path.extname(originalName).toLowerCase() ||
        (file.mimetype?.includes('csv')
          ? '.csv'
          : file.mimetype?.includes('excel')
          ? '.xlsx'
          : '');

      if (!['.csv', '.xls', '.xlsx'].includes(ext)) {
        return res.status(400).json({
          error: 'Unsupported file type',
          debug: {
            originalFilename: file.originalFilename,
            mimetype: file.mimetype,
          },
        });
      }

      const buffer = fs.readFileSync(file.filepath);

      const workbook = XLSX.read(buffer, {
        type: 'buffer',
        raw: true,
      });

      const sheet = workbook.Sheets[workbook.SheetNames[0]];
      const rows = XLSX.utils.sheet_to_json<Record<string, any>>(sheet, {
        defval: '',
      });

      const FIELD_MAP: Record<string, string> = {
        name: 'name',
        email: 'email',
        phone: 'phone',
        whatsapp: 'whatsapp',
        location: 'location',
        segments: 'segments',
      };

      const contacts: any[] = [];
      let rejected = 0;

      rows.forEach((row) => {
        const contact: any = {};

        for (const key in row) {
          const normalized = normalizeHeader(key);
          const mapped = FIELD_MAP[normalized];
          if (mapped) {
            contact[mapped] = String(row[key]).trim();
          }
        }

        contact.segments = contact.segments
          ? contact.segments.split(',').map((s: string) => s.trim())
          : [];

        if (!contact.name || !contact.email) {
          rejected++;
          return;
        }

        contacts.push(contact);
      });

      return res.status(200).json({
        total: rows.length,
        valid: contacts.length,
        rejected,
        contacts,
      });
    } catch (e: any) {
      return res.status(500).json({ error: e.message });
    }
  });
}
