import clientPromise from '../../../lib/mongo';
import formidable from 'formidable';
import * as XLSX from 'xlsx';

export const config = { api: { bodyParser: false } };

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).send('Method Not Allowed');

  const form = new formidable.IncomingForm();
  form.parse(req, async (err, fields, files) => {
    if (err) return res.status(500).json({ error: err.message });

    const filePath = files.file.filepath;
    const workbook = XLSX.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const data = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName]);

    // Map CSV/Excel headers to your schema
    const FIELD_MAP: Record<string, string> = {
      name: 'name',
      Name: 'name',
      email: 'email',
      Email: 'email',
      phone: 'phone',
      Phone: 'phone',
      whatsapp: 'whatsapp',
      WhatsApp: 'whatsapp',
      location: 'location',
      Location: 'location',
      segments: 'segments',
      Segments: 'segments',
    };

    const validatedContacts: any[] = [];
    let rejectedCount = 0;

    for (const row of data) {
      const contact: any = {};

      // Map fields according to FIELD_MAP
      for (const key in row) {
        const mappedKey = FIELD_MAP[key];
        if (mappedKey) contact[mappedKey] = row[key];
      }

      // Ensure segments is an array
      if (contact.segments) {
        contact.segments = contact.segments
          .toString()
          .split(',')
          .map((s: string) => s.trim());
      } else {
        contact.segments = [];
      }

      // Reject if missing name or email
      if (!contact.name || !contact.email) {
        rejectedCount++;
        continue;
      }

      validatedContacts.push(contact);
    }

    res.status(200).json({
      total: data.length,
      valid: validatedContacts.length,
      rejected: rejectedCount,
      contacts: validatedContacts,
    });
  });
}