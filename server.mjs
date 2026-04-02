import 'dotenv/config';

import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { mkdir, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import nodemailer from 'nodemailer';
import PDFDocument from 'pdfkit';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const HOST = (process.env.HOST || '127.0.0.1').trim();
const PORT = Number(process.env.PORT || 3000);
const APP_BASE_URL = (process.env.APP_BASE_URL || `http://${HOST}:${PORT}`).replace(/\/$/, '');
const WEBAPP_URL = (process.env.WEBAPP_URL || `${APP_BASE_URL}/`).trim();
const ANALYTICS_API_BASE_URL = (process.env.ANALYTICS_API_BASE_URL || 'http://127.0.0.1:8090').replace(/\/$/, '');
const TELEGRAM_BOT_TOKEN = (process.env.TELEGRAM_BOT_TOKEN || '').trim();
const GENERATED_TTL_MS = 1000 * 60 * 30;

const STATIC_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.txt': 'text/plain; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg'
};

const generatedFiles = new Map();
let telegramOffset = 0;
let ratesCache = null;

const fontPath = path.join(__dirname, 'node_modules', 'dejavu-fonts-ttf', 'ttf', 'DejaVuSans.ttf');

function json(res, status, payload) {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store'
  });
  res.end(JSON.stringify(payload));
}

function sendError(res, status, message, details) {
  json(res, status, { ok: false, message, details });
}

function formatMoney(value, currency) {
  const formatted = new Intl.NumberFormat('ru-RU', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(value);

  return `${formatted} ${currency === 'RUB' ? '₽' : currency}`;
}

function normalizeEmail(value) {
  return String(value || '').trim().toLowerCase();
}

function isValidEmail(value) {
  return !value || /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

function decodeCbrXml(buffer) {
  return new TextDecoder('windows-1251').decode(buffer);
}

function parseCbrRate(xml, code) {
  const match = xml.match(new RegExp(`<CharCode>${code}</CharCode>[\\s\\S]*?<Nominal>(\\d+)</Nominal>[\\s\\S]*?<Value>([\\d,]+)</Value>`));
  if (!match) {
    throw new Error(`Не удалось найти курс ${code} в ответе ЦБ`);
  }

  const nominal = Number(match[1]);
  const value = Number(match[2].replace(',', '.'));
  return Number((value / nominal).toFixed(4));
}

async function fetchCbrRates(force = false) {
  if (!force && ratesCache && Date.now() - ratesCache.fetchedAt < 1000 * 60 * 30) {
    return ratesCache;
  }

  const response = await fetch('https://www.cbr.ru/scripts/XML_daily.asp', {
    headers: { 'User-Agent': 'ct-miniapp/1.0' }
  });
  if (!response.ok) {
    throw new Error(`ЦБ РФ вернул статус ${response.status}`);
  }

  const xml = decodeCbrXml(await response.arrayBuffer());
  const date = xml.match(/<ValCurs Date="([^"]+)"/)?.[1] || new Date().toLocaleDateString('ru-RU');

  ratesCache = {
    USD: parseCbrRate(xml, 'USD'),
    EUR: parseCbrRate(xml, 'EUR'),
    CNY: parseCbrRate(xml, 'CNY'),
    RUB: 1,
    date,
    source: 'https://www.cbr.ru/scripts/XML_daily.asp',
    fetchedAt: Date.now()
  };

  return ratesCache;
}

function buildOrderFromPayload(rawPayload) {
  if (!rawPayload || typeof rawPayload !== 'object') {
    throw new Error('Пустой payload расчёта');
  }

  const currency = ['RUB', 'USD', 'EUR', 'CNY'].includes(rawPayload.currency) ? rawPayload.currency : 'RUB';
  const vatRate = rawPayload.vat === '0%' ? 0 : 0.2;
  const email = normalizeEmail(rawPayload.email);

  if (!isValidEmail(email)) {
    throw new Error('Некорректный e-mail');
  }

  const rows = (Array.isArray(rawPayload.rows) ? rawPayload.rows : [])
    .map((row) => {
      const title = String(row?.title || '').trim();
      const unit = String(row?.unit || '').trim() || 'шт';
      const qty = Math.max(0, Math.floor(Number(row?.qty || 0)));
      const price = Math.max(0, Number(row?.price || 0));

      if (!title || qty <= 0) {
        return null;
      }

      const sum = price * qty;
      const vat = sum * vatRate;
      const total = sum + vat;

      return {
        title,
        qty,
        unit,
        price,
        sum,
        vat,
        total
      };
    })
    .filter(Boolean);

  if (!rows.length) {
    throw new Error('Не выбраны услуги для расчёта');
  }

  const subtotal = rows.reduce((acc, row) => acc + row.sum, 0);
  const vat = rows.reduce((acc, row) => acc + row.vat, 0);
  const total = subtotal + vat;
  const questionnaire = (Array.isArray(rawPayload.questionnaireSummary) ? rawPayload.questionnaireSummary : [])
    .map((item) => ({
      label: String(item?.label || '').trim(),
      value: String(item?.value || '').trim()
    }))
    .filter((item) => item.label && item.value);

  const createdAt = new Date(rawPayload.ts || Date.now());

  return {
    email,
    currency,
    vatLabel: vatRate === 0 ? '0%' : '20%',
    ratesText: String(rawPayload.rates || '').trim(),
    questionnaire,
    rows,
    totals: { subtotal, vat, total },
    createdAt: Number.isNaN(createdAt.getTime()) ? new Date().toISOString() : createdAt.toISOString()
  };
}

function pdfBufferFromDocument(doc) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    doc.on('data', (chunk) => chunks.push(chunk));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);
  });
}

async function generatePdf(order) {
  const doc = new PDFDocument({ size: 'A4', margin: 48 });
  doc.font(fontPath);

  const bufferPromise = pdfBufferFromDocument(doc);

  doc.fontSize(22).text('Расчет услуг по маркировке', { align: 'left' });
  doc.moveDown(0.4);
  doc.fontSize(11).fillColor('#6b7280').text(`Дата: ${new Date(order.createdAt).toLocaleString('ru-RU')}`);
  doc.text(`Валюта: ${order.currency}`);
  doc.text(`Ставка НДС: ${order.vatLabel}`);
  if (order.ratesText) {
    doc.text(order.ratesText);
  }
  if (order.email) {
    doc.text(`E-mail: ${order.email}`);
  }

  if (order.questionnaire.length) {
    doc.moveDown(0.8);
    doc.fillColor('#111827').fontSize(13).text('Анкета маркировки');
    doc.moveDown(0.4);
    order.questionnaire.forEach((item) => {
      doc.fontSize(10).fillColor('#4b5563').text(`${item.label}: ${item.value}`);
    });
  }

  doc.moveDown();
  doc.fillColor('#111827').fontSize(13).text('Позиции');
  doc.moveDown(0.4);

  for (const row of order.rows) {
    doc.fontSize(12).fillColor('#111827').text(row.title);
    doc.fontSize(10).fillColor('#4b5563').text(
      `Цена: ${formatMoney(row.price, order.currency)} | Количество: ${row.qty} ${row.unit} | Сумма: ${formatMoney(row.sum, order.currency)}`
    );
    doc.text(`НДС: ${formatMoney(row.vat, order.currency)} | Итого: ${formatMoney(row.total, order.currency)}`);
    doc.moveDown(0.5);
  }

  doc.moveDown(0.6);
  doc.fontSize(14).fillColor('#111827').text(`Без НДС: ${formatMoney(order.totals.subtotal, order.currency)}`);
  doc.text(`НДС: ${formatMoney(order.totals.vat, order.currency)}`);
  doc.fontSize(16).text(`Итого: ${formatMoney(order.totals.total, order.currency)}`);

  doc.end();
  return bufferPromise;
}

async function createMailer() {
  if (!process.env.SMTP_HOST || !process.env.SMTP_FROM) {
    return null;
  }

  return nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: Number(process.env.SMTP_PORT || 465),
    secure: String(process.env.SMTP_SECURE || 'true') === 'true',
    auth: process.env.SMTP_USER
      ? {
          user: process.env.SMTP_USER,
          pass: process.env.SMTP_PASS
        }
      : undefined
  });
}

async function sendOrderEmail(order, pdfBuffer, filename) {
  if (!order.email) {
    return { sent: false, reason: 'email_not_provided' };
  }

  const transporter = await createMailer();
  if (!transporter) {
    return { sent: false, reason: 'smtp_not_configured' };
  }

  await transporter.sendMail({
    from: process.env.SMTP_FROM,
    to: order.email,
    subject: 'Расчет услуг по маркировке',
    text: `Во вложении PDF-расчет. Итого: ${formatMoney(order.totals.total, order.currency)}.`,
    html: `
      <div style="font-family:Arial,sans-serif;color:#111827">
        <h2>Расчет услуг по маркировке</h2>
        <p>Итого: <strong>${escapeHtml(formatMoney(order.totals.total, order.currency))}</strong></p>
        <p>PDF-расчет приложен к письму.</p>
      </div>
    `,
    attachments: [
      {
        filename,
        content: pdfBuffer,
        contentType: 'application/pdf'
      }
    ]
  });

  return { sent: true };
}

async function telegramApi(method, body, isMultipart = false) {
  if (!TELEGRAM_BOT_TOKEN) {
    throw new Error('TELEGRAM_BOT_TOKEN не задан');
  }

  const response = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/${method}`, {
    method: 'POST',
    headers: isMultipart ? undefined : { 'Content-Type': 'application/json; charset=utf-8' },
    body: isMultipart ? body : JSON.stringify(body)
  });

  const payload = await response.json();
  if (!payload.ok) {
    throw new Error(payload.description || `Telegram API error (${response.status})`);
  }

  return payload.result;
}

async function sendTelegramMessage(chatId, text, extra = {}) {
  return telegramApi('sendMessage', {
    chat_id: chatId,
    text,
    ...extra
  });
}

async function sendTelegramDocument(chatId, pdfBuffer, filename, caption) {
  const form = new FormData();
  form.append('chat_id', String(chatId));
  form.append('caption', caption);
  form.append('document', new Blob([pdfBuffer], { type: 'application/pdf' }), filename);
  return telegramApi('sendDocument', form, true);
}

async function processOrder(rawPayload, options = {}) {
  const order = buildOrderFromPayload(rawPayload);
  const pdfBuffer = await generatePdf(order);
  const filename = `ct-mentor-${new Date(order.createdAt).toISOString().slice(0, 10)}.pdf`;
  const emailResult = await sendOrderEmail(order, pdfBuffer, filename);
  const caption = `Расчет готов. Итого: ${formatMoney(order.totals.total, order.currency)}`;

  if (options.chatId) {
    await sendTelegramDocument(options.chatId, pdfBuffer, filename, caption);

    if (order.email) {
      const message = emailResult.sent
        ? `PDF также отправлен на ${order.email}.`
        : `PDF в чат отправлен, но e-mail не отправлен: ${emailResult.reason}.`;
      await sendTelegramMessage(options.chatId, message);
    }
  }

  return { order, pdfBuffer, filename, emailResult };
}

async function readRawBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }

  return Buffer.concat(chunks);
}

async function readJsonBody(req) {
  const raw = (await readRawBody(req)).toString('utf8');
  if (!raw) {
    return {};
  }

  return JSON.parse(raw);
}

function rememberGeneratedPdf(pdfBuffer, filename) {
  const id = randomUUID();
  generatedFiles.set(id, {
    pdfBuffer,
    filename,
    createdAt: Date.now()
  });
  return id;
}

function cleanupGeneratedFiles() {
  const now = Date.now();
  for (const [id, value] of generatedFiles.entries()) {
    if (now - value.createdAt > GENERATED_TTL_MS) {
      generatedFiles.delete(id);
    }
  }
}

async function handleApi(req, res, url) {
  if (url.pathname.startsWith('/api/analytics')) {
    try {
      const upstreamUrl = new URL(`${ANALYTICS_API_BASE_URL}${url.pathname}${url.search}`);
      const requestBody = req.method === 'GET' || req.method === 'HEAD'
        ? undefined
        : await readRawBody(req);

      const upstream = await fetch(upstreamUrl, {
        method: req.method,
        headers: {
          Accept: req.headers.accept || 'application/json',
          ...(req.headers['content-type'] ? { 'Content-Type': req.headers['content-type'] } : {})
        },
        body: requestBody && requestBody.length ? requestBody : undefined
      });

      const responseBody = req.method === 'HEAD'
        ? undefined
        : Buffer.from(await upstream.arrayBuffer());

      res.writeHead(upstream.status, {
        'Content-Type': upstream.headers.get('content-type') || 'application/json; charset=utf-8',
        'Cache-Control': upstream.headers.get('cache-control') || 'no-store'
      });
      res.end(responseBody);
      return;
    } catch (error) {
      return sendError(res, 502, 'Не удалось получить аналитику из backend бота', error.message);
    }
  }

  if (req.method === 'GET' && url.pathname === '/api/health') {
    return json(res, 200, {
      ok: true,
      botEnabled: Boolean(TELEGRAM_BOT_TOKEN),
      smtpEnabled: Boolean(process.env.SMTP_HOST && process.env.SMTP_FROM)
    });
  }

  if (req.method === 'GET' && url.pathname === '/api/rates') {
    try {
      const force = url.searchParams.get('force') === '1';
      const rates = await fetchCbrRates(force);
      return json(res, 200, { ok: true, rates });
    } catch (error) {
      return sendError(res, 502, 'Не удалось получить курсы ЦБ РФ', error.message);
    }
  }

  if (req.method === 'POST' && url.pathname === '/api/orders') {
    try {
      const payload = await readJsonBody(req);
      const result = await processOrder(payload);
      const docId = rememberGeneratedPdf(result.pdfBuffer, result.filename);
      return json(res, 200, {
        ok: true,
        emailSent: result.emailResult.sent,
        emailReason: result.emailResult.reason || null,
        downloadUrl: `${APP_BASE_URL}/api/orders/${docId}/pdf`,
        total: formatMoney(result.order.totals.total, result.order.currency)
      });
    } catch (error) {
      return sendError(res, 400, 'Не удалось обработать расчет', error.message);
    }
  }

  const pdfMatch = url.pathname.match(/^\/api\/orders\/([a-z0-9-]+)\/pdf$/i);
  if ((req.method === 'GET' || req.method === 'HEAD') && pdfMatch) {
    const doc = generatedFiles.get(pdfMatch[1]);
    if (!doc) {
      return sendError(res, 404, 'PDF не найден или уже истек');
    }

    res.writeHead(200, {
      'Content-Type': 'application/pdf',
      'Content-Disposition': `inline; filename*=UTF-8''${encodeURIComponent(doc.filename)}`,
      'Cache-Control': 'no-store'
    });
    res.end(req.method === 'HEAD' ? undefined : doc.pdfBuffer);
    return;
  }

  return false;
}

async function serveStatic(req, res, url) {
  let pathname = decodeURIComponent(url.pathname);
  if (pathname === '/') {
    pathname = '/index.html';
  } else if (pathname === '/analytics' || pathname === '/analytics/') {
    pathname = '/analytics.html';
  } else if (pathname === '/auth' || pathname === '/auth/') {
    pathname = '/auth.html';
  }

  const filePath = path.join(__dirname, pathname);
  if (!filePath.startsWith(__dirname)) {
    sendError(res, 403, 'Доступ запрещён');
    return;
  }

  try {
    const fileStat = await stat(filePath);
    if (!fileStat.isFile()) {
      throw new Error('not a file');
    }

    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, {
      'Content-Type': STATIC_TYPES[ext] || 'application/octet-stream'
    });
    createReadStream(filePath).pipe(res);
  } catch {
    sendError(res, 404, 'Файл не найден');
  }
}

function buildKeyboard() {
  return {
    keyboard: [[{ text: 'Открыть калькулятор', web_app: { url: WEBAPP_URL } }]],
    resize_keyboard: true
  };
}

async function handleTelegramUpdate(update) {
  const message = update.message;
  if (!message) {
    return;
  }

  const chatId = message.chat?.id;
  const text = String(message.text || '').trim();

  if (text === '/start' || text === '/calc') {
    const intro = WEBAPP_URL
      ? 'Открой калькулятор кнопкой ниже. После отправки расчета я пришлю PDF в чат и, если указан e-mail, отправлю его на почту.'
      : 'Бот запущен, но переменная WEBAPP_URL пока не настроена.';
    await sendTelegramMessage(chatId, intro, WEBAPP_URL ? { reply_markup: buildKeyboard() } : {});
    return;
  }

  const rawData = message.web_app_data?.data;
  if (!rawData) {
    return;
  }

  try {
    const parsed = JSON.parse(rawData);
    const payload = parsed?.payload ?? parsed;
    await processOrder(payload, { chatId });
  } catch (error) {
    await sendTelegramMessage(chatId, `Не удалось сформировать расчет: ${error.message}`);
  }
}

async function pollTelegram() {
  if (!TELEGRAM_BOT_TOKEN) {
    console.log('Telegram bot disabled: TELEGRAM_BOT_TOKEN не задан.');
    return;
  }

  console.log('Telegram bot polling enabled.');

  while (true) {
    try {
      const updates = await telegramApi('getUpdates', {
        offset: telegramOffset,
        timeout: 50,
        allowed_updates: ['message']
      });

      for (const update of updates) {
        telegramOffset = update.update_id + 1;
        await handleTelegramUpdate(update);
      }
    } catch (error) {
      console.error('Telegram polling error:', error.message);
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }
  }
}

const server = createServer(async (req, res) => {
  cleanupGeneratedFiles();

  const url = new URL(req.url || '/', APP_BASE_URL);

  if (url.pathname.startsWith('/api/')) {
    const handled = await handleApi(req, res, url);
    if (handled !== false) {
      return;
    }
  }

  await serveStatic(req, res, url);
});

await mkdir(path.join(__dirname, '.tmp'), { recursive: true });

server.on('error', (error) => {
  console.error(`Server failed to start on ${HOST}:${PORT}:`, error.message);
  process.exitCode = 1;
});

server.listen(PORT, HOST, () => {
  console.log(`CT Miniapp running on ${APP_BASE_URL}`);
});

pollTelegram().catch((error) => {
  console.error('Telegram bot fatal error:', error);
});
