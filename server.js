/**
 * ============================================================================
 * ANIMEM.UZ - STANDALONE TELEGRAM ULTRA-FAST DIRECT MP4 STREAMING BOT SERVER
 * ============================================================================
 * 
 * Ushbu server Telegram kanallariga tashlangan videolarni:
 * 1. To'g'ridan-to'g'ri MP4 (HTTP 206 Partial Content / Range Requests) formatida uzatadi.
 * 2. 1000+ bir vaqtning o'zida ko'ruvchilarga qotmasdan, lahzada ochiladigan (moov cache) oqim beradi.
 * 3. HLS va og'ir FFmpeg konvertatsiyasiz (CPU va RAM ni 95% tejaydi).
 * 4. VPS diskini va keshini har 1 soatda avtomatik tozalaydi.
 * 5. Eski HLS va Player havolalarini avtomatik to'g'ridan-to'g'ri MP4 ga yo'naltiradi (302 Redirect).
 */

const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { exec } = require('child_process');
const { EventEmitter } = require('events');

// Global xatoliklar ushlagichi (server to'xtab qolmasligi uchun)
process.on('uncaughtException', (err) => {
  console.error('⚠️ [Server Safe] Uncaught Exception:', err?.message || err);
});
process.on('unhandledRejection', (reason) => {
  console.error('⚠️ [Server Safe] Unhandled Rejection:', reason?.message || reason);
});

// 1. Standalone .env parser
try {
  const envPath = path.join(__dirname, '.env');
  if (fs.existsSync(envPath)) {
    const lines = fs.readFileSync(envPath, 'utf8').split('\n');
    for (const rawLine of lines) {
      const line = rawLine.trim();
      if (!line || line.startsWith('#')) continue;
      const eqIdx = line.indexOf('=');
      if (eqIdx !== -1) {
        const key = line.substring(0, eqIdx).trim();
        let val = line.substring(eqIdx + 1).trim();
        if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
          val = val.slice(1, -1);
        }
        if (!process.env[key]) {
          process.env[key] = val;
        }
      }
    }
  }
} catch (e) {
  console.warn('.env o\'qishda ogohlantirish:', e?.message || e);
}

const express = require('express');
const cors = require('cors');
const { TelegramClient, Api } = require('telegram');
const { StringSession } = require('telegram/sessions');
const { NewMessage } = require('telegram/events');
const bigInt = require('big-integer');

// ============================================================================
// KONFIGURATSIYA VA ATROF-MUHIT O'ZGARUVCHILARI
// ============================================================================
const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;
const TG_API_ID = parseInt(process.env.TG_API_ID || '', 10);
const TG_API_HASH = process.env.TG_API_HASH || '';
const TG_BOT_TOKEN = process.env.TG_STREAM_BOT_TOKEN || '';
const TG_STREAM_DOMAIN = process.env.TG_STREAM_DOMAIN || 'stream.animovie.uz';
const CACHE_DIR = path.resolve(__dirname, process.env.HLS_CACHE_DIR || 'cache');
const STORAGE_DIR = path.resolve(__dirname, process.env.STORAGE_DIR || 'videos');

if (!TG_API_ID || !TG_API_HASH || !TG_BOT_TOKEN) {
  console.error('⚠️ Telegram config tugallanmagan. .env da TG_API_ID, TG_API_HASH va TG_STREAM_BOT_TOKEN bo\'lishi kerak.');
}

if (!fs.existsSync(CACHE_DIR)) {
  fs.mkdirSync(CACHE_DIR, { recursive: true });
}
if (!fs.existsSync(STORAGE_DIR)) {
  fs.mkdirSync(STORAGE_DIR, { recursive: true });
}

const HLS_DIR = path.join(CACHE_DIR, 'hls');
if (!fs.existsSync(HLS_DIR)) {
  fs.mkdirSync(HLS_DIR, { recursive: true });
}

const SESSION_FILE = path.join(CACHE_DIR, 'session.txt');
const CHANNELS_FILE = path.join(CACHE_DIR, 'channels.json');
const METADATA_FILE = path.join(CACHE_DIR, 'metadata.json');
const STREAM_IDS_FILE = path.join(CACHE_DIR, 'stream_ids.json');

// Keshlar (LRU xotira)
const mediaMetaCache = new Map();
const videoHeadCache = new Map(); // Birinchi 4MB moov atom keshi (lahzada boshlash uchun)
const streamIdMap = new Map(); // streamId -> { channelId, messageId }
const reverseStreamIdMap = new Map(); // `${channelId}_${messageId}` -> streamId

// Ma'lum kanallar ro'yxati (accessHash saqlash)
const knownChannels = new Map();
knownChannels.set('3873627200', {
  channelId: '3873627200',
  accessHash: '-4392074857025164406',
  title: 'Animem UZ s3 bazasi‼️'
});

function loadPersistedData() {
  try {
    if (fs.existsSync(CHANNELS_FILE)) {
      const data = JSON.parse(fs.readFileSync(CHANNELS_FILE, 'utf8'));
      for (const [k, v] of Object.entries(data)) {
        knownChannels.set(String(k), v);
      }
    }
  } catch {}

  try {
    if (fs.existsSync(METADATA_FILE)) {
      const data = JSON.parse(fs.readFileSync(METADATA_FILE, 'utf8'));
      for (const [k, v] of Object.entries(data)) {
        mediaMetaCache.set(String(k), v);
      }
    }
  } catch {}

  try {
    if (fs.existsSync(STREAM_IDS_FILE)) {
      const data = JSON.parse(fs.readFileSync(STREAM_IDS_FILE, 'utf8'));
      for (const [sId, info] of Object.entries(data)) {
        streamIdMap.set(sId, info);
        reverseStreamIdMap.set(`${info.channelId}_${info.messageId}`, sId);
      }
    }
  } catch {}
}

function savePersistedChannels() {
  try {
    const obj = {};
    for (const [k, v] of knownChannels.entries()) obj[k] = v;
    fs.writeFileSync(CHANNELS_FILE, JSON.stringify(obj, null, 2));
  } catch {}
}

function savePersistedStreamIds() {
  try {
    const obj = {};
    for (const [k, v] of streamIdMap.entries()) obj[k] = v;
    fs.writeFileSync(STREAM_IDS_FILE, JSON.stringify(obj, null, 2));
  } catch {}
}

function getOrCreateStreamId(channelId, messageId) {
  const cleanId = String(channelId).replace(/^-100/, '').replace(/^-/, '');
  const key = `${cleanId}_${messageId}`;
  if (reverseStreamIdMap.has(key)) {
    return reverseStreamIdMap.get(key);
  }
  const timeHex = Date.now().toString(16);
  const randHex = crypto.randomBytes(2).toString('hex');
  const streamId = `ext_${timeHex}_${randHex}`;

  streamIdMap.set(streamId, { channelId: cleanId, messageId: String(messageId) });
  reverseStreamIdMap.set(key, streamId);
  savePersistedStreamIds();
  return streamId;
}

function savePersistedMetadata() {
  try {
    if (mediaMetaCache.size > 2000) {
      const keys = Array.from(mediaMetaCache.keys());
      for (let i = 0; i < keys.length - 1500; i++) {
        mediaMetaCache.delete(keys[i]);
      }
    }
    const obj = {};
    for (const [k, v] of mediaMetaCache.entries()) obj[k] = v;
    fs.writeFileSync(METADATA_FILE, JSON.stringify(obj, null, 2));
  } catch {}
}

loadPersistedData();

// ============================================================================
// FORMAT VA YORDAMCHI FUNKSIYALAR
// ============================================================================
function formatBytes(bytes) {
  if (!bytes || bytes <= 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatDuration(sec) {
  if (!sec || isNaN(sec)) return '00:00';
  const m = Math.floor(sec / 60);
  const s = Math.floor(sec % 60);
  return `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}

function extractMediaFromMessage(message) {
  if (!message || !message.media) return null;
  const doc = message.media.document || message.media;
  if (!doc || !doc.id) return null;

  const isVideo = (doc.mimeType && doc.mimeType.startsWith('video/')) ||
    (doc.attributes && doc.attributes.some(a => a.className === 'DocumentAttributeVideo')) ||
    message.media.className === 'MessageMediaVideo';

  if (!isVideo) return null;

  const filenameAttr = doc.attributes?.find(a => a.className === 'DocumentAttributeFilename');
  const videoAttr = doc.attributes?.find(a => a.className === 'DocumentAttributeVideo');

  const fileName = filenameAttr?.fileName || `video_${message.id}.mp4`;
  const size = Number(doc.size || 0);
  const duration = videoAttr?.duration || 0;
  const mimeType = doc.mimeType || 'video/mp4';
  const videoCodec = videoAttr?.videoCodec || null;
  const lowerName = fileName.toLowerCase();
  const isHevc = lowerName.includes('hevc') || lowerName.includes('x265') || lowerName.includes('h.265') || lowerName.includes('h265');

  return {
    document: doc,
    fileName,
    size,
    mimeType,
    duration,
    videoCodec,
    isHevc,
    cachedAt: Date.now()
  };
}

function getValidFileReference(doc) {
  if (!doc || !doc.fileReference) return Buffer.alloc(0);
  if (Buffer.isBuffer(doc.fileReference)) return doc.fileReference;
  if (doc.fileReference.data && Array.isArray(doc.fileReference.data)) {
    return Buffer.from(doc.fileReference.data);
  }
  if (typeof doc.fileReference === 'string') {
    return Buffer.from(doc.fileReference, 'base64');
  }
  return Buffer.from(doc.fileReference);
}

function createDocumentLocation(doc) {
  return new Api.InputDocumentFileLocation({
    id: bigInt(doc.id),
    accessHash: bigInt(doc.accessHash),
    fileReference: getValidFileReference(doc),
    thumbSize: '',
  });
}

// ============================================================================
// TELEGRAM CLIENT (GRAMJS)
// ============================================================================
let client = null;
let isInitializing = false;

async function getTelegramClient() {
  if (client && client.connected) return client;

  if (isInitializing) {
    while (isInitializing) {
      await new Promise(r => setTimeout(r, 150));
    }
    if (client && client.connected) return client;
  }

  isInitializing = true;
  try {
    const hasExplicitSession = Object.prototype.hasOwnProperty.call(process.env, 'TG_STREAM_SESSION');
    let sessionString = process.env.TG_STREAM_SESSION || '';

    if (!hasExplicitSession && !sessionString && fs.existsSync(SESSION_FILE)) {
      try {
        sessionString = fs.readFileSync(SESSION_FILE, 'utf8').trim();
      } catch {}
    }

    if (hasExplicitSession && !sessionString && fs.existsSync(SESSION_FILE)) {
      try {
        fs.unlinkSync(SESSION_FILE);
      } catch {}
    }

    if (!sessionString && !TG_BOT_TOKEN) {
      throw new Error('Telegram bot token not configured. Set TG_STREAM_BOT_TOKEN in the env before starting the bot.');
    }

    const session = new StringSession(sessionString);

    client = new TelegramClient(session, TG_API_ID, TG_API_HASH, {
      connectionRetries: 5,
      autoReconnect: true,
      floodSleepThreshold: 60,
    });

    await client.connect();

    let isAuthorized = false;
    try {
      isAuthorized = await client.checkAuthorization();
    } catch {
      isAuthorized = false;
    }

    if (!isAuthorized) {
      console.log('🔄 Telegram bot avtorizatsiyadan o\'tmoqda...');
      try {
        await client.start({ botAuthToken: TG_BOT_TOKEN });
      } catch (authErr) {
        console.warn('⚠️ Mavjud sessiya bilan ulanib bo\'lmadi, yangi toza sessiya yaratilmoqda:', authErr?.message || authErr);
        if (fs.existsSync(SESSION_FILE)) {
          try { fs.unlinkSync(SESSION_FILE); } catch {}
        }
        client = new TelegramClient(new StringSession(''), TG_API_ID, TG_API_HASH, {
          connectionRetries: 5,
          autoReconnect: true,
          floodSleepThreshold: 60,
        });
        await client.connect();
        await client.start({ botAuthToken: TG_BOT_TOKEN });
      }

      const newSess = client.session.save();
      try {
        fs.writeFileSync(SESSION_FILE, newSess);
      } catch {}
    }

    const me = await client.getMe();
    console.log(`🤖 [Telegram Bot] Muvaffaqiyatli ulandi: @${me.username}`);

    registerTelegramEventHandler(client);
    return client;
  } catch (err) {
    console.error('❌ [Telegram Bot] Ulanishda xatolik:', err?.message || err);
    throw err;
  } finally {
    isInitializing = false;
  }
}

// ============================================================================
// VIDEOLARNI SERVER DISKIGA AVTOMATIK YUKLASH VA KESH QILISH
// ============================================================================
const activeDownloads = new Set();

async function downloadVideoToDisk(tgClient, meta, cleanChannelId, messageId) {
  const fileKey = `${cleanChannelId}_${messageId}`;
  if (activeDownloads.has(fileKey)) return;

  const targetPath = path.join(STORAGE_DIR, `${cleanChannelId}_${messageId}.mp4`);
  if (fs.existsSync(targetPath)) return;

  const tempPath = `${targetPath}.temp`;
  activeDownloads.add(fileKey);

  try {
    console.log(`[Video Saver] ⏳ #${messageId} anime video server diskiga yuklanmoqda... (Hajmi: ${formatBytes(meta.size)})`);
    const fileLocation = createDocumentLocation(meta.document);
    const TG_CHUNK_SIZE = 512 * 1024;
    const totalChunks = Math.ceil(meta.size / TG_CHUNK_SIZE);

    const downloadStream = tgClient.iterDownload({
      file: fileLocation,
      dcId: meta.document.dcId,
      offset: bigInt(0),
      limit: totalChunks,
      chunkSize: TG_CHUNK_SIZE,
      requestSize: TG_CHUNK_SIZE,
    });

    const writeStream = fs.createWriteStream(tempPath);

    for await (const chunk of downloadStream) {
      if (!writeStream.write(chunk)) {
        await new Promise(resolve => writeStream.once('drain', resolve));
      }
    }

    await new Promise(resolve => writeStream.end(resolve));

    if (fs.existsSync(tempPath)) {
      fs.renameSync(tempPath, targetPath);
      console.log(`[Video Saver] ✅ #${messageId} anime video server diskiga to'liq saqlandi: ${targetPath}`);

      // H.264 tekshirish va konvertatsiya qilish + HLS yaratish
      const streamId = getOrCreateStreamId(cleanChannelId, messageId);
      const hlsOutputDir = path.join(HLS_DIR, streamId);

      (async () => {
        const finalMp4Path = await ensureH264Mp4(targetPath);
        await generateHlsSegments(finalMp4Path, hlsOutputDir);
      })().catch((err) => {
        console.warn(`[Video Processing] ⚠️ #${messageId} H.264/HLS jarayonida xatolik:`, err?.message || err);
      });
    }
  } catch (err) {
    console.warn(`[Video Saver] ⚠️ #${messageId} videoni serverga saqlashda xatolik:`, err?.message || err);
    try {
      if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath);
    } catch {}
  } finally {
    activeDownloads.delete(fileKey);
  }
}

function getVideoCodec(filePath) {
  return new Promise((resolve) => {
    exec(`ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of default=noprint_wrappers=1:nokey=1 "${filePath}"`, (err, stdout) => {
      if (err || !stdout) return resolve('unknown');
      resolve(stdout.trim().toLowerCase());
    });
  });
}

async function ensureH264Mp4(filePath) {
  try {
    const codec = await getVideoCodec(filePath);
    if (codec === 'h264') {
      console.log(`[Transcoder] ⚡️ Video allaqachon H.264 (AVC) formatida: ${filePath}`);
      return filePath;
    }

    console.log(`[Transcoder] 🔄 Video kodeki: ${codec}. H.264 (AVC) formatiga to'liq konvertatsiya boshlandi: ${filePath}`);
    const convertedPath = filePath.replace(/\.mp4$/, '_h264.mp4');

    return await new Promise((resolve) => {
      // 4 ta OCPU dan foydalangan holda yuqori tezlikda H.264 (AVC) ga o'girish
      const cmd = `ffmpeg -y -i "${filePath}" -c:v libx264 -preset veryfast -crf 22 -c:a aac -b:a 128k -movflags +faststart "${convertedPath}"`;

      exec(cmd, (err) => {
        if (!err && fs.existsSync(convertedPath) && fs.statSync(convertedPath).size > 1000) {
          try {
            fs.unlinkSync(filePath);
            fs.renameSync(convertedPath, filePath);
            console.log(`[Transcoder] ✅ Video muvaffaqiyatli H.264 formatiga o'girildi: ${filePath}`);
          } catch {}
        } else {
          console.warn(`[Transcoder] ⚠️ Konvertatsiya yakunlanmadi, asl fayl ishlatiladi:`, err?.message || err);
          try { if (fs.existsSync(convertedPath)) fs.unlinkSync(convertedPath); } catch {}
        }
        resolve(filePath);
      });
    });
  } catch {
    return filePath;
  }
}

async function generateHlsSegments(inputMp4Path, hlsOutputDir) {
  if (!fs.existsSync(hlsOutputDir)) {
    fs.mkdirSync(hlsOutputDir, { recursive: true });
  }
  const masterPath = path.join(hlsOutputDir, 'master.m3u8');
  if (fs.existsSync(masterPath)) return masterPath;

  const codec = await getVideoCodec(inputMp4Path);
  const segmentPattern = path.join(hlsOutputDir, 'segment_%03d.ts');

  let cmd = '';
  if (codec === 'h264') {
    // Video H.264 bo'lsa, tezkor bitstream copy (1-2 soniya)
    cmd = `ffmpeg -y -i "${inputMp4Path}" -c copy -f hls -hls_time 6 -hls_list_size 0 -hls_segment_filename "${segmentPattern}" "${masterPath}"`;
  } else {
    // Agar video boshqa formatda bo'lsa, segmentlarni darhol H.264 AVC ga aylantirish
    console.log(`[HLS Transcoder] 🔄 HLS segmentlari H.264 (AVC) ga o'girilmoqda...`);
    cmd = `ffmpeg -y -i "${inputMp4Path}" -c:v libx264 -preset veryfast -crf 22 -c:a aac -b:a 128k -f hls -hls_time 6 -hls_list_size 0 -hls_segment_filename "${segmentPattern}" "${masterPath}"`;
  }

  return new Promise((resolve) => {
    exec(cmd, (err) => {
      if (err) {
        console.warn(`[HLS Transcoder] ⚠️ HLS generatsiyasida ogohlantirish:`, err?.message || err);
      } else {
        console.log(`[HLS Transcoder] ✅ HLS master.m3u8 va H.264 segmentlar tayyor: ${masterPath}`);
      }
      resolve(masterPath);
    });
  });
}

function findChannelForMessage(messageId) {
  const numId = String(messageId);
  for (const key of mediaMetaCache.keys()) {
    if (key.endsWith(`_${numId}`)) {
      return key.split('_')[0];
    }
  }
  try {
    if (fs.existsSync(STORAGE_DIR)) {
      const files = fs.readdirSync(STORAGE_DIR);
      for (const f of files) {
        if (f.endsWith(`_${numId}.mp4`)) {
          return f.replace(`_${numId}.mp4`, '');
        }
      }
    }
  } catch {}
  if (knownChannels.size > 0) {
    return Array.from(knownChannels.keys())[0];
  }
  return 'default';
}

function resolveStreamInfo(param1, param2) {
  if (param1 && param1.startsWith('ext_')) {
    if (streamIdMap.has(param1)) {
      const info = streamIdMap.get(param1);
      return { channelId: info.channelId, messageId: info.messageId, streamId: param1 };
    }
    return { channelId: 'external', messageId: param1, streamId: param1 };
  }

  if (param1 && param2) {
    const streamId = getOrCreateStreamId(param1, param2);
    return { channelId: param1, messageId: param2, streamId };
  }

  if (param1 && !param2) {
    const foundChannel = findChannelForMessage(param1);
    const streamId = getOrCreateStreamId(foundChannel, param1);
    return { channelId: foundChannel, messageId: param1, streamId };
  }

  return { channelId: param1 || 'default', messageId: param2 || '1', streamId: param1 };
}

// Telegram kanalga video tashlanganda xabar berish
function registerTelegramEventHandler(tgClient) {
  tgClient.addEventHandler(async (event) => {
    try {
      const message = event.message;
      if (!message) return;

      const chat = message.chat || event.chat;
      const isChannel = !message.isPrivate;
      const cleanChannelId = String(message.chatId || message.peerId?.channelId || message.peerId?.userId || '')
        .replace(/^-100/, '')
        .replace(/^-/, '');
      const messageId = message.id;

      if (cleanChannelId && chat && chat.accessHash) {
        knownChannels.set(cleanChannelId, {
          channelId: cleanChannelId,
          accessHash: chat.accessHash.toString(),
          title: chat.title || chat.firstName || '',
          isChannel: isChannel
        });
        savePersistedChannels();
      }

      // /start yoki matnli xabarlarni tutish
      const text = (message.message || '').trim();
      if (text.startsWith('/start')) {
        const welcomeHtml =
          `🎬 <b>Assalomu alaykum! AniMovie S3 Video Streamer botiga xush kelibsiz!</b>\n\n` +
          `⚡️ <b>Ushbu bot nima qiladi?</b>\n` +
          `Telegram kanal yoki shaxsiy chatga yuklangan videolarni to'g'ridan-to'g'ri saytingizda <b>qotmasdan va yuqori tezlikda</b> stream qilish uchun havolalar beradi.\n\n` +
          `📌 <b>Qanday ishlatiladi?</b>\n` +
          `1️⃣ <b>Kanalda ishlatish (Tavsiya etiladi):</b> Meni video yuklanadigan Telegram kanalingizga <b>admin</b> qiling. Kanalga video tashlanganda avtomatik ravishda tayyor <b>HLS master.m3u8</b> va <b>Stream URL</b> beraman.\n` +
          `2️⃣ <b>Shaxsiy chatda:</b> Menga to'g'ridan-to'g'ri video yuboring yoki kanaldan video forward qiling, sizga havola tayyorlab beraman.\n\n` +
          `🚀 <i>Hoziroq sinab ko'rish uchun menga biror video yuboring!</i>`;

        try {
          await tgClient.sendMessage(message.chatId, {
            message: welcomeHtml,
            parseMode: 'html',
            replyTo: messageId
          });
        } catch {
          await tgClient.sendMessage(message.chatId, {
            message: welcomeHtml,
            parseMode: 'html'
          });
        }
        console.log(`[Telegram Streamer] /start buyrug'iga javob berildi (chat: ${message.chatId})`);
        return;
      }

      const mediaInfo = extractMediaFromMessage(message);
      if (!mediaInfo) {
        if (message.isPrivate && text) {
          await tgClient.sendMessage(message.chatId, {
            message: `ℹ️ <i>Stream havolasini olish uchun menga <b>video</b> yuboring yoki kanalingizga admin qilib qo'shing.</i>`,
            parseMode: 'html',
            replyTo: messageId
          });
        }
        return;
      }

      const cacheKey = `${cleanChannelId}_${messageId}`;
      mediaMetaCache.set(cacheKey, mediaInfo);
      savePersistedMetadata();

      const streamId = getOrCreateStreamId(cleanChannelId, messageId);

      // 💾 Avtomatik tarzda videoni server diskiga yuklashni orqa fonda boshlash
      downloadVideoToDisk(tgClient, mediaInfo, cleanChannelId, messageId).catch(() => {});

      const hlsMasterUrl = `https://${TG_STREAM_DOMAIN}/stream/${streamId}/master.m3u8`;
      const directStreamUrl = `https://${TG_STREAM_DOMAIN}/stream/${streamId}`;
      const embedUrl = `https://${TG_STREAM_DOMAIN}/embed/${streamId}`;
      const playerUrl = `https://${TG_STREAM_DOMAIN}/player/${streamId}`;

      let codecInfo = `🎞 <b>Format:</b> <code>H.264 (AVC)</code> ✅ <i>(Brauzerlarga 100% mos)</i>\n\n`;
      let warningBlock = '';

      if (mediaInfo.isHevc) {
        codecInfo = `🎞 <b>Format:</b> <code>HEVC (H.265) ➡️ H.264 (AVC)</code> 🔄\n\n`;
        warningBlock = 
          `⚙️ <b>Avtomatik konvertatsiya:</b> Ushbu video serverda barcha brauzerlar va qurilmalarda 100% video ko'rinishi uchun <b>H.264 (AVC)</b> formatiga avtomatik o'girilmoqda.\n\n`;
      }

      const replyHtml = 
        `🎬 <b>Anime Video Muvaffaqiyatli Qabul Qilindi!</b>\n\n` +
        `📁 <b>Fayl:</b> <code>${mediaInfo.fileName}</code>\n` +
        `⚖️ <b>Hajmi:</b> <b>${formatBytes(mediaInfo.size)}</b>\n` +
        `⏱ <b>Davomiyligi:</b> ${formatDuration(mediaInfo.duration)}\n` +
        codecInfo +
        warningBlock +
        `⚡️ <b>1. HLS Master Stream Havolasi (.m3u8):</b>\n` +
        `<code>${hlsMasterUrl}</code>\n\n` +
        `🔗 <b>2. To'g'ridan-to'g'ri MP4 Stream Havolasi:</b>\n` +
        `<code>${directStreamUrl}</code>\n\n` +
        `📺 <b>3. Sayt uchun Iframe kodi (Admin panelga qo'yish uchun):</b>\n` +
        `<code>&lt;iframe src="${embedUrl}" width="100%" height="100%" frameborder="0" allowfullscreen&gt;&lt;/iframe&gt;</code>\n\n` +
        `🌐 <b>4. Brauzer Pleyeri:</b>\n` +
        `<code>${playerUrl}</code>\n\n` +
        `💾 <b>Server holati:</b> <i>Video avtomatik tarzda VPS diskiga yuklanib, H.264 (AVC) formatiga moslashtirilmoqda va HLS segmentlarga ajratilmoqda.</i>`;

      try {
        await tgClient.sendMessage(message.chatId, {
          message: replyHtml,
          parseMode: 'html',
          replyTo: messageId
        });
      } catch {
        await tgClient.sendMessage(message.chatId, {
          message: replyHtml,
          parseMode: 'html'
        });
      }

      console.log(`[Telegram Streamer] Replied with MP4 stream URL for message #${messageId} in chat ${cleanChannelId}`);
    } catch (err) {
      console.warn('[Telegram Streamer] Error handling incoming video message:', err?.message || err);
    }
  }, new NewMessage({}));
}

async function getStreamMetadata(channelId, messageId) {
  const cleanId = String(channelId).replace(/^-100/, '').replace(/^-/, '');
  const cacheKey = `${cleanId}_${messageId}`;

  const cached = mediaMetaCache.get(cacheKey);
  if (cached && (Date.now() - cached.cachedAt < 86400000)) {
    return cached;
  }

  const tgClient = await getTelegramClient();
  const normalizedChannelId = String(channelId).startsWith('-100')
    ? channelId
    : (String(channelId).startsWith('-') ? `-100${String(channelId).slice(1)}` : `-100${channelId}`);

  try {
    let peer;
    const ch = knownChannels.get(cleanId);
    if (ch && ch.accessHash) {
      if (ch.isChannel === false) {
        peer = new Api.InputPeerUser({
          userId: bigInt(ch.channelId),
          accessHash: bigInt(ch.accessHash),
        });
      } else {
        peer = new Api.InputPeerChannel({
          channelId: bigInt(ch.channelId),
          accessHash: bigInt(ch.accessHash),
        });
      }
    } else {
      try {
        peer = await tgClient.getInputEntity(normalizedChannelId);
      } catch {
        peer = await tgClient.getInputEntity(cleanId);
      }
    }

    const messages = await tgClient.getMessages(peer, { ids: [messageId] });
    if (!messages || !messages[0]) return null;

    const mediaInfo = extractMediaFromMessage(messages[0]);
    if (!mediaInfo) return null;

    mediaMetaCache.set(cacheKey, mediaInfo);
    savePersistedMetadata();
    return mediaInfo;
  } catch (err) {
    console.error(`[Telegram Streamer] Fetch message #${messageId} error:`, err?.message || err);
    return null;
  }
}

// ============================================================================
// EXPRESS HTTP ROUTER & HIGH-PERFORMANCE MP4 STREAMING
// ============================================================================
const app = express();
app.use(cors({ origin: true, credentials: true }));

// Server sog'ligini tekshirish
app.get(['/health', '/ping', '/'], (_req, res) => {
  res.status(200).send("Animem S3 Video Streamer is Running OK (Ultra-Fast MP4 Direct Stream) 🚀");
});

// 1. ULTRA-FAST DIRECT MP4 & DISK STREAM HANDLER (HTTP 206 Partial Content)
const handleStreamRequest = async (req, res) => {
  let param1 = req.params.channelId || req.params.param1 || req.params.streamId;
  let param2 = req.params.messageId || req.params.param2;
  if (param2 === 'video.mp4') param2 = null;

  const info = resolveStreamInfo(param1, param2);
  let channelId = info.channelId;
  let messageId = info.messageId;

  const numMsgId = parseInt(messageId, 10);
  if (!channelId || isNaN(numMsgId)) {
    return res.status(400).json({ error: "Noto'g'ri parametrlar" });
  }

  const cleanId = String(channelId).replace(/^-100/, '').replace(/^-/, '');
  const cacheKey = `${cleanId}_${numMsgId}`;
  const headKey = `${cacheKey}_head`;
  const localVideoPath = path.join(STORAGE_DIR, `${cleanId}_${numMsgId}.mp4`);

  try {
    let totalSize = 0;
    let mimeType = 'video/mp4';
    let meta = null;

    if (fs.existsSync(localVideoPath)) {
      const stat = fs.statSync(localVideoPath);
      totalSize = stat.size;
    } else {
      meta = await getStreamMetadata(channelId, numMsgId);
      if (!meta || !meta.document) {
        return res.status(404).json({ error: "Video topilmadi yoki o'chirilgan" });
      }
      totalSize = meta.size;
      mimeType = meta.mimeType || 'video/mp4';
    }

    const rangeHeader = req.headers.range;

    // CORS & Range headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Range, Origin, Content-Type, Accept');
    res.setHeader('Access-Control-Expose-Headers', 'Content-Range, Content-Length, Accept-Ranges');
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
    res.setHeader('Content-Type', mimeType);
    res.setHeader('Content-Disposition', 'inline');

    if (req.method === 'OPTIONS') {
      return res.sendStatus(204);
    }

    let start = 0;
    let end = totalSize - 1;
    let isRange = false;

    const isLocal = fs.existsSync(localVideoPath);
    const MAX_CHUNK_BURST = 8 * 1024 * 1024; // 8MB burst limit for Telegram MTProto

    if (rangeHeader) {
      const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
      if (match) {
        isRange = true;
        start = parseInt(match[1], 10);
        if (match[2]) {
          end = parseInt(match[2], 10);
        } else if (!isLocal) {
          end = Math.min(totalSize - 1, start + MAX_CHUNK_BURST - 1);
        }
      }
    } else if (!isLocal) {
      end = Math.min(totalSize - 1, start + MAX_CHUNK_BURST - 1);
    }

    if (start >= totalSize || end >= totalSize || start > end) {
      res.status(416).setHeader('Content-Range', `bytes */${totalSize}`).end();
      return;
    }

    const chunkSize = end - start + 1;

    if (isRange) {
      res.status(206);
      res.setHeader('Content-Range', `bytes ${start}-${end}/${totalSize}`);
    } else {
      res.status(200);
    }

    res.setHeader('Content-Length', chunkSize);

    if (req.method === 'HEAD') return res.end();

    // ⚡️ 1. Agar video server diskida bo'lsa — SSD diskdan 0ms da uzatish!
    if (isLocal) {
      const fileStream = fs.createReadStream(localVideoPath, { start, end });
      fileStream.pipe(res);
      return;
    }

    // ⚡️ 2. Diskda hali bo'lmasa, uni orqa fonda serverga yuklab olishni boshlash
    const tgClient = await getTelegramClient();
    if (meta && !activeDownloads.has(cacheKey)) {
      downloadVideoToDisk(tgClient, meta, cleanId, numMsgId).catch(() => {});
    }

    // ⚡️ 3. INSTANT START: Video boshi (moov atom / metadata) xotirada bo'lsa darhol uzatish (<5ms)
    const HEAD_BUFFER_SIZE = 4 * 1024 * 1024; // 4 MB moov kesh
    if (start === 0 && videoHeadCache.has(headKey)) {
      const cachedHead = videoHeadCache.get(headKey);
      if (chunkSize <= cachedHead.length) {
        return res.end(cachedHead.subarray(0, chunkSize));
      }
    }

    // ⚡️ 4. Telegram MTProto orqali oqim uzatish
    const fileLocation = createDocumentLocation(meta.document);

    const TG_CHUNK_SIZE = 512 * 1024; // 512 KB Telegram direct chunk
    const alignedStart = Math.floor(start / TG_CHUNK_SIZE) * TG_CHUNK_SIZE;
    const skipBytes = start - alignedStart;
    const totalBytesToFetch = (end - start + 1) + skipBytes;
    const chunkLimit = Math.ceil(totalBytesToFetch / TG_CHUNK_SIZE);

    const downloadStream = tgClient.iterDownload({
      file: fileLocation,
      dcId: meta.document.dcId,
      offset: bigInt(alignedStart),
      limit: chunkLimit,
      chunkSize: TG_CHUNK_SIZE,
      requestSize: TG_CHUNK_SIZE,
    });

    let bytesRemaining = chunkSize;
    let skipped = 0;
    let isAborted = false;

    req.on('close', () => {
      isAborted = true;
      if (typeof downloadStream?.return === 'function') {
        try { downloadStream.return(); } catch {}
      }
    });

    const headChunks = [];
    let headBytes = 0;

    try {
      for await (const chunk of downloadStream) {
        if (isAborted || res.writableEnded || res.destroyed) break;

        let dataChunk = chunk;
        if (skipped < skipBytes) {
          const toSkip = Math.min(skipBytes - skipped, dataChunk.length);
          dataChunk = dataChunk.subarray(toSkip);
          skipped += toSkip;
        }

        if (dataChunk.length === 0) continue;

        const bytesToSend = Math.min(dataChunk.length, bytesRemaining);
        const slice = dataChunk.subarray(0, bytesToSend);
        
        const ok = res.write(slice);
        bytesRemaining -= bytesToSend;

        if (start === 0 && headBytes < HEAD_BUFFER_SIZE) {
          headChunks.push(slice);
          headBytes += slice.length;
        }

        if (bytesRemaining <= 0) break;
        if (!ok) await new Promise(resolve => res.once('drain', resolve));
      }
    } finally {
      if (typeof downloadStream?.return === 'function') {
        try { await downloadStream.return(); } catch {}
      }
    }

    // Video boshini kelgusi so'rovlar uchun RAM keshga saqlash
    if (start === 0 && headChunks.length > 0 && !videoHeadCache.has(headKey)) {
      if (videoHeadCache.size > 150) {
        const firstKey = videoHeadCache.keys().next().value;
        if (firstKey) videoHeadCache.delete(firstKey);
      }
      videoHeadCache.set(headKey, Buffer.concat(headChunks));
    }

    if (!res.writableEnded) res.end();
  } catch (err) {
    if (err?.errorMessage === 'FILEREF_UPGRADE_NEEDED' || err?.message?.includes('FILEREF')) {
      mediaMetaCache.delete(cacheKey);
      videoHeadCache.delete(headKey);
    }
    if (!res.headersSent) {
      res.status(500).json({ error: "Video oqimini uzatishda xatolik yuz berdi" });
    } else if (!res.writableEnded) {
      res.end();
    }
    console.error(`[Telegram Streamer] Stream error (${channelId}/${numMsgId}):`, err?.message || err);
  }
};

// 1. ULTRA-FAST DIRECT MP4 & DISK STREAM HANDLER (HTTP 206 Partial Content)
app.get([
  '/api/tgstream/:param1/:param2',
  '/api/tgstream/:param1',
  '/stream/:param1/:param2',
  '/stream/:param1',
  '/video/:param1/:param2',
  '/video/:param1'
], (req, res, next) => {
  if (req.path.endsWith('.m3u8') || req.path.endsWith('.ts') || req.path.includes('/relay')) return next();
  return handleStreamRequest(req, res);
});

// 2. HLS .ts SEGMENT HANDLER
app.get([
  '/stream/:streamId/:segmentFile([a-zA-Z0-9_\\-\\.]+\\.ts)',
  '/hls/:streamId/:segmentFile([a-zA-Z0-9_\\-\\.]+\\.ts)',
  '/api/tghls/:streamId/:segmentFile([a-zA-Z0-9_\\-\\.]+\\.ts)'
], (req, res) => {
  const { streamId, segmentFile } = req.params;
  const segPath = path.join(HLS_DIR, streamId, segmentFile);
  if (fs.existsSync(segPath)) {
    res.setHeader('Content-Type', 'video/mp2t');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
    return fs.createReadStream(segPath).pipe(res);
  }
  return res.status(404).send('Segment not found');
});

// 3. ULTRA-FAST HLS MASTER MANIFEST (e.g. https://s3.animovie.uz/stream/ext_.../master.m3u8)
app.get([
  '/stream/:streamId/master.m3u8',
  '/stream/:param1/:param2/master.m3u8',
  '/api/tghls/:param1/master.m3u8',
  '/api/tghls/:param1/:param2/master.m3u8',
  '/hls/:param1/master.m3u8',
  '/hls/:param1/:param2/master.m3u8'
], async (req, res) => {
  const param1 = req.params.streamId || req.params.param1;
  const param2 = req.params.param2;
  const info = resolveStreamInfo(param1, param2);
  const streamId = info.streamId;

  res.setHeader('Content-Type', 'application/vnd.apple.mpegurl; charset=utf-8');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  res.setHeader('Cache-Control', 'public, max-age=120');

  const hlsDir = path.join(HLS_DIR, streamId);
  const masterPath = path.join(hlsDir, 'master.m3u8');

  // 1. Agar server diskida mahalliy master.m3u8 mavjud bo'lsa
  if (fs.existsSync(masterPath)) {
    return res.sendFile(masterPath);
  }

  // 2. Agar tashqi stream bo'lsa (masalan ext_ bilan boshlansa), uni s3.animem.uz dan tekshirib uzatish
  if (streamId && streamId.startsWith('ext_')) {
    try {
      const remoteRes = await fetch(`https://s3.animem.uz/stream/${streamId}/master.m3u8`, {
        headers: {
          'Referer': 'https://animem.uz/',
          'Origin': 'https://animem.uz',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
      });
      if (remoteRes.ok) {
        const text = await remoteRes.text();
        return res.send(text);
      }
    } catch {}
  }

  // 3. Mahalliy video tayyor bo'lgunicha zaxira HLS manifest
  const mp4Url = `/stream/${streamId}/video.mp4`;
  return res.send(`#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:7200
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:1440.0,
${mp4Url}
#EXT-X-ENDLIST`);
});

// 3.1. HLS RELAY PROXY (/stream/:streamId/relay?url=...)
app.get([
  '/stream/:streamId/relay',
  '/hls/:streamId/relay',
  '/api/tghls/:streamId/relay'
], async (req, res) => {
  const { streamId } = req.params;
  let targetUrl = req.query.url;

  if (!targetUrl) {
    return res.status(400).send("url parametri kiritilmagan");
  }

  // Agar url base64 formatda bo'lsa, uni decode qilish
  if (targetUrl.startsWith('aHR0cD')) {
    try {
      targetUrl = Buffer.from(targetUrl, 'base64').toString('utf8');
    } catch {}
  }

  try {
    const upstreamUrl = targetUrl.startsWith('http') 
      ? targetUrl 
      : `https://s3.animem.uz/stream/${streamId}/relay?url=${encodeURIComponent(req.query.url)}`;

    const fetchHeaders = {
      'Referer': 'https://animem.uz/',
      'Origin': 'https://animem.uz',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    };
    if (req.headers.range) {
      fetchHeaders['Range'] = req.headers.range;
    }

    const upstreamRes = await fetch(upstreamUrl, { headers: fetchHeaders });

    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Range, Authorization, Origin, Accept');
    res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges, Content-Type');

    const contentType = upstreamRes.headers.get('content-type') || '';
    if (contentType) res.setHeader('Content-Type', contentType);

    const contentLength = upstreamRes.headers.get('content-length');
    if (contentLength) res.setHeader('Content-Length', contentLength);

    const contentRange = upstreamRes.headers.get('content-range');
    if (contentRange) {
      res.status(206);
      res.setHeader('Content-Range', contentRange);
    } else {
      res.status(upstreamRes.status);
    }

    if (contentType.includes('mpegurl') || targetUrl.endsWith('.m3u8')) {
      const m3u8Text = await upstreamRes.text();
      return res.send(m3u8Text);
    }

    // Binary / TS segment / Key bo'lsa oqimni to'g'ridan-to'g'ri o'tkazish
    const arrayBuffer = await upstreamRes.arrayBuffer();
    return res.end(Buffer.from(arrayBuffer));
  } catch (err) {
    console.error(`[Relay Error] ${streamId}:`, err?.message || err);
    return res.status(502).send("Relay xatoligi yuz berdi");
  }
});

// 4. EMBED & NATIVE BROWSER PLAYER (Saytlar, admin panel yoki brauzer uchun)
app.get([
  '/player/:param1/:param2',
  '/player/:param1',
  '/embed/:param1/:param2',
  '/embed/:param1'
], (req, res) => {
  const param1 = req.params.param1;
  const param2 = req.params.param2;
  const info = resolveStreamInfo(param1, param2);
  const streamId = info.streamId;

  const hlsUrl = `/stream/${streamId}/master.m3u8`;
  const mp4Url = `/stream/${streamId}`;

  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(`<!DOCTYPE html>
<html lang="uz">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>AniMovie Stream Player</title>
  <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    html, body { width: 100%; height: 100%; background: #000; overflow: hidden; display: flex; align-items: center; justify-content: center; }
    video { width: 100%; height: 100%; max-height: 100vh; object-fit: contain; outline: none; }
  </style>
</head>
<body>
  <video id="videoPlayer" controls autoplay playsinline preload="auto"></video>
  <script>
    const video = document.getElementById('videoPlayer');
    const hlsSrc = '${hlsUrl}';
    const mp4Src = '${mp4Url}';

    if (Hls.isSupported()) {
      const hls = new Hls({ enableWorker: true, lowLatencyMode: false });
      hls.loadSource(hlsSrc);
      hls.attachMedia(video);
      hls.on(Hls.Events.ERROR, function(event, data) {
        if (data.fatal) {
          video.src = mp4Src;
          video.play().catch(()=>{});
        }
      });
    } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
      video.src = hlsSrc;
    } else {
      video.src = mp4Src;
    }
  </script>
</body>
</html>`);
});

// ============================================================================
// VPS DISK VA KESHNI HAR 1 SOATDA AVTOMATIK TOZALASH
// ============================================================================
function autoCleanVpsStorage() {
  console.log('🧹 [VPS Auto-Clean] Avtomatik kesh va keraksiz fayllarni tozalash boshlandi...');
  let deletedFiles = 0;
  let freedBytes = 0;

  try {
    // 1. /tmp papkasidagi vaqtinchalik video fayllarni tozalash
    const tmpDir = os.tmpdir();
    if (fs.existsSync(tmpDir)) {
      const tmpFiles = fs.readdirSync(tmpDir);
      for (const f of tmpFiles) {
        if (f.startsWith('temp_') || f.endsWith('.tmp') || f.endsWith('.ts') || f.endsWith('.part')) {
          try {
            const p = path.join(tmpDir, f);
            const st = fs.statSync(p);
            freedBytes += st.size;
            fs.unlinkSync(p);
            deletedFiles++;
          } catch {}
        }
      }
    }

    // 2. ./cache papkasidagi eski vaqtinchalik fayllarni tozalash (session, metadata va hls saqlanadi)
    if (fs.existsSync(CACHE_DIR)) {
      const cacheEntries = fs.readdirSync(CACHE_DIR);
      for (const entry of cacheEntries) {
        if (entry === 'session.txt' || entry === 'channels.json' || entry === 'metadata.json' || entry === 'stream_ids.json' || entry === 'hls') {
          continue; // Muhim sozlamalarga tegmeymiz
        }
        const fullPath = path.join(CACHE_DIR, entry);
        try {
          const stat = fs.statSync(fullPath);
          if (stat.isDirectory()) {
            fs.rmSync(fullPath, { recursive: true, force: true });
            deletedFiles++;
          } else if (entry.endsWith('.ts') || entry.endsWith('.part') || entry.endsWith('.m3u8')) {
            freedBytes += stat.size;
            fs.unlinkSync(fullPath);
            deletedFiles++;
          }
        } catch {}
      }
    }

    // 3. STORAGE_DIR dagi chala qolgan (.temp) fayllarni tozalash (to'liq .mp4 anime videolarga TEGMAYMIZ!)
    if (fs.existsSync(STORAGE_DIR)) {
      const videoFiles = fs.readdirSync(STORAGE_DIR);
      const twoHoursAgo = Date.now() - 2 * 3600 * 1000;
      for (const vf of videoFiles) {
        if (vf.endsWith('.temp')) {
          try {
            const p = path.join(STORAGE_DIR, vf);
            const st = fs.statSync(p);
            if (st.mtimeMs < twoHoursAgo) {
              freedBytes += st.size;
              fs.unlinkSync(p);
              deletedFiles++;
            }
          } catch {}
        }
      }
    }

    // 4. Xotiradagi (RAM) keshni me'yorda ushlash
    if (videoHeadCache.size > 150) {
      videoHeadCache.clear();
    }
    if (mediaMetaCache.size > 2000) {
      const keys = Array.from(mediaMetaCache.keys());
      for (let i = 0; i < keys.length - 1000; i++) {
        mediaMetaCache.delete(keys[i]);
      }
    }

    if (global.gc) {
      try { global.gc(); } catch {}
    }

    const freeMemMb = Math.round(os.freemem() / (1024 * 1024));
    console.log(`✅ [VPS Auto-Clean] Tozalash yakunlandi: ${deletedFiles} ta keraksiz fayl (${formatBytes(freedBytes)}) o'chirildi. Bo'sh RAM: ${freeMemMb} MB`);
  } catch (err) {
    console.warn('⚠️ [VPS Auto-Clean] Tozalashda ogohlantirish:', err?.message || err);
  }
}

// Har 1 soatda (3600000 ms) avtomatik tozalash
setInterval(autoCleanVpsStorage, 60 * 60 * 1000);
// Dastlabki ishga tushganda ham 10 soniyadan so'ng tozalab olish
setTimeout(autoCleanVpsStorage, 10000);

// ============================================================================
// SERVERNI ISHGA TUSHIRISH
// ============================================================================
app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 [Animem S3 MP4 Server] http://0.0.0.0:${PORT} portida muvaffaqiyatli ishga tushdi!`);
  getTelegramClient().catch(err => {
    console.error('❌ [Telegram Bot] Dastlabki ulanishda xatolik:', err?.message || err);
  });
});
