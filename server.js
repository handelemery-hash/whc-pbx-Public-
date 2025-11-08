import 'dotenv/config';
import express from 'express';
import bodyParser from 'body-parser';
import axios from 'axios';
import nodemailer from 'nodemailer';
import FormData from 'form-data';

// ---- App ----
const app = express();
app.use(bodyParser.json({ limit: '2mb' }));

// ---- Env ----
const TELNYX_API_KEY = process.env.TELNYX_API_KEY || ""; // optional; Retell usually manages Telnyx
const TELNYX_BASE = 'https://api.telnyx.com/v2';

const MOH_URL = process.env.MOH_URL || 'https://cdn.winchesterheartcentre.com/hold.mp3';
const MAIN_876 = process.env.WHC_MAIN_876 || '+18769082658';

const DIR = {
  winchester: process.env.WINCHESTER || '+18769082658',
  portmore:   [process.env.PORTMORE_1, process.env.PORTMORE_2].filter(Boolean),
  ardenne:    process.env.ARDENNE || '',
  sav:        process.env.SAV || ''
};

const BRANCH_EMAIL = {
  winchester: process.env.EMAIL_WINCHESTER || '',
  portmore:   process.env.EMAIL_PORTMORE || '',
  ardenne:    process.env.EMAIL_ARDENNE || '',
  sav:        process.env.EMAIL_SAV || ''
};

// ---- Helpers ----
const tx = (cmd, data) => axios.post(`${TELNYX_BASE}/${cmd}`, data, {
  headers: { Authorization: `Bearer ${TELNYX_API_KEY}` }
});

async function speak(callId, text) {
  if (!text) return;
  await tx('call_control/commands/speak', { call_control_id: callId, payload: text });
}

async function hold(callId) {
  await tx('call_control/commands/hold', { call_control_id: callId, audio_url: MOH_URL });
}

async function bridge(callId, to) {
  try {
    await tx('call_control/commands/bridge', { call_control_id: callId, to, from: MAIN_876 });
    return true;
  } catch (e) {
    console.error('Bridge error:', e?.response?.data || e.message);
    return false;
  }
}

async function bridgeTryList(callId, toList) {
  if (!Array.isArray(toList)) return await bridge(callId, toList);
  for (const to of toList) {
    const ok = await bridge(callId, to);
    if (ok) return true;
  }
  return false;
}

async function startRecord(callId) {
  await tx('call_control/commands/record_start', {
    call_control_id: callId, format: 'mp3', channels: 'single', beep: true
  });
}

function normalizeBranch(b) {
  if (!b) return 'winchester';
  const k = (b || '').toString().toLowerCase();
  if (k.startsWith('win')) return 'winchester';
  if (k.startsWith('port')) return 'portmore';
  if (k.startsWith('ard')) return 'ardenne';
  if (k.startsWith('sav')) return 'sav';
  return 'winchester';
}

// ---- Email (Nodemailer) ----
const mailer = nodemailer.createTransport({
  host: process.env.SMTP_HOST,
  port: parseInt(process.env.SMTP_PORT || '587', 10),
  secure: false,
  auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS }
});

// ---- Routes ----

// A) Telnyx inbound: answer + quick greeting (optional). Retell usually connects audio.
app.post('/telnyx/inbound', async (req, res) => {
  res.sendStatus(200);
  try {
    const evt = req.body?.data?.event_type;
    const p = req.body?.data?.payload;
    if (evt === 'call.initiated') {
      await tx('call_control/commands/answer', { call_control_id: p.call_control_id });
      await speak(p.call_control_id, 'Good day, thank you for calling the Winchester Heart Centre. This is Kimberley. How may I assist you today?');
      // Attach Media Streams to Retell here if you run your own AI edge (not needed if Retell handles media directly)
    }
  } catch (e) {
    console.error('TELNYX inbound error:', e?.response?.data || e.message);
  }
});

// B) Retell → Action webhook (route / voicemail / speak)
app.post('/retell/action', async (req, res) => {
  res.sendStatus(200);
  const { call_control_id, action, office, readback, reason } = req.body || {};
  if (!call_control_id || !action) return;
  const branch = normalizeBranch(office);

  try {
    if (action === 'speak') {
      await speak(call_control_id, readback);
      return;
    }

    if (action === 'route') {
      await speak(call_control_id, readback || 'Connecting you now. One moment please.');
      await hold(call_control_id);

      const target = DIR[branch] ?? DIR.winchester;
      const ok = await bridgeTryList(call_control_id, target);
      if (ok) return;

      // overflow to Winchester if not already tried
      if (branch !== 'winchester') {
        const ok2 = await bridgeTryList(call_control_id, DIR.winchester);
        if (ok2) return;
      }

      // voicemail fallback
      await speak(call_control_id, 'We’re unable to reach the team right now. Please leave your name, number, and how we can help after the tone.');
      await startRecord(call_control_id);
      return;
    }

    if (action === 'voicemail') {
      await speak(call_control_id, reason || 'Please leave your name, number, and how we can help after the tone.');
      await startRecord(call_control_id);
      return;
    }
  } catch (e) {
    console.error('RETELL action error:', e?.response?.data || e.message);
    try {
      await speak(call_control_id, 'We are unable to connect you right now. Please leave a message after the tone.');
      await startRecord(call_control_id);
    } catch {}
  }
});

// C) Telnyx recording webhook: download MP3, transcribe (OpenAI Whisper via REST), email to branch
app.post('/telnyx/recording', async (req, res) => {
  res.sendStatus(200);
  try {
    const evt = req.body?.data?.event_type;
    const pl = req.body?.data?.payload || {};
    if (!evt || !pl?.media_url) return;

    const branch = normalizeBranch(pl?.metadata?.branch);
    const caller = pl?.metadata?.caller || 'Unknown caller';

    // 1) Download MP3
    const mp3Resp = await axios.get(pl.media_url, {
      responseType: 'arraybuffer',
      headers: { Authorization: `Bearer ${process.env.TELNYX_API_KEY}` }
    });
    const buf = Buffer.from(mp3Resp.data);

    // 2) Transcribe with OpenAI Whisper REST
    let transcript = '';
    try {
      const form = new FormData();
      form.append('model', 'whisper-1');
      form.append('file', buf, { filename: 'voicemail.mp3', contentType: 'audio/mpeg' });

      const resp = await axios.post('https://api.openai.com/v1/audio/transcriptions', form, {
        headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}`, ...form.getHeaders() },
        timeout: 120000
      });
      transcript = resp.data?.text || '';
    } catch (e) {
      console.error('Whisper error:', e?.response?.data || e.message);
    }

    // 3) Email
    const toEmail = BRANCH_EMAIL[branch] || BRANCH_EMAIL.winchester;
    const subject = `[VOICEMAIL] ${branch[0].toUpperCase() + branch.slice(1)} | ${pl?.metadata?.reason || 'New voicemail'}`;
    const body = [
      `Branch: ${branch}`,
      `Caller: ${caller}`,
      `Call Control ID: ${pl.call_control_id || 'n/a'}`,
      `Reason: ${pl?.metadata?.reason || 'n/a'}`,
      '',
      'Transcript:',
      transcript || '(no transcript available)'
    ].join('\\n');

    await mailer.sendMail({
      from: process.env.FROM_EMAIL,
      to: toEmail,
      subject,
      text: body,
      attachments: [{ filename: 'voicemail.mp3', content: buf }]
    });

  } catch (e) {
    console.error('Recording handler error:', e?.response?.data || e.message);
  }
});

// D) Optional: DTMF handler for return-to-AI (#9)
app.post('/telnyx/dtmf', async (req, res) => {
  res.sendStatus(200);
  const digits = req.body?.data?.payload?.digits;
  const agentLegId = req.body?.data?.payload?.call_control_id;
  if (digits !== '#9') return;
  try {
    await hold(agentLegId); // park the agent; reattach caller to AI in your media handler if needed
  } catch (e) {
    console.error('DTMF handler error:', e?.response?.data || e.message);
  }
});

// Health check
app.get('/', (req, res) => res.send('WHC PBX server is running.'));

// Boot
const port = parseInt(process.env.PORT || '3000', 10);
app.listen(port, () => console.log(`WHC PBX server listening on :${port}`));
