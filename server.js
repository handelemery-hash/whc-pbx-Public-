// server.js — WHC PBX + Calendar (Retell + Telnyx + Email)
// ---------------------------------------------------------

import express from "express";
import cors from "cors";
import { google } from "googleapis";
import axios from "axios";
import nodemailer from "nodemailer";

// -------------------------- App --------------------------
const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));

app.get("/health", (_req, res) => res.status(200).send("ok"));

// Simple req log
app.use((req, _res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`);
  next();
});

// -------------------------- Config -----------------------
const HANDOFF_TIMEOUT_MS = Number(process.env.HANDOFF_TIMEOUT_MS || 25000);
const MOH_URL = process.env.MOH_URL || "https://example.com/moh.mp3";

const BRANCH_NUMBERS = {
  winchester: process.env.BRANCH_WINCHESTER || "+18769082658",
  portmore: process.env.BRANCH_PORTMORE || "+18767042739",
  ardenne: process.env.BRANCH_ARDENNE || "+18766713825",
  sav: process.env.BRANCH_SAV || "+18769540252",
};

const BRANCH_EMAILS = {
  winchester: process.env.EMAIL_WINCHESTER || "winchester@winchesterheartcentre.com",
  portmore: process.env.EMAIL_PORTMORE || "portmore@winchesterheartcentre.com",
  ardenne: process.env.EMAIL_ARDENNE || "ardenne@winchesterheartcentre.com",
  sav: process.env.EMAIL_SAV || "savlamar@winchesterheartcentre.com",
};

const FROM_EMAIL = process.env.FROM_EMAIL || "Winchester Heart Centre <no-reply@whc.local>";

// Physician calendars
const PHYSICIANS = {
  dr_emery:
    "uh7ehq6qg5c1qfdciic3v8l0s8@group.calendar.google.com",
  dr_thompson:
    "eburtl0ebphsp3h9qdfurpbqeg@group.calendar.google.com",
  dr_dowding:
    "a70ab6c4e673f04f6d40fabdb0f4861cf2fac5874677d5dd9961e357b8bb8af9@group.calendar.google.com",
  dr_blair:
    "ad21642079da12151a39c9a5aa455d56c306cfeabdfd712fb34a4378c3f04c4a@group.calendar.google.com",
  dr_williams:
    "7343219d0e34a585444e2a39fd1d9daa650e082209a9e5dc85e0ce73d63c7393@group.calendar.google.com",
  dr_wright:
    "b8a27f6d34e63806408f975bf729a3089b0d475b1b58c18ae903bc8bc63aa0ea@group.calendar.google.com",
  dr_dixon:
    "ed382c812be7a6d3396a874ca19368f2d321805f80526e6f3224f713f0637cee@group.calendar.google.com",
};

const PHYSICIAN_DISPLAY = {
  dr_emery: "Dr Emery",
  dr_thompson: "Dr Thompson",
  dr_dowding: "Dr Dowding",
  dr_blair: "Dr Blair",
  dr_williams: "Dr Williams",
  dr_wright: "Dr Wright",
  dr_dixon: "Dr Dixon",
};

// ---------------------- Google Calendar ------------------
function loadServiceAccountJSON() {
  const b64 = process.env.GOOGLE_CREDENTIALS_B64;
  const inline = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;
  if (inline) {
    const txt = inline.trim();
    const json = JSON.parse(txt.startsWith("{") ? txt : Buffer.from(txt, "base64").toString("utf8"));
    console.log("✅ [Calendar] Loaded credentials from GOOGLE_APPLICATION_CREDENTIALS_JSON");
    return json;
  }
  if (b64) {
    const json = JSON.parse(Buffer.from(b64, "base64").toString("utf8"));
    console.log("✅ [Calendar] Loaded credentials from GOOGLE_CREDENTIALS_B64");
    return json;
  }
  throw new Error("Missing Google credentials env");
}

function getJWTAuth() {
  const creds = loadServiceAccountJSON();
  return new google.auth.JWT(
    creds.client_email,
    null,
    creds.private_key,
    ["https://www.googleapis.com/auth/calendar"]
  );
}

function normalizePhys(s) {
  return String(s || "").trim().toLowerCase().replace(/\s+/g, "_");
}

function getCalendarIdForPhys(phys) {
  const k = normalizePhys(phys);
  const id = PHYSICIANS[k];
  if (!id) throw new Error(`Unknown physician '${phys}'`);
  return { key: k, id };
}

const Calendar = {
  async createEvent({ physician, start, end, summary, phone, note }) {
    const { key, id } = getCalendarIdForPhys(physician);
    const auth = getJWTAuth();
    const calendar = google.calendar({ version: "v3", auth });

    const description = [
      phone ? `Phone: ${phone}` : null,
      note ? `Note: ${note}` : null,
    ]
      .filter(Boolean)
      .join("\n");

    const { data } = await calendar.events.insert({
      calendarId: id,
      requestBody: {
        summary: summary || "Consultation",
        description,
        start: { dateTime: start },
        end: { dateTime: end },
      },
    });

    return { key, id, event: data };
  },

  async upcoming(physician, max = 10) {
    const { key, id } = getCalendarIdForPhys(physician);
    const auth = getJWTAuth();
    const calendar = google.calendar({ version: "v3", auth });

    const { data } = await calendar.events.list({
      calendarId: id,
      timeMin: new Date().toISOString(),
      maxResults: Math.min(Math.max(+max || 10, 1), 50),
      singleEvents: true,
      orderBy: "startTime",
    });

    return { key, id, items: data.items || [] };
  },

  async deleteEvent(physician, eventId) {
    const { id } = getCalendarIdForPhys(physician);
    const auth = getJWTAuth();
    const calendar = google.calendar({ version: "v3", auth });
    await calendar.events.delete({ calendarId: id, eventId });
    return true;
  },
};

// --------------------------- Email -----------------------
function makeTransport() {
  const host = process.env.SMTP_HOST;
  const port = Number(process.env.SMTP_PORT || 587);
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;
  if (!host || !user || !pass) return null;
  return nodemailer.createTransport({ host, port, secure: port === 465, auth: { user, pass } });
}

async function sendVoicemailEmail({ branch, caller, recordingUrl, transcript }) {
  const to = BRANCH_EMAILS[branch] || BRANCH_EMAILS.winchester;
  const transporter = makeTransport();
  if (!transporter) {
    console.warn("[Email] SMTP not configured; skipping voicemail email");
    return;
  }
  const html = `
    <p><b>New voicemail</b> for <b>${branch}</b></p>
    <p><b>From:</b> ${caller || "Unknown"}</p>
    <p><b>Recording:</b> <a href="${recordingUrl}">${recordingUrl}</a></p>
    ${transcript ? `<pre>${transcript}</pre>` : ""}
  `;
  await transporter.sendMail({
    from: FROM_EMAIL,
    to,
    subject: `New Voicemail - ${branch} branch`,
    html,
  });
}

// ------------------------ Calendar Routes ----------------
app.post("/calendar/create", async (req, res) => {
  try {
    const { physician, start, end, summary, phone, note } = req.body || {};
    if (!physician || !start || !end) {
      return res.status(400).json({ ok: false, error: "physician, start, end required" });
    }
    const { key, event } = await Calendar.createEvent({ physician, start, end, summary, phone, note });
    return res.json({
      ok: true,
      eventId: event.id,
      htmlLink: event.htmlLink,
      response: `Created appointment for ${PHYSICIAN_DISPLAY[key] || key}`,
    });
  } catch (err) {
    console.error("[/calendar/create] error:", err?.response?.data || err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get("/calendar/upcoming/:physician", async (req, res) => {
  try {
    const { physician } = req.params;
    const { max } = req.query;
    const { key, items } = await Calendar.upcoming(physician, Number(max || 10));
    res.json({
      ok: true,
      physician: PHYSICIAN_DISPLAY[key] || key,
      count: items.length,
      events: items.map(ev => ({
        id: ev.id,
        summary: ev.summary,
        start: ev.start?.dateTime || ev.start?.date,
        end: ev.end?.dateTime || ev.end?.date,
        link: ev.htmlLink,
      })),
    });
  } catch (err) {
    console.error("[/calendar/upcoming] error:", err?.response?.data || err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.post("/calendar/delete", async (req, res) => {
  try {
    const { physician, eventId } = req.body || {};
    if (!physician || !eventId) return res.status(400).json({ ok: false, error: "physician and eventId required" });
    await Calendar.deleteEvent(physician, eventId);
    res.json({ ok: true });
  } catch (err) {
    console.error("[/calendar/delete] error:", err?.response?.data || err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ------------------------ Retell Action ------------------
app.post("/retell/action", async (req, res) => {
  try {
    const event = req.body || {};
    const action = String(event.action || "").toLowerCase();
    if (action.includes("book")) {
      const r = await Calendar.createEvent({
        physician: event.physician,
        start: event.start,
        end: event.end,
        summary: event.summary,
        phone: event.phone,
        note: event.note,
      });
      return res.json({ ok: true, response: `Booked for ${PHYSICIAN_DISPLAY[r.key] || r.key}`, eventId: r.event.id });
    }
    if (action.includes("next")) {
      const u = await Calendar.upcoming(event.physician, 1);
      if (!u.items.length) return res.json({ ok: true, response: "No upcoming events." });
      const first = u.items[0];
      return res.json({ ok: true, response: `Next for ${PHYSICIAN_DISPLAY[u.key] || u.key}: ${first.summary} at ${first.start?.dateTime || first.start?.date}`, eventId: first.id });
    }
    return res.json({ ok: true, response: "Ready." });
  } catch (err) {
    console.error("[/retell/action] error:", err?.response?.data || err.message);
    res.json({ ok: false, response: "Error." });
  }
});

// ------------------------ Telnyx PBX ---------------------
// Only activate if env is present
const TELNYX_API_KEY = process.env.TELNYX_API_KEY;
const TELNYX_CONNECTION_ID = process.env.TELNYX_CONNECTION_ID;
const TELNYX_OUTBOUND_CALLER_ID = process.env.TELNYX_OUTBOUND_CALLER_ID;

const telnyx = TELNYX_API_KEY
  ? axios.create({
      baseURL: "https://api.telnyx.com/v2/",
      headers: { Authorization: `Bearer ${TELNYX_API_KEY}` },
    })
  : null;

async function tx(cmd, payload) {
  if (!telnyx) throw new Error("Telnyx not configured");
  return telnyx.post(`call_commands/${cmd}`, payload);
}

// Helper: branch resolution (default Winchester)
function resolveBranchFromMeta(meta = {}) {
  const m = (meta.branch || meta.forwarded_from || "").toString().toLowerCase();
  if (m.includes("portmore")) return "portmore";
  if (m.includes("ardenne")) return "ardenne";
  if (m.includes("sav")) return "sav";
  return "winchester";
}

// Telnyx Inbound Webhook
app.post("/telnyx/inbound", async (req, res) => {
  if (!telnyx) return res.status(200).json({ ok: true, note: "Telnyx not configured" });
  try {
    const data = req.body?.data || {};
    const eventType = data?.event_type;
    const payload = data?.payload || {};
    const callControlId = payload.call_control_id;

    console.log("[Telnyx]", eventType);

    // Answer & play MOH while transferring
    if (eventType === "call.initiated") {
      await tx("answer", { call_control_id: callControlId });
      await tx("playback_start", {
        call_control_id: callControlId,
        audio_url: MOH_URL,
        overlay: true,
        loop: true,
      });

      // Decide branch to ring
      const branch = resolveBranchFromMeta(payload.client_state ? JSON.parse(Buffer.from(payload.client_state, "base64").toString("utf8")) : {});
      const target = BRANCH_NUMBERS[branch] || BRANCH_NUMBERS.winchester;

      // Try branch (dual-channel: we could use transfer or dial outbound + bridge)
      await tx("transfer", {
        call_control_id: callControlId,
        to: target,
        from: TELNYX_OUTBOUND_CALLER_ID,
        timeout_secs: Math.ceil(HANDOFF_TIMEOUT_MS / 1000),
      });

      return res.json({ ok: true });
    }

    // If branch answers, stop MOH
    if (eventType === "call.bridged") {
      await tx("playback_stop", { call_control_id: callControlId });
      return res.json({ ok: true });
    }

    // If no answer -> voicemail
    if (eventType === "call.ended" || eventType === "transfer.failed") {
      // Start simple voicemail record (Telnyx will send recording.saved later if configured)
      // Here we just email that no one answered; you can expand to start/stop record via call control if needed.
      const branch = resolveBranchFromMeta(payload.client_state ? JSON.parse(Buffer.from(payload.client_state, "base64").toString("utf8")) : {});
      await sendVoicemailEmail({
        branch,
        caller: payload?.from || payload?.from_number || "Unknown",
        recordingUrl: "(no recording in this minimal build)",
        transcript: null,
      });
      return res.json({ ok: true });
    }

    res.json({ ok: true });
  } catch (err) {
    console.error("[/telnyx/inbound] error:", err?.response?.data || err.message);
    res.status(200).json({ ok: true }); // acknowledge to avoid webhook retries
  }
});

// ------------------------ Start --------------------------
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`WHC server listening on :${PORT}`));
