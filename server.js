// server.js — WHC PBX + Calendar + Jobs (Retell + Telnyx + Email)
// Birthdays + Procedure Reminders + Consult Reminders + Follow-ups
// Webhook write-back + Calendar→Sheets sync for Procedures & Consults
// -----------------------------------------------------------------

import express from "express";
import cors from "cors";
import { google } from "googleapis";
import axios from "axios";
import nodemailer from "nodemailer";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc.js";
import timezone from "dayjs/plugin/timezone.js";
dayjs.extend(utc);
dayjs.extend(timezone);

// ---------------- Time / TZ ----------------
const LOCAL_TZ = process.env.LOCAL_TZ || "America/Jamaica";
const nowTz = () => dayjs().tz(LOCAL_TZ);
const isoLocal = () => nowTz().toISOString();
const parseLocalIso = (s) => (s ? dayjs(s).tz(LOCAL_TZ) : null);

// ---------------- App ----------------------
const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));
app.get("/health", (_req, res) => res.status(200).send("ok"));
app.use((req, _res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`);
  next();
});

// ---------------- Config -------------------
const STATUS_TOKEN = process.env.STATUS_TOKEN || "";
const HANDOFF_TIMEOUT_MS = Number(process.env.HANDOFF_TIMEOUT_MS || 25000);
const MOH_URL = process.env.MOH_URL || "https://example.com/moh.mp3";

// Calling window (local minutes)
const CALL_WINDOW_START_MIN = Number(process.env.CALL_WINDOW_START_MIN || 8 * 60);
const CALL_WINDOW_END_MIN = Number(process.env.CALL_WINDOW_END_MIN || 18 * 60);
const insideWindowNow = () => {
  const t = nowTz();
  const m = t.hour() * 60 + t.minute();
  return m >= CALL_WINDOW_START_MIN && m <= CALL_WINDOW_END_MIN;
};

// Branch phones (transfer targets)
const BRANCH_NUMBERS = {
  winchester: process.env.BRANCH_WINCHESTER || "+18769082658",
  portmore: process.env.BRANCH_PORTMORE || "+18767042739",
  ardenne: process.env.BRANCH_ARDENNE || "+18766713825",
  sav: process.env.BRANCH_SAV || "+18769540252",
};

// Branch emails (voicemail)
const BRANCH_EMAILS = {
  winchester: process.env.EMAIL_WINCHESTER || "winchester@winchesterheartcentre.com",
  portmore: process.env.EMAIL_PORTMORE || "portmore@winchesterheartcentre.com",
  ardenne: process.env.EMAIL_ARDENNE || "ardenne@winchesterheartcentre.com",
  sav: process.env.EMAIL_SAV || "savlamar@winchesterheartcentre.com",
};
const FROM_EMAIL = process.env.FROM_EMAIL || "Winchester Heart Centre <no-reply@whc.local>";

// Physician calendars
const PHYSICIANS = {
  dr_emery: "uh7ehq6qg5c1qfdciic3v8l0s8@group.calendar.google.com",
  dr_thompson: "eburtl0ebphsp3h9qdfurpbqeg@group.calendar.google.com",
  dr_dowding: "a70ab6c4e673f04f6d40fabdb0f4861cf2fac5874677d5dd9961e357b8bb8af9@group.calendar.google.com",
  dr_blair: "ad21642079da12151a39c9a5aa455d56c306cfeabdfd712fb34a4378c3f04c4a@group.calendar.google.com",
  dr_williams: "7343219d0e34a585444e2a39fd1d9daa650e082209a9e5dc85e0ce73d63c7393@group.calendar.google.com",
  dr_wright: "b8a27f6d34e63806408f975bf729a3089b0d475b1b58c18ae903bc8bc63aa0ea@group.calendar.google.com",
  dr_dixon: "ed382c812be7a6d3396a874ca19368f2d321805f80526e6f3224f713f0637cee@group.calendar.google.com",
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

// Procedure calendars by branch (IDs provided)
const PROCEDURE_CALENDARS = {
  winchester: "a434db3192bb86669c1238fd8840d62d72a254cea1b58d01da3dd61eefeb1ba6@group.calendar.google.com",
  ardenne: "d86eacf4f06f69310210007c96a91941ba3f0eb37a6165e350f3227b604cee06@group.calendar.google.com",
  portmore: "1dce088aed350313f0848a6950f97fdaa44616145b523097b23877f3aa0278a5@group.calendar.google.com",
  sav: "79ff142150e20453f5600b25f445c1d27f03d30594dadc5c777b4ed9af360e5e@group.calendar.google.com",
};

// --------------- Google Auth ----------------
function loadServiceAccountJSON() {
  const inline = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;
  const b64 = process.env.GOOGLE_CREDENTIALS_B64;
  if (inline) {
    const txt = inline.trim();
    return JSON.parse(txt.startsWith("{") ? txt : Buffer.from(txt, "base64").toString("utf8"));
  }
  if (b64) return JSON.parse(Buffer.from(b64, "base64").toString("utf8"));
  throw new Error("Missing Google credentials env");
}
function getJWTAuth(scopes) {
  const creds = loadServiceAccountJSON();
  return new google.auth.JWT(creds.client_email, null, creds.private_key, scopes);
}

// --------------- Calendar helpers ----------
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
    const auth = getJWTAuth(["https://www.googleapis.com/auth/calendar"]);
    const calendar = google.calendar({ version: "v3", auth });
    const description = [phone ? `Phone: ${phone}` : null, note ? `Note: ${note}` : null]
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
    const auth = getJWTAuth(["https://www.googleapis.com/auth/calendar"]);
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
    const auth = getJWTAuth(["https://www.googleapis.com/auth/calendar"]);
    const calendar = google.calendar({ version: "v3", auth });
    await calendar.events.delete({ calendarId: id, eventId });
    return true;
  },
};

// --------------- Email ----------------------
function makeTransport() {
  const host = process.env.SMTP_HOST,
    port = Number(process.env.SMTP_PORT || 587),
    user = process.env.SMTP_USER,
    pass = process.env.SMTP_PASS;
  if (!host || !user || !pass) return null;
  return nodemailer.createTransport({ host, port, secure: port === 465, auth: { user, pass } });
}
async function sendVoicemailEmail({ branch, caller, recordingUrl, transcript }) {
  const to = BRANCH_EMAILS[branch] || BRANCH_EMAILS.winchester;
  const transporter = makeTransport();
  if (!transporter) {
    console.warn("[Email] SMTP not configured");
    return;
  }
  const html = `
    <p><b>New voicemail</b> for <b>${branch}</b></p>
    <p><b>From:</b> ${caller || "Unknown"}</p>
    <p><b>Recording:</b> <a href="${recordingUrl}">${recordingUrl}</a></p>
    ${transcript ? `<pre>${transcript}</pre>` : ""}`;
  await transporter.sendMail({ from: FROM_EMAIL, to, subject: `New Voicemail - ${branch} branch`, html });
}

// --------------- Calendar Routes ------------
app.post("/calendar/create", async (req, res) => {
  try {
    const { physician, start, end, summary, phone, note } = req.body || {};
    if (!physician || !start || !end) return res.status(400).json({ ok: false, error: "physician, start, end required" });
    const { key, event } = await Calendar.createEvent({ physician, start, end, summary, phone, note });
    res.json({ ok: true, eventId: event.id, htmlLink: event.htmlLink, response: `Created appointment for ${PHYSICIAN_DISPLAY[key] || key}` });
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
      events: items.map((ev) => ({
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

// --------------- Retell in-call -------------
app.post("/retell/action", async (req, res) => {
  try {
    const e = req.body || {};
    const action = String(e.action || "").toLowerCase();
    if (action.includes("book")) {
      const r = await Calendar.createEvent({ physician: e.physician, start: e.start, end: e.end, summary: e.summary, phone: e.phone, note: e.note });
      return res.json({ ok: true, response: `Booked for ${PHYSICIAN_DISPLAY[r.key] || r.key}`, eventId: r.event.id });
    }
    if (action.includes("next")) {
      const u = await Calendar.upcoming(e.physician, 1);
      if (!u.items.length) return res.json({ ok: true, response: "No upcoming events." });
      const first = u.items[0];
      return res.json({ ok: true, response: `Next for ${PHYSICIAN_DISPLAY[u.key] || u.key}: ${first.summary} at ${first.start?.dateTime || first.start?.date}`, eventId: first.id });
    }
    res.json({ ok: true, response: "Ready." });
  } catch (err) {
    console.error("[/retell/action] error:", err?.response?.data || err.message);
    res.json({ ok: false, response: "Error." });
  }
});

// --------------- Telnyx PBX -----------------
const TELNYX_API_KEY = process.env.TELNYX_API_KEY;
const TELNYX_OUTBOUND_CALLER_ID = process.env.TELNYX_OUTBOUND_CALLER_ID;
const telnyx = TELNYX_API_KEY
  ? axios.create({ baseURL: "https://api.telnyx.com/v2/", headers: { Authorization: `Bearer ${TELNYX_API_KEY}` } })
  : null;
async function tx(cmd, payload) {
  if (!telnyx) throw new Error("Telnyx not configured");
  return telnyx.post(`call_commands/${cmd}`, payload);
}
function resolveBranchFromMeta(meta = {}) {
  const m = (meta.branch || meta.forwarded_from || "").toString().toLowerCase();
  if (m.includes("portmore")) return "portmore";
  if (m.includes("ardenne")) return "ardenne";
  if (m.includes("sav")) return "sav";
  return "winchester";
}
function isOfficeOpen(branchKey, when = nowTz()) {
  const dow = when.day();
  const minutes = when.hour() * 60 + when.minute();
  if (dow === 0) return false; // Sunday
  if (branchKey === "portmore") {
    if (dow >= 1 && dow <= 5) return minutes >= 600 && minutes < 1020;
    if (dow === 6) return minutes >= 600 && minutes < 840;
    return false;
  }
  if (dow >= 1 && dow <= 5) return minutes >= 510 && minutes < 990; // 08:30–16:30
  return false;
}
app.post("/telnyx/inbound", async (req, res) => {
  if (!telnyx) return res.status(200).json({ ok: true, note: "Telnyx not configured" });
  try {
    const data = req.body?.data || {};
    const eventType = data?.event_type;
    const payload = data?.payload || {};
    const callControlId = payload.call_control_id;
    if (eventType === "call.initiated") {
      await tx("answer", { call_control_id: callControlId });
      const meta = payload.client_state ? JSON.parse(Buffer.from(payload.client_state, "base64").toString("utf8")) : {};
      const branch = resolveBranchFromMeta(meta);
      if (!isOfficeOpen(branch)) {
        await sendVoicemailEmail({ branch, caller: payload?.from || payload?.from_number || "Unknown", recordingUrl: "(no recording)", transcript: null });
        try {
          await tx("hangup", { call_control_id: callControlId });
        } catch {}
        return res.json({ ok: true, note: "after-hours; message taken" });
      }
      await tx("playback_start", { call_control_id: callControlId, audio_url: MOH_URL, overlay: true, loop: true });
      const target = BRANCH_NUMBERS[branch] || BRANCH_NUMBERS.winchester;
      await tx("transfer", { call_control_id: callControlId, to: target, from: TELNYX_OUTBOUND_CALLER_ID, timeout_secs: Math.ceil(HANDOFF_TIMEOUT_MS / 1000) });
      return res.json({ ok: true });
    }
    if (eventType === "call.bridged") {
      await tx("playback_stop", { call_control_id: callControlId });
      return res.json({ ok: true });
    }
    if (eventType === "call.ended" || eventType === "transfer.failed") {
      const meta = payload.client_state ? JSON.parse(Buffer.from(payload.client_state, "base64").toString("utf8")) : {};
      const branch = resolveBranchFromMeta(meta);
      await sendVoicemailEmail({ branch, caller: payload?.from || payload?.from_number || "Unknown", recordingUrl: "(no recording)", transcript: null });
      return res.json({ ok: true });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("[/telnyx/inbound] error:", err?.response?.data || err.message);
    res.status(200).json({ ok: true });
  }
});

// --------------- Jobs scaffolding -----------
function assertToken(req, res) {
  const t = String(req.query.token || "");
  if (!STATUS_TOKEN || t !== STATUS_TOKEN) {
    res.status(401).json({ ok: false, error: "unauthorized" });
    return false;
  }
  return true;
}
app.post("/jobs/run-reminders", async (req, res) => {
  if (!assertToken(req, res)) return;
  try {
    return res.json({ ok: true, results: [] });
  } catch (err) {
    console.error("[/jobs/run-reminders] error:", err?.response?.data || err.message);
    return res.status(200).json({ ok: false, error: err.message });
  }
});

// --------------- Retell Outbound -----------
const ENABLE_BDAY_DIAL = String(process.env.ENABLE_BDAY_DIAL || "").trim() === "1";
const ENABLE_PROC_DIAL = String(process.env.ENABLE_PROC_DIAL || "").trim() === "1";
const ENABLE_CONSULT_DIAL = String(process.env.ENABLE_CONSULT_DIAL || "").trim() === "1";

const RETELL_API_KEY = process.env.RETELL_API_KEY || "";
const RETELL_OUTBOUND_AGENT_ID = process.env.RETELL_OUTBOUND_AGENT_ID || "";
const RETELL_OUTBOUND_FROM = process.env.RETELL_OUTBOUND_FROM || ""; // +1305...
const RETELL_OUTBOUND_URL = process.env.RETELL_OUTBOUND_URL || "https://api.retellai.com/v2/outbound-calls";
async function placeRetellCall({ to, from, agent_id, variables, metadata, webhook_url }) {
  if (!RETELL_API_KEY || !agent_id || !from) throw new Error("Retell outbound not configured");
  const client = axios.create({ headers: { Authorization: `Bearer ${RETELL_API_KEY}` }, timeout: 15000 });
  const body = { to, from, agent_id, variables, metadata, webhook_url };
  const { data } = await client.post(RETELL_OUTBOUND_URL, body);
  return data; // { call_id, status } expected
}

// --------------- Birthdays Job -------------
app.post("/jobs/run-birthdays", async (req, res) => {
  if (!assertToken(req, res)) return;
  try {
    const creds = loadServiceAccountJSON();
    const auth = new google.auth.JWT(creds.client_email, null, creds.private_key, ["https://www.googleapis.com/auth/spreadsheets"]);
    const sheets = google.sheets({ version: "v4", auth });

    const SHEET_ID = process.env.BIRTHDAYS_SHEET_ID;
    const RANGE = process.env.BIRTHDAYS_RANGE || "Sheet1!A:Z";
    if (!SHEET_ID) return res.json({ ok: false, error: "missing BIRTHDAYS_SHEET_ID" });

    const read = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: RANGE,
      valueRenderOption: "UNFORMATTED_VALUE",
      dateTimeRenderOption: "FORMATTED_STRING",
    });
    const rows = read.data.values || [];
    if (!rows.length) return res.json({ ok: true, processed: 0, details: [] });

    const headers = (rows[0] || []).map((h) => String(h || "").trim().toLowerCase());
    const col = (n) => headers.indexOf(String(n).trim().toLowerCase());
    const idx = {
      full_name: col("full_name"),
      phone: col("phone_e164"),
      dob: col("dob_yyyy_mm_dd"),
      branch: col("branch"),
      opt_out: col("opt_out"),
      opt_out_reason: col("opt_out_reason"),
      last_called_year: col("last_called_year"),
      last_outcome: col("last_outcome"),
      last_outcome_ts: col("last_outcome_ts"),
      deferred_for: col("deferred_for_yyyy_mm_dd"),
      deferred_reason: col("deferred_reason"),
      dob_correction: col("dob_correction"),
      new_phone_candidate: col("new_phone_candidate"),
      status_flag: col("status_flag"),
      status_note: col("status_note"),
      preferred_contact: col("preferred_contact"),
      caregiver_name: col("caregiver_name"),
      caregiver_phone: col("caregiver_phone"),
      pause_until: col("pause_until_yyyy_mm_dd"),
      last_call_id: col("last_call_id"),
      retry_count: col("retry_count"),
    };
    const val = (r, i) => (i >= 0 && r[i] != null ? String(r[i]).trim() : "");
    const asBool = (s) => /^true|1|yes|y$/i.test(String(s || "").trim());
    const parseYmd = (s) => {
      const t = String(s || "").trim();
      if (!t) return null;
      const m = dayjs(t, ["YYYY-MM-DD", "YYYY/M/D", "YYYY/M/DD", "YYYY/MM/D", "YYYY/MM/DD"], true);
      return m.isValid() ? m : null;
    };

    const today = nowTz();
    const todayY = today.year();
    const todayMD = today.format("MM-DD");
    const insideWindow = insideWindowNow();

    const updates = [];
    const rangePart = (RANGE.split("!")[1] || "A:Z");
    const startColLetter = rangePart.split(":")[0].replace(/[0-9]/g, "") || "A";
    const letterToIndex = (L) => L.split("").reduce((n, ch) => n * 26 + (ch.charCodeAt(0) - 64), 0) - 1;
    const indexToLetters = (n) => {
      let s = "";
      n++;
      while (n > 0) {
        const rem = (n - 1) % 26;
        s = String.fromCharCode(65 + rem) + s;
        n = Math.floor((n - 1) / 26);
      }
      return s;
    };
    const startBase = letterToIndex(startColLetter.toUpperCase());
    const setCell = (row1, col0, val_) => {
      const a1 = `${indexToLetters(startBase + col0)}${row1}`;
      updates.push({ range: `${RANGE.split("!")[0]}!${a1}`, values: [[val_]] });
    };

    for (let r = 1; r < rows.length; r++) {
      const row = rows[r] || [];
      const row1 = r + 1;

      const fullName = val(row, idx.full_name);
      const phone = val(row, idx.phone);
      const dobStr = val(row, idx.dob);
      const branch = (val(row, idx.branch).toLowerCase() || "winchester");
      const optOut = asBool(val(row, idx.opt_out));
      const statusFlag = val(row, idx.status_flag).toUpperCase();
      const pauseUntil = parseYmd(val(row, idx.pause_until));
      const deferredFor = parseYmd(val(row, idx.deferred_for));
      const dobCorrection = parseYmd(val(row, idx.dob_correction));
      const lastCalledYear = val(row, idx.last_called_year);

      const dob = dobCorrection || parseYmd(dobStr);
      if (!dob || !fullName) continue;
      if (dob.tz(LOCAL_TZ).format("MM-DD") !== todayMD) continue;

      const writeOutcome = (k) => {
        if (idx.last_outcome >= 0) setCell(row1, idx.last_outcome, k);
        if (idx.last_outcome_ts >= 0) setCell(row1, idx.last_outcome_ts, isoLocal());
      };
      const writeYearNow = () => {
        if (idx.last_called_year >= 0) setCell(row1, idx.last_called_year, String(todayY));
      };
      const writeCallId = (id) => {
        if (idx.last_call_id >= 0 && id) setCell(row1, idx.last_call_id, id);
      };

      if (optOut) {
        writeOutcome("skipped_opt_out");
        continue;
      }
      if (statusFlag === "DO_NOT_CALL" || statusFlag === "INACTIVE") {
        writeOutcome(`skipped_${statusFlag.toLowerCase()}`);
        continue;
      }
      if (pauseUntil && pauseUntil.tz(LOCAL_TZ).isAfter(today, "day")) {
        writeOutcome("paused_until");
        continue;
      }
      if (deferredFor && deferredFor.tz(LOCAL_TZ).isAfter(today, "day")) {
        writeOutcome("deferred");
        continue;
      }
      if (!insideWindow) {
        writeOutcome("skipped_outside_window");
        continue;
      }
      if (!phone) {
        writeOutcome("skipped_missing_phone");
        continue;
      }
      if (String(lastCalledYear || "").trim() === String(todayY)) {
        writeOutcome("skipped_already_called_this_year");
        continue;
      }

      if (ENABLE_BDAY_DIAL && RETELL_API_KEY && RETELL_OUTBOUND_AGENT_ID && RETELL_OUTBOUND_FROM) {
        try {
          const webhookUrl = `${process.env.PUBLIC_BASE_URL || ""}/retell/outbound/callback?token=${encodeURIComponent(STATUS_TOKEN)}`;
          const meta = { kind: "birthday", sheet_id: SHEET_ID, range: RANGE, row_number: row1 };
          const variables = { call_type: "birthday", patient_name: fullName, branch, local_time: nowTz().format("h:mm A") };
          const dial = await placeRetellCall({
            to: phone,
            from: RETELL_OUTBOUND_FROM,
            agent_id: RETELL_OUTBOUND_AGENT_ID,
            variables,
            metadata: meta,
            webhook_url: webhookUrl,
          });
          writeYearNow();
          writeOutcome("call_placed");
          writeCallId(dial?.call_id || dial?.id || "");
        } catch (e) {
          writeOutcome("dial_error");
        }
      } else {
        writeYearNow();
        writeOutcome("queued");
      }
    }

    if (updates.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption: "RAW", data: updates },
      });
    }
    return res.json({ ok: true });
  } catch (err) {
    console.error("[/jobs/run-birthdays] error:", err?.response?.data || err.message);
    return res.status(200).json({ ok: false, error: err.message });
  }
});

// --------------- Procedures Reminders -------
app.post("/jobs/run-procedure-reminders", async (req, res) => {
  if (!assertToken(req, res)) return;
  try {
    const creds = loadServiceAccountJSON();
    const auth = new google.auth.JWT(creds.client_email, null, creds.private_key, ["https://www.googleapis.com/auth/spreadsheets"]);
    const sheets = google.sheets({ version: "v4", auth });

    const SHEET_ID = process.env.PROCEDURES_SHEET_ID;
    const RANGE = process.env.PROCEDURES_RANGE || "Sheet1!A:Z";
    if (!SHEET_ID) return res.json({ ok: false, error: "missing PROCEDURES_SHEET_ID" });

    const read = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: RANGE,
      valueRenderOption: "UNFORMATTED_VALUE",
      dateTimeRenderOption: "FORMATTED_STRING",
    });
    const rows = read.data.values || [];
    if (!rows.length) return res.json({ ok: true, processed: 0 });

    const headers = (rows[0] || []).map((h) => String(h || "").trim().toLowerCase());
    const col = (n) => headers.indexOf(String(n).trim().toLowerCase());
    const idx = {
      event_id: col("event_id"),
      full_name: col("full_name") >= 0 ? col("full_name") : col("patient_name"),
      phone: col("phone_e164"),
      appt_iso: col("appt_iso"),
      appt_date: col("appt_yyyy_mm_dd"),
      appt_time: col("appt_time_hh_mm"),
      branch: col("branch"),
      service: col("service"),
      opt_out: col("opt_out"),
      status_flag: col("status_flag"),
      status_note: col("status_note"),
      pause_until: col("pause_until_yyyy_mm_dd"),
      last_reminded_offset: col("last_reminded_offset"),
      last_outcome: col("last_outcome"),
      last_outcome_ts: col("last_outcome_ts"),
      last_call_id: col("last_call_id"),
      retry_count: col("retry_count"),
      deferred_for: col("deferred_for_yyyy_mm_dd"),
      deferred_reason: col("deferred_reason"),
    };
    const val = (r, i) => (i >= 0 && r[i] != null ? String(r[i]).trim() : "");
    const asBool = (s) => /^true|1|yes|y$/i.test(String(s || "").trim());
    const parseYmd = (s) => {
      const t = String(s || "").trim();
      if (!t) return null;
      const m = dayjs(t, ["YYYY-MM-DD", "YYYY/M/D", "YYYY/M/DD", "YYYY/MM/D", "YYYY/MM/DD"], true).tz(LOCAL_TZ);
      return m.isValid() ? m : null;
    };

    const offsets = (process.env.PROC_REMINDER_OFFSETS || "7,3,1")
      .split(",")
      .map((s) => parseInt(s.trim(), 10))
      .filter((n) => Number.isFinite(n) && n >= 0);
    const today = nowTz().startOf("day");
    const insideWindow = insideWindowNow();

    const rangePart = (RANGE.split("!")[1] || "A:Z");
    const startColLetter = rangePart.split(":")[0].replace(/[0-9]/g, "") || "A";
    const letterToIndex = (L) => L.split("").reduce((n, ch) => n * 26 + (ch.charCodeAt(0) - 64), 0) - 1;
    const indexToLetters = (n) => {
      let s = "";
      n++;
      while (n > 0) {
        const r = (n - 1) % 26;
        s = String.fromCharCode(65 + r) + s;
        n = Math.floor((n - 1) / 26);
      }
      return s;
    };
    const startBase = letterToIndex(startColLetter.toUpperCase());
    const updates = [];
    const setCell = (row1, col0, val_) => {
      if (col0 < 0) return;
      const a1 = `${indexToLetters(startBase + col0)}${row1}`;
      updates.push({ range: `${RANGE.split("!")[0]}!${a1}`, values: [[val_]] });
    };

    for (let r = 1; r < rows.length; r++) {
      const row = rows[r] || [];
      const row1 = r + 1;
      const fullName = val(row, idx.full_name),
        phone = val(row, idx.phone),
        branch = (val(row, idx.branch).toLowerCase() || "winchester");
      const optOut = asBool(val(row, idx.opt_out));
      const statusFlag = val(row, idx.status_flag).toUpperCase();
      const pauseUntil = parseYmd(val(row, idx.pause_until));
      const apptIso = val(row, idx.appt_iso);

      let appt = null;
      if (apptIso) {
        const m = parseLocalIso(apptIso);
        appt = m?.isValid() ? m : null;
      } else {
        const d = parseYmd(val(row, idx.appt_date));
        if (d) {
          const t = val(row, idx.appt_time);
          if (t && /^\d{1,2}:\d{2}$/.test(t)) {
            const [H, M] = t.split(":").map(Number);
            appt = d.hour(H).minute(M);
          } else appt = d.hour(9).minute(0);
        }
      }
      if (!fullName || !phone || !appt) continue;

      const daysUntil = appt.startOf("day").diff(today, "day");
      if (!offsets.includes(daysUntil)) continue;
      if (optOut) {
        setCell(row1, idx.last_outcome, "skipped_opt_out");
        continue;
      }
      if (statusFlag === "DO_NOT_CALL" || statusFlag === "INACTIVE") {
        setCell(row1, idx.last_outcome, `skipped_${statusFlag.toLowerCase()}`);
        continue;
      }
      if (pauseUntil && pauseUntil.isAfter(today, "day")) {
        setCell(row1, idx.last_outcome, "paused_until");
        continue;
      }
      if (!insideWindow) {
        setCell(row1, idx.last_outcome, "skipped_outside_window");
        continue;
      }

      const service = val(row, idx.service) || "procedure";
      const apptNice = appt.format("dddd, MMM D [at] h:mm A");
      if (ENABLE_PROC_DIAL && RETELL_API_KEY && RETELL_OUTBOUND_AGENT_ID && RETELL_OUTBOUND_FROM) {
        try {
          const webhookUrl = `${process.env.PUBLIC_BASE_URL || ""}/retell/outbound/callback?token=${encodeURIComponent(STATUS_TOKEN)}`;
          const meta = { kind: "procedure", sheet_id: SHEET_ID, range: RANGE, row_number: row1 };
          const variables = { call_type: "reminder", patient_name: fullName, branch, service, appointment_time: apptNice };
          const dial = await placeRetellCall({
            to: phone,
            from: RETELL_OUTBOUND_FROM,
            agent_id: RETELL_OUTBOUND_AGENT_ID,
            variables,
            metadata: meta,
            webhook_url: webhookUrl,
          });
          if (idx.last_outcome >= 0) setCell(row1, idx.last_outcome, "call_placed");
          if (idx.last_outcome_ts >= 0) setCell(row1, idx.last_outcome_ts, isoLocal());
          if (idx.last_reminded_offset >= 0) setCell(row1, idx.last_reminded_offset, String(daysUntil));
          if (idx.last_call_id >= 0 && (dial?.call_id || dial?.id)) setCell(row1, idx.last_call_id, dial?.call_id || dial?.id);
        } catch (e) {
          if (idx.last_outcome >= 0) setCell(row1, idx.last_outcome, "dial_error");
          if (idx.last_outcome_ts >= 0) setCell(row1, idx.last_outcome_ts, isoLocal());
        }
      } else {
        if (idx.last_outcome >= 0) setCell(row1, idx.last_outcome, "queued");
        if (idx.last_outcome_ts >= 0) setCell(row1, idx.last_outcome_ts, isoLocal());
        if (idx.last_reminded_offset >= 0) setCell(row1, idx.last_reminded_offset, String(daysUntil));
      }
    }

    if (updates.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption: "RAW", data: updates },
      });
    }
    return res.json({ ok: true });
  } catch (err) {
    console.error("[/jobs/run-procedure-reminders] error:", err?.response?.data || err.message);
    return res.status(200).json({ ok: false, error: err.message });
  }
});

// --------------- Retry Helpers -------------
const RETRY_ENABLE = String(process.env.RETRY_ENABLE || "").trim() === "1";
const RETRY_OUTCOMES = (process.env.RETRY_OUTCOMES || "no_answer,left_voicemail").split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
const RETRY_MAX = Number(process.env.RETRY_MAX || 2);
const RETRY_SKIP_WEEKENDS = String(process.env.RETRY_SKIP_WEEKENDS || "1") === "1";
function nextRetryDate(base = nowTz()) {
  let d = base.add(1, "day").startOf("day");
  if (RETRY_SKIP_WEEKENDS) {
    while (d.day() === 0 || d.day() === 6) d = d.add(1, "day");
  }
  return d;
}

// --------------- Retell Webhook w/ Retries -
app.post("/retell/outbound/callback", async (req, res) => {
  const token = String(req.query.token || "");
  if (!STATUS_TOKEN || token !== STATUS_TOKEN) return res.status(401).json({ ok: false });
  try {
    const body = req.body || {};
    const callId = body.call_id || body.id || "";
    const final = (body.final_status || body.status || "").toLowerCase();
    const meta = body.metadata || {};

    const SHEET_ID = meta.sheet_id || "";
    const RANGE = meta.range || "Sheet1!A:Z";
    const row1 = Number(meta.row_number || 0);
    const kind = String(meta.kind || "").toLowerCase(); // "birthday" | "procedure" | "consult" | "followup"

    if (!SHEET_ID || !row1) return res.json({ ok: true, note: "missing sheet/row" });

    const creds = loadServiceAccountJSON();
    const auth = new google.auth.JWT(creds.client_email, null, creds.private_key, ["https://www.googleapis.com/auth/spreadsheets"]);
    const sheets = google.sheets({ version: "v4", auth });

    const read = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: RANGE,
      valueRenderOption: "UNFORMATTED_VALUE",
      dateTimeRenderOption: "FORMATTED_STRING",
    });
    const rows = read.data.values || [];
    if (!rows.length) return res.json({ ok: false, error: "empty sheet" });

    const headers = (rows[0] || []).map((h) => String(h || "").trim().toLowerCase());
    const col = (n) => headers.indexOf(String(n).trim().toLowerCase());
    const idx = {
      last_outcome: col("last_outcome"),
      last_outcome_ts: col("last_outcome_ts"),
      last_called_year: col("last_called_year"), // birthday-only
      last_call_id: col("last_call_id"),

      deferred_for: col("deferred_for_yyyy_mm_dd"),
      deferred_reason: col("deferred_reason"),
      retry_count: col("retry_count"),

      // consult/followup extras
      last_followup_year: col("last_followup_year"),
      last_followup_outcome: col("last_followup_outcome"),
      last_followup_ts: col("last_followup_ts"),
    };

    const rangePart = (RANGE.split("!")[1] || "A:Z");
    const startColLetter = rangePart.split(":")[0].replace(/[0-9]/g, "") || "A";
    const letterToIndex = (L) => L.split("").reduce((n, ch) => n * 26 + (ch.charCodeAt(0) - 64), 0) - 1;
    const indexToLetters = (n) => {
      let s = "";
      n++;
      while (n > 0) {
        const r = (n - 1) % 26;
        s = String.fromCharCode(65 + r) + s;
        n = Math.floor((n - 1) / 26);
      }
      return s;
    };
    const startBase = letterToIndex(startColLetter.toUpperCase());
    const updates = [];
    const setCell = (row1_, col0, val_) => {
      if (col0 < 0) return;
      const a1 = `${indexToLetters(startBase + col0)}${row1_}`;
      updates.push({ range: `${RANGE.split("!")[0]}!${a1}`, values: [[val_]] });
    };
    const getCell = (row1_, col0) => {
      if (col0 < 0) return "";
      const row = rows[row1_ - 1] || [];
      return row[col0] != null ? String(row[col0]).trim() : "";
    };

    // Map Retell final statuses → our outcomes
    let outcome = "completed";
    if (final.includes("voicemail")) outcome = "left_voicemail";
    else if (final.includes("no_answer") || final.includes("noanswer")) outcome = "no_answer";
    else if (final.includes("cancel")) outcome = "cancelled";
    else if (final.includes("error") || final.includes("failed")) outcome = "dial_error";
    else if (final.includes("complete")) {
      if (kind === "birthday") outcome = "wished_happy_birthday";
      else if (kind === "followup") outcome = "followup_completed";
      else outcome = "reminder_completed";
    }

    // Base write-back
    setCell(row1, idx.last_outcome, outcome);
    setCell(row1, idx.last_outcome_ts, isoLocal());
    if (kind === "birthday" && idx.last_called_year >= 0) {
      setCell(row1, idx.last_called_year, String(nowTz().year()));
    }
    if (idx.last_call_id >= 0 && callId) setCell(row1, idx.last_call_id, callId);

    // Special follow-up columns
    if (kind === "followup") {
      if (idx.last_followup_outcome >= 0) setCell(row1, idx.last_followup_outcome, outcome);
      if (idx.last_followup_ts >= 0) setCell(row1, idx.last_followup_ts, isoLocal());
      if (idx.last_followup_year >= 0) setCell(row1, idx.last_followup_year, String(nowTz().year()));
    }

    // Retry logic
    const retryEligible = RETRY_ENABLE && RETRY_OUTCOMES.includes(outcome);
    if (retryEligible) {
      let currentRetries = 0;
      if (idx.retry_count >= 0) {
        const cur = parseInt(getCell(row1, idx.retry_count), 10);
        if (Number.isFinite(cur)) currentRetries = cur;
      }
      if (currentRetries < RETRY_MAX) {
        const nextDate = nextRetryDate(nowTz()).format("YYYY-MM-DD");
        if (idx.deferred_for >= 0) setCell(row1, idx.deferred_for, nextDate);
        if (idx.deferred_reason >= 0) {
          const prev = getCell(row1, idx.deferred_reason);
          const note = `[${isoLocal()}] auto-retry due to ${outcome}; next=${nextDate}`;
          setCell(row1, idx.deferred_reason, prev ? `${prev} | ${note}` : note);
        }
        if (idx.retry_count >= 0) setCell(row1, idx.retry_count, String(currentRetries + 1));
      } else {
        if (idx.deferred_reason >= 0) {
          const prev = getCell(row1, idx.deferred_reason);
          const note = `[${isoLocal()}] retries_exhausted (${RETRY_MAX}) after outcome=${outcome}`;
          setCell(row1, idx.deferred_reason, prev ? `${prev} | ${note}` : note);
        }
        if (idx.deferred_for >= 0) setCell(row1, idx.deferred_for, "");
      }
    } else {
      if (idx.deferred_for >= 0) setCell(row1, idx.deferred_for, "");
    }

    if (updates.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption: "RAW", data: updates },
      });
    }
    return res.json({ ok: true });
  } catch (err) {
    console.error("[/retell/outbound/callback] error:", err?.response?.data || err.message);
    return res.status(200).json({ ok: false, error: err.message });
  }
});

// --------------- Procedures: Calendar → Sheet Sync ----------
app.post("/jobs/sync-procedures", async (req, res) => {
  if (!assertToken(req, res)) return;
  try {
    const SHEET_ID = process.env.PROCEDURES_SHEET_ID;
    const RANGE = process.env.PROCEDURES_RANGE || "Sheet1!A:Z";
    if (!SHEET_ID) return res.json({ ok: false, error: "missing PROCEDURES_SHEET_ID" });

    const lookAheadDays = Number(process.env.PROC_SYNC_LOOKAHEAD_DAYS || 30);
    const lookBackDays = Number(process.env.PROC_SYNC_LOOKBACK_DAYS || 1);
    const timeMin = nowTz().subtract(lookBackDays, "day").toISOString();
    const timeMax = nowTz().add(lookAheadDays, "day").toISOString();

    const authCal = getJWTAuth(["https://www.googleapis.com/auth/calendar"]);
    const calendar = google.calendar({ version: "v3", auth: authCal });
    const authSheets = getJWTAuth(["https://www.googleapis.com/auth/spreadsheets"]);
    const sheets = google.sheets({ version: "v4", auth: authSheets });

    const read = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: RANGE,
      valueRenderOption: "UNFORMATTED_VALUE",
      dateTimeRenderOption: "FORMATTED_STRING",
    });
    const rows = read.data.values || [];
    const headers = rows[0] || [];
    const lower = headers.map((h) => String(h || "").trim().toLowerCase());
    const colIndex = (name) => lower.indexOf(String(name).toLowerCase());
    const idx = {
      event_id: colIndex("event_id"),
      full_name: colIndex("full_name") >= 0 ? colIndex("full_name") : colIndex("patient_name"),
      phone: colIndex("phone_e164"),
      appt_iso: colIndex("appt_iso"),
      branch: colIndex("branch"),
      service: colIndex("service"),
    };
    const haveHeaders = ["event_id", "full_name", "phone_e164", "appt_iso", "branch", "service"].every((h) => colIndex(h) >= 0);
    if (!haveHeaders) return res.json({ ok: false, error: "Procedure sheet missing headers: event_id, full_name, phone_e164, appt_iso, branch, service" });

    const existing = new Map();
    for (let r = 1; r < rows.length; r++) {
      const rid = rows[r]?.[idx.event_id];
      if (rid) existing.set(String(rid).trim(), r + 1);
    }

    const appends = [];
    const updates = [];
    const inferService = (s) => {
      const t = String(s || "").toLowerCase();
      if (t.includes("dobutamine")) return "dobutamine stress echo";
      if (t.includes("stress") && t.includes("echo")) return "stress echocardiogram";
      if (t.includes("stress") && t.includes("ecg")) return "stress ecg";
      if (t.includes("stress")) return "stress test";
      if (t.includes("bubble")) return "bubble echocardiogram";
      if (t.includes("holter")) return "holter monitor";
      if (t.includes("abpm") || t.includes("ambulatory blood pressure")) return "abpm";
      if (t.includes("pacemaker")) return "pacemaker interrogation";
      if (t.includes("echocardiogram") || t.includes("echo")) return "echocardiogram";
      if (t.includes("ecg") || t.includes("electrocardiogram")) return "ecg";
      return "procedure";
    };
    const parseDesc = (desc) => {
      const out = { name: "", phone: "" };
      const d = String(desc || "");
      const m1 = d.match(/name\s*:\s*(.+)/i);
      if (m1) out.name = m1[1].trim();
      const m2 = d.match(/phone\s*:\s*([+0-9\-\s]+)/i);
      if (m2) out.phone = m2[1].replace(/\s+/g, "").replace(/-/g, "");
      return out;
    };

    for (const [branch, calId] of Object.entries(PROCEDURE_CALENDARS)) {
      const { data } = await calendar.events.list({ calendarId: calId, timeMin, timeMax, singleEvents: true, orderBy: "startTime", maxResults: 2500 });
      const items = data.items || [];
      for (const ev of items) {
        const event_id = ev.id;
        const startIso = ev.start?.dateTime || ev.start?.date || null;
        if (!event_id || !startIso) continue;

        const service = inferService(ev.summary);
        const parsed = parseDesc(ev.description);
        const full_name = parsed.name || "";
        const phone_e164 = parsed.phone || "";
        const appt_iso = dayjs(startIso).tz(LOCAL_TZ).toISOString();

        const row1 = existing.get(event_id);
        if (row1) {
          const row = rows[row1 - 1] || [];
          const setIfEmpty = (colName, val) => {
            const c = colIndex(colName);
            if (c < 0) return;
            const cur = row[c];
            if (!cur && val) updates.push({ row1, col: c, value: val });
          };
          setIfEmpty("full_name", full_name);
          setIfEmpty("phone_e164", phone_e164);
          setIfEmpty("appt_iso", appt_iso);
          setIfEmpty("branch", branch);
          setIfEmpty("service", service);
        } else {
          const rowArr = new Array(headers.length).fill("");
          rowArr[idx.event_id] = event_id;
          if (idx.full_name >= 0) rowArr[idx.full_name] = full_name;
          if (idx.phone >= 0) rowArr[idx.phone] = phone_e164;
          if (idx.appt_iso >= 0) rowArr[idx.appt_iso] = appt_iso;
          if (idx.branch >= 0) rowArr[idx.branch] = branch;
          if (idx.service >= 0) rowArr[idx.service] = service;
          appends.push(rowArr);
        }
      }
    }

    if (updates.length) {
      const dataReq = updates.map(({ row1, col, value }) => {
        const rangePart2 = (RANGE.split("!")[1] || "A:Z");
        const startColLetter2 = rangePart2.split(":")[0].replace(/[0-9]/g, "") || "A";
        const letterToIndex2 = (L) => L.split("").reduce((n, ch) => n * 26 + (ch.charCodeAt(0) - 64), 0) - 1;
        const indexToLetters2 = (n) => {
          let s = "";
          n++;
          while (n > 0) {
            const r = (n - 1) % 26;
            s = String.fromCharCode(65 + r) + s;
            n = Math.floor((n - 1) / 26);
          }
          return s;
        };
        const startBase2 = letterToIndex2(startColLetter2.toUpperCase());
        const a1 = `${indexToLetters2(startBase2 + col)}${row1}`;
        return { range: `${RANGE.split("!")[0]}!${a1}`, values: [[value]] };
      });
      await sheets.spreadsheets.values.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { valueInputOption: "RAW", data: dataReq } });
    }
    if (appends.length) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: RANGE,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: { values: appends },
      });
    }
    return res.json({ ok: true, updated: updates.length, appended: appends.length });
  } catch (err) {
    console.error("[/jobs/sync-procedures] error:", err?.response?.data || err.message);
    return res.status(200).json({ ok: false, error: err.message });
  }
});

// --------------- CONSULTS: Calendar → Sheet Sync ----------
app.post("/jobs/sync-consults", async (req, res) => {
  if (!assertToken(req, res)) return;
  try {
    const SHEET_ID = process.env.CONSULTS_SHEET_ID;
    const RANGE = process.env.CONSULTS_RANGE || "Sheet1!A:Z";
    if (!SHEET_ID) return res.json({ ok: false, error: "missing CONSULTS_SHEET_ID" });

    const lookAheadDays = Number(process.env.CONSULT_SYNC_LOOKAHEAD_DAYS || 30);
    const lookBackDays = Number(process.env.CONSULT_SYNC_LOOKBACK_DAYS || 1);
    const timeMin = nowTz().subtract(lookBackDays, "day").toISOString();
    const timeMax = nowTz().add(lookAheadDays, "day").toISOString();

    const authCal = getJWTAuth(["https://www.googleapis.com/auth/calendar"]);
    const calendar = google.calendar({ version: "v3", auth: authCal });
    const authSheets = getJWTAuth(["https://www.googleapis.com/auth/spreadsheets"]);
    const sheets = google.sheets({ version: "v4", auth: authSheets });

    const read = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: RANGE,
      valueRenderOption: "UNFORMATTED_VALUE",
      dateTimeRenderOption: "FORMATTED_STRING",
    });
    const rows = read.data.values || [];
    const headers = rows[0] || [];
    const lower = headers.map((h) => String(h || "").trim().toLowerCase());
    const colIndex = (name) => lower.indexOf(String(name).toLowerCase());
    const idx = {
      event_id: colIndex("event_id"),
      full_name: colIndex("full_name") >= 0 ? colIndex("full_name") : colIndex("patient_name"),
      phone: colIndex("phone_e164"),
      appt_iso: colIndex("appt_iso"),
      branch: colIndex("branch"),
      service: colIndex("service"),
    };
    const haveHeaders = ["event_id", "full_name", "phone_e164", "appt_iso", "branch", "service"].every((h) => colIndex(h) >= 0);
    if (!haveHeaders) return res.json({ ok: false, error: "Consult sheet missing headers: event_id, full_name, phone_e164, appt_iso, branch, service" });

    const existing = new Map();
    for (let r = 1; r < rows.length; r++) {
      const rid = rows[r]?.[idx.event_id];
      if (rid) existing.set(String(rid).trim(), r + 1);
    }

    const appends = [];
    const updates = [];

    // Helpers
    const parseDesc = (desc) => {
      const out = { name: "", phone: "" };
      const d = String(desc || "");
      const m1 = d.match(/name\s*:\s*(.+)/i);
      if (m1) out.name = m1[1].trim();
      const m2 = d.match(/phone\s*:\s*([+0-9\-\s]+)/i);
      if (m2) out.phone = m2[1].replace(/\s+/g, "").replace(/-/g, "");
      return out;
    };
    const inferBranchFrom = (s) => {
      const t = String(s || "").toLowerCase();
      if (t.includes("ardenne")) return "ardenne";
      if (t.includes("portmore")) return "portmore";
      if (t.includes("sav")) return "sav";
      if (t.includes("winchester")) return "winchester";
      return "";
    };

    for (const [physKey, calId] of Object.entries(PHYSICIANS)) {
      const { data } = await calendar.events.list({ calendarId: calId, timeMin, timeMax, singleEvents: true, orderBy: "startTime", maxResults: 2500 });
      const items = data.items || [];
      for (const ev of items) {
        const event_id = ev.id;
        const startIso = ev.start?.dateTime || ev.start?.date || null;
        if (!event_id || !startIso) continue;

        const parsed = parseDesc(ev.description);
        const full_name = parsed.name || "";
        const phone_e164 = parsed.phone || "";
        const appt_iso = dayjs(startIso).tz(LOCAL_TZ).toISOString();
        const branch = inferBranchFrom(ev.location || ev.summary || ev.description);
        const service = "consult";

        const row1 = existing.get(event_id);
        if (row1) {
          const row = rows[row1 - 1] || [];
          const setIfEmpty = (colName, val) => {
            const c = colIndex(colName);
            if (c < 0) return;
            const cur = row[c];
            if (!cur && val) updates.push({ row1, col: c, value: val });
          };
          setIfEmpty("full_name", full_name);
          setIfEmpty("phone_e164", phone_e164);
          setIfEmpty("appt_iso", appt_iso);
          setIfEmpty("branch", branch);
          setIfEmpty("service", service);
        } else {
          const rowArr = new Array(headers.length).fill("");
          rowArr[idx.event_id] = event_id;
          if (idx.full_name >= 0) rowArr[idx.full_name] = full_name;
          if (idx.phone >= 0) rowArr[idx.phone] = phone_e164;
          if (idx.appt_iso >= 0) rowArr[idx.appt_iso] = appt_iso;
          if (idx.branch >= 0) rowArr[idx.branch] = branch;
          if (idx.service >= 0) rowArr[idx.service] = service;
          appends.push(rowArr);
        }
      }
    }

    if (updates.length) {
      const dataReq = updates.map(({ row1, col, value }) => {
        const rangePart2 = (RANGE.split("!")[1] || "A:Z");
        const startColLetter2 = rangePart2.split(":")[0].replace(/[0-9]/g, "") || "A";
        const letterToIndex2 = (L) => L.split("").reduce((n, ch) => n * 26 + (ch.charCodeAt(0) - 64), 0) - 1;
        const indexToLetters2 = (n) => {
          let s = "";
          n++;
          while (n > 0) {
            const r = (n - 1) % 26;
            s = String.fromCharCode(65 + r) + s;
            n = Math.floor((n - 1) / 26);
          }
          return s;
        };
        const startBase2 = letterToIndex2(startColLetter2.toUpperCase());
        const a1 = `${indexToLetters2(startBase2 + col)}${row1}`;
        return { range: `${RANGE.split("!")[0]}!${a1}`, values: [[value]] };
      });
      await sheets.spreadsheets.values.batchUpdate({ spreadsheetId: SHEET_ID, requestBody: { valueInputOption: "RAW", data: dataReq } });
    }
    if (appends.length) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: RANGE,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: { values: appends },
      });
    }
    return res.json({ ok: true, updated: updates.length, appended: appends.length });
  } catch (err) {
    console.error("[/jobs/sync-consults] error:", err?.response?.data || err.message);
    return res.status(200).json({ ok: false, error: err.message });
  }
});

// --------------- Consult Reminders (7/3/1) ---------------
app.post("/jobs/run-consult-reminders", async (req, res) => {
  if (!assertToken(req, res)) return;
  try {
    const creds = loadServiceAccountJSON();
    const auth = new google.auth.JWT(creds.client_email, null, creds.private_key, ["https://www.googleapis.com/auth/spreadsheets"]);
    const sheets = google.sheets({ version: "v4", auth });

    const SHEET_ID = process.env.CONSULTS_SHEET_ID;
    const RANGE = process.env.CONSULTS_RANGE || "Sheet1!A:Z";
    if (!SHEET_ID) return res.json({ ok: false, error: "missing CONSULTS_SHEET_ID" });

    const read = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: RANGE,
      valueRenderOption: "UNFORMATTED_VALUE",
      dateTimeRenderOption: "FORMATTED_STRING",
    });
    const rows = read.data.values || [];
    if (!rows.length) return res.json({ ok: true, processed: 0 });

    const headers = (rows[0] || []).map((h) => String(h || "").trim().toLowerCase());
    const col = (n) => headers.indexOf(String(n).trim().toLowerCase());
    const idx = {
      event_id: col("event_id"),
      full_name: col("full_name") >= 0 ? col("full_name") : col("patient_name"),
      phone: col("phone_e164"),
      appt_iso: col("appt_iso"),
      branch: col("branch"),
      service: col("service"),
      opt_out: col("opt_out"),
      status_flag: col("status_flag"),
      status_note: col("status_note"),
      pause_until: col("pause_until_yyyy_mm_dd"),
      last_reminded_offset: col("last_reminded_offset"),
      last_outcome: col("last_outcome"),
      last_outcome_ts: col("last_outcome_ts"),
      last_call_id: col("last_call_id"),
      retry_count: col("retry_count"),
      deferred_for: col("deferred_for_yyyy_mm_dd"),
      deferred_reason: col("deferred_reason"),
    };
    const val = (r, i) => (i >= 0 && r[i] != null ? String(r[i]).trim() : "");
    const asBool = (s) => /^true|1|yes|y$/i.test(String(s || "").trim());

    const offsets = (process.env.CONSULT_REMINDER_OFFSETS || "7,3,1")
      .split(",")
      .map((s) => parseInt(s.trim(), 10))
      .filter((n) => Number.isFinite(n) && n >= 0);

    const today = nowTz().startOf("day");
    const insideWindow = insideWindowNow();

    const rangePart = (RANGE.split("!")[1] || "A:Z");
    const startColLetter = rangePart.split(":")[0].replace(/[0-9]/g, "") || "A";
    const letterToIndex = (L) => L.split("").reduce((n, ch) => n * 26 + (ch.charCodeAt(0) - 64), 0) - 1;
    const indexToLetters = (n) => {
      let s = "";
      n++;
      while (n > 0) {
        const r = (n - 1) % 26;
        s = String.fromCharCode(65 + r) + s;
        n = Math.floor((n - 1) / 26);
      }
      return s;
    };
    const startBase = letterToIndex(startColLetter.toUpperCase());
    const updates = [];
    const setCell = (row1, col0, val_) => {
      if (col0 < 0) return;
      const a1 = `${indexToLetters(startBase + col0)}${row1}`;
      updates.push({ range: `${RANGE.split("!")[0]}!${a1}`, values: [[val_]] });
    };

    for (let r = 1; r < rows.length; r++) {
      const row = rows[r] || [];
      const row1 = r + 1;

      const fullName = val(row, idx.full_name);
      const phone = val(row, idx.phone);
      const apptIso = val(row, idx.appt_iso);
      const branch = (val(row, idx.branch).toLowerCase() || "winchester");
      const optOut = asBool(val(row, idx.opt_out));
      const statusFlag = val(row, idx.status_flag).toUpperCase();
      const pauseUntil = dayjs(val(row, idx.pause_until)).isValid() ? dayjs(val(row, idx.pause_until)).tz(LOCAL_TZ) : null;

      if (!fullName || !phone || !apptIso) continue;

      const appt = parseLocalIso(apptIso);
      if (!appt?.isValid?.()) continue;

      const daysUntil = appt.startOf("day").diff(today, "day");
      if (!offsets.includes(daysUntil)) continue;

      if (optOut) {
        setCell(row1, idx.last_outcome, "skipped_opt_out");
        continue;
      }
      if (statusFlag === "DO_NOT_CALL" || statusFlag === "INACTIVE") {
        setCell(row1, idx.last_outcome, `skipped_${statusFlag.toLowerCase()}`);
        continue;
      }
      if (pauseUntil && pauseUntil.isAfter(today, "day")) {
        setCell(row1, idx.last_outcome, "paused_until");
        continue;
      }
      if (!insideWindow) {
        setCell(row1, idx.last_outcome, "skipped_outside_window");
        continue;
      }

      const service = "consult";
      const apptNice = appt.format("dddd, MMM D [at] h:mm A");
      if (ENABLE_CONSULT_DIAL && RETELL_API_KEY && RETELL_OUTBOUND_AGENT_ID && RETELL_OUTBOUND_FROM) {
        try {
          const webhookUrl = `${process.env.PUBLIC_BASE_URL || ""}/retell/outbound/callback?token=${encodeURIComponent(STATUS_TOKEN)}`;
          const meta = { kind: "consult", sheet_id: SHEET_ID, range: RANGE, row_number: row1 };
          const variables = { call_type: "reminder", patient_name: fullName, branch, service, appointment_time: apptNice };
          const dial = await placeRetellCall({
            to: phone,
            from: RETELL_OUTBOUND_FROM,
            agent_id: RETELL_OUTBOUND_AGENT_ID,
            variables,
            metadata: meta,
            webhook_url: webhookUrl,
          });
          if (idx.last_outcome >= 0) setCell(row1, idx.last_outcome, "call_placed");
          if (idx.last_outcome_ts >= 0) setCell(row1, idx.last_outcome_ts, isoLocal());
          if (idx.last_reminded_offset >= 0) setCell(row1, idx.last_reminded_offset, String(daysUntil));
          if (idx.last_call_id >= 0 && (dial?.call_id || dial?.id)) setCell(row1, idx.last_call_id, dial?.call_id || dial?.id);
        } catch (e) {
          if (idx.last_outcome >= 0) setCell(row1, idx.last_outcome, "dial_error");
          if (idx.last_outcome_ts >= 0) setCell(row1, idx.last_outcome_ts, isoLocal());
        }
      } else {
        if (idx.last_outcome >= 0) setCell(row1, idx.last_outcome, "queued");
        if (idx.last_outcome_ts >= 0) setCell(row1, idx.last_outcome_ts, isoLocal());
        if (idx.last_reminded_offset >= 0) setCell(row1, idx.last_reminded_offset, String(daysUntil));
      }
    }

    if (updates.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption: "RAW", data: updates },
      });
    }
    return res.json({ ok: true });
  } catch (err) {
    console.error("[/jobs/run-consult-reminders] error:", err?.response?.data || err.message);
    return res.status(200).json({ ok: false, error: err.message });
  }
});

// --------------- Post-Visit Follow-ups (next-day) --------
app.post("/jobs/run-followups", async (req, res) => {
  if (!assertToken(req, res)) return;
  try {
    const creds = loadServiceAccountJSON();
    const auth = new google.auth.JWT(creds.client_email, null, creds.private_key, ["https://www.googleapis.com/auth/spreadsheets"]);
    const sheets = google.sheets({ version: "v4", auth });

    const SHEET_ID = process.env.CONSULTS_SHEET_ID;
    const RANGE = process.env.CONSULTS_RANGE || "Sheet1!A:Z";
    if (!SHEET_ID) return res.json({ ok: false, error: "missing CONSULTS_SHEET_ID" });

    const read = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: RANGE,
      valueRenderOption: "UNFORMATTED_VALUE",
      dateTimeRenderOption: "FORMATTED_STRING",
    });
    const rows = read.data.values || [];
    if (!rows.length) return res.json({ ok: true, processed: 0 });

    const headers = (rows[0] || []).map((h) => String(h || "").trim().toLowerCase());
    const col = (n) => headers.indexOf(String(n).trim().toLowerCase());
    const idx = {
      full_name: col("full_name") >= 0 ? col("full_name") : col("patient_name"),
      phone: col("phone_e164"),
      appt_iso: col("appt_iso"),
      branch: col("branch"),
      service: col("service"),
      opt_out: col("opt_out"),
      status_flag: col("status_flag"),
      status_note: col("status_note"),
      pause_until: col("pause_until_yyyy_mm_dd"),
      last_followup_year: col("last_followup_year"),
      last_followup_outcome: col("last_followup_outcome"),
      last_followup_ts: col("last_followup_ts"),
      last_call_id: col("last_call_id"),
      retry_count: col("retry_count"),
      deferred_for: col("deferred_for_yyyy_mm_dd"),
      deferred_reason: col("deferred_reason"),
    };
    const val = (r, i) => (i >= 0 && r[i] != null ? String(r[i]).trim() : "");
    const asBool = (s) => /^true|1|yes|y$/i.test(String(s || "").trim());

    const offsetDays = Number(process.env.FOLLOWUP_OFFSET_DAYS || 1);
    const targetDay = nowTz().subtract(offsetDays, "day").startOf("day");
    const insideWindow = insideWindowNow();

    const rangePart = (RANGE.split("!")[1] || "A:Z");
    const startColLetter = rangePart.split(":")[0].replace(/[0-9]/g, "") || "A";
    const letterToIndex = (L) => L.split("").reduce((n, ch) => n * 26 + (ch.charCodeAt(0) - 64), 0) - 1;
    const indexToLetters = (n) => {
      let s = "";
      n++;
      while (n > 0) {
        const r = (n - 1) % 26;
        s = String.fromCharCode(65 + r) + s;
        n = Math.floor((n - 1) / 26);
      }
      return s;
    };
    const startBase = letterToIndex(startColLetter.toUpperCase());
    const updates = [];
    const setCell = (row1, col0, val_) => {
      if (col0 < 0) return;
      const a1 = `${indexToLetters(startBase + col0)}${row1}`;
      updates.push({ range: `${RANGE.split("!")[0]}!${a1}`, values: [[val_]] });
    };

    for (let r = 1; r < rows.length; r++) {
      const row = rows[r] || [];
      const row1 = r + 1;

      const fullName = val(row, idx.full_name);
      const phone = val(row, idx.phone);
      const apptIso = val(row, idx.appt_iso);
      const branch = (val(row, idx.branch).toLowerCase() || "winchester");
      const optOut = asBool(val(row, idx.opt_out));
      const statusFlag = val(row, idx.status_flag).toUpperCase();
      const pauseUntil = dayjs(val(row, idx.pause_until)).isValid() ? dayjs(val(row, idx.pause_until)).tz(LOCAL_TZ) : null;

      if (!fullName || !phone || !apptIso) continue;

      const appt = parseLocalIso(apptIso);
      if (!appt?.isValid?.()) continue;

      const daysSince = nowTz().startOf("day").diff(appt.startOf("day"), "day");
      if (daysSince !== offsetDays) continue; // exactly N days after

      if (optOut) {
        if (idx.last_followup_outcome >= 0) setCell(row1, idx.last_followup_outcome, "skipped_opt_out");
        continue;
      }
      if (statusFlag === "DO_NOT_CALL" || statusFlag === "INACTIVE") {
        if (idx.last_followup_outcome >= 0) setCell(row1, idx.last_followup_outcome, `skipped_${statusFlag.toLowerCase()}`);
        continue;
      }
      if (pauseUntil && pauseUntil.isAfter(targetDay, "day")) {
        if (idx.last_followup_outcome >= 0) setCell(row1, idx.last_followup_outcome, "paused_until");
        continue;
      }
      if (!insideWindow) {
        if (idx.last_followup_outcome >= 0) setCell(row1, idx.last_followup_outcome, "skipped_outside_window");
        continue;
      }

      if (ENABLE_CONSULT_DIAL && RETELL_API_KEY && RETELL_OUTBOUND_AGENT_ID && RETELL_OUTBOUND_FROM) {
        try {
          const webhookUrl = `${process.env.PUBLIC_BASE_URL || ""}/retell/outbound/callback?token=${encodeURIComponent(STATUS_TOKEN)}`;
          const meta = { kind: "followup", sheet_id: SHEET_ID, range: RANGE, row_number: row1 };
          const variables = { call_type: "followup", patient_name: fullName, branch };
          const dial = await placeRetellCall({
            to: phone,
            from: RETELL_OUTBOUND_FROM,
            agent_id: RETELL_OUTBOUND_AGENT_ID,
            variables,
            metadata: meta,
            webhook_url: webhookUrl,
          });
          if (idx.last_followup_outcome >= 0) setCell(row1, idx.last_followup_outcome, "call_placed");
          if (idx.last_followup_ts >= 0) setCell(row1, idx.last_followup_ts, isoLocal());
          if (idx.last_call_id >= 0 && (dial?.call_id || dial?.id)) setCell(row1, idx.last_call_id, dial?.call_id || dial?.id);
        } catch (e) {
          if (idx.last_followup_outcome >= 0) setCell(row1, idx.last_followup_outcome, "dial_error");
          if (idx.last_followup_ts >= 0) setCell(row1, idx.last_followup_ts, isoLocal());
        }
      } else {
        if (idx.last_followup_outcome >= 0) setCell(row1, idx.last_followup_outcome, "queued");
        if (idx.last_followup_ts >= 0) setCell(row1, idx.last_followup_ts, isoLocal());
      }
    }

    if (updates.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: { valueInputOption: "RAW", data: updates },
      });
    }
    return res.json({ ok: true });
  } catch (err) {
    console.error("[/jobs/run-followups] error:", err?.response?.data || err.message);
    return res.status(200).json({ ok: false, error: err.message });
  }
});

// --------------- Status --------------------
app.get("/status.json", (req, res) => {
  const t = String(req.query.token || "");
  if (!STATUS_TOKEN || t !== STATUS_TOKEN) return res.status(401).json({ ok: false, error: "unauthorized" });
  const haveGoogle = !!(process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON || process.env.GOOGLE_CREDENTIALS_B64);
  res.json({
    ok: true,
    time: isoLocal(),
    tz: LOCAL_TZ,
    checks: {
      google_creds: haveGoogle,
      birthdays: { sheet_id_present: !!process.env.BIRTHDAYS_SHEET_ID, range: process.env.BIRTHDAYS_RANGE || "Sheet1!A:Z" },
      procedures: {
        sheet_id_present: !!process.env.PROCEDURES_SHEET_ID,
        range: process.env.PROCEDURES_RANGE || "Sheet1!A:Z",
        offsets: process.env.PROC_REMINDER_OFFSETS || "7,3,1",
      },
      consults: {
        sheet_id_present: !!process.env.CONSULTS_SHEET_ID,
        range: process.env.CONSULTS_RANGE || "Sheet1!A:Z",
        offsets: process.env.CONSULT_REMINDER_OFFSETS || "7,3,1",
        followup_offset_days: Number(process.env.FOLLOWUP_OFFSET_DAYS || 1),
      },
      sync: {
        proc_look_ahead_days: Number(process.env.PROC_SYNC_LOOKAHEAD_DAYS || 30),
        proc_look_back_days: Number(process.env.PROC_SYNC_LOOKBACK_DAYS || 1),
        consult_look_ahead_days: Number(process.env.CONSULT_SYNC_LOOKAHEAD_DAYS || 30),
        consult_look_back_days: Number(process.env.CONSULT_SYNC_LOOKBACK_DAYS || 1),
      },
      outbound: {
        retell_configured: !!(RETELL_API_KEY && RETELL_OUTBOUND_AGENT_ID && RETELL_OUTBOUND_FROM),
        birthdays_enabled: ENABLE_BDAY_DIAL,
        procedures_enabled: ENABLE_PROC_DIAL,
        consults_enabled: ENABLE_CONSULT_DIAL,
      },
      smtp_configured: !!(process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS),
      telnyx_configured: !!process.env.TELNYX_API_KEY,
    },
  });
});

// --------------- Start ----------------------
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`WHC server listening on :${PORT}`));
