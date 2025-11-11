// server.js — WHC PBX + Calendar + Jobs (Retell + Telnyx + Email)
// Outbound Birthday Calls + Webhook write-back
// ---------------------------------------------------------------

import express from "express";
import cors from "cors";
import { google } from "googleapis";
import axios from "axios";
import nodemailer from "nodemailer";

// ---- Timezone (Jamaica) ----
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc.js";
import timezone from "dayjs/plugin/timezone.js";
dayjs.extend(utc);
dayjs.extend(timezone);

const LOCAL_TZ = process.env.LOCAL_TZ || "America/Jamaica";
const nowTz = () => dayjs().tz(LOCAL_TZ);
const isoLocal = () => nowTz().toISOString();

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
const STATUS_TOKEN = process.env.STATUS_TOKEN || "";

// Branch phones (for transfer targets)
const BRANCH_NUMBERS = {
  winchester: process.env.BRANCH_WINCHESTER || "+18769082658",
  portmore: process.env.BRANCH_PORTMORE || "+18767042739",
  ardenne: process.env.BRANCH_ARDENNE || "+18766713825",
  sav: process.env.BRANCH_SAV || "+18769540252",
};

// Branch emails for voicemail summary
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

// Procedure calendars by branch (provided)
const PROCEDURE_CALENDARS = {
  winchester: "a434db3192bb86669c1238fd8840d62d72a254cea1b58d01da3dd61eefeb1ba6@group.calendar.google.com",
  ardenne:   "d86eacf4f06f69310210007c96a91941ba3f0eb37a6165e350f3227b604cee06@group.calendar.google.com",
  portmore:  "1dce088aed350313f0848a6950f97fdaa44616145b523097b23877f3aa0278a5@group.calendar.google.com",
  sav:       "79ff142150e20453f5600b25f445c1d27f03d30594dadc5c777b4ed9af360e5e@group.calendar.google.com",
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
    ["https://www.googleapis.com/auth/calendar", "https://www.googleapis.com/auth/spreadsheets"]
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
    the:
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

// ------------------------ Retell Action (in-call) --------
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

// Office hours (local Jamaica time)
function isOfficeOpen(branchKey, when = nowTz()) {
  // Sun=0 ... Sat=6
  const dow = when.day();
  const minutes = when.hour() * 60 + when.minute();

  // Closed Sundays
  if (dow === 0) return false;

  // Portmore: Mon–Fri 10:00–17:00; Sat 10:00–14:00
  if (branchKey === "portmore") {
    if (dow >= 1 && dow <= 5) return minutes >= 10 * 60 && minutes < 17 * 60;
    if (dow === 6) return minutes >= 10 * 60 && minutes < 14 * 60;
    return false;
  }

  // Winchester / Ardenne / Sav: Mon–Fri 08:30–16:30
  if (dow >= 1 && dow <= 5) return minutes >= (8 * 60 + 30) && minutes < (16 * 60 + 30);
  return false;
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

    // Answer & decision
    if (eventType === "call.initiated") {
      await tx("answer", { call_control_id: callControlId });

      // Determine branch from client_state
      const meta = payload.client_state ? JSON.parse(Buffer.from(payload.client_state, "base64").toString("utf8")) : {};
      const branch = resolveBranchFromMeta(meta);

      // After-hours guard
      if (!isOfficeOpen(branch)) {
        await sendVoicemailEmail({
          branch,
          caller: payload?.from || payload?.from_number || "Unknown",
          recordingUrl: "(no recording in this minimal build)",
          transcript: null,
        });
        try { await tx("hangup", { call_control_id: callControlId }); } catch {}
        return res.json({ ok: true, note: "after-hours; message taken" });
      }

      // In hours: MOH + transfer
      await tx("playback_start", {
        call_control_id: callControlId,
        audio_url: MOH_URL,
        overlay: true,
        loop: true,
      });

      const target = BRANCH_NUMBERS[branch] || BRANCH_NUMBERS.winchester;
      await tx("transfer", {
        call_control_id: callControlId,
        to: target,
        from: TELNYX_OUTBOUND_CALLER_ID,
        timeout_secs: Math.ceil(HANDOFF_TIMEOUT_MS / 1000),
      });

      return res.json({ ok: true });
    }

    if (eventType === "call.bridged") {
      await tx("playback_stop", { call_control_id: callControlId });
      return res.json({ ok: true });
    }

    if (eventType === "call.ended" || eventType === "transfer.failed") {
      const meta = payload.client_state ? JSON.parse(Buffer.from(payload.client_state, "base64").toString("utf8")) : {};
      const branch = resolveBranchFromMeta(meta);
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

// ------------------------ Jobs ---------------------------

// Helper auth
function assertToken(req, res) {
  const token = String(req.query.token || "");
  if (!STATUS_TOKEN || token !== STATUS_TOKEN) {
    res.status(401).json({ ok: false, error: "unauthorized" });
    return false;
  }
  return true;
}

// No-op placeholder so your cron never fails (expand later if needed)
app.post("/jobs/run-reminders", async (req, res) => {
  if (!assertToken(req, res)) return;
  try {
    return res.json({ ok: true, results: [] });
  } catch (err) {
    console.error("[/jobs/run-reminders] error:", err?.response?.data || err.message);
    return res.status(200).json({ ok: false, error: err.message });
  }
});

// ----------------- Retell Outbound (helper) -----------------
const ENABLE_BDAY_DIAL = String(process.env.ENABLE_BDAY_DIAL || "").trim() === "1";
const RETELL_API_KEY = process.env.RETELL_API_KEY || "";
const RETELL_OUTBOUND_AGENT_ID = process.env.RETELL_OUTBOUND_AGENT_ID || ""; // the agent profile to use for outbound
const RETELL_OUTBOUND_FROM = process.env.RETELL_OUTBOUND_FROM || ""; // +1305... (your Retell number)
const RETELL_OUTBOUND_URL = process.env.RETELL_OUTBOUND_URL || "https://api.retellai.com/v2/outbound-calls";

// Place an outbound call via Retell. Adjust body fields if your tenant differs.
async function placeRetellCall({
  to,
  from,
  agent_id,
  variables,
  metadata,
  webhook_url
}) {
  if (!RETELL_API_KEY || !agent_id || !from) {
    throw new Error("Retell outbound not configured");
  }
  const client = axios.create({
    baseURL: RETELL_OUTBOUND_URL.startsWith("http") ? undefined : undefined,
    headers: { Authorization: `Bearer ${RETELL_API_KEY}` },
    timeout: 15000,
  });
  const body = { to, from, agent_id, variables, metadata, webhook_url };
  const { data } = await client.post(RETELL_OUTBOUND_URL, body);
  // Expect { call_id: "...", status: "queued" } or similar
  return data;
}

// ------------------------ Birthday Job (Jamaica time + extra columns + outbound dial) ---------------------
app.post("/jobs/run-birthdays", async (req, res) => {
  if (!assertToken(req, res)) return;
  try {
    const creds = loadServiceAccountJSON();
    const auth = new google.auth.JWT(
      creds.client_email,
      null,
      creds.private_key,
      ["https://www.googleapis.com/auth/spreadsheets"]
    );
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
    if (rows.length === 0) return res.json({ ok: true, processed: 0, details: [] });

    const headers = (rows[0] || []).map(h => String(h || "").trim().toLowerCase());
    const col = (name) => headers.indexOf(String(name).trim().toLowerCase());
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
      last_call_id: col("last_call_id"), // OPTIONAL but recommended
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

    // Calling window (local minutes)
    const startMin = Number(process.env.CALL_WINDOW_START_MIN || 8 * 60);
    const endMin = Number(process.env.CALL_WINDOW_END_MIN || 18 * 60);
    const minutesNow = today.hour() * 60 + today.minute();
    const insideWindow = minutesNow >= startMin && minutesNow <= endMin;

    const details = [];
    const updates = [];

    const rangePart = RANGE.split("!")[1] || "A:Z";
    const startColLetter = rangePart.split(":")[0].replace(/[0-9]/g, "") || "A";
    const letterToIndex = (L) =>
      L.split("").reduce((n, ch) => n * 26 + (ch.charCodeAt(0) - 64), 0) - 1;
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
    const setCell = (rowIndex1Based, colIndex0Based, value) => {
      const targetColLetter = indexToLetters(startBase + colIndex0Based);
      const a1 = `${targetColLetter}${rowIndex1Based}`;
      updates.push({
        range: `${RANGE.split("!")[0]}!${a1}`,
        values: [[value]],
      });
    };

    const offs = {
      last_called_year: idx.last_called_year,
      last_outcome: idx.last_outcome,
      last_outcome_ts: idx.last_outcome_ts,
      last_call_id: idx.last_call_id,
    };

    for (let r = 1; r < rows.length; r++) {
      const row = rows[r] || [];

      const fullName = val(row, idx.full_name);
      const phone = val(row, idx.phone);
      const dobStr = val(row, idx.dob);
      const branch = (val(row, idx.branch).toLowerCase() || "winchester");
      const optOut = asBool(val(row, idx.opt_out));
      const optOutReason = val(row, idx.opt_out_reason);
      const lastCalledYear = val(row, idx.last_called_year);

      const statusFlag = val(row, idx.status_flag).toUpperCase();
      const statusNote = val(row, idx.status_note);
      const pauseUntil = parseYmd(val(row, idx.pause_until));
      const deferredFor = parseYmd(val(row, idx.deferred_for));
      const deferredReason = val(row, idx.deferred_reason);
      const dobCorrection = parseYmd(val(row, idx.dob_correction));
      const newPhoneCandidate = val(row, idx.new_phone_candidate);

      const dob = dobCorrection || parseYmd(dobStr);
      if (!dob || !fullName) {
        details.push({ row: r + 1, fullName, skip: "missing_name_or_dob" });
        continue;
      }

      const dobMD = dob.tz(LOCAL_TZ).format("MM-DD");
      if (dobMD !== todayMD) continue;

      const row1 = r + 1;

      const writeOutcome = (key) => {
        if (offs.last_outcome >= 0) setCell(row1, offs.last_outcome, key);
        if (offs.last_outcome_ts >= 0) setCell(row1, offs.last_outcome_ts, isoLocal());
      };
      const writeYearNow = () => {
        if (offs.last_called_year >= 0) setCell(row1, offs.last_called_year, String(todayY));
      };
      const writeCallId = (id) => {
        if (offs.last_call_id >= 0) setCell(row1, offs.last_call_id, id);
      };

      if (optOut) {
        writeOutcome("skipped_opt_out");
        details.push({ row: row1, fullName, branch, reason: "opt_out", note: optOutReason });
        continue;
      }

      if (statusFlag === "DO_NOT_CALL" || statusFlag === "INACTIVE") {
        writeOutcome(`skipped_${statusFlag.toLowerCase()}`);
        details.push({ row: row1, fullName, branch, reason: "status_flag", statusFlag, statusNote });
        continue;
      }

      if (pauseUntil && pauseUntil.tz(LOCAL_TZ).isAfter(today, "day")) {
        writeOutcome("paused_until");
        details.push({
          row: row1, fullName, branch,
          reason: "paused_until",
          pause_until: pauseUntil.format("YYYY-MM-DD")
        });
        continue;
      }

      if (deferredFor && deferredFor.tz(LOCAL_TZ).isAfter(today, "day")) {
        writeOutcome("deferred");
        details.push({
          row: row1, fullName, branch,
          reason: "deferred",
          deferred_for: deferredFor.format("YYYY-MM-DD"),
          deferred_reason: deferredReason
        });
        continue;
      }

      if (!insideWindow) {
        writeOutcome("skipped_outside_window");
        details.push({ row: row1, fullName, branch, reason: "outside_window" });
        continue;
      }

      if (!phone) {
        writeOutcome("skipped_missing_phone");
        details.push({ row: row1, fullName, branch, reason: "missing_phone", new_phone_candidate: newPhoneCandidate });
        continue;
      }

      if (String(lastCalledYear || "").trim() === String(todayY)) {
        writeOutcome("skipped_already_called_this_year");
        details.push({ row: row1, fullName, branch, reason: "already_called_this_year" });
        continue;
      }

      // ---------- Outbound call ----------
      if (ENABLE_BDAY_DIAL && RETELL_API_KEY && RETELL_OUTBOUND_AGENT_ID && RETELL_OUTBOUND_FROM) {
        try {
          const webhookUrl = `${process.env.PUBLIC_BASE_URL || ""}/retell/outbound/callback?token=${encodeURIComponent(STATUS_TOKEN)}`;
          const meta = {
            sheet_id: SHEET_ID,
            range: RANGE,
            row_number: row1,   // 1-based row so webhook can update safely
          };
          const variables = {
            call_type: "birthday",
            patient_name: fullName,
            branch,
            local_time: nowTz().format("h:mm A"),
          };
          const dial = await placeRetellCall({
            to: phone,
            from: RETELL_OUTBOUND_FROM,
            agent_id: RETELL_OUTBOUND_AGENT_ID,
            variables,
            metadata: meta,
            webhook_url: webhookUrl
          });

          const callId = dial?.call_id || dial?.id || "";
          writeYearNow();
          writeOutcome("call_placed");
          if (callId) writeCallId(callId);

          details.push({
            row: row1,
            fullName,
            branch,
            action: "call_placed",
            phone_e164: phone,
            call_id: callId
          });
        } catch (e) {
          writeOutcome("dial_error");
          details.push({ row: row1, fullName, branch, action: "dial_error", error: e.message });
        }
      } else {
        // If outbound disabled, just queue logically
        writeYearNow();
        writeOutcome("queued");
        details.push({
          row: row1,
          fullName,
          branch,
          action: "queued",
          phone_e164: phone,
        });
      }
    }

    if (updates.length) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        requestBody: {
          valueInputOption: "RAW",
          data: updates
        }
      });
    }

    return res.json({ ok: true, processed: details.filter(d => d.action === "call_placed" || d.action === "queued").length, details });
  } catch (err) {
    console.error("[/jobs/run-birthdays] error:", err?.response?.data || err.message);
    return res.status(200).json({ ok: false, error: err.message });
  }
});

// ---------------- Retell post-call webhook → update sheet ----------------
app.post("/retell/outbound/callback", async (req, res) => {
  // Simple shared-secret check via token (query)
  const token = String(req.query.token || "");
  if (!STATUS_TOKEN || token !== STATUS_TOKEN) return res.status(401).json({ ok: false });

  try {
    const body = req.body || {};
    // Normalized fields we try to read
    const callId = body.call_id || body.id || "";
    const final = (body.final_status || body.status || "").toLowerCase();
    const meta = body.metadata || {};

    const SHEET_ID = meta.sheet_id || process.env.BIRTHDAYS_SHEET_ID;
    const RANGE = meta.range || process.env.BIRTHDAYS_RANGE || "Sheet1!A:Z";
    const row1 = Number(meta.row_number || 0);

    if (!SHEET_ID) return res.json({ ok: false, error: "missing sheet_id" });

    const creds = loadServiceAccountJSON();
    const auth = new google.auth.JWT(
      creds.client_email,
      null,
      creds.private_key,
      ["https://www.googleapis.com/auth/spreadsheets"]
    );
    const sheets = google.sheets({ version: "v4", auth });

    const read = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: RANGE,
      valueRenderOption: "UNFORMATTED_VALUE",
      dateTimeRenderOption: "FORMATTED_STRING",
    });
    const rows = read.data.values || [];
    if (!rows.length) return res.json({ ok: false, error: "empty sheet" });
    const headers = (rows[0] || []).map(h => String(h || "").trim().toLowerCase());
    const col = (name) => headers.indexOf(String(name).trim().toLowerCase());
    const idx = {
      last_outcome: col("last_outcome"),
      last_outcome_ts: col("last_outcome_ts"),
      last_called_year: col("last_called_year"),
      last_call_id: col("last_call_id"),
    };

    const rangePart = RANGE.split("!")[1] || "A:Z";
    const startColLetter = rangePart.split(":")[0].replace(/[0-9]/g, "") || "A";
    const letterToIndex = (L) =>
      L.split("").reduce((n, ch) => n * 26 + (ch.charCodeAt(0) - 64), 0) - 1;
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
    const setCell = (rowIndex1Based, colIndex0Based, value) => {
      const targetColLetter = indexToLetters(startBase + colIndex0Based);
      const a1 = `${targetColLetter}${rowIndex1Based}`;
      updates.push({
        range: `${RANGE.split("!")[0]}!${a1}`,
        values: [[value]],
      });
    };

    // If the webhook didn’t carry row_number, try to locate by call_id
    let targetRow1 = row1 || 0;
    if (!targetRow1 && idx.last_call_id >= 0 && callId) {
      for (let r = 1; r < rows.length; r++) {
        const v = (rows[r] || [])[idx.last_call_id];
        if (String(v || "").trim() === callId) {
          targetRow1 = r + 1;
          break;
        }
      }
    }

    if (!targetRow1) {
      // No place to write, but acknowledge
      return res.json({ ok: true, note: "no target row identified" });
    }

    const updates = [];
    const write = (c, val) => {
      if (c >= 0) {
        const targetColLetter = indexToLetters(startBase + c);
        const a1 = `${targetColLetter}${targetRow1}`;
        updates.push({ range: `${RANGE.split("!")[0]}!${a1}`, values: [[val]] });
      }
    };

    // Map Retell final statuses to our outcomes
    let outcome = "completed";
    if (final.includes("voicemail")) outcome = "left_voicemail";
    else if (final.includes("no_answer") || final.includes("noanswer")) outcome = "no_answer";
    else if (final.includes("cancel")) outcome = "cancelled";
    else if (final.includes("error") || final.includes("failed")) outcome = "dial_error";
    else if (final.includes("complete")) outcome = "wished_happy_birthday";

    write(idx.last_outcome, outcome);
    write(idx.last_outcome_ts, isoLocal());
    write(idx.last_called_year, String(nowTz().year()));
    if (idx.last_call_id >= 0 && callId) write(idx.last_call_id, callId);

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

// ------------------------ Status -------------------------
app.get("/status.json", (req, res) => {
  const token = String(req.query.token || "");
  if (!STATUS_TOKEN || token !== STATUS_TOKEN) {
    return res.status(401).json({ ok: false, error: "unauthorized" });
    }
  const haveGoogle = !!(process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON || process.env.GOOGLE_CREDENTIALS_B64);
  res.json({
    ok: true,
    time: isoLocal(),
    tz: LOCAL_TZ,
    checks: {
      google_creds: haveGoogle,
      birthdays: {
        sheet_id_present: !!process.env.BIRTHDAYS_SHEET_ID,
        range: process.env.BIRTHDAYS_RANGE || "Sheet1!A:Z"
      },
      smtp_configured: !!(process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS),
      telnyx_configured: !!TELNYX_API_KEY,
      retell_outbound_enabled: ENABLE_BDAY_DIAL
    }
  });
});

// ------------------------ Start --------------------------
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`WHC server listening on :${PORT}`));
