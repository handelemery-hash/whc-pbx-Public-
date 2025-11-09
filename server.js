// server.js — WHC PBX + Calendar + Retell + Email + Status + Reminders
// --------------------------------------------------------------------

import http from "http";
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
app.get("/", (_req, res) => res.status(200).send("WHC PBX running"));

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
const TZ = "America/Jamaica";

// Physician calendars
const PHYSICIANS = {
  dr_emery:    "uh7ehq6qg5c1qfdciic3v8l0s8@group.calendar.google.com",
  dr_thompson: "eburtl0ebphsp3h9qdfurpbqeg@group.calendar.google.com",
  dr_dowding:  "a70ab6c4e673f04f6d40fabdb0f4861cf2fac5874677d5dd9961e357b8bb8af9@group.calendar.google.com",
  dr_blair:    "ad21642079da12151a39c9a5aa455d56c306cfeabdfd712fb34a4378c3f04c4a@group.calendar.google.com",
  dr_williams: "7343219d0e34a585444e2a39fd1d9daa650e082209a9e5dc85e0ce73d63c7393@group.calendar.google.com",
  dr_wright:   "b8a27f6d34e63806408f975bf729a3089b0d475b1b58c18ae903bc8bc63aa0ea@group.calendar.google.com",
  dr_dixon:    "ed382c812be7a6d3396a874ca19368f2d321805f80526e6f3224f713f0637cee@group.calendar.google.com",
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

function normalizePhys(s) {
  return String(s || "").trim().toLowerCase().replace(/\s+/g, "_");
}
function getCalendarIdForPhys(phys) {
  const k = normalizePhys(phys);
  const id = PHYSICIANS[k];
  if (!id) throw new Error(`Unknown physician '${phys}'`);
  return { key: k, id };
}

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
  await transporter.sendMail({ from: FROM_EMAIL, to, subject: `New Voicemail - ${branch} branch`, html });
}

// ------------------- Retell Outbound (for reminders) ----
const RETELL_API_KEY = process.env.RETELL_API_KEY;
const RETELL_AGENT_ID = process.env.RETELL_AGENT_ID;
const RETELL_NUMBER   = process.env.RETELL_NUMBER;

async function callPatient({ phone, patientName, apptTime, branch }) {
  if (!RETELL_API_KEY || !RETELL_AGENT_ID || !RETELL_NUMBER) {
    console.warn("[Retell] Missing RETELL_* env vars, skipping outbound call");
    return { skipped: true };
  }
  const r = await axios.post(
    "https://api.retellai.com/v1/calls/outbound",
    {
      agent_id: RETELL_AGENT_ID,
      from_number: RETELL_NUMBER,
      to_number: phone,
      variables: {
        patient_name: patientName || "Patient",
        appointment_time: apptTime,
        branch: branch || "winchester",
      }
    },
    { headers: { Authorization: `Bearer ${RETELL_API_KEY}` }, timeout: 10000 }
  );
  return r.data;
}

// ------------------- Reminder helpers -------------------
function minutesUntil(dateISO) {
  const start = new Date(dateISO);
  return Math.round((start.getTime() - Date.now()) / 60000);
}
function dueWindowsFor(startISO) {
  const m = minutesUntil(startISO);
  const due = [];
  const isWithin = (targetMin, window=15) => Math.abs(m - targetMin) <= window;
  if (isWithin(7*24*60)) due.push("7d");
  if (isWithin(3*24*60)) due.push("3d");
  if (isWithin(24*60))   due.push("1d");
  return due;
}
// Only place calls during daytime hours in Jamaica (Mon–Sat 09:00–18:00)
function shouldCallNow() {
  const d  = new Date();
  const hour = Number(d.toLocaleString("en-GB", { timeZone: TZ, hour: "2-digit", hour12: false }));
  const dow  = d.toLocaleString("en-GB", { timeZone: TZ, weekday: "short" }); // Mon..Sun
  if (dow === "Sun") return false;
  return hour >= 9 && hour <= 18;
}

// -------------------- Calendar wrapper ------------------
const Calendar = {
  async createEvent({ physician, start, end, summary, phone, note, patientName, patientPhone, branch }) {
    const { key, id } = getCalendarIdForPhys(physician);
    const auth = getJWTAuth();
    const calendar = google.calendar({ version: "v3", auth });

    const description = [
      phone ? `Phone: ${phone}` : null,
      note ? `Note: ${note}` : null,
    ].filter(Boolean).join("\n");

    const requestBody = {
      summary: summary || "Consultation",
      description,
      start: { dateTime: start },
      end:   { dateTime: end },
      // store structured patient details for reminders/follow-ups
      extendedProperties: {
        private: {
          ...(patientName  ? { patient_name:  patientName }  : {}),
          ...(patientPhone ? { patient_phone: patientPhone } : (phone ? { patient_phone: phone } : {})),
          ...(branch       ? { branch } : {})
        }
      }
    };

    const { data } = await calendar.events.insert({ calendarId: id, requestBody });
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

// ------------------------ Retell Action ------------------
app.post("/retell/action", async (req, res) => {
  try {
    const event = req.body || {};
    const action = String(event.action || "").toLowerCase();

    // BOOK
    if (action.includes("book")) {
      const r = await Calendar.createEvent({
        physician: event.physician,
        start: event.start,
        end: event.end,
        summary: event.summary,
        phone: event.phone,
        note: event.note,
        patientName: event.name || event.patientName,
        patientPhone: event.phone || event.patientPhone,
        branch: event.branch,
      });
      return res.json({
        ok: true,
        response: `Booked for ${PHYSICIAN_DISPLAY[r.key] || r.key}.`,
        eventId: r.event.id,
        htmlLink: r.event.htmlLink
      });
    }

    // NEXT AVAILABILITY
    if (action.includes("next")) {
      const u = await Calendar.upcoming(event.physician, 1);
      if (!u.items.length) return res.json({ ok: true, response: "No upcoming events." });
      const first = u.items[0];
      return res.json({
        ok: true,
        response: `Next for ${PHYSICIAN_DISPLAY[u.key] || u.key}: ${first.summary} at ${first.start?.dateTime || first.start?.date}`,
        eventId: first.id
      });
    }

    // TRANSFER (Retell performs bridge)
    if (action.includes("transfer")) {
      const branch = String(event.branch || "winchester").toLowerCase();
      const to = BRANCH_NUMBERS[branch] || BRANCH_NUMBERS.winchester;
      return res.json({
        ok: true,
        response: `One moment please while I connect you to our ${branch} branch.`,
        connect: { to },
        transferPolicy: { ringSeconds: Math.ceil((Number(process.env.HANDOFF_TIMEOUT_MS || 25000))/1000) }
      });
    }

    // MESSAGE (email summary to branch inbox)
    if (action.includes("message")) {
      const branch = String(event.branch || "winchester").toLowerCase();
      const transporter = makeTransport();
      if (transporter) {
        const to = BRANCH_EMAILS[branch] || BRANCH_EMAILS.winchester;
        const subj = `[PRIORITY] ${branch} | ${event.reason || "General"} – ${event.name || "Caller"}`;
        const html = `
          <p><b>NEW MESSAGE – ${branch.toUpperCase()}</b></p>
          <p><b>Caller:</b> ${event.name || "Unknown"}<br/>
          <b>Phone:</b> ${event.phone || "Unknown"}<br/>
          <b>Reason:</b> ${event.reason || "General"}<br/>
          <b>Summary:</b> ${(event.summary || "").replace(/\n/g,"<br/>")}<br/>
          <b>Preferred callback:</b> ${event.preferred_callback || "—"}<br/>
          <b>Urgency:</b> ${(event.urgency || "routine").toUpperCase()}<br/>
          <b>Tone:</b> ${event.tone || "calm"}</p>
          <p><i>Recorded by: Kimberley – AI Receptionist</i></p>`;
        await transporter.sendMail({ from: FROM_EMAIL, to, subject: subj, html });
      } else {
        console.warn("[Message] SMTP not configured; skipped email.");
      }
      return res.json({
        ok: true,
        response: "Thank you. I’ll make sure this is passed along to the right team. Someone will return your call shortly."
      });
    }

    return res.json({ ok: true, response: "Ready." });
  } catch (err) {
    console.error("[/retell/action] error:", err?.response?.data || err.message);
    res.json({ ok: false, response: "Error." });
  }
});

// ------------------------ Status Dashboard --------------------------
function requireStatusAuth(req, res) {
  const must = process.env.STATUS_TOKEN;
  if (!must) return true; // public if no token is set
  const token = req.query.token || req.get("x-status-token");
  if (token === must) return true;
  res.status(401).send("Unauthorized");
  return false;
}

async function checkGoogleCalendar() {
  try {
    const auth = getJWTAuth();
    await auth.getAccessToken();
    return { ok: true, note: "Google auth OK" };
  } catch (e) {
    return { ok: false, note: e?.message || "Google auth failed" };
  }
}

async function checkSMTP() {
  try {
    const t = makeTransport();
    if (!t) return { ok: false, note: "SMTP not configured" };
    await t.verify();
    return { ok: true, note: "SMTP connection OK" };
  } catch (e) {
    return { ok: false, note: e?.message || "SMTP verify failed" };
  }
}

function summarizeConfig() {
  const hasGoogleJson = !!process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;
  const hasGoogleB64 = !!process.env.GOOGLE_CREDENTIALS_B64;
  return {
    branch_numbers: BRANCH_NUMBERS,
    branch_emails: BRANCH_EMAILS,
    moh_url: MOH_URL,
    handoff_timeout_ms: HANDOFF_TIMEOUT_MS,
    google_creds: hasGoogleJson ? "inline JSON" : (hasGoogleB64 ? "base64" : "missing"),
    smtp_host: process.env.SMTP_HOST || null,
    from_email: FROM_EMAIL
  };
}

app.get("/status.json", async (req, res) => {
  if (!requireStatusAuth(req, res)) return;
  const [google, smtp] = await Promise.all([checkGoogleCalendar(), checkSMTP()]);
  res.json({
    ok: google.ok && smtp.ok,
    time: new Date().toISOString(),
    uptime_seconds: Math.round(process.uptime()),
    checks: { google, smtp },
    config: summarizeConfig(),
    endpoints: { health: "/health", action_server: "/retell/action", status_html: "/status" }
  });
});

app.get("/status", async (req, res) => {
  if (!requireStatusAuth(req, res)) return;
  const [google, smtp] = await Promise.all([checkGoogleCalendar(), checkSMTP()]);
  const cfg = summarizeConfig();
  const ok = google.ok && smtp.ok;
  const badge = (b) => b ? `<span class="ok">OK</span>` : `<span class="fail">FAIL</span>`;
  const row = (k, v) => `<tr><td>${k}</td><td>${v ?? ""}</td></tr>`;
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.send(`<!doctype html><html><head><meta charset="utf-8"/>
  <title>WHC PBX Status</title>
  <style>
    body { font-family:system-ui, sans-serif; margin:24px; color:#111; }
    .ok{background:#ecfdf5;color:#065f46;padding:2px 8px;border-radius:999px;font-weight:600;}
    .fail{background:#fef2f2;color:#991b1b;padding:2px 8px;border-radius:999px;font-weight:600;}
    table{width:100%;border-collapse:collapse;}
    td{border-bottom:1px solid #eee;padding:6px 4px;vertical-align:top;}
    h3{margin-top:24px;}
  </style></head><body>
  <h1>WHC PBX Status ${badge(ok)}</h1>
  <p><b>Time:</b> ${new Date().toLocaleString("en-JM",{timeZone:"America/Jamaica"})}</p>
  <p><b>Uptime:</b> ${Math.round(process.uptime())}s</p>
  <h3>Checks</h3>
  <table>${row("Google Calendar", `${badge(google.ok)} ${google.note}`)}${row("SMTP", `${badge(smtp.ok)} ${smtp.note}`)}</table>
  <h3>Config</h3>
  <table>${row("MOH URL", cfg.moh_url)}${row("Handoff Timeout (ms)", cfg.handoff_timeout_ms)}${row("Google Credentials", cfg.google_creds)}${row("SMTP Host", cfg.smtp_host)}${row("From Email", cfg.from_email)}</table>
  <h3>Branch Numbers</h3>
  <table>${row("Winchester", cfg.branch_numbers.winchester)}${row("Portmore", cfg.branch_numbers.portmore)}${row("Ardenne", cfg.branch_numbers.ardenne)}${row("Sav", cfg.branch_numbers.sav)}</table>
  </body></html>`);
});

// --------------- Scheduled Job: reminders & follow-ups ---------------
app.post("/jobs/run-reminders", async (req, res) => {
  try {
    const must = process.env.STATUS_TOKEN;
    const token = req.query.token || req.get("x-status-token");
    if (must && token !== must) return res.status(401).json({ ok: false, error: "Unauthorized" });

    if (!shouldCallNow()) {
      return res.json({ ok: true, note: "Outside calling window; skipped." });
    }

    const auth = getJWTAuth();
    const calendar = google.calendar({ version: "v3", auth });

    const physicianKeys = Object.keys(PHYSICIANS);
    const results = [];

    for (const key of physicianKeys) {
      const calendarId = PHYSICIANS[key];
      const timeMin = new Date(Date.now() - 2*24*3600e3).toISOString(); // look back 2 days (follow-ups)
      const timeMax = new Date(Date.now() + 8*24*3600e3).toISOString(); // ahead 8 days (7/3/1d)
      const { data } = await calendar.events.list({
        calendarId, timeMin, timeMax, singleEvents: true, orderBy: "startTime"
      });

      let actions = 0;
      for (const ev of (data.items || [])) {
        const startISO = ev.start?.dateTime || ev.start?.date;
        if (!startISO) continue;

        // read structured patient data
        const priv = ev.extendedProperties?.private || {};
        const patientName  = priv.patient_name || null;
        const patientPhone = priv.patient_phone || null;
        const branch       = priv.branch || "winchester";
        if (!patientPhone) continue; // nothing to call

        // pre-visit reminders
        let changed = false;
        for (const win of dueWindowsFor(startISO)) {
          const flag = `reminded_${win}`;
          if (!priv[flag]) {
            await callPatient({ phone: patientPhone, patientName, apptTime: startISO, branch });
            priv[flag] = "true";
            actions++; changed = true;
          }
        }

        // post-visit follow-up (~1 day after end)
        const endISO = ev.end?.dateTime || ev.end?.date || startISO;
        const minsSinceEnd = -minutesUntil(endISO); // positive after end
        if (minsSinceEnd >= (24*60 - 15) && minsSinceEnd <= (24*60 + 15) && !priv.followup_1d) {
          await callPatient({ phone: patientPhone, patientName, apptTime: startISO, branch });
          priv.followup_1d = "true";
          actions++; changed = true;
        }

        if (changed) {
          await calendar.events.patch({
            calendarId, eventId: ev.id,
            requestBody: { extendedProperties: { private: priv } }
          });
        }
      }
      results.push({ physician: key, scanned: (data.items || []).length, actions });
    }

    res.json({ ok: true, results });
  } catch (e) {
    console.error("[/jobs/run-reminders]", e.response?.data || e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ------------------------ Start --------------------------
const PORT = process.env.PORT || 8080;
const HOST = "0.0.0.0";
const server = http.createServer(app);

server.listen(PORT, HOST, () => console.log(`WHC server listening on :${PORT}`));

process.on("SIGTERM", () => {
  console.log("Received SIGTERM, shutting down gracefully…");
  server.close(() => {
    console.log("HTTP server closed");
    process.exit(0);
  });
  setTimeout(() => process.exit(0), 10000).unref();
});
