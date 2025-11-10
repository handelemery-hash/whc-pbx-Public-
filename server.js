// server.js — WHC PBX + Calendar + Procedures + Jobs (Retell + Telnyx + Email + Google)
// -------------------------------------------------------------------------------------
// ENV REQUIRED (Railway → Variables):
// TELNYX_API_KEY, TELNYX_CONNECTION_ID, TELNYX_OUTBOUND_CALLER_ID, MOH_URL, HANDOFF_TIMEOUT_MS
// BRANCH_WINCHESTER, BRANCH_PORTMORE, BRANCH_ARDENNE, BRANCH_SAV
// SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, FROM_EMAIL
// EMAIL_WINCHESTER, EMAIL_PORTMORE, EMAIL_ARDENNE, EMAIL_SAV
// GOOGLE_APPLICATION_CREDENTIALS_JSON or GOOGLE_CREDENTIALS_B64
// PROC_CAL_WINCHESTER, PROC_CAL_PORTMORE, PROC_CAL_ARDENNE, PROC_CAL_SAV
// RETELL_API_KEY, RETELL_AGENT_ID  (for outbound reminder/birthday calls)
// BIRTHDAYS_SHEET_ID               (Google Sheet id for birthdays)
// STATUS_TOKEN                     (protect /status and /jobs/* endpoints)

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

app.use((req, _res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`);
  next();
});

// -------------------------- Config -----------------------
const HANDOFF_TIMEOUT_MS = Number(process.env.HANDOFF_TIMEOUT_MS || 25000);
const MOH_URL = process.env.MOH_URL || "https://example.com/moh.mp3";
const STATUS_TOKEN = process.env.STATUS_TOKEN || "";

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

// Procedure calendars (by branch)
const PROCEDURE_CALENDARS = {
  winchester: process.env.PROC_CAL_WINCHESTER || "a434db3192bb86669c1238fd8840d62d72a254cea1b58d01da3dd61eefeb1ba6@group.calendar.google.com",
  portmore:   process.env.PROC_CAL_PORTMORE   || "1dce088aed350313f0848a6950f97fdaa44616145b523097b23877f3aa0278a5@group.calendar.google.com",
  ardenne:    process.env.PROC_CAL_ARDENNE    || "d86eacf4f06f69310210007c96a91941ba3f0eb37a6165e350f3227b604cee06@group.calendar.google.com",
  sav:        process.env.PROC_CAL_SAV        || "79ff142150e20453f5600b25f445c1d27f03d30594dadc5c777b4ed9af360e5e@group.calendar.google.com",
};

// ---------------------- Hours & After-Hours Guard ----------------------
function isSunday(d) { return d.getUTCDay ? d.getUTCDay() === 0 : new Date(d).getUTCDay() === 0; }
// NOTE: Jamaica is America/Jamaica (UTC-5, no DST). For precise TZ use a timezone lib.
const HOURS = {
  winchester: { monfri: { start: 8.5, end: 16.5 }, sat: null },
  ardenne:    { monfri: { start: 8.5, end: 16.5 }, sat: null },
  sav:        { monfri: { start: 8.5, end: 16.5 }, sat: null },
  portmore:   { monfri: { start: 10,  end: 17   }, sat: { start: 10, end: 14 } },
};
function isAfterHours(branch, when = new Date()) {
  const b = (branch || "winchester").toLowerCase();
  const h = HOURS[b] || HOURS.winchester;
  const day = when.getDay(); // 0=Sun..6=Sat
  const hour = when.getHours() + when.getMinutes()/60;
  if (day === 0) return true; // Sunday closed
  if (day === 6) {
    if (!h.sat) return true;
    return !(hour >= h.sat.start && hour < h.sat.end);
  }
  // Mon-Fri
  return !(hour >= h.monfri.start && hour < h.monfri.end);
}

// ---------------------- Google Auth ----------------------
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
function getJWTAuth(scopes = ["https://www.googleapis.com/auth/calendar"]) {
  const creds = loadServiceAccountJSON();
  return new google.auth.JWT(creds.client_email, null, creds.private_key, scopes);
}
function normalizePhys(s) {
  return String(s || "").trim().toLowerCase().replace(/\s+/g, "_");
}

// ---------------------- Procedure Classification ----------------------
const SERVICE_SYNONYMS = [
  { canon: "ecg", terms: ["ecg","electrocardiogram","electrocardiograms","stress ecg"] },
  { canon: "echo", terms: ["echo","echocardiogram","echocardiograms"] },
  { canon: "stress_echo", terms: ["stress echo","stress echocardiogram","stress echocardiograms","exercise stress echocardiogram","exercise stress echocardiograms"] },
  { canon: "stress_test", terms: ["stress test","exercise stress test","treadmill test","exercise test"] },
  { canon: "bubble_echo", terms: ["bubble study","bubble studies","bubble echo","bubble echocardiogram","bubble echocardiograms"] },
  { canon: "dobutamine_stress_echo", terms: ["dobutamine stress echo","dobutamine stress echocardiogram"] },
  { canon: "holter_24", terms: ["24 hour holter","24 hr holter","24-hour holter","holter 24"] },
  { canon: "holter_48", terms: ["48 hour holter","48 hr holter","48-hour holter","holter 48"] },
  { canon: "abpm_24", terms: ["24 hour abpm","24 hr abpm","24-hour abpm","abpm 24","ambulatory blood pressure 24","ambulatory bp 24"] },
  { canon: "abpm_48", terms: ["48 hour abpm","48 hr abpm","48-hour abpm","abpm 48","ambulatory blood pressure 48","ambulatory bp 48"] },
  { canon: "pacemaker", terms: ["pacemaker interrogation","pacemaker check","pacemaker clinic"] },
  { canon: "consult", terms: ["consult","consultation","follow up","follow-up","review"] },
];
function normalizeText(s){return String(s||"").toLowerCase().replace(/\s+/g," ").trim();}
function classifyService(rawService){
  const txt = normalizeText(rawService||"");
  for (const g of SERVICE_SYNONYMS) for (const t of g.terms) if (txt.includes(normalizeText(t))) return { canon:g.canon, isProcedure:g.canon!=="consult" };
  if (["ecg","echo","echocardiogram","stress","bubble","holter","abpm","pacemaker"].some(h=>txt.includes(h))) return { canon:"procedure_generic", isProcedure:true };
  return { canon:"consult", isProcedure:false };
}
function getCalendarIdForBooking({ service, branch, physicianKey }) {
  const { isProcedure } = classifyService(service||"");
  if (isProcedure) {
    const b = (branch||"winchester").toLowerCase();
    const id = PROCEDURE_CALENDARS[b] || PROCEDURE_CALENDARS.winchester;
    if (!id) throw new Error(`Missing procedure calendar for branch '${b}'`);
    return id;
  }
  const k = normalizePhys(physicianKey);
  const physId = PHYSICIANS[k];
  if (!physId) throw new Error(`Unknown physician for consult booking: '${physicianKey}'`);
  return physId;
}
function getCalendarIdForPhys(physician){
  const k = normalizePhys(physician);
  const id = PHYSICIANS[k];
  if(!id) throw new Error(`Unknown physician '${physician}'`);
  return { key:k, id };
}

// ---------------------- Calendar Wrapper ----------------------
const Calendar = {
  async createEvent({ physician, start, end, summary, service, phone, note, branch }) {
    const calendarId = getCalendarIdForBooking({
      service: service || summary,
      branch: (branch || note?.branch || "winchester"),
      physicianKey: physician || ""
    });
    const auth = getJWTAuth();
    const calendar = google.calendar({ version:"v3", auth });
    const description = [
      phone ? `Phone: ${phone}` : null,
      note ? `Note: ${typeof note === "string" ? note : JSON.stringify(note)}` : null,
      physician ? `Physician: ${PHYSICIAN_DISPLAY[normalizePhys(physician)] || physician}` : null,
      `Branch: ${(branch || note?.branch || "winchester")}`,
      service ? `Service: ${service}` : null,
    ].filter(Boolean).join("\n");
    const title = service || summary || "Consultation";
    const { data } = await calendar.events.insert({
      calendarId,
      requestBody: { summary: title, description, start:{dateTime:start}, end:{dateTime:end} },
    });
    const physKey = physician ? normalizePhys(physician) : null;
    return { key: physKey, id: calendarId, event: data };
  },

  async upcoming(physician, max = 10) {
    const { key, id } = getCalendarIdForPhys(physician);
    const auth = getJWTAuth();
    const calendar = google.calendar({ version:"v3", auth });
    const { data } = await calendar.events.list({
      calendarId: id, timeMin: new Date().toISOString(),
      maxResults: Math.min(Math.max(+max || 10, 1), 50),
      singleEvents:true, orderBy:"startTime"
    });
    return { key, id, items: data.items || [] };
  },

  async upcomingByCalendarId(calendarId, max = 50) {
    const auth = getJWTAuth();
    const calendar = google.calendar({ version:"v3", auth });
    const { data } = await calendar.events.list({
      calendarId, timeMin: new Date().toISOString(),
      maxResults: Math.min(Math.max(+max || 50, 1), 50),
      singleEvents:true, orderBy:"startTime"
    });
    return data.items || [];
  },

  async deleteEvent(physician, eventId) {
    const { id } = getCalendarIdForPhys(physician);
    const auth = getJWTAuth();
    const calendar = google.calendar({ version:"v3", auth });
    await calendar.events.delete({ calendarId:id, eventId });
    return true;
  },
};

// --------------------------- Email -----------------------
function makeTransport(){
  const host = process.env.SMTP_HOST;
  const port = Number(process.env.SMTP_PORT || 587);
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;
  if (!host || !user || !pass) return null;
  return nodemailer.createTransport({ host, port, secure: port===465, auth:{user,pass} });
}
async function sendVoicemailEmail({ branch, caller, recordingUrl, transcript }){
  const to = BRANCH_EMAILS[branch] || BRANCH_EMAILS.winchester;
  const transporter = makeTransport();
  if (!transporter) { console.warn("[Email] SMTP not configured; skipping voicemail email"); return; }
  const html = `
    <p><b>New voicemail</b> for <b>${branch}</b></p>
    <p><b>From:</b> ${caller || "Unknown"}</p>
    <p><b>Recording:</b> <a href="${recordingUrl}">${recordingUrl}</a></p>
    ${transcript ? `<pre>${transcript}</pre>` : ""}
  `;
  await transporter.sendMail({ from: FROM_EMAIL, to, subject:`New Voicemail - ${branch} branch`, html });
}

// ------------------------ Calendar Routes ----------------
app.post("/calendar/create", async (req, res) => {
  try {
    const { physician, start, end, summary, phone, note, service, branch } = req.body || {};
    if (!start || !end || (!physician && !service)) {
      return res.status(400).json({ ok:false, error:"start, end, and (physician or service) required" });
    }
    const mergedNote = typeof note === "object" ? note : (note ? { text: note } : {});
    const r = await Calendar.createEvent({
      physician, start, end, summary, service, phone,
      note: { ...mergedNote, branch: branch || mergedNote.branch },
      branch
    });
    return res.json({
      ok:true,
      eventId:r.event.id, htmlLink:r.event.htmlLink,
      response:`Created ${service ? "procedure" : "appointment"} ${service ? `(${service})` : ""}${physician ? ` for ${PHYSICIAN_DISPLAY[r.key] || r.key}` : ""}`,
    });
  } catch (err) {
    console.error("[/calendar/create] error:", err?.response?.data || err.message);
    res.status(500).json({ ok:false, error: err.message });
  }
});

app.get("/calendar/upcoming/:physician", async (req, res) => {
  try {
    const { physician } = req.params;
    const { max } = req.query;
    const { key, items } = await Calendar.upcoming(physician, Number(max || 10));
    res.json({
      ok:true,
      physician: PHYSICIAN_DISPLAY[key] || key,
      count: items.length,
      events: items.map(ev => ({
        id: ev.id, summary: ev.summary,
        start: ev.start?.dateTime || ev.start?.date,
        end: ev.end?.dateTime || ev.end?.date,
        link: ev.htmlLink,
      })),
    });
  } catch (err) {
    console.error("[/calendar/upcoming] error:", err?.response?.data || err.message);
    res.status(500).json({ ok:false, error: err.message });
  }
});

app.post("/calendar/delete", async (req, res) => {
  try {
    const { physician, eventId } = req.body || {};
    if (!physician || !eventId) return res.status(400).json({ ok:false, error:"physician and eventId required" });
    await Calendar.deleteEvent(physician, eventId);
    res.json({ ok:true });
  } catch (err) {
    console.error("[/calendar/delete] error:", err?.response?.data || err.message);
    res.status(500).json({ ok:false, error: err.message });
  }
});

// ------------------------ Retell Action ------------------
const RETELL_API_KEY = process.env.RETELL_API_KEY || "";
const RETELL_AGENT_ID = process.env.RETELL_AGENT_ID || "";

async function retellCall({ to, from, variables = {}, call_type = "reminder" }) {
  if (!RETELL_API_KEY || !RETELL_AGENT_ID) {
    console.warn("[Retell] Missing RETELL_API_KEY / RETELL_AGENT_ID; skipping call");
    return { ok: false, error: "missing_retell_keys" };
  }
  try {
    const r = await axios.post(
      "https://api.retellai.com/v2/calls",
      {
        agent_id: RETELL_AGENT_ID,
        to_number: to,
        from_number: from || process.env.TELNYX_OUTBOUND_CALLER_ID,
        metadata: { call_type, ...variables },
      },
      { headers: { Authorization: `Bearer ${RETELL_API_KEY}` } }
    );
    return { ok: true, data: r.data };
  } catch (e) {
    console.error("[Retell] outbound error:", e?.response?.data || e.message);
    return { ok: false, error: e.message };
  }
}

app.post("/retell/action", async (req, res) => {
  try {
    const event = req.body || {};
    const action = String(event.action || "").toLowerCase();

    if (action.includes("book")) {
      const r = await Calendar.createEvent({
        physician: event.physician,
        start: event.start, end: event.end,
        summary: event.service || event.summary,
        service: event.service, phone: event.phone,
        note: { ...event.note, branch: event.branch, service: event.service },
        branch: event.branch,
      });
      const who = event.service ? "procedure" : "appointment";
      return res.json({
        ok:true,
        response:`Booked ${who}${event.service ? ` (${event.service})` : ""}${event.physician ? ` for ${event.physician}` : ""}.`,
        eventId:r.event.id
      });
    }

    if (action.includes("next")) {
      const u = await Calendar.upcoming(event.physician, 1);
      if (!u.items.length) return res.json({ ok:true, response:"No upcoming events." });
      const first = u.items[0];
      return res.json({
        ok:true,
        response:`Next for ${PHYSICIAN_DISPLAY[u.key] || u.key}: ${first.summary} at ${first.start?.dateTime || first.start?.date}`,
        eventId:first.id
      });
    }

    if (action.includes("connect")) {
      const result = await retellCall({
        to: event.to, from: event.from, call_type: "connect_test",
        variables: { note: event.note || "" }
      });
      return res.json({ ok: true, response: result.ok ? "Call placed." : `Failed: ${result.error}` });
    }

    return res.json({ ok:true, response:"Ready." });
  } catch (err) {
    console.error("[/retell/action] error:", err?.response?.data || err.message);
    res.json({ ok:false, response:"Error." });
  }
});

// ------------------------ Telnyx PBX ---------------------
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

function resolveBranchFromMeta(meta = {}) {
  const m = (meta.branch || meta.forwarded_from || "").toString().toLowerCase();
  if (m.includes("portmore")) return "portmore";
  if (m.includes("ardenne")) return "ardenne";
  if (m.includes("sav")) return "sav";
  return "winchester";
}

// Inbound webhook with after-hours guard
app.post("/telnyx/inbound", async (req, res) => {
  if (!telnyx) return res.status(200).json({ ok: true, note: "Telnyx not configured" });
  try {
    const data = req.body?.data || {};
    const eventType = data?.event_type;
    const payload = data?.payload || {};
    const callControlId = payload.call_control_id;

    console.log("[Telnyx]", eventType);

    if (eventType === "call.initiated") {
      await tx("answer", { call_control_id: callControlId });
      await tx("playback_start", { call_control_id: callControlId, audio_url: MOH_URL, overlay:true, loop:true });

      const branchMeta = payload.client_state ? JSON.parse(Buffer.from(payload.client_state, "base64").toString("utf8")) : {};
      const branch = resolveBranchFromMeta(branchMeta);

      // AFTER-HOURS GUARD: if closed, skip transfer & trigger voicemail email
      if (isAfterHours(branch, new Date())) {
        await tx("playback_stop", { call_control_id: callControlId });
        await sendVoicemailEmail({
          branch,
          caller: payload?.from || payload?.from_number || "Unknown",
          recordingUrl: "(no recording)",
          transcript: null,
        });
        return res.json({ ok:true, note:"after-hours, message taken" });
      }

      const target = BRANCH_NUMBERS[branch] || BRANCH_NUMBERS.winchester;
      await tx("transfer", {
        call_control_id: callControlId,
        to: target,
        from: TELNYX_OUTBOUND_CALLER_ID,
        timeout_secs: Math.ceil(HANDOFF_TIMEOUT_MS / 1000),
      });
      return res.json({ ok:true });
    }

    if (eventType === "call.bridged") {
      await tx("playback_stop", { call_control_id: callControlId });
      return res.json({ ok:true });
    }

    if (eventType === "call.ended" || eventType === "transfer.failed") {
      const branchMeta = payload.client_state ? JSON.parse(Buffer.from(payload.client_state, "base64").toString("utf8")) : {};
      const branch = resolveBranchFromMeta(branchMeta);
      await sendVoicemailEmail({
        branch,
        caller: payload?.from || payload?.from_number || "Unknown",
        recordingUrl: "(no recording in this minimal build)",
        transcript: null,
      });
      return res.json({ ok:true });
    }

    res.json({ ok:true });
  } catch (err) {
    console.error("[/telnyx/inbound] error:", err?.response?.data || err.message);
    res.status(200).json({ ok:true }); // acknowledge to avoid retries
  }
});

// ------------------------ Jobs: Reminders & Birthdays ------------------
// Google Sheets client
async function getSheets() {
  const auth = getJWTAuth(["https://www.googleapis.com/auth/spreadsheets"]);
  return google.sheets({ version: "v4", auth });
}

// Parse helpers from event description
function extractPhoneFromDescription(desc="") {
  const m = desc.match(/Phone:\s*([+\d][\d\s\-()]+)/i);
  return m ? m[1].replace(/[^\d+]/g,"") : null;
}
function extractBranchFromDescription(desc="") {
  const m = desc.match(/Branch:\s*(\w+)/i);
  return m ? m[1].toLowerCase() : "winchester";
}
function extractServiceFromDescription(desc="") {
  const m = desc.match(/Service:\s*([^\n]+)/i);
  return m ? m[1].trim() : null;
}

// Secure middleware for jobs/status
function requireStatusToken(req, res, next){
  const t = req.query.token || req.headers["x-status-token"];
  if (!STATUS_TOKEN || t !== STATUS_TOKEN) return res.status(401).json({ ok:false, error:"unauthorized" });
  next();
}

// Reminders (7/3/1 days before) + Follow-ups (1 day after)
// — now runs for BOTH physician calendars AND procedure calendars.
app.post("/jobs/run-reminders", requireStatusToken, async (req, res) => {
  try {
    const today = new Date();
    const results = [];

    // 1) Physician calendars (existing behavior)
    for (const physKey of Object.keys(PHYSICIANS)) {
      const { items } = await Calendar.upcoming(physKey, 50);
      for (const ev of items) {
        const startIso = ev.start?.dateTime || ev.start?.date;
        if (!startIso) continue;
        const start = new Date(startIso);
        const daysDiff = Math.round((start - today) / (1000*60*60*24));
        const desc = ev.description || "";
        const phone = extractPhoneFromDescription(desc);
        const branch = extractBranchFromDescription(desc);

        if ([7,3,1].includes(daysDiff) && phone) {
          const r = await retellCall({
            to: phone, call_type:"reminder",
            variables: {
              appointment_time: start.toISOString(),
              branch,
              physician: PHYSICIAN_DISPLAY[physKey] || physKey
            }
          });
          results.push({ type:"physician", eventId: ev.id, phone, daysDiff, ok: r.ok });
        }

        const daysAfter = Math.round((today - start) / (1000*60*60*24));
        if (daysAfter === 1 && phone) {
          const r = await retellCall({
            to: phone, call_type:"followup",
            variables: { branch, physician: PHYSICIAN_DISPLAY[physKey] || physKey }
          });
          results.push({ type:"physician", eventId: ev.id, phone, followup:true, ok: r.ok });
        }
      }
    }

    // 2) Procedure calendars (new)
    for (const [branchKey, calId] of Object.entries(PROCEDURE_CALENDARS)) {
      if (!calId) continue;
      const items = await Calendar.upcomingByCalendarId(calId, 50);
      for (const ev of items) {
        const startIso = ev.start?.dateTime || ev.start?.date;
        if (!startIso) continue;
        const start = new Date(startIso);
        const daysDiff = Math.round((start - today) / (1000*60*60*24));
        const desc = ev.description || "";
        const phone = extractPhoneFromDescription(desc);
        // Prefer explicit Branch in description; else infer from calendar we’re scanning
        const branch = extractBranchFromDescription(desc) || branchKey;
        const service = extractServiceFromDescription(desc) || ev.summary || "Procedure";

        if ([7,3,1].includes(daysDiff) && phone) {
          const r = await retellCall({
            to: phone, call_type:"reminder",
            variables: {
              appointment_time: start.toISOString(),
              branch,
              service
            }
          });
          results.push({ type:"procedure", eventId: ev.id, phone, daysDiff, service, ok: r.ok });
        }

        const daysAfter = Math.round((today - start) / (1000*60*60*24));
        if (daysAfter === 1 && phone) {
          const r = await retellCall({
            to: phone, call_type:"followup",
            variables: { branch, service }
          });
          results.push({ type:"procedure", eventId: ev.id, phone, followup:true, service, ok: r.ok });
        }
      }
    }

    res.json({ ok:true, results });
  } catch (err) {
    console.error("[/jobs/run-reminders] error:", err?.response?.data || err.message);
    res.status(500).json({ ok:false, error: err.message });
  }
});

// Birthday calls (Google Sheet)
app.post("/jobs/run-birthdays", requireStatusToken, async (req, res) => {
  try {
    const SHEET_ID = process.env.BIRTHDAYS_SHEET_ID;
    if (!SHEET_ID) return res.status(400).json({ ok:false, error:"missing BIRTHDAYS_SHEET_ID" });
    const sheets = await getSheets();

    // Expect header row; range A:Z is safe default
    const range = "Sheet1!A:Z";
    const { data } = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range });
    const rows = data.values || [];
    if (!rows.length) return res.json({ ok:true, note:"no rows" });

    const header = rows[0].map(h => String(h||"").trim().toLowerCase());
    const idx = (name) => header.indexOf(name);
    const iName = idx("full_name");
    const iPhone = idx("phone_e164");
    const iDob = idx("dob_yyyy_mm_dd");
    const iBranch = idx("branch");
    const iOptOut = idx("opt_out");
    const iLastYear = idx("last_called_year");

    const today = new Date();
    const mmdd = ("0"+(today.getMonth()+1)).slice(-2) + "-" + ("0"+today.getDate()).slice(-2);
    const currentYear = today.getFullYear();
    const out = [];

    for (let r = 1; r < rows.length; r++) {
      const row = rows[r];
      const name = row[iName] || "";
      const phone = row[iPhone] || "";
      const dob = row[iDob] || "";
      const branch = (row[iBranch] || "winchester").toLowerCase();
      const optOut = String(row[iOptOut] || "").toLowerCase() === "yes";
      const lastYear = Number(row[iLastYear] || 0);

      if (!phone || !dob || optOut) continue;
      const dobMmdd = dob.slice(5,10);
      if (dobMmdd !== mmdd) continue;
      if (lastYear === currentYear) continue;

      const rCall = await retellCall({
        to: phone, call_type:"birthday",
        variables: { patient_name: name, branch }
      });
      out.push({ row:r+1, name, phone, ok: rCall.ok });

      // Write back last_called_year
      if (iLastYear >= 0 && rCall.ok) {
        const col = String.fromCharCode(65 + iLastYear); // naive A..Z
        const rangeWrite = `Sheet1!${col}${r+1}`;
        await sheets.spreadsheets.values.update({
          spreadsheetId: SHEET_ID, range: rangeWrite, valueInputOption:"RAW",
          requestBody: { values: [[ String(currentYear) ]] }
        });
      }
    }

    res.json({ ok:true, processed: out.length, details: out });
  } catch (err) {
    console.error("[/jobs/run-birthdays] error:", err?.response?.data || err.message);
    res.status(500).json({ ok:false, error: err.message });
  }
});

// ------------------------ Diagnostics --------------------
app.get("/status.json", requireStatusToken, async (req, res) => {
  try {
    const checks = {
      smtp: !!(process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS),
      google_creds: !!(process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON || process.env.GOOGLE_CREDENTIALS_B64),
      telnyx: !!TELNYX_API_KEY,
      retell: !!(process.env.RETELL_API_KEY && process.env.RETELL_AGENT_ID),
      proc_cals: Object.values(PROCEDURE_CALENDARS).every(Boolean),
    };
    res.json({
      ok:true,
      time: new Date().toISOString(),
      checks,
      branches: { numbers: BRANCH_NUMBERS, emails: BRANCH_EMAILS },
      physicians: Object.keys(PHYSICIANS),
    });
  } catch (e) {
    res.status(500).json({ ok:false, error:e.message });
  }
});

// ------------------------ Start --------------------------
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`WHC server listening on :${PORT}`));
