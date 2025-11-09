// server.js — WHC PBX + Calendar + Retell + Email + Status + Reminders + Hours Guard + Birthdays (enhanced)
// ---------------------------------------------------------------------------------------------------------

import http from "http";
import express from "express";
import cors from "cors";
import { google } from "googleapis";
import axios from "axios";
import nodemailer from "nodemailer";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc.js";
import tz from "dayjs/plugin/timezone.js";
dayjs.extend(utc);
dayjs.extend(tz);

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
const TZ = "America/Jamaica";

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

function normalizePhys(s) {
  return String(s || "").trim().toLowerCase().replace(/\s+/g, "_");
}
function getCalendarIdForPhys(phys) {
  const key = normalizePhys(phys);
  const id = PHYSICIANS[key];
  if (!id) throw new Error(`Unknown physician '${phys}'`);
  return { key, id };
}

// ----------------------- Business Hours -----------------------
const HOURS = {
  timezone: TZ,
  winchester: { mon_fri: ["08:30","16:30"], sat: null,               sun: null },
  ardenne:    { mon_fri: ["08:30","16:30"], sat: null,               sun: null },
  sav:        { mon_fri: ["08:30","16:30"], sat: null,               sun: null },
  portmore:   { mon_fri: ["10:00","17:00"], sat: ["10:00","14:00"],  sun: null },
};
function hmToMin(hm){ const [h,m]=hm.split(":").map(Number); return h*60+(m||0); }
function nowJM(d=new Date()){ return dayjs(d).tz(TZ); }
function isOpenNow(branchRaw, d=new Date()){
  const branch = (branchRaw||"winchester").toLowerCase();
  const spec = HOURS[branch] || HOURS.winchester;
  const local = nowJM(d);
  const day = local.day(); // 0..6
  const minutes = local.hour()*60 + local.minute();
  let open=null, close=null;
  if (day===6 && spec.sat) [open, close] = spec.sat.map(hmToMin);
  else if (day>=1 && day<=5 && spec.mon_fri) [open, close] = spec.mon_fri.map(hmToMin);
  return (open!=null && minutes>=open && minutes<=close);
}
function nextOpenString(branchRaw, from=new Date()){
  const branch = (branchRaw||"winchester").toLowerCase();
  const spec = HOURS[branch] || HOURS.winchester;
  let base = nowJM(from);
  for (let i=0;i<7;i++){
    const d = base.add(i,'day');
    const day = d.day();
    let window = null;
    if (day===6 && spec.sat) window = spec.sat;
    else if (day>=1 && day<=5 && spec.mon_fri) window = spec.mon_fri;
    if (!window) continue;
    const labelDay = i===0 ? "today" : i===1 ? "tomorrow" : d.format("dddd");
    return `${labelDay} at ${window[0]}`;
  }
  return "the next business day";
}

// ---------------------- Google Auth (Calendar + Sheets) -------
function loadServiceAccountJSON() {
  const inline = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;
  const b64 = process.env.GOOGLE_CREDENTIALS_B64;
  if (inline) {
    const txt = inline.trim();
    const json = JSON.parse(txt.startsWith("{") ? txt : Buffer.from(txt, "base64").toString("utf8"));
    console.log("✅ [Google] Loaded credentials from GOOGLE_APPLICATION_CREDENTIALS_JSON");
    return json;
  }
  if (b64) {
    const json = JSON.parse(Buffer.from(b64, "base64").toString("utf8"));
    console.log("✅ [Google] Loaded credentials from GOOGLE_CREDENTIALS_B64");
    return json;
  }
  throw new Error("Missing Google credentials env");
}
function getJWTAuth() {
  const creds = loadServiceAccountJSON();
  return new google.auth.JWT(
    creds.client_email, null, creds.private_key,
    [
      "https://www.googleapis.com/auth/calendar",
      "https://www.googleapis.com/auth/spreadsheets"
    ]
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
  if (!transporter) { console.warn("[Email] SMTP not configured; skipping voicemail email"); return; }
  const html = `
    <p><b>New voicemail</b> for <b>${branch}</b></p>
    <p><b>From:</b> ${caller || "Unknown"}</p>
    <p><b>Recording:</b> <a href="${recordingUrl}">${recordingUrl}</a></p>
    ${transcript ? `<pre>${transcript}</pre>` : ""}
  `;
  await transporter.sendMail({ from: FROM_EMAIL, to, subject: `New Voicemail - ${branch} branch`, html });
}

// ------------------- Retell Outbound (reminders etc.) ---
const RETELL_API_KEY = process.env.RETELL_API_KEY;
const RETELL_AGENT_ID = process.env.RETELL_AGENT_ID;
const RETELL_NUMBER   = process.env.RETELL_NUMBER;

function toISOish(dt) {
  try { return new Date(dt).toISOString().replace(/\.\d{3}Z$/, "Z"); } catch { return dt; }
}
async function callPatient({ phone, patientName, apptTime, branch, callType }) {
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
        appointment_time: toISOish(apptTime),
        branch: branch || "winchester",
        call_type: callType || "reminder"
      }
    },
    { headers: { Authorization: `Bearer ${RETELL_API_KEY}` }, timeout: 10000 }
  );
  return r.data || { ok: true };
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
  const d  = dayjs().tz(TZ);
  const hour = d.hour();
  const dow  = d.day(); // 0=Sun
  if (dow === 0) return false;
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

// -------------------- Birthdays (Google Sheets) ---------
const BIRTHDAYS_SHEET_ID = process.env.BIRTHDAYS_SHEET_ID;

// Header cache for column mapping
let BDAY_HDR = null;
async function getBirthdayHeader() {
  if (BDAY_HDR) return BDAY_HDR;
  const auth = getJWTAuth();
  const sheets = google.sheets({ version: "v4", auth });
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: BIRTHDAYS_SHEET_ID,
    range: "Sheet1!A1:Z1"
  });
  BDAY_HDR = (data.values && data.values[0]) || [];
  return BDAY_HDR;
}
function colIdxToA1(idx) {
  // 0 -> A, 1 -> B ...
  let n = idx + 1, s = "";
  while (n > 0) {
    const r = (n - 1) % 26;
    s = String.fromCharCode(65 + r) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

async function readBirthdaySheet() {
  if (!BIRTHDAYS_SHEET_ID) return [];
  const auth = getJWTAuth();
  const sheets = google.sheets({ version: "v4", auth });
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: BIRTHDAYS_SHEET_ID,
    range: "Sheet1!A:Z"
  });
  const rows = data.values || [];
  const header = rows.shift() || [];
  const idx = (name) => header.indexOf(name);
  const out = [];
  for (let r = 0; r < rows.length; r++) {
    const row = rows[r];
    out.push({
      rowIndex: r + 2,
      full_name: row[idx("full_name")] || "",
      phone_e164: row[idx("phone_e164")] || "",
      dob_yyyy_mm_dd: row[idx("dob_yyyy_mm_dd")] || "",
      branch: (row[idx("branch")] || "winchester").toLowerCase(),
      opt_out: (row[idx("opt_out")] || "").trim(),
      opt_out_reason: (row[idx("opt_out_reason")] || "").trim(),
      last_called_year: (row[idx("last_called_year")] || "").trim(),
      last_outcome: (row[idx("last_outcome")] || "").trim(),
      last_outcome_ts: (row[idx("last_outcome_ts")] || "").trim(),
      deferred_for_yyyy_mm_dd: (row[idx("deferred_for_yyyy_mm_dd")] || "").trim(),
      deferred_reason: (row[idx("deferred_reason")] || "").trim(),
      dob_correction: (row[idx("dob_correction")] || "").trim(),
      new_phone_candidate: (row[idx("new_phone_candidate")] || "").trim(),
      status_flag: (row[idx("status_flag")] || "").trim(),
      status_note: (row[idx("status_note")] || "").trim(),
      preferred_contact: (row[idx("preferred_contact")] || "").trim(),
      caregiver_name: (row[idx("caregiver_name")] || "").trim(),
      caregiver_phone: (row[idx("caregiver_phone")] || "").trim(),
      pause_until_yyyy_mm_dd: (row[idx("pause_until_yyyy_mm_dd")] || "").trim(),
    });
  }
  return out;
}

async function writeBirthdayRow(rowIndex, patch) {
  if (!BIRTHDAYS_SHEET_ID || !rowIndex || !patch) return;
  const header = await getBirthdayHeader();
  const auth = getJWTAuth();
  const sheets = google.sheets({ version: "v4", auth });

  const data = [];
  for (const [key, value] of Object.entries(patch)) {
    const col = header.indexOf(key);
    if (col === -1) continue; // unknown key
    const a1 = `${colIdxToA1(col)}${rowIndex}`;
    data.push({ range: `Sheet1!${a1}`, values: [[value ?? ""]] });
  }
  if (!data.length) return;
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: BIRTHDAYS_SHEET_ID,
    requestBody: { data, valueInputOption: "RAW" },
  });
}

async function findBirthdayRowByPhoneOrName(phone, name) {
  const rows = await readBirthdaySheet();
  const p = String(phone || "").replace(/\s+/g, "");
  let row = rows.find(r => (r.phone_e164 || "").replace(/\s+/g, "") === p);
  if (!row && name) {
    const n = String(name).toLowerCase().trim();
    row = rows.find(r => String(r.full_name || "").toLowerCase().trim() === n);
  }
  return row || null;
}

// ---- Jamaica 2025 public holidays (simple set; optional to expand) ----
const JM_HOLIDAYS_2025 = new Set([
  "2025-01-01","2025-02-26","2025-04-18","2025-04-21","2025-05-23",
  "2025-08-01","2025-10-20","2025-12-25","2025-12-26"
]);
function isSunday(d) { return dayjs.tz(d, TZ).day() === 0; }
function isHoliday(d) { return JM_HOLIDAYS_2025.has(dayjs.tz(d, TZ).format("YYYY-MM-DD")); }
function isOpenBusinessDay(d) {
  const dj = dayjs.tz(d, TZ);
  return dj.day() !== 0 && !isHoliday(dj);
}
function nextBusinessDay(d) {
  let dj = dayjs.tz(d, TZ).add(1, "day");
  while (!isOpenBusinessDay(dj)) dj = dj.add(1, "day");
  return dj.format("YYYY-MM-DD");
}

// ------------------------ Retell Action ------------------
// Global after-hours guard: booking allowed anytime; transfer/route after-hours => message-taking.
app.post("/retell/action", async (req, res) => {
  try {
    const event  = req.body || {};
    const action = String(event.action || "").toLowerCase();
    const branch = String(event.branch || "winchester").toLowerCase();

    // testing override
    const tokenOk =
      !process.env.STATUS_TOKEN ||
      req.query.token === process.env.STATUS_TOKEN ||
      req.get("x-status-token") === process.env.STATUS_TOKEN;
    const forceOpen =
      tokenOk && (req.query.force_open === "1" || event.force_open === true);

    // After-hours guard for human transfers
    const wantsHuman =
      action.includes("transfer") ||
      action.includes("route") ||
      action.includes("route_human") ||
      action.includes("connect");

    if (wantsHuman && !isOpenNow(branch) && !forceOpen) {
      const nextOpen = nextOpenString(branch);
      return res.json({
        ok: true,
        response: `Thank you for holding. Our ${branch} office is currently closed. I can take your name, number, and a brief message for the team to return your call ${nextOpen}. Would you like me to do that now?`
      });
    }

    // BOOK (allowed anytime)
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

    // HOURS query
    if (action.includes("hours")) {
      const open = isOpenNow(branch);
      if (branch === "portmore") {
        return res.json({
          ok: true,
          response: open
            ? "Yes, our Portmore office is currently open: Monday to Friday 10 AM to 5 PM, and Saturdays 10 AM to 2 PM."
            : `We’re currently closed. Portmore hours are Monday to Friday 10 AM to 5 PM, and Saturdays 10 AM to 2 PM. We’ll reopen ${nextOpenString(branch)}.`
        });
      }
      return res.json({
        ok: true,
        response: open
          ? `Yes, our ${branch} office is open: Monday to Friday 8:30 AM to 4:30 PM.`
          : `We’re currently closed. ${branch} hours are Monday to Friday 8:30 AM to 4:30 PM. We’ll reopen ${nextOpenString(branch)}.`
      });
    }

    // TRANSFER (Retell performs bridge)
    if (action.includes("transfer")) {
      const to = BRANCH_NUMBERS[branch] || BRANCH_NUMBERS.winchester;
      const open = isOpenNow(branch);
      console.log(`[Transfer] action=transfer branch=${branch} to=${to} open=${open} at=${dayjs().tz(TZ).format("YYYY-MM-DD HH:mm:ss")}`);
      return res.json({
        ok: true,
        response: `One moment please while I connect you to our ${branch} branch.`,
        connect: { to },
        transferPolicy: { ringSeconds: Math.ceil((Number(process.env.HANDOFF_TIMEOUT_MS || 25000))/1000) }
      });
    }

    // MESSAGE (email summary to branch inbox)
    if (action.includes("message")) {
      const transporter = makeTransport();
      if (transporter) {
        const b = branch || "winchester";
        const to = BRANCH_EMAILS[b] || BRANCH_EMAILS.winchester;
        const subj = `[PRIORITY] ${b} | ${event.reason || "General"} – ${event.name || "Caller"}`;
        const html = `
          <p><b>NEW MESSAGE – ${b.toUpperCase()}</b></p>
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

    // -------- Birthday maintenance actions --------
    if (action === "update_dob" || action === "dob_update") {
      const phone = event?.patient?.phone || event.phone;
      const name  = event?.patient?.full_name || event.patient_name;
      const newDob = event.dob; // YYYY-MM-DD
      if (!phone || !newDob) return res.json({ ok: false, response: "Missing phone or DOB." });

      const row = await findBirthdayRowByPhoneOrName(phone, name);
      if (!row) return res.json({ ok: false, response: "Patient not found in birthday list." });

      await writeBirthdayRow(row.rowIndex, {
        dob_correction: newDob,
        last_outcome: "corrected_dob",
        last_outcome_ts: dayjs().tz(TZ).toISOString(),
        status_flag: "needs_review",
        status_note: "DOB correction submitted by Kimberley"
      });

      // optional email to branch
      try {
        const t = makeTransport();
        const to = BRANCH_EMAILS[row.branch] || BRANCH_EMAILS.winchester;
        await t?.sendMail({
          from: FROM_EMAIL, to,
          subject: `DOB correction submitted – ${row.full_name}`,
          html: `<p>Kimberley captured a DOB correction.</p>
                 <p><b>Patient:</b> ${row.full_name} (${row.phone_e164})</p>
                 <p><b>New DOB:</b> ${newDob}</p>
                 <p>Please verify and update <b>dob_yyyy_mm_dd</b> in the sheet, then clear <b>dob_correction</b>.</p>`
        });
      } catch {}
      return res.json({ ok: true, response: "Thanks — I’ve sent that update to our team." });
    }

    if (action === "mark_wrong_number") {
      const phone = event?.patient?.phone || event.phone;
      const name  = event?.patient?.full_name || event.patient_name;
      const row = await findBirthdayRowByPhoneOrName(phone, name);
      if (!row) return res.json({ ok: false, response: "Record not found." });

      await writeBirthdayRow(row.rowIndex, {
        opt_out: "yes",
        opt_out_reason: "wrong_number",
        status_flag: "wrong_number",
        status_note: "Marked by Kimberley",
        last_outcome: "wrong_number",
        last_outcome_ts: dayjs().tz(TZ).toISOString()
      });
      return res.json({ ok: true, response: "Noted. We won’t call this number again." });
    }

    if (action === "mark_deceased") {
      const phone = event?.patient?.phone || event.phone;
      const name  = event?.patient?.full_name || event.patient_name;
      const row = await findBirthdayRowByPhoneOrName(phone, name);
      if (!row) return res.json({ ok: false, response: "Record not found." });

      await writeBirthdayRow(row.rowIndex, {
        opt_out: "yes",
        opt_out_reason: "deceased",
        status_flag: "deceased",
        status_note: "Marked by Kimberley",
        last_outcome: "deceased",
        last_outcome_ts: dayjs().tz(TZ).toISOString()
      });
      return res.json({ ok: true, response: "Our sincere condolences. We’ve updated our records." });
    }

    if (action === "opt_out") {
      const phone = event?.patient?.phone || event.phone;
      const name  = event?.patient?.full_name || event.patient_name;
      const reason = event.reason || "request";
      const row = await findBirthdayRowByPhoneOrName(phone, name);
      if (!row) return res.json({ ok: false, response: "Record not found." });

      await writeBirthdayRow(row.rowIndex, {
        opt_out: "yes",
        opt_out_reason: reason,
        last_outcome: "opt_out",
        last_outcome_ts: dayjs().tz(TZ).toISOString()
      });
      return res.json({ ok: true, response: "Understood. We’ll stop calls to this number." });
    }

    if (action === "update_contact") {
      const phone = event?.patient?.phone || event.phone;
      const name  = event?.patient?.full_name || event.patient_name;
      const newPhone = event.new_phone_candidate || event.new_phone;
      const caregiverName  = event.caregiver_name || "";
      const caregiverPhone = event.caregiver_phone || "";
      const row = await findBirthdayRowByPhoneOrName(phone, name);
      if (!row) return res.json({ ok: false, response: "Record not found." });

      await writeBirthdayRow(row.rowIndex, {
        new_phone_candidate: newPhone || "",
        caregiver_name: caregiverName,
        caregiver_phone: caregiverPhone,
        status_flag: "needs_review",
        status_note: "New contact info submitted by Kimberley",
        last_outcome: "contact_update",
        last_outcome_ts: dayjs().tz(TZ).toISOString()
      });
      return res.json({ ok: true, response: "Thank you, I’ve noted the updated contact details." });
    }

    // default
    return res.json({ ok: true, response: "Ready." });
  } catch (err) {
    console.error("[/retell/action] error:", err?.response?.data || err.message);
    res.json({ ok: false, response: "Error." });
  }
});

// ------------------------ Status Dashboard --------------------------
function requireStatusAuth(req, res) {
  const must = process.env.STATUS_TOKEN;
  if (!must) return true;
  const token = req.query.token || req.get("x-status-token");
  if (token === must) return true;
  res.status(401).send("Unauthorized");
  return false;
}
async function checkGoogleCalendar() {
  try { const auth = getJWTAuth(); await auth.getAccessToken(); return { ok: true, note: "Google auth OK" }; }
  catch (e) { return { ok: false, note: e?.message || "Google auth failed" }; }
}
async function checkSMTP() {
  try { const t = makeTransport(); if (!t) return { ok: false, note: "SMTP not configured" }; await t.verify(); return { ok: true, note: "SMTP connection OK" }; }
  catch (e) { return { ok: false, note: e?.message || "SMTP verify failed" }; }
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
    from_email: FROM_EMAIL,
    birthdays_sheet_id: BIRTHDAYS_SHEET_ID ? "set" : "missing"
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
  <p><b>Time:</b> ${dayjs().tz(TZ).format("YYYY-MM-DD HH:mm:ss")}</p>
  <p><b>Uptime:</b> ${Math.round(process.uptime())}s</p>
  <h3>Checks</h3>
  <table>${row("Google Calendar", `${badge(google.ok)} ${google.note}`)}${row("SMTP", `${badge(smtp.ok)} ${smtp.note}`)}</table>
  <h3>Config</h3>
  <table>${row("MOH URL", cfg.moh_url)}${row("Handoff Timeout (ms)", cfg.handoff_timeout_ms)}${row("Google Credentials", cfg.google_creds)}${row("SMTP Host", cfg.smtp_host)}${row("From Email", cfg.from_email)}${row("Birthdays Sheet", cfg.birthdays_sheet_id)}</table>
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
      const timeMin = new Date(Date.now() - 2*24*3600e3).toISOString(); // back 2 days (follow-ups)
      const timeMax = new Date(Date.now() + 8*24*3600e3).toISOString(); // ahead 8 days (7/3/1d)
      const { data } = await calendar.events.list({
        calendarId, timeMin, timeMax, singleEvents: true, orderBy: "startTime"
      });

      let actions = 0;
      for (const ev of (data.items || [])) {
        const startISO = ev.start?.dateTime || ev.start?.date;
        if (!startISO) continue;

        const priv = ev.extendedProperties?.private || {};
        const patientName  = priv.patient_name || null;
        const patientPhone = priv.patient_phone || null;
        const branch       = priv.branch || "winchester";
        if (!patientPhone) continue;

        let changed = false;

        // pre-visit reminders
        for (const win of dueWindowsFor(startISO)) {
          const flag = `reminded_${win}`;
          if (!priv[flag]) {
            await callPatient({
              phone: patientPhone,
              patientName,
              apptTime: startISO,
              branch,
              callType: "reminder"
            });
            priv[flag] = "true";
            actions++; changed = true;
          }
        }

        // post-visit follow-up (~1 day after end)
        const endISO = ev.end?.dateTime || ev.end?.date || startISO;
        const minsSinceEnd = -minutesUntil(endISO); // positive after end
        if (minsSinceEnd >= (24*60 - 15) && minsSinceEnd <= (24*60 + 15) && !priv.followup_1d) {
          await callPatient({
            phone: patientPhone,
            patientName,
            apptTime: startISO,
            branch,
            callType: "followup"
          });
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

// --------------- Scheduled Job: birthday calls (with deferral) ---------------
app.post("/jobs/run-birthdays", async (req, res) => {
  try {
    const must = process.env.STATUS_TOKEN;
    const token = req.query.token || req.get("x-status-token");
    if (must && token !== must) return res.status(401).json({ ok: false, error: "Unauthorized" });

    const today = dayjs().tz(TZ);
    const todayIso = today.format("YYYY-MM-DD");
    const todayMMDD = today.format("MM-DD");
    const thisYear = today.year();

    const rows = await readBirthdaySheet();
    const clean = (s) => String(s || "").trim();

    // Closed day (Sunday/holiday): mark deferrals and exit
    if (!isOpenBusinessDay(today)) {
      const toDefer = rows.filter(r =>
        clean(r.dob_yyyy_mm_dd).slice(5) === todayMMDD &&
        !clean(r.opt_out) &&
        clean(r.last_called_year) !== String(thisYear)
      );
      const carryTo = toDefer.length ? nextBusinessDay(today) : null;
      for (const r of toDefer) {
        await writeBirthdayRow(r.rowIndex, {
          deferred_for_yyyy_mm_dd: carryTo,
          deferred_reason: isSunday(today) ? "sunday" : "holiday",
          last_outcome: "deferred",
          last_outcome_ts: today.toISOString()
        });
      }
      return res.json({ ok: true, closed_today: true, deferred: toDefer.length, carry_to: carryTo });
    }

    // Open day: due = (today's birthdays OR deferred to today), not opted out, not already called this year, not paused
    const due = rows.filter(r => {
      const mmdd = clean(r.dob_yyyy_mm_dd).slice(5);
      const deferredFor = clean(r.deferred_for_yyyy_mm_dd);
      const notCalledThisYear = clean(r.last_called_year) !== String(thisYear);
      const ok = !clean(r.opt_out);
      const paused = !!clean(r.pause_until_yyyy_mm_dd) && dayjs.tz(r.pause_until_yyyy_mm_dd, TZ).isAfter(today, "day");
      return ok && !paused && notCalledThisYear && (mmdd === todayMMDD || deferredFor === todayIso);
    });

    // Respect calling hours inside the open day
    if (!shouldCallNow()) {
      return res.json({ ok: true, note: "Outside calling hours; will try later today.", candidates: due.length });
    }

    let placed = 0;
    for (const r of due) {
      try {
        const resp = await callPatient({
          phone: r.phone_e164,
          patientName: r.full_name,
          branch: r.branch,
          callType: "birthday",
        });
        placed++;
        await writeBirthdayRow(r.rowIndex, {
          last_called_year: String(thisYear),
          last_outcome: "success",
          last_outcome_ts: today.toISOString(),
          deferred_for_yyyy_mm_dd: "",
          deferred_reason: "",
          status_note: ""
        });
      } catch (err) {
        // soft retry next business day
        await writeBirthdayRow(r.rowIndex, {
          last_outcome: "failed",
          last_outcome_ts: today.toISOString(),
          deferred_for_yyyy_mm_dd: nextBusinessDay(today),
          deferred_reason: "retry",
          status_note: "Retry next business day"
        });
      }
    }

    return res.json({ ok: true, birthdays_due: due.length, calls_placed: placed });
  } catch (e) {
    console.error("[/jobs/run-birthdays]", e.response?.data || e.message);
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
