/**
 * WHC PBX + Calendar + Handoff (Retell-first, Telnyx-optional)
 * -------------------------------------------------------------
 * - Health + logging
 * - Google Calendar free-slot lookup and booking
 * - Optional SMTP email
 * - Retell Action Webhook:
 *     • "handoff_branch": transfer caller to a configured branch number
 *       - If TELNYX_* env exists, it will try Telnyx Call Control
 *       - Otherwise, it returns a "transfer instruction" for Retell to do the PSTN transfer
 *
 * Env (required for calendar):
 *   PORT=8080
 *   GOOGLE_APPLICATION_CREDENTIALS_JSON=<your service-account JSON as one secret>
 *
 * Optional email:
 *   SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS
 *   MAIL_FROM="WHC Reception <reception@yourdomain.com>"
 *   MAIL_TO=appointments@yourdomain.com
 *
 * Optional Telnyx (for in-server initiated handoff; not needed if Retell transfers directly):
 *   TELNYX_API_KEY=
 *   TELNYX_CONNECTION_ID=
 *   TELNYX_OUTBOUND_CALLER_ID=+13056769686
 *
 * Timezone: America/Jamaica
 */

import express from "express";
import fs from "fs";
import path from "path";
import axios from "axios";
import nodemailer from "nodemailer";
import { google } from "googleapis";

// --------------------------- Bootstrapping ---------------------------
const app = express();
const PORT = process.env.PORT || 8080;
const JAMAICA_TZ = "America/Jamaica";

// Persist Google creds from Railway secret to a temp file
try {
  const credsPath = path.join("/tmp", "google-creds.json");
  if (!process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON) {
    console.warn("[Calendar] GOOGLE_APPLICATION_CREDENTIALS_JSON not set.");
  } else {
    fs.writeFileSync(
      credsPath,
      process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON,
      "utf8"
    );
    process.env.GOOGLE_APPLICATION_CREDENTIALS = credsPath;
  }
} catch (e) {
  console.error("Failed to write Google creds JSON:", e);
}

app.use(express.json({ limit: "2mb" }));
app.use((req, _res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`);
  next();
});

// -------------------------- Config Maps ------------------------------
// 1) Physicians → Google Calendar IDs (EDIT THESE)
const PHYSICIANS = {
  // Example:
  // 'dr_williams': { calendarId: 'williams@yourdomain.com' },
  // 'dr_reid':     { calendarId: 'reid@yourdomain.com' },
};

// 2) Branch phone numbers in E.164 (EDIT THESE)
// Add all branches you want to handoff to
const BRANCH_NUMBERS = {
  // Examples:
  // kingston:  '+18766488257',
  // portmore:  '+18767042739', // primary
  // portmore_alt: '+18766710478', // backup
  // ardenne:  '+18769082658',
  // sav:      '+18763529677',
};

// Business hours (Mon–Fri 09:00–17:00)
const WORK_START = { hour: 9, minute: 0 };
const WORK_END = { hour: 17, minute: 0 };

// --------------------------- Optional Email ---------------------------
let transporter = null;
if (
  process.env.SMTP_HOST &&
  process.env.SMTP_PORT &&
  process.env.SMTP_USER &&
  process.env.SMTP_PASS
) {
  transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: Number(process.env.SMTP_PORT),
    secure: Number(process.env.SMTP_PORT) === 465,
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
  });

  transporter.verify().then(
    () => console.log("[SMTP] Transport verified."),
    (err) => console.warn("[SMTP] Transport verify failed:", err?.message)
  );
}

async function sendMail({ subject, text }) {
  if (!transporter) return;
  const from = process.env.MAIL_FROM || "whc-bot@localhost";
  const to = process.env.MAIL_TO || "";
  if (!to) return;

  try {
    await transporter.sendMail({ from, to, subject, text });
    console.log("[SMTP] Sent:", subject);
  } catch (e) {
    console.warn("[SMTP] Send failed:", e?.message);
  }
}

// -------------------------- Google Calendar --------------------------
const SCOPES = ["https://www.googleapis.com/auth/calendar"];
const auth = new google.auth.GoogleAuth({ scopes: SCOPES });
const calendar = google.calendar({ version: "v3", auth });

function addMinutes(dt, mins) {
  const d = new Date(dt);
  d.setMinutes(d.getMinutes() + mins);
  return d;
}
function isWeekend(d) {
  const day = d.getDay();
  return day === 0 || day === 6;
}
function dayWorkWindow(date) {
  const start = new Date(date);
  start.setHours(WORK_START.hour, WORK_START.minute, 0, 0);
  const end = new Date(date);
  end.setHours(WORK_END.hour, WORK_END.minute, 0, 0);
  return { start, end };
}

async function getBusyBlocks(calendarId, timeMin, timeMax) {
  const res = await calendar.freebusy.query({
    requestBody: {
      timeMin: timeMin.toISOString(),
      timeMax: timeMax.toISOString(),
      timeZone: JAMAICA_TZ,
      items: [{ id: calendarId }],
    },
  });
  const cal = res.data.calendars?.[calendarId];
  return cal?.busy ?? [];
}

async function getFreeSlotsForDay(calendarId, date, durationMins, maxSlots = 10) {
  if (isWeekend(date)) return [];
  const { start, end } = dayWorkWindow(date);
  const busy = await getBusyBlocks(calendarId, start, end);

  const results = [];
  let cursor = new Date(start);

  while (cursor < end && results.length < maxSlots) {
    const slotStart = new Date(cursor);
    const slotEnd = addMinutes(slotStart, durationMins);
    if (slotEnd > end) break;

    const overlapsBusy = busy.some((b) => {
      const bStart = new Date(b.start);
      const bEnd = new Date(b.end);
      return slotStart < bEnd && slotEnd > bStart;
    });

    if (!overlapsBusy) results.push(slotStart);
    cursor = addMinutes(cursor, durationMins);
  }
  return results;
}

// GET /calendar/:phys/free?days=5&duration=15
app.get("/calendar/:phys/free", async (req, res) => {
  try {
    const phys = req.params.phys;
    const days = Math.min(parseInt(req.query.days ?? "5", 10), 14);
    const duration = Math.max(5, parseInt(req.query.duration ?? "15", 10));

    const doc = PHYSICIANS[phys];
    if (!doc?.calendarId) {
      return res
        .status(400)
        .json({ error: "Unknown physician. Add calendarId in PHYSICIANS." });
    }
    if (!process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON) {
      return res.status(500).json({
        error:
          "GOOGLE_APPLICATION_CREDENTIALS_JSON is missing. Set it in Railway.",
      });
    }

    const today = new Date();
    const all = [];
    for (let i = 0; i < days; i++) {
      const date = new Date(today);
      date.setDate(today.getDate() + i);
      const slots = await getFreeSlotsForDay(doc.calendarId, date, duration, 10);
      all.push({
        date: date.toISOString().slice(0, 10),
        slots: slots.map((s) => s.toISOString()),
      });
    }

    res.json({
      physician: phys,
      duration_minutes: duration,
      timezone: JAMAICA_TZ,
      days: all,
    });
  } catch (err) {
    console.error("GET /calendar/:phys/free error:", err);
    res.status(500).json({ error: "Failed to fetch free slots." });
  }
});

// POST /calendar/:phys/book
// Body: { start: ISOString, duration: minutes, patientName, caller, notes }
app.post("/calendar/:phys/book", async (req, res) => {
  try {
    const phys = req.params.phys;
    const doc = PHYSICIANS[phys];
    if (!doc?.calendarId) {
      return res
        .status(400)
        .json({ error: "Unknown physician. Add calendarId in PHYSICIANS." });
    }
    const {
      start,
      duration = 15,
      patientName = "Patient",
      caller = "",
      notes = "",
    } = req.body || {};
    if (!start) return res.status(400).json({ error: "Missing 'start' ISO datetime" });
    if (!process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON) {
      return res.status(500).json({
        error:
          "GOOGLE_APPLICATION_CREDENTIALS_JSON is missing. Set it in Railway.",
      });
    }

    const startDt = new Date(start);
    const endDt = addMinutes(startDt, duration);

    const event = {
      summary: `WHC Phone booking – ${patientName}`,
      description: `${notes}${caller ? `\nCaller: ${caller}` : ""}`,
      start: { dateTime: startDt.toISOString(), timeZone: JAMAICA_TZ },
      end: { dateTime: endDt.toISOString(), timeZone: JAMAICA_TZ },
    };

    const created = await calendar.events.insert({
      calendarId: doc.calendarId,
      requestBody: event,
      conferenceDataVersion: 0,
      sendUpdates: "all",
    });

    await sendMail({
      subject: `New booking for ${phys} – ${patientName}`,
      text: `Booking confirmed for ${patientName}
Physician: ${phys}
Start: ${startDt.toISOString()}
Duration: ${duration} minutes
Caller: ${caller}
Notes: ${notes}
Event: ${created.data.htmlLink || ""}`,
    });

    res.json({
      physician: phys,
      status: "booked",
      eventId: created.data.id,
      htmlLink: created.data.htmlLink,
      start: startDt.toISOString(),
      end: endDt.toISOString(),
      timezone: JAMAICA_TZ,
    });
  } catch (err) {
    console.error("POST /calendar/:phys/book error:", err);
    res.status(500).json({ error: "Failed to create event." });
  }
});

// ----------------------- (Optional) Telnyx Handoff --------------------
// Only used if TELNYX_* env are present. We keep it simple: we trigger an
// outbound PSTN leg to the branch number. In real bridging scenarios, you'd
// bridge with your inbound leg using Call Control events.
async function handoffViaTelnyx(toNumber, callerId) {
  const apiKey = process.env.TELNYX_API_KEY;
  const connectionId = process.env.TELNYX_CONNECTION_ID;
  const fromNumber =
    callerId || process.env.TELNYX_OUTBOUND_CALLER_ID || "+10000000000";

  if (!apiKey || !connectionId) {
    throw new Error("Telnyx not configured.");
  }

  const url = "https://api.telnyx.com/v2/call_control/calls";
  const headers = {
    Authorization: `Bearer ${apiKey}`,
    "Content-Type": "application/json",
  };

  // This initiates an outbound leg. Without an inbound leg to bridge,
  // the transfer is effectively "call the branch directly". If you want
  // a live bridge, you’ll need to pass/track the Retell inbound leg id
  // and use Call Control (answer, bridge, etc).
  const body = {
    connection_id: connectionId,
    to: toNumber,
    from: fromNumber,
  };

  const resp = await axios.post(url, body, { headers, timeout: 8000 });
  return resp.data;
}

// ------------------------ Retell Action Webhook -----------------------
// Retell can call this endpoint with an "action_type" and payload.
// We recognize 'handoff_branch' and return a transfer directive
// (or use Telnyx if configured).
//
// NOTE: Different Retell plans/sdks send slightly different schemas.
// If your agent expects a specific response shape, adapt the response
// object at the two places marked: (A) & (B).
app.post("/retell/action", async (req, res) => {
  try {
    console.log("[Retell Action]", JSON.stringify(req.body));

    const action = req.body?.action_type || req.body?.action || "";
    const payload = req.body?.payload || req.body || {};
    const caller = payload?.from_number || req.body?.from_number || "";

    if (action === "handoff_branch") {
      const key = (payload?.branch || "").toLowerCase();
      const toNumber = BRANCH_NUMBERS[key];

      if (!toNumber) {
        return res.status(400).json({
          error: `Unknown branch '${payload?.branch}'. Configure BRANCH_NUMBERS.`,
        });
      }

      // If Telnyx configured -> try outbound via Telnyx
      if (
        process.env.TELNYX_API_KEY &&
        process.env.TELNYX_CONNECTION_ID &&
        process.env.TELNYX_OUTBOUND_CALLER_ID
      ) {
        try {
          const telnyxResp = await handoffViaTelnyx(toNumber, caller);
          console.log("[Telnyx] Outbound call created:", telnyxResp?.data?.id);

          // (A) Response back to Retell when we dialed via Telnyx ourselves:
          return res.json({
            ok: true,
            method: "telnyx",
            telnyx_call_id: telnyxResp?.data?.id || null,
            message: `Dialed ${toNumber} via Telnyx`,
          });
        } catch (e) {
          console.warn("[Telnyx] Handoff failed, falling back to transfer:", e?.message);
          // fall through to Retell transfer
        }
      }

      // Otherwise: ask Retell to transfer the call
      // (B) Adjust this shape to the schema your Retell agent expects.
      return res.json({
        ok: true,
        action: "transfer",
        phone_number: toNumber,
        message: `Transfer caller to ${key} (${toNumber})`,
      });
    }

    // Default no-op
    return res.json({ ok: true, note: "no action taken" });
  } catch (e) {
    console.error("/retell/action error", e);
    res.status(500).json({ error: "action failed" });
  }
});

// Optional summary hook
app.post("/retell/summary", async (req, res) => {
  try {
    console.log("[Retell Summary]", JSON.stringify(req.body));
    const caller = req.body?.from_number || "";
    const text =
      typeof req.body?.summary === "string"
        ? req.body.summary
        : JSON.stringify(req.body, null, 2);
    await sendMail({ subject: `Call summary ${caller}`, text });
    res.json({ ok: true });
  } catch (e) {
    console.error("/retell/summary error", e);
    res.status(500).json({ error: "summary failed" });
  }
});

// -------------------------- Basic Routes -----------------------------
app.get("/", (_req, res) => {
  res.type("text/plain").send("WHC PBX + Calendar + Handoff is running.");
});
app.get("/health", (_req, res) => {
  res.type("text/plain").send("ok");
});

// -------------------------- Error Handler ----------------------------
app.use((err, _req, res, _next) => {
  console.error("Unhandled error:", err);
  res.status(500).json({ error: "internal server error" });
});

// -------------------------- Start Server -----------------------------
app.listen(PORT, () => {
  console.log(`WHC server listening on :${PORT}`);
});

