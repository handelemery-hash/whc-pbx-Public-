// ----------------------------
// Winchester Heart Centre Server
// ----------------------------

import express from "express";
import cors from "cors";
import bodyParser from "body-parser";
import { google } from "googleapis";

// ----------------------------
// Configuration Maps
// ----------------------------

// Physician calendars
const PHYSICIANS = {
  dr_emery: {
    calendarId: "uh7ehq6qg5c1qfdciic3v8l0s8@group.calendar.google.com",
  },
  dr_thompson: {
    calendarId: "eburtl0ebphsp3h9qdfurpbqeg@group.calendar.google.com",
  },
  dr_dowding: {
    calendarId:
      "a70ab6c4e673f04f6d40fabdb0f4861cf2fac5874677d5dd9961e357b8bb8af9@group.calendar.google.com",
  },
  dr_blair: {
    calendarId:
      "ad21642079da12151a39c9a5aa455d56c306cfeabdfd712fb34a4378c3f04c4a@group.calendar.google.com",
  },
  dr_williams: {
    calendarId:
      "7343219d0e34a585444e2a39fd1d9daa650e082209a9e5dc85e0ce73d63c7393@group.calendar.google.com",
  },
  dr_wright: {
    calendarId:
      "b8a27f6d34e63806408f975bf729a3089b0d475b1b58c18ae903bc8bc63aa0ea@group.calendar.google.com",
  },
  dr_dixon: {
    calendarId:
      "ed382c812be7a6d3396a874ca19368f2d321805f80526e6f3224f713f0637cee@group.calendar.google.com",
  },
};

// ----------------------------
// Initialize App
// ----------------------------
const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: "1mb" }));

// Log every incoming request body for debugging
app.use((req, _res, next) => {
  try {
    console.log(
      `[Webhook] ${req.method} ${req.originalUrl} ct=${req.headers["content-type"]}`,
      "body=",
      JSON.stringify(req.body)
    );
  } catch (err) {
    console.error("[Logger] Failed to log body:", err);
  }
  next();
});

// ----------------------------
// Google Calendar Helper
// ----------------------------
let calendarClient = null;

async function initGoogleClient() {
  try {
    const b64 = process.env.GOOGLE_CREDENTIALS_B64 || process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;
    if (!b64) throw new Error("Missing Google credentials environment variable.");
    const jsonCreds = JSON.parse(Buffer.from(b64, "base64").toString("utf8"));

    const auth = new google.auth.GoogleAuth({
      credentials: jsonCreds,
      scopes: ["https://www.googleapis.com/auth/calendar"],
    });
    calendarClient = google.calendar({ version: "v3", auth });
    console.log("✅ [Calendar] Google API initialized");
  } catch (err) {
    console.error("[Calendar] Failed to initialize:", err);
  }
}

await initGoogleClient();

// ----------------------------
// Calendar Service
// ----------------------------
const Calendar = {
  async createEvent({ physician, start, end, summary, email, phone, note }) {
    if (!calendarClient) throw new Error("Google Calendar client not initialized");

    const phys = PHYSICIANS[physician];
    if (!phys) throw new Error(`Unknown physician '${physician}'`);

    const calendarId = phys.calendarId;

    const event = {
      summary: summary || "New Appointment",
      description: `Email: ${email || "N/A"}\nPhone: ${phone || "N/A"}\nNote: ${
        note || ""
      }`,
      start: { dateTime: start },
      end: { dateTime: end },
    };

    const res = await calendarClient.events.insert({
      calendarId,
      resource: event,
    });

    console.log(`✅ [Calendar] Created event for ${physician}`, res.data.id);
    return res.data;
  },
};

// ----------------------------
// Routes
// ----------------------------

// Health Check
app.get("/health", (_req, res) => {
  res.status(200).send("ok");
});

// ----------------------------
// Debug Calendar Write Route
// ----------------------------
app.post("/calendar/debug/create", async (req, res) => {
  try {
    const { physician, start, end, summary, email, phone, note } = req.body || {};
    if (!physician || !start || !end) {
      return res
        .status(400)
        .json({ ok: false, error: "physician, start, end are required" });
    }

    const ev = await Calendar.createEvent({
      physician,
      start,
      end,
      summary,
      email,
      phone,
      note,
    });

    res.json({ ok: true, eventId: ev.id });
  } catch (e) {
    console.error("[debug/create] error:", e);
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// ----------------------------
// Retell Action Webhook Route
// ----------------------------
app.post("/retell/action", async (req, res) => {
  try {
    const event = req.body;
    console.log("[Webhook] Payload:", event);

    // Handle appointment booking
    if (
      event.action &&
      event.action.toLowerCase().includes("book") &&
      event.physician
    ) {
      const result = await Calendar.createEvent({
        physician: event.physician,
        start: event.start,
        end: event.end,
        summary: event.summary,
        email: event.email,
        phone: event.phone,
        note: event.note,
      });
      return res.json({
        ok: true,
        createdEventId: result.id,
        response: `Appointment booked for ${event.physician}`,
      });
    }

    // Otherwise respond with a generic status
    return res.json({
      ok: true,
      response: `Next appointment for ${event.physician || "unknown"}: Consults– Ardenne`,
    });
  } catch (err) {
    console.error("[retell/action] error:", err);
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// ----------------------------
// Start Server
// ----------------------------
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`🚀 WHC server listening on :${PORT}`);
});
