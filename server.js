// server.js
/**
 * Winchester Heart Centre – PBX/AI Backend with Google Calendar integration
 * - Loads Google service account from env (GOOGLE_CREDENTIALS_B64 or GOOGLE_APPLICATION_CREDENTIALS_JSON)
 * - Physicians → calendarId map (edit below if needed)
 * - Endpoints:
 *    GET  /health
 *    POST /calendar/create
 *    GET  /calendar/upcoming/:physician?max=10
 *    POST /calendar/delete
 *    POST /retell/action   (example workflow)
 */

import express from "express";
import cors from "cors";
import { google } from "googleapis";

// -------------------------- Config Maps ------------------------------
// Physicians → Google Calendar IDs (EDIT THESE IF NEEDED)
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

// Optional “display” names (used only in friendly responses)
const PHYSICIAN_DISPLAY = {
  dr_emery: "Dr Emery",
  dr_thompson: "Dr Thompson",
  dr_dowding: "Dr Dowding",
  dr_blair: "Dr Blair",
  dr_williams: "Dr Williams",
  dr_wright: "Dr Wright",
  dr_dixon: "Dr Dixon",
};

// -------------------------- Google Calendar Helper -------------------
function loadServiceAccountJSON() {
  // Prefer base64 JSON (Railway variable GOOGLE_CREDENTIALS_B64).
  const b64 = process.env.GOOGLE_CREDENTIALS_B64;
  const inline = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;

  if (b64) {
    try {
      const decoded = Buffer.from(b64, "base64").toString("utf8");
      const json = JSON.parse(decoded);
      console.log("✅ [Calendar] Loaded credentials from GOOGLE_CREDENTIALS_B64");
      return json;
    } catch (err) {
      console.error("❌ [Calendar] Failed to decode GOOGLE_CREDENTIALS_B64:", err.message);
      throw err;
    }
  }
  if (inline) {
    try {
      const json = JSON.parse(
        inline.trim().startsWith("{") ? inline : Buffer.from(inline, "base64").toString("utf8")
      );
      console.log("✅ [Calendar] Loaded credentials from GOOGLE_APPLICATION_CREDENTIALS_JSON");
      return json;
    } catch (err) {
      console.error(
        "❌ [Calendar] Failed to parse GOOGLE_APPLICATION_CREDENTIALS_JSON:", err.message
      );
      throw err;
    }
  }
  throw new Error(
    "GOOGLE_CREDENTIALS_B64 or GOOGLE_APPLICATION_CREDENTIALS_JSON must be set."
  );
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

function normalizePhysKey(s) {
  if (!s) return "";
  return String(s).trim().toLowerCase().replace(/\s+/g, "_");
}

function ensureCalendarIdFromPhys(physKey) {
  const key = normalizePhysKey(physKey);
  const id = PHYSICIANS[key];
  if (!id) {
    throw new Error(`Unknown physician key '${physKey}'.`);
  }
  return { key, id };
}

const Calendar = {
  /**
   * Create an event.
   * @param {string} calendarId
   * @param {object} payload { start, end, summary, description, attendees? }
   */
  async createEvent(calendarId, payload) {
    const auth = getJWTAuth();
    const calendar = google.calendar({ version: "v3", auth });

    const event = {
      summary: payload.summary || "Appointment",
      description: payload.description || "",
      start: { dateTime: payload.start }, // Expect ISO string with timezone offset
      end: { dateTime: payload.end },
      attendees: payload.attendees || [],
    };

    const { data } = await calendar.events.insert({
      calendarId,
      requestBody: event,
      conferenceDataVersion: 0,
      supportsAttachments: false,
    });

    return data; // includes id, htmlLink, etc.
  },

  /**
   * List upcoming events.
   * @param {string} calendarId
   * @param {number} maxResults
   */
  async upcoming(calendarId, maxResults = 10) {
    const auth = getJWTAuth();
    const calendar = google.calendar({ version: "v3", auth });

    const { data } = await calendar.events.list({
      calendarId,
      timeMin: new Date().toISOString(),
      maxResults: Math.min(Math.max(+maxResults || 10, 1), 50),
      singleEvents: true,
      orderBy: "startTime",
    });
    return data.items || [];
  },

  /**
   * Delete event by id.
   * @param {string} calendarId
   * @param {string} eventId
   */
  async deleteEvent(calendarId, eventId) {
    const auth = getJWTAuth();
    const calendar = google.calendar({ version: "v3", auth });

    await calendar.events.delete({
      calendarId,
      eventId,
    });
    return true;
  },
};

// -------------------------- Express App ------------------------------
const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));

// Simple request log
app.use((req, _res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`);
  next();
});

// Health check
app.get("/health", (_req, res) => {
  res.status(200).send("ok");
});

// -------------------------- Calendar Routes --------------------------

/**
 * Create event
 * body: {
 *   physician: "dr_emery",
 *   start: "2025-11-09T15:30:00-05:00",
 *   end:   "2025-11-09T16:00:00-05:00",
 *   summary?: "Test",
 *   email?: "patient@example.com",
 *   phone?: "+15551234567",
 *   note?: "notes"
 * }
 */
app.post("/calendar/create", async (req, res) => {
  try {
    const { physician, start, end, summary, email, phone, note } = req.body || {};
    if (!physician || !start || !end) {
      return res.status(400).json({ ok: false, error: "physician, start, end are required" });
    }
    const { key, id } = ensureCalendarIdFromPhys(physician);

    const attendees = [];
    if (email) attendees.push({ email });

    const description = [
      note ? `Note: ${note}` : null,
      phone ? `Phone: ${phone}` : null,
    ]
      .filter(Boolean)
      .join("\n");

    const event = await Calendar.createEvent(id, {
      start,
      end,
      summary: summary || "Consultation",
      description,
      attendees,
    });

    return res.json({
      ok: true,
      eventId: event.id,
      htmlLink: event.htmlLink,
      response: `Created appointment for ${PHYSICIAN_DISPLAY[key] || key}`,
    });
  } catch (err) {
    console.error("Create error:", err?.response?.data || err.message);
    return res.status(500).json({ ok: false, error: err.message || "create_failed" });
  }
});

/**
 * Upcoming events
 * GET /calendar/upcoming/:physician?max=10
 */
app.get("/calendar/upcoming/:physician", async (req, res) => {
  try {
    const { physician } = req.params;
    const { max } = req.query;
    const { key, id } = ensureCalendarIdFromPhys(physician);
    const items = await Calendar.upcoming(id, Number(max || 10));

    const formatted = items.map((ev) => ({
      id: ev.id,
      summary: ev.summary,
      start: ev.start?.dateTime || ev.start?.date,
      end: ev.end?.dateTime || ev.end?.date,
      htmlLink: ev.htmlLink,
    }));
    res.json({
      ok: true,
      physician: PHYSICIAN_DISPLAY[key] || key,
      count: formatted.length,
      events: formatted,
    });
  } catch (err) {
    console.error("Upcoming error:", err?.response?.data || err.message);
    res.status(500).json({ ok: false, error: err.message || "list_failed" });
  }
});

/**
 * Delete event
 * body: { physician: "dr_emery", eventId: "xxxxxxxxxxxxxx" }
 */
app.post("/calendar/delete", async (req, res) => {
  try {
    const { physician, eventId } = req.body || {};
    if (!physician || !eventId) {
      return res.status(400).json({ ok: false, error: "physician and eventId are required" });
    }
    const { key, id } = ensureCalendarIdFromPhys(physician);
    await Calendar.deleteEvent(id, eventId);
    res.json({
      ok: true,
      response: `Deleted event for ${PHYSICIAN_DISPLAY[key] || key}`,
    });
  } catch (err) {
    console.error("Delete error:", err?.response?.data || err.message);
    res.status(500).json({ ok: false, error: err.message || "delete_failed" });
  }
});

// -------------------------- Retell webhook (example) -----------------
/**
 * This example shows how you could call calendar helpers from Retell.
 * Feel free to adapt your action payload.
 *
 * - Book appointment:
 *    body: {
 *      action: "book_appointment",
 *      physician: "dr_emery",
 *      start: "2025-11-09T15:30:00-05:00",
 *      end:   "2025-11-09T16:00:00-05:00",
 *      summary?: "Consult",
 *      email?: "patient@x.com",
 *      phone?: "+1…",
 *      note?: "…"
 *    }
 *
 * - Check next:
 *    body: { action: "check_next", physician: "dr_emery" }
 */
app.post("/retell/action", async (req, res) => {
  try {
    const event = req.body || {};
    const action = String(event.action || "").toLowerCase();

    if (action === "book_appointment") {
      const { physician, start, end, summary, email, phone, note } = event;
      if (!physician || !start || !end) {
        return res.json({ ok: false, response: "physician, start and end are required" });
      }
      const { key, id } = ensureCalendarIdFromPhys(physician);
      const attendees = [];
      if (email) attendees.push({ email });

      const description = [
        note ? `Note: ${note}` : null,
        phone ? `Phone: ${phone}` : null,
      ]
        .filter(Boolean)
        .join("\n");

      const created = await Calendar.createEvent(id, {
        start,
        end,
        summary: summary || "Consultation",
        description,
        attendees,
      });

      const reply = `Booked appointment for ${
        PHYSICIAN_DISPLAY[key] || key
      }.`;
      return res.json({ ok: true, response: reply, eventId: created.id });
    }

    if (action === "check_next") {
      const { physician } = event;
      if (!physician) return res.json({ ok: false, response: "physician required" });
      const { key, id } = ensureCalendarIdFromPhys(physician);
      const items = await Calendar.upcoming(id, 1);
      if (!items.length) {
        return res.json({
          ok: true,
          response: `No upcoming events for ${PHYSICIAN_DISPLAY[key] || key}.`,
        });
      }
      const first = items[0];
      const start = first.start?.dateTime || first.start?.date;
      const reply = `Next appointment for ${
        PHYSICIAN_DISPLAY[key] || key
      }: ${first.summary || "Consult"}. Starts ${start}.`;
      return res.json({ ok: true, response: reply, eventId: first.id });
    }

    // Fallback: simple reply
    return res.json({
      ok: true,
      response: "Action received.",
    });
  } catch (err) {
    console.error("Retell action error:", err?.response?.data || err.message);
    res.json({ ok: false, response: "An error occurred." });
  }
});

// -------------------------- Start server ----------------------------
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`WHC server listening on :${PORT}`);
});
