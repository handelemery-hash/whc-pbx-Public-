/**
 * Winchester Heart Centre — PBX Bridge (Retell + Calendar)
 * - Health endpoint
 * - Retell action webhook placeholder
 * - Google Calendar integration (supports B64 or JSON env)
 */

import express from "express";
import cors from "cors";
import { google } from "googleapis";

// -------------------------- App setup ------------------------------
const app = express();
const PORT = process.env.PORT || 8080;

app.use(cors());
app.use(express.json({ limit: "2mb" }));
app.use((req, _res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`);
  next();
});

// ---------------- Physician → Calendar mapping (EDIT NAMES IF NEEDED) ----------
const PHYSICIANS = {
  dr_emery: {
    calendarId:
      "uh7ehq6qg5c1qfdciic3v8l0s8@group.calendar.google.com",
  },
  dr_thompson: {
    calendarId:
      "eburtl0ebphsp3h9qdfurpbqeg@group.calendar.google.com",
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

// -------------------------- Google Calendar Auth ------------------------------
/**
 * We support two ways of supplying credentials:
 * 1) GOOGLE_CREDENTIALS_B64  -> base64 of the entire service-account JSON file
 * 2) GOOGLE_APPLICATION_CREDENTIALS_JSON -> raw JSON string of the same file
 *
 * The first is the recommended (cleanest) option on Railway.
 */
function initGoogleCalendarClient() {
  const credentialsB64 = process.env.GOOGLE_CREDENTIALS_B64;
  const credentialsJson = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;

  if (!credentialsB64 && !credentialsJson) {
    console.warn(
      "[Calendar] No Google credentials found. Set GOOGLE_CREDENTIALS_B64 (preferred) or GOOGLE_APPLICATION_CREDENTIALS_JSON."
    );
    return null;
  }

  try {
    let creds;
    if (credentialsB64) {
      const decoded = Buffer.from(credentialsB64, "base64").toString("utf8");
      creds = JSON.parse(decoded);
      console.log("✅ [Calendar] Loaded credentials from GOOGLE_CREDENTIALS_B64");
    } else {
      creds = JSON.parse(credentialsJson);
      console.log(
        "✅ [Calendar] Loaded credentials from GOOGLE_APPLICATION_CREDENTIALS_JSON"
      );
    }

    const auth = new google.auth.GoogleAuth({
      credentials: creds,
      scopes: ["https://www.googleapis.com/auth/calendar.readonly"],
    });

    const calendar = google.calendar({ version: "v3", auth });
    return calendar;
  } catch (err) {
    console.error("[Calendar] Failed to parse/initialize credentials:", err);
    return null;
  }
}

const calendarClient = initGoogleCalendarClient();

// -------------------------- Calendar helper ------------------------------
/**
 * Returns next upcoming event summary for a physician, or null.
 */
async function getNextEventSummary(physicianKey) {
  if (!calendarClient) return null;

  const entry = PHYSICIANS[physicianKey];
  if (!entry || !entry.calendarId) {
    console.warn(`[Calendar] Unknown physician: ${physicianKey}`);
    return null;
  }

  try {
    const now = new Date().toISOString();
    const resp = await calendarClient.events.list({
      calendarId: entry.calendarId,
      timeMin: now,
      maxResults: 1,
      singleEvents: true,
      orderBy: "startTime",
    });
    const ev = resp?.data?.items?.[0];
    if (!ev) return null;
    return ev.summary || "(no title)";
  } catch (err) {
    console.error("[Calendar] events.list error:", err?.response?.data || err);
    return null;
  }
}

// -------------------------- Health ------------------------------
app.get("/health", (_req, res) => {
  res.status(200).send("ok");
});

// -------------------------- Retell action webhook (example) -------------------
/**
 * This is a minimal example that shows how you might use the calendar helper
 * inside your Retell webhook. Adapt/expand according to your real logic.
 */
app.post("/retell/action", async (req, res) => {
  try {
    const event = req.body;

    // Example usage:
    // Expect "physician" key in the payload (e.g., "dr_williams")
    const physicianKey = event?.physician?.toLowerCase?.();
    let replyText = "How can I help you today?";

    if (physicianKey && PHYSICIANS[physicianKey]) {
      const nextSummary = await getNextEventSummary(physicianKey);
      if (nextSummary) {
        replyText = `Next appointment for ${physicianKey.replace(
          "dr_",
          "Dr "
        )}: ${nextSummary}`;
      } else {
        replyText = `I couldn't find an upcoming appointment for ${physicianKey.replace(
          "dr_",
          "Dr "
        )} right now.`;
      }
    }

    return res.json({
      ok: true,
      response: replyText,
    });
  } catch (e) {
    console.error("retell/action error:", e);
    return res.status(500).json({ ok: false });
  }
});

// -------------------------- Start ------------------------------
app.listen(PORT, () => {
  console.log(`WHC server listening on :${PORT}`);
});
