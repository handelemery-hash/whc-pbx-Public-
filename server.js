/**
 * Winchester Heart Centre – AI PBX Server
 * Handles Retell AI webhooks, Telnyx call control, email summaries,
 * and call routing to Winchester / Portmore / Ardenne / Sav branches.
 */

import express from "express";
import bodyParser from "body-parser";
import axios from "axios";
import nodemailer from "nodemailer";

const app = express();
const PORT = process.env.PORT || 8080;

// ──────────────────────────────────────────────
// Middleware
// ──────────────────────────────────────────────
app.use(bodyParser.json({ limit: "5mb" }));

// Log every incoming request for visibility
app.use((req, res, next) => {
  console.log(
    `[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`
  );
  next();
});

// Simple health endpoints for Railway / debugging
app.get("/", (req, res) => res.status(200).send("root ok"));
app.get("/health", (req, res) => res.status(200).send("ok"));

// ──────────────────────────────────────────────
// Configuration
// ──────────────────────────────────────────────
const BRANCHES = {
  WINCHESTER: {
    numbers: ["+18766488257", "+18769082658", "+18763529677"],
    email: process.env.EMAIL_WINCHESTER,
    handoff: "+18769082658",
  },
  PORTMORE: {
    numbers: ["+18766710478", "+18767042739", "+18763527650"],
    email: process.env.EMAIL_PORTMORE,
    handoff: ["+18767042739", "+18766710478"],
  },
  ARDENNE: {
    numbers: ["+18766713825", "+18763531170"],
    email: process.env.EMAIL_ARDENNE,
    handoff: "+18766713825",
  },
  SAV: {
    numbers: ["+18769540252", "+18762987513"],
    email: process.env.EMAIL_SAV,
    handoff: "+18769540252",
  },
};

const MOH_URL =
  process.env.MOH_URL ||
  "https://cdn.winchesterheartcentre.com/hold.mp3";

// ──────────────────────────────────────────────
// Email transport (for voicemail + summaries)
// ──────────────────────────────────────────────
const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST,
  port: process.env.SMTP_PORT,
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS,
  },
});

// ──────────────────────────────────────────────
// Utility: send email summary / voicemail alert
// ──────────────────────────────────────────────
async function sendEmail(to, subject, text) {
  try {
    await transporter.sendMail({
      from: process.env.FROM_EMAIL,
      to,
      subject,
      text,
    });
    console.log(`📧 Email sent to ${to}`);
  } catch (err) {
    console.error("❌ Email send failed:", err.message);
  }
}

// ──────────────────────────────────────────────
// Retell AI Webhook Handler
// ──────────────────────────────────────────────
app.post("/retell/action", async (req, res) => {
  // Always respond quickly so Retell doesn’t retry
  res.status(200).json({ ok: true });

  console.log("🎧 Received Retell webhook:");
  console.log(JSON.stringify(req.body, null, 2));

  try {
    const { event, call, data } = req.body;
    console.log(`➡️ Event: ${event || "unknown"}`);

    // Example: when Retell notifies of a new incoming call
    if (event === "call.initiated") {
      const from = call?.from || "unknown";
      const to = call?.to || "unknown";

      // Determine which branch based on “to” number
      let branchKey = "WINCHESTER"; // default
      for (const [key, b] of Object.entries(BRANCHES)) {
        if (b.numbers.includes(to)) branchKey = key;
      }
      console.log(`📞 Incoming call for branch: ${branchKey}`);

      const branch = BRANCHES[branchKey];
      const summary = `New call from ${from} to ${to} → ${branchKey}`;

      // Send summary email
      await sendEmail(branch.email, `[${branchKey}] New Call`, summary);

      // TODO: if using Telnyx, initiate handoff here
      // await axios.post('https://api.telnyx.com/v2/calls', { ... });
    }

    // Example: when voicemail or message left
    if (event === "voicemail.received") {
      const branch = BRANCHES.WINCHESTER;
      const audioUrl = data?.recording_url || "(no audio)";
      await sendEmail(
        branch.email,
        `[VOICEMAIL] New message`,
        `Voicemail received: ${audioUrl}`
      );
    }

    // Log any other events for visibility
    if (!["call.initiated", "voicemail.received"].includes(event)) {
      console.log("ℹ️ Unhandled event type:", event);
    }
  } catch (err) {
    console.error("❌ Webhook handler error:", err);
  }
});

// ──────────────────────────────────────────────
// Start Server
// ──────────────────────────────────────────────
app.listen(PORT, () =>
  console.log(`🚀 WHC PBX server listening on port ${PORT}`)
);
