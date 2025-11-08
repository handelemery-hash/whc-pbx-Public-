/**
 * Winchester Heart Centre – AI PBX Bridge
 * - Receives Retell AI webhooks
 * - Routes by branch (Winchester / Portmore / Ardenne / Sav)
 * - Sends email summaries / voicemail alerts
 * - Ready for Telnyx call-control integration
 */

import express from "express";
import nodemailer from "nodemailer";
// Optional for future Telnyx actions:
// import axios from "axios";

const app = express();
const PORT = process.env.PORT || 8080;

// ──────────────────────────────────────────────
// Middleware
// ──────────────────────────────────────────────
app.use(express.json({ limit: "2mb" }));

// Log every incoming request (shows in Railway logs)
app.use((req, res, next) => {
  console.log(
    `[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`
  );
  next();
});

// ──────────────────────────────────────────────
/** Health & Root (for testing) */
// ──────────────────────────────────────────────
app.get("/health", (req, res) => res.status(200).send("ok"));
app.get("/", (req, res) => res.status(200).send("root ok"));

// ──────────────────────────────────────────────
// Configuration
// ──────────────────────────────────────────────
/** Music On Hold (used when we later add transfers) */
const MOH_URL =
  process.env.MOH_URL ||
  "https://cdn.winchesterheartcentre.com/hold.mp3";

/** Branch directory (numbers used to identify which branch was called) */
const BRANCHES = {
  WINCHESTER: {
    numbers: ["+18766488257", "+18769082658", "+18763529677"],
    email: process.env.EMAIL_WINCHESTER,
    handoff: "+18769082658",
  },
  PORTMORE: {
    numbers: ["+18766710478", "+18767042739", "+18763527650"],
    email: process.env.EMAIL_PORTMORE,
    handoff: ["+18767042739", "+18766710478"], // primary, fallback
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

// ──────────────────────────────────────────────
// Email transport (for summaries / voicemail alerts)
// ──────────────────────────────────────────────
let transporter = null;
if (process.env.SMTP_HOST && process.env.SMTP_PORT && process.env.SMTP_USER) {
  transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: Number(process.env.SMTP_PORT) || 587,
    secure: false,
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
  });
} else {
  console.warn(
    "⚠️  SMTP not fully configured (no SMTP_HOST/SMTP_USER). Email sending will be skipped."
  );
}

async function sendEmail(to, subject, text) {
  if (!transporter || !to) {
    console.warn("ℹ️  Skipping email: transporter or recipient missing.");
    return;
  }
  try {
    await transporter.sendMail({
      from: process.env.FROM_EMAIL || "no-reply@winchesterheartcentre.com",
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
// Helpers
// ──────────────────────────────────────────────
function detectBranchByToNumber(to) {
  if (!to) return "WINCHESTER"; // default
  for (const [key, b] of Object.entries(BRANCHES)) {
    if (b.numbers.includes(to)) return key;
  }
  return "WINCHESTER";
}

// ──────────────────────────────────────────────
// Retell Webhook
// ──────────────────────────────────────────────
/**
 * NOTE: Retell should POST to:
 * https://<your-domain>.railway.app/retell/action
 */
app.post("/retell/action", async (req, res) => {
  // Acknowledge immediately so Retell doesn’t retry
  res.status(200).json({ ok: true });

  try {
    console.log("🎧 Received Retell webhook:");
    console.log(JSON.stringify(req.body, null, 2));

    const { event, call, data } = req.body || {};
    const from = call?.from || "unknown";
    const to = call?.to || "unknown";

    // Identify branch
    const branchKey = detectBranchByToNumber(to);
    const branch = BRANCHES[branchKey];

    if (event === "call.initiated") {
      const summary = `New call from ${from} to ${to} → ${branchKey}`;
      console.log("📞", summary);
      await sendEmail(
        branch.email,
        `[${branchKey}] New Call`,
        `${summary}\nMOH: ${MOH_URL}`
      );

      // TODO: when ready, place Telnyx transfer here:
      // await axios.post('https://api.telnyx.com/v2/calls', { ... }, { headers: { Authorization: `Bearer ${process.env.TELNYX_API_KEY}` }});
    }

    if (event === "voicemail.received") {
      const audio = data?.recording_url || "(no recording URL)";
      const msg = `Voicemail for ${branchKey} from ${from}\nAudio: ${audio}`;
      console.log("📨", msg);
      await sendEmail(branch.email, `[VOICEMAIL] ${branchKey}`, msg);
    }

    // Log everything else for now
    if (!["call.initiated", "voicemail.received"].includes(event)) {
      console.log("ℹ️ Unhandled event:", event);
    }
  } catch (err) {
    console.error("❌ Webhook error:", err);
  }
});

// ──────────────────────────────────────────────
// Start server
// ──────────────────────────────────────────────
const server = app.listen(PORT, () =>
  console.log(`🚀 WHC PBX server listening on port ${PORT}`)
);

// ──────────────────────────────────────────────
/** Keep-alive: ping the local server every 4 minutes
 *  (prevents the free tier from idling too aggressively)
 */
const KEEPALIVE_MS = 240000; // 4 minutes
setInterval(() => {
  // Use localhost inside the same container
  fetch(`http://127.0.0.1:${PORT}/health`)
    .then((r) => console.log("🔄 Keep-alive ping:", r.status))
    .catch((e) => console.error("Keep-alive error:", e.message));
}, KEEPALIVE_MS);

// ──────────────────────────────────────────────
/** Graceful shutdown */
function shutdown(signal) {
  console.log(`↩️  Received ${signal}. Closing server...`);
  server.close(() => {
    console.log("✅ HTTP server closed.");
    process.exit(0);
  });
  // Force exit if not closed in 5s
  setTimeout(() => {
    console.warn("⏱️  Force exiting after 5s.");
    process.exit(0);
  }, 5000).unref();
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
