/**
 * Winchester Heart Centre – AI PBX Bridge with Telnyx handoff
 * - Receives Retell AI webhooks
 * - Sends branch emails
 * - Handoff to human via Telnyx (transfer live leg or outbound fallback)
 */

import express from "express";
import nodemailer from "nodemailer";
import axios from "axios";

const app = express();
const PORT = process.env.PORT || 8080;

// ──────────────────────────────────────────────
// Middleware & basic routes
// ──────────────────────────────────────────────
app.use(express.json({ limit: "2mb" }));
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`);
  next();
});
app.get("/health", (req, res) => res.status(200).send("ok"));
app.get("/", (req, res) => res.status(200).send("root ok"));

// ──────────────────────────────────────────────
// Config
// ──────────────────────────────────────────────
const MOH_URL =
  process.env.MOH_URL || "https://cdn.winchesterheartcentre.com/hold.mp3";

const BRANCHES = {
  WINCHESTER: {
    numbers: ["+18766488257", "+18769082658", "+18763529677"],
    email: process.env.EMAIL_WINCHESTER,
    handoff: "+18769082658",
  },
  PORTMORE: {
    numbers: ["+18766710478", "+18767042739", "+18763527650"],
    email: process.env.EMAIL_PORTMORE,
    handoffPrimary: "+18767042739",
    handoffBackup: "+18766710478",
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

const HANDOFF_TIMEOUT_MS = Number(process.env.HANDOFF_TIMEOUT_MS || 25000);

// ──────────────────────────────────────────────
// Email transport
// ──────────────────────────────────────────────
let transporter = null;
if (process.env.SMTP_HOST && process.env.SMTP_USER) {
  transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: Number(process.env.SMTP_PORT || 587),
    secure: false,
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
  });
} else {
  console.warn("⚠️  SMTP not fully configured; email will be skipped.");
}

async function sendEmail(to, subject, text) {
  if (!transporter || !to) {
    console.warn("ℹ️  Skipping email (no transporter or recipient).");
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
  } catch (e) {
    console.error("❌ Email send failed:", e.message);
  }
}

// ──────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────
function detectBranchByToNumber(to) {
  if (!to) return "WINCHESTER";
  for (const [key, b] of Object.entries(BRANCHES)) {
    if (b.numbers.includes(to)) return key;
  }
  return "WINCHESTER";
}

// Telnyx client
const telnyx = axios.create({
  baseURL: "https://api.telnyx.com/v2",
  timeout: 15000,
  headers: {
    Authorization: `Bearer ${process.env.TELNYX_API_KEY || ""}`,
    "Content-Type": "application/json",
  },
});

function telnyxReady() {
  return !!(process.env.TELNYX_API_KEY && process.env.TELNYX_CONNECTION_ID);
}

/**
 * Transfer an existing Telnyx call-control leg to a destination number.
 * (Works when webhook includes inbound call_control_id)
 */
async function transferExistingLeg(callControlId, toE164) {
  console.log(`🔁 Transferring leg ${callControlId} -> ${toE164}`);
  const url = `/calls/${callControlId}/actions/transfer`;
  const body = {
    to: toE164,
    // optional: sip:..., or use audio_url during hold
  };
  const { data } = await telnyx.post(url, body);
  return data;
}

/**
 * Create a new outbound call to the branch (fallback when we
 * don’t have a call_control_id for the inbound leg).
 */
async function createOutboundCall(toE164, callerIdE164, whisper) {
  console.log(`📞 Outbound call -> ${toE164} (from ${callerIdE164})`);
  const payload = {
    connection_id: process.env.TELNYX_CONNECTION_ID,
    to: toE164,
    from: process.env.TELNYX_OUTBOUND_CALLER_ID || callerIdE164,
    timeout_secs: Math.ceil(HANDOFF_TIMEOUT_MS / 1000),
    // Optional: answer_url or audio_url to play a whisper
    // audio_url: MOH_URL,
  };
  const { data } = await telnyx.post("/calls", payload);
  // Optionally play a whisper to the callee:
  if (whisper) {
    try {
      await telnyx.post(`/calls/${data.data.call_control_id}/actions/speak`, {
        voice: "female",
        language: "en-US",
        payload: whisper,
      });
    } catch (e) {
      console.warn("Whisper speak failed:", e?.response?.data || e.message);
    }
  }
  return data;
}

// ──────────────────────────────────────────────
// Retell Webhook
// ──────────────────────────────────────────────
app.post("/retell/action", async (req, res) => {
  // Always ACK immediately
  res.status(200).json({ ok: true });

  try {
    console.log("🎧 Retell webhook:", JSON.stringify(req.body, null, 2));
    const { event, call, data } = req.body || {};
    const from = call?.from || "unknown";
    const to = call?.to || "unknown";
    const branchKey = detectBranchByToNumber(to);
    const branch = BRANCHES[branchKey];

    // 1) call started → send heads-up email
    if (event === "call.initiated") {
      await sendEmail(
        branch.email,
        `[${branchKey}] New Call`,
        `New call from ${from} to ${to}\nMOH: ${MOH_URL}`
      );
    }

    // 2) voicemail → email link
    if (event === "voicemail.received") {
      const audio = data?.recording_url || "(no recording url)";
      await sendEmail(
        branch.email,
        `[VOICEMAIL] ${branchKey}`,
        `Voicemail from ${from}\nAudio: ${audio}`
      );
    }

    // 3) analysis → optional email (nice for triage logs)
    if (event === "call_analyzed") {
      await sendEmail(
        branch.email,
        `[${branchKey}] Call Analyzed`,
        `From ${from} to ${to}\n\n${JSON.stringify(data, null, 2)}`
      );
    }

    // 4) custom handoff action from Retell (what we care about now)
    // Have your Retell agent send:
    // { "event":"action", "data": { "action":"transfer", "target":"PORTMORE" , "call_control_id":"xxxx" } }
    if (event === "action" && data?.action === "transfer") {
      const target = (data?.target || branchKey || "WINCHESTER").toUpperCase();
      let dest = BRANCHES[target]?.handoffPrimary || BRANCHES[target]?.handoff;

      // Portmore fallback
      if (target === "PORTMORE" && !dest) {
        dest = BRANCHES.PORTMORE.handoffPrimary;
      }

      if (!dest) {
        console.warn("No destination configured for target:", target);
      } else if (!telnyxReady()) {
        console.warn("Telnyx not configured; cannot transfer.");
        await sendEmail(
          branch.email,
          `[${target}] Transfer Requested (Telnyx not configured)`,
          `Caller ${from} requested transfer to ${target} (${dest}).`
        );
      } else {
        try {
          // Prefer true transfer if we have call_control_id of the inbound leg:
          if (data?.call_control_id) {
            await transferExistingLeg(data.call_control_id, dest);
            await sendEmail(
              branch.email,
              `[${target}] Live Transfer`,
              `Transferred live call from ${from} to ${dest}.`
            );
          } else {
            // Fallback: create outbound call to the branch with a whisper
            const whisper = `Winchester Heart Centre call for ${target}. Caller number ${from}.`;
            await createOutboundCall(dest, from, whisper);
            await sendEmail(
              branch.email,
              `[${target}] Callback Dial Started`,
              `Placed outbound call to ${dest}. Caller: ${from}`
            );
          }
        } catch (e) {
          console.error("Transfer/Outbound error:", e?.response?.data || e);
          await sendEmail(
            branch.email,
            `[${target}] Transfer Error`,
            `Error during transfer for caller ${from} → ${dest}.\n\n${
              e?.response?.data
                ? JSON.stringify(e.response.data, null, 2)
                : e.message
            }`
          );
        }
      }
    }

    // log unknown events for visibility
    if (
      ![
        "call.initiated",
        "voicemail.received",
        "call_analyzed",
        "action",
      ].includes(event)
    ) {
      console.log("ℹ️ Unhandled event:", event);
    }
  } catch (err) {
    console.error("❌ Webhook error:", err);
  }
});

// ──────────────────────────────────────────────
// Start + keep-alive + graceful shutdown
// ──────────────────────────────────────────────
const server = app.listen(PORT, () =>
  console.log(`🚀 WHC PBX server listening on port ${PORT}`)
);

const KEEPALIVE_MS = 240000;
setInterval(() => {
  fetch(`http://127.0.0.1:${PORT}/health`)
    .then((r) => console.log("🔄 Keep-alive ping:", r.status))
    .catch((e) => console.error("Keep-alive error:", e.message));
}, KEEPALIVE_MS);

function shutdown(signal) {
  console.log(`↩️  Received ${signal}. Closing server...`);
  server.close(() => {
    console.log("✅ HTTP server closed.");
    process.exit(0);
  });
  setTimeout(() => {
    console.warn("⏱️  Force exiting after 5s.");
    process.exit(0);
  }, 5000).unref();
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
