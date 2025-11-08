Winchester Heart Centre – PBX-in-code (Retell + Telnyx)
===========================================================

What this does
--------------
- Receives Retell AI action webhooks (/retell/action)
- Plays Music-on-Hold and bridges calls to the right branch
- Overflow to Winchester, then voicemail
- Emails voicemail MP3 + Whisper transcript to the branch inbox
- Optional DTMF handler for "#9" to return caller to AI

Deploy on Render (ZIP)
----------------------
1) Create a free account at https://render.com
2) New ➜ Web Service ➜ Upload this zip
3) Build Command:  npm install
   Start Command:  node server.js
4) Add environment variables (copy from .env.example)
5) Deploy – you'll get a URL like https://pbx-whc.onrender.com

Connect to Retell
-----------------
- In your Kimberley agent ➜ Webhook Settings ➜ set:
  https://YOUR-RENDER-URL/retell/action
- Publish the agent.

Forwarding
----------
- Forward your Jamaica main number (+1 876 908 2658) to your Retell number (+1 305 ...).

Test
----
- Call the Retell number. Ask for "Connect me to Portmore."
- You'll hear readback + hold music. System dials +1 876 704 2739 then +1 876 671 0478.
- If no answer ➜ overflow to Winchester ➜ voicemail ➜ email + transcript.

Notes
-----
- If you don't use OpenAI Whisper, leave OPENAI_API_KEY empty and you'll just get MP3 without transcript.
- TELNYX_API_KEY is optional when Retell manages the Telnyx number internally.
