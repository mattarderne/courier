# Gemini Chat

Push-to-talk voice chat with Google's Gemini Live API, relayed through a
Cloudflare Worker. An M5Stick captures mic audio, streams it over a
WebSocket to the Worker, and plays back the Gemini audio response.

```
M5Stick ──(WS binary: 16kHz PCM)──▶ Cloudflare Worker (DO) ──▶ Gemini Live API
        ◀──(WS binary: 24kHz PCM)──                         ◀──  (speech-to-speech)
        ◀──(WS JSON:  control)─────
```

## Why this example exists

This demo exercises two parts of Courier that don't show up in the simpler
examples:

- **Binary WS frames** (`onBinaryMessage`) — Gemini's audio comes back as
  raw PCM in binary frames, not JSON. The device plays them straight into
  the speaker.
- **Bursts of JSON frames** — the Worker sends three control messages in
  the same scheduler slice on connect (`session` → `ready` → `settings`).
  Courier's internal FIFO absorbs the burst so none of them get dropped.

If you're building voice, streaming telemetry, or anything where the
server talks back with both binary payloads _and_ control messages, this
is the shape.

## Structure

```
gemini-chat/
├── device/     # PlatformIO firmware for M5StickC Plus2 / S3
└── server/     # Cloudflare Worker — Durable Object that relays to Gemini
```

## Wire protocol

**Device → Server**

| Frame            | Type   | Meaning                                   |
| ---------------- | ------ | ----------------------------------------- |
| `{"type":"start"}` | text   | Button pressed, start of turn             |
| raw `int16` PCM  | binary | 16 kHz mono audio, ~20 ms chunks           |
| `{"type":"stop"}`  | text   | Button released, end of turn              |

**Server → Device**

| Frame                          | Type   | Meaning                                        |
| ------------------------------ | ------ | ---------------------------------------------- |
| `{"type":"session",...}`       | text   | Chat id (first of the burst on connect)        |
| `{"type":"ready"}`             | text   | Gemini is ready (second of the burst)          |
| `{"type":"settings",...}`      | text   | Device-side config (third of the burst)        |
| `{"type":"transcript",...}`    | text   | Streaming user / model transcripts             |
| `{"type":"turn_complete"}`     | text   | Model finished speaking                        |
| `{"type":"drop_audio"}`        | text   | Interrupted — drop any buffered playback       |
| raw 24 kHz `int16` PCM         | binary | Gemini audio chunks                            |

## Server

A Cloudflare Worker with one Durable Object (`LiveSession`) bridging each
device WebSocket to Gemini's Live API.

### Setup

```bash
cd server
npm install

# Set your Gemini API key for local dev
cp .dev.vars.example .dev.vars
# edit .dev.vars with a GEMINI_API_KEY from https://aistudio.google.com/

npm run dev
```

### Deploy

```bash
cd server
npx wrangler secret put GEMINI_API_KEY
npm run deploy
```

Then copy the deployed hostname into `device/src/main.cpp` (the `cfg.host`
line) and re-flash.

## Device

Two M5Stick variants are supported:

- **M5StickC Plus2** (yellow) — ESP32-PICO-V3-02, CH9102 USB-UART bridge
- **M5StickS3** (grey) — ESP32-S3, native USB (uses the built-in speaker
  and MEMS mic)

The S3 has a proper speaker and microphone, so that's the one to pick if
you want audible voice. The Plus2 mic + buzzer will still run the demo
but the audio quality is limited.

### Build & flash

```bash
cd device

# M5StickC Plus2 (default):
pio run -t upload
pio device monitor

# M5StickS3:
pio run -e m5sticks3 -t upload
pio device monitor
```

### Connect to Wi-Fi

On first boot the device starts an access point called **Gemini Chat**.
Connect to it from your phone and enter your Wi-Fi credentials via the
captive portal. Courier persists them for next boot. (ESP32 is 2.4 GHz
only.)

### Using it

1. Hold button A to talk.
2. Release when you're done.
3. Listen to the response through the speaker.
4. Transcripts stream on the display as they arrive.

## Notes

- Audio in is 16 kHz mono `int16`; audio out is 24 kHz mono `int16`.
  Gemini Live is fixed at these rates.
- The `sendBinaryTo("ws", ...)` API on the device and `onBinaryMessage`
  callback are the binary-frame primitives. They live alongside the
  existing text `send()` / `onMessage()` — you can use both in the same
  session.
- The burst of `session`/`ready`/`settings` frames on connect is
  intentional — it's the minimum set the device needs to start a session
  and it's the reason Courier's transport uses a FIFO rather than a
  single-slot pending buffer.
