// Gemini Chat — push-to-talk voice assistant backed by Gemini Live.
//
// This sketch demonstrates the two Courier features the simpler demos
// don't exercise:
//
//   1. Binary WS frames, both directions — mic PCM out, Gemini audio
//      PCM back in. See sendBinaryTo() and onBinaryMessage().
//   2. A burst of JSON frames on connect — session / ready / settings
//      arrive in one scheduler slice and all land in onMessage().
//
// The button-to-talk loop is deliberately the simplest thing that works:
// while A is held, grab a mic chunk, ship it; on release, send a {stop}
// so the server can flush its end-of-turn silence to Gemini.

#include <M5Unified.h>
#include <Courier.h>

namespace {

constexpr uint32_t kMicSampleRate = 16000;  // Gemini Live input format
constexpr uint32_t kSpkSampleRate = 24000;  // Gemini Live output format
constexpr size_t   kMicChunkSamples = 320;  // 20ms @ 16kHz
constexpr size_t   kPlaybackQueueBytes = 64 * 1024;

int16_t micBuffer[kMicChunkSamples];

// Track session state derived from the server's JSON burst.
bool sessionReady  = false;     // set when we see {"type":"ready"}
bool wasPressed    = false;     // for A-button edge detection
String activeChatId;            // captured from {"type":"session"}
String lastTranscript;          // latest streaming transcript line

// Path including device_id — filled in from the chip MAC in setup(), so
// each board lands in its own Durable Object on the server side. The
// buffer is static so the pointer handed to CourierConfig stays valid.
char wsPath[64] = "/ws?device_id=gemini-chat-default";

CourierConfig makeConfig() {
  CourierConfig cfg;
  // Replace with your deployed Worker hostname before flashing.
  cfg.host   = "gemini-chat.YOUR-SUBDOMAIN.workers.dev";
  cfg.port   = 443;
  cfg.path   = wsPath;
  cfg.apName = "Gemini Chat";
  return cfg;
}

Courier courier(makeConfig());

void drawStatus(const char *line1, const char *line2 = nullptr) {
  M5.Display.fillScreen(BLACK);
  M5.Display.setCursor(4, 4);
  M5.Display.setTextColor(WHITE, BLACK);
  M5.Display.setTextSize(2);
  M5.Display.setTextWrap(true);
  M5.Display.println(line1);
  if (line2) {
    M5.Display.setTextSize(1);
    M5.Display.setTextColor(0xC618, BLACK);  // light grey
    M5.Display.println();
    M5.Display.println(line2);
  }
}

void startListening() {
  sessionReady && courier.send(R"({"type":"start"})");
  M5.Mic.begin();
  drawStatus("Listening...", activeChatId.c_str());
}

void stopListening() {
  M5.Mic.end();
  courier.send(R"({"type":"stop"})");
  drawStatus("Thinking...", activeChatId.c_str());
}

// Server → device audio. Gemini's 24kHz PCM chunks stream in as raw
// binary frames; hand them straight to the speaker.
void onAudio(const uint8_t *data, size_t len) {
  if (len == 0 || (len & 1)) return;  // must be int16-aligned
  M5.Speaker.playRaw(reinterpret_cast<const int16_t *>(data),
                     len / sizeof(int16_t),
                     kSpkSampleRate,
                     /*stereo=*/false,
                     /*repeat=*/1,
                     /*channel=*/0);
}

// Server → device control frames. These arrive as a burst on connect —
// three in the same scheduler slice — so Courier's FIFO is doing real
// work here.
void onControl(const char *type, JsonDocument &doc) {
  if (!type) return;

  if (strcmp(type, "session") == 0) {
    activeChatId = doc["chatId"].as<const char *>() ? doc["chatId"].as<const char *>() : "";
    drawStatus("Session started", activeChatId.c_str());
    return;
  }
  if (strcmp(type, "ready") == 0) {
    sessionReady = true;
    drawStatus("Hold A to talk", activeChatId.c_str());
    return;
  }
  if (strcmp(type, "settings") == 0) {
    // Applied silently — here we just log that it arrived, which is the
    // whole point: before the FIFO fix, this third frame of the burst
    // used to get dropped.
    const uint8_t vol = doc["volume"] | 128;
    M5.Speaker.setVolume(vol);
    return;
  }
  if (strcmp(type, "transcript") == 0) {
    const char *source = doc["source"] | "";
    const char *text   = doc["text"]   | "";
    lastTranscript = String(source) + ": " + text;
    drawStatus(lastTranscript.c_str(), activeChatId.c_str());
    return;
  }
  if (strcmp(type, "turn_complete") == 0) {
    drawStatus("Hold A to talk", activeChatId.c_str());
    return;
  }
  if (strcmp(type, "drop_audio") == 0) {
    M5.Speaker.stop();
    return;
  }
}

}  // namespace

void setup() {
  auto m5cfg = M5.config();
  M5.begin(m5cfg);
  M5.Display.setRotation(1);
  M5.Speaker.begin();
  M5.Speaker.setVolume(180);

  // Allocate a reasonable playback queue — 24kHz audio eats buffer fast.
  M5.Speaker.config().task_priority = 2;

  Serial.begin(115200);

  // Derive a per-board device_id from the chip MAC so two devices on the
  // same Worker land in separate Durable Objects.
  snprintf(wsPath, sizeof(wsPath),
           "/ws?device_id=gemini-chat-%012llx",
           (unsigned long long)ESP.getEfuseMac());

  drawStatus("Connecting...", nullptr);

  courier.onConnected([]() {
    drawStatus("Waiting for AI...", nullptr);
  });

  courier.onDisconnected([]() {
    sessionReady = false;
    drawStatus("Reconnecting...", nullptr);
  });

  courier.onMessage(onControl);
  courier.onBinaryMessage(onAudio);

  courier.onError([](const char *category, const char *message) {
    Serial.printf("[Courier] %s: %s\n", category, message);
  });

  courier.setup();
}

void loop() {
  M5.update();
  courier.loop();

  const bool pressed = M5.BtnA.isPressed();
  if (pressed && !wasPressed && sessionReady) {
    startListening();
  } else if (!pressed && wasPressed && sessionReady) {
    stopListening();
  }
  wasPressed = pressed;

  // While the button is held, stream mic chunks as fast as we can
  // capture them. Each chunk goes out as a single binary WS frame.
  if (pressed && sessionReady && M5.Mic.isEnabled()) {
    if (M5.Mic.record(micBuffer, kMicChunkSamples, kMicSampleRate)) {
      courier.sendBinaryTo("ws",
                           reinterpret_cast<const uint8_t *>(micBuffer),
                           sizeof(micBuffer));
    }
  }
}
