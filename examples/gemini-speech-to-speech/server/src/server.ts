// Gemini speech-to-speech relay — per-device Durable Object that bridges
// the device's WebSocket to a Gemini Live API WebSocket.
//
// Protocol (device-facing):
//   client → server
//     {"type":"start"}                          // button pressed
//     <binary int16 PCM @ 16kHz>                // mic audio
//     {"type":"stop"}                           // button released
//
//   server → client
//     {"type":"session", "chatId":"..."}        // burst: frame 1/3
//     {"type":"ready"}                          // burst: frame 2/3
//     {"type":"settings", "volume":180,
//      "brightness":128, "voice":"Puck"}        // burst: frame 3/3
//                                               // (also re-sent when
//                                               //  volume/brightness change)
//     {"type":"transcript", "source":"user"|"model", "text":"..."}
//     {"type":"turn_complete"}
//     {"type":"drop_audio"}                     // interrupted
//     <binary int16 PCM @ 24kHz>                // Gemini audio

interface GeminiMessage {
	setupComplete?: Record<string, unknown>;
	serverContent?: {
		modelTurn?: { parts?: Array<{ inlineData?: { mimeType: string; data: string } }> };
		turnComplete?: boolean;
		interrupted?: boolean;
		inputTranscription?: { text: string };
		outputTranscription?: { text: string };
	};
	toolCall?: {
		functionCalls: Array<{
			name: string;
			id: string;
			args: Record<string, unknown>;
		}>;
	};
}

const GEMINI_MODEL = 'models/gemini-3.1-flash-live-preview';
const GEMINI_WS_URL =
	'https://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent';

// Self-modifying config — persisted in DO storage; reconnects pick up
// the latest values. `voice` and `persona` require a Gemini reconnect
// (they live in the one-shot setup). `volume` and `brightness` are
// applied by re-broadcasting the `settings` frame to the device.
interface DeviceConfig {
	voice: string;
	persona: string;
	volume: number;
	brightness: number;
}

const VOICE_NAMES = ['Puck', 'Charon', 'Kore', 'Fenrir', 'Aoede', 'Zephyr'];

const BASE_PROMPT = [
	'You are a voice assistant running on a handheld M5Stick — a small ESP32',
	'device with a tiny speaker. Keep replies short, warm, and conversational.',
	'If you cannot hear the user clearly, say so briefly rather than guessing.',
	'',
	'Tool-use rules:',
	'- set_voice(voice): when the user asks you to switch voices, change',
	'  accent, or sound different. Voices: ' + VOICE_NAMES.join(', ') + '.',
	'  Call immediately — do not ask for confirmation.',
	'- set_persona(persona): when the user asks you to act differently',
	'  (e.g. "be terse", "act like a pirate", "be more formal"). The',
	'  persona string is appended to your system prompt; keep it short.',
	'- set_volume(level): 0 (silent) to 255 (max). When the user asks you',
	"  to speak louder/quieter, pick a sensible level and call. If they",
	"  just say \"louder\", nudge up by about 40 from current.",
	'- set_brightness(level): 0 (off) to 255 (max). Same style for',
	'  "brighter" / "dimmer".',
	'',
	'If the user asks you to speak faster or slower, explain that speed',
	"control is not supported yet — do not call any tool for that.",
	"After any tool call, briefly acknowledge — don't narrate the JSON.",
].join(' ');

const DEFAULT_DEVICE_CONFIG: DeviceConfig = {
	voice: 'Puck',
	persona: '',
	volume: 180,
	brightness: 128,
};

const VOLUME_MIN = 0;
const VOLUME_MAX = 255;
const BRIGHTNESS_MIN = 0;
const BRIGHTNESS_MAX = 255;
const PERSONA_MAX = 500;

const TOOLS = {
	functionDeclarations: [
		{
			name: 'set_voice',
			description:
				"Change this voice assistant's voice. Takes effect on the next user turn. Persists across reconnects.",
			parameters: {
				type: 'OBJECT',
				properties: {
					voice: {
						type: 'STRING',
						description: `Voice name. One of: ${VOICE_NAMES.join(', ')}.`,
					},
				},
				required: ['voice'],
			},
		},
		{
			name: 'set_persona',
			description:
				"Set a persona/style layer appended to this assistant's base prompt. Use for tone/personality changes. Pass an empty string to clear. Takes effect on the next user turn.",
			parameters: {
				type: 'OBJECT',
				properties: {
					persona: {
						type: 'STRING',
						description: `Short persona/style instruction. Max ${PERSONA_MAX} chars. Empty string clears it.`,
					},
				},
				required: ['persona'],
			},
		},
		{
			name: 'set_volume',
			description:
				"Set the speaker volume on the device. Applied immediately. Persists across reconnects.",
			parameters: {
				type: 'OBJECT',
				properties: {
					level: {
						type: 'INTEGER',
						description: `Volume, ${VOLUME_MIN}–${VOLUME_MAX}. 180 is the default.`,
					},
				},
				required: ['level'],
			},
		},
		{
			name: 'set_brightness',
			description:
				"Set the display brightness on the device. Applied immediately. Persists across reconnects.",
			parameters: {
				type: 'OBJECT',
				properties: {
					level: {
						type: 'INTEGER',
						description: `Brightness, ${BRIGHTNESS_MIN}–${BRIGHTNESS_MAX}. 128 is the default.`,
					},
				},
				required: ['level'],
			},
		},
	],
};

function buildSystemPrompt(config: DeviceConfig): string {
	if (!config.persona) return BASE_PROMPT;
	return BASE_PROMPT + '\n\nPersona: ' + config.persona;
}

export class LiveSession {
	private state: DurableObjectState;
	private env: Env;
	private deviceWs: WebSocket | null = null;
	private geminiWs: WebSocket | null = null;
	private geminiReady = false;
	private chatId = '';
	private config: DeviceConfig = { ...DEFAULT_DEVICE_CONFIG };
	private pendingReconnect = false;
	private deviceId = '';
	private connectedAt: number | null = null;
	private lastGeminiError: { ts: number; msg: string } | null = null;
	private logBuffer: Array<{ ts: number; level: 'log' | 'warn' | 'error'; msg: string }> = [];
	private static readonly LOG_MAX = 100;

	constructor(state: DurableObjectState, env: Env) {
		this.state = state;
		this.env = env;
	}

	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);
		if (url.pathname.endsWith('/reset') && request.method === 'POST') {
			return this.handleReset();
		}
		if (url.pathname.startsWith('/debug')) {
			// DO may have just woken up cold for this probe — load
			// persisted config so the snapshot shows real values, not
			// in-memory defaults.
			await this.loadConfig();
			return this.handleDebug();
		}
		if (request.headers.get('Upgrade') !== 'websocket') {
			return new Response('Expected WebSocket', { status: 426 });
		}

		this.deviceId = url.searchParams.get('device_id') ?? 'anon';
		this.chatId = url.searchParams.get('chat_id') ?? crypto.randomUUID();
		await this.loadConfig();

		const pair = new WebSocketPair();
		const [client, server] = Object.values(pair);
		server.accept();
		this.deviceWs = server;
		this.connectedAt = Date.now();
		this.log('log', `[Device] connected device=${this.deviceId} chat=${this.chatId}`);

		// Deliberate burst: three JSON frames in one scheduler slice, sent
		// before the Gemini WS even opens. This is the reason Courier's
		// transport uses a FIFO — on the device side, all three need to
		// reach onMessage() without any being dropped.
		this.sendToDevice({ type: 'session', chatId: this.chatId });

		// Bind cleanup to this specific socket. If a new connection arrives
		// before the old one's close fires, the stale listener must not tear
		// down the replacement.
		server.addEventListener('message', (event) => this.onDeviceMessage(event.data));
		server.addEventListener('close', () => { if (this.deviceWs === server) this.cleanup(); });
		server.addEventListener('error', () => { if (this.deviceWs === server) this.cleanup(); });

		await this.connectGemini();

		return new Response(null, { status: 101, webSocket: client });
	}

	private async handleReset(): Promise<Response> {
		// Wipes persisted config so the next WS connect starts from
		// DEFAULT_DEVICE_CONFIG. Also resets in-memory so a live session
		// doesn't keep applying the stale values.
		await this.state.storage.delete('config');
		this.config = { ...DEFAULT_DEVICE_CONFIG };
		this.log('log', '[Reset] persisted config cleared');
		if (this.deviceWs) this.broadcastSettings();
		return new Response(JSON.stringify({ ok: true, config: this.config }, null, 2), {
			headers: { 'Content-Type': 'application/json' },
		});
	}

	private handleDebug(): Response {
		// Auth is enforced at the Worker boundary (isAuthorizedDebug); by
		// the time we get here the caller has presented a valid token.
		const snapshot = {
			deviceId: this.deviceId || null,
			chatId: this.chatId || null,
			geminiReady: this.geminiReady,
			hasDeviceWs: this.deviceWs !== null,
			hasGeminiWs: this.geminiWs !== null,
			pendingReconnect: this.pendingReconnect,
			config: this.config,
			connectedAt: this.connectedAt,
			uptimeMs: this.connectedAt !== null ? Date.now() - this.connectedAt : null,
			lastGeminiError: this.lastGeminiError,
			logs: this.logBuffer.slice(),
			now: Date.now(),
		};
		return new Response(JSON.stringify(snapshot, null, 2), {
			headers: { 'Content-Type': 'application/json' },
		});
	}

	private log(level: 'log' | 'warn' | 'error', msg: string) {
		this.logBuffer.push({ ts: Date.now(), level, msg });
		if (this.logBuffer.length > LiveSession.LOG_MAX) this.logBuffer.shift();
		console[level](msg);
	}

	private async loadConfig() {
		const saved = await this.state.storage.get<Partial<DeviceConfig>>('config');
		if (saved) this.config = { ...DEFAULT_DEVICE_CONFIG, ...saved };
	}

	private async connectGemini() {
		// Retry once — the second connect after a reconnectGemini() often
		// races an upstream close and returns a null webSocket. One short
		// backoff is enough to let Gemini release the previous session.
		let ws: WebSocket | null = null;
		for (let attempt = 1; attempt <= 2; attempt++) {
			try {
				const resp = await fetch(`${GEMINI_WS_URL}?key=${this.env.GEMINI_API_KEY}`, {
					headers: { Upgrade: 'websocket' },
				});
				ws = resp.webSocket ?? null;
			} catch (err) {
				const msg = `[Gemini] fetch attempt ${attempt} threw: ${err}`;
				this.log('error', msg);
				this.lastGeminiError = { ts: Date.now(), msg };
			}
			if (ws) break;
			this.log('warn', `[Gemini] attempt ${attempt} upgrade failed${attempt < 2 ? ', retrying' : ''}`);
			if (attempt < 2) await sleep(250);
		}
		if (!ws) {
			const msg = '[Gemini] upgrade failed after retry';
			this.log('error', msg);
			this.lastGeminiError = { ts: Date.now(), msg };
			this.deviceWs?.close(1011, 'gemini unavailable');
			return;
		}
		ws.accept();
		this.geminiWs = ws;

		ws.addEventListener('message', (event) => {
			if (this.geminiWs !== ws) return;  // stale
			const text =
				typeof event.data === 'string'
					? event.data
					: new TextDecoder().decode(event.data as ArrayBuffer);
			try {
				this.onGeminiMessage(JSON.parse(text) as GeminiMessage);
			} catch {
				// ignore non-JSON frames
			}
		});
		ws.addEventListener('close', () => {
			if (this.geminiWs !== ws) return;  // stale — reconnectGemini replaced us
			this.geminiWs = null;
			this.geminiReady = false;
			// If Gemini dropped (idle timeout, transient upstream failure),
			// tear the device WS down too so Courier reconnects into a
			// fresh session — otherwise every later mic chunk is silently
			// discarded while geminiReady is false.
			if (this.deviceWs) {
				try { this.deviceWs.close(1011, 'gemini disconnected'); } catch { /* ignore */ }
			}
		});

		// `speechConfig` lives inside `generationConfig` for the Live API.
		ws.send(
			JSON.stringify({
				setup: {
					model: GEMINI_MODEL,
					generationConfig: {
						responseModalities: ['AUDIO'],
						speechConfig: {
							voiceConfig: {
								prebuiltVoiceConfig: { voiceName: this.config.voice },
							},
						},
					},
					inputAudioTranscription: {},
					outputAudioTranscription: {},
					systemInstruction: { parts: [{ text: buildSystemPrompt(this.config) }] },
					tools: [TOOLS],
				},
			})
		);
	}

	private onDeviceMessage(data: string | ArrayBuffer) {
		if (data instanceof ArrayBuffer) {
			if (!this.geminiReady || !this.geminiWs) return;
			this.geminiWs.send(
				JSON.stringify({
					realtimeInput: {
						audio: {
							data: arrayBufferToBase64(data),
							mimeType: 'audio/pcm;rate=16000',
						},
					},
				})
			);
			return;
		}

		let msg: { type?: string };
		try {
			msg = JSON.parse(data);
		} catch {
			return;
		}

		// Flush 1s of trailing silence so Gemini's VAD detects end-of-turn.
		if (msg.type === 'stop' && this.geminiWs && this.geminiReady) {
			const silence = new ArrayBuffer(32000);  // 16kHz * 1s * 2 bytes
			this.geminiWs.send(
				JSON.stringify({
					realtimeInput: {
						audio: { data: arrayBufferToBase64(silence), mimeType: 'audio/pcm;rate=16000' },
					},
				})
			);
		}
	}

	private onGeminiMessage(msg: GeminiMessage) {
		if (!this.geminiReady && !msg.setupComplete) {
			// Pre-setup messages from Gemini are almost always errors —
			// log them so malformed setup payloads don't fail silently.
			const raw = JSON.stringify(msg).slice(0, 500);
			this.log('log', `[Gemini] pre-setupComplete message: ${raw}`);
			this.lastGeminiError = { ts: Date.now(), msg: raw };
		}
		if (msg.setupComplete) {
			this.log('log', '[Gemini] setupComplete');
			this.geminiReady = true;
			// Second and third frames of the burst. Sent back-to-back —
			// these land in the same WS task tick on the device and all
			// three must survive the FIFO. On a mutation-triggered
			// reconnect, this also re-broadcasts the new voice.
			this.sendToDevice({ type: 'ready' });
			this.broadcastSettings();
			return;
		}

		if (msg.toolCall) {
			for (const call of msg.toolCall.functionCalls) {
				this.handleToolCall(call).catch((err) => {
					this.log('error', `[Tool] handler error: ${err}`);
				});
			}
			return;
		}

		const sc = msg.serverContent;
		if (!sc) return;

		for (const part of sc.modelTurn?.parts ?? []) {
			if (part.inlineData?.data) {
				this.deviceWs?.send(base64ToArrayBuffer(part.inlineData.data));
			}
		}

		if (sc.inputTranscription?.text) {
			this.sendToDevice({ type: 'transcript', source: 'user', text: sc.inputTranscription.text });
		}
		if (sc.outputTranscription?.text) {
			this.sendToDevice({ type: 'transcript', source: 'model', text: sc.outputTranscription.text });
		}
		if (sc.interrupted) {
			this.sendToDevice({ type: 'drop_audio' });
		}
		if (sc.turnComplete) {
			this.sendToDevice({ type: 'turn_complete' });
			if (this.pendingReconnect) {
				this.pendingReconnect = false;
				this.log('log', '[Tool] turn complete — reconnecting gemini now');
				this.reconnectGemini().catch((err) => {
					this.log('error', `[Tool] deferred reconnect failed: ${err}`);
				});
			}
		}
	}

	private async handleToolCall(call: { name: string; id: string; args: Record<string, unknown> }) {
		this.log('log', `[Tool] call: ${call.name}(${JSON.stringify(call.args)})`);

		// Tools that change Gemini setup (voice, persona) need a reconnect;
		// we defer it until turnComplete so the model's confirmation audio
		// isn't cut off mid-sentence. Device-side tools (volume, brightness)
		// just re-broadcast the settings frame — no reconnect needed.
		const reject = (error: string) => {
			this.log('warn', `[Tool] rejected ${call.name}: ${error}`);
			this.sendToolResponse(call, { ok: false, error });
		};

		switch (call.name) {
			case 'set_voice': {
				const voice = call.args.voice;
				if (typeof voice !== 'string' || !VOICE_NAMES.includes(voice)) {
					return reject(`voice must be one of: ${VOICE_NAMES.join(', ')}`);
				}
				await this.updateConfig({ voice });
				this.sendToolResponse(call, { ok: true, voice });
				this.pendingReconnect = true;
				this.log('log', `[Tool] applied voice=${voice}, reconnect queued for turn end`);
				return;
			}
			case 'set_persona': {
				const persona = call.args.persona;
				if (typeof persona !== 'string') return reject('persona must be a string');
				if (persona.length > PERSONA_MAX) {
					return reject(`persona too long (${persona.length} > ${PERSONA_MAX})`);
				}
				await this.updateConfig({ persona });
				this.sendToolResponse(call, { ok: true, persona });
				this.pendingReconnect = true;
				this.log('log', `[Tool] applied persona (${persona.length} chars), reconnect queued for turn end`);
				return;
			}
			case 'set_volume': {
				const level = call.args.level;
				if (typeof level !== 'number' || !Number.isFinite(level)) {
					return reject('level must be a number');
				}
				const volume = Math.round(clamp(level, VOLUME_MIN, VOLUME_MAX));
				await this.updateConfig({ volume });
				this.broadcastSettings();
				this.sendToolResponse(call, { ok: true, volume });
				this.log('log', `[Tool] applied volume=${volume}`);
				return;
			}
			case 'set_brightness': {
				const level = call.args.level;
				if (typeof level !== 'number' || !Number.isFinite(level)) {
					return reject('level must be a number');
				}
				const brightness = Math.round(clamp(level, BRIGHTNESS_MIN, BRIGHTNESS_MAX));
				await this.updateConfig({ brightness });
				this.broadcastSettings();
				this.sendToolResponse(call, { ok: true, brightness });
				this.log('log', `[Tool] applied brightness=${brightness}`);
				return;
			}
			default:
				return reject(`unknown tool: ${call.name}`);
		}
	}

	private async updateConfig(patch: Partial<DeviceConfig>) {
		this.config = { ...this.config, ...patch };
		await this.state.storage.put('config', this.config);
	}

	private broadcastSettings() {
		this.sendToDevice({
			type: 'settings',
			voice: this.config.voice,
			volume: this.config.volume,
			brightness: this.config.brightness,
		});
	}

	private sendToolResponse(call: { name: string; id: string }, response: unknown) {
		if (!this.geminiWs) return;
		this.geminiWs.send(
			JSON.stringify({
				toolResponse: {
					functionResponses: [{ name: call.name, id: call.id, response }],
				},
			})
		);
	}

	private async reconnectGemini() {
		if (this.geminiWs) {
			// Null *before* close so the stale-guard in the close listener
			// trips and the listener does not tear down deviceWs.
			const old = this.geminiWs;
			this.geminiWs = null;
			this.geminiReady = false;
			try { old.close(); } catch { /* ignore */ }
		}
		await this.connectGemini();
	}

	private sendToDevice(msg: Record<string, unknown>) {
		this.deviceWs?.send(JSON.stringify(msg));
	}

	private cleanup() {
		if (this.geminiWs) {
			try { this.geminiWs.close(); } catch { /* ignore */ }
			this.geminiWs = null;
		}
		this.geminiReady = false;
		this.deviceWs = null;
	}
}

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);

		if (url.pathname === '/ws') {
			// One DO per device so reconnects land back in the same session.
			const deviceId = url.searchParams.get('device_id') ?? 'anon';
			const id = env.LIVE_SESSION.idFromName(deviceId);
			return env.LIVE_SESSION.get(id).fetch(request);
		}

		// Debug probe: `GET /debug/:deviceId` returns the DO's current
		// state + recent log buffer. `POST /debug/:deviceId/reset` wipes
		// persisted config. Both require `Authorization: Bearer <DEBUG_TOKEN>`.
		const debugMatch = url.pathname.match(/^\/debug\/([\w.-]+)(\/reset)?$/);
		if (debugMatch) {
			if (!isAuthorizedDebug(request, env)) {
				return new Response('Unauthorized', { status: 401 });
			}
			const deviceId = debugMatch[1];
			const id = env.LIVE_SESSION.idFromName(deviceId);
			return env.LIVE_SESSION.get(id).fetch(request);
		}

		return new Response('Not found', { status: 404 });
	},
} satisfies ExportedHandler<Env>;

function isAuthorizedDebug(request: Request, env: Env): boolean {
	const configured = env.DEBUG_TOKEN?.trim();
	if (!configured) return false;
	const auth = request.headers.get('Authorization') ?? '';
	const match = auth.match(/^Bearer\s+(.+)$/);
	if (!match) return false;
	return timingSafeEqual(match[1], configured);
}

function timingSafeEqual(a: string, b: string): boolean {
	if (a.length !== b.length) return false;
	let result = 0;
	for (let i = 0; i < a.length; i++) {
		result |= a.charCodeAt(i) ^ b.charCodeAt(i);
	}
	return result === 0;
}

// ─── utils ───

function arrayBufferToBase64(buffer: ArrayBuffer): string {
	const bytes = new Uint8Array(buffer);
	const CHUNK = 8192;
	let binary = '';
	for (let i = 0; i < bytes.length; i += CHUNK) {
		binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
	}
	return btoa(binary);
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function clamp(value: number, min: number, max: number): number {
	return Math.min(Math.max(value, min), max);
}

function base64ToArrayBuffer(base64: string): ArrayBuffer {
	const binary = atob(base64);
	const bytes = new Uint8Array(binary.length);
	for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
	return bytes.buffer;
}
