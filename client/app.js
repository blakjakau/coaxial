// Screen Relay browser client.
//
// The live desktop video is served two ways:
//
//   - RAW (primary, low latency): /raw is a WebSocket that carries raw H.264
//     (Annex-B) access units as binary messages (one message per access
//     unit). A WebCodecs-capable browser (Chrome 94+) decodes each access
//     unit with a VideoDecoder and renders the decoded frame to a <canvas>
//     as it arrives - no container, no fragment buffering, ~1-2 frames of
//     end-to-end latency. The first access unit carries the SPS/PPS (NAL
//     types 7/8); the decoder config (codec string + description) is
//     built from them.
//   - fMP4 (fallback): /stream is a plain HTTP progressive download of
//     fragmented MP4 (fMP4). Chrome cannot play fMP4 in a bare <video src>,
//     so the bytes are fetched and piped into a MediaSource/SourceBuffer
//     (the same approach YouTube/Replit use). The server's Go fMP4 muxer
//     writes an up-front moov carrying the avcC (SPS/PPS), so the codec
//     string is parsed from the stream itself.
//
// Mode selection: ?mode=fmp4 forces the fMP4 fallback; otherwise the raw
// WebCodecs path is used when VideoDecoder is available (the default), and
// the fMP4 path is used when it is not.
//
// The raw path is the PRIMARY stream and is reconnect-tolerant: a transient
// failure (the /raw socket closes, the upgrade hangs, a decode error) does
// NOT fail over to fMP4 - the socket reconnects (onclose -> ensureRawPath)
// and the decoder keeps waiting for the next SPS/PPS. The fMP4/MSE path is
// a MANUAL fallback (?mode=fmp4) for browsers without WebCodecs, not an
// automatic failover.
//
// The /relay WebSocket is the control channel only: ping/pong keepalive
// plus key/mouse input forwarding (kind 'I').
//
// Wire protocol (server -> browser):
//   [1 byte kind][payload...]
//   kind 'P' = ping, kind 'G' = pong
//
// Browser -> server:
//   [1 byte kind][payload...]
//   kind 'I' = input event (payload: JSON of {type,x,y,keys})
//   kind 'P' = ping

// Wire up UI status element.
const statusEl = document.getElementById("status")
function setStatus(state, text) {
	statusEl.className = state
	statusEl.textContent = text
}

// Connection state.
let ws = null
let reconnectTimer = null
let closed = false
// mediaSource is the current MediaSource; a pipeline reconnect closes it and
// creates a fresh one (the old object URL is revoked and the <video> element
// is re-pointed at the new one).
let mediaSource = null
// mode is 'raw' (WebCodecs VideoDecoder -> <canvas>, the primary low-latency
// path) or 'fmp4' (MSE SourceBuffer -> <video>, the fallback).
let mode = null
// rawSocket is the /raw WebSocket (one binary message per H.264 access unit).
let rawSocket = null
// rawReconnectTimer guards the raw-path reconnect: a single pending timer
// (cleared on a successful open) prevents stacked reconnect timers from
// piling up when the socket flaps.
let rawReconnectTimer = null
// decoder is the WebCodecs VideoDecoder for the raw path.
let decoder = null
// rawConfigured is true once the decoder config (SPS/PPS) has been built from
// the first access unit.
let rawConfigured = false
// rawConfigFailures counts consecutive failed config attempts on SPS-carrying
// access units (diagnostics only). A failure is not fatal (the client may have
// joined mid-GOP on a delta slice, or the first SPS-carrying access unit may
// be malformed); we retry on each subsequent SPS-carrying access unit and keep
// the raw path (the fMP4 fallback is manual-only: ?mode=fmp4).
let rawConfigFailures = 0
// rawFrameIndex counts encoded frames fed to the decoder (timestamp = index *
// 33333us, a monotonic ~30fps clock; the decoder only requires monotonicity,
// not wall-clock accuracy, for the live-edge render).
let rawFrameIndex = 0
// rawDecoded counts frames rendered to the canvas (diagnostics).
let rawDecoded = 0
// rawBytes counts bytes received over the /raw socket (bandwidth metric for the
// performance overlay, sampled per second alongside rawDecoded).
let rawBytes = 0
// rawInFlight = frames submitted to decoder.decode() but not yet returned via
// the output() callback (self-tracked, because VideoDecoder exposes no
// frameQueueSize - that is an MSE SourceBuffer property, not a WebCodecs one).
// A steady rawInFlight ≈ N means the decoder is running ~N frames behind live:
// the ~260ms decode latency = rawInFlight x frame interval. This is the metric
// that tracks the pipeline depth while tuning res/fps/bitrate.
let rawInFlight = 0
// --- Audio path (Phase 4) ---
// audioSocket is the /audio WebSocket (one binary message per opus packet).
let audioSocket = null
// audioDecoder is the WebCodecs AudioDecoder for the opus stream.
let audioDecoder = null
// audioCtx is the AudioContext that plays decoded PCM. It is created on the
// first audio button press (an AudioContext cannot start before a user
// gesture; the button click is that gesture).
let audioCtx = null
// audioNextStartTime is the AudioContext time at which the next decoded
// buffer is scheduled (a monotonic playback clock; each decoded buffer is
// scheduled at audioNextStartTime and the clock advances by the buffer's
// duration).
let audioNextStartTime = 0
// audioTs is the opus packet timestamp counter (microseconds). The server
// sends bare opus packets (no PTS), so the client assigns a monotonic PTS
// per packet. opusenc at 32kbps emits one packet per 20ms frame, so each
// packet advances the clock by 20000us.
let audioTs = 0
// audioFirstFrame tracks whether the next opus packet to be decoded is the
// FIRST one for the current decoder. Chrome's WebCodecs opus decoder
// initializes its internal state from the first frame, and that frame must be
// marked type:"key" (opus has no real keyframes, but the decoder needs a
// "key" chunk to start); every subsequent frame is type:"delta". Feeding the
// first frame as "delta" makes the decoder reject the ENTIRE stream with a
// DataError (verified empirically: all-delta -> 0/200 decoded, key-first ->
// 200/200). Reset to true whenever a new decoder is created (ensureAudioPath).
let audioFirstFrame = true
// audioReconnectTimer guards the audio-path reconnect (single-flight, like
// rawReconnectTimer).
let audioReconnectTimer = null
// audioEnabled is the user's audio on/off preference (the button's active
// state). When true, the /audio socket is open and decoding.
let audioEnabled = false
// --- Audio diagnostics (temporary; remove once audio is confirmed working) ---
// audioDiag counts packets received / decoded / scheduled so the console log
// (every 2s while audio is on) shows whether the break is in the socket
// (no packets), the decoder (packets but no output), or playback (output but
// nothing scheduled). audioDiagZero reports whether the last decoded chunk
// contained any non-zero sample (a silent monitor source produces all-zero
// PCM, which plays as silence).
let audioDiag = { pkt: 0, sizes: [], decoded: 0, chunks: [], scheduled: 0, zero: 0, ctxState: "", decodeErr: null, closeErr: null, head: null }
let audioHead = null // last OpusHead received from the server (stream channels + rate)
let audioHeadApplied = false // whether the decoder has been (re)configured from the head
// Prefix the server prepends to the OpusHead re-emit message so a client can
// distinguish it from a bare opus audio packet on the same binary channel.
const AUDIO_HEAD_PREFIX = "AUDIOHEAD:"
function isAudioHeadMsg(pkt) {
	const n = AUDIO_HEAD_PREFIX.length
	if (pkt.length < n) return false
	for (let i = 0; i < n; i++) if (pkt[i] !== AUDIO_HEAD_PREFIX.charCodeAt(i)) return false
	return true
}
let audioDiagTimer = null
// Per-frame debug records (capped at DBG_KEEP). Each record is ONE frame and
// carries its three wall-clock timestamps (performance.now(), ms) TOGETHER so
// the on-canvas overlay and the click-to-copy report can show PER-FRAME
// latencies that are aligned to the same frame (the old design used three
// separate cadence arrays, so row N of "render" was NOT the same frame as row
// N of "decode"/"arrive" - the columns were misaligned and unreadable):
//   arrive - the access unit hit the WebSocket (onmessage).
//   decode - the VideoDecoder output() callback fired for this frame.
//   blit   - this frame was painted to the canvas (blitLatest). Absent if the
//            frame was decoded but then dropped (a newer frame won the
//            latest-frame-wins race before the next rAF tick).
//   cad    - the gap since the PREVIOUS blit (the on-screen blit cadence).
// The three per-frame latencies the report shows are:
//   decode = decode - arrive  (WS delivery -> decoder output; decoder + queue)
//   render = blit - decode    (decoder output -> painted; rAF + main-thread blit)
//   cad    = blit - prevBlit  (on-screen cadence; a spike = the visible hitch)
// A hitch in `cad` with small `decode`/`render` = the main thread / blit is the
// cause. A hitch in `decode` = the decoder. A hitch in `arrive` (the WS
// cadence, shown by the server's "AU gap" log) = encoder / network.
const dbgFrames = []
const DBG_KEEP = 60
const DBG_SHOW = 30
// dbgPendingArrivals is a FIFO of access-unit arrival timestamps (ms) awaiting
// a matching decoder output. onmessage pushes one per AU; the output() callback
// shifts one per decoded frame. The VideoDecoder outputs frames in the order
// they were fed (one AU = one frame), so the shift pairs each decoded frame
// with the access unit that produced it. Bounded by the decoder's internal
// queue depth (small for a live stream).
const dbgPendingArrivals = []
// prevBlit is the performance.now() (ms) of the last blit (or 0); used to
// compute the on-screen blit cadence (the gap between consecutive blits).
let prevBlit = 0
// MAX_RENDER_W caps the canvas BACKING STORE width (aspect preserved). The CSS
// scales the #canvas element up to fill the stage (max-width/max-height:100%),
// so a smaller backing store still fills the display. Capping the backing store
// keeps the per-frame 2D blit small: blitting the full native resolution
// (~2.6M px) every frame — and especially the largest keyframes once per GOP
// (~1s) — pushes the main-thread frame over budget and shows as a momentary
// halt. A 1280-wide blit (~0.7M px) stays well under the 33ms frame budget.
const MAX_RENDER_W = 3440

// concatUint8 concatenates two Uint8Arrays into a new one.
function concatUint8(a, b) {
	const out = new Uint8Array(a.length + b.length)
	out.set(a)
	out.set(b, a.length)
	return out
}

// parseAvcCCodec extracts the avc1.PPCCLL codec string from an avcC box
// payload (the bytes after the 8-byte box header). Returns "" if the bytes
// do not look like a valid avcC.
//
// avcC layout: version(1) profile_idc(1) constraint(1) level_idc(1) ...
// codec string: avc1.<PP><CC><LL> where PP = profile_idc, CC = the
// constraint byte, LL = level_idc, each as 2 uppercase hex digits (6 total).
// e.g. profile 0x4D (High), constraint 0x40, level 0x32 (5.0) -> avc1.4D4032.
function parseAvcCCodec(payload) {
	if (payload.length < 4) return ""
	const profile = payload[1]
	const compat = payload[2]
	const level = payload[3]
	const two = (n) => n.toString(16).toUpperCase().padStart(2, "0")
	return "avc1." + two(profile) + two(compat) + two(level)
}

// findAvcCCodec scans a byte buffer for an avcC box and returns the codec
// string parsed from it, or "" if not found. The avcC lives inside the
// up-front moov box that GStreamer's mp4mux writes (streamable=true),
// within the first ~1KB of the stream.
function findAvcCCodec(buf) {
	// "avcC" marker.
	const m0 = 0x61, m1 = 0x76, m2 = 0x63, m3 = 0x43
	for (let i = 0; i + 4 <= buf.length; i++) {
		if (buf[i] === m0 && buf[i + 1] === m1 && buf[i + 2] === m2 && buf[i + 3] === m3) {
			// The 4 bytes after the type are the avcC payload start.
			const payloadStart = i + 4
			const payloadEnd = Math.min(buf.length, payloadStart + 32)
			return parseAvcCCodec(buf.subarray(payloadStart, payloadEnd))
		}
	}
	return ""
}

// Fmp4Assembler reassembles a continuous fMP4 byte stream into complete
// ISO-BMFF boxes and emits each complete box (or box group) to a callback.
//
// Box layout: [4 byte big-endian size][4 byte type][payload...]. A size of 1
// means the real size is the following 8 bytes (rare; treated as
// incomplete for safety). Boxes are only emitted once fully received, so the
// callback always gets well-formed box boundaries.
class Fmp4Assembler {
	constructor(onBoxes) {
		this.onBoxes = onBoxes
		this.residual = new Uint8Array(0)
	}

	// feed appends a chunk and emits every complete box it now contains.
	feed(chunk) {
		const buf = concatUint8(this.residual, chunk)
		let off = 0
		let emitted = new Uint8Array(0)
		while (buf.length - off >= 8) {
			const size =
				(buf[off] << 24) | (buf[off + 1] << 16) | (buf[off + 2] << 8) | buf[off + 3]
			if (size === 1 || size < 8) break // 64-bit or malformed: wait for more
			if (buf.length - off < size) break // incomplete box
			emitted = concatUint8(emitted, buf.subarray(off, off + size))
			off += size
		}
		this.residual = buf.subarray(off)
		if (emitted.length > 0) this.onBoxes(emitted)
	}
}

// attachVideo wires the <video> element to the live /stream endpoint via
// MediaSource. The stream is fetched and its fMP4 boxes are appended to a
// SourceBuffer as they arrive.
//
// Playback is deferred to the "Click to start" overlay: Chrome's autoplay
// policy rejects play() before any user gesture on the document.
//
// getToken() is async, so this must be async too and the caller must await
// it: otherwise the URL would carry token=[object Promise] and the server
// would reject the request with 401.
async function attachVideo() {
	const video = document.getElementById("video")
	const token = await getToken()
	const params = new URLSearchParams()
	// The browser cannot set headers on a <video> src, so the bearer token
	// is carried in the URL query string.
	if (token) params.set("token", token)
	const url = `/stream${params.toString() ? `?${params.toString()}` : ""}`

	// Close and revoke the previous MediaSource (if any) so a reconnect
	// replaces the pipeline rather than leaking a dead one.
	if (mediaSource) {
		try {
			mediaSource.readyState === "closed"
				? null
				: mediaSource.close()
		} catch (e) {
			// already closed; ignore.
		}
		try {
			URL.revokeObjectURL(video.src)
		} catch (e) {
			// not a blob URL; ignore.
		}
	}
	const ms = new MediaSource()
	mediaSource = ms
	video.src = URL.createObjectURL(ms)

	ms.addEventListener("sourceopen", () => {
		startStreamPipeline(video, url, ms)
	})
	ms.addEventListener("error", () => {
		setStatus("err", "media source error")
	})
	// First fMP4 frame visible (playback started): fade out the splash screen.
	video.addEventListener("playing", hideSplash, { once: true })
}

// parseNals splits an Annex-B access unit into its NAL units. Each unit is
// {type, data} where type is the H.264 NAL unit type (byte & 0x1F) and data
// is the NAL header byte plus its payload (without the start code).
//
// NAL types: 1 = non-IDR slice, 5 = IDR slice, 7 = SPS, 8 = PPS, 9 = SEI.
//
// Single-pass O(n): the start codes are located in one scan (the old version
// re-scanned from the previous start code on every NAL, which is O(n^2) and
// stalls the main thread on the large keyframe access units once per GOP).
// Both the 4-byte (00 00 00 01, what h264parse emits) and 3-byte (00 00 01)
// start codes are accepted; the NAL header is the first byte after the code.
function parseNals(au) {
	const offsets = []
	for (let i = 0; i + 3 <= au.length; i++) {
		if (au[i] !== 0 || au[i + 1] !== 0 || au[i + 2] !== 0) continue
		// 4-byte start code (00 00 00 01): the NAL header is at i+4. The 3-byte
		// form (00 00 01) is the tail of this when au[i-1] is 0.
		if (i + 4 <= au.length && au[i + 3] === 1) {
			let start = i
			if (i > 0 && au[i - 1] === 0) start = i - 1
			offsets.push({ start, nal: i + 4 })
			i += 3 // skip past this start code (avoids a spurious 3-byte match)
			continue
		}
		// 3-byte start code (00 00 01): the NAL header is at i+3.
		if (i + 3 <= au.length && au[i + 2] === 1) {
			offsets.push({ start: i, nal: i + 3 })
		}
	}
	const nals = []
	for (let k = 0; k < offsets.length; k++) {
		const nal = offsets[k].nal
		const end = k + 1 < offsets.length ? offsets[k + 1].start : au.length
		if (nal < end) {
			nals.push({ type: au[nal] & 0x1f, data: au.subarray(nal, end) })
		}
	}
	return nals
}

// hasIdrSlice reports whether the access unit contains an IDR slice NAL (type
// 5). It scans for a NAL header byte whose low 5 bits (nal_unit_type) equal 5
// AND which is immediately preceded by a start code, in a single O(n) pass -
// much cheaper than parseNals, which was the per-keyframe main-thread cost
// (keyframes are the largest access units and arrive once per GOP ~1s).
//
// The NAL header byte is forbit(1) nal_ref_idc(2) nal_unit_type(5). The top
// 2 bits (nal_ref_idc) vary by encoder (NVENC emits 0b11, so an IDR slice
// header is 0x65, not 0x45), so we match on the low 5 bits only:
// (byte & 0x1F) === 5.
//
// A byte is a NAL header only if it is immediately preceded by a start code:
// [0,0,1] (3-byte) or [0,0,0,1] (4-byte). In both cases the byte just before
// the header is the start code's trailing 1 and the two bytes before that are
// 0. H.264 emulation prevention guarantees 00 00 01 never appears inside a
// NAL payload, so a 00 00 01 XX sequence is unambiguously a start code + NAL
// header, never payload data - no false positives.
function hasIdrSlice(au) {
	for (let i = 3; i < au.length; i++) {
		if ((au[i] & 0x1F) !== 5) continue
		// Preceded by a start code: [0,0,1] (3-byte) or [0,0,0,1] (4-byte).
		// In both cases the byte just before the header is the start code's
		// trailing 1 and the two bytes before that are 0. H.264 emulation
		// prevention guarantees 00 00 01 never appears inside a NAL payload,
		// so a 00 00 01 XX sequence is unambiguously a start code + NAL header.
		if (au[i - 1] === 1 && au[i - 2] === 0 && au[i - 3] === 0) {
			return true
		}
	}
	return false
}

// buildDecoderConfig builds a WebCodecs VideoDecoderConfig from the SPS/PPS
// NAL units carried in an access unit. The codec string is avc1.PPCCLL
// (profile_idc, constraint byte, level_idc from the SPS).
//
// Per the W3C AVC codec registration, when VideoDecoderConfig.description is
// PRESENT it is assumed to be an AVCDecoderConfigurationRecord (ISO 14496-15
// §5.3.3.1) and the bitstream is assumed to be in "avc" (length-prefixed)
// format; when it is ABSENT the bitstream is assumed to be "annexb". Our
// raw WS path sends Annex-B access units (start-code delimited), so we MUST
// omit the description: a description built from raw SPS+PPS NAL bytes is not
// a valid AVCDecoderConfigurationRecord and would make Chrome assume a
// length-prefixed bitstream, causing isConfigSupported to return false. With
// no description, Chrome reads SPS/PPS from the stream itself (h264parse
// re-emits them before each IDR, so a mid-stream join still gets config).
// Returns the codec string on success, or null if no SPS is present or the
// codec is not supported.
async function buildDecoderConfig(au) {
	const nals = parseNals(au)
	let sps = null
	for (const n of nals) {
		if (n.type === 7 && !sps) sps = n.data
	}
	if (!sps || sps.length < 4) return null
	// n.data is the NAL header byte plus its payload (WITHOUT the start
	// code), so the SPS payload fields are at: sps[0] = NAL header (0x67),
	// sps[1] = profile_idc, sps[2] = constraint byte, sps[3] = level_idc.
	// (The fMP4 path reads these from the start-code-relative offsets
	// m.sps[5..7] on the server; here they are NAL-relative.)
	const profile = sps[1]
	const constraint = sps[2]
	const level = sps[3]
	const two = (n) => n.toString(16).toUpperCase().padStart(2, "0")
	const codec = `avc1.${two(profile)}${two(constraint)}${two(level)}`
	const cfg = { codec, optimizeForLatency: true, hardwareAcceleration: "prefer-software"}
	const supported = await VideoDecoder.isConfigSupported(cfg)
	if (!supported.supported) {
		console.warn("raw: codec not supported:", codec, supported)
		return null
	}
	return codec
}

// drawDebugOverlay paints a semi-transparent debug column on the left edge of
// the canvas: the current FPS (over the whole dbgFrames window, derived from
// the on-screen blit cadences) and three PER-FRAME latencies (ms) for the
// last DBG_SHOW frames, each row aligned to the SAME frame:
//   decode = WS-arrival -> decoder-output (decoder + queue)
//   render = decoder-output -> blit-to-canvas (rAF + main-thread blit)
//   cad    = blit -> previous blit (the on-screen cadence; the visible hitch)
// It runs on the hot render path, so it is deliberately cheap (a few
// fillRect/fillText calls, no per-frame allocations beyond the small strings).
// A spike in a latency (e.g. once per GOP at a keyframe) is exactly the
// main-thread hitch we are debugging, so it is highlighted in red.
function drawDebugOverlay(ctx, w, h) {
	const colW = 238
	const pad = 8
	const rowH = 14
	const x0 = pad
	const x1 = pad + 44
	const x2 = pad + 88
	const x3 = pad + 132
	const x4 = pad + 176
	// Panel height covers the header + DBG_SHOW rows + a "click to copy" hint.
	const panelH = pad + 48 + DBG_SHOW * rowH + 26
	ctx.save()
	// Semi-transparent backing panel so the text stays legible over any frame.
	ctx.fillStyle = "rgba(0, 0, 0, 0.55)"
	ctx.fillRect(0, 0, colW, panelH)
	// FPS over the whole window: (n-1) intervals / (t_last - t_first), where
	// the intervals are the on-screen blit cadences (the dbgFrames `cad`
	// field). This is the cadence the user actually sees.
	let fps = 0
	const cads = []
	for (const r of dbgFrames) if (r.cad !== undefined) cads.push(r.cad)
	if (cads.length >= 2) {
		const span = cads[cads.length - 1] - cads[0]
		if (span > 0) fps = ((cads.length - 1) * 1000) / span
	}
	ctx.textBaseline = "top"
	ctx.font = "13px monospace"
	ctx.fillStyle = "#7cfc00"
	ctx.fillText("FPS " + fps.toFixed(1), x0, pad)
	// Three PER-FRAME latencies side by side (each = the gap between two
	// timestamps on the SAME frame, ms). A spike (red, >40ms) in one column
	// pinpoints the stalling stage for that frame:
	//   decode = decode - arrive (WS delivery -> decoder output; the decoder
	//            + its queue; a spike = the decoder is slow, e.g. a keyframe).
	//   render = blit - decode (decoder output -> painted; rAF + the main-
	//            thread blit; a spike = main-thread contention or a slow blit).
	//   cad    = blit - prevBlit (the on-screen blit cadence; a spike = the
	//            visible hitch the user sees).
	ctx.fillStyle = "#e6edf3"
	ctx.font = "11px monospace"
	// Per-column headers, each aligned to its value column. dq = the decoder's
	// decodeQueueSize at decode-completion (the backlog of access units still
	// queued in the VideoDecoder; a steady dq >> 0 = the decoder is running a
	// constant number of frames behind live).
	ctx.fillText("decode", x0, pad + 18)
	ctx.fillText("render", x1, pad + 18)
	ctx.fillText("cad", x2, pad + 18)
	ctx.fillText("dq", x3, pad + 18)
	ctx.fillText("inflt", x4, pad + 18)
	ctx.fillText("ms/frame (last " + DBG_SHOW + "):", x0, pad + 32)
	const len = dbgFrames.length
	const start = Math.max(0, len - DBG_SHOW)
	for (let i = 0; i < DBG_SHOW; i++) {
		const rec = dbgFrames[start + i]
		if (!rec) break
		const y = pad + 48 + i * rowH
		// decode latency (arrive -> decode). undefined if the arrival was not
		// yet paired (should not happen in steady state).
		const dec = rec.arrive !== undefined ? rec.decode - rec.arrive : undefined
		// render latency (decode -> blit). undefined if the frame was decoded
		// but dropped (a newer frame won the latest-frame-wins race).
		const ren = rec.blit !== undefined ? rec.blit - rec.decode : undefined
		// blit cadence (the gap since the previous blit).
		const cad = rec.cad
		ctx.font = "11px monospace"
		const cell = (ms, x) => {
			if (ms === undefined) {
				ctx.fillStyle = "#484f58"
				ctx.fillText("-", x, y)
				return
			}
			ctx.fillStyle = ms > 40 ? "#f85149" : "#e6edf3" // red = hitch (>40ms)
			ctx.fillText(ms.toFixed(1), x, y)
		}
		cell(dec, x0)
		cell(ren, x1)
		cell(cad, x2)
		// dq = the decoder's decodeQueueSize at decode-completion (integer
		// frames queued, not ms). Red when the backlog is large (>4 frames ≈
		// >130ms of decode latency at 30fps) - the decoder is the floor.
		if (rec.dq === undefined) {
			ctx.fillStyle = "#484f58"
			ctx.fillText("-", x3, y)
		} else {
			ctx.fillStyle = rec.dq > 4 ? "#f85149" : "#e6edf3"
			ctx.fillText(String(rec.dq), x3, y)
		}
		// inflt = frames submitted to decode() but not yet returned via
		// output() (self-tracked; VideoDecoder has no frameQueueSize - that's
		// an MSE SourceBuffer property). A steady inflt ≈ N means the decoder
		// is running ~N frames behind live (the ~260ms decode latency = inflt
		// x frame interval). This is the real pipeline-depth metric for tuning.
		// Red when >4 frames (>130ms of decode latency at 30fps).
		if (rec.inflt === undefined) {
			ctx.fillStyle = "#484f58"
			ctx.fillText("-", x4, y)
		} else {
			ctx.fillStyle = rec.inflt > 4 ? "#f85149" : "#e6edf3"
			ctx.fillText(String(rec.inflt), x4, y)
		}
	}
	// Hint: a click on the canvas copies the report (see wireDebugCopy).
	ctx.fillStyle = "#8b949e"
	ctx.font = "10px monospace"
	ctx.fillText("click = copy", x0, pad + 48 + DBG_SHOW * rowH + 8)
	ctx.restore()
}

// buildDebugReport assembles a copyable text report of the debug series: the
// last DBG_SHOW render/decode/arrive gaps (ms) as three labelled columns, plus
// the FPS. Used by wireDebugCopy (click the canvas) so the on-canvas overlay
// (pixels) can be grabbed as text for analysis.
function buildDebugReport() {
	// FPS over the whole window: (n-1) intervals / (t_last - t_first), where
	// the intervals are the on-screen blit cadences (the `cad` field).
	let fps = 0
	const cads = []
	for (const r of dbgFrames) if (r.cad !== undefined) cads.push(r.cad)
	if (cads.length >= 2) {
		const span = cads[cads.length - 1] - cads[0]
		if (span > 0) fps = ((cads.length - 1) * 1000) / span
	}
	const lines = [
		"FPS " + fps.toFixed(1),
		"decode render cad dq inflt", // per-frame latencies (ms), aligned to the SAME frame
		"decode = WS->decode | render = decode->blit | cad = blit cadence | dq = decodeQueueSize | inflt = frames in flight (submitted, not yet output)",
		"(red = >40ms or dq>4/inflt>4; - = frame dropped / not yet paired)"
	]
	const len = dbgFrames.length
	const start = Math.max(0, len - DBG_SHOW)
	for (let i = 0; i < DBG_SHOW; i++) {
		const rec = dbgFrames[start + i]
		if (!rec) break
		const dec = rec.arrive !== undefined ? rec.decode - rec.arrive : undefined
		const ren = rec.blit !== undefined ? rec.blit - rec.decode : undefined
		const cad = rec.cad
		const fmt = (ms) => (ms === undefined ? "-" : ms.toFixed(1))
		const fmtDq = (n) => (n === undefined ? "-" : String(n))
		lines.push(
			fmt(dec).padStart(7) +
				fmt(ren).padStart(7) +
				fmt(cad).padStart(7) +
				fmtDq(rec.dq).padStart(4) +
				fmtDq(rec.inflt).padStart(4)
		)
	}
	return lines.join("\n")
}

// wireDebugCopy binds a click on the raw canvas to copy the debug series
// (buildDebugReport) to the clipboard and log it to the console. The canvas
// has no other click handler in raw mode, so this is safe to bind once. The
// clipboard write is best-effort (navigator.clipboard needs a secure context +
// the click gesture); the console copy always works as a fallback.
let debugCopyWired = true
function wireDebugCopy() {
	if (debugCopyWired) return
	debugCopyWired = true
	const c = document.getElementById("canvas")
	c.addEventListener("click", () => {
		const report = buildDebugReport()
		console.debug&&console.debug("raw debug (last " + DBG_SHOW + " frames):\n" + report)
		if (navigator.clipboard && navigator.clipboard.writeText) {
			navigator.clipboard.writeText(report).then(
				() => console.debug&&console.debug("raw debug copied to clipboard"),
				(e) => console.warn("clipboard copy failed:", e)
			)
		}
	})
}

// ensureRawPath opens the /raw WebSocket (the primary low-latency path) and
// starts the WebCodecs decode/render pipeline. Each binary message is one
// H.264 access unit (Annex-B); the first access unit carries the SPS/PPS,
// which configures the decoder. Decoded frames are rendered to the <canvas>
// as they arrive (live edge: the latest decoded frame wins).
//
// The raw path is the PRIMARY stream: a transient failure (the WebSocket
// closes, the upgrade hangs, the decoder hits an error) does NOT fail over to
// the fMP4 fallback - it reconnects (onclose) or keeps waiting (config/decode
// errors). The fMP4/MSE path is manual-only (?mode=fmp4) for browsers without
// WebCodecs.
async function ensureRawPath() {
	if (mode !== "raw") return
	const token = await fetchToken()
	const protocol = location.protocol === "https:" ? "wss" : "ws"
	const url = `${protocol}://${location.host}/raw${token ? `?token=${encodeURIComponent(token)}` : ""}`
	const sock = new WebSocket(url)
	sock.binaryType = "arraybuffer"
	rawSocket = sock

	// Cache the canvas + 2d context (set up once; the context is reused for
	// every frame).
	const c = document.getElementById("canvas")
	const ctx = c.getContext("2d")

	// If the /raw upgrade never succeeds (401/404/500 from the server, or a
	// network failure), the WebSocket fires onerror + onclose without
	// onopen. The raw path is the PRIMARY stream, so a transient failure is
	// tolerated: the onclose handler (below) reconnects with backoff rather
	// than failing over to the fMP4 fallback (which is now manual-only:
	// ?mode=fmp4). The open-timer is a safety net for a HUNG connection (no
	// onclose fired yet): after the grace period, force-close the socket so
	// onclose fires and the reconnect path runs.
	const openTimer = setTimeout(() => {
		if (sock.readyState !== WebSocket.OPEN && mode === "raw") {
			try {
				sock.close()
			} catch (e) {
				// already closed; ignore.
			}
		}
	}, 5000)

	sock.onopen = () => {
		clearTimeout(openTimer)
		// A successful open cancels any pending reconnect (the socket is live
		// again).
		if (rawReconnectTimer) {
			clearTimeout(rawReconnectTimer)
			rawReconnectTimer = null
		}
		setStatus("ok", "connected (raw)")
	}

		// onmessage is NOT async: the hot path (decode + render) is
		// fire-and-forget and must not yield to the event loop per access unit
		// (the async handler + O(n^2) NAL scan on the large keyframes once per
		// GOP ~1s was the source of the momentary client-side stall). Only the
		// rare pre-config branch (once, before the decoder is configured) needs
		// await, so it runs in a fire-and-forget IIFE.
	sock.onmessage = (ev) => {
		const au = new Uint8Array(ev.data)
		if (au.length < 5) return
		// Performance overlay: count the received bytes (bandwidth metric).
		if (perfOn) perfBytes += au.length
		// Record the access-unit ARRIVAL timestamp (the WebSocket delivered
		// this AU). It is queued in dbgPendingArrivals and paired with the
		// matching decoder output (shifted in output()). The arrival cadence
		// (gap between consecutive arrivals) is the client-side view of the
		// encoder + network - it should track the server's "AU gap" log.
		dbgPendingArrivals.push(performance.now())
		if (dbgPendingArrivals.length > DBG_KEEP) dbgPendingArrivals.shift()
		// Until the decoder is configured we cannot decode anything (no SPS/PPS).
		// The first access unit received may be a delta slice (no SPS/PPS) if the
		// client joined mid-GOP, so a single access unit without an SPS is NOT
		// fatal: skip it and keep waiting. h264parse re-emits SPS/PPS before each
		// IDR, so an SPS-carrying access unit arrives within one GOP. We retry
		// the config on each SPS-carrying access unit and keep the raw path
		// (the fMP4 fallback is manual-only: ?mode=fmp4).
		if (!rawConfigured) {
			// Pre-config branch (runs once, before the decoder is configured):
			// the only await in this handler, so it is isolated in a
			// fire-and-forget IIFE to keep the hot path synchronous.
			;(async () => {
			let hasSps = false
			for (const n of parseNals(au)) {
				if (n.type === 7) {
					hasSps = true
					break
				}
			}
			if (!hasSps) return // delta slice; no config possible yet.
			const codec = await buildDecoderConfig(au)
			console.debug&&console.debug(codec)
			// A competing access unit may have configured the decoder while we
			// were awaiting (the pre-config branch is fire-and-forget). Only the
			// first to reach this point creates the decoder + primes it.
			if (rawConfigured) return
			if (!codec) {
				rawConfigFailures++
				// A config failure is not fatal: the raw path is the PRIMARY
				// stream and the fMP4 fallback is now manual-only (?mode=fmp4).
				// Log and keep waiting for the next SPS-carrying access unit
				// (h264parse re-emits SPS/PPS before each IDR, so a fresh
				// SPS/PPS pair arrives within one GOP). If the codec is
				// genuinely unsupported, the user can switch to ?mode=fmp4.
				console.warn(
					"raw: decoder config failed (" +
						rawConfigFailures +
						" consecutive); waiting for next SPS/PPS"
				)
				return
			}
			rawConfigFailures = 0
			// Latest-frame-wins live-edge drop. The VideoDecoder is a queue: it
			// fires output() as each frame finishes, and when it drains a
			// backlog it does so in a burst. Blitting every output frame (the
			// old behavior) makes the canvas jump forward several frames at
			// once, then freeze during the next slow decode (the ~238ms gap in
			// the debug report). Instead we keep only the MOST RECENT decoded
			// frame (latestFrame) and close (discard) any older one still in
			// flight. The blit is deferred to the next requestAnimationFrame
			// tick, so a burst of output() calls coalesces into a single blit
			// of the latest frame — the canvas always shows the newest frame
			// and never paints a backlog.
			let latestFrame = null
			let blitScheduled = false
			function blitLatest() {
				const frame = latestFrame
				if (!frame) return
				// Blit the latest frame to the canvas. The backing store is
				// capped at MAX_RENDER_W (aspect preserved) instead of the
				// native resolution: the CSS scales the canvas element up to
				// fill the stage, so a smaller backing store still fills the
				// display while the per-frame blit stays small enough to
				// keep the main-thread frame under budget.
				const scale = Math.min(1, MAX_RENDER_W / frame.displayWidth)
				const w = Math.max(1, Math.round(frame.displayWidth * scale))
				const h = Math.max(1, Math.round(frame.displayHeight * scale))
				if (c.width !== w || c.height !== h) {
					c.width = w
					c.height = h
				}
				ctx.drawImage(frame, 0, 0, w, h)
				frame.close()
				latestFrame = null
				rawDecoded++
				// Record the blit timestamp on this frame's debug record (the
				// record was created in output() when the decoder handed the
				// frame back; we now know it was painted). Compute the on-screen
				// blit cadence (the gap since the previous blit) for the debug
				// overlay, then paint the overlay.
				const now = performance.now()
				const rec = dbgFrames[dbgFrames.length - 1]
				if (rec) {
					rec.blit = now
					rec.cad = prevBlit ? now - prevBlit : undefined
				}
				prevBlit = now
				//drawDebugOverlay(ctx, w, h)
				// First frame visible: fade out the splash screen.
				hideSplash()
				// Performance overlay: count this blit as a rendered frame.
				if (perfOn) perfFrames++
			}
			decoder = new VideoDecoder({
				output: (frame) => {
					// Record this frame's decode-completion time (the decoder
					// finished it and handed it back), paired with the access
					// unit that produced it (shift from dbgPendingArrivals). This
					// is independent of the blit: even if the frame is later
					// discarded (closed without being painted because a newer
					// frame arrived), the decode time is still useful for
					// diagnosing the decoder's cadence. The record is created here
					// and its `blit` field is filled in later by blitLatest() if
					// this frame actually gets painted.
					const rec = {
						arrive: dbgPendingArrivals.shift(),
						decode: performance.now(),
						// Decoder queue depths at decode-completion time (cheap).
						// dq = decoder.decodeQueueSize: access units submitted but
						// not yet "made sufficient progress" (pending decode
						// requests). Stays 0 in steady state because the codec
						// accepts requests at the submission rate even while its
						// pipeline is deep.
						dq: decoder.decodeQueueSize,
						// inflt = rawInFlight: frames submitted to decode() but
						// not yet returned via output() (self-tracked; VideoDecoder
						// has no frameQueueSize - that's an MSE SourceBuffer
						// property). A steady inflt ≈ N = the decoder is running ~N
						// frames behind live (the ~260ms decode latency = inflt x
						// frame interval). This is the real pipeline-depth metric.
						inflt: rawInFlight
					}
					dbgFrames.push(rec)
					if (dbgFrames.length > DBG_KEEP) dbgFrames.shift()
					// This frame just left the pipeline: decrement the in-flight
					// count (the record above already captured the pre-decrement
					// value, so the displayed depth = frames in flight including
					// the one that just completed).
					if (rawInFlight > 0) rawInFlight--
					// Latest-frame-wins: discard the previously-pending frame
					// (it is now stale: a newer frame just arrived) and keep
					// only the newest. close() releases its GPU resources; it
					// was never blitted, so nothing to undo. If the decoder
					// drains a backlog in a burst (multiple output() calls
					// before the next rAF tick), only the last frame survives
					// to be blitted — the intermediate ones are closed without
					// being painted, so the canvas tracks the live edge rather
					// than jumping forward several frames at once.
					if (latestFrame && latestFrame !== frame) {
						latestFrame.close()
					}
					latestFrame = frame
					// Blit on the next animation frame (not synchronously in
					// output()), so a burst of output() calls coalesces into a
					// single blit of the latest frame. rAF aligns the blit with
					// the display refresh, avoiding tearing and keeping the
					// canvas in sync with the compositor.
					if (!blitScheduled) {
						blitScheduled = true
						requestAnimationFrame(() => {
							blitScheduled = false
							blitLatest()
						})
					}
				},
				error: (e) => {
					// A decode error (a corrupted frame, a delta before a key,
					// etc.) is not fatal: the raw path is the PRIMARY stream and
					// the fMP4 fallback is manual-only (?mode=fmp4). Log it and
					// keep going - the decoder skips the bad frame and the next
					// IDR (the server re-emits SPS/PPS before each IDR) re-syncs
					// the stream. If the decoder is truly wedged, the user can
					// refresh or switch to ?mode=fmp4.
					console.error("raw: decoder error (continuing):", e)
				}
			})
				// No description: the bitstream is Annex-B (we send start-code
				// delimited access units), so Chrome reads SPS/PPS from the stream.
				decoder.configure({ 
					codec, 
					optimizeForLatency: true, 
					hardwareAcceleration: "prefer-software" 
				})
			rawConfigured = true
			console.debug&&console.debug("raw: decoder configured:", codec)
			// Prime the decoder with THIS access unit. The SPS-carrying AU is
			// the IDR frame (the server groups SPS/PPS into the same AU as the
			// IDR slice), and WebCodecs requires the first chunk after
			// configure() to be a key frame. If we returned without decoding
			// it, the next AU (a delta) would fail with "A key frame is
			// required after configure() or flush()".
			decodeAu(au)
		})()
			return
		}
		decodeAu(au)
	}

	// decodeAu feeds one access unit to the decoder. A chunk is "key" if it
	// contains an IDR slice (type 5); otherwise "delta". The timestamp is a
	// monotonic ~33ms frame clock (the decoder only requires monotonicity,
	// not wall-clock accuracy, for the live-edge render). hasIdrSlice is a
	// single O(n) scan for the IDR NAL header byte (no full NAL parse), so the
	// per-access-unit cost stays cheap even for the large keyframes.
	function decodeAu(au) {
		const isKey = hasIdrSlice(au)
		const ts = rawFrameIndex * 33333
		rawFrameIndex++
		try {
			decoder.decode(
				new EncodedVideoChunk({
					type: isKey ? "key" : "delta",
					timestamp: ts,
					data: au.buffer.slice(au.byteOffset, au.byteOffset + au.byteLength)
				})
			)
			// A frame is now in the decoder's pipeline: submitted, not yet
			// returned via output(). Decrement in output() when it completes.
			rawInFlight++
		} catch (e) {
			console.error("raw: decode failed:", e)
		}
	}

	sock.onclose = () => {
		if (mode !== "raw") return // manual fMP4 mode; ignore.
		setStatus("err", "raw stream closed - reconnecting...")
		rawSocket = null
		rawConfigured = false
		rawConfigFailures = 0
		rawInFlight = 0
		try {
			if (decoder) decoder.close()
		} catch (e) {
			// already closed; ignore.
		}
		decoder = null
		// Reset the per-frame debug records (a reconnect starts a fresh stream;
		// stale records from the dead pipeline would mislead the report).
		dbgFrames.length = 0
		dbgPendingArrivals.length = 0
		prevBlit = 0
		// Reconnect with a short backoff. The timer is single-flight
		// (rawReconnectTimer) so a flapping socket doesn't stack reconnect
		// attempts; onopen clears it once the socket is live again.
		if (rawReconnectTimer) return
		rawReconnectTimer = setTimeout(() => {
			rawReconnectTimer = null
			if (mode === "raw" && !closed) ensureRawPath()
		}, 1000)
	}

	sock.onerror = () => {
		setStatus("err", "raw stream error")
	}
}

// toggleAudio flips the audio relay on or off. Enabling opens the /audio
// WebSocket and starts the WebCodecs AudioDecoder; disabling tears the path
// down (socket, decoder, AudioContext suspended). The AudioContext is created
// here (inside the button click) because an AudioContext cannot start before
// a user gesture.
async function toggleAudio() {
	if (audioEnabled) {
		closeAudioPath()
		audioEnabled = false
		const btn = document.getElementById("btn-audio")
		if (btn) btn.classList.remove("active")
		return
	}
	// The audio path needs WebCodecs AudioDecoder (the video path already
	// requires it in raw mode; fMP4 mode may run in a browser without it).
	if (typeof AudioDecoder === "undefined") {
		console.warn("audio: WebCodecs AudioDecoder not available")
		return
	}
	// The audio path also needs an AudioContext (Web Audio API).
	if (typeof AudioContext === "undefined") {
		console.warn("audio: AudioContext not available")
		return
	}
	// User gesture: create/resume the AudioContext now (autoplay policy).
	if (!audioCtx) {
		audioCtx = new AudioContext()
		audioNextStartTime = 0
	}
	if (audioCtx.state === "suspended") {
		await audioCtx.resume()
	}
	audioEnabled = true
	const btn = document.getElementById("btn-audio")
	if (btn) btn.classList.add("active")
	// DIAG: reset the counters and log a summary every 2s while audio is on.
	// pkt = packets received from the socket; decoded = chunks the decoder
	// emitted; scheduled = buffers scheduled on the AudioContext; zero =
	// decoded chunks that were all-zero (silence). A healthy stream shows all
	// three counters advancing at ~50/s (20ms opus packets); zero should be
	// ~0 if the host is producing sound.
	audioDiag = { pkt: 0, sizes: [], decoded: 0, chunks: [], scheduled: 0, zero: 0, ctxState: audioCtx ? audioCtx.state : "", head: null, headSeen: 0, keyFrames: 0, layout: null }
	audioHead = null
	audioHeadApplied = false
	if (audioDiagTimer) clearInterval(audioDiagTimer)
	audioDiagTimer = setInterval(() => {
	console.debug&&console.debug(
			"audio diag:",
			JSON.stringify({
				pkt: audioDiag.pkt,
				decoded: audioDiag.decoded,
				scheduled: audioDiag.scheduled,
				zero: audioDiag.zero,
				ctx: audioCtx ? audioCtx.state : "null",
				dest: audioCtx ? audioCtx.destination.channelCount : "?",
				decoder: audioDecoder ? audioDecoder.state : "null",
				head: audioDiag.head,
				headSeen: audioDiag.headSeen,
				keyFrames: audioDiag.keyFrames,
				layout: audioDiag.layout,
				sizes: audioDiag.sizes.length ? audioDiag.sizes : "(no packets)",
				chunks: audioDiag.chunks.length ? audioDiag.chunks : "(no output)",
				decodeErr: audioDiag.decodeErr,
				closeErr: audioDiag.closeErr,
			})
		)
	}, 2000)
	ensureAudioPath()
}

// ensureAudioPath opens the /audio WebSocket and starts the WebCodecs
// AudioDecoder for the opus stream. Each binary message is one opus packet;
// it is fed to the decoder with a monotonic PTS (audioTs, +20000us per
// packet). Decoded PCM is scheduled onto the AudioContext at
// audioNextStartTime.
async function ensureAudioPath() {
	if (!audioEnabled) return
	const token = await fetchToken()
	const protocol = location.protocol === "https:" ? "wss" : "ws"
	const url = `${protocol}://${location.host}/audio${token ? `?token=${encodeURIComponent(token)}` : ""}`
	const sock = new WebSocket(url)
	sock.binaryType = "arraybuffer"
	audioSocket = sock

	// Configure the AudioDecoder for opus. The server encodes at 48kHz
	// (audioconvert/audioresample -> 48000), so the decoder is configured for
	// 48kHz. The channel count is left to the decoder (opus is mono or
	// stereo; the server's audioconvert normalizes to 2ch, but the decoder
	// handles both).
	if (audioDecoder) {
		try {
			audioDecoder.close()
		} catch (e) {
			// already closed; ignore.
		}
		audioDecoder = null
	}
	audioTs = 0
	audioNextStartTime = 0
	audioHead = null
	audioHeadApplied = false
	// New decoder: the next opus packet is its first frame (type:"key").
	audioFirstFrame = true
	audioDecoder = new AudioDecoder({
			output: (chunk) => {
				playAudioData(chunk)
			},
			error: (e) => {
				// DIAG: capture the async decode error (invalid opus packet, etc.)
				// so the 2s summary shows it even if the console scroll lost it.
				audioDiag.closeErr = e ? (e.name + ": " + e.message) : String(e)
				// Allow a later OpusHead to (re)configure this decoder: a
				// config-mismatch DataError that closed the decoder is cured by
				// reconfiguring to the stream's real channels/rate.
				audioHeadApplied = false
				console.warn("audio: decoder error:", e)
			},
			// DIAG: onclose fires when the decoder transitions to "closed" (an
			// error, or an explicit close). Without this, a dead decoder is
			// silent - the socket keeps feeding, the state guard returns early,
			// and nothing ever plays again.
			onclose: () => {
				console.warn("audio: decoder closed (state -> closed); will not decode further")
			},
		})
	// Configure the decoder from the stream's ACTUAL OpusHead (channel count +
	// sample rate) rather than a hard-coded 48kHz/2ch guess. WebCodecs requires
	// codec, sampleRate, and numberOfChannels (all required members of
	// AudioDecoderConfig), and the config MUST match the stream exactly or
	// every decode throws a DataError. applyAudioHead is called with the
	// server's OpusHead (re-emitted to every subscriber, like the fMP4 init
	// segment); until the head arrives the decoder stays unconfigured and
	// onmessage skips audio packets (it only decodes when state ===
	// "configured"). The head is the first message on the audio channel, so
	// this is a one-time setup, not a per-packet stall.
	function applyAudioHead(head) {
		if (!audioDecoder || audioDecoder.state === "closed") return
		let channels = 2, sampleRate = 48000
		// OpusHead (RFC 3533 / oggopus, 19 bytes): "OpusHead"(0-7) version(8)
		// channel_count(9) pre_skip(10-11 LE) output_sample_rate(12-15 LE)
		// gain(16-17) mapping_family(18). channel_count: 1 = mono, 2 = stereo.
		if (head && head.length >= 16) {
			const b = new DataView(head.buffer, head.byteOffset, head.byteLength)
			channels = head[9]
			if (channels < 1) channels = 1
			if (channels > 2) channels = 2 // WebCodecs opus: 1 or 2 channels
			sampleRate = b.getUint32(12, true) // 0 = unspecified -> 48000
			if (!sampleRate) sampleRate = 48000
		}
		audioDiag.head = { channels, sampleRate }
		// configure() only returns a Promise while the decoder is
		// "unconfigured"; on any other state it returns undefined (or throws),
		// and a closed decoder cannot be reconfigured in place at all. So only
		// configure from the "unconfigured" state (the head is the first
		// message on the channel, so this is a one-time setup); duplicate head
		// messages (the server re-emits the cached head to new subscribers AND
		// fans out the live head) are ignored once configured.
		if (audioDecoder.state !== "unconfigured") return
		audioHeadApplied = true
		const ret = audioDecoder.configure({
			codec: "opus",
			sampleRate,
			numberOfChannels: channels,
		})
		const p = ret && typeof ret.then === "function" ? ret : Promise.resolve()
		p
			.then(() =>
				console.debug&&console.debug(
					"audio: decoder configured from OpusHead:",
					sampleRate + "Hz " + channels + "ch"
				)
			)
			.catch((e) => {
				console.warn("audio: decoder configure failed:", e)
				audioDiag.decodeErr = e ? e.name + ": " + e.message : String(e)
			})
	}
	if (audioHead) applyAudioHead(audioHead) // head already received before decoder existed (rare)

	sock.onopen = () => {
		if (audioReconnectTimer) {
			clearTimeout(audioReconnectTimer)
			audioReconnectTimer = null
		}
	}

		sock.onmessage = (ev) => {
			const pkt = new Uint8Array(ev.data)
			// The server re-emits the OpusHead (prefixed "AUDIOHEAD:") to every
			// subscriber before/alongside audio packets. Detect it, configure the
			// decoder to the stream's ACTUAL channels/rate, and skip the rest of
			// the handler (the head is not an opus packet).
			if (isAudioHeadMsg(pkt)) {
				const head = pkt.subarray(AUDIO_HEAD_PREFIX.length)
				if (head.length >= 19) {
					audioHead = head.slice() // copy: ev.data buffer is reusable
					audioDiag.headSeen++
					if (audioDiag.headSeen === 1) {
						// DIAG: log the first head's raw bytes + parsed values
						// so a config mismatch is visible without guessing.
						const h = audioHead
						const dv = new DataView(h.buffer, h.byteOffset, h.byteLength)
						console.debug&&console.debug(
							"audio: head received:",
							Array.from(h.slice(0, 19)),
							"-> channels=" + h[9] +
								" sampleRate=" + dv.getUint32(12, true)
						)
					}
					applyAudioHead(audioHead)
					return
				}
			}
			// DIAG: record the first 10 packet sizes (a healthy opus stream is
			// ~10-100 bytes per 20ms packet; tiny ~2-4 byte packets mean a silent
			// source; a steady size with no decoder output means the decoder is
			// rejecting them).
			if (audioDiag.pkt < 10) audioDiag.sizes.push(pkt.length)
			audioDiag.pkt++
			if (!audioDecoder || audioDecoder.state !== "configured") return
		// Feed the packet with a monotonic PTS. opusenc emits one packet per
		// 20ms frame, so each packet advances the clock by 20000us.
		//
		// Chunk type: the FIRST frame for a decoder must be type:"key" —
		// Chrome's opus decoder initializes its internal state from it.
		// Feeding the first frame as "delta" makes the decoder reject the
		// ENTIRE stream with a DataError (verified empirically: all-delta ->
		// 0/200 decoded, key-first -> 200/200). Every subsequent frame is
		// type:"delta" (opus frames are independently decodable).
		const chunkType = audioFirstFrame ? "key" : "delta"
		try {
			audioDecoder.decode(
				new EncodedAudioChunk({
					type: chunkType,
					timestamp: audioTs,
					data: pkt,
				})
			)
			// DIAG: count key frames (should be exactly 1 per decoder session;
			// >1 means a decode() threw and the next packet retried as "key").
			if (chunkType === "key") audioDiag.keyFrames++
			// The decoder accepted this chunk; it is no longer the first
			// frame. If decode() threw, audioFirstFrame stays true so the
			// next packet retries as a "key" frame.
			audioFirstFrame = false
			audioTs += 20000
		} catch (e) {
			// DIAG: log the FULL error (name + message). The message tail after
			// "on 'AudioDecoder':" is the actual reason (e.g. "The AudioDecoder
			// is closed." / "not configured."). Also capture it for the 2s
			// summary so it isn't lost in console scroll.
			const msg = e ? (e.name + ": " + e.message) : String(e)
			if (audioDiag.decodeErr !== msg) {
				audioDiag.decodeErr = msg
				console.warn("audio: decode failed:", msg)
			}
		}
	}

	sock.onclose = () => {
		if (!audioEnabled) return // user turned audio off; don't reconnect.
		audioSocket = null
		// Reconnect with a short backoff (single-flight).
		if (audioReconnectTimer) return
		audioReconnectTimer = setTimeout(() => {
			audioReconnectTimer = null
			if (audioEnabled && !closed) ensureAudioPath()
		}, 1000)
	}

	sock.onerror = () => {
		console.warn("audio: socket error")
	}
}

// playAudioData schedules a decoded PCM buffer onto the AudioContext at
// audioNextStartTime and advances the playback clock by the buffer's
// duration. The buffer is a copy (AudioBuffer is immutable once created);
// the decoded chunk is closed after the copy.
function playAudioData(chunk) {
	if (!audioCtx || chunk.numberOfFrames === 0) {
		chunk.close()
		return
	}
	const sampleRate = chunk.sampleRate || 48000
	const numChannels = chunk.numberOfChannels
	const numFrames = chunk.numberOfFrames
	// DIAG: record the first 5 decoded chunks (sampleRate/channels/frames
	// must match the decoder config: 48000/2/960 for opus at 48kHz).
	if (audioDiag.decoded < 5) {
		audioDiag.chunks.push({ sr: sampleRate, ch: numChannels, fr: numFrames })
	}
	// AudioData.copyTo(destination, options) REQUIRES the options argument in
	// Chrome (even though the spec marks it optional). The decoded sample
	// LAYOUT is implementation-defined: "interleaved" (one plane, samples
	// [L0,R0,L1,R1,...]) or "planar" (one plane per channel, each of length
	// numFrames). Handle both, driven by chunk.layout, and log the layout on
	// the first chunk so a wrong assumption is visible without guessing.
	const layout = chunk.layout
	if (audioDiag.decoded === 0) {
		audioDiag.layout = layout
		console.debug&&console.debug(
			"audio: decoded PCM layout:",
			layout,
			"ch=" + numChannels,
			"frames=" + numFrames,
			"sr=" + sampleRate
		)
	}
	// DIAG: check whether the chunk is all-zero (a silent monitor source
	// decodes to all-zero PCM, which plays as silence). The probe must match
	// plane 0's size: interleaved -> all samples (numFrames * numChannels);
	// planar -> the first channel (numFrames).
	{
		const probeLen = layout === "planar" ? numFrames : numFrames * numChannels
		const probe = new Float32Array(probeLen)
		chunk.copyTo(probe, { planeIndex: 0 })
		let nonzero = false
		for (let i = 0; i < probe.length; i++) {
			if (probe[i] !== 0) { nonzero = true; break }
		}
		if (!nonzero) audioDiag.zero++
	}
	audioDiag.decoded++
	// Copy the PCM into an AudioBuffer (the decoded chunk is closed after).
	const buffer = audioCtx.createBuffer(numChannels, numFrames, sampleRate)
	if (layout === "planar") {
		// One plane per channel; each plane is numFrames long.
		for (let ch = 0; ch < numChannels; ch++) {
			const data = new Float32Array(numFrames)
			chunk.copyTo(data, { planeIndex: ch })
			buffer.copyToChannel(data, ch)
		}
	} else {
		// Interleaved: a single plane of length numFrames * numChannels
		// ([L0,R0,L1,R1,...]); de-interleave into the per-channel planes.
		const interleaved = new Float32Array(numFrames * numChannels)
		chunk.copyTo(interleaved, { planeIndex: 0 })
		for (let ch = 0; ch < numChannels; ch++) {
			const data = new Float32Array(numFrames)
			for (let i = 0; i < numFrames; i++) {
				data[i] = interleaved[i * numChannels + ch]
			}
			buffer.copyToChannel(data, ch)
		}
	}
	chunk.close()
	audioDiag.scheduled++
	// Schedule the buffer at audioNextStartTime (or now if the clock is
	// behind, e.g. after a gap).
	const now = audioCtx.currentTime
	if (audioNextStartTime < now) {
		audioNextStartTime = now
	}
	const src = audioCtx.createBufferSource()
	src.buffer = buffer
	src.connect(audioCtx.destination)
	src.start(audioNextStartTime)
	audioNextStartTime += buffer.duration
}

// closeAudioPath tears down the audio path: closes the socket, the decoder,
// and suspends the AudioContext (the context is kept so a re-enable doesn't
// have to recreate it; the button click resumes it).
function closeAudioPath() {
	if (audioDiagTimer) {
		clearInterval(audioDiagTimer)
		audioDiagTimer = null
	}
	if (audioReconnectTimer) {
		clearTimeout(audioReconnectTimer)
		audioReconnectTimer = null
	}
	if (audioSocket) {
		try {
			audioSocket.close()
		} catch (e) {
			// already closed; ignore.
		}
		audioSocket = null
	}
	if (audioDecoder) {
		try {
			audioDecoder.close()
		} catch (e) {
			// already closed; ignore.
		}
		audioDecoder = null
	}
	if (audioCtx) {
		audioCtx.suspend()
	}
}

// startStreamPipeline fetches /stream and appends complete fMP4 boxes to a
// SourceBuffer. The codec string is parsed from the up-front moov (avcC)
// before the SourceBuffer is created, so the decoder config matches the
// actual stream.
//
// Reconnect policy:
// - Clean stream-end (server FIN: the server dropped this slow subscriber
//   or tore down the capture pipeline): re-fetch on the SAME pipeline (same
//   MediaSource/SourceBuffer). The buffer and playhead stay intact, so the
//   reconnect is invisible (no stutter); the new capture pipeline's moov is
//   appended to the existing SourceBuffer in sequence mode.
// - Wedged fetch (no bytes for stallTimeout) or wedged SourceBuffer update
//   (no updateend for stallTimeout): the pipeline is unrecoverable in place
//   (the MediaSource is tied to the dead HTTP connection), so the whole
//   pipeline is rebuilt on a fresh MediaSource via reconnectStream.
const stallTimeout = 2500 // ms without data before the watchdog fires

async function startStreamPipeline(video, url, ms) {
	const controller = new AbortController()
	let lastByte = Date.now()
	let lastUpdateEnd = Date.now()
	let watchdog = null
	// fetchStallReject is the current fetch's stall watchdog reject; the
	// watchdog interval (cleared in finally) checks it.
	let fetchStallReject = null
	// header accumulates bytes until the avcC (inside the up-front moov) is
	// seen, so the SourceBuffer can be created with the correct codec.
	let header = new Uint8Array(0)
	let codec = ""
	let assembler = null
	let sourceBuffer = null
	let pendingBoxes = new Uint8Array(0)

		// assemblerReady is invoked with each complete box group. If the
		// SourceBuffer is not mid-update, append immediately; otherwise queue
		// the group and flush on updateend (appendBuffer while updating
		// throws InvalidStateError).
		const assemblerReady = (boxes) => {
			if (sourceBuffer && !sourceBuffer.updating) {
				sourceBuffer.appendBuffer(boxes)
			} else {
				pendingBoxes = concatUint8(pendingBoxes, boxes)
			}
		}

		const flushPending = () => {
			if (!sourceBuffer || pendingBoxes.length === 0) return
			sourceBuffer.appendBuffer(pendingBoxes)
			pendingBoxes = new Uint8Array(0)
		}

		// Live sync: keep the SourceBuffer bounded to the last maxBuffer
		// seconds so the client stays near the live edge. Without this, the
		// buffer grows as fast as bytes arrive (network bursts, encoder
		// bursts) and the client plays old content, falling behind the live
		// edge. In sequence mode the buffered ranges are relative to the
		// sequence start, so we trim everything before (end - maxBuffer).
		//
		// maxBuffer is small (0.75s) so the steady-state offset from real
		// time stays under ~1s; the live-edge seek below (see syncToLiveEdge)
		// handles the initial attach so the client does not start at t=0.
		const maxBuffer = 0.75 // seconds of video to keep buffered
		const trimBuffer = () => {
			// Guard the WHOLE body: the SourceBuffer can be removed from its
			// MediaSource (a pipeline reconnect closes the old MediaSource)
			// while a stray trimTimer tick or updateend callback is still in
			// flight. Reading .buffered on a removed SourceBuffer throws
			// InvalidStateError, which would be uncaught here if only the
			// remove() call were wrapped. Swallow it: the timer is cleared in
			// the pipeline's finally block, so this is a benign race.
			try {
				if (!sourceBuffer || sourceBuffer.updating) return
				const n = sourceBuffer.buffered.length
				if (n === 0) return
				const end = sourceBuffer.buffered.end(n - 1)
				const start = end - maxBuffer
				if (start <= 0) return
				// Keep the buffer bounded to the live edge, but NEVER advance
				// the buffer start past the playhead. The video starts paused
				// (waiting for the "Click to start" overlay), so while paused
				// the playhead is fixed; trimming past it orphans the playhead
				// behind the buffer start and the element has no data to
				// render (shows only the first frame despite bytes arriving).
				// Clamp the trim point to the playhead so the buffer start
				// never overtakes it. While paused the buffer is therefore
				// bounded to ~maxBuffer ahead of the playhead; if it grows
				// past 2*maxBuffer (the playhead was left behind while
				// paused), advance the playhead to the buffer start and trim
				// normally — at most once per 500ms tick, so the paused
				// preview jumps at most once per second rather than on every
				// fragment. (currentTime can be NaN before any data is
				// buffered; treat that as "no playhead to protect" and trim
				// normally.)
				const playhead = video.currentTime
				if (Number.isFinite(playhead) && start > playhead) {
					if (start - playhead > maxBuffer) {
						// Playhead left behind while paused: advance it to the
						// buffer start (a keyframe, see syncToLiveEdge) and
						// trim normally.
						video.currentTime = start
						sourceBuffer.remove(0, start)
					}
					// else: buffer start is within maxBuffer ahead of the
					// playhead; clamp the trim to the playhead so the buffer
					// start never overtakes it (bounded to ~maxBuffer).
					return
				}
				if (start <= 0) return
				sourceBuffer.remove(0, start)
			} catch (e) {
				// SourceBuffer removed (reconnect race) or a range that does not
				// intersect the buffer (gap); skip this tick.
			}
		}
		let trimTimer = null

		// Live-edge seek: on the first buffered range, seek the playhead near
		// the live edge so playback starts near real time instead of at the
		// buffer start (t=0). The seek happens on updateend (buffered ranges
		// are only valid after the append completes); the init segment
		// (ftyp+moov) carries no duration, so the first real range appears
		// with the first moof+mdat.
		//
		// KEYFRAME ALIGNMENT: the server encodes with frag_keyframe, so every
		// fragment boundary is a keyframe, and the buffered range start
		// (bufferStart) is a fragment boundary — i.e. a keyframe. A seek
		// target that falls MID-GOP (between keyframes) makes the H.264
		// decoder snap back to the previous keyframe and decode the P-frames
		// forward: that reads as a brief "half-speed" catch-up, then a jump
		// to the edge, then a stall waiting for the next keyframe. So we seek
		// to bufferStart itself (a keyframe that is present in the buffer),
		// which is ~maxBuffer (0.75s) behind the live edge — close enough to
		// real time, and guaranteed decodable with no snap-back. This is
		// robust to the actual capture rate (the keyframe interval is fps/-g,
		// which varies), so no GOP-interval assumption is needed.
		let didSync = false
		const syncToLiveEdge = () => {
			try {
				if (!sourceBuffer) return
				const n = sourceBuffer.buffered.length
				if (n === 0) return
				const bufferStart = sourceBuffer.buffered.start(0)
				// Seek to the start of the earliest buffered range: a
				// fragment boundary (keyframe) that is present in the buffer,
				// so the decoder starts cleanly with no snap-back.
				const target = bufferStart
				const playhead = video.currentTime
				// Seek when (a) the initial sync hasn't happened yet, or (b)
				// the playhead has been orphaned behind the buffer start (the
				// trim advanced the start past a stuck playhead). We
				// deliberately do NOT re-seek merely because the video is
				// paused: while paused the playhead is fixed, and re-seeking on
				// every fragment (every updateend) makes the paused preview
				// jump to the edge repeatedly (the "jump to the edge"
				// symptom). The clamped trim below keeps the buffer start at
				// the playhead while paused, so the buffer stays bounded.
				const orphaned = playhead < bufferStart
				if (didSync && !orphaned) return
				video.currentTime = target
				didSync = true
			} catch (e) {
				// SourceBuffer removed (reconnect race) or seek not possible
				// yet; retry on the next updateend.
			}
		}

	let res = null
	let reader = null
	let stallPromise = null
	// gotFirstByte hides the splash on the first chunk of stream data (the
	// "Click to start" overlay becomes visible; the user click is what starts
	// playback per the autoplay policy).
	let gotFirstByte = false
	const markByte = () => {
		lastByte = Date.now()
		if (!gotFirstByte) {
			gotFirstByte = true
			hideSplash()
		}
	}

		// watchdog fires the current fetch's stall reject (fetchStallReject)
		// once no bytes have arrived for stallTimeout. It is raced against
		// reader.read() so a permanently blocked read (the server stopped
		// pushing but the connection stays open) still breaks the loop; a
		// plain flag checked inside the loop would never be evaluated while
		// the read is blocked.
		watchdog = setInterval(() => {
			if (Date.now() - lastByte >= stallTimeout && fetchStallReject) {
				fetchStallReject(new Error("stalled: no data"))
			}
		}, 500)

		try {
			// The outer loop re-fetches on the SAME pipeline (same
			// MediaSource, same SourceBuffer) after a clean stream-end, so
			// the buffer and playhead survive the reconnect (no stutter). A
			// genuine error propagates to the catch block, which rebuilds
			// the pipeline on a fresh MediaSource.
			while (true) {
				const fetchRes = await fetch(url, { signal: controller.signal })
				if (!fetchRes.ok || !fetchRes.body) {
					setStatus("err", `stream ${fetchRes.status}`)
					return
				}
				res = fetchRes
				reader = fetchRes.body.getReader()

				// stallPromise rejects once no bytes have arrived for
				// stallTimeout (see the watchdog above).
				stallPromise = new Promise((_, reject) => {
					fetchStallReject = reject
				})

				// Read loop: stream bytes through the assembler until the
				// stream ends (server FIN) or the stall watchdog fires.
				while (true) {
					// Race the read against the stall watchdog: if no bytes
					// arrive for stallTimeout, stallPromise rejects and
					// breaks the loop even if reader.read() itself is
					// blocked forever.
					const { done, value } = await Promise.race([reader.read(), stallPromise])
					if (done) break
					markByte()
					// Performance overlay: count the received bytes (bandwidth metric).
					if (perfOn) perfBytes += value.length
					// A SourceBuffer update wedged (still updating) for
					// stallTimeout means the append pipeline is frozen; rebuild
					// it on a fresh MediaSource. (Checked here because bytes
					// are still arriving, so the loop is running.)
					if (sourceBuffer && sourceBuffer.updating && Date.now() - lastUpdateEnd >= stallTimeout) {
						console.warn("stream watchdog: updateend stalled for", stallTimeout, "ms; rebuilding pipeline")
						throw new Error("stalled: no data")
					}
					if (sourceBuffer === null) {
						// Still in the header phase: buffer bytes until the
						// avcC (inside the up-front moov) is seen, so the
						// SourceBuffer can be created with the correct codec.
						header = concatUint8(header, value)
						codec = findAvcCCodec(header)
						if (codec) {
							// Create the SourceBuffer now that the codec is
							// known.
							const mime = `video/mp4;codecs="${codec}"`
							if (!ms.addSourceBuffer(mime)) {
								setStatus("err", `MSE rejects ${mime}`)
								return
							}
							sourceBuffer = ms.sourceBuffers[0]
							sourceBuffer.mode = "sequence"
							sourceBuffer.addEventListener("updateend", () => {
								lastUpdateEnd = Date.now()
								flushPending()
								syncToLiveEdge()
							})
							assembler = new Fmp4Assembler(assemblerReady)
							// Start the periodic live-edge trim.
							trimTimer = setInterval(trimBuffer, 500)
							// Feed the buffered header (ftyp + moov) through
							// the assembler so the init segment is appended.
							assembler.feed(header)
							header = new Uint8Array(0)
						}
					} else {
						// SourceBuffer exists (a re-fetch after a clean
						// stream-end): the new capture pipeline emits a fresh
						// moov that is appended to the SAME SourceBuffer in
						// sequence mode, continuing the timeline. Feed all
						// bytes through the assembler; the new moov is
						// handled as an init segment.
						assembler.feed(value)
					}
				}
				// Inner loop exited: the server cleanly closed the connection
				// (stream FIN). This is the normal outcome when the server
				// drops a slow subscriber or tears down the capture pipeline.
				// The server is alive and will re-push, so re-fetch on the
				// SAME pipeline (same MediaSource/SourceBuffer — the buffer
				// and playhead stay intact, no visible stutter) rather than
				// tearing down the MediaSource.
				lastByte = Date.now()
				fetchStallReject = null
				try {
					reader.releaseLock()
				} catch (e) {
					// reader already released; ignore.
				}
				console.debug&&console.debug("stream ended (server FIN); re-fetching on the same pipeline")
			}
		} catch (e) {
			console.error("stream pipeline failed:", e)
			// A wedged fetch/SourceBuffer throws "stalled: no data"; abort
			// the fetch and restart the pipeline on a fresh MediaSource,
			// which reconnects the HTTP stream and reconfigures the decoder
			// from the new moov. A genuine failure (bad status, MSE error)
			// is surfaced. A clean stream-end never reaches here: the outer
			// loop re-fetches on the same pipeline instead.
			if (e.message === "stalled: no data") {
				controller.abort()
				reconnectStream(video)
			} else {
				setStatus("err", "stream failed")
			}
		} finally {
			if (trimTimer) clearInterval(trimTimer)
			if (watchdog) clearInterval(watchdog)
			fetchStallReject = null
		}
	}

// reconnectStream restarts the stream pipeline on a fresh MediaSource.
// Used by the stall watchdog: a wedged fetch or a wedged SourceBuffer update
// is unrecoverable in place (the MediaSource is tied to the dead HTTP
// connection), so the whole pipeline is rebuilt. attachVideo closes and
// revokes the old MediaSource and re-points the <video> element at a new
// one; the live-edge seek repositions the playhead on attach.
function reconnectStream(video) {
	setStatus("connecting", "reconnecting stream...")
	// Defer so the abort propagates before the new fetch starts.
	setTimeout(() => attachVideo(), 500)
}

// wireStartOverlay binds the "Click to start" overlay. In fMP4 mode the click
// handler runs inside a user gesture, which satisfies Chrome's autoplay policy,
// so video.play() is allowed from within it. In raw mode the overlay is hidden
// immediately: the WebCodecs/canvas path needs no autoplay gesture (the canvas
// renders decoded frames as they arrive, no play() call).
//
// Must be called after the video path is chosen: in fMP4 mode after
// attachVideo() so video.src is set (play() on an empty src rejects).
//
// It is safe to call more than once: the raw branch just toggles the hidden
// class, and the fMP4 branch removes the hidden class (added during raw mode)
// and binds the click handler exactly once (startOverlayWired guards against a
// double bind).
let startOverlayWired = false
function wireStartOverlay() {
	const start = document.getElementById("start")
	if (mode === "raw") {
		// Canvas path: no gesture needed; hide the overlay so the live canvas
		// is visible.
		start.classList.add("hidden")
		return
	}
	// fMP4 path: show the overlay (it may have been hidden during an earlier
	// raw mode) and bind the click->play handler exactly once.
	start.classList.remove("hidden")
	if (startOverlayWired) return
	startOverlayWired = true
	const video = document.getElementById("video")
	start.addEventListener("click", () => {
		start.classList.add("hidden")
		video.play().catch((e) => {
			start.classList.remove("hidden")
			console.error("play failed:", e)
		})
	})
}

// fetchToken asks the server for the bearer token so the /stream URL can carry it.
async function fetchToken() {
	try {
		const res = await fetch("/health")
		if (!res.ok) return ""
		const data = await res.json()
		const token = data.token || ""
		if (token) localStorage.setItem("screenrelay_token", token)
		return token
	} catch (e) {
		return ""
	}
}

// getToken returns the persisted bearer token (or one fetched from /health).
async function getToken() {
	let token = localStorage.getItem("screenrelay_token")
	if (!token) token = await fetchToken()
	return token
}

// openSocket establishes the WebSocket control channel (ping/pong + input).
async function openSocket() {
	const token = await fetchToken()
	const protocol = location.protocol === "https:" ? "wss" : "ws"
	const url = `${protocol}://${location.host}/relay${token ? `?token=${encodeURIComponent(token)}` : ""}`
	const sock = new WebSocket(url)
	sock.binaryType = "arraybuffer"

	sock.onopen = () => {
		setStatus("ok", "connected")
		if (reconnectTimer) {
			clearTimeout(reconnectTimer)
			reconnectTimer = null
		}
	}

	sock.onmessage = (ev) => {
		const buf = new Uint8Array(ev.data)
		if (buf.length < 1) return
		switch (buf[0]) {
			case 70: // 'F' - legacy frame frame, ignored (video comes over /stream).
				break
			case 71: // 'G' pong
				break
			case 84: // 'T' settings ack - sync local state + reconfigure the pipeline.
				handleSettingsAck(buf.subarray(1))
				break
			default:
				break
		}
	}

	sock.onclose = () => {
		setStatus("err", "disconnected - reconnecting...")
		if (!closed) scheduleReconnect()
	}

	sock.onerror = () => {
		setStatus("err", "error")
	}

	ws = sock
}

function scheduleReconnect() {
	if (reconnectTimer) return
	reconnectTimer = setTimeout(() => {
		reconnectTimer = null
		openSocket()
	}, 1000)
}

// Temporary diagnostic: confirm the window-level input handlers fire and what
// state the /relay socket is in. sendInput is a silent no-op when ws isn't
// OPEN, so this reveals whether events are being dropped (ws=CONNECTING/CLOSED)
// vs actually sent (ws=OPEN). Throttled to one line per second so mousemove
// spam doesn't flood the console. Remove once input is confirmed working.
let lastInputDiag = 0
const wsStateName = ["CONNECTING", "OPEN", "CLOSING", "CLOSED"]
function inputDiag(type) {
	const now = Date.now()
	if (now - lastInputDiag < 1000) return
	lastInputDiag = now
	const state = ws ? wsStateName[ws.readyState] + "(" + ws.readyState + ")" : "null"
	console.debug&&console.debug("[input] " + type + " (ws=" + state + ")")
}

function sendInput(type, x, y, key, delta, button) {
	inputDiag(type)
	if (!ws || ws.readyState !== WebSocket.OPEN) return
	const msg = { type, x, y, key, delta, button }
	const buf = new TextEncoder().encode(JSON.stringify(msg))
	ws.send(new Uint8Array([73 /* 'I' */, ...buf]))
}

// --- Pointer-lock "capture input" mode (Phase 1) ---
//
// By default the client forwards ABSOLUTE pointer positions (normalized [0,1]
// within the visible video element), which the server maps to an absolute
// screen position. Capture mode (opt-in) switches to RELATIVE movement: the
// pointer is locked to the video element (requestPointerLock), the local
// cursor is hidden, and pointer motion is forwarded as pointer-lock deltas
// (movementX/movementY) that the server scales and emits as REL_X/REL_Y. This
// feels like a normal mouse (the remote cursor moves 1:1 with the local
// pointer, without the local cursor being clamped to the video element's
// edges) and is the natural mode for interactive use (games, drawing, etc.).
//
// Toggle: Ctrl+Shift+L (intercepted BEFORE the key is forwarded to the host,
// so the toggle key never reaches the remote machine). While locked, the
// browser's native Escape releases the pointer lock (unpreventable); that
// Escape is swallowed (not forwarded) so the host does not receive a stray
// Escape keypress from the "release capture" gesture. Re-locking requires a
// fresh Ctrl+Shift+L (the browser enforces a ~1.25s cooldown after an Escape
// release, so an immediate re-lock is rejected by the browser - by design).
let captureMode = false
let captureEl = null
// --- Settings (quality/fps/scale/monitor) ---
// The client-side view of the capture parameters. The server is the source of
// truth: it acks each kindSettings message with the effective values
// (kindSettingsAck), which the client syncs here. The bar buttons cycle the
// local value and send it; the ack confirms (or corrects) it.
const settings = {
	quality: "med", // "low" (3200/7200), "med" (5000/15000), "high" (7200/21000)
	fps: 60, // 15/24/30/45/60
	scale: 1.0, // 0.5-1.0
	monitor: "" // xrandr output name; "" = primary
}
const QUALITY_CYCLE = ["low", "med", "high"]
const FPS_CYCLE = [15, 24, 30, 45, 60]
const SCALE_CYCLE = [1.0, 0.75, 0.5]
// monitors is the /monitors list (cached); the display button cycles through
// it ("" = primary first).
let monitors = [""]
let monitorIdx = 0

// sendSettings sends a kindSettings ('S') message over the control channel.
// The payload is a partial settingsMsg: only the provided fields are applied
// (the server merges over the current parameters), so a single knob can be
// changed without resending the whole configuration.
function sendSettings(obj) {
	if (!ws || ws.readyState !== WebSocket.OPEN) return
	const buf = new TextEncoder().encode(JSON.stringify(obj))
	ws.send(new Uint8Array([83 /* 'S' */, ...buf]))
}

// handleSettingsAck processes a kindSettingsAck ('T') message: the server's
// confirmation of an applied settings change, carrying the effective values.
// The client syncs its local settings state to the acked values and
// reconfigures the video pipeline if the encoder changed (a new SPS/PPS or a
// fresh moov is required to decode the reconfigured stream):
//   raw  - the raw socket stays open across the capture restart (the
//          broadcaster persists), so only the decoder is reset: the next
//          SPS/PPS-carrying access unit reconfigures it (rawConfigured=false).
//   fmp4 - the SourceBuffer's codec (avcC) is fixed at creation, so the whole
//          pipeline is rebuilt on a fresh MediaSource (reconnectStream).
function handleSettingsAck(payload) {
	let s
	try {
		s = JSON.parse(new TextDecoder().decode(payload))
	} catch (e) {
		return
	}
	if (s.fps) settings.fps = s.fps
	if (s.scale) settings.scale = s.scale
	if (s.monitor !== undefined) settings.monitor = s.monitor
	if (mode === "raw") {
		// Reset the decoder so the next SPS/PPS reconfigures it. The raw socket
		// stays open (the broadcaster persists across the capture restart).
		rawConfigured = false
		rawConfigFailures = 0
		rawInFlight = 0
		try {
			if (decoder) decoder.close()
		} catch (e) {
			// already closed; ignore.
		}
		decoder = null
		rawFrameIndex = 0
		dbgFrames.length = 0
		dbgPendingArrivals.length = 0
		prevBlit = 0
	} else if (mode === "fmp4") {
		// Rebuild the fMP4 pipeline on a fresh MediaSource (the new capture
		// emits a fresh moov that the SourceBuffer must be created from).
		reconnectStream(document.getElementById("video"))
	}
}

// --- Splash screen ---
// splashDone is set once the splash has faded out. hideSplash() is idempotent:
// it is called from the first raw blit (the first decoded frame is visible)
// and from the first fMP4 stream byte (the "Click to start" overlay becomes
// visible; the user click is what starts playback per the autoplay policy),
// and only the first call does anything.
let splashDone = false
function hideSplash() {
	if (splashDone) return
	splashDone = true
	const s = document.getElementById("splash")
	if (!s) return
	s.classList.add("hidden")
	// Remove from the DOM after the fade-out transition so it stops intercepting
	// pointer events (the .hidden class sets opacity:0; the element would
	// otherwise still cover the stage).
	setTimeout(() => {
		s.style.display = "none"
	}, 700)
}

// --- Performance overlay ---
// The performance overlay (bottom-left) shows the current FPS and bandwidth
// (kbps) with a 30-second sparkline for each. Both are measured client-side:
//   raw  - frames = blitLatest() count, bytes = /raw onmessage length.
//   fmp4 - frames = <video> requestVideoFrameCallback count, bytes = the
//          /stream read-loop chunk length.
// A 1-second sampler (perfTimer) aggregates the counters into per-second
// samples (fpsHist/bwHist, 30 samples = 30s) and redraws the sparklines.
const PERF_SAMPLES = 30 // 30 samples x 1s = 30s window
let perfTimer = null
let perfOn = false
let perfFrames = 0 // frames since the last sample (raw: blits, fmp4: rVFC)
let perfBytes = 0 // bytes since the last sample
let fpsHist = [] // per-second FPS samples (oldest first)
let bwHist = [] // per-second kbps samples (oldest first)
let perfFpsEl = null
let perfBwEl = null
let perfFpsCanvas = null
let perfBwCanvas = null
let perfFpsCtx = null
let perfBwCtx = null

// perfSample runs once per second: fold the counters into a sample, push it
// (capped at PERF_SAMPLES), and redraw both sparklines.
function perfSample() {
	const fps = perfFrames
	const kbps = perfBytes * 8 / 1000
	perfFrames = 0
	perfBytes = 0
	fpsHist.push(fps)
	bwHist.push(kbps)
	if (fpsHist.length > PERF_SAMPLES) fpsHist.shift()
	if (bwHist.length > PERF_SAMPLES) bwHist.shift()
	if (perfFpsEl) perfFpsEl.textContent = String(fps)
	if (perfBwEl) perfBwEl.textContent = kbps.toFixed(0)
	drawSparkline(perfFpsCtx, fpsHist, "#3fb950")
	drawSparkline(perfBwCtx, bwHist, "#58a6ff")
}

// drawSparkline paints a 30-sample sparkline (one column per second) on a
// 120x24 canvas: a filled area under a line, scaled to the window's max.
// A flat zero window draws a single baseline dot so the overlay reads as
// "no data" rather than an empty box.
function drawSparkline(ctx, hist, color) {
	const w = ctx.canvas.width
	const h = ctx.canvas.height
	ctx.clearRect(0, 0, w, h)
	if (!hist.length) return
	let max = 1
	for (const v of hist) if (v > max) max = v
	const n = hist.length
	const step = w / PERF_SAMPLES
	ctx.beginPath()
	for (let i = 0; i < n; i++) {
		const x = (PERF_SAMPLES - n + i) * step
		const y = h - 2 - (hist[i] / max) * (h - 4)
		if (i === 0) ctx.moveTo(x + step / 2, y)
		else ctx.lineTo(x + step / 2, y)
	}
	ctx.strokeStyle = color
	ctx.lineWidth = 1.5
	ctx.stroke()
	// Fill under the line.
	ctx.lineTo((PERF_SAMPLES - 1) * step + step / 2, h - 1)
	ctx.lineTo((PERF_SAMPLES - n) * step + step / 2, h - 1)
	ctx.closePath()
	ctx.globalAlpha = 0.25
	ctx.fillStyle = color
	ctx.fill()
	ctx.globalAlpha = 1
}

// rVFC loop handle for fMP4-mode frame counting (requestVideoFrameCallback is
// only available on the <video> element; the raw path counts blits instead).
let rVFCActive = false
function rVFCLoop() {
	if (!rVFCActive) return
	perfFrames++
	const video = document.getElementById("video")
	if (video && typeof video.requestVideoFrameCallback === "function") {
		video.requestVideoFrameCallback(() => rVFCLoop())
	}
}

// wirePerfOverlay starts/stops the 1-second performance sampler and shows/hides
// the overlay. It is toggled from the settings bar's performance button. In
// raw mode frames are counted in blitLatest(); in fMP4 mode a
// requestVideoFrameCallback loop counts <video> presentation frames.
function wirePerfOverlay() {
	const el = document.getElementById("perf")
	perfFpsEl = document.getElementById("perf-fps-val")
	perfBwEl = document.getElementById("perf-bw-val")
	perfFpsCanvas = document.getElementById("perf-fps-graph")
	perfBwCanvas = document.getElementById("perf-bw-graph")
	perfFpsCtx = perfFpsCanvas.getContext("2d")
	perfBwCtx = perfBwCanvas.getContext("2d")
	const btn = document.getElementById("btn-perf")
	btn.addEventListener("click", () => {
		if (perfOn) {
			perfOn = false
			el.classList.add("hidden")
			btn.classList.remove("active")
			clearInterval(perfTimer)
			perfTimer = null
			rVFCActive = false
		} else {
			perfOn = true
			el.classList.remove("hidden")
			btn.classList.add("active")
			perfTimer = setInterval(perfSample, 1000)
			if (mode === "fmp4") {
				rVFCActive = true
				rVFCLoop()
			}
		}
	})
}

// wireSettingsBar wires the bottom-right settings bar: capture (pointer-lock
// toggle), quality/fps/scale (cycle + send), display (cycle /monitors), audio
// (disabled until Phase 4), fullscreen, and performance overlay. Each
// quality/fps/scale/display click updates the local settings state, updates
// the button's label, and sends a kindSettings message; the server's ack
// (handleSettingsAck) confirms the effective values.
function wireSettingsBar() {
	const label = document.getElementById("bar-label")
	const showLabel = (text) => {
		label.textContent = text
		label.classList.add("show")
		clearTimeout(showLabel.t)
		showLabel.t = setTimeout(() => label.classList.remove("show"), 1500)
	}
	// Capture: toggle pointer-lock capture mode (reuses the Ctrl+Shift+L path).
	const btnCapture = document.getElementById("btn-capture")
	btnCapture.addEventListener("click", () => {
		if (captureMode) exitCaptureMode()
		else enterCaptureMode()
	})
	document.addEventListener("pointerlockchange", () => {
		btnCapture.classList.toggle("active", captureMode)
	})
	// Quality: cycle low -> med -> high and send the preset.
	const btnQuality = document.getElementById("btn-quality")
	btnQuality.addEventListener("click", () => {
		const i = QUALITY_CYCLE.indexOf(settings.quality)
		settings.quality = QUALITY_CYCLE[(i + 1) % QUALITY_CYCLE.length]
		showLabel("quality: " + settings.quality)
		sendSettings({ quality: settings.quality })
	})
	// FPS: cycle 15 -> 24 -> 30 -> 45 -> 60 and send.
	const btnFps = document.getElementById("btn-fps")
	btnFps.addEventListener("click", () => {
		const i = FPS_CYCLE.indexOf(settings.fps)
		settings.fps = FPS_CYCLE[(i + 1) % FPS_CYCLE.length]
		showLabel("fps: " + settings.fps)
		sendSettings({ fps: settings.fps })
	})
	// Scale: cycle 1.0 -> 0.75 -> 0.5 and send.
	const btnScale = document.getElementById("btn-scale")
	btnScale.addEventListener("click", () => {
		const i = SCALE_CYCLE.indexOf(settings.scale)
		settings.scale = SCALE_CYCLE[(i + 1) % SCALE_CYCLE.length]
		showLabel("scale: " + settings.scale.toFixed(2))
		sendSettings({ scale: settings.scale })
	})
	// Display: cycle the /monitors list ("" = primary first) and send the
	// xrandr output name. The list is fetched once (cached); a failure leaves
	// the list as [""] (primary only).
	const btnDisplay = document.getElementById("btn-display")
	const updateDisplayLabel = () => {
		const name = monitors[monitorIdx]
		showLabel(name ? "display: " + name : "display: primary")
	}
	fetchMonitors()
	btnDisplay.addEventListener("click", () => {
		if (monitors.length < 2) return // only primary; nothing to cycle.
		monitorIdx = (monitorIdx + 1) % monitors.length
		settings.monitor = monitors[monitorIdx]
		updateDisplayLabel()
		sendSettings({ monitor: settings.monitor })
	})
	// Audio: toggle the /audio relay (opus over WS -> WebCodecs AudioDecoder
	// -> AudioContext). The AudioContext is created inside the click handler
	// (a user gesture) so autoplay policy is satisfied.
	const btnAudio = document.getElementById("btn-audio")
	if (btnAudio) {
		btnAudio.addEventListener("click", () => {
			toggleAudio().then(() => {
				showLabel(audioEnabled ? "audio: on" : "audio: off")
			})
		})
	}
	// Fullscreen: toggle the active video element's fullscreen.
	const btnFull = document.getElementById("btn-fullscreen")
	btnFull.addEventListener("click", () => {
		const el = activeVideoEl()
		if (!el) return
		if (document.fullscreenElement) {
			try {
				document.exitFullscreen()
			} catch (e) {
				// ignore
			}
		} else if (typeof el.requestFullscreen === "function") {
			el.requestFullscreen().catch((e) => console.warn("fullscreen:", e))
		}
	})
	document.addEventListener("fullscreenchange", () => {
		btnFull.classList.toggle("active", !!document.fullscreenElement)
	})
	// Performance overlay: toggle the 1-second FPS/bandwidth sampler.
	wirePerfOverlay()
}

// fetchMonitors fetches the /monitors list (xrandr outputs) and caches it for
// the display-cycling button. A failure (xrandr unavailable, auth) leaves the
// list as [""] (primary only) so the button is a harmless no-op.
async function fetchMonitors() {
	try {
		const token = await getToken()
		const params = new URLSearchParams()
		if (token) params.set("token", token)
		const res = await fetch(`/monitors${params.toString() ? `?${params.toString()}` : ""}`)
		if (!res.ok) return
		const list = await res.json()
		if (Array.isArray(list) && list.length > 0) {
			monitors = ["", ...list.map((m) => m.name)]
			// Clamp the index into the new list.
			if (monitorIdx >= monitors.length) monitorIdx = 0
		}
	} catch (e) {
		// xrandr unavailable or network error; primary-only.
	}
}

// escapeSwallowUntil suppresses forwarding an Escape keypress to the host for
// this many ms after the pointer lock is released (so the "release capture"
// Escape does not also press Escape on the remote machine).
let escapeSwallowUntil = 0

// activeVideoEl returns the element currently showing the remote desktop: the
// <canvas> in raw mode, the <video> in fMP4 mode. Pointer lock is requested
// on this element (the one the user is looking at).
function activeVideoEl() {
	return document.getElementById("stage")
	// using "stage" as the fullscreen element, beaucse it allows
	// us to keep the control and status bar's present
	// return mode === "raw"
	// 	? document.getElementById("canvas")
	// 	: document.getElementById("video")
}

// enterCaptureMode requests pointer lock on the active video element. A
// pointer-lock request MUST originate from a user gesture (browser security
// policy), so this is called from the Ctrl+Shift+L keydown handler (a gesture).
// The lock actually engages asynchronously (pointerlockchange fires); the
// capture-mode state is set from that event, not here, so a rejected request
// (e.g. the ~1.25s post-Escape cooldown) does not leave captureMode stuck on.
function enterCaptureMode() {
	const el = activeVideoEl()
	if (!el || typeof el.requestPointerLock !== "function") return
	try {
		// requestPointerLock may return a Promise (Chrome) that rejects on a
		// cooldown/permission failure; swallow it (the pointerlockchange/
		// pointerlockerror events are the source of truth).
		const r = el.requestPointerLock()
		if (r && typeof r.catch === "function") r.catch((e) => console.warn("capture: pointer lock request rejected:", e))
	} catch (e) {
		console.warn("capture: requestPointerLock threw:", e)
	}
}

// exitCaptureMode releases the pointer lock (the browser fires
// pointerlockchange, which clears captureMode).
function exitCaptureMode() {
	if (document.pointerLockElement) {
		try {
			document.exitPointerLock()
		} catch (e) {
			// already unlocked; ignore.
		}
	}
}

// wirePointerLock binds the pointer-lock state to captureMode. The browser
// fires pointerlockchange whenever the lock engages or releases (including a
// native Escape release, which we cannot prevent). We mirror that state into
// captureMode so the mousemove handler knows whether to send relative or
// absolute events.
function wirePointerLock() {
	document.addEventListener("pointerlockchange", () => {
		const locked = document.pointerLockElement !== null
		captureMode = locked
		captureEl = locked ? document.pointerLockElement : null
		// Reflect the capture state in the status bar so the user knows when
		// the pointer is locked (cursor hidden, relative movement active).
		const hint = document.getElementById("hint")
		if (hint) hint.textContent = locked ? "Screen Relay — capture mode (Ctrl+Shift+L to release)" : "Screen Relay"
		if (!locked) {
			// The lock just released. If it was released by a native Escape
			// (the common case), swallow the Escape that caused it so the host
			// does not receive a stray Escape keypress. We cannot distinguish
			// an Escape release from an exitPointerLock() release here, so we
			// always swallow the next Escape for a short window; a deliberate
			// host Escape within that ~600ms window is an acceptable trade-off
			// (the user is in the middle of a capture-mode transition).
			escapeSwallowUntil = Date.now() + 600
		}
	})
	document.addEventListener("pointerlockerror", (e) => {
		console.warn("capture: pointer lock error:", e)
	})
}

// normXY converts a client pointer position to normalized [0,1] coordinates
// within the VISIBLE video element (the <canvas> in raw mode, the <video> in
// fMP4 mode). The server scales these to the full screen resolution (the
// streamed video is a scaled-down version of the full screen, so a normalized
// position in the video maps to the full screen, not the stream resolution).
function normXY(e) {
	const stage = document.getElementById("stage")
	const el = (mode === "raw" ? document.getElementById("canvas") : document.getElementById("video")) || stage
	const r = el.getBoundingClientRect()
	if (!r.width || !r.height) return [0, 0]
	const x = (e.clientX - r.left) / r.width
	const y = (e.clientY - r.top) / r.height
	return [Math.max(0, Math.min(1, x)), Math.max(0, Math.min(1, y))]
}

// Phase 4: capture input events. Pointer positions are sent normalized
// [0,1] (within the visible video element); keys are sent as the JS
// KeyboardEvent.code (e.g. "KeyA", "Enter", "ShiftLeft") so the server can
// map them to Linux EV_KEY codes; wheel is sent as a normalized scroll notch.
window.addEventListener("keydown", (e) => {
	// Ctrl+Shift+L toggles pointer-lock "capture input" mode. Intercepted
	// BEFORE the key is forwarded to the host, so the toggle combination never
	// reaches the remote machine (the host never sees a Ctrl+Shift+L press).
	if (e.ctrlKey && e.shiftKey && e.code === "KeyL") {
		e.preventDefault()
		e.stopPropagation()
		if (captureMode) exitCaptureMode()
		else enterCaptureMode()
		return
	}
	// While releasing capture mode via a native Escape, swallow the Escape so
	// the host does not receive a stray Escape keypress from the "release
	// capture" gesture. The swallow window is set by the pointerlockchange
	// handler when the lock releases.
	if (e.code === "Escape" && Date.now() < escapeSwallowUntil) {
		e.preventDefault()
		e.stopPropagation()
		return
	}
	sendInput("keydown", 0, 0, e.code)
	// Prevent browser shortcuts (Ctrl+Tab, etc.) from firing while the remote
	// session is active.
	if (e.code !== "F5" && e.code !== "F12" && !(e.ctrlKey && e.shiftKey)) e.preventDefault()
})
window.addEventListener("keyup", (e) => {
	// Mirror the keydown interception: do not forward the release of the
	// Ctrl+Shift+L toggle combo (the host never saw the press) or the release
	// of a swallowed Escape (the host never saw the press).
	if (e.ctrlKey && e.shiftKey && e.code === "KeyL") {
		e.preventDefault()
		e.stopPropagation()
		return
	}
	if (e.code === "Escape" && Date.now() < escapeSwallowUntil) {
		e.preventDefault()
		e.stopPropagation()
		return
	}
	sendInput("keyup", 0, 0, e.code)
	if (e.code !== "F5" && e.code !== "F12" && !(e.ctrlKey && e.shiftKey)) e.preventDefault()
})
window.addEventListener("mousedown", (e) => {
	const [x, y] = normXY(e)
	sendInput("mousedown", x, y, "", 0, e.button)
})
window.addEventListener("mouseup", (e) => {
	const [x, y] = normXY(e)
	sendInput("mouseup", x, y, "", 0, e.button)
})
window.addEventListener("mousemove", (e) => {
	if (captureMode) {
		// Pointer-lock mode: forward the pointer-lock deltas (movementX/Y,
		// CSS pixels) as a relative movement. The server scales them by its
		// relSensitivity and emits REL_X/REL_Y. movementX/Y are only
		// meaningful while the pointer is locked (the browser zeroes them
		// otherwise), so this path is only reached when captureMode is set.
		sendInput("relmousemove", e.movementX, e.movementY)
		return
	}
	const [x, y] = normXY(e)
	sendInput("mousemove", x, y)
})
// wireWheel binds the scroll handler to the STAGE element (the video/canvas
// container), not `window`. The previous window-scoped handler forwarded ANY
// page scroll (e.g. the user scrolling the local page while the cursor was
// over the status bar or a margin) to the host as a REL_WHEEL event - the
// "wheel triggering on host" symptom. Scoping to the stage means only scrolls
// over the remote desktop surface reach the host. In pointer-lock (capture)
// mode the locked element is the stage's child, so wheel events still bubble
// to the stage handler and the remote page scrolls as expected.
//
// The delta is normalized to a scroll "notch" (deltaMode 0 = pixels, ~100px
// per notch; deltaMode 1 = lines, ~3 per notch) so the host receives a
// consistent magnitude regardless of the client's wheel granularity. The
// server amplifies each notch by wheelUnitsPerNotch (env-tunable) REL_WHEEL
// units.
function wireWheel() {
	const stage = document.getElementById("stage")
	stage.addEventListener("wheel", (e) => {
		const notch = e.deltaMode === 1 ? Math.round(e.deltaY / 3) : Math.round(e.deltaY / 100)
		if (notch !== 0) {
			sendInput("wheel", 0, 0, "", -notch)
			e.preventDefault()
		}
	}, { passive: false })
}

// selectMode picks the video path: ?mode=fmp4 forces the fMP4/MSE fallback;
// otherwise the raw WebCodecs path is the default when VideoDecoder is
// available (Chrome 94+), and the fMP4 path is used when it is not.
function selectMode() {
	const params = new URLSearchParams(location.search)
	if (params.get("mode") === "fmp4") {
		mode = "fmp4"
		return
	}
	if (params.get("mode") === "raw") {
		mode = "raw"
		return
	}
	// Default: raw when WebCodecs is available, fMP4 otherwise.
	mode = typeof VideoDecoder !== "undefined" ? "raw" : "fmp4"
}

// Bootstrap: select the video path (raw WebCodecs default, fMP4 fallback),
// attach the chosen video stream, wire the click-to-start overlay, then open
// the control socket.
;(async function bootstrap() {
	setStatus("connecting", "connecting...")
	selectMode()
	// Show the active surface: raw mode hides <video> (canvas shows by
	// default); fMP4 mode hides <canvas> (<video> shows by default).
	document.getElementById("stage").classList.add(mode === "raw" ? "raw-mode" : "fmp4-mode")
	if (mode === "raw") {
		ensureRawPath()
	} else {
		await attachVideo()
	}
	wireStartOverlay()
	wireDebugCopy()
	wirePointerLock()
	wireWheel()
	wireSettingsBar()
	await openSocket()
})()
 