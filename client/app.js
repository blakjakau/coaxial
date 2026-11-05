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
// rawInFlight = frames submitted to decoder.decode() but not yet returned via
// the output() callback (self-tracked, because VideoDecoder exposes no
// frameQueueSize - that is an MSE SourceBuffer property, not a WebCodecs one).
// A steady rawInFlight ≈ N means the decoder is running ~N frames behind live:
// the ~260ms decode latency = rawInFlight x frame interval. This is the metric
// that tracks the pipeline depth while tuning res/fps/bitrate.
let rawInFlight = 0
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
let debugCopyWired = false
function wireDebugCopy() {
	if (debugCopyWired) return
	debugCopyWired = true
	const c = document.getElementById("canvas")
	c.addEventListener("click", () => {
		const report = buildDebugReport()
		console.log("raw debug (last " + DBG_SHOW + " frames):\n" + report)
		if (navigator.clipboard && navigator.clipboard.writeText) {
			navigator.clipboard.writeText(report).then(
				() => console.log("raw debug copied to clipboard"),
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
			console.log(codec)
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
				drawDebugOverlay(ctx, w, h)
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
			console.log("raw: decoder configured:", codec)
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
		const markByte = () => {
			lastByte = Date.now()
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
				console.log("stream ended (server FIN); re-fetching on the same pipeline")
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
	console.log("[input] " + type + " (ws=" + state + ")")
}

function sendInput(type, x, y, key, delta, button) {
	inputDiag(type)
	if (!ws || ws.readyState !== WebSocket.OPEN) return
	const msg = { type, x, y, key, delta, button }
	const buf = new TextEncoder().encode(JSON.stringify(msg))
	ws.send(new Uint8Array([73 /* 'I' */, ...buf]))
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
	sendInput("keydown", 0, 0, e.code)
	// Prevent browser shortcuts (Ctrl+Tab, etc.) from firing while the remote
	// session is active.
	if (e.code !== "F5" && e.code !== "F12" && !(e.ctrlKey && e.shiftKey)) e.preventDefault()
})
window.addEventListener("keyup", (e) => {
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
	const [x, y] = normXY(e)
	sendInput("mousemove", x, y)
})
window.addEventListener("wheel", (e) => {
	// Normalize the wheel delta to a scroll "notch" (deltaMode 0 = pixels,
	// ~100px per notch; deltaMode 1 = lines, ~3 per notch). The server
	// forwards it as a REL_WHEEL event.
	const notch = e.deltaMode === 1 ? Math.round(e.deltaY / 3) : Math.round(e.deltaY / 100)
	if (notch !== 0) {
		sendInput("wheel", 0, 0, "", notch)
		e.preventDefault()
	}
}, { passive: false })

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
	await openSocket()
})()
