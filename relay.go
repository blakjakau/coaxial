package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
)

// messageKind identifies the type of a framed message flowing over the relay
// control channel.
//
// The wire format is: [1 byte kind][payload...].
type messageKind byte

const (
	// kindInput is a key/mouse event from browser -> server.
	kindInput messageKind = 'I'
	// kindPing is a keepalive probe; the server echoes kindPong.
	kindPing messageKind = 'P'
	// kindPong is a keepalive reply.
	kindPong messageKind = 'G'
	// kindSettings is a capture/encode configuration change from browser ->
	// server (quality/fps/scale/monitor). The server validates the payload,
	// applies it to rs.curParams and restarts the capture pipeline with the
	// new values, then replies with kindSettingsAck.
	kindSettings messageKind = 'S'
	// kindSettingsAck is the server's acknowledgement of a kindSettings
	// message (applied successfully). The payload is the settingsMsg JSON
	// that was applied, so the client can confirm the effective values.
	kindSettingsAck messageKind = 'T'
)

// settingsMsg is the JSON payload of a kindSettings ('S') message: the
// capture/encode configuration the client wants to apply. Any field left at
// its zero value is left unchanged (the server merges the provided fields
// over rs.curParams), so the client can change a single knob without
// resending the whole configuration.
type settingsMsg struct {
	// Quality is a named preset: "low" (3200/7200 kbps), "med"
	// (5000/15000 kbps), "high" (7200/21000 kbps). When set, it overrides
	// Bitrate/Maxrate (which are otherwise applied directly if non-empty).
	Quality string `json:"quality"`
	// Bitrate is the target VBR bitrate in kbps (e.g. "5500").
	Bitrate string `json:"bitrate"`
	// Maxrate is the peak bitrate in kbps (e.g. "14400").
	Maxrate string `json:"maxrate"`
	// FPS is the clean output framerate (15/24/30/45/60).
	FPS int `json:"fps"`
	// Scale is the output scale factor (0.5-1.0); 1.0 = full resolution.
	Scale float64 `json:"scale"`
	// Monitor is the xrandr output name to capture ("" = primary). It is a
	// pointer so an explicit "" (switch back to the primary monitor) is
	// distinguishable from "not provided" (nil = leave the current monitor
	// unchanged), which a plain string cannot express (both are "").
	Monitor *string `json:"monitor"`
}

// subscriber is a /stream client's receive channel plus its lag state. lag
// counts consecutive chunks that could not be delivered because the channel
// was full; a client that lags beyond dropLag is dropped (its channel is
// closed and removed) so a persistently slow client cannot wedge the pump.
type subscriber struct {
	ch  chan []byte
	lag int
}

// dropLag is the number of consecutive undelivered chunks before a subscriber
// is dropped. The channel buffer (subscriberChannelSize) already absorbs
// bursts; if a client is still full after dropLag more chunks it is not
// keeping up with a live stream and is dropped rather than being starved
// forever. Together with the channel depth, a fully dead client is dropped
// within (channel + dropLag) chunks ~= (64+256)*64KB ~= 25MB ~= ~25s of
// stream at the default bitrate.
const dropLag = 256

// StreamServer fans out the live video byte-stream (fMP4) to any number of
// connected HTTP clients. Each client gets its own buffered channel; a slow or
// slow-draining client simply drops frames rather than blocking the encoder.
// A client that is persistently full (lag >= dropLag) is dropped: its channel
// is closed so the client's ServeStream loop returns, freeing the TCP
// connection instead of letting the client wedge indefinitely.
type StreamServer struct {
	mu      sync.Mutex
	clients map[chan []byte]*subscriber
	dropped int64 // total subscribers dropped for persistent lag (diagnostics)
}

// newStreamServer creates an empty broadcaster.
func newStreamServer() *StreamServer {
	return &StreamServer{clients: make(map[chan []byte]*subscriber)}
}

// subscriberChannelSize is the per-client channel depth in chunks. Each chunk
// is up to 64KB (the pump's read buffer), so 64 slots ~= 4MB ~= ~4s of
// video at the ~8Mbps default bitrate: enough to absorb network bursts and
// brief TCP stalls without dropping frames, but small enough that a dead
// client's channel fills quickly and the drop policy (dropLag) fires in a
// bounded time rather than after minutes.
const subscriberChannelSize = 64

// add registers a new subscriber and returns its receive channel. The caller
// MUST pass the returned channel to remove when done.
func (s *StreamServer) add() chan []byte {
	ch := make(chan []byte, subscriberChannelSize)
	s.mu.Lock()
	s.clients[ch] = &subscriber{ch: ch}
	s.mu.Unlock()
	return ch
}

// remove unregisters a subscriber previously obtained via add.
func (s *StreamServer) remove(ch chan []byte) {
	s.mu.Lock()
	delete(s.clients, ch)
	s.mu.Unlock()
}

// write broadcasts a single chunk of the stream to every subscriber.
// Back-pressured subscribers (full channel) are skipped for that chunk; their
// lag counter increments. A subscriber whose lag reaches dropLag is dropped
// (channel closed + removed) so a persistently slow client cannot wedge the
// pump. The pump never blocks on a client: every send is non-blocking, so the
// GStreamer read loop keeps draining the encoder at full rate regardless of
// any individual client's TCP window.
func (s *StreamServer) write(chunk []byte) {
	s.mu.Lock()
	for ch, sub := range s.clients {
		select {
		case sub.ch <- chunk:
			sub.lag = 0
		default:
			sub.lag++
			if sub.lag >= dropLag {
				delete(s.clients, ch)
				close(sub.ch) // ServeStream loop sees the close and returns.
				s.dropped++
				log.Printf("stream: dropped slow subscriber (lag %d chunks)", sub.lag)
			}
		}
	}
	s.mu.Unlock()
}

// RawStreamServer fans out the live raw H.264 (Annex-B) access units to
// connected /raw WebSocket clients (the primary, lowest-latency video path).
// It mirrors StreamServer's fan-out + lag-drop policy: each client gets its
// own buffered channel, and a client whose channel stays full for dropLag
// access units is dropped (channel closed) so a persistently slow client
// cannot wedge the raw pump.
type RawStreamServer struct {
	mu      sync.Mutex
	clients map[chan []byte]*subscriber
	dropped int64 // total subscribers dropped for persistent lag (diagnostics)
}

// newRawStreamServer creates an empty raw broadcaster.
func newRawStreamServer() *RawStreamServer {
	return &RawStreamServer{clients: make(map[chan []byte]*subscriber)}
}

// add registers a new raw subscriber and returns its receive channel. The
// caller MUST pass the returned channel to remove when done.
func (s *RawStreamServer) add() chan []byte {
	ch := make(chan []byte, subscriberChannelSize)
	s.mu.Lock()
	s.clients[ch] = &subscriber{ch: ch}
	s.mu.Unlock()
	return ch
}

// remove unregisters a raw subscriber previously obtained via add.
func (s *RawStreamServer) remove(ch chan []byte) {
	s.mu.Lock()
	delete(s.clients, ch)
	s.mu.Unlock()
}

// write broadcasts a single access unit to every raw subscriber. Back-
// pressured subscribers are skipped for that unit; their lag counter
// increments. A subscriber whose lag reaches dropLag is dropped (channel
// closed + removed) so a persistently slow client cannot wedge the pump.
// The pump never blocks on a client: every send is non-blocking.
func (s *RawStreamServer) write(au []byte) {
	s.mu.Lock()
	for ch, sub := range s.clients {
		select {
		case sub.ch <- au:
			sub.lag = 0
		default:
			sub.lag++
			if sub.lag >= dropLag {
				delete(s.clients, ch)
				close(sub.ch) // ServeRaw loop sees the close and returns.
				s.dropped++
				log.Printf("raw: dropped slow subscriber (lag %d access units)", sub.lag)
			}
		}
	}
	s.mu.Unlock()
}

// AudioStreamServer fans out the live audio (opus packets, one per Ogg page
// packet) to connected /audio WebSocket clients. It mirrors RawStreamServer's
// fan-out + lag-drop policy: each client gets its own buffered channel, and a
// client whose channel stays full for dropLag packets is dropped (channel
// closed) so a persistently slow client cannot wedge the audio pump.
// audioHeadMagic prefixes the OpusHead re-emit message so a client can
// distinguish it from a bare opus audio packet on the same binary channel.
const audioHeadMagic = "AUDIOHEAD:"

type AudioStreamServer struct {
	mu      sync.Mutex
	clients map[chan []byte]*subscriber
	dropped int64 // total subscribers dropped for persistent lag (diagnostics)
	// head is the last OpusHead packet seen from the pipeline (the stream's
	// channel count + sample rate). Cached so a client that connects after the
	// pipeline already started still receives it (like the fMP4 init segment):
	// without it the client cannot configure its AudioDecoder to match the
	// ACTUAL stream and decodes fail with a DataError.
	head []byte
}

// newAudioStreamServer creates an empty audio broadcaster.
func newAudioStreamServer() *AudioStreamServer {
	return &AudioStreamServer{clients: make(map[chan []byte]*subscriber)}
}

// setHead stores the OpusHead packet and re-emits it to every current
// subscriber (non-blocking; a slow subscriber simply misses it and will get
// the next one). Called once by the audio pump when the demuxer first sees
// the OpusHead.
func (s *AudioStreamServer) setHead(head []byte) {
	msg := append([]byte(audioHeadMagic), head...)
	s.mu.Lock()
	s.head = head
	for _, sub := range s.clients {
		select {
		case sub.ch <- msg:
		default: // subscriber is back-pressured; skip (it will get live packets).
		}
	}
	s.mu.Unlock()
}

// add registers a new audio subscriber and returns its receive channel. If a
// head has already been captured (the pipeline started before this client
// connected), the head is re-emitted to the new subscriber first so it can
// configure its decoder before any audio packets arrive. The caller MUST pass
// the returned channel to remove when done.
func (s *AudioStreamServer) add() chan []byte {
	ch := make(chan []byte, subscriberChannelSize)
	s.mu.Lock()
	s.clients[ch] = &subscriber{ch: ch}
	if s.head != nil {
		msg := append([]byte(audioHeadMagic), s.head...)
		select {
		case ch <- msg:
		default:
		}
	}
	s.mu.Unlock()
	return ch
}

// remove unregisters an audio subscriber previously obtained via add.
func (s *AudioStreamServer) remove(ch chan []byte) {
	s.mu.Lock()
	delete(s.clients, ch)
	s.mu.Unlock()
}

// write broadcasts a single opus packet to every audio subscriber. Back-
// pressured subscribers are skipped for that packet; their lag counter
// increments. A subscriber whose lag reaches dropLag is dropped (channel
// closed + removed) so a persistently slow client cannot wedge the pump.
// The pump never blocks on a client: every send is non-blocking.
func (s *AudioStreamServer) write(pkt []byte) {
	s.mu.Lock()
	for ch, sub := range s.clients {
		select {
		case sub.ch <- pkt:
			sub.lag = 0
		default:
			sub.lag++
			if sub.lag >= dropLag {
				delete(s.clients, ch)
				close(sub.ch) // ServeAudio loop sees the close and returns.
				s.dropped++
				log.Printf("audio: dropped slow subscriber (lag %d packets)", sub.lag)
			}
		}
	}
	s.mu.Unlock()
}

// RelayServer multiplexes the WebSocket control channel (ping/pong + key/mouse
// input) between the connected browser and the remote capture/encode pipeline.
// The live video is served two ways:
//   - /raw (PRIMARY): raw H.264 (Annex-B) access units over a WebSocket, one
//     binary message per access unit, decoded client-side with WebCodecs and
//     rendered to a <canvas> as each frame arrives (~1-2 frames of latency).
//   - /stream (fallback): the same access units re-muxed by the Go fMP4 muxer
//     into fragmented MP4, served over plain HTTP for browsers without
//     WebCodecs (MediaSource/SourceBuffer).
type RelayServer struct {
	cfg      config
	upgrader websocket.Upgrader
	// stream fans out the live fragmented-MP4 video (MSE fallback) to
	// connected /stream HTTP clients.
	stream *StreamServer
	// raw fans out the live raw H.264 access units (primary path) to
	// connected /raw WebSocket clients.
	raw *RawStreamServer
	// audio fans out the live audio (opus packets) to connected /audio
	// WebSocket clients.
	audio *AudioStreamServer
	// capture owns the Mutter ScreenCast session + GStreamer pipeline
	// (capture, NVENC, raw H.264 + Go fMP4 re-mux). Set by ensureCapture;
	// torn down in Close / client disconnect. nil until capture has started.
	capture *Capture
	// audioCap owns the audio GStreamer pipeline (pulsesrc monitor -> opusenc
	// -> oggmux -> fdsink). Set by ensureAudio; torn down in Close / last
	// /audio client disconnect. nil until the audio pipeline has started.
	audioCap *AudioCapture
	// captureMu guards the rs.capture lifecycle and the connect->start /
	// disconnect->teardown transition so capture starts on the first video
	// client connect (raw WS or fMP4 HTTP) and tears down on the last
	// disconnect.
	captureMu sync.Mutex
	// streamCount is the number of active /stream HTTP (fMP4/MSE) video
	// clients.
	streamCount int64
	// rawCount is the number of active /raw WebSocket (WebCodecs) video
	// clients.
	rawCount int64
	// audioCount is the number of active /audio WebSocket (opus) audio
	// clients. It drives the audio pipeline's lifecycle: start on the first
	// connect, tear down on the last disconnect.
	audioCount int64
	// inputCh receives key/mouse events from the connected browser; drained
	// into the uinput virtual device by the inputLoop goroutine.
	inputCh chan []byte
	// input is the uinput virtual pointer+keyboard device that the inputLoop
	// drains inputCh into. Created once (lazily, on first event) and closed in
	// Close. nil if /dev/uinput is unavailable (input is then a no-op).
	input *uinputInjector
	// authEnabled reports whether a bearer token is required.
	authEnabled bool
	// initMu guards initSeg: the fMP4 init segment (ftyp+moov) is built once
	// by the capture's muxer and stored here so a /stream client that joins
	// after the init was first broadcast can be re-served it (MSE cannot start
	// without the moov). Written by storeInitSegment (the muxer's emitInit
	// callback, on the pump goroutine), read by ServeStream when a
	// late-joining subscriber registers.
	initMu  sync.Mutex
	initSeg []byte
	// curParams is the current capture/encode configuration (bitrate, maxrate,
	// fps, scale, monitor). It is the source of truth for ensureCapture: the
	// initial value comes from the environment (defaultCaptureParams) and the
	// settings channel (kindSettings, 'S') updates it at runtime, restarting
	// the capture pipeline with the new values. Guarded by captureMu (the
	// same lock that guards the rs.capture lifecycle).
	curParams captureParams
}

// NewRelayServer builds the relay server and its upgrader.
func NewRelayServer(cfg config) *RelayServer {
	rs := &RelayServer{
		cfg:         cfg,
		authEnabled: cfg.Bearer != "",
		stream:      newStreamServer(),
		raw:         newRawStreamServer(),
		audio:       newAudioStreamServer(),
	}
	// AllowOrigin is intentionally permissive for MVP: the transport is
	// protected by the bearer handshake (and/or SSH tunnel).

rs.upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024 * 64,
		// CORS is not a factor for the relay; auth is enforced at the WS level.
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	rs.inputCh = make(chan []byte, 64)
	rs.input = newUinputInjector()
	// The initial capture parameters come from the environment (see
	// defaultCaptureParams); the settings channel (kindSettings, 'S') updates
	// them at runtime.
	rs.curParams = defaultCaptureParams()
	go rs.inputLoop()
	return rs
}

// inputLoop drains the browser's key/mouse events (inputCh) into the uinput
// virtual device. It runs for the relay's lifetime; the device is created
// lazily on the first event (so a video-only relay never touches /dev/uinput)
// and destroyed in Close.
func (rs *RelayServer) inputLoop() {
	for raw := range rs.inputCh {
		rs.input.dispatchInput(raw)
	}
}

// ServeHTTP implements http.Handler; it dispatches to the fMP4 video stream,
// the raw H.264 WebSocket stream, or the WebSocket control channel based on
// the request path.
func (rs *RelayServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/stream":
		rs.ServeStream(w, r)
	case "/raw":
		rs.ServeRaw(w, r)
	case "/audio":
		rs.ServeAudio(w, r)
	case "/monitors":
		rs.ServeMonitors(w, r)
	default:
		rs.ServeWS(w, r)
	}
}

// ServeMonitors lists the connected xrandr outputs (name, resolution,
// position, refresh rate, primary flag) as JSON. The client uses this to
// populate its display-switching control (the settings channel then captures
// the chosen monitor by name). It is read-only and does not start capture.
func (rs *RelayServer) ServeMonitors(w http.ResponseWriter, r *http.Request) {
	if rs.authEnabled {
		token := r.URL.Query().Get("token")
		if token != rs.cfg.Bearer {
			http.Error(w, "invalid bearer token", http.StatusUnauthorized)
			return
		}
	}
	mons := listMonitors()
	if mons == nil {
		mons = []monitorInfo{} // marshal as [] rather than null
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	_ = json.NewEncoder(w).Encode(mons)
}

// ServeRaw upgrades the HTTP connection to a WebSocket and serves the live
// raw H.264 (Annex-B) access-unit stream: one binary message per access unit.
// The client decodes each access unit with WebCodecs (VideoDecoder) and
// renders the decoded frame to a <canvas> as it arrives.
//
// The capture lifecycle is driven by the combined raw-WS + fMP4-HTTP client
// count: a /raw client connecting when no capture is running starts it (the
// "raw relay starts first, owns the pipe" case), and the capture tears down
// only when both /raw and /stream clients are gone.
func (rs *RelayServer) ServeRaw(w http.ResponseWriter, r *http.Request) {
	// The browser WebSocket API cannot set arbitrary headers (Authorization),
	// so the bearer token is passed as a query parameter: /raw?token=...
	if rs.authEnabled {
		token := r.URL.Query().Get("token")
		if token != rs.cfg.Bearer {
			http.Error(w, "invalid bearer token", http.StatusUnauthorized)
			return
		}
	}

	conn, err := rs.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// Drive the capture lifecycle from the combined video-client count (raw
	// WS + fMP4 HTTP). Start capture on the first connect; tear it down on
	// the last disconnect.
	atomic.AddInt64(&rs.rawCount, 1)
	defer func() {
		atomic.AddInt64(&rs.rawCount, -1)
		rs.stopCapture()
	}()
	rs.ensureCapture()

	// Register as a subscriber to the raw access-unit stream.
	ch := rs.raw.add()
	defer rs.raw.remove(ch)

	// Write pump: one binary WS message per access unit. A slow client's
	// channel fills and the lag-drop policy drops it (the channel is closed
	// and the loop below returns), so the pump never blocks on a client.
	for {
		select {
		case <-r.Context().Done():
			return
		case au, ok := <-ch:
			if !ok {
				// Channel closed: the subscriber was dropped (persistent lag)
				// or the broadcaster shut down.
				return
			}
			if werr := conn.WriteMessage(websocket.BinaryMessage, au); werr != nil {
				return
			}
		}
	}
}

// ServeAudio upgrades the HTTP connection to a WebSocket and serves the live
// audio stream: one binary message per opus packet (extracted from the Ogg
// page framing by the audio pump). The client decodes each packet with
// WebCodecs (AudioDecoder, codec "opus") and plays it through an
// AudioContext.
//
// The audio pipeline lifecycle is driven by the /audio client count: a
// client connecting when no audio pipeline is running starts it (the audio
// pump owns the ogg-demuxed packet stream), and the pipeline tears down when
// the last client disconnects.
func (rs *RelayServer) ServeAudio(w http.ResponseWriter, r *http.Request) {
	// The browser WebSocket API cannot set arbitrary headers (Authorization),
	// so the bearer token is passed as a query parameter: /audio?token=...
	if rs.authEnabled {
		token := r.URL.Query().Get("token")
		if token != rs.cfg.Bearer {
			http.Error(w, "invalid bearer token", http.StatusUnauthorized)
			return
		}
	}

	conn, err := rs.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// Drive the audio pipeline lifecycle from the audio-client count. Start
	// the pipeline on the first connect; tear it down on the last disconnect.
	atomic.AddInt64(&rs.audioCount, 1)
	defer func() {
		atomic.AddInt64(&rs.audioCount, -1)
		rs.stopAudio()
	}()
	rs.ensureAudio()

	// Register as a subscriber to the audio packet stream.
	ch := rs.audio.add()
	defer rs.audio.remove(ch)

	// Write pump: one binary WS message per opus packet. A slow client's
	// channel fills and the lag-drop policy drops it (the channel is closed
	// and the loop below returns), so the pump never blocks on a client.
	for {
		select {
		case <-r.Context().Done():
			return
		case pkt, ok := <-ch:
			if !ok {
				// Channel closed: the subscriber was dropped (persistent lag)
				// or the broadcaster shut down.
				return
			}
			if werr := conn.WriteMessage(websocket.BinaryMessage, pkt); werr != nil {
				return
			}
		}
	}
}

// ServeWS upgrades the HTTP connection to a WebSocket and runs the relay loop
// until the client disconnects or the server is closed. It is the control
// channel only: ping/pong keepalive and key/mouse input forwarding. The live
// video is served separately by ServeStream over plain HTTP.
func (rs *RelayServer) ServeWS(w http.ResponseWriter, r *http.Request) {
	// The browser WebSocket API cannot set arbitrary headers (Authorization),
	// so the bearer token is passed as a query parameter: /relay?token=...
	if rs.authEnabled {
		token := r.URL.Query().Get("token")
		if token != rs.cfg.Bearer {
			http.Error(w, "invalid bearer token", http.StatusUnauthorized)
			return
		}
	}

	conn, err := rs.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// Keepalive + input loop until the client disconnects.
	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		if len(data) < 1 {
			continue
		}
		switch messageKind(data[0]) {
		case kindPing:
			_ = conn.WriteMessage(websocket.TextMessage, []byte{byte(kindPong)})
		case kindInput:
			// Clone the payload (minus the kind byte); the buffer may be
			// reused by the next read.
			cp := make([]byte, len(data)-1)
			copy(cp, data[1:])
			select {
			case rs.inputCh <- cp:
			default:
				// Backpressure: drop if the input pipeline isn't draining.
			}
		case kindSettings:
			// Parse the settings payload, validate it, apply it to curParams
			// and restart the capture pipeline with the new values. Reply
			// with a kindSettingsAck carrying the effective values so the
			// client can confirm what was applied.
			var s settingsMsg
			if err := json.Unmarshal(data[1:], &s); err != nil {
				log.Printf("settings: invalid payload: %v", err)
				continue
			}
			applied := rs.applySettings(s)
			if ack, err := json.Marshal(applied); err == nil {
				ackMsg := make([]byte, 0, 1+len(ack))
				ackMsg = append(ackMsg, byte(kindSettingsAck))
				ackMsg = append(ackMsg, ack...)
				_ = conn.WriteMessage(websocket.TextMessage, ackMsg)
			}
		}
		_ = msgType
	}
}

// ServeStream serves the live fragmented-MP4 byte stream (MSE fallback path)
// to a single HTTP client via progressive download. The browser fetches it
// and pipes the bytes into a MediaSource/SourceBuffer (Chrome cannot play
// fMP4 in a bare <video src>, so the client uses MSE).
//
// The capture pipeline is driven by the combined raw-WS + fMP4-HTTP client
// count: it starts on the first video client connect (raw or fMP4) and tears
// down on the last disconnect, so idle relays don't leak the
// Mutter/PipeWire/GStreamer pipeline.
func (rs *RelayServer) ServeStream(w http.ResponseWriter, r *http.Request) {
	// The browser cannot set arbitrary headers, so the bearer token is passed
	// as a query parameter: /stream?token=...
	if rs.authEnabled {
		token := r.URL.Query().Get("token")
		if token != rs.cfg.Bearer {
			http.Error(w, "invalid bearer token", http.StatusUnauthorized)
			return
		}
	}

	// Drive the capture lifecycle from the video-client count. Start capture on
	// the first connect; tear it down on the last disconnect so idle relays
	// don't leak the Mutter/PipeWire/GStreamer pipeline.
	atomic.AddInt64(&rs.streamCount, 1)
	defer func() {
		atomic.AddInt64(&rs.streamCount, -1)
		rs.stopCapture()
	}()
	rs.ensureCapture()

	// Progressive-download headers: no caching, keep the connection open, and
	// advertise fragmented MP4 so the browser's fetch/MSE pipeline accepts it.
	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Register as a subscriber to the live stream.
	ch := rs.stream.add()
	defer rs.stream.remove(ch)

	// Re-emit the fMP4 init segment (ftyp+moov) to this subscriber if one has
	// already been built. A /stream client that joins after the init was first
	// broadcast never sees it (it was written to the old subscribers' channels),
	// and MSE cannot start without the moov, so we write it here before any
	// live fragment. If no init segment has been built yet (the capture is
	// still starting up), the init will arrive via the channel when the muxer
	// builds it, so there is nothing to write.
	if initSeg := rs.initSegment(); initSeg != nil {
		if _, werr := w.Write(initSeg); werr != nil {
			return
		}
		flusher.Flush()
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case chunk, ok := <-ch:
			if !ok {
				// Channel closed: the subscriber was dropped (persistent lag)
				// or the broadcaster shut down. End the response so the client
				// sees a clean stream end (FIN) instead of a hung connection.
				return
			}
			if _, werr := w.Write(chunk); werr != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// videoClientCount returns the combined number of active video clients: the
// raw-WS (WebCodecs, primary) clients plus the fMP4-HTTP (MSE, fallback)
// clients. The capture lifecycle is driven by this combined count: a raw
// client starting capture is the "raw relay starts first, owns the pipe"
// case, and the capture tears down only when both kinds are gone.
func (rs *RelayServer) videoClientCount() int64 {
	return atomic.LoadInt64(&rs.rawCount) + atomic.LoadInt64(&rs.streamCount)
}

// streamClientCount returns the number of active fMP4-HTTP (MSE, fallback)
// /stream clients. The capture pump uses this to gate the fMP4 muxer's
// feed: building moof+mdat fragments is only worth the CPU while at least
// one MSE client is actually consuming them. The raw-WS (primary) path is
// always fanned out regardless.
func (rs *RelayServer) streamClientCount() int64 {
	return atomic.LoadInt64(&rs.streamCount)
}

// ensureCapture starts the capture pipeline when the first video client
// (raw WS or fMP4 HTTP) connects and it isn't already running. It holds
// captureMu for the whole check-and-start so capture starts exactly once
// regardless of how many clients connect.
//
// run() is spawned in a goroutine so the connecting handler can register its
// subscription immediately; the D-Bus session setup (which takes several
// seconds) happens in the background. When run() returns (pipeline dead or
// torn down) the goroutine clears the draining flag and, on failure, drops
// the capture ref so a later connect can retry.
func (rs *RelayServer) ensureCapture() {
	// COAXIAL_DISABLE_CAPTURE: serve the control channel only; never start
	// the (heavy) Mutter/PipeWire/GStreamer pipeline. The video stream stays
	// empty, which is expected in this mode.
	if rs.cfg.DisableCapture {
		return
	}
	rs.captureMu.Lock()
	defer rs.captureMu.Unlock()
	if rs.capture != nil || rs.videoClientCount() == 0 {
		return
	}
	cap := newCapture(rs.curParams)
	if cap == nil {
		return
	}
	rs.capture = cap
	go func() {
		ok := cap.run(rs)
		// run() has returned: the pipeline is dead (failure or teardown).
		// Drop the capture ref only if it still points at this capture so a
		// later connect can retry; a newer capture already running wins.
		rs.captureMu.Lock()
		if !ok && rs.capture == cap {
			rs.capture = nil
		}
		rs.captureMu.Unlock()
	}()
	log.Printf("capture: starting (%d connected video client(s): %d raw, %d fMP4)",
		rs.videoClientCount(), atomic.LoadInt64(&rs.rawCount), atomic.LoadInt64(&rs.streamCount))
}

// stopCapture tears down the capture pipeline when the last video client
// (raw WS or fMP4 HTTP) disconnects. It holds captureMu so it can't race the
// ensureCapture transition. teardown() is synchronous (signals + D-Bus Stop);
// the previous run() goroutine returns on its own and only clears the ref if
// it still points at the torn-down capture, so a rapid reconnect can't leave
// two pipelines running.
func (rs *RelayServer) stopCapture() {
	rs.captureMu.Lock()
	defer rs.captureMu.Unlock()
	if rs.capture == nil || rs.videoClientCount() > 0 {
		return
	}
	rs.capture.teardown()
	rs.capture = nil
	log.Printf("capture: torn down (0 connected video client(s))")
}

// audioClientCount returns the number of active /audio WebSocket clients.
func (rs *RelayServer) audioClientCount() int64 {
	return atomic.LoadInt64(&rs.audioCount)
}

// ensureAudio starts the audio pipeline when the first /audio client
// connects and it isn't already running. It holds captureMu for the whole
// check-and-start so the audio pipeline starts exactly once regardless of how
// many clients connect. The audio pipeline is a separate GStreamer process
// (pulsesrc monitor -> opusenc -> oggmux -> fdsink) that fans its ogg-demuxed
// opus packets out to rs.audio.
func (rs *RelayServer) ensureAudio() {
	// COAXIAL_DISABLE_AUDIO: never start the audio pipeline.
	if rs.cfg.DisableAudio {
		return
	}
	rs.captureMu.Lock()
	defer rs.captureMu.Unlock()
	if rs.audioCap != nil || rs.audioClientCount() == 0 {
		return
	}
	cap := newAudioCapture()
	if cap == nil {
		return
	}
	rs.audioCap = cap
	go func() {
		ok := cap.run(rs)
		// run() has returned: the pipeline is dead (failure or teardown).
		// Drop the audio capture ref only if it still points at this capture
		// so a later connect can retry; a newer capture already running wins.
		rs.captureMu.Lock()
		if !ok && rs.audioCap == cap {
			rs.audioCap = nil
		}
		rs.captureMu.Unlock()
	}()
	log.Printf("audio: starting (%d connected audio client(s))", rs.audioClientCount())
}

// stopAudio tears down the audio pipeline when the last /audio client
// disconnects. It holds captureMu so it can't race the ensureAudio
// transition.
func (rs *RelayServer) stopAudio() {
	rs.captureMu.Lock()
	defer rs.captureMu.Unlock()
	if rs.audioCap == nil || rs.audioClientCount() > 0 {
		return
	}
	rs.audioCap.teardown()
	rs.audioCap = nil
	log.Printf("audio: torn down (0 connected audio client(s))")
}

// qualityPresets maps a named quality preset to its target/peak bitrate
// (kbps). The client cycles through these via the settings channel; a
// custom bitrate/maxrate (non-empty) overrides the preset.
var qualityPresets = map[string][2]string{
	"low":  {"3200", "7200"},
	"med":  {"5000", "15000"},
	"high": {"7200", "21000"},
}

// validFPS is the set of clean output framerates the encoder supports
// (videorate resample target + gop-size).
var validFPS = map[int]bool{15: true, 24: true, 30: true, 45: true, 60: true}

// applySettings validates and merges a settingsMsg over rs.curParams (zero
// fields are left unchanged), updates rs.curParams, and restarts the capture
// pipeline with the new values if one is running. It holds captureMu for the
// whole check-and-restart so the capture lifecycle (ensureCapture /
// stopCapture) can't race the transition. It returns the effective
// parameters that were applied, for the client's confirmation.
//
// The restart is a teardown + ensureCapture: the old pipeline is stopped
// (signalling the process group + the D-Bus session) and a new one is started
// with the new parameters. The fMP4 init segment is re-built and
// re-broadcast (storeInitSegment) so connected /stream clients pick up the
// new codec configuration; /raw clients re-sync on the next SPS/PPS.
func (rs *RelayServer) applySettings(s settingsMsg) captureParams {
	rs.captureMu.Lock()
	defer rs.captureMu.Unlock()

	p := rs.curParams

	// Merge the provided fields over the current parameters. Zero values are
	// left unchanged so the client can change a single knob.
	if s.Bitrate != "" {
		p.Bitrate = s.Bitrate
	}
	if s.Maxrate != "" {
		p.Maxrate = s.Maxrate
	}
	if s.FPS > 0 {
		p.FPS = s.FPS
	}
	if s.Scale > 0 {
		p.Scale = s.Scale
	}
	// Monitor is a pointer so an explicit "" (switch back to the primary
	// monitor) is distinguishable from "not provided" (nil = leave the current
	// monitor unchanged). A plain string could not express this (both are "").
	if s.Monitor != nil {
		p.MonitorID = *s.Monitor
	}
	// A named quality preset overrides the explicit bitrate/maxrate (the
	// client sends the preset it selected, not the raw values).
	if s.Quality != "" {
		if pr, ok := qualityPresets[s.Quality]; ok {
			p.Bitrate = pr[0]
			p.Maxrate = pr[1]
		}
	}

	// Validate the framerate (the encoder's gop-size + videorate target).
	if !validFPS[p.FPS] {
		p.FPS = defaultOutFPS
	}
	// Clamp the scale to a sane range (0.5 = half resolution, 1.0 = full).
	if p.Scale < 0.5 {
		p.Scale = 0.5
	}
	if p.Scale > 1.0 {
		p.Scale = 1.0
	}

	rs.curParams = p
	log.Printf("settings: applied (quality=%q bitrate=%s maxrate=%s fps=%d scale=%.2f monitor=%q)",
		s.Quality, p.Bitrate, p.Maxrate, p.FPS, p.Scale, p.MonitorID)

	// Restart the capture pipeline with the new values if one is running.
	// If no capture is running (no video clients), the new parameters take
	// effect when the next client connects (ensureCapture).
	if rs.capture != nil {
		old := rs.capture
		old.teardown()
		rs.capture = nil
		if rs.videoClientCount() > 0 {
			cap := newCapture(rs.curParams)
			if cap == nil {
				return p
			}
			rs.capture = cap
			go func() {
				ok := cap.run(rs)
				// run() has returned: the pipeline is dead (failure or
				// teardown). Drop the capture ref only if it still points at
				// this capture so a newer capture or a concurrent teardown
				// wins.
				rs.captureMu.Lock()
				if !ok && rs.capture == cap {
					rs.capture = nil
				}
				rs.captureMu.Unlock()
			}()
			log.Printf("settings: capture restarted with new parameters")
		}
	}
	return p
}

// storeInitSegment stores the fMP4 init segment (ftyp+moov) so a /stream
// client that joins after the init was first built can be re-served it, and
// broadcasts it to the subscribers that are currently connected. It is called
// by the capture's muxer (emitInit) the first time the init segment is built
// (on the pump goroutine). The broadcast is required because the muxer's
// feed() routes the init through emitInit (not emit) when emitInit is set, so
// current subscribers would otherwise never see it on their channels. The
// stored copy is taken before the broadcast so a concurrent teardown of a
// subscriber cannot race the write.
func (rs *RelayServer) storeInitSegment(initSeg []byte) {
	rs.initMu.Lock()
	rs.initSeg = append([]byte{}, initSeg...)
	rs.initMu.Unlock()
	// Broadcast to the subscribers connected at this moment; a subscriber
	// that registers after this point gets the init via ServeStream (which
	// writes rs.initSegment() before its first live fragment).
	rs.stream.write(initSeg)
}

// initSegment returns a copy of the last fMP4 init segment (ftyp+moov) stored,
// or nil if none yet (the capture is still starting up; the init will arrive
// via the subscriber channel when the muxer builds it). ServeStream writes it
// to a newly added subscriber so a late joiner can start MSE playback: the
// init was broadcast to the subscribers that were connected when it was built,
// so a subscriber added after that moment would otherwise miss it.
func (rs *RelayServer) initSegment() []byte {
	rs.initMu.Lock()
	defer rs.initMu.Unlock()
	if rs.initSeg == nil {
		return nil
	}
	return append([]byte{}, rs.initSeg...)
}

// Close shuts down the relay and terminates the capture and audio pipelines.
func (rs *RelayServer) Close() {
	rs.captureMu.Lock()
	if rs.capture != nil {
		rs.capture.teardown()
		rs.capture = nil
	}
	if rs.audioCap != nil {
		rs.audioCap.teardown()
		rs.audioCap = nil
	}
	rs.captureMu.Unlock()
	// Destroy the uinput virtual device (no-op if it was never created).
	if rs.input != nil {
		rs.input.Close()
	}
}
