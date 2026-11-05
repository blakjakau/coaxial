package main

import (
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

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
)

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
	// capture owns the Mutter ScreenCast session + GStreamer pipeline
	// (capture, NVENC, raw H.264 + Go fMP4 re-mux). Set by ensureCapture;
	// torn down in Close / client disconnect. nil until capture has started.
	capture *Capture
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
	// inputCh receives key/mouse events from the connected browser; drained
	// into the uinput virtual device by the inputLoop goroutine.
	inputCh chan []byte
	// input is the uinput virtual pointer+keyboard device that the inputLoop
	// drains inputCh into. Created once (lazily, on first event) and closed in
	// Close. nil if /dev/uinput is unavailable (input is then a no-op).
	input *uinputInjector
	// authEnabled reports whether a bearer token is required.
	authEnabled bool
}

// NewRelayServer builds the relay server and its upgrader.
func NewRelayServer(cfg config) *RelayServer {
	rs := &RelayServer{
		cfg:         cfg,
		authEnabled: cfg.Bearer != "",
		stream:      newStreamServer(),
		raw:         newRawStreamServer(),
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
	go rs.inputLoop()
	return rs
}

// inputLoop drains the browser's key/mouse events (inputCh) into the uinput
// virtual device. It runs for the relay's lifetime; the device is created
// lazily on the first event (so a video-only relay never touches /dev/uinput)
// and destroyed in Close.
//
// A throttled diagnostic logs the first event of each type (and once per
// second thereafter) so you can confirm events are arriving and being
// dispatched. Remove once input is confirmed working end-to-end.
func (rs *RelayServer) inputLoop() {
	var lastLog int64
	for raw := range rs.inputCh {
		// Throttled: log once per second (mousemove spam otherwise floods).
		now := time.Now().UnixMilli()
		if now-lastLog > 1000 {
			lastLog = now
			log.Printf("input: received %s", string(raw))
		}
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
	default:
		rs.ServeWS(w, r)
	}
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
	// SCREENRELAY_DISABLE_CAPTURE: serve the control channel only; never start
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
	cap := newCapture()
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

// Close shuts down the relay and terminates the capture pipeline.
func (rs *RelayServer) Close() {
	rs.captureMu.Lock()
	if rs.capture != nil {
		rs.capture.teardown()
		rs.capture = nil
	}
	rs.captureMu.Unlock()
	// Destroy the uinput virtual device (no-op if it was never created).
	if rs.input != nil {
		rs.input.Close()
	}
}
