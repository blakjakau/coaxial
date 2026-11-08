// Command screenrelay is a remote-side server that captures the screen,
// encodes it to H.264 via NVENC (GStreamer nvh264enc), and serves the live
// stream two ways:
//
//   - /raw (PRIMARY): raw H.264 (Annex-B) access units over a WebSocket, one
//     binary message per access unit. WebCodecs-capable browsers decode each
//     access unit with VideoDecoder and render it to a <canvas> as it
//     arrives (~1-2 frames of end-to-end latency).
//   - /stream (fallback): the same access units re-muxed by a Go fMP4 muxer
//     into fragmented MP4, served over plain HTTP for browsers without
//     WebCodecs (MediaSource/SourceBuffer).
//
// It relays key/mouse input back to the remote machine over a separate
// WebSocket control channel (/relay).
//
// Architecture:
//   Remote (this binary): frame capture -> GStreamer nvh264enc -> raw H.264 (stdout)
//     -> Go pump -> /raw (WS, primary) + Go fMP4 muxer -> /stream (HTTP, fallback)
//   Local  (Chromium): WebCodecs VideoDecoder -> <canvas> (raw, primary) or
//     MediaSource/SourceBuffer fed by /stream (fallback); forwards key/mouse over WS /relay
//
// The live video and the control channel are fully decoupled: the browser
// connects to /raw (or /stream) for the video and to /relay for ping/pong +
// input.
//
// See the implementation plan for the full transport/encoder rationale.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
)

func main() {
	cfg := loadConfig()

	mux := http.NewServeMux()

	// Health check endpoint for bootstrapping (client fetches the bearer
	// token) and liveness probes.
	mux.Handle("/health", healthHandler(cfg.Bearer))

	// Static file server for the browser client.
	fileServer := http.FileServer(http.Dir(cfg.ClientDir))
	// Strip the /client/ prefix so the FileServer resolves paths relative to
	// its directory. Trailing slash on the mux pattern is required for the
	// subtree to match /client/*.
	mux.Handle("/client/", http.StripPrefix("/client/", fileServer))

	// Relay: dispatches /raw (live raw H.264 access units over WebSocket,
	// the primary low-latency path for WebCodecs clients), /stream (live
	// fMP4 video over HTTP, the MSE fallback), and /relay (WebSocket control
	// channel) based on the request path.
	relay := NewRelayServer(cfg)
	mux.Handle("/raw", relay)
	mux.Handle("/stream", relay)
	mux.Handle("/relay", relay)
	// Live audio (opus packets over WebSocket; the client decodes with
	// WebCodecs AudioDecoder and plays through an AudioContext).
	mux.Handle("/audio", relay)
	// Read-only monitor list for the client's display-switching control.
	mux.Handle("/monitors", relay)

	srv := &http.Server{
		Addr:    cfg.Addr,
		Handler: mux,
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, os.Kill)
	go func() {
		<-stop
		relay.Close()
		if err := srv.Shutdown(context.Background()); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()

	log.Printf("screenrelay listening on %s (client dir: %s)", cfg.Addr, cfg.ClientDir)
	if cfg.Bearer == "" {
		log.Printf("WARNING: no SCREENRELAY_BEARER set; relay auth is DISABLED")
	}
	if cfg.DisableCapture {
		log.Printf("WARNING: SCREENRELAY_DISABLE_CAPTURE set; serving control channel only (no capture)")
	}
	if cfg.DisableAudio {
		log.Printf("WARNING: SCREENRELAY_DISABLE_AUDIO set; /audio WS connects but no audio pipeline starts")
	}
	log.Printf("video: browser connects to /raw (raw H.264 via WebCodecs, primary) or /stream (progressive fMP4 via MediaSource, fallback) and /relay (WS control channel)")

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

// config holds runtime configuration for the server.
type config struct {
	Addr      string // listen address, e.g. ":8080"
	ClientDir string // directory containing client index.html + app.js
	Bearer    string // shared bearer token for the WS handshake
	// DisableCapture, when true, makes ServeWS skip the capture lifecycle
	// entirely (no Mutter/PipeWire/GStreamer). The relay only serves the control
	// channel, which lets a browser client connect for testing without spinning
	// up the (heavy) capture pipeline.
	DisableCapture bool
	// DisableAudio, when true, makes ServeAudio skip the audio pipeline
	// entirely (no pulsesrc/opusenc GStreamer). The /audio WS still
	// connects but stays silent.
	DisableAudio bool
}

func loadConfig() config {
	addr := os.Getenv("SCREENRELAY_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	clientDir := os.Getenv("SCREENRELAY_CLIENT_DIR")
	if clientDir == "" {
		clientDir = "client"
	}
	bearer := os.Getenv("SCREENRELAY_BEARER")
	disableCapture := os.Getenv("SCREENRELAY_DISABLE_CAPTURE") != ""
	disableAudio := os.Getenv("SCREENRELAY_DISABLE_AUDIO") != ""

	return config{
		Addr:           addr,
		ClientDir:      clientDir,
		Bearer:         bearer,
		DisableCapture: disableCapture,
		DisableAudio:   disableAudio,
	}
}

// compile-time assertion that time is used (kept for future logging).
var _ = time.Second
