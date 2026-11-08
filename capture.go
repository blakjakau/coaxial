package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/godbus/dbus/v5"
)

const (
	mutterDest = "org.gnome.Mutter.ScreenCast"
	mutterPath = "/org/gnome/Mutter/ScreenCast"

	// maxFragmentFrames caps the number of access units in a single fMP4
	// fragment (fallback path). Fragments are cut at IDR boundaries (so the
	// client's live-edge seek lands on a keyframe present in the buffer) or
	// after this many frames, whichever comes first. 15 frames at 30fps is
	// ~500ms of content per fragment - close to the old mp4mux
	// fragment-duration=250 behaviour but keyframe-aligned.
	maxFragmentFrames = 15
)

const (
	// Maximum stream constraints. newCapture reads the actual primary
	// monitor (xrandr) and streams at min(actual, max), so a 3440x1440
	// monitor streams at 2560x1440 and the 179.92Hz source is resampled
	// to a clean 60fps CFR output.
	maxWidth  = 3440
	maxHeight = 1440
	maxFPS    = 90

	// defaultOutFPS is the clean output framerate the stream is resampled to
	// (GStreamer videorate). The source's delivery rate varies (~50-79fps on
	// this 180Hz box, since the compositor only pushes frames when the screen
	// changes), so the input timestamps are the frames' actual arrival times
	// (GStreamer do-timestamp=true) and the CFR resampler downsamples to a
	// clean 60fps so the stream is a clean 60fps and the timeline tracks real
	// time (1:1).
	defaultWidth  = 1920
	defaultHeight = 1080
	defaultOutFPS = 60
)

// captureParams is the tunable capture/encode configuration. The initial
// values come from the environment (COAXIAL_BITRATE / COAXIAL_MAXRATE
// / COAXIAL_FPS, see defaultCaptureParams); the client can change them at
// runtime via the settings channel (kindSettings, 'S'), which restarts the
// capture pipeline with the new values. Resolution is NOT stored here: it is
// derived from the selected monitor (MonitorID) at capture start, clamped to
// the maximum stream constraints and scaled by Scale.
type captureParams struct {
	Bitrate   string  `json:"bitrate"` // target VBR bitrate in kbps, e.g. "5500"
	Maxrate   string  `json:"maxrate"` // peak bitrate in kbps, e.g. "14400"
	FPS       int     `json:"fps"`     // clean output framerate (videorate target)
	Scale     float64 `json:"scale"`   // output scale factor (0.5-1.0); 1.0 = full
	MonitorID string  `json:"monitor"` // xrandr output name; "" = primary
}

// defaultCaptureParams reads the initial capture parameters from the
// environment, falling back to the defaults. The client can override these at
// runtime via the settings channel.
func defaultCaptureParams() captureParams {
	p := captureParams{
		Bitrate: "5500",
		Maxrate: "14400",
		FPS:     defaultOutFPS,
		Scale:   1.0,
	}
	if v := os.Getenv("COAXIAL_BITRATE"); v != "" {
		p.Bitrate = v
	}
	if v := os.Getenv("COAXIAL_MAXRATE"); v != "" {
		p.Maxrate = v
	}
	if v := os.Getenv("COAXIAL_FPS"); v != "" {
		fmt.Sscanf(v, "%d", &p.FPS)
	}
	return p
}

// monitorInfo describes one connected xrandr output, as reported by the
// "connected" line (name, resolution, position) and its active ("*+") mode
// (refresh rate). The settings channel uses this list to let the client switch
// which monitor is captured (RecordMonitor by monitor ID).
type monitorInfo struct {
	Name    string `json:"name"`    // xrandr output name, e.g. "DP-7"
	Width   int    `json:"width"`   // active mode width
	Height  int    `json:"height"`  // active mode height
	X       int    `json:"x"`       // position within the virtual screen
	Y       int    `json:"y"`
	FPS     int    `json:"fps"`     // active mode refresh rate
	Primary bool   `json:"primary"` // true if this is the primary output
}

// listMonitors returns the connected xrandr outputs (name, resolution, position,
// active-mode refresh rate, primary flag). It returns an empty slice if xrandr
// is unavailable. The order is xrandr's (primary first in practice, but not
// guaranteed) - the client cycles through the list as-is.
func listMonitors() []monitorInfo {
	out, err := exec.Command("xrandr", "--query").Output()
	if err != nil {
		return nil
	}
	connectedRe := regexp.MustCompile(`^(\S+) connected(?: primary)? (\d+)x(\d+)\+(\d+)\+(\d+)`)
	modeRe := regexp.MustCompile(`^\s*(\d+)x(\d+)\s+(\d+(?:\.\d+)?)\*\+`)
	var mons []monitorInfo
	var cur *monitorInfo
	for _, line := range strings.Split(string(out), "\n") {
		if m := connectedRe.FindStringSubmatch(line); m != nil {
			w, _ := strconv.Atoi(m[2])
			h, _ := strconv.Atoi(m[3])
			x, _ := strconv.Atoi(m[4])
			y, _ := strconv.Atoi(m[5])
			mons = append(mons, monitorInfo{Name: m[1], Width: w, Height: h, X: x, Y: y, Primary: strings.Contains(line, "primary")})
			cur = &mons[len(mons)-1]
			continue
		}
		// A mode line: when it is the active ("*+") mode of the current output
		// at its resolution, record its refresh rate.
		if fm := modeRe.FindStringSubmatch(line); fm != nil && cur != nil {
			mw, _ := strconv.Atoi(fm[1])
			mh, _ := strconv.Atoi(fm[2])
			if mw == cur.Width && mh == cur.Height {
				f, _ := strconv.ParseFloat(fm[3], 64)
				cur.FPS = int(f + 0.5)
			}
		}
	}
	return mons
}

// monitorByName returns the resolution and refresh rate of the named xrandr
// output (an empty name selects the primary, falling back to the first
// connected output). It returns ok=false if xrandr is unavailable or the
// named output is not found, in which case the caller falls back to the
// defaultWidth/defaultHeight/defaultOutFPS constants.
func monitorByName(name string) (width, height, fps int, ok bool) {
	if name == "" {
		return primaryMonitor()
	}
	mons := listMonitors()
	for _, m := range mons {
		if m.Name == name {
			if m.FPS <= 0 {
				m.FPS = defaultOutFPS
			}
			return m.Width, m.Height, m.FPS, true
		}
	}
	return 0, 0, 0, false
}

// Capture owns the Mutter ScreenCast session and a single GStreamer pipeline
// that captures the screen and encodes it on the GPU (nvautogpuh264enc /
// NVENC). The pipeline emits raw H.264 (Annex-B byte-stream) on stdout;
// the Go process is the single consumer of that stream.
//
// The whole capture->encode chain runs in one GStreamer process - no ffmpeg,
// no intermediate container, no pipe between two subprocesses. The pipeline
// is:
//
//	gst-launch-1.0 -q pipewiresrc path=<NODE> do-timestamp=true
//	  ! videoconvert ! video/x-raw,format=NV12
//	  ! videorate ! video/x-raw,framerate=R/1
//	  ! nvh264enc bitrate=B max-bitrate=M gop-size=R preset=p1 tune=ultra-low-latency
//	  ! h264parse
//	  ! video/x-h264,stream-format=byte-stream
//	  ! fdsink fd=1 sync=false
//
// where R is the clean output framerate. GStreamer stamps each frame with its
// actual arrival time (do-timestamp=true) and the videorate element
// resamples the variable-rate source to a clean CFR stream whose timeline is
// the arrival timeline = real time. This keeps the stream timeline locked to
// real time 1:1 even when the source's delivery rate varies (the 180Hz
// compositor only pushes frames when the screen changes, so delivery is
// ~50-79fps, not a fixed rate).
//
// The PipeWire node is process-scoped: only the process that owns the Mutter
// session can read it, which is why Go holds the session and spawns the
// GStreamer pipeline.
//
// The pipeline's stdout is a continuous raw H.264 Annex-B byte stream
// (start-code delimited access units). The Go pump (pumpRaw) splits it into
// access units and fans each one out to two consumers:
//
//  1. the raw WebSocket path (PRIMARY): each access unit is one binary WS
//     message to /raw clients, which decode it with WebCodecs
//     (VideoDecoder) and render to a <canvas> as it arrives - no container,
//     no fragment buffering, ~1-2 frames of end-to-end latency.
//  2. the fMP4 fallback path: a Go fMP4 muxer (fmp4Muxer) re-muxes the
//     access units into fragmented MP4 (ftyp+moov init segment, then
//     moof+mdat fragments) which is pumped to the relay's StreamServer so
//     /stream HTTP clients pipe it into a MediaSource/SourceBuffer. The
//     init segment (ftyp+moov with avcC) is emitted once, up front, as soon
//     as the first SPS/PPS pair is seen; fragments are cut at IDR
//     boundaries (keyframe-aligned) or after maxFragmentFrames frames.
type Capture struct {
	conn *dbus.Conn // session bus connection
	obj  dbus.BusObject // session object, stopped on teardown

	node uint32 // PipeWire node ID from PipeWireStreamAdded
	width  int // capture width
	height int // capture height
	outFPS int // clean output framerate (60); videorate resample target + gop-size

	// Encoder rate-control parameters (overridable via COAXIAL_BITRATE,
	// COAXIAL_MAXRATE). nvh264enc uses a VBR target (bitrate, in kbps)
	// with an optional peak (maxrate) for motion bursts.
	bitrate string // target VBR bitrate in kbps, e.g. "8000"
	maxrate string // peak bitrate in kbps allowed for motion bursts

	// monitorID is the xrandr output name to record ("" = primary). A named
	// output that cannot be found falls back to the primary. The pipeline's
	// RecordMonitor call passes this as the monitor argument.
	monitorID string
	// scale is the output scale factor (0.25-1.0); 1.0 = full resolution. The
	// pipeline's cudascale stage downscales to width x height.
	scale float64

	// gst holds the single GStreamer pipeline (capture→NVENC→raw H.264),
	// terminated on teardown. pgid is the process-group id the pipeline runs
	// in (Setpgid) so teardown can signal the whole group - the pipeline
	// spawns child processes (CUDA/NVENC workers) that a single-process
	// signal would leave orphaned.
	gst  *exec.Cmd
	pgid int

	// muxer is the Go fMP4 muxer that re-muxes the raw H.264 access units
	// into fragmented MP4 for the /stream MSE fallback path. Created in
	// launch; Close() is called on teardown.
	muxer *fmp4Muxer

	// lastAU is the arrival time (pump goroutine, the instant the access unit
	// is emitted from the GStreamer pipeline) and size of the most recent
	// access unit. It feeds the per-frame encode-delivery diagnostics: the
	// gap between consecutive AUs is the encoder's delivery cadence (a spike
	// = a frame the encoder held back). dbgArrivalWindow holds the last
	// 300 arrival intervals (ms) for a max/p99 summary at teardown.
	lastAU         time.Time
	lastAUSize     int
	dbgArrivalWindow []float64

	// teardownOnce ensures the pipeline and mutter session are stopped exactly
	// once.
	teardownOnce sync.Once
}

// primaryMonitor returns the resolution and refresh rate of the primary
// monitor as reported by xrandr. The primary line is the one marked
// "primary" (falling back to the first connected output), e.g.
//
//	DP-7 connected primary 3440x1440+0+0 ... 179.92*+
//
// It returns (width, height, fps, ok). ok is false if xrandr is unavailable
// or no connected monitor is found, in which case the caller falls back to
// the defaultWidth/defaultHeight/defaultOutFPS constants.
func primaryMonitor() (width, height, fps int, ok bool) {
	out, err := exec.Command("xrandr", "--query").Output()
	if err != nil {
		return 0, 0, 0, false
	}
	// The connected line names the monitor and its resolution; the active
	// mode (with the "*+" marker) on the following lines carries the refresh
	// rate, e.g. "   3440x1440    179.92*+".
	connectedRe := regexp.MustCompile(`^(\S+) connected(?: primary)? (\d+)x(\d+)\+`)
	modeRe := regexp.MustCompile(`^\s*(\d+)x(\d+)\s+(\d+(?:\.\d+)?)\*\+`)
	var curW, curH int // resolution of the most recent connected line
	var curIsPrimary bool
	var hasPrimary, hasFallback bool
	var pw, ph, pfps int // primary output resolution + refresh rate
	var fw, fh, ffps int // first non-primary output (fallback)
	for _, line := range strings.Split(string(out), "\n") {
		if m := connectedRe.FindStringSubmatch(line); m != nil {
			curW, _ = strconv.Atoi(m[2])
			curH, _ = strconv.Atoi(m[3])
			curIsPrimary = strings.Contains(line, "primary")
			continue
		}
		// A mode line: when it is the active ("*+") mode of the current
		// output, record its refresh rate. The primary output is preferred;
		// the first non-primary output is a fallback.
		if fm := modeRe.FindStringSubmatch(line); fm != nil {
			mw, _ := strconv.Atoi(fm[1])
			mh, _ := strconv.Atoi(fm[2])
			if mw == curW && mh == curH {
				f, _ := strconv.ParseFloat(fm[3], 64)
				if curIsPrimary && !hasPrimary {
					pw, ph, pfps, hasPrimary = curW, curH, int(f+0.5), true
				} else if !curIsPrimary && !hasFallback {
					fw, fh, ffps, hasFallback = curW, curH, int(f+0.5), true
				}
			}
		}
	}
	if hasPrimary {
		return pw, ph, pfps, true
	}
	if hasFallback {
		return fw, fh, ffps, true
	}
	return 0, 0, 0, false
}

// virtualScreen returns the full multi-monitor virtual-screen geometry: the
// bounding box of all connected outputs (vw, vh) and the offset (ox, oy) of
// the primary monitor within that box. The streaming video shows ONLY the
// primary monitor, but the compositor (Mutter) maps an absolute pointer's
// full coordinate space onto the ENTIRE virtual screen. So the input device's
// ABS range must span the full virtual screen, and the client's normalized
// [0,1] video coordinates must be mapped into the primary monitor's region
// (ox, oy)-(ox+pw, oy+ph) of that space. Returns ok=false if xrandr is
// unavailable or no connected monitor is found.
func virtualScreen() (vw, vh, ox, oy, pw, ph int, ok bool) {
	out, err := exec.Command("xrandr", "--query").Output()
	if err != nil {
		return 0, 0, 0, 0, 0, 0, false
	}
	connectedRe := regexp.MustCompile(`^(\S+) connected(?: primary)? (\d+)x(\d+)\+(\d+)\+(\d+)`)
	var maxW, maxH int
	var found bool
	var pW, pH, pOx, pOy int
	var hasPrimary, hasFirst bool
	for _, line := range strings.Split(string(out), "\n") {
		if m := connectedRe.FindStringSubmatch(line); m != nil {
			w, _ := strconv.Atoi(m[2])
			h, _ := strconv.Atoi(m[3])
			x, _ := strconv.Atoi(m[4])
			y, _ := strconv.Atoi(m[5])
			found = true
			if x+w > maxW {
				maxW = x + w
			}
			if y+h > maxH {
				maxH = y + h
			}
			if strings.Contains(line, "primary") && !hasPrimary {
				pW, pH, pOx, pOy = w, h, x, y
				hasPrimary = true
			} else if !hasFirst {
				// First connected output (top-left) as a primary fallback.
				pW, pH, pOx, pOy = w, h, x, y
				hasFirst = true
			}
		}
	}
	if !found {
		return 0, 0, 0, 0, 0, 0, false
	}
	return maxW, maxH, pOx, pOy, pW, pH, true
}

// newCapture builds a Capture from the given parameters. The resolution is
// the selected monitor's (p.MonitorID, "" = primary) clamped to the maximum
// stream constraints (maxWidth/maxHeight/maxFPS) and scaled by p.Scale (the
// pipeline's cudascale stage does the actual downscale). The
// COAXIAL_WIDTH/HEIGHT/FPS/BITRATE/MAXRATE environment variables are
// applied as overrides on top of the params (env wins), so the initial
// capture (from defaultCaptureParams) can still be tuned without a client.
// A monitor that cannot be found falls back to the primary, and to the
// defaultWidth/defaultHeight/defaultOutFPS constants if xrandr is
// unavailable.
func newCapture(p captureParams) *Capture {
	c := &Capture{
		bitrate:   p.Bitrate,
		maxrate:   p.Maxrate,
		outFPS:    p.FPS,
		monitorID: p.MonitorID,
		scale:     p.Scale,
	}
	
	// Clamp the scale to a sane range (0.25 = quarter resolution, 1.0 = full).
	if c.scale < 0.25 {
		c.scale = 0.25
	}
	if c.scale > 1.0 {
		c.scale = 1.0
	}

	// Select the monitor to record. "" = primary (fall back to the first
	// connected output); a named output is looked up in xrandr. A monitor
	// that cannot be found falls back to the primary.
	rawW, rawH, rawFPS, ok := monitorByName(p.MonitorID)
	if !ok {
		log.Printf("capture: monitor %q not found; falling back to defaults %dx%d@%d", p.MonitorID, defaultWidth, defaultHeight, defaultOutFPS)
		rawW, rawH, rawFPS = defaultWidth, defaultHeight, defaultOutFPS
		c.monitorID = ""
	}
	if rawFPS <= 0 {
		rawFPS = defaultOutFPS
	}

	// Clamp the actual monitor to the maximum stream constraints.
	c.width = rawW
	c.height = rawH
	// c.outFPS = rawFPS
	if c.width > maxWidth {
		c.width = maxWidth
	}
	if c.height > maxHeight {
		c.height = maxHeight
	}
	if c.outFPS > maxFPS {
		c.outFPS = maxFPS
	}

	// Environment overrides (the initial capture can be tuned without a
	// client; the client's settings channel wins for runtime changes).
	if v := os.Getenv("COAXIAL_WIDTH"); v != "" {
		fmt.Sscanf(v, "%d", &c.width)
	}
	if v := os.Getenv("COAXIAL_HEIGHT"); v != "" {
		fmt.Sscanf(v, "%d", &c.height)
	}
	if v := os.Getenv("COAXIAL_FPS"); v != "" {
		fmt.Sscanf(v, "%d", &c.outFPS)
	}
	if v := os.Getenv("COAXIAL_BITRATE"); v != "" {
		c.bitrate = v
	}
	if v := os.Getenv("COAXIAL_MAXRATE"); v != "" {
		c.maxrate = v
	}

	// Scale the output resolution (e.g. 0.5 = half resolution). This reduces
	// the encode load and bandwidth at the cost of detail; the pipeline's
	// cudascale stage does the actual downscale to c.width x c.height.
	if c.scale != 1.0 {
		c.height = int(float64(c.height) * c.scale)
		c.width = int(float64(c.width) * c.scale)
	}
	log.Printf("capture: monitor %q %dx%d@%dHz -> stream %dx%d@%d fps (max %dx%d@%d, scale %.2f, bitrate %skbps, maxrate %skbps)",
		p.MonitorID, rawW, rawH, rawFPS, c.width, c.height, c.outFPS, maxWidth, maxHeight, maxFPS, c.scale, c.bitrate, c.maxrate)

	log.Printf("capture FPS: %d source - %d param - %d capture - %d max", rawFPS, p.FPS, c.outFPS, maxFPS)


	return c
}

// run executes the capture lifecycle:
//   1. connect to the session bus + create a Mutter session
//   2. record a monitor and wait for the PipeWire node id
	//   3. launch the single GStreamer pipeline (capture, NVENC, fMP4)
	//   4. pump the fMP4 byte stream to rs.stream until the pipeline dies
//
// It is called in a goroutine by ensureCapture. It returns false if it failed
// to start the pipeline.
func (c *Capture) run(rs *RelayServer) bool {
	conn, err := dbus.ConnectSessionBus()
	if err != nil {
		log.Printf("capture: failed to connect to session bus: %v", err)
		return false
	}
	defer conn.Close()
	c.conn = conn

	mutterObj := conn.Object(mutterDest, mutterPath)

	// Create the session.
	var sessionPath dbus.ObjectPath
	if err := mutterObj.Call(
		"org.gnome.Mutter.ScreenCast.CreateSession", 0, map[string]dbus.Variant{}).
		Store(&sessionPath); err != nil {
		log.Printf("capture: CreateSession failed: %v", err)
		return false
	}
	sessionObj := conn.Object(mutterDest, sessionPath)
	defer sessionObj.Call("org.gnome.Mutter.ScreenCast.Session.Stop", 0)
	c.obj = sessionObj

	// Record the monitor (cursor-mode = 0 = no cursor).
	// so we don't see that lag in the cursor input
	var streamPath dbus.ObjectPath
	recordProps := map[string]dbus.Variant{
		"cursor-mode": dbus.MakeVariant(uint32(0)),
	}
	// The monitor argument
	// is the xrandr output name to capture ("" = the primary monitor). This is
	// how the settings channel's display switch works: a new capture is
	// started with the selected monitor's name.
	if err := sessionObj.Call(
		"org.gnome.Mutter.ScreenCast.Session.RecordMonitor", 0, c.monitorID, recordProps).
		Store(&streamPath); err != nil {
		log.Printf("capture: RecordMonitor (monitor %q) failed: %v", c.monitorID, err)
		return false
	}

	// Listen for the PipeWireStreamAdded signal on the stream object.
	rule := fmt.Sprintf(
		"type='signal',sender='%s',path='%s',interface='org.gnome.Mutter.ScreenCast.Stream',member='PipeWireStreamAdded'",
		mutterDest, streamPath,
	)
	if call := conn.BusObject().Call("org.freedesktop.DBus.AddMatch", 0, rule); call.Err != nil {
		log.Printf("capture: failed to add D-Bus match rule: %v", call.Err)
	}
	signals := make(chan *dbus.Signal, 10)
	conn.Signal(signals)

	// Start the session so the compositor publishes the node.
	if err := sessionObj.Call(
		"org.gnome.Mutter.ScreenCast.Session.Start", 0).Err; err != nil {
		log.Printf("capture: Start failed: %v", err)
		return false
	}

	// Wait for the node id (process-scoped PipeWire node).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			log.Printf("capture: timed out waiting for PipeWireStreamAdded signal")
			return false
		case sig := <-signals:
			if sig.Path == streamPath && sig.Name == "org.gnome.Mutter.ScreenCast.Stream.PipeWireStreamAdded" {
				if len(sig.Body) > 0 {
					if id, ok := sig.Body[0].(uint32); ok {
						c.node = id
						log.Printf("capture: PipeWire node %d; launching GStreamer pipeline", id)
						return c.launch(rs)
					}
				}
			}
		}
	}
}

// launch spawns a single GStreamer pipeline (capture, NVENC, raw H.264) and
// pumps the live raw H.264 Annex-B byte stream until the pipeline dies. It
// blocks until the pipeline exits.
//
// The whole chain runs in one GStreamer process: no ffmpeg, no intermediate
// container, no pipe between two subprocesses. GStreamer stamps each frame
// with its actual arrival time (do-timestamp=true), resamples the
// variable-rate source to a clean CFR (videorate), and encodes on the GPU
// (nvautogpuh264enc). h264parse normalises the bitstream to Annex-B
// (byte-stream) format with 4-byte start codes and re-emits the SPS/PPS
// before each IDR, so a client joining mid-stream still gets its decoder
// configuration. The pipeline's stdout is raw H.264 - no mp4mux, no tee,
// no FIFO - and the Go pump is the single consumer.
//
// Pipeline:
//
//	pipewiresrc path=N do-timestamp=true
//	  ! videoconvert ! video/x-raw,format=NV12
//	  ! videorate ! video/x-raw,framerate=R/1
//	  ! nvh264enc bitrate=B max-bitrate=M gop-size=R preset=p1 tune=ultra-low-latency
//	  ! h264parse
//	  ! video/x-h264,stream-format=byte-stream
//	  ! fdsink fd=1 sync=false
//
// Why each stage:
//   - do-timestamp=true: stamps each frame with its arrival stream time (the
//     real-time moment the frame was delivered). The 180Hz compositor only
//     pushes a frame when the screen changes, so delivery is ~50-79fps, not
//     a fixed rate; carrying the arrival timestamps is what keeps the stream
//     timeline locked to real time (1:1) instead of drifting.
//   - videoconvert to NV12: the source is BGRx (32-bit, no chroma
//     subsampling); NV12 is the 4:2:0 format the encoder expects and the
//     universal H.264 baseline the client's decoder handles well. The
//     explicit video/x-raw caps after videoconvert pin the output format so
//     the encoder receives a consistent pixel format.
//   - videorate to outFPS/1: resamples the variable-rate (50-79fps) source
//     to a clean CFR. Because the input timestamps are real arrival times,
//     the output timeline tracks real time exactly (1s content per 1s wall
//     clock) regardless of how the delivery rate fluctuates.
//   - nvautogpuh264enc: NVENC H.264 encode on the GPU (no ffmpeg, no CPU
//     transcoding). bitrate/maxrate are the VBR target/peak in kbps.
//     gop-size = outFPS (1s of output timeline) so keyframes land every
//     second.
//   - h264parse: guarantees clean 4-byte start codes and re-emits SPS/PPS
//     before each IDR. stream-format=byte-stream (Annex-B) is what both the
//     Go pump and the client's WebCodecs VideoDecoder expect; the AVCC
//     (length-prefixed) format is for container muxers only.
//   - fdsink fd=1 sync=false: raw H.264 bytes on stdout, written as soon as
//     h264parse emits them (no sync element buffering).
//
// The pump (below) splits the byte stream into access units and fans each
// one out to the raw WS subscribers (rs.raw, the primary path) and the Go
// fMP4 muxer (rs.stream, the MSE fallback path).
func (c *Capture) launch(rs *RelayServer) bool {

	gstCmd := exec.Command(
		"gst-launch-1.0", "-q",
		"pipewiresrc",
		fmt.Sprintf("path=%d", c.node),
		"do-timestamp=true",
		"always-copy=false",
		"!", "videorate", "drop-only=true",
		"!", fmt.Sprintf("video/x-raw,format=BGRx,framerate=%d/1", c.outFPS),
		"!", "queue", "max-size-buffers=1", "max-size-bytes=0", "max-size-time=0", "leaky=downstream",
		"!", "cudaupload",
		"!", "cudascale",
		"!", fmt.Sprintf("video/x-raw(memory:CUDAMemory),format=BGRx,width=%d,height=%d", int(float64(c.width)), int(float64(c.height))),
		"!", "queue", "max-size-buffers=1", "max-size-bytes=0", "max-size-time=0", "leaky=downstream",
		"!", "nvautogpuh264enc", "preset=p1", "tune=ultra-low-latency", "rc-mode=cbr", 
		"zerolatency=true",
		"bframes=0",
		"rc-lookahead=0",   // Disables rate-control lookahead queue
    	"aud=false",        // Stops emitting Access Unit Delimiters
		fmt.Sprintf("bitrate=%s", c.bitrate),
		fmt.Sprintf("max-bitrate=%s", c.maxrate),
		fmt.Sprintf("gop-size=%d", c.outFPS),
		"!", "video/x-h264,profile=constrained-baseline,level=(string)4.1",
		"!", "h264parse", "config-interval=-1",
		"!", "video/x-h264,stream-format=byte-stream,alignment=au",
		"!", "fdsink", "fd=1", "sync=false",
	)

	gstCmd.Stderr = os.Stderr
	

	// Pick the encoder GPU: prefer NVIDIA (for NVENC), choosing the device
	// with the most free VRAM. nvh264enc inits CUDA across every visible
	// GPU, so restricting CUDA_VISIBLE_DEVICES to the chosen device avoids
	// the OOM that a busy high-VRAM device would otherwise cause.
	if gpu := selectEncoderGPU(); gpu != "" {
		gstCmd.Env = append(gstCmd.Environ(), "CUDA_VISIBLE_DEVICES="+gpu)
		log.Printf("capture: using encoder GPU device %s (CUDA_VISIBLE_DEVICES)", gpu)
	} else {
		log.Printf("capture: no encoder GPU selected; nvh264enc may fail to init")
	}

	// Attach the pipeline's stdout (raw H.264 byte stream) before starting.
	stdout, err := gstCmd.StdoutPipe()
	if err != nil {
		log.Printf("capture: failed to attach gst-launch stdout: %v", err)
		c.teardown()
		return false
	}

	// Run the pipeline in its own process group so teardown can signal the
	// whole group (the pipeline spawns child processes that a single-process
	// signal would leave orphaned).
	gstCmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := gstCmd.Start(); err != nil {
		log.Printf("capture: failed to start gst-launch: %v", err)
		c.teardown()
		return false
	}
	c.gst = gstCmd
	c.pgid = gstCmd.Process.Pid
	log.Printf("capture: stream %dx%d @ %d fps", c.width, c.height, c.outFPS)
	log.Printf("capture: gst-launch (pid %d, pgid %d) started", gstCmd.Process.Pid, c.pgid)

	// Pump the live raw H.264 byte stream until the pipeline exits (EOF) or
	// is torn down. The stream is a continuous Annex-B byte stream; we read
	// it in chunks, split it into access units (start-code delimited), and
	// fan each access unit out to the two consumers:
	//
	//  1. rs.raw: the raw WebSocket path (primary). Each access unit is one
	//     binary WS message; a slow client simply drops the unit
	//     (best-effort live stream) rather than blocking the encoder.
	//  2. the fMP4 muxer: re-muxes the access units into fragmented MP4 for
	//     the /stream MSE fallback path. The muxer is created here with a
	//     callback that pushes each emitted box group (init segment or
	//     moof+mdat fragment) to rs.stream, and Close() is called when the
	//     pump exits so any trailing fragment is flushed.
	muxer := newFmp4Muxer(c.outFPS, func(boxGroup []byte) {
		rs.stream.write(boxGroup)
	}, func(initSeg []byte) {
		// Store the init segment for late-joining /stream subscribers and
		// broadcast it to current subscribers (they already have it via emit,
		// so this is a no-op for them; new subscribers get it via
		// rs.stream.serveInit when they register).
		rs.storeInitSegment(initSeg)
	})
	c.muxer = muxer
	acc := newAccessUnitAccumulator()
	const readBufferSize = 64 * 1024
	buf := make([]byte, readBufferSize)
	// trackAU records the arrival time + size of an access unit just emitted
	// from the GStreamer pipeline (the pump goroutine, the instant the AU is
	// ready to fan out). The gap between consecutive arrivals is the encoder's
	// delivery cadence: at a clean CFR stream it is ~1000/outFPS ms; a spike
	// means the encoder held a frame back (NVENC backpressure, a large
	// keyframe, or a compositor stall). A spike over 50ms is a real stutter
	// moment and is logged with the preceding gap so the cause can be chased
	// (e.g. "is it once per GOP at a keyframe?"). Only the pump goroutine
	// calls this, so no lock is needed.
	trackAU := func(au []byte) {
		now := time.Now()
		if !c.lastAU.IsZero() {
			gap := now.Sub(c.lastAU).Seconds() * 1000
			// if gap > 50 {
			// 	log.Printf("capture: AU gap %.1fms size=%d - encoder held a frame",
			// 		gap, len(au))
			// }
			c.dbgArrivalWindow = append(c.dbgArrivalWindow, gap)
			if len(c.dbgArrivalWindow) > 300 {
				c.dbgArrivalWindow = c.dbgArrivalWindow[1:]
			}
		}
		c.lastAU = now
		c.lastAUSize = len(au)
	}
	// fanOut is the common access-unit fan-out: raw WS (primary) + fMP4 muxer
	// (fallback, gated on live MSE clients) + arrival tracking.
	fanOut := func(au []byte) {
		trackAU(au)
		// Assert constraint_set2_flag (0x20) on the SPS constraint byte so the
		// client's hardware WebCodecs decoder dispatches frames instantly (no
		// 8-frame reordering buffer). The patch is in-place on the access unit
		// and idempotent (constraint_set2 is OR'd in), so it is safe to apply
		// to every access unit (most carry no SPS and are no-ops). Both the raw
		// WS path (rs.raw) and the fMP4 muxer (noteSPSPPS -> avcC) see the
		// patched SPS, so the client's avc1.PPCCLL codec string and the MSE
		// path's avcC both carry the constraint_set2 flag.
		patchSPSConstrainedBaseline(au)
		rs.raw.write(au)
		muxer.noteSPSPPS(au)
		if rs.streamClientCount() > 0 {
			muxer.feed(au)
		}
		// bumpFrame AFTER feed: feed reads m.frameCount as the DTS
		// index of the current AU (number of AUs seen before it), so
		// the counter must not yet include this AU.
		muxer.bumpFrame()
	}
	for {
		n, err := stdout.Read(buf)
		if n > 0 {
			// Feed the chunk to the access-unit splitter; each complete
			// access unit is fanned out to the raw WS subscribers (always,
			// the primary path) and conditionally to the fMP4 muxer (only
			// while at least one /stream MSE client is connected, so we
			// don't build moof+mdat fragments for nobody). The splitter is
			// stateful across chunks (an access unit may straddle a chunk
			// boundary).
			//
			// noteSPSPPS + bumpFrame run unconditionally so the muxer always
			// has the first SPS/PPS pair (to emit the init segment when the
			// first MSE client joins) and a correct frame index (so a
			// fragment's DTS reflects the true frame index even after a
			// period with no MSE clients). feed (the expensive fragment
			// assembly + emit) is gated on the live MSE client count.
			acc.feed(buf[:n], fanOut)
		}
		if err != nil {
			// EOF or read error: the pipeline exited.
			break
		}
	}
	// Flush any pending access unit held in the accumulator (the trailing
	// access unit whose payload was held back waiting for more bytes), then
	// flush the muxer: emit any pending fragment and close the writer so
	// the /stream clients see a clean stream end (FIN).
	acc.flush(fanOut)
	muxer.Close()
	// Log a delivery-cadence summary from the rolling arrival window so the
	// steady-state encode cadence and its worst case are visible at teardown.
	if len(c.dbgArrivalWindow) > 1 {
		w := append([]float64(nil), c.dbgArrivalWindow...)
		sort.Float64s(w)
		n := len(w)
		p99 := w[int(float64(n-1)*0.99)]
		maxGap := w[n-1]
		log.Printf("capture: AU delivery summary n=%d p99=%.1fms max=%.1fms (target ~%.1fms)",
			n, p99, maxGap, 1000.0/float64(c.outFPS))
	}

	// Wait for the pipeline to exit.
	if err := gstCmd.Wait(); err != nil {
		log.Printf("capture: gst-launch exited: %v", err)
	} else {
		log.Printf("capture: gst-launch exited cleanly")
	}
	return true
}

// accessUnitAccumulator splits a continuous raw H.264 Annex-B byte stream
// into access units. An access unit is the run of NAL units from one start
// code up to the next start code that begins a slice (NAL type 1 = non-IDR
// slice, type 5 = IDR slice). SPS (type 7), PPS (type 8) and SEI (type 9)
// NAL units preceding a slice belong to that slice's access unit (they are
// emitted just before the slice by h264parse).
//
// The accumulator is stateful across chunks: an access unit may straddle a
// chunk boundary, and a start code may be split across two reads (e.g. a
// chunk ends with 00 00 and the next begins with 00 01). feed() appends the
// chunk to the residual buffer, scans for start codes, and invokes onAU for
// every complete access unit it now contains.
type accessUnitAccumulator struct {
	residual []byte // bytes not yet consumed as complete access units
}

// newAccessUnitAccumulator creates an empty splitter.
func newAccessUnitAccumulator() *accessUnitAccumulator {
	return &accessUnitAccumulator{}
}

// findStartCode returns the offset of the first Annex-B start code (3-byte
// 00 00 01 or 4-byte 00 00 00 01) in buf, or -1 if none. The 4-byte form is
// what h264parse emits; the 3-byte form is accepted for robustness.
func findStartCode(buf []byte) int {
	for i := 0; i+2 < len(buf); i++ {
		if buf[i] == 0 && buf[i+1] == 0 && buf[i+2] == 1 {
			// This is a 3-byte 00 00 01 match. If the preceding byte is also
			// 0, the match is actually the tail of a 4-byte 00 00 00 01
			// start code (the form h264parse emits); return the true start
			// of the start code so the access unit is framed correctly.
			start := i
			if i > 0 && buf[i-1] == 0 {
				start = i - 1
			}
			return start
		}
	}
	return -1
}

// nalType returns the NAL unit type (low 5 bits) of the first byte after a
// start code. buf must begin at the NAL unit's first byte (after the start
// code).
func nalType(buf []byte) int {
	if len(buf) == 0 {
		return -1
	}
	return int(buf[0] & 0x1F)
}

// patchSPSConstrainedBaseline asserts the SPS constraint_set2_flag (0x20) on
// the constraint byte of the first SPS NAL in au (in place), so the constraint
// byte becomes 0xE0 (was 0xC0). The patched SPS is what the client's
// buildDecoderConfig reads to build its avc1.PPCCLL codec string (the
// constraint byte is the middle pair), and what the fMP4 muxer copies into its
// avcC init segment. Asserting constraint_set2 tells the client's hardware
// WebCodecs decoder to dispatch frames instantly (no 8-frame reordering
// buffer), dropping the ~260ms decode pipeline latency to ~1 frame.
//
// au is an Annex-B access unit (4-byte start codes from h264parse). The SPS
// NAL header byte is 0x67 (forbidden=0, NRI=3, type=7). The constraint byte is
// the byte after the NAL header + profile_idc: start code (4) + header (1) +
// profile_idc (1) = offset 5 from the start code, i.e. nal[i+5] when the
// start code ends at nal[i+3]. Returns true if an SPS was found and patched.
func patchSPSConstrainedBaseline(nal []byte) bool {
	for i := 0; i+5 < len(nal); i++ {
		if nal[i] == 0x00 && nal[i+1] == 0x00 && nal[i+2] == 0x01 && (nal[i+3]&0x1F) == 7 {
			// nal[i+3] is the SPS NAL header (0x67), nal[i+4] is profile_idc
			// (0x42), nal[i+5] is the constraint byte (0xC0). Assert
			// constraint_set2_flag (0x20): 0xC0 | 0x20 = 0xE0.
			nal[i+5] |= 0x20
			return true
		}
	}
	return false
}

// feed appends a chunk to the residual and emits every complete access unit
// it now contains via onAU. The onAU callback receives a slice of the
// residual (NOT a copy): the caller must consume or copy it before the next
// feed call.
//
// The residual is always positioned at the start of a pending access unit:
// either it begins with a start code (the pending AU's first NAL) or it is
// the tail of a NAL whose start code was already consumed. An access unit is
// the run of NAL units from the current position up to the next start code
// (the start code of the following access unit); it ends with the NAL unit
// that begins a slice (type 1 or 5), and any SPS/PPS/SEI NALs preceding
// that slice belong to the slice's access unit. When the buffer holds only
// SPS/PPS/SEI NALs (no slice yet), they are held until the slice's start
// code arrives so the config NALs and their slice are emitted together.
func (a *accessUnitAccumulator) feed(chunk []byte, onAU func([]byte)) {
	a.residual = append(a.residual, chunk...)
	buf := a.residual
	for {
		// Locate the leading start code (if the buffer begins with one). The
		// pending access unit's data starts immediately after it. If the
		// buffer does not begin with a start code, auStart is 0 (the pending
		// AU's data is the leading bytes, its start code already consumed).
		// The pending access unit begins at the leading start code (if
		// any); its first NAL starts immediately after it. Scan the NAL
		// units in order (the leading NAL first, then subsequent NALs at
		// their start codes). The access unit is complete at the first NAL
		// that begins a slice (type 1 or 5); SPS/PPS/SEI NALs preceding
		// that slice belong to the slice's access unit.
		//
		// Check the leading NAL first.
		lead := startCodeLen(buf)
		nt := nalType(buf[lead:])
		if nt == 1 || nt == 5 {
			// The leading NAL is a slice. It must have at least one payload
			// byte in the buffer (a slice NAL with only its type byte is
			// incomplete - the payload spans into the next chunk).
			if len(buf) <= lead+1 {
				return
			}
			// The access unit ends at the next start code (the start code
			// of the following access unit). If there is no next start code
			// in the buffer, the slice's payload may span into the next
			// chunk - hold the access unit and wait for more bytes.
			sc := findStartCode(buf[lead:])
			if sc < 0 {
				return
			}
			end := sc + lead
			onAU(buf[0:end])
			a.residual = buf[end:]
			buf = a.residual
			continue
		}
		// The leading NAL is not a slice (SPS/PPS/SEI): scan for the next
		// start code whose NAL begins a slice.
		off := lead
		for {
			sc := findStartCode(buf[off:])
			if sc < 0 {
				// No further start code: the pending access unit is
				// incomplete (no slice NAL yet) - wait for more bytes.
				return
			}
			sc += off
			lead = startCodeLen(buf[sc:])
			nt = nalType(buf[sc+lead:])
			if nt == 1 || nt == 5 {
				// The NAL at sc is a slice. It must have at least one
				// payload byte in the buffer (a slice NAL with only its
				// type byte is incomplete - the payload spans into the next
				// chunk).
				if sc+lead+1 >= len(buf) {
					return
				}
				// The pending access unit is complete. The access unit spans
				// from the buffer start to the start code of the NEXT access
				// unit (after the slice NAL). Find that start code.
				end := findStartCode(buf[sc+lead:])
				if end < 0 {
					// No start code after the slice NAL: the slice's payload
					// may span into the next chunk - hold the access unit
					// and wait for more bytes.
					return
				}
				end += sc + lead
				// Emit the access unit (including its leading start code so
				// the client sees a standard Annex-B access unit) and
				// advance the residual to the next access unit.
				onAU(buf[0:end])
				a.residual = buf[end:]
				buf = a.residual
				break
			}
			// The NAL at sc is not a slice (SPS/PPS/SEI): the access unit
			// continues past it; scan on.
			off = sc + lead
		}
	}
}

// flush emits any pending access unit held in the residual (the trailing
// access unit whose payload was held back waiting for the next chunk). It is
// called on EOF to ensure the last access unit is not lost.
func (a *accessUnitAccumulator) flush(onAU func([]byte)) {
	if len(a.residual) == 0 {
		return
	}
	onAU(a.residual)
	a.residual = a.residual[:0]
}

// startCodeLen returns the length of the start code at the head of buf, or 0
// if buf does not begin with a start code.
func startCodeLen(buf []byte) int {
	if len(buf) >= 4 && buf[0] == 0 && buf[1] == 0 && buf[2] == 0 && buf[3] == 1 {
		return 4
	}
	if len(buf) >= 3 && buf[0] == 0 && buf[1] == 0 && buf[2] == 1 {
		return 3
	}
	return 0
}

// selectEncoderGPU returns the CUDA device index to restrict the NVENC
// encoder (nvh264enc) to, preferring an NVIDIA GPU and choosing the one with
// the most free VRAM. It returns "" if no NVIDIA GPU is found or nvidia-smi
// is absent.
//
// The two NVIDIA dGPUs on this box share the CUDA context, so the encoder
// inits every visible GPU; the busy high-VRam RTX PRO 4000 (GPU 0)
// frequently runs out of memory during init. Pinning to the most-freed
// NVIDIA device avoids that while still using NVENC.
func selectEncoderGPU() string {
	// Allow an explicit override so tests/other boxes need not depend on the
	// nvidia-smi CLI.
	if v := os.Getenv("COAXIAL_CUDA_DEVICE"); v != "" {
		return v
	}

	if _, err := exec.LookPath("nvidia-smi"); err != nil {
		log.Printf("capture: nvidia-smi not found; cannot select encoder GPU")
		return ""
	}

	out, err := exec.Command("nvidia-smi", "--query-gpu", "index,memory.free", "--format=csv,noheader").Output()
	if err != nil {
		log.Printf("capture: nvidia-smi failed: %v", err)
		return ""
	}

	var best string
	var bestFree int64
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Split(line, ",")
		if len(fields) < 2 {
			continue
		}
		idx := strings.TrimSpace(fields[0])
		freeStr := strings.TrimSpace(fields[1])
		var free int64
		if _, err := fmt.Sscanf(freeStr, "%d", &free); err != nil {
			free = 0
		}
		// Prefer NVIDIA (any index is NVIDIA here); pick the most free VRAM.
		if free > bestFree {
			best = idx
			bestFree = free
		}
	}
	return best
}

// teardown terminates the GStreamer pipeline subprocess and stops the Mutter
// session. Safe to call more than once.
func (c *Capture) teardown() {
	c.teardownOnce.Do(func() {
		// Flush and close the fMP4 muxer so any trailing fragment is written
		// and the /stream clients see a clean stream end (FIN). The raw pump
		// goroutine exits on its own when the pipeline's stdout hits EOF.
		if c.muxer != nil {
			c.muxer.Close()
			c.muxer = nil
		}
		// Signal the whole pipeline process group (Setpgid), not just the
		// gst-launch process: the pipeline spawns child processes (CUDA/NVENC
		// workers) that a single-process signal would leave orphaned.
		if c.pgid != 0 {
			_ = syscall.Kill(-c.pgid, syscall.SIGINT)
			// Guarantee: if the group ignores SIGINT (and the pump goroutine
			// stays blocked on its stdout read), force-kill it after a short
			// grace period. Kill is a no-op if the group already exited.
			go func(pgid int) {
				time.Sleep(2 * time.Second)
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
			}(c.pgid)
		} else if c.gst != nil && c.gst.Process != nil {
			_ = c.gst.Process.Signal(os.Interrupt)
		}
		if c.obj != nil {
			if err := c.obj.Call("org.gnome.Mutter.ScreenCast.Session.Stop", 0).Err; err != nil {
				log.Printf("capture: failed to stop Mutter session: %v", err)
			}
		}
	})
}
