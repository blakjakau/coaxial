# coaxial

Remote-side server that captures the screen, encodes it (NVENC, GStreamer), and
serves the live stream to a browser client. It also relays the browser's
key/mouse input back to the remote machine via a Linux **uinput** virtual
device.

## Video paths

- `/raw` (PRIMARY): raw H.264 (Annex-B) access units over a WebSocket, one
  binary message per access unit. WebCodecs-capable browsers (Chrome 94+)
  decode each access unit with `VideoDecoder` and render it to a `<canvas>` as
  it arrives (~1–2 frames of end-to-end latency).
- `/stream` (fallback): the same access units re-muxed by a Go fMP4 muxer into
  fragmented MP4, served over plain HTTP for browsers without WebCodecs
  (MediaSource/`SourceBuffer`).
- `/relay`: WebSocket control channel (ping/pong keepalive + key/mouse input).

The browser connects to `/raw` (or `/stream`) for video and to `/relay` for
the control channel.

## Configuration (environment)

| Variable | Default | Meaning |
|---|---|---|
| `COAXIAL_ADDR` | `:8080` | listen address |
| `COAXIAL_CLIENT_DIR` | `client` | directory with `index.html` + `app.js` |
| `COAXIAL_BEARER` | *(empty)* | shared bearer token for the WS handshake. **Empty = auth disabled** (the server logs a warning). |
| `COAXIAL_DISABLE_CAPTURE` | *(unset)* | when set, skip the capture pipeline (Mutter/PipeWire/GStreamer) and serve the control channel only — useful for testing without the heavy capture path. |
| `COAXIAL_WIDTH` / `COAXIAL_HEIGHT` | monitor resolution | initial capture resolution (clamped to the display's maximum). The client's settings channel can change these at runtime. |
| `COAXIAL_FPS` | `60` | clean output framerate (the stream is resampled to this via `videorate`). |
| `COAXIAL_BITRATE` | `5500` | target VBR bitrate in kbps for `nvh264enc`. |
| `COAXIAL_MAXRATE` | `14400` | peak bitrate in kbps for `nvh264enc`. |
| `COAXIAL_CUDA_DEVICE` | *(auto)* | pin the NVENC encoder to a specific CUDA device index. By default the most-freed NVIDIA device is selected via `nvidia-smi` (a busy high-VRAM GPU often runs out of memory during init). |
| `COAXIAL_REL_SENSITIVITY` | `1.0` | multiplier applied to the client's pointer-lock deltas (capture mode) before they are emitted as `REL_X`/`REL_Y` events. `1.0` is a 1:1 CSS-pixel-to-screen-pixel mapping; raise it for high-DPI hosts where the remote cursor should move faster than the local pointer. |
| `COAXIAL_WHEEL_UNITS_PER_NOTCH` | `3` | `REL_WHEEL` magnitude emitted per client scroll notch. A single unit scrolls a very small amount in GNOME, so the default amplifies each notch 3× for a visible step; tune to taste. |
| `COAXIAL_DISABLE_AUDIO` | *(unset)* | when set, never start the audio pipeline (pulsesrc/opusenc GStreamer). The `/audio` WS still connects but stays silent. |
| `COAXIAL_AUDIO_SOURCE` | `@DEFAULT_MONITOR@` | the `pulsesrc` monitor device to tap. The default is `@DEFAULT_MONITOR@` (the default sink's monitor = the system audio the user hears). A specific sink can be named (e.g. `default` or a sink name from `pactl list short sinks`) to tap only that sink's output. **Do not set this to a plain source (microphone)** — a bare `pulsesrc` with no device property captures the default *source*, which is silent when the mic is unused and makes opus emit ~2–4 byte silence packets. |
| `COAXIAL_AUDIO_DUMP` | *(unset)* | when set to a path (e.g. `audio_dump.ogg`), dump the raw Ogg stream to that file for inspection (`ffmpeg -i audio_dump.ogg`, `ogginfo audio_dump.ogg`). |

## Security / trust model

The relay is a LAN / SSH-tunnel tool. The single bearer token
(`COAXIAL_BEARER`) is the only gate; if it is unset the relay is open.

**`/health` returns the bearer token in its JSON response by design.** The
browser client cannot set arbitrary HTTP headers, so it bootstraps the token
from `/health` and then passes it as a `?token=` query parameter on `/raw`,
`/stream`, and `/relay`. This means anyone who can reach `/health` can reach
the full relay — so the trust model is: **the relay is only reachable over a
trusted channel** (localhost, a private LAN, or an SSH tunnel). Returning the
token does not widen the attack surface beyond "can reach the port".

If the relay must be reachable from an untrusted network, put it behind an SSH
tunnel or a VPN; do not rely on the bearer token alone.

## Input relay (uinput) — permission setup

The input relay emulates an **absolute pointer + wheel + keyboard** as a Linux
virtual input device (`/dev/uinput`). This is the Wayland-safe mechanism (the
compositor, Mutter, sees the virtual device on both X11 and Wayland). It is a
deliberate pivot away from `xinput`/XTest, which is X11-only and a dead end on
Wayland.

The device is created **lazily on the first input event**, so a relay that only
serves video never touches `/dev/uinput`. If `/dev/uinput` is unavailable, the
relay degrades to video-only (input events are no-ops) and logs the failure.

### `/dev/uinput` is root-owned

`/dev/uinput` is owned by `root` and is not normally accessible to a regular
user. You need **one** of:

**Option A — run the relay as root** (simplest for a trusted machine):

```sh
sudo ./coaxial
```

**Option B — grant the logged-in seat access via a udev rule** (recommended):

```sh
# Allow the seat to access /dev/uinput (uaccess tag => logged-in users get it)
echo 'KERNEL=="uinput", SUBSYSTEM=="misc", TAG+="uaccess", OPTIONS+="static_node=uinput"' \
  | sudo tee /etc/udev/rules.d/60-uinput.rules

# Reload rules and (re)create the node
sudo udevadm control --reload-rules
sudo udevadm trigger

# Make sure the node exists and is world-readable (uaccess also handles this,
# but an explicit chmod is a safe belt-and-braces):
sudo chmod 666 /dev/uinput   # or: sudo chmod 660 /dev/uinput && sudo usermod -a video $USER
```

Then **log out and back in** (or `sudo udevadm control --reload && sudo udevadm trigger`)
so the new uaccess tag is applied to your session.

### Verify

```sh
# The node should exist and be accessible to your user:
ls -l /dev/uinput

# Run the relay and send a mouse move from the browser; the virtual device
# should appear:
ls /dev/input/by-id/ | grep -i coaxial
# e.g. "coaxial-pointer-event-if01"
```

When the relay is closed, the virtual device is destroyed (held keys are
released first, so no key stays stuck on the remote machine).

### What is emulated

- **Absolute pointer**: `ABS_X`/`ABS_Y` spanning the full primary-monitor
  resolution (the browser sends normalized [0,1] coordinates within the
  visible video element; the server scales them to the full screen, since the
  streamed video is a scaled-down version of the full screen).
- **Buttons**: `BTN_LEFT` / `BTN_MIDDLE` / `BTN_RIGHT`.
- **Wheel**: `REL_WHEEL` (vertical scroll, sign preserved).
- **Keyboard**: full `EV_KEY` set; the browser sends `KeyboardEvent.code`
  (e.g. `KeyA`, `Enter`, `ShiftLeft`) and the server maps it to the Linux
  `EV_KEY` code (`input_keys.go`).

### Capture mode (pointer lock)

By default the client forwards **absolute** pointer positions (normalized [0,1]
within the visible video element), which the server maps to an absolute screen
position. **Capture mode** (opt-in) switches to **relative** movement: the
pointer is locked to the video element, the local cursor is hidden, and pointer
motion is forwarded as pointer-lock deltas (`movementX`/`movementY`) that the
server scales and emits as `REL_X`/`REL_Y` events. This feels like a normal
mouse (the remote cursor moves 1:1 with the local pointer, without the local
cursor being clamped to the video element's edges) and is the natural mode for
interactive use (games, drawing, etc.).

- **Toggle**: `Ctrl+Shift+L` (intercepted before the key is forwarded to the
  host, so the toggle combination never reaches the remote machine).
- **Release**: `Escape` (the browser's native pointer-lock release). The
  `Escape` that releases the lock is swallowed so the host does not receive a
  stray `Escape` keypress. Re-locking requires a fresh `Ctrl+Shift+L` (the
  browser enforces a ~1.25s cooldown after an `Escape` release).
- **Sensitivity**: `COAXIAL_REL_SENSITIVITY` (default `1.0`) scales the
  pointer-lock deltas before they are emitted as `REL_X`/`REL_Y` events.

The virtual device registers both `ABS_X`/`ABS_Y` and `REL_X`/`REL_Y` axes on
the same node, so the client can switch between absolute (unlocked) and
relative (locked) movement without recreating the device.

## Build

```sh
go build -o bin/coaxial .
```

The browser client is static (`client/index.html` + `client/app.js`); the
server serves it from `COAXIAL_CLIENT_DIR`.
