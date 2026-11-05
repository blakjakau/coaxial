# screenrelay

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
| `SCREENRELAY_ADDR` | `:8080` | listen address |
| `SCREENRELAY_CLIENT_DIR` | `client` | directory with `index.html` + `app.js` |
| `SCREENRELAY_BEARER` | *(empty)* | shared bearer token for the WS handshake. **Empty = auth disabled** (the server logs a warning). |
| `SCREENRELAY_DISABLE_CAPTURE` | *(unset)* | when set, skip the capture pipeline (Mutter/PipeWire/GStreamer) and serve the control channel only — useful for testing without the heavy capture path. |

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
sudo ./screenrelay
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

## Build

```sh
go build -o bin/screenrelay .
```

The browser client is static (`client/index.html` + `client/app.js`); the
server serves it from `SCREENRELAY_CLIENT_DIR`.
