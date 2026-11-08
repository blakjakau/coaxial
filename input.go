// Package main - input.go
//
// uinputInjector is a self-contained Linux uinput virtual device that emulates
// an absolute pointer (mouse) + wheel + keyboard. It is the Wayland-safe
// replacement for xinput: xinput/XTest is X11-only (a dead end on Wayland),
// whereas /dev/uinput is a kernel virtual device that the compositor (Mutter)
// sees on both X11 and Wayland - the same mechanism ydotool/pynput use to
// inject input under Wayland.
//
// Why a hand-rolled device instead of a library: the two Go uinput libraries
// (sashko/go-uinput, bendahl/uinput) only expose a RELATIVE mouse (MoveX/
// MoveY) or a buttonless touchscreen. A screen relay needs an ABSOLUTE
// pointer (the browser sends screen coordinates), so we open /dev/uinput
// directly and emit ABS_X/ABS_Y + BTN_* + REL_WHEEL + EV_KEY events.
//
// Setup protocol (matches the kernel's uinput driver, drivers/input/
// misc/uinput.c):
//  1. open /dev/uinput
//  2. UI_SET_EVBIT for each event type (EV_KEY, EV_REL, EV_ABS)
//  3. UI_SET_KEYBIT for each key/button, UI_SET_RELBIT for each rel axis,
//     UI_SET_ABSBIT for each abs axis, UI_SET_PROPBIT for properties
//  4. write() a struct uinput_user_dev (name, id, absmin/absmax)
//  5. UI_DEV_CREATE
//
// Permissions: /dev/uinput is owned by root. The process needs either root or
// a udev rule granting the logged-in seat access:
//
//	echo 'KERNEL=="uinput", SUBSYSTEM=="misc", TAG+="uaccess", OPTIONS+="static_node=uinput"' \
//	  | sudo tee /etc/udev/rules.d/60-uinput.rules
//	sudo udevadm control --reload-rules && sudo udevadm trigger
//
// The device is created lazily on the first input event (so a relay that only
// serves video never touches /dev/uinput) and is created at most once.

package main

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// lastInputParseLog throttles the "parse failed" diagnostic (Unix millis).
var lastInputParseLog int64

// initRetryInterval is how long to wait after a FAILED init before attempting
// createDevice again. Long enough that a persistent failure does not hammer
// the /dev/uinput open syscall on every input event; short enough that a
// transient failure (udev rule applied after startup, node not ready) recovers
// promptly.
const initRetryInterval = 5000

// relSensitivity is the multiplier applied to the client's relative pointer
// deltas (pointer-lock movementX/movementY, in CSS pixels) before they are
// emitted as REL_X/REL_Y events. 1.0 is a 1:1 mapping (one CSS pixel of
// client movement = one REL_X/REL_Y unit on the host). The host's REL axis
// unit is screen pixels, so a value > 1.0 makes the remote cursor move faster
// than the local pointer (useful when the host screen is higher-DPI than the
// client). Tunable via COAXIAL_REL_SENSITIVITY (default 1.0).
var relSensitivity = parseRelSensitivity()

// parseRelSensitivity reads COAXIAL_REL_SENSITIVITY (a float), defaulting
// to 1.0. A non-numeric or non-positive value falls back to 1.0 (a zero or
// negative sensitivity would make the relative cursor inert or invert it).
func parseRelSensitivity() float64 {
	v := os.Getenv("COAXIAL_REL_SENSITIVITY")
	if v == "" {
		return 1.0
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		return 1.0
	}
	return f
}

// uinput ioctl request numbers (from include/uapi/linux/uinput.h).
// _IOW('U', nr, type) = (1<<30) | (sizeof(type)<<16) | ('U'<<8) | nr, 'U'=0x55.
// _IO('U', nr)        = ('U'<<8) | nr.
const (
	uiDevCreate  = 0x00005501 // _IO('U', 1)
	uiDevDestroy = 0x00005502 // _IO('U', 2)
	uiSetEvbit   = 0x40045564 // _IOW('U', 100, int)
	uiSetKeybit  = 0x40045565 // _IOW('U', 101, int)
	uiSetRelbit  = 0x40045566 // _IOW('U', 102, int)
	uiSetAbsbit  = 0x40045567 // _IOW('U', 103, int)
	uiSetPropbit = 0x4004556E // _IOW('U', 110, int)

	// Modern (kernel >= 5.9) device-description ioctls. These replace the
	// legacy write(struct uinput_user_dev) path, whose required byte count is
	// sizeof(struct uinput_user_dev) AS THE KERNEL SEES IT - an EXACT match
	// (uinput_setup_device_legacy does `if (count != sizeof(...)) return
	// -EINVAL`). That size depends on the kernel's ABS_CNT (92 + ABS_CNT*16),
	// which exceeds 54 on modern kernels, so a fixed-size write() can never
	// match. The UI_DEV_SETUP / UI_ABS_SETUP ioctls carry the same data in
	// fixed-size structs (92 and 20 bytes) that do NOT depend on ABS_CNT.
	uiDevSetup  = 0x405c5503 // _IOW('U', 3, struct uinput_setup[92])
	uiAbsSetup  = 0x401c5504 // _IOW('U', 4, struct uinput_abs_setup[28])
)

// event type / code / property constants (from input-event-codes.h / input.h).
const (
	evKey = 0x01
	evRel = 0x02
	evAbs = 0x03
	evSyn = 0x00

	synReport = 0x00

	relX      = 0x00
	relY      = 0x01
	relWheel  = 0x08 // updated from 0x0e
	relHWheel = 0x06 // added for future horizontal wheel support

	absX = 0x00
	absY = 0x01

	btnLeft   = 0x110
	btnRight  = 0x111
	btnMiddle = 0x112

	inputPropPointer = 0x00
)

// uinputSetup mirrors struct uinput_setup (linux/uinput.h), the argument to
// the UI_DEV_SETUP ioctl (kernel >= 5.9). It is 92 bytes: input_id (4 x u16
// = 8) + name[80] + ff_effects_max (4). All fields are 4-byte aligned (8, 88
// are multiples of 4), so the Go struct (4-byte alignment throughout) matches
// the C layout byte-for-byte with no padding.
type uinputSetup struct {
	ID          struct{ Bus, Vendor, Product, Version uint16 }
	Name        [80]byte
	FFEfectsMax uint32
}

// uinputAbsSetup mirrors struct uinput_abs_setup (linux/uinput.h), the
// argument to the UI_ABS_SETUP ioctl. It is 28 bytes: code (u16) + 2 bytes of
// filler (the header comments the field out: `/* __u16 filler; */`) +
// input_absinfo (6 x int32 = 24). input_absinfo has SIX fields (value,
// minimum, maximum, fuzz, flat, resolution), NOT four - omitting value/
// resolution makes the struct 8 bytes too short, so the UI_ABS_SETUP command
// number (which embeds sizeof=28) won't match the kernel's dispatch and the
// ioctl fails with EINVAL. The filler is required for 4-byte alignment of the
// absinfo field, so the Go struct (code u16, Filler u16, AbsInfo [6]int32)
// matches the C layout byte-for-byte.
type uinputAbsSetup struct {
	Code   uint16
	Filler uint16
	AbsInfo struct {
		Value      int32
		Min        int32
		Max        int32
		Fuzz       int32
		Flat       int32
		Resolution int32
	}
}

// absCnt is the number of absolute axis slots in the kernel's uinput_user_dev
// struct (ABS_CNT = 54: ABS_X=0x00 through ABS_MT_WIDTH_COARSE=0x35). It is
// only used by the LEGACY write() fallback path (kernels < 5.9 without
// UI_DEV_SETUP); the modern UI_DEV_SETUP/UI_ABS_SETUP path does not depend on
// it.
const absCnt = 54

// inputEvent mirrors struct input_event (linux/input.h). On 64-bit it is 24
// bytes: struct timeval (16) + type (2) + code (2) + value (4). We marshal it
// little-endian by hand (x86_64/ARM64 are both little-endian, matching the
// kernel's native-endian layout). The time fields are left zero: the kernel
// fills in the current time for zero-time events.
type inputEvent struct {
	Type  uint16
	Code  uint16
	Value int32
}

// uinputUserDev mirrors struct uinput_user_dev (linux/uinput.h):
//
//	struct uinput_user_dev {
//		char name[UINPUT_MAX_NAME_SIZE]; // 80
//		struct input_id id;              // 4 x u16 = 8
//		__u32 ff_effects_max;            // 4
//		__s32 absmax[ABS_CNT];           // 54 x 4 = 216
//		__s32 absmin[ABS_CNT];           // 54 x 4 = 216
//		__s32 absfuzz[ABS_CNT];          // 54 x 4 = 216
//		__s32 absflat[ABS_CNT];          // 54 x 4 = 216
//	};
//
// Total: 80 + 8 + 4 + 216*4 = 956 bytes. All fields are 4-byte aligned (80,
// 88, 92, 308, 524, 740 are all multiples of 4), so there is no padding and
// the Go struct (4-byte alignment throughout) matches the C layout
// byte-for-byte.
type uinputUserDev struct {
	Name        [80]byte
	ID          struct{ Bus, Vendor, Product, Version uint16 }
	FFEfectsMax uint32
	AbsMax      [absCnt]int32
	AbsMin      [absCnt]int32
	AbsFuzz     [absCnt]int32
	AbsFlat     [absCnt]int32
}

// uinputInjector owns one /dev/uinput fd and the virtual device it created.
// All methods are safe for concurrent use; the device is created once (lazily)
// and events are serialized by mu.
type uinputInjector struct {
	mu      sync.Mutex
	fd      int
	created bool
	// initErr is the last createDevice error, kept for diagnostics only. It is
	// NOT used to short-circuit future attempts (ensureInitLocked retries,
	// throttled by lastInitAttempt); a persistent failure is re-attempted.
	initErr error

	// lastInitAttempt throttles how often a FAILED init is retried (Unix
	// millis). A persistent failure (e.g. no /dev/uinput permission) would
	// otherwise do a failed open syscall on every single input event. We
	// retry at most once per initRetryInterval, which still recovers from
	// transient failures (udev rule applied after startup, node not ready).
	lastInitAttempt int64

	// screenW/screenH are the FULL multi-monitor virtual-screen dimensions
	// (from virtualScreen). The uinput ABS_X/ABS_Y range is set to this full
	// screen because the compositor (Mutter) maps an absolute pointer's full
	// coordinate space onto the entire virtual screen, not just the primary
	// monitor.
	screenW int
	screenH int

	// primX/primY/primW/primH are the primary monitor's region (offset + size)
	// within the full virtual screen. The streamed video shows ONLY the
	// primary monitor, so the client's normalized [0,1] pointer position maps
	// into this region of the full screen (not the whole screen).
	primX int
	primY int
	primW int
	primH int

	// pressed tracks which keys are currently held down, so that a
	// disconnect (or the relay's Close) can release them (a stuck key would
	// otherwise keep firing on the remote machine). Mirrors chromote's
	// keysDown + clearInput.
	pressed map[uint16]bool
}

// newUinputInjector returns an uninitialized injector. The device is created
// on the first event (see ensureInit).
func newUinputInjector() *uinputInjector {
	return &uinputInjector{pressed: make(map[uint16]bool)}
}

// ensureInit opens /dev/uinput and creates the virtual device exactly once.
// It is called from every public method, so the first input event triggers
// device creation. A failure is logged once and the injector becomes a no-op
// (so a missing /dev/uinput permission degrades to "video only" rather than
// crashing the relay).
func (i *uinputInjector) ensureInit() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.ensureInitLocked()
}

// ensureInitLocked is ensureInit for callers that already hold mu.
//
// The device is created at most once (created==true short-circuits), but a
// FAILED init is NOT cached: createDevice is retried on a later event (at
// most once per initRetryInterval). This matters because init can fail for
// transient reasons (e.g. /dev/uinput not ready, a udev rule applied after
// startup) - caching the first error would permanently disable input even
// after the cause is resolved. A persistent failure costs at most one
// open+ioctl attempt per interval and logs once per failed attempt.
func (i *uinputInjector) ensureInitLocked() bool {
	if i.created {
		return true
	}
	now := time.Now().UnixMilli()
	// Throttle the ATTEMPT itself: if the last init attempt was recent, do not
	// re-run createDevice (which does a /dev/uinput open + ~300 ioctls) on
	// every single input event. A persistent failure thus costs at most one
	// attempt per initRetryInterval, while a transient failure still recovers
	// within that window.
	if now-i.lastInitAttempt < initRetryInterval {
		return false
	}
	i.lastInitAttempt = now
	// The ABS device range spans the FULL multi-monitor virtual screen (the
	// compositor maps an absolute pointer's coordinate space onto the whole
	// virtual screen). The streamed video shows only the primary monitor, so
	// the client's normalized coordinates are mapped into the primary region
	// (primX/primY/primW/primH) of that space.
	vw, vh, ox, oy, pw, ph, ok := virtualScreen()
	if !ok || vw <= 0 || vh <= 0 {
		// Fall back to the primary monitor alone (single-monitor setups, or
		// xrandr unavailable): the primary region is the whole screen.
		pw, ph = defaultWidth, defaultHeight
		vw, vh = defaultWidth, defaultHeight
	}
	if pw <= 0 || ph <= 0 {
		pw, ph = vw, vh
	}
	i.screenW, i.screenH = vw, vh
	i.primX, i.primY, i.primW, i.primH = ox, oy, pw, ph
	if err := i.createDevice(); err != nil {
		i.initErr = err
		log.Printf("input: uinput init failed (video still works, input is no-op; retrying in %ds): %v", initRetryInterval/1000, err)
		return false
	}
	i.initErr = nil
	i.created = true
	log.Printf("input: uinput virtual pointer+keyboard created (virtual screen %dx%d, primary region %dx%d at +%d+%d)",
		i.screenW, i.screenH, i.primW, i.primH, i.primX, i.primY)
	return true
}

// createDevice opens /dev/uinput and builds the virtual device: an absolute
// pointer (ABS_X/ABS_Y over the full screen, BTN_LEFT/RIGHT/MIDDLE,
// REL_WHEEL) plus a full keyboard (EV_KEY 0..255).
func (i *uinputInjector) createDevice() error {
	// Open /dev/uinput directly with unix.Open so we hold ONLY the int fd.
	// We must NOT use os.OpenFile here and then drop the *os.File: Go's
	// *os.File has a finalizer that closes the underlying fd when it is
	// garbage-collected. If we stored only the int fd and let the *os.File
	// go out of scope, the GC would close the fd a moment later and the
	// kernel would destroy the uinput device (the device is tied to the fd
	// staying open). Using unix.Open avoids the finalizer entirely: the fd
	// stays open until we explicitly unix.Close it in Close().
	fd, err := unix.Open("/dev/uinput", unix.O_WRONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	// Close the fd on ANY error path. The device is only valid while the fd
	// stays open, so a leaked fd (from an early return mid-setup) would leave
	// a half-configured device that the kernel destroys when the fd is
	// eventually reclaimed. We release ownership of the fd only on success
	// (by resetting the closure), so the fd survives for the device's
	// lifetime and is closed later in Close().
	released := false
	defer func() {
		if !released {
			_ = unix.Close(fd)
		}
	}()
	i.fd = fd

	// Enable the event types we will emit.
	if err := ioctl(fd, uiSetEvbit, evKey); err != nil {
		return setErr("UI_SET_EVBIT(EV_KEY)", err)
	}
	if err := ioctl(fd, uiSetEvbit, evRel); err != nil {
		return setErr("UI_SET_EVBIT(EV_REL)", err)
	}
	if err := ioctl(fd, uiSetEvbit, evAbs); err != nil {
		return setErr("UI_SET_EVBIT(EV_ABS)", err)
	}

	// Pointer buttons + wheel.
	for _, code := range []uint16{btnLeft, btnRight, btnMiddle} {
		if err := ioctl(fd, uiSetKeybit, uintptr(code)); err != nil {
			return setErr("UI_SET_KEYBIT(btn)", err)
		}
	}
	if err := ioctl(fd, uiSetRelbit, relWheel); err != nil {
		return setErr("UI_SET_RELBIT(REL_WHEEL)", err)
	}
	// Relative pointer axes (pointer-lock mode). Both ABS and REL axes are
	// registered on the SAME device so the client can switch between absolute
	// (unlocked) and relative (locked) movement without recreating the device
	// (recreation would destroy the node the compositor is tracking and drop
	// any in-flight state). The compositor treats a device with both as a
	// normal pointer; REL events move the cursor by a delta, ABS events set
	// it to an absolute position.
	if err := ioctl(fd, uiSetRelbit, relX); err != nil {
		return setErr("UI_SET_RELBIT(REL_X)", err)
	}
	if err := ioctl(fd, uiSetRelbit, relY); err != nil {
		return setErr("UI_SET_RELBIT(REL_Y)", err)
	}

	// Absolute pointer axes spanning the full screen.
	if err := ioctl(fd, uiSetAbsbit, absX); err != nil {
		return setErr("UI_SET_ABSBIT(ABS_X)", err)
	}
	if err := ioctl(fd, uiSetAbsbit, absY); err != nil {
		return setErr("UI_SET_ABSBIT(ABS_Y)", err)
	}

	// Mark it a pointer device so the compositor treats it as a mouse.
	if err := ioctl(fd, uiSetPropbit, inputPropPointer); err != nil {
		return setErr("UI_SET_PROPBIT(INPUT_PROP_POINTER)", err)
	}

	// Full keyboard (EV_KEY 0..255).
	for code := uint16(0); code <= 255; code++ {
		if err := ioctl(fd, uiSetKeybit, uintptr(code)); err != nil {
			return setErr("UI_SET_KEYBIT(key)", err)
		}
	}

	log.Printf("input: uinput fd=%d opened; set EV bits (KEY/REL/ABS), 3 buttons, REL_X/Y/WHEEL, ABS_X/Y, PROP_POINTER, 256 key bits OK", i.fd)

	// Describe the device. The MODERN path (kernel >= 5.9) uses two ioctls
	// that carry the same data in fixed-size structs independent of the
	// kernel's ABS_CNT:
	//   1. UI_DEV_SETUP - name + input_id + ff_effects_max (92 bytes).
	//      Sets the device name/ID and transitions the device state to
	//      UIST_SETUP_COMPLETE (required before UI_DEV_CREATE).
	//   2. UI_ABS_SETUP - one absolute axis (code + min/max/fuzz/flat, 20
	//      bytes). Called once per axis; enables the axis's absbit.
	//
	// This is preferred over the legacy write(struct uinput_user_dev) path
	// because that write() requires the byte count to EXACTLY match the
	// kernel's sizeof(struct uinput_user_dev) (92 + ABS_CNT*16), which
	// varies by kernel and cannot be known from userspace. The ioctls have
	// fixed sizes, so they work on any kernel >= 5.9.
	var setup uinputSetup
	copy(setup.Name[:], "coaxial pointer")
	setup.ID.Bus = 0x06 // BUS_VIRTUAL
	setup.ID.Vendor = 0x01
	setup.ID.Product = 0x02
	setup.ID.Version = 0x01
	log.Printf("input: UI_DEV_SETUP (name=%q bus=0x%02x vendor=0x%02x product=0x%02x)", setup.Name[:16], setup.ID.Bus, setup.ID.Vendor, setup.ID.Product)
	if err := ioctl(i.fd, uiDevSetup, uintptr(unsafe.Pointer(&setup))); err != nil {
		// ENOTTY means the kernel predates UI_DEV_SETUP (< 5.9). Fall back
		// to the legacy write() path (only viable on those old kernels, where
		// ABS_CNT is still 54 and the 956-byte struct matches exactly). Any
		// other error is a genuine failure on a modern kernel - return it
		// rather than attempting a fallback that would also fail.
		if errno, ok := err.(syscall.Errno); !ok || errno != syscall.ENOTTY {
			return setErr("UI_DEV_SETUP", err)
		}
		if err := i.legacyDeviceSetup(); err != nil {
			return err
		}
	} else {
		// Set the ABS_X / ABS_Y ranges (spanning the full screen). Each call
		// enables the axis's absbit and records its min/max/fuzz/flat.
		for _, ax := range []struct {
			code uint16
			max  int32
		}{
			{absX, int32(i.screenW - 1)},
			{absY, int32(i.screenH - 1)},
		} {
			var abs uinputAbsSetup
			abs.Code = ax.code
			abs.AbsInfo.Value = 0
			abs.AbsInfo.Min = 0
			abs.AbsInfo.Max = ax.max
			abs.AbsInfo.Fuzz = 0
			abs.AbsInfo.Flat = 0
			abs.AbsInfo.Resolution = 0
			if err := ioctl(i.fd, uiAbsSetup, uintptr(unsafe.Pointer(&abs))); err != nil {
				return setErr("UI_ABS_SETUP", err)
			}
			log.Printf("input: UI_ABS_SETUP axis %d range [0, %d] OK", ax.code, ax.max)
		}
	}

	// Create the device (this makes it appear in /dev/input). Requires the
	// device to be in UIST_SETUP_COMPLETE (set by UI_DEV_SETUP above, or by
	// the legacy write() in the fallback).
	if err := ioctl(i.fd, uiDevCreate, 0); err != nil {
		return setErr("UI_DEV_CREATE", err)
	}

	// The device is now live and persistent for as long as the fd stays open.
	// Release ownership of the fd so the deferred close above does NOT run -
	// it will be closed later in Close() when the relay shuts down.
	released = true

	// Diagnose where the device actually landed: find the event node and its
	// seat. A device on a different seat than the display (or with no seat)
	// would be invisible to the compositor's input handling. The class node
	// lags UI_DEV_CREATE by a few ms (input class symlink + udev), so retry a
	// few times rather than scanning once and reporting a false "not found".
	node, seat := "", ""
	for attempt := 0; attempt < 20; attempt++ {
		if node, seat = findUinputNode("coaxial pointer"); node != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if node != "" {
		log.Printf("input: uinput device registered: /dev/input/%s seat=%q (fd=%d kept open for device lifetime)", node, seat, i.fd)
	} else {
		log.Printf("input: WARNING: UI_DEV_CREATE succeeded but no /sys/class/input/event* named 'coaxial pointer' found after 1s (fd=%d)", i.fd)
	}
	return nil
}

// findUinputNode scans /sys/class/input for an event node whose name contains
// `name` and returns the node name (e.g. "event23") and its seat (e.g.
// "seat0"). Used after UI_DEV_CREATE to confirm the device is registered and
// on which seat it landed. Returns "", "" if not found.
func findUinputNode(name string) (node, seat string) {
	entries, err := os.ReadDir("/sys/class/input")
	if err != nil {
		return "", ""
	}
	for _, e := range entries {
		n := e.Name()
		if !strings.HasPrefix(n, "event") {
			continue
		}
		b, err := os.ReadFile("/sys/class/input/" + n + "/name")
		if err == nil && strings.Contains(string(b), name) {
			seat = strings.TrimSpace(readSysFile("/sys/class/input/" + n + "/device/seat"))
			return n, seat
		}
	}
	return "", ""
}

// readSysFile reads a sysfs file, returning "" on any error.
func readSysFile(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(b)
}

// legacyDeviceSetup is the pre-5.9 fallback: write a struct uinput_user_dev
// (956 bytes, ABS_CNT=54) to /dev/uinput. On these old kernels the kernel's
// ABS_CNT is still 54, so the 956-byte struct matches sizeof exactly and the
// write is accepted. (On modern kernels this would fail with EINVAL because
// the kernel's ABS_CNT is larger - which is why the modern UI_DEV_SETUP path
// is preferred and this is only reached on ENOTTY.)
func (i *uinputInjector) legacyDeviceSetup() error {
	var uud uinputUserDev
	copy(uud.Name[:], "coaxial pointer")
	uud.ID.Bus = 0x06 // BUS_VIRTUAL
	uud.ID.Vendor = 0x01
	uud.ID.Product = 0x02
	uud.ID.Version = 0x01
	uud.AbsMax[absX] = int32(i.screenW - 1)
	uud.AbsMin[absX] = 0
	uud.AbsMax[absY] = int32(i.screenH - 1)
	uud.AbsMin[absY] = 0
	// The kernel requires the write count to EXACTLY match its
	// sizeof(struct uinput_user_dev). On kernels < 5.9 (where this fallback
	// is used) that is 956 bytes (ABS_CNT=54). Writing a different size
	// (e.g. an oversized 1024) is rejected with EINVAL.
	buf := unsafe.Slice((*byte)(unsafe.Pointer(&uud)), int(unsafe.Sizeof(uud)))
	if _, err := unix.Write(i.fd, buf); err != nil {
		return setErr("write(uinput_user_dev)", err)
	}
	return nil
}

// dispatchInput parses a raw input event from the browser (JSON) and dispatches
// it to the virtual device. The payload is the JSON body of a kindInput ('I')
// message (see inputMsg in input_keys.go): {"type","x","y","key","delta","button"}.
// The "x"/"y" fields are normalized [0,1] positions for "mousemove" but
// pointer-lock deltas (CSS pixels) for "relmousemove".
func (i *uinputInjector) dispatchInput(raw []byte) {
	var msg inputMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		// A parse failure means a wire-format mismatch (e.g. the kind byte
		// wasn't stripped). Log it (throttled) so it is visible rather than
		// silently dropping every event.
		now := time.Now().UnixMilli()
		if now-lastInputParseLog > 2000 {
			lastInputParseLog = now
			log.Printf("input: parse failed (%v), first bytes: %q", err, string(raw[:min(len(raw), 32)]))
		}
		return
	}
	switch msg.Type {
	case "mousemove":
		i.move(msg.X, msg.Y)
	case "relmousemove":
		// Pointer-lock mode: x/y carry the pointer-lock deltas
		// (movementX/movementY, CSS pixels), not normalized positions.
		i.relMove(msg.X, msg.Y)
	case "mousedown":
		i.button(btnFor(msg.Button), true)
	case "mouseup":
		i.button(btnFor(msg.Button), false)
	case "wheel":
		i.wheel(msg.Delta)
	case "keydown":
		if code, ok := evKeyLinux[msg.Key]; ok {
			i.key(code, true)
		}
	case "keyup":
		if code, ok := evKeyLinux[msg.Key]; ok {
			i.key(code, false)
		}
	}
}

// btnFor maps a JS MouseEvent.button (0=left, 1=middle, 2=right) to a Linux
// BTN_* code.
func btnFor(b int) uint16 {
	switch b {
	case 1:
		return btnMiddle
	case 2:
		return btnRight
	default:
		return btnLeft
	}
}

// move emits an absolute pointer position. nx,ny are normalized [0,1] within
// the visible video element (which shows the primary monitor). They are mapped
// into the primary monitor's region (primX,primY)-(primX+primW,primY+primH)
// of the full virtual screen, because the uinput ABS device spans the full
// screen (the compositor maps the full ABS range onto the whole virtual
// screen).
func (i *uinputInjector) move(nx, ny float64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.ensureInitLocked() {
		return
	}
	x := int32(float64(i.primX) + nx*float64(i.primW-1))
	y := int32(float64(i.primY) + ny*float64(i.primH-1))
	i.emit(evAbs, absX, x)
	i.emit(evAbs, absY, y)
	i.emit(evSyn, synReport, 0)
}

// relMove emits a RELATIVE pointer movement (pointer-lock mode). dx,dy are the
// client's pointer-lock deltas (movementX/movementY, in CSS pixels) and are
// scaled by relSensitivity before being emitted as REL_X/REL_Y events. Unlike
// move (absolute, which sets the cursor to a screen position), relMove shifts
// the cursor by a delta from its current position - the compositor accumulates
// the REL events, so the remote cursor tracks the local pointer's motion
// 1:1 (times the sensitivity) without the client needing to know the host's
// cursor position. A zero delta is a no-op (no event, no SYN) so idle
// pointer-lock frames do not emit empty reports.
func (i *uinputInjector) relMove(dx, dy float64) {
	if dx == 0 && dy == 0 {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.ensureInitLocked() {
		return
	}
	// Round to int32 after scaling: the client sends CSS-pixel deltas (which
	// can be fractional on high-DPI displays), and the REL axis is integer
	// screen pixels. Rounding (not truncating) keeps sub-pixel motion
	// accumulated across frames rather than dropped.
	i.emit(evRel, relX, int32(math.Round(dx*relSensitivity)))
	i.emit(evRel, relY, int32(math.Round(dy*relSensitivity)))
	i.emit(evSyn, synReport, 0)
}

// button emits a pointer button press (pressed=true) or release (false).
func (i *uinputInjector) button(code uint16, pressed bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.ensureInitLocked() {
		return
	}
	v := int32(0)
	if pressed {
		v = 1
	}
	i.emit(evKey, code, v)
	i.emit(evSyn, synReport, 0)
}

// wheelUnitsPerNotch is the REL_WHEEL magnitude emitted per client scroll
// wheelUnitsPerNotch is the REL_WHEEL magnitude emitted per client scroll
// notch. A single REL_WHEEL unit scrolls by a very small amount in GNOME, so
// one unit per notch feels nearly inert. Emitting several units per notch
// gives a normal-feeling scroll step. Tunable via COAXIAL_WHEEL_UNITS_PER_NOTCH
// (default 3) so the scroll "feel" can be tuned to the host's compositor
// without a rebuild.
var wheelUnitsPerNotch = parseWheelUnitsPerNotch()

// parseWheelUnitsPerNotch reads COAXIAL_WHEEL_UNITS_PER_NOTCH (a positive
// int), defaulting to 3. A zero or negative value falls back to 3 (zero
// amplification would make the wheel inert; negative would invert it).
func parseWheelUnitsPerNotch() int32 {
	v := os.Getenv("COAXIAL_WHEEL_UNITS_PER_NOTCH")
	if v == "" {
		return 1
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 1
	}
	return int32(n)
}

// wheel emits a vertical scroll (delta in client wheel "notch" units; the
// sign is preserved). Each notch is amplified to wheelUnitsPerNotch REL_WHEEL
// units so the desktop scrolls a visible amount per physical wheel click.
func (i *uinputInjector) wheel(delta int32) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.ensureInitLocked() {
		return
	}
	i.emit(evRel, relWheel, delta*wheelUnitsPerNotch)
	i.emit(evSyn, synReport, 0)
}

// key emits a keyboard key press (pressed=true) or release (false), tracking
// the pressed state so releaseAll/Close can clear any keys still held down.
func (i *uinputInjector) key(code uint16, pressed bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.ensureInitLocked() {
		return
	}
	v := int32(0)
	if pressed {
		v = 1
		i.pressed[code] = true
	} else {
		delete(i.pressed, code)
	}
	i.emit(evKey, code, v)
	i.emit(evSyn, synReport, 0)
}

// releaseAll emits a release for every key currently held down (and for the
// pointer buttons), so a disconnect does not leave a key stuck on the remote
// machine.
func (i *uinputInjector) releaseAll() {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.ensureInitLocked() {
		return
	}
	for code := range i.pressed {
		i.emit(evKey, code, 0)
		delete(i.pressed, code)
	}
	for _, code := range []uint16{btnLeft, btnRight, btnMiddle} {
		i.emit(evKey, code, 0)
	}
	i.emit(evSyn, synReport, 0)
}

// emit writes a single input_event to the device fd. The caller MUST hold mu.
func (i *uinputInjector) emit(typ, code uint16, value int32) {
	var ev inputEvent
	ev.Type = typ
	ev.Code = code
	ev.Value = value
	b, _ := marshalEvent(ev)
	// A write failure (fd closed, etc.) is non-fatal: the device may have
	// been destroyed; the next event retries. Ignore the error here.
	_, _ = unix.Write(i.fd, b)
}

// Close releases any held keys, destroys the virtual device, and closes the
// fd. Safe to call multiple times.
func (i *uinputInjector) Close() {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.created {
		return
	}
	// Release any held keys before destroying the device, so the remote
	// machine does not keep a key pressed after the relay goes away.
	for code := range i.pressed {
		i.emit(evKey, code, 0)
		delete(i.pressed, code)
	}
	i.emit(evSyn, synReport, 0)
	_ = ioctl(i.fd, uiDevDestroy, 0)
	_ = unix.Close(i.fd)
	i.created = false
	i.initErr = nil
	log.Printf("input: uinput virtual device destroyed")
}

// --- helpers ---

func setErr(op string, err error) error {
	return os.NewSyscallError(op, err)
}

// ioctl issues a simple (no-pointer) ioctl on fd.
func ioctl(fd int, op, arg uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), op, arg)
	if errno != 0 {
		return errno
	}
	return nil
}

// marshalEvent serializes an inputEvent to its 24-byte wire form:
// timeval (sec i64, usec i64) + type u16 + code u16 + value i32.
func marshalEvent(ev inputEvent) ([]byte, error) {
	var buf [24]byte
	// time fields left zero (kernel fills in the current time).
	binary.LittleEndian.PutUint16(buf[16:18], ev.Type)
	binary.LittleEndian.PutUint16(buf[18:20], ev.Code)
	binary.LittleEndian.PutUint32(buf[20:24], uint32(ev.Value))
	return buf[:], nil
}
