// input_keys.go - JS KeyboardEvent.code -> Linux EV_KEY mapping, and the
// shared inputMsg payload type for the relay's input loop.
//
// The client sends normalized [0,1] pointer positions (within the visible
// video element) and the JS KeyboardEvent.code string (e.g. "KeyA", "Enter",
// "ShiftLeft"). The server scales the pointer to the full screen resolution
// and maps the key code to a Linux EV_KEY (see evKeyLinux). The dispatch
// entry point (uinputInjector.dispatchInput) lives in input.go.

package main

// evKeyLinux maps a JS KeyboardEvent.code to a Linux EV_KEY code. Only the
// keys that are unambiguous and useful for a remote session are mapped; an
// unmapped code is a no-op. The values are the standard Linux input event
// codes (include/uapi/linux/input-event-codes.h).
var evKeyLinux = map[string]uint16{
	// Letters (QWERTY physical positions).
	"KeyA": 30, "KeyB": 48, "KeyC": 46, "KeyD": 32, "KeyE": 18, "KeyF": 33,
	"KeyG": 34, "KeyH": 35, "KeyI": 23, "KeyJ": 36, "KeyK": 37, "KeyL": 38,
	"KeyM": 50, "KeyN": 49, "KeyO": 24, "KeyP": 25, "KeyQ": 16, "KeyR": 19,
	"KeyS": 31, "KeyT": 20, "KeyU": 22, "KeyV": 47, "KeyW": 17, "KeyX": 45,
	"KeyY": 21, "KeyZ": 44,
	// Digits + top-row symbols.
	"Digit0": 11, "Digit1": 2, "Digit2": 3, "Digit3": 4, "Digit4": 5,
	"Digit5": 6, "Digit6": 7, "Digit7": 8, "Digit8": 9, "Digit9": 10,
	"Minus": 12, "Equal": 13, "BracketLeft": 26, "BracketRight": 27,
	"Semicolon": 39, "Quote": 40, "Backslash": 43, "Comma": 51, "Period": 52,
	"Slash": 53, "Backquote": 41,
	// Controls / navigation.
	"Enter": 28, "Backspace": 14, "Tab": 15, "Escape": 1, "Space": 57,
	"ArrowLeft": 105, "ArrowRight": 106, "ArrowUp": 103, "ArrowDown": 108,
	"Insert": 110, "Delete": 111, "Home": 102, "End": 107, "PageUp": 104,
	"PageDown": 109,
	// Modifiers.
	"ShiftLeft": 42, "ShiftRight": 54, "ControlLeft": 29, "ControlRight": 97,
	"AltLeft": 56, "AltRight": 100, "MetaLeft": 125, "MetaRight": 126,
	"CapsLock": 58,
	// Function keys.
	"F1": 59, "F2": 60, "F3": 61, "F4": 62, "F5": 63, "F6": 64, "F7": 65,
	"F8": 66, "F9": 67, "F10": 68, "F11": 87, "F12": 88,
}

// inputMsg is the JSON payload the client sends for a kind 'I' (input)
// message. The meaning of x/y depends on the event type:
//   - mousemove: normalized [0,1] pointer positions (within the visible video
//     element); the server maps them to an absolute screen position.
//   - relmousemove: pointer-lock deltas (movementX/movementY, in CSS pixels);
//     the server scales them by relSensitivity and emits REL_X/REL_Y (a
//     relative cursor nudge). The same x/y fields carry the delta so no new
//     wire fields are needed.
// key is the JS KeyboardEvent.code (keydown/keyup only); delta is the wheel
// scroll delta (wheel only); button is the JS MouseEvent.button (0=left,
// 1=middle, 2=right) for mousedown/mouseup.
type inputMsg struct {
	Type   string  `json:"type"`
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Key    string  `json:"key"`
	Delta  int32   `json:"delta"`
	Button int     `json:"button"`
}
