# TODO - Coaxial Roadmap

## Quick settings
- splash screen with the Coaxial logo
- login (possibly Google auth?) for security
- add a flyout config bar at the bottom of the screen (configurable left/right/top)
- add settings icon buttons (simple vector graphics)

### Settings (client app)
- input capture
- fullscreen (canvas to fullscreen)
- switch displays (if multi-display hosts)
- relay audio
- performance overlay (24px bar, bottom left - shows fps (current + graph last 30 sec), bandwidth per second (current + graph last 30 sec)

### Settings (sent to host, stream restart?)
- stream quality (cycle min+max bitrate: 3200-7200, 5000-15000, 7200-21000)
- stream framerate (cycle 15, 24, 30, 45, 60)
- stream scaling (0.5-1 scaling factor)
- relay audio  (pipewire)

## Input controls
- Capture input (mouse capture API, suppress defaults like context_menu, capture "super" key)
- Enable clipboard relay (look at xclip and xsel implementation from cromote? modern alts)
 
### Mouse interactions:
- fix the mouse wheel triggering on host

#### When captured:
- hide the client mouse cursor
- use the mouse capture API to lock the mouse to center window
- run relative mouse input on the host
- long press "escape" to uncapture the mouse

