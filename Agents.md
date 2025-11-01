# Monkey Tracker – Agent Operations Guide

This repository serves a browser-based operations console backed by a Node.js/Express API and a `sql.js` storage layer. Use this guide to understand the current system, coordinate multi-agent updates, and plan future expansions such as account-level access control.

---

## Current Application Snapshot (v1 launch)
- **Front-end** (`public/index.html`, `public/app.js`): vanilla JS single-page interface built around role-specific workspaces (Lead, Pilot, Archive).
- **Settings drawer**: the hamburger menu now exposes **only the Admin settings**. Pilots and monkey lead rosters, crew lists, unit label, data refresh, and webhook controls all live inside the admin section and require the 4-digit PIN (`ADMIN_PIN` in `public/app.js`).
- **State & sync**: shared app state and broadcast coordination live in `public/app.js` (`state` object, `setupSyncChannel`). The BroadcastChannel named `monkey-tracker-sync` is used to fan out mutations across tabs.
- **Server** (`server/index.js` and `server/storage/sqlProvider.js`): Express handles REST endpoints for configuration, staff, shows, entries, and archive operations. Data persists via `sql.js` with JSON payloads, retention, and webhook dispatch support (`server/webhookDispatcher.js`).

### Key workflows to preserve
1. **Admin roster management** – saving the config form must update `/api/staff`, refresh dropdowns (`renderPilotAssignments`, `renderCrewOptions`), and notify other clients via sync events.
2. **Entry lifecycle** – creation/update/removal enforces pilot uniqueness per show and triggers webhooks when enabled.
3. **Archive analytics** – charts draw from `/api/shows/archive` and rely on metrics defined at the top of `public/app.js` (`ARCHIVE_METRIC_DEFS`).
4. **Multi-client awareness** – whenever you change show data or config, update the BroadcastChannel payloads so concurrent sessions stay consistent.

---

## Admin Drawer Implementation Notes
- `toggleConfig` in `public/app.js` ensures the drawer opens directly to the admin section and summons the PIN prompt when locked.
- `setConfigSection` still supports multiple sections, but only the `admin` section remains active. Keep the guard clauses in place if you later reintroduce additional sections.
- Staff textarea values map 1:1 to the `/api/staff` payload. Maintain the `parseStaffTextarea` normalization rules (trim, dedupe, sort).
- When modifying Admin UI markup, keep IDs stable (`pilotList`, `monkeyLeadList`, `crewList`, etc.) so existing event handlers continue working.

---

## Preparing for User Accounts & Role-Based Access
When you introduce authentication/authorization layers:
1. **Boundary placement** – add auth middleware in `server/index.js` before route definitions. Encapsulate role checks so front-end workspace toggles can consume a single `GET /api/session` payload describing capabilities.
2. **Client gating** – centralize role enforcement in `public/app.js` (e.g., `setView`) so new roles don’t bypass restrictions. Consider deriving view availability from a `state.session.roles` array.
3. **PIN retirement strategy** – transition the hard-coded PIN flow to token-based checks. Preserve backwards compatibility with an environment flag until rollout is complete.
4. **Sync events** – extend BroadcastChannel messages to include session identifiers to avoid applying stale updates from users who lose access mid-session.

---

## Future Forms & Unit ID Expansions
- **Dynamic unit labels**: `state.unitLabel` drives multiple UI strings. When adding new unit types, update `populateUnitOptions` and ensure CSV/webhook exports include the new label values.
- **Additional forms**: reuse the existing grid/layout utility classes in `public/styles.css`. Keep accessibility attributes (`aria-labelledby`, `role="dialog"`) consistent with current panels to meet deployment standards.
- **Validation hooks**: centralize new form validation in dedicated helpers (mirroring `ensureShowHeaderValid`) so both Lead and Pilot experiences stay aligned.
- **Schema updates**: if new forms require persisted fields, update both `sqlProvider` and any future Postgres provider simultaneously. Record migration steps in this file under a new changelog section.

---

## Testing Expectations
- Run `npm test` (placeholder) and any targeted scripts such as `npm run simulate:webhook` after touching API or webhook code.
- Manually verify multi-tab behaviour by opening the app in two sessions; ensure BroadcastChannel updates continue to work after structural changes to admin settings.
- Document additional manual or automated checks in your pull request descriptions for traceability.

Keep this guide current whenever you adjust critical flows, expand role handling, or modify persistence logic.
