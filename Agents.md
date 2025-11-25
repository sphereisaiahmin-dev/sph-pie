# Production Information Environment – Agent Operations Guide

This repository serves a browser-based operations console backed by a Node.js/Express API and a `sql.js` storage layer. Use this guide to understand the current system, coordinate multi-agent updates, and plan future expansions such as account-level access control.

---

## Current Application Snapshot (multi-discipline release)
- **Front-end** (`public/index.html`, `public/app.js`): vanilla JS single-page interface with a discipline selector that leads into role-specific workspaces (Lead, Operator, Archive) gated by the login/password reset flow.
- **Settings drawer**: the hamburger menu exposes **only the Admin settings** after authentication. User management, unit label, discipline role assignments, data refresh, and webhook controls all live inside the admin section and require the signed-in user to have the Admin role.
- **State & sync**: shared app state and broadcast coordination live in `public/app.js` (`state` object, `setupSyncChannel`). The BroadcastChannel named `pie-sync` is used to fan out mutations across tabs.
- **Server** (`server/index.js` and `server/storage/sqlProvider.js`): Express handles REST endpoints for configuration, discipline-aware staff data, shows, entries, and archive operations. Data persists via `sql.js` with JSON payloads, retention, and webhook dispatch support (`server/webhookDispatcher.js`).

### Key workflows to preserve
1. **Admin roster management** – saving the config form must update `/api/staff`, refresh dropdowns (`renderPilotAssignments`, `renderCrewOptions`), and notify other clients via sync events.
2. **Entry lifecycle** – creation/update/removal enforces pilot uniqueness per show and triggers webhooks when enabled.
3. **Archive analytics** – charts draw from `/api/shows/archive` and rely on metrics defined at the top of `public/app.js` (`ARCHIVE_METRIC_DEFS`).
4. **Multi-client awareness** – whenever you change show data or config, update the BroadcastChannel payloads so concurrent sessions stay consistent.

---

## Admin Drawer Implementation Notes
- `toggleConfig` in `public/app.js` ensures the drawer opens directly to the admin section and now verifies the signed-in user has the Admin role before revealing any settings.
- `setConfigSection` still supports multiple sections, but only the `admin` section remains active. Keep the guard clauses in place if you later reintroduce additional sections.
- Staff textarea values map 1:1 to the `/api/staff` payload. Maintain the `parseStaffTextarea` normalization rules (trim, dedupe, sort).
- When modifying Admin UI markup, keep IDs stable for the new directory widgets (`userDirectory`, `userForm`, `userName`, `userEmail`, `userFormStatus`, etc.) so existing event handlers continue working.

---

## Preparing for User Accounts & Role-Based Access
When you introduce authentication/authorization layers:
1. **Boundary placement** – add auth middleware in `server/index.js` before route definitions. Encapsulate role checks so front-end workspace toggles can consume a single `GET /api/session` payload describing capabilities.
2. **Client gating** – centralize role enforcement in `public/app.js` (e.g., `setView`) so new roles don’t bypass restrictions. Consider deriving view availability from a `state.session.roles` array.
3. **Auth guard strategy** – keep all sensitive routes and UI sections behind `state.session.roles`. The `PASSWORD_RESET_ALLOW` list plus `apiRequest` error handling should be updated alongside any new endpoints so expired sessions or locked accounts never leak data.
4. **Sync events** – extend BroadcastChannel messages to include session identifiers to avoid applying stale updates from users who lose access mid-session.

---

## Future Forms & Unit ID Expansions
- **Dynamic unit labels**: `state.unitLabel` drives multiple UI strings. When adding new unit types, update `populateUnitOptions` and ensure CSV/webhook exports include the new label values.
- **Discipline config**: `config/disciplines.json` controls the discipline list and role levels surfaced by both the client and server. Update this file when introducing new tracks or role tiers.
- **Additional forms**: reuse the existing grid/layout utility classes in `public/styles.css`. Keep accessibility attributes (`aria-labelledby`, `role="dialog"`) consistent with current panels to meet deployment standards.
- **Validation hooks**: centralize new form validation in dedicated helpers (mirroring `ensureShowHeaderValid`) so both Lead and Pilot experiences stay aligned.
- **Schema updates**: if new forms require persisted fields, update both `sqlProvider` and any future Postgres provider simultaneously. Record migration steps in this file under a new changelog section.

---

## Testing Expectations
- Run `npm test` (placeholder) and any targeted scripts such as `npm run simulate:webhook` after touching API or webhook code.
- Manually verify multi-tab behaviour by opening the app in two sessions; ensure BroadcastChannel updates continue to work after structural changes to admin settings.
- Document additional manual or automated checks in your pull request descriptions for traceability.

Keep this guide current whenever you adjust critical flows, expand role handling, or modify persistence logic.

---

## Authentication & RBAC architecture
- `server/userStore.js` seeds the directory from the provided Sphere roster and hashes passwords with `crypto.scrypt`. Accounts are persisted to `data/users.json` (created automatically the first time you start `node server/index.js`).
- `server/sessionStore.js` issues 12-hour session tokens that are hashed in-memory and stored client-side via an `HttpOnly` `mt_session` cookie. `app.use` middleware in `server/index.js` resolves the session into `req.user` for every `/api/*` request.
- `public/app.js` drives the login overlay (`#loginScreen`) and the password reset view (`#passwordResetScreen`). `apiRequest` traps `401` responses to bounce the user back to the login form and `423` responses to force the password reset UI.
- `PASSWORD_RESET_ALLOW` in `server/index.js` whitelists the session/password/logout endpoints so locked accounts can still complete the forced-reset flow.

### Role definitions & permissions
- **Admin** – access the settings drawer, create/update/delete users, reset passwords, edit config/webhook settings, and invoke admin-only API endpoints.
- **Discipline Lead** – create/update/archive shows and export archives for the selected discipline. Leads can also access the archive workspace.
- **Discipline Operator** – log entries against shows via the Operator workspace for the selected discipline. Operators also have archive read-only access.
- **Discipline Crew** – appear in crew selection lists and can read archived shows. Crew members do not see Lead/Operator workspaces.

### Admin management workflows
1. After signing in with an Admin role, open the settings drawer (`#configPanel`). The user directory (`#userDirectory`) auto-loads via `loadUsers()` and renders clickable rows.
2. Use the inline user form (`#userForm`) to add or update accounts. Required inputs: name, email, and at least one role checkbox (`name="userRole"`).
3. Saving triggers `/api/users` (`POST` for create, `PUT /api/users/:id` for edits). The UI resets the form, refreshes the directory, reloads the `/api/staff` payload, and calls `notifyStaffChanged()` so other tabs refresh their selectors.
4. Password resets run through `/api/users/:id/reset-password` and immediately invalidate active sessions via `deleteSessionsForUser` on the server.
5. Staff selectors (`renderOperatorOptions`, `renderPilotAssignments`, `renderCrewOptions`) always pull from `state.staff`, which is hydrated by `/api/staff` and mirrors the user directory (Operators → operator pickers, Leads → lead/crew-lead selects, Crew → crew multi-select).

### Security considerations
- Password strength enforcement lives in `server/userStore.js` (`validatePasswordStrength`) and requires 12+ characters with upper/lower/number/symbol. New accounts are flagged with `passwordResetRequired` so the client forces the password reset flow before the SPA loads.
- Scrypt hashing (per-user salts, `N=16384`) protects stored passwords. `DEFAULT_TEMP_PASSWORD` is only used during seeding or admin-triggered resets.
- Session cookies are `HttpOnly`/`SameSite=Lax`, and the server hashes session tokens before caching them. `apiRequest` now traps `401/423` statuses to avoid leaking stale state in the SPA when sessions expire or resets are required.
- `/api/staff` no longer trusts textarea inputs; it is read-only and derives roster data from the user directory to reduce tampering risk.

### Setup instructions
- Run `node server/index.js` from the repo root to initialize both `data/users.json` and the SQL.js database. The seed list in `server/userStore.js` contains all requested Sphere accounts (Leads/Operators, Crew, and Admins Isaiah Mincher + Zach Harvest).
- Default password for every new or reset account is `adminsphere1`. Users must change it on first login via the password reset overlay.
- To bootstrap additional admins for a new environment, either extend `DEFAULT_USER_SEED` in `server/userStore.js` before the first launch or sign in as an existing Admin and create the new account through the UI.
- Use the login overlay (`/#loginScreen`) to authenticate before the SPA renders. Credentials are cached via the `mt_session` cookie, so refreshing the page keeps you signed in until the 12-hour TTL expires or you click Logout.
