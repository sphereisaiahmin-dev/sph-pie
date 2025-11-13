# Production Information Environment (PIE)

The Production Information Environment (PIE) is a browser-based operations platform that unifies
show logging, crew coordination, and administrative controls across multiple production
disciplines. PIE pairs a vanilla JavaScript single-page application with a Node.js/Express API and
a flexible storage layer powered by SQL.js (with PostgreSQL support for larger deployments).

## Highlights

- **Discipline selector** – every authenticated session begins with a discipline selection screen.
  PIE ships with Audio, Video, Lighting, 4D, Drones, Show Control, and Broadcast tracks. The
  Drones workspace includes the full logging suite today and the remaining disciplines are ready
  for future modules.
- **Hierarchical roles** – each discipline exposes Lead, Operator, and Crew roles. The admin
  workspace dynamically renders the per-discipline role grid so new tracks can be added without
  modifying core logic.
- **Form migration for Drones** – all legacy Lead, Operator, Crew, and logging forms have been
  migrated into the Drones discipline. Selecting Drones reveals the familiar Lead, Operator, and
  Archive workspaces.
- **Extensible architecture** – discipline and role definitions live in `config/disciplines.json`
  and are consumed by both the client and the server. Adding a new discipline or role tier is as
  simple as updating the config file.
- **Secure administration** – Admins manage users, role assignments, and unit labels directly in
  the app. Role filters, discipline groupings, and the role grid all respect the new hierarchy.
- **Storage flexibility** – SQL.js remains the default zero-dependency store (persisted to
  `data/pie.sqlite`). PostgreSQL is available for environments that need shared storage.

## Getting Started

1. Install dependencies at the repository root:

   ```bash
   npm install
   ```

2. Start the Express server directly:

   ```bash
   node server/index.js
   ```

   The server binds to `10.241.211.120:3000` by default so the UI is reachable across the LAN.
   Override with `HOST`/`PORT` environment variables when needed (for example
   `HOST=0.0.0.0 node server/index.js`).

3. Visit [http://10.241.211.120:3000](http://10.241.211.120:3000) (or your configured host) and
   sign in with one of the seeded accounts listed in `server/userStore.js`. Each account begins
   with the temporary password `adminsphere1` and must reset on first login. Admins can open the
   settings menu (hamburger button) to manage users, assign discipline roles, and configure
   webhooks. SQL.js data is persisted to `data/pie.sqlite`.

## Configuration

Runtime configuration is stored in `config/app-config.json` (created on first run). A reference
file lives at `config/app-config.example.json`.

- **host / port** – network binding for Express (defaults to `10.241.211.120:3000`).
- **unitLabel** – UI label used throughout the Drones workspace (defaults to "Drone").
- **sql.filename** – path to the SQL.js persistence file (`data/pie.sqlite`).
- **postgres** – PostgreSQL connection details when using the `postgres` provider.
- **webhook** – enable/disable per-entry webhooks and configure delivery details.

## Storage Providers

PIE ships with two storage providers:

- **SQL.js (default)** – zero-dependency persistence stored in `data/pie.sqlite`.
- **PostgreSQL** – enable by setting `storageProvider` to `"postgres"` in
  `config/app-config.json` (or via the admin UI). The default connection string is
  `postgres://postgres:postgres@localhost:5432/pie`. Standard `PG*` environment variables are
  respected.

## Role & Discipline Model

Discipline metadata lives in `config/disciplines.json`. The structure defines the available
roles for each discipline and flags which disciplines currently expose workspaces. The server
exposes this via `GET /api/disciplines`, and the client renders both the discipline selector and
admin role grid from the same source. Adding a new discipline only requires updating the config.

User accounts are stored in `data/users.json` (seeded on first launch). Admins can create or edit
accounts, assign discipline-specific roles, and trigger password resets. Lead/Operator/Crew
assignments automatically populate the workspace selectors for the active discipline.

## Development Notes

- Front-end logic lives in `public/app.js`, with styles in `public/styles.css` and markup in
  `public/index.html`.
- The server entry point is `server/index.js`. Storage providers are in `server/storage/` and the
  discipline helpers live in `server/disciplineConfig.js`.
- Broadcast synchronization uses the `pie-sync` channel so multiple browser tabs stay aligned.
- Additional scripts for testing webhooks and archive exports are located in the `scripts/`
  directory.

## Testing

PIE currently ships without automated tests. When making changes, manually verify:

- Discipline selection flows (selecting Drones loads the workspaces, other disciplines show the
  workspace placeholder).
- Admin role management updates the grid and filters correctly.
- Lead/Operator/Archive workspaces continue to function for the Drones discipline.
- Webhook simulations (`npm run simulate:webhook`) behave as expected when storage data changes.

## License

This project is released under the ISC license. See `LICENSE` for details.
