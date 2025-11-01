const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');

const DAY_IN_MS = 24 * 60 * 60 * 1000;
const ARCHIVE_RETENTION_MONTHS = 2;
const VALID_ROLES = ['admin', 'lead', 'operator', 'crew'];

const IDENTIFIER_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;

function normalizeRole(role){
  if(typeof role !== 'string'){
    return null;
  }
  const trimmed = role.trim().toLowerCase();
  if(!trimmed){
    return null;
  }
  if(trimmed === 'pilot' || trimmed === 'pilots'){
    return 'operator';
  }
  if(trimmed === 'monkeylead' || trimmed === 'monkey_lead' || trimmed === 'monkey-lead'){
    return 'crew';
  }
  if(trimmed === 'leadpilot' || trimmed === 'lead_pilot' || trimmed === 'lead-pilot' || trimmed === 'leads'){
    return 'lead';
  }
  if(VALID_ROLES.includes(trimmed)){
    return trimmed;
  }
  return null;
}

function normalizeRoles(input){
  const values = Array.isArray(input) ? input : [input];
  const normalized = [];
  values.forEach(value =>{
    const role = normalizeRole(value);
    if(!role){
      return;
    }
    if(!normalized.includes(role)){
      normalized.push(role);
    }
  });
  return normalized;
}

class PostgresProvider {
  constructor(config = {}){
    this.config = config || {};
    this.pool = null;
    this.schema = this._sanitizeIdentifier(this.config.schema);
  }

  async init(){
    if(this.pool){
      await this.dispose();
    }
    const poolConfig = this._buildPoolConfig();
    await this._ensureDatabaseExists(poolConfig);
    this.pool = this._createPool(poolConfig);
    // sanity check connection
    await this.pool.query('SELECT 1');

    if(this.schema){
      await this._run(`CREATE SCHEMA IF NOT EXISTS ${this._formatIdentifier(this.schema)}`);
    }

    await this._ensureSchema();
    await this._refreshArchive();
  }

  async dispose(){
    if(this.pool){
      const pool = this.pool;
      this.pool = null;
      await pool.end();
    }
  }

  getStorageLabel(){
    return 'PostgreSQL v1';
  }

  getStorageMetadata(){
    const params = this.pool?.options || this.pool?.connectionParameters || {};
    return {
      label: this.getStorageLabel(),
      driver: 'postgres',
      host: params.host,
      port: params.port,
      database: params.database,
      user: params.user,
      schema: this.schema || 'public'
    };
  }

  async listShows(){
    await this._refreshArchive();
    const rows = await this._select(`SELECT data FROM ${this._table('shows')} ORDER BY updated_at DESC`);
    return rows.map(row => this._normalizeShow(this._parseRowData(row?.data) || {}));
  }

  async getShow(id){
    if(!id){
      return null;
    }
    await this._refreshArchive();
    const row = await this._selectOne(`SELECT data FROM ${this._table('shows')} WHERE id = $1`, [id]);
    return row ? this._normalizeShow(this._parseRowData(row.data) || {}) : null;
  }

  async createShow(input){
    const payload = input || {};
    this._assertRequiredShowFields(payload);
    const now = Date.now();
    const createdAtCandidate = Number(payload.createdAt);
    const updatedAtCandidate = Number(payload.updatedAt);
    const createdAt = Number.isFinite(createdAtCandidate) ? createdAtCandidate : now;
    let updatedAt = Number.isFinite(updatedAtCandidate) ? updatedAtCandidate : now;
    if(updatedAt < createdAt){
      updatedAt = createdAt;
    }
    const show = this._normalizeShow({
      ...payload,
      id: payload.id || uuidv4(),
      createdAt,
      updatedAt,
      entries: Array.isArray(payload.entries) ? payload.entries : []
    });
    await this._enforceShowLimit(show.date, show.id);
    await this._persist(show);
    await this._refreshArchive();
    return show;
  }

  async updateShow(id, updates){
    const existing = await this.getShow(id);
    if(!existing){
      return null;
    }
    this._assertRequiredShowFields({...existing, ...updates});
    const updated = this._normalizeShow({
      ...existing,
      ...updates,
      updatedAt: Date.now()
    });
    await this._enforceShowLimit(updated.date, updated.id);
    await this._persist(updated);
    await this._refreshArchive();
    return updated;
  }

  async deleteShow(id){
    if(!id){
      return null;
    }
    const showsTable = this._table('shows');
    const archiveTable = this._table('show_archive');
    let archivedShow = null;
    const deleted = await this._withClient(async client =>{
      const res = await client.query(`SELECT data FROM ${showsTable} WHERE id = $1`, [id]);
      if(res.rows.length === 0){
        return false;
      }
      const row = res.rows[0];
      const show = this._parseRowData(row.data);
      if(!show || typeof show !== 'object'){
        await client.query(`DELETE FROM ${showsTable} WHERE id = $1`, [id]);
        return false;
      }
      const normalized = this._normalizeShow(show);
      const archiveTime = Date.now();
      normalized.archivedAt = archiveTime;
      normalized.deletedAt = archiveTime;
      await this._saveArchiveRow(normalized, archiveTime, archiveTime, client);
      await client.query(`DELETE FROM ${showsTable} WHERE id = $1`, [normalized.id]);
      archivedShow = normalized;
      return true;
    });
    if(!deleted){
      return null;
    }
    await this._refreshArchive();
    if(!archivedShow){
      const row = await this._selectOne(`SELECT data, archived_at, created_at FROM ${archiveTable} WHERE id = $1`, [id]);
      return row ? this._mapArchiveRow(row) : null;
    }
    return archivedShow;
  }

  async addEntry(showId, entryInput){
    const show = await this.getShow(showId);
    if(!show){
      return null;
    }
    const entry = this._normalizeEntry({
      ...entryInput,
      id: entryInput.id || uuidv4(),
      ts: entryInput.ts || Date.now()
    });
    this._assertPilotUnique(show, entry);
    const idx = show.entries.findIndex(e => e.id === entry.id);
    if(idx >= 0){
      show.entries[idx] = entry;
    }else{
      show.entries.push(entry);
    }
    show.updatedAt = Date.now();
    await this._persist(show);
    await this._refreshArchive();
    return entry;
  }

  async updateEntry(showId, entryId, updates){
    const show = await this.getShow(showId);
    if(!show){
      return null;
    }
    const idx = show.entries.findIndex(e => e.id === entryId);
    if(idx < 0){
      return null;
    }
    const entry = this._normalizeEntry({
      ...show.entries[idx],
      ...updates
    });
    this._assertPilotUnique(show, entry);
    show.entries[idx] = entry;
    show.updatedAt = Date.now();
    await this._persist(show);
    await this._refreshArchive();
    return entry;
  }

  async deleteEntry(showId, entryId){
    const show = await this.getShow(showId);
    if(!show){
      return null;
    }
    const idx = show.entries.findIndex(e => e.id === entryId);
    if(idx < 0){
      return null;
    }
    show.entries.splice(idx, 1);
    show.updatedAt = Date.now();
    await this._persist(show);
    await this._refreshArchive();
    return true;
  }

  async replaceShow(show){
    const normalized = this._normalizeShow(show);
    await this._persist(normalized);
    await this._refreshArchive();
    return normalized;
  }

  async listArchivedShows(){
    await this._refreshArchive();
    const rows = await this._select(`SELECT data, archived_at, created_at, deleted_at FROM ${this._table('show_archive')} ORDER BY archived_at DESC, id ASC`);
    return rows.map(row => this._mapArchiveRow(row)).filter(Boolean);
  }

  async getArchivedShow(id){
    if(!id){
      return null;
    }
    await this._refreshArchive();
    const row = await this._selectOne(`SELECT data, archived_at, created_at, deleted_at FROM ${this._table('show_archive')} WHERE id = $1`, [id]);
    return row ? this._mapArchiveRow(row) : null;
  }

  async archiveShowNow(id){
    if(!id){
      return null;
    }
    const showsTable = this._table('shows');
    const row = await this._selectOne(`SELECT data FROM ${showsTable} WHERE id = $1`, [id]);
    if(!row){
      return this.getArchivedShow(id);
    }
    const show = this._parseRowData(row.data);
    if(!show || typeof show !== 'object'){
      return null;
    }
    const normalized = this._normalizeShow(show);
    const archiveTime = Date.now();
    await this._withClient(async client =>{
      await this._saveArchiveRow(normalized, archiveTime, null, client);
      await client.query(`DELETE FROM ${showsTable} WHERE id = $1`, [normalized.id]);
    });
    await this._refreshArchive();
    return this.getArchivedShow(id);
  }

  async runArchiveMaintenance(){
    await this._refreshArchive();
  }

  async getStaff(){
    const users = await this.listUsers();
    const crew = [];
    const pilots = [];
    const crewLeads = [];
    users.forEach(user =>{
      if(!user || typeof user !== 'object'){
        return;
      }
      const roles = Array.isArray(user.roles) ? user.roles : [];
      if(roles.includes('crew')){
        crew.push(user.name);
        crewLeads.push(user.name);
      }
      if(roles.includes('operator')){
        pilots.push(user.name);
      }
    });
    crew.sort((a, b) => a.localeCompare(b));
    pilots.sort((a, b) => a.localeCompare(b));
    crewLeads.sort((a, b) => a.localeCompare(b));
    return {
      crew,
      pilots,
      monkeyLeads: crewLeads,
      users: users.map(user => ({
        id: user.id,
        name: user.name,
        email: user.email,
        roles: Array.isArray(user.roles) ? [...user.roles] : [],
        isActive: user.isActive
      }))
    };
  }

  async replaceStaff(staff = {}){
    const err = new Error('Legacy staff management is deprecated. Use /api/users to manage accounts.');
    err.status = 410;
    throw err;
  }

  async listUsers(){
    const usersTable = this._table('users');
    const userRolesTable = this._table('user_roles');
    const rows = await this._select(`
      SELECT u.id, u.name, u.email, u.password_hash, u.is_active, u.created_at, u.updated_at, u.must_change_password,
             ARRAY_REMOVE(ARRAY_AGG(r.role), NULL) AS roles
      FROM ${usersTable} u
      LEFT JOIN ${userRolesTable} r ON r.user_id = u.id
      GROUP BY u.id
      ORDER BY LOWER(u.name)
    `);
    return rows.map(row => this._mapUserRow(row));
  }

  async getUser(id){
    if(!id){
      return null;
    }
    const usersTable = this._table('users');
    const userRolesTable = this._table('user_roles');
    const row = await this._selectOne(`
      SELECT u.id, u.name, u.email, u.password_hash, u.is_active, u.created_at, u.updated_at, u.must_change_password,
             ARRAY_REMOVE(ARRAY_AGG(r.role), NULL) AS roles
      FROM ${usersTable} u
      LEFT JOIN ${userRolesTable} r ON r.user_id = u.id
      WHERE u.id = $1
      GROUP BY u.id
    `, [id]);
    return row ? this._mapUserRow(row) : null;
  }

  async getUserByEmail(email){
    if(typeof email !== 'string' || !email.trim()){
      return null;
    }
    const normalized = email.trim().toLowerCase();
    const usersTable = this._table('users');
    const userRolesTable = this._table('user_roles');
    const row = await this._selectOne(`
      SELECT u.id, u.name, u.email, u.password_hash, u.is_active, u.created_at, u.updated_at, u.must_change_password,
             ARRAY_REMOVE(ARRAY_AGG(r.role), NULL) AS roles
      FROM ${usersTable} u
      LEFT JOIN ${userRolesTable} r ON r.user_id = u.id
      WHERE LOWER(u.email) = LOWER($1)
      GROUP BY u.id
    `, [normalized]);
    return row ? this._mapUserRow(row) : null;
  }

  async createUser({name, email, passwordHash = null, roles = [], isActive = true, mustChangePassword = false}){
    const usersTable = this._table('users');
    const now = new Date();
    const id = uuidv4();
    const normalizedRoles = this._normalizeRoles(roles);
    await this._run(`
      INSERT INTO ${usersTable} (id, name, email, password_hash, is_active, must_change_password, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
    `, [id, name, email, passwordHash, isActive ? true : false, mustChangePassword ? true : false, now]);
    await this._replaceUserRoles(id, normalizedRoles, now);
    return this.getUser(id);
  }

  async updateUser(id, updates = {}){
    const existing = await this.getUser(id);
    if(!existing){
      return null;
    }
    const fields = [];
    const params = [];
    let paramIndex = 1;
    let touched = false;
    if(updates.name !== undefined){
      fields.push(`name = $${paramIndex += 1}`);
      params.push(updates.name);
    }
    if(updates.email !== undefined){
      fields.push(`email = $${paramIndex += 1}`);
      params.push(updates.email);
    }
    if(updates.isActive !== undefined){
      fields.push(`is_active = $${paramIndex += 1}`);
      params.push(updates.isActive ? true : false);
    }
    if(updates.mustChangePassword !== undefined){
      fields.push(`must_change_password = $${paramIndex += 1}`);
      params.push(updates.mustChangePassword ? true : false);
    }
    const now = new Date();
    if(fields.length){
      fields.push(`updated_at = $${paramIndex += 1}`);
      params.push(now);
      params.unshift(id);
      await this._run(`UPDATE ${this._table('users')} SET ${fields.join(', ')} WHERE id = $1`, params);
      touched = true;
    }
    if(updates.roles !== undefined){
      const normalizedRoles = this._normalizeRoles(updates.roles);
      await this._replaceUserRoles(id, normalizedRoles, now);
      touched = true;
    }
    if(touched && !fields.length){
      await this._run(`UPDATE ${this._table('users')} SET updated_at = $2 WHERE id = $1`, [id, now]);
    }
    return this.getUser(id);
  }

  async setUserPassword(id, passwordHash, options = {}){
    const now = new Date();
    const opts = options || {};
    const flag = opts.requireChange ? true : false;
    await this._run(`UPDATE ${this._table('users')} SET password_hash = $2, must_change_password = $3, updated_at = $4 WHERE id = $1`, [id, passwordHash, flag, now]);
    return this.getUser(id);
  }

  _assertRequiredShowFields(raw = {}){
    const date = typeof raw.date === 'string' ? raw.date.trim() : '';
    if(!date){
      const err = new Error('Date is required');
      err.status = 400;
      throw err;
    }
    const time = typeof raw.time === 'string' ? raw.time.trim() : '';
    if(!time){
      const err = new Error('Show start time is required');
      err.status = 400;
      throw err;
    }
    const label = typeof raw.label === 'string' ? raw.label.trim() : '';
    if(!label){
      const err = new Error('Show label is required');
      err.status = 400;
      throw err;
    }
    const leadName = this._extractAssignmentName(raw.lead ?? raw.leadPilot, 'lead');
    if(!leadName){
      const err = new Error('Lead assignment is required');
      err.status = 400;
      throw err;
    }
    const crewLeadName = this._extractAssignmentName(raw.crewLead ?? raw.monkeyLead, 'crew');
    if(!crewLeadName){
      const err = new Error('Crew lead assignment is required');
      err.status = 400;
      throw err;
    }
  }

  _normalizeShow(raw){
    const createdAt = typeof raw.createdAt === 'number' ? raw.createdAt : Number(raw.createdAt);
    const updatedAt = typeof raw.updatedAt === 'number' ? raw.updatedAt : Number(raw.updatedAt);
    const leadAssignment = this._normalizeUserAssignment(raw.lead !== undefined ? raw.lead : raw.leadPilot, 'lead');
    const crewLeadAssignment = this._normalizeUserAssignment(raw.crewLead !== undefined ? raw.crewLead : raw.monkeyLead, 'crew');
    const crewSource = raw.crewAssignments !== undefined ? raw.crewAssignments : raw.crew;
    const crewAssignments = this._normalizeAssignmentList(crewSource, 'crew');
    crewAssignments.sort((a, b)=>{
      const aName = (a.displayName || '').toLowerCase();
      const bName = (b.displayName || '').toLowerCase();
      if(aName < bName){
        return -1;
      }
      if(aName > bName){
        return 1;
      }
      return 0;
    });
    const crewNames = crewAssignments.map(item => item.displayName).filter(Boolean);
    return {
      id: raw.id,
      date: typeof raw.date === 'string' ? raw.date.trim() : '',
      time: typeof raw.time === 'string' ? raw.time.trim() : '',
      label: typeof raw.label === 'string' ? raw.label.trim() : '',
      lead: leadAssignment,
      crewLead: crewLeadAssignment,
      crewAssignments,
      crew: crewNames,
      leadPilot: leadAssignment.displayName,
      monkeyLead: crewLeadAssignment.displayName,
      notes: typeof raw.notes === 'string' ? raw.notes.trim() : '',
      entries: Array.isArray(raw.entries) ? raw.entries.map(e => this._normalizeEntry(e)) : [],
      createdAt: Number.isFinite(createdAt) ? createdAt : Date.now(),
      updatedAt: Number.isFinite(updatedAt) ? updatedAt : Date.now()
    };
  }

  _normalizeEntry(raw){
    const ts = typeof raw.ts === 'number' ? raw.ts : Number(raw.ts);
    const operatorSource = raw.assignedOperator !== undefined ? raw.assignedOperator
      : (raw.operator && typeof raw.operator === 'object' ? raw.operator : null);
    const operatorAssignment = this._normalizeUserAssignment(operatorSource, 'operator');
    let operatorName = typeof raw.operatorName === 'string' ? raw.operatorName.trim() : '';
    if(!operatorName && typeof raw.operator === 'string'){
      operatorName = raw.operator.trim();
    }
    if(!operatorName){
      operatorName = operatorAssignment.displayName;
    }
    let operatorId = typeof raw.operatorId === 'string' ? raw.operatorId.trim() : '';
    if(!operatorId){
      operatorId = operatorAssignment.userId || '';
    }
    const operatorRoles = operatorAssignment.roles && operatorAssignment.roles.length
      ? operatorAssignment.roles
      : ['operator'];
    const assignedOperator = {
      ...operatorAssignment,
      displayName: operatorName || operatorAssignment.displayName,
      roles: operatorRoles
    };
    return {
      id: raw.id || uuidv4(),
      ts: Number.isFinite(ts) ? ts : Date.now(),
      unitId: typeof raw.unitId === 'string' ? raw.unitId.trim() : '',
      planned: typeof raw.planned === 'string' ? raw.planned.trim() : '',
      launched: typeof raw.launched === 'string' ? raw.launched.trim() : '',
      status: typeof raw.status === 'string' ? raw.status.trim() : '',
      primaryIssue: typeof raw.primaryIssue === 'string' ? raw.primaryIssue.trim() : '',
      subIssue: typeof raw.subIssue === 'string' ? raw.subIssue.trim() : '',
      otherDetail: typeof raw.otherDetail === 'string' ? raw.otherDetail.trim() : '',
      severity: typeof raw.severity === 'string' ? raw.severity.trim() : '',
      rootCause: typeof raw.rootCause === 'string' ? raw.rootCause.trim() : '',
      actions: Array.isArray(raw.actions) ? this._normalizeNameList(raw.actions) : [],
      operator: operatorName,
      operatorName,
      operatorId: operatorId || null,
      operatorRoles,
      assignedOperator,
      batteryId: typeof raw.batteryId === 'string' ? raw.batteryId.trim() : '',
      delaySec: raw.delaySec === null || raw.delaySec === undefined || raw.delaySec === ''
        ? null
        : Number(raw.delaySec),
      commandRx: typeof raw.commandRx === 'string' ? raw.commandRx.trim() : '',
      notes: typeof raw.notes === 'string' ? raw.notes.trim() : ''
    };
  }

  async _enforceShowLimit(date, excludeId){
    const trimmedDate = typeof date === 'string' ? date.trim() : '';
    if(!trimmedDate){
      return;
    }
    const shows = await this.listShows();
    const matching = shows.filter(show => {
      if(!show || typeof show !== 'object'){
        return false;
      }
      const showDate = typeof show.date === 'string' ? show.date.trim() : '';
      if(showDate !== trimmedDate){
        return false;
      }
      return show.id !== excludeId;
    });
    if(matching.length >= 5){
      const err = new Error('Daily show limit reached. Maximum of 5 shows per date.');
      err.status = 400;
      throw err;
    }
  }

  _assertPilotUnique(show, entry){
    if(!show){
      return;
    }
    const normalizedId = typeof entry.operatorId === 'string' ? entry.operatorId.trim().toLowerCase() : '';
    const normalizedName = (entry.operatorName || entry.operator || '').trim().toLowerCase();
    if(!normalizedId && !normalizedName){
      return;
    }
    const hasDuplicate = (show.entries || []).some(existing => {
      if(!existing){
        return false;
      }
      if(existing.id === entry.id){
        return false;
      }
      const existingId = typeof existing.operatorId === 'string' ? existing.operatorId.trim().toLowerCase() : '';
      if(normalizedId && existingId && existingId === normalizedId){
        return true;
      }
      const existingName = (existing.operatorName || existing.operator || '').trim().toLowerCase();
      if(normalizedName && existingName && existingName === normalizedName){
        return true;
      }
      return false;
    });
    if(hasDuplicate){
      const err = new Error('Operator already has an entry for this show.');
      err.status = 400;
      throw err;
    }
  }

  async _ensureSchema(){
    const showsTable = this._table('shows');
    const archiveTable = this._table('show_archive');
    const usersTable = this._table('users');
    const userRolesTable = this._table('user_roles');
    const legacyStaffTable = this._table('staff');
    const legacyMonkeyTable = this._table('monkey_leads');
    await this._run(`
      CREATE TABLE IF NOT EXISTS ${showsTable} (
        id UUID PRIMARY KEY,
        data JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
      )
    `);
    await this._run(`
      CREATE TABLE IF NOT EXISTS ${archiveTable} (
        id UUID PRIMARY KEY,
        data JSONB NOT NULL,
        show_date TEXT,
        created_at TIMESTAMPTZ,
        archived_at TIMESTAMPTZ NOT NULL,
        deleted_at TIMESTAMPTZ
      )
    `);
    await this._run(`
      CREATE TABLE IF NOT EXISTS ${usersTable} (
        id UUID PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        password_hash TEXT,
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        must_change_password BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
      )
    `);
    await this._run(`ALTER TABLE ${usersTable} ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN NOT NULL DEFAULT FALSE`);
    await this._run(`
      CREATE TABLE IF NOT EXISTS ${userRolesTable} (
        user_id UUID NOT NULL REFERENCES ${usersTable}(id) ON DELETE CASCADE,
        role TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (user_id, role)
      )
    `);
    await this._run(`CREATE UNIQUE INDEX IF NOT EXISTS ${this._indexName('users_email_unique')} ON ${usersTable} (LOWER(email))`);
    await this._run(`CREATE INDEX IF NOT EXISTS ${this._indexName('user_roles_role_idx')} ON ${userRolesTable} (role)`);
    // Legacy tables retained for backward compatibility with deployments that still reference them
    await this._run(`
      CREATE TABLE IF NOT EXISTS ${legacyStaffTable} (
        id UUID PRIMARY KEY,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
      )
    `);
    await this._run(`
      CREATE TABLE IF NOT EXISTS ${legacyMonkeyTable} (
        id UUID PRIMARY KEY,
        name TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
      )
    `);
    await this._run(`CREATE INDEX IF NOT EXISTS ${this._indexName('show_archive_archived_at_idx')} ON ${archiveTable} (archived_at DESC)`);
  }

  async _persist(show, client = null){
    const normalized = this._normalizeShow(show);
    const query = `
      INSERT INTO ${this._table('shows')} (id, data, updated_at)
      VALUES ($1, $2::jsonb, $3)
      ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at
    `;
    const params = [normalized.id, JSON.stringify(normalized), this._toDate(normalized.updatedAt)];
    if(client){
      await client.query(query, params);
    }else{
      await this.pool.query(query, params);
    }
    return normalized;
  }

  async _refreshArchive(){
    if(!this.pool){
      return;
    }
    await this._archiveDailyShows();
    await this._purgeExpiredArchives();
  }

  async _archiveDailyShows(){
    const showsTable = this._table('shows');
    const rows = await this._select(`SELECT id, data FROM ${showsTable}`);
    if(!rows.length){
      return false;
    }
    const groups = new Map();
    rows.forEach(row => {
      const show = this._parseRowData(row.data);
      if(!show || typeof show !== 'object'){
        return;
      }
      const key = typeof show.date === 'string' && show.date.trim() ? show.date.trim() : '__undated__';
      const createdAt = this._getTimestamp(show.createdAt) ?? this._getTimestamp(show.updatedAt);
      if(!groups.has(key)){
        groups.set(key, []);
      }
      groups.get(key).push({show, createdAt});
    });
    const now = Date.now();
    const showsToArchive = [];
    for(const [, list] of groups.entries()){
      const earliest = list.reduce((min, item)=>{
        const value = this._getTimestamp(item.createdAt);
        if(value === null){
          return min;
        }
        if(min === null || value < min){
          return value;
        }
        return min;
      }, null);
      if(earliest === null){
        continue;
      }
      if(now - earliest >= DAY_IN_MS){
        list.forEach(item => showsToArchive.push(item.show));
      }
    }
    if(!showsToArchive.length){
      return false;
    }
    await this._withClient(async client =>{
      for(const show of showsToArchive){
        const normalized = this._normalizeShow(show);
        const archiveTime = Date.now();
        await this._saveArchiveRow(normalized, archiveTime, null, client);
        await client.query(`DELETE FROM ${showsTable} WHERE id = $1`, [normalized.id]);
      }
    });
    return true;
  }

  async _purgeExpiredArchives(){
    const archiveTable = this._table('show_archive');
    const rows = await this._select(`SELECT id, data, created_at FROM ${archiveTable}`);
    if(!rows.length){
      return false;
    }
    const now = Date.now();
    const expiredIds = [];
    rows.forEach(row =>{
      const show = this._parseRowData(row.data);
      const createdAt = this._getTimestamp(show?.createdAt) ?? this._getTimestamp(row.created_at);
      if(createdAt === null){
        return;
      }
      if(this._isArchiveExpired(createdAt, now)){
        expiredIds.push(row.id);
      }
    });
    if(!expiredIds.length){
      return false;
    }
    await this._run(`DELETE FROM ${archiveTable} WHERE id = ANY($1::uuid[])`, [expiredIds]);
    return true;
  }

  async _saveArchiveRow(show, archivedAt, deletedAt, client = null){
    const archiveTimestamp = this._getTimestamp(archivedAt) ?? Date.now();
    const createdTimestamp = this._getTimestamp(show.createdAt);
    const deletedTimestamp = this._getTimestamp(deletedAt ?? show.deletedAt);
    show.archivedAt = archiveTimestamp;
    if(createdTimestamp !== null){
      show.createdAt = createdTimestamp;
    }
    if(deletedTimestamp !== null){
      show.deletedAt = deletedTimestamp;
    }else{
      delete show.deletedAt;
    }
    const query = `
      INSERT INTO ${this._table('show_archive')} (id, data, show_date, created_at, archived_at, deleted_at)
      VALUES ($1, $2::jsonb, $3, $4, $5, $6)
      ON CONFLICT(id) DO UPDATE SET data = EXCLUDED.data, show_date = EXCLUDED.show_date, created_at = EXCLUDED.created_at, archived_at = EXCLUDED.archived_at, deleted_at = EXCLUDED.deleted_at
    `;
    const params = [
      show.id,
      JSON.stringify(show),
      typeof show.date === 'string' && show.date.trim() ? show.date.trim() : null,
      this._toDate(createdTimestamp),
      this._toDate(archiveTimestamp),
      this._toDate(deletedTimestamp)
    ];
    if(client){
      await client.query(query, params);
    }else{
      await this.pool.query(query, params);
    }
  }

  _mapArchiveRow(row){
    if(!row){
      return null;
    }
    const show = this._parseRowData(row.data);
    if(!show || typeof show !== 'object'){
      return null;
    }
    const archivedAt = this._getTimestamp(row.archived_at) ?? this._getTimestamp(show.archivedAt);
    const createdAt = this._getTimestamp(row.created_at) ?? this._getTimestamp(show.createdAt);
    const deletedAt = this._getTimestamp(row.deleted_at) ?? this._getTimestamp(show.deletedAt);
    if(archivedAt !== null){
      show.archivedAt = archivedAt;
    }
    if(createdAt !== null){
      show.createdAt = createdAt;
    }
    if(deletedAt !== null){
      show.deletedAt = deletedAt;
    }else{
      delete show.deletedAt;
    }
    if(!Array.isArray(show.entries)){
      show.entries = [];
    }
    if(!Array.isArray(show.crew)){
      show.crew = [];
    }
    return show;
  }

  async _replaceUserRoles(userId, roles = [], timestamp = new Date()){
    if(!userId){
      return;
    }
    const userRolesTable = this._table('user_roles');
    await this._run(`DELETE FROM ${userRolesTable} WHERE user_id = $1`, [userId]);
    const normalized = this._normalizeRoles(roles);
    for(const role of normalized){
      await this._run(`INSERT INTO ${userRolesTable} (user_id, role, created_at) VALUES ($1, $2, $3)`, [userId, role, timestamp]);
    }
  }

  _mapUserRow(row){
    if(!row){
      return null;
    }
    const roleList = Array.isArray(row.roles) ? row.roles : (typeof row.roles === 'string' ? row.roles.split(',') : []);
    const normalizedRoles = normalizeRoles(roleList);
    return {
      id: row.id,
      name: row.name,
      email: row.email,
      passwordHash: row.password_hash || null,
      isActive: row.is_active === true || row.is_active === 1 || row.is_active === '1',
      createdAt: row.created_at ? new Date(row.created_at).toISOString() : new Date().toISOString(),
      updatedAt: row.updated_at ? new Date(row.updated_at).toISOString() : (row.created_at ? new Date(row.created_at).toISOString() : new Date().toISOString()),
      roles: Array.from(new Set(normalizedRoles)),
      mustChangePassword: row.must_change_password === true || row.must_change_password === 1 || row.must_change_password === '1'
    };
  }

  _normalizeRoles(roles){
    const normalized = normalizeRoles(roles);
    const filtered = normalized.filter(role => VALID_ROLES.includes(role));
    if(filtered.length === 0){
      throw new Error('At least one valid role is required.');
    }
    return filtered;
  }

  _normalizeDisplayName(value){
    if(typeof value === 'string'){
      return value.trim();
    }
    return '';
  }

  _normalizeUserAssignment(raw, defaultRole = null){
    const roles = [];
    if(defaultRole){
      roles.push(defaultRole);
    }
    let userId = null;
    let email = null;
    let displayName = '';
    if(typeof raw === 'string'){
      displayName = this._normalizeDisplayName(raw);
    }else if(raw && typeof raw === 'object'){
      if(typeof raw.userId === 'string' && raw.userId.trim()){
        userId = raw.userId.trim();
      }else if(typeof raw.id === 'string' && raw.id.trim()){
        userId = raw.id.trim();
      }
      if(typeof raw.email === 'string' && raw.email.trim()){
        email = raw.email.trim();
      }
      const candidateNames = [raw.displayName, raw.name, raw.fullName, raw.label, raw.monkeyLead];
      for(const candidate of candidateNames){
        const normalizedName = this._normalizeDisplayName(candidate);
        if(normalizedName){
          displayName = normalizedName;
          break;
        }
      }
      if(!displayName && raw && typeof raw.toString === 'function'){
        const fallback = this._normalizeDisplayName(raw.toString());
        if(fallback && fallback !== '[object Object]'){
          displayName = fallback;
        }
      }
      if(Array.isArray(raw.roles)){
        roles.push(...raw.roles);
      }
      if(typeof raw.role === 'string'){
        roles.push(raw.role);
      }
    }
    const normalizedRoles = normalizeRoles(roles);
    const finalRoles = normalizedRoles.length ? normalizedRoles : (defaultRole ? [defaultRole] : []);
    const assignment = {
      userId: userId || null,
      displayName,
      roles: finalRoles
    };
    if(email){
      assignment.email = email;
    }
    if(finalRoles.length){
      assignment.primaryRole = finalRoles[0];
    }else if(defaultRole){
      assignment.primaryRole = defaultRole;
    }
    return assignment;
  }

  _normalizeAssignmentList(list = [], defaultRole = null){
    const normalized = Array.isArray(list) ? list : [];
    const assignments = [];
    const seen = new Set();
    normalized.forEach(entry =>{
      const assignment = this._normalizeUserAssignment(entry, defaultRole);
      const key = assignment.userId ? `id:${assignment.userId}` : assignment.displayName ? `name:${assignment.displayName.toLowerCase()}` : null;
      if(!key){
        return;
      }
      if(seen.has(key)){
        return;
      }
      seen.add(key);
      assignments.push(assignment);
    });
    return assignments;
  }

  _extractAssignmentName(raw, defaultRole = null){
    const assignment = this._normalizeUserAssignment(raw, defaultRole);
    return assignment.displayName;
  }

  _normalizeNameList(list, options = {}){
    if(!Array.isArray(list)){
      return [];
    }
    const trimmed = list
      .map(item => typeof item === 'string' ? item.trim() : '')
      .filter(Boolean);
    if(options.sort){
      trimmed.sort((a, b) => a.localeCompare(b));
    }
    return Array.from(new Set(trimmed));
  }

  _parseRowData(value){
    if(value === null || value === undefined){
      return null;
    }
    if(typeof value === 'object'){
      return value;
    }
    try{
      return JSON.parse(value);
    }catch(err){
      return null;
    }
  }

  _getTimestamp(value){
    if(typeof value === 'number' && Number.isFinite(value)){
      return value;
    }
    const numeric = Number(value);
    if(Number.isFinite(numeric)){
      return numeric;
    }
    if(value instanceof Date){
      const time = value.getTime();
      return Number.isFinite(time) ? time : null;
    }
    if(typeof value === 'string'){
      const parsed = Date.parse(value);
      if(Number.isFinite(parsed)){
        return parsed;
      }
    }
    return null;
  }

  _toDate(value){
    const ts = this._getTimestamp(value);
    return ts === null ? null : new Date(ts);
  }

  _isArchiveExpired(createdAt, now = Date.now()){
    if(!Number.isFinite(createdAt)){
      return false;
    }
    const expiry = this._addMonths(createdAt, ARCHIVE_RETENTION_MONTHS);
    return now >= expiry;
  }

  _addMonths(timestamp, months){
    if(!Number.isFinite(timestamp)){
      return timestamp;
    }
    const date = new Date(timestamp);
    if(Number.isNaN(date.getTime())){
      return timestamp;
    }
    date.setMonth(date.getMonth() + months);
    return date.getTime();
  }

  async _select(query, params = []){
    const result = await this.pool.query(query, params);
    return result.rows;
  }

  async _selectOne(query, params = []){
    const rows = await this._select(query, params);
    return rows.length ? rows[0] : null;
  }

  async _run(query, params = []){
    await this.pool.query(query, params);
  }

  async _withClient(handler, {transaction = true} = {}){
    const client = await this.pool.connect();
    try{
      if(transaction){
        await client.query('BEGIN');
      }
      const result = await handler(client);
      if(transaction){
        await client.query('COMMIT');
      }
      return result;
    }catch(err){
      if(transaction){
        try{
          await client.query('ROLLBACK');
        }catch(rollbackErr){
          console.error('Failed to rollback transaction', rollbackErr);
        }
      }
      throw err;
    }finally{
      client.release();
    }
  }

  _createPool(config){
    return new Pool(config);
  }

  _buildPoolConfig(){
    const cfg = this.config || {};
    const poolConfig = {...(cfg.pool || {})};
    const envConnectionString = process.env.DATABASE_URL || process.env.POSTGRES_URL || process.env.PGURL;
    if(cfg.connectionString){
      poolConfig.connectionString = cfg.connectionString;
    }else if(envConnectionString){
      poolConfig.connectionString = envConnectionString;
    }
    const envHost = process.env.PGHOST || process.env.POSTGRES_HOST;
    const envPort = Number.parseInt(process.env.PGPORT || process.env.POSTGRES_PORT, 10);
    const envDatabase = process.env.PGDATABASE || process.env.POSTGRES_DB;
    const envUser = process.env.PGUSER || process.env.POSTGRES_USER;
    const envPassword = process.env.PGPASSWORD || process.env.POSTGRES_PASSWORD;
    ['host','port','database','user','password'].forEach(key =>{
      if(cfg[key] !== undefined && cfg[key] !== null && cfg[key] !== ''){
        poolConfig[key] = cfg[key];
      }
    });
    if(!poolConfig.host && envHost){
      poolConfig.host = envHost;
    }
    if(!poolConfig.port && Number.isFinite(envPort)){
      poolConfig.port = envPort;
    }
    if(!poolConfig.database && envDatabase){
      poolConfig.database = envDatabase;
    }
    if(!poolConfig.user && envUser){
      poolConfig.user = envUser;
    }
    if(!poolConfig.password && envPassword){
      poolConfig.password = envPassword;
    }
    const envSslMode = (process.env.PGSSLMODE || process.env.POSTGRES_SSLMODE || '').toLowerCase();
    if(cfg.ssl){
      if(typeof cfg.ssl === 'object'){
        poolConfig.ssl = cfg.ssl;
      }else if(cfg.ssl === true){
        poolConfig.ssl = {rejectUnauthorized: false};
      }
    }else if(envSslMode){
      if(envSslMode === 'disable'){
        poolConfig.ssl = false;
      }else if(['require','prefer'].includes(envSslMode)){
        poolConfig.ssl = {rejectUnauthorized: false};
      }
    }
    if(Number.isFinite(cfg.max)){
      poolConfig.max = cfg.max;
    }
    if(Number.isFinite(cfg.idleTimeoutMillis)){
      poolConfig.idleTimeoutMillis = cfg.idleTimeoutMillis;
    }
    if(Number.isFinite(cfg.connectionTimeoutMillis)){
      poolConfig.connectionTimeoutMillis = cfg.connectionTimeoutMillis;
    }
    if(Number.isFinite(cfg.statement_timeout)){
      poolConfig.statement_timeout = cfg.statement_timeout;
    }
    if(!poolConfig.connectionString && !poolConfig.host){
      poolConfig.host = '127.0.0.1';
      poolConfig.port = 5432;
      poolConfig.database = 'monkey_tracker';
      poolConfig.user = 'postgres';
      poolConfig.password = cfg.password || 'postgres';
    }
    return poolConfig;
  }

  async _ensureDatabaseExists(poolConfig){
    const databaseName = this._getDatabaseNameFromConfig(poolConfig);
    if(!databaseName){
      return;
    }
    let probePool = null;
    try{
      probePool = this._createPool(poolConfig);
      await probePool.query('SELECT 1');
    }catch(err){
      if(err?.code !== '3D000'){
        throw err;
      }
      await this._createDatabaseIfMissing(poolConfig, databaseName);
    }finally{
      if(probePool){
        try{
          await probePool.end();
        }catch(poolErr){
          console.error('Failed to dispose probe pool', poolErr);
        }
      }
    }
  }

  async _createDatabaseIfMissing(poolConfig, databaseName){
    const adminConfig = this._buildAdminPoolConfig(poolConfig);
    let adminPool = null;
    try{
      adminPool = this._createPool(adminConfig);
      await adminPool.query(`CREATE DATABASE ${this._quoteIdentifier(databaseName)}`);
    }catch(err){
      if(err?.code === '42P04'){
        return;
      }
      throw err;
    }finally{
      if(adminPool){
        try{
          await adminPool.end();
        }catch(poolErr){
          console.error('Failed to dispose admin pool', poolErr);
        }
      }
    }
  }

  _buildAdminPoolConfig(poolConfig){
    const adminDatabase = this.config?.adminDatabase
      || process.env.PGADMIN_DB
      || process.env.PGDEFAULT_DB
      || 'postgres';
    if(poolConfig.connectionString){
      try{
        const url = new URL(poolConfig.connectionString);
        url.pathname = `/${encodeURIComponent(adminDatabase)}`;
        const adminConfig = {...poolConfig, connectionString: url.toString()};
        if(poolConfig.ssl !== undefined){
          adminConfig.ssl = poolConfig.ssl;
        }
        return adminConfig;
      }catch(err){
        console.error('Failed to parse connection string for admin pool', err);
      }
    }
    return {
      ...poolConfig,
      database: adminDatabase
    };
  }

  _getDatabaseNameFromConfig(poolConfig){
    if(poolConfig.database){
      return poolConfig.database;
    }
    if(poolConfig.connectionString){
      try{
        const url = new URL(poolConfig.connectionString);
        const pathname = url.pathname || '';
        const dbName = decodeURIComponent(pathname.replace(/^\//, ''));
        return dbName || null;
      }catch(err){
        console.error('Failed to parse connection string for database name', err);
      }
    }
    return null;
  }

  _sanitizeIdentifier(value){
    if(typeof value !== 'string'){
      return null;
    }
    const trimmed = value.trim();
    if(!trimmed){
      return null;
    }
    if(!IDENTIFIER_REGEX.test(trimmed)){
      throw new Error(`Invalid identifier: ${trimmed}`);
    }
    return trimmed;
  }

  _formatIdentifier(identifier){
    if(!IDENTIFIER_REGEX.test(identifier)){
      throw new Error(`Invalid identifier: ${identifier}`);
    }
    return `"${identifier}"`;
  }

  _quoteIdentifier(identifier){
    if(typeof identifier !== 'string' || !identifier){
      throw new Error(`Invalid identifier: ${identifier}`);
    }
    return `"${identifier.replace(/"/g, '""')}"`;
  }

  _table(name){
    if(!IDENTIFIER_REGEX.test(name)){
      throw new Error(`Invalid table name: ${name}`);
    }
    if(this.schema){
      return `${this._formatIdentifier(this.schema)}.${this._formatIdentifier(name)}`;
    }
    return this._formatIdentifier(name);
  }

  _indexName(name){
    const base = `${this.schema || 'public'}_${name}`;
    if(!IDENTIFIER_REGEX.test(base)){
      throw new Error(`Invalid index name: ${base}`);
    }
    return this._formatIdentifier(base.toLowerCase());
  }
}

module.exports = PostgresProvider;
