const fs = require('fs');
const path = require('path');
const initSqlJs = require('sql.js');
const { v4: uuidv4 } = require('uuid');

const DAY_IN_MS = 24 * 60 * 60 * 1000;
const ARCHIVE_RETENTION_MONTHS = 2;
const VALID_ROLES = ['admin', 'lead', 'operator', 'crew'];

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
  values.forEach(value => {
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

class SqlProvider {
  constructor(config = {}){
    this.config = config;
    this.db = null;
    this.SQL = null;
    this.filename = this.config.filename || path.join(process.cwd(), 'data', 'monkey-tracker.sqlite');
  }

  async init(){
    if(!this.SQL){
      const wasmDir = path.dirname(require.resolve('sql.js/dist/sql-wasm.js'));
      this.SQL = await initSqlJs({
        locateFile: (file) => path.join(wasmDir, file)
      });
    }

    await fs.promises.mkdir(path.dirname(this.filename), {recursive: true});

    if(this.db){
      return;
    }

    let shouldPersist = false;
    if(await this._fileExists(this.filename)){
      const content = await fs.promises.readFile(this.filename);
      this.db = new this.SQL.Database(content);
      shouldPersist = this._ensureSchema();
    }else{
      this.db = new this.SQL.Database();
      this._ensureSchema();
      shouldPersist = true;
    }

    if(shouldPersist){
      await this._persistDatabase();
    }

    await this._refreshArchive();
  }

  async dispose(){
    if(this.db){
      this.db.close();
      this.db = null;
    }
  }

  getStorageLabel(){
    return 'SQL.js v2';
  }

  getStorageMetadata(){
    return {
      label: this.getStorageLabel(),
      driver: 'sqljs',
      filename: this.filename
    };
  }

  async listShows(){
    await this._refreshArchive();
    const rows = this._select('SELECT data FROM shows ORDER BY updated_at DESC');
    return rows.map(r => JSON.parse(r.data));
  }

  async getShow(id){
    await this._refreshArchive();
    const row = this._selectOne('SELECT data FROM shows WHERE id = ?', [id]);
    return row ? JSON.parse(row.data) : null;
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
    const row = this._selectOne('SELECT data FROM shows WHERE id = ?', [id]);
    if(!row){
      return null;
    }
    let show;
    try{
      show = JSON.parse(row.data);
    }catch(err){
      show = null;
    }
    if(!show || typeof show !== 'object'){
      this._run('DELETE FROM shows WHERE id = ?', [id]);
      await this._persistDatabase();
      return null;
    }
    const normalized = this._normalizeShow(show);
    const archiveTime = Date.now();
    normalized.archivedAt = archiveTime;
    normalized.deletedAt = archiveTime;
    this._saveArchiveRow(normalized, archiveTime, archiveTime);
    this._run('DELETE FROM shows WHERE id = ?', [normalized.id]);
    await this._persistDatabase();
    return this.getArchivedShow(id);
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
    const rows = this._select('SELECT data, archived_at, created_at FROM show_archive ORDER BY archived_at DESC, id ASC');
    return rows.map(row => this._mapArchiveRow(row)).filter(Boolean);
  }

  async getArchivedShow(id){
    if(!id){
      return null;
    }
    await this._refreshArchive();
    const row = this._selectOne('SELECT data, archived_at, created_at FROM show_archive WHERE id = ?', [id]);
    return row ? this._mapArchiveRow(row) : null;
  }

  async archiveShowNow(id){
    if(!id){
      return null;
    }
    const row = this._selectOne('SELECT data FROM shows WHERE id = ?', [id]);
    if(!row){
      return this.getArchivedShow(id);
    }
    let show;
    try{
      show = JSON.parse(row.data);
    }catch(err){
      show = null;
    }
    if(!show || typeof show !== 'object'){
      return null;
    }
    const normalized = this._normalizeShow(show);
    const archiveTime = Date.now();
    this._saveArchiveRow(normalized, archiveTime, null);
    this._run('DELETE FROM shows WHERE id = ?', [normalized.id]);
    await this._persistDatabase();
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
    const rows = this._select(`
      SELECT u.id, u.name, u.email, u.password_hash, u.is_active, u.created_at, u.updated_at, u.must_change_password,
             GROUP_CONCAT(r.role) AS roles
      FROM users u
      LEFT JOIN user_roles r ON r.user_id = u.id
      GROUP BY u.id
      ORDER BY LOWER(u.name)
    `);
    return rows.map(row => this._mapUserRow(row));
  }

  async getUser(id){
    if(!id){
      return null;
    }
    const row = this._selectOne(`
      SELECT u.id, u.name, u.email, u.password_hash, u.is_active, u.created_at, u.updated_at, u.must_change_password,
             GROUP_CONCAT(r.role) AS roles
      FROM users u
      LEFT JOIN user_roles r ON r.user_id = u.id
      WHERE u.id = ?
      GROUP BY u.id
    `, [id]);
    return row ? this._mapUserRow(row) : null;
  }

  async getUserByEmail(email){
    if(typeof email !== 'string' || !email.trim()){
      return null;
    }
    const normalized = email.trim().toLowerCase();
    const row = this._selectOne(`
      SELECT u.id, u.name, u.email, u.password_hash, u.is_active, u.created_at, u.updated_at, u.must_change_password,
             GROUP_CONCAT(r.role) AS roles
      FROM users u
      LEFT JOIN user_roles r ON r.user_id = u.id
      WHERE LOWER(u.email) = LOWER(?)
      GROUP BY u.id
    `, [normalized]);
    return row ? this._mapUserRow(row) : null;
  }

  async createUser({name, email, passwordHash = null, roles = [], isActive = true, mustChangePassword = false}){
    const now = new Date().toISOString();
    const id = uuidv4();
    const normalizedRoles = this._normalizeRoles(roles);
    this._run(`
      INSERT INTO users (id, name, email, password_hash, is_active, created_at, updated_at, must_change_password)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `, [
      id,
      name,
      email,
      passwordHash,
      isActive ? 1 : 0,
      now,
      now,
      mustChangePassword ? 1 : 0
    ]);
    this._replaceUserRoles(id, normalizedRoles, now);
    await this._persistDatabase();
    return this.getUser(id);
  }

  async updateUser(id, updates = {}){
    const existing = await this.getUser(id);
    if(!existing){
      return null;
    }
    const fields = [];
    const params = [];
    const timestamp = new Date().toISOString();
    let touched = false;
    if(updates.name !== undefined){
      fields.push('name = ?');
      params.push(updates.name);
    }
    if(updates.email !== undefined){
      fields.push('email = ?');
      params.push(updates.email);
    }
    if(updates.isActive !== undefined){
      fields.push('is_active = ?');
      params.push(updates.isActive ? 1 : 0);
    }
    if(updates.mustChangePassword !== undefined){
      fields.push('must_change_password = ?');
      params.push(updates.mustChangePassword ? 1 : 0);
    }
    if(fields.length){
      fields.push('updated_at = ?');
      params.push(timestamp);
      params.push(id);
      this._run(`UPDATE users SET ${fields.join(', ')} WHERE id = ?`, params);
      touched = true;
    }
    if(updates.roles !== undefined){
      const normalizedRoles = this._normalizeRoles(updates.roles);
      this._replaceUserRoles(id, normalizedRoles, timestamp);
      touched = true;
    }
    if(touched && !fields.length){
      this._run('UPDATE users SET updated_at = ? WHERE id = ?', [timestamp, id]);
    }
    await this._persistDatabase();
    return this.getUser(id);
  }

  async setUserPassword(id, passwordHash, options = {}){
    if(!id){
      return null;
    }
    const opts = options || {};
    const timestamp = new Date().toISOString();
    const flag = opts.requireChange ? 1 : 0;
    this._run('UPDATE users SET password_hash = ?, must_change_password = ?, updated_at = ? WHERE id = ?', [passwordHash, flag, timestamp, id]);
    await this._persistDatabase();
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

  _ensureSchema(){
    let mutated = false;
    this.db.exec('PRAGMA foreign_keys = ON;');
    if(!this._tableExists('shows')){
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS shows (
          id TEXT PRIMARY KEY,
          data TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
      `);
      mutated = true;
    }else{
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS shows (
          id TEXT PRIMARY KEY,
          data TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
      `);
    }

    if(!this._tableExists('users')){
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS users (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT NOT NULL UNIQUE,
          password_hash TEXT,
          is_active INTEGER NOT NULL DEFAULT 1,
          must_change_password INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
      `);
      mutated = true;
    }else{
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS users (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT NOT NULL UNIQUE,
          password_hash TEXT,
          is_active INTEGER NOT NULL DEFAULT 1,
          must_change_password INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
      `);
      if(!this._columnExists('users', 'password_hash')){
        this.db.exec('ALTER TABLE users ADD COLUMN password_hash TEXT');
        mutated = true;
      }
      if(!this._columnExists('users', 'is_active')){
        this.db.exec('ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1');
        mutated = true;
      }
      if(!this._columnExists('users', 'must_change_password')){
        this.db.exec('ALTER TABLE users ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 0');
        mutated = true;
      }
      if(!this._columnExists('users', 'created_at')){
        this.db.exec('ALTER TABLE users ADD COLUMN created_at TEXT');
        mutated = true;
      }
      if(!this._columnExists('users', 'updated_at')){
        this.db.exec('ALTER TABLE users ADD COLUMN updated_at TEXT');
        mutated = true;
      }
    }

    this.db.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users(email)');

    if(!this._tableExists('user_roles')){
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS user_roles (
          user_id TEXT NOT NULL,
          role TEXT NOT NULL,
          created_at TEXT NOT NULL,
          PRIMARY KEY (user_id, role),
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
      `);
      mutated = true;
    }else{
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS user_roles (
          user_id TEXT NOT NULL,
          role TEXT NOT NULL,
          created_at TEXT NOT NULL,
          PRIMARY KEY (user_id, role),
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
      `);
    }

    this.db.exec('CREATE INDEX IF NOT EXISTS idx_user_roles_role ON user_roles(role)');

    if(!this._tableExists('staff')){
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS staff (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          role TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
      `);
      mutated = true;
    }else{
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS staff (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          role TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
      `);
    }

    if(!this._tableExists('monkey_leads')){
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS monkey_leads (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
      `);
      mutated = true;
    }else{
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS monkey_leads (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
      `);
    }

    if(!this._tableExists('show_archive')){
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS show_archive (
          id TEXT PRIMARY KEY,
          data TEXT NOT NULL,
          show_date TEXT,
          created_at TEXT,
          archived_at TEXT NOT NULL,
          deleted_at TEXT
        )
      `);
      mutated = true;
    }else{
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS show_archive (
          id TEXT PRIMARY KEY,
          data TEXT NOT NULL,
          show_date TEXT,
          created_at TEXT,
          archived_at TEXT NOT NULL,
          deleted_at TEXT
        )
      `);
      if(!this._columnExists('show_archive', 'deleted_at')){
        this.db.exec('ALTER TABLE show_archive ADD COLUMN deleted_at TEXT');
        mutated = true;
      }
    }

    return mutated;
  }

  _replaceUserRoles(userId, roles = [], timestamp = new Date().toISOString()){
    if(!userId){
      return;
    }
    this._run('DELETE FROM user_roles WHERE user_id = ?', [userId]);
    const normalized = this._normalizeRoles(roles);
    normalized.forEach(role =>{
      this._run('INSERT INTO user_roles (user_id, role, created_at) VALUES (?, ?, ?)', [userId, role, timestamp]);
    });
  }

  _mapUserRow(row){
    if(!row){
      return null;
    }
    const roleList = typeof row.roles === 'string' && row.roles.length
      ? row.roles.split(',').map(value => normalizeRole(value)).filter(Boolean)
      : [];
    const uniqueRoles = Array.from(new Set(roleList));
    return {
      id: row.id,
      name: row.name,
      email: row.email,
      passwordHash: row.password_hash || null,
      isActive: row.is_active === 1 || row.is_active === '1' || row.is_active === true,
      createdAt: row.created_at ? row.created_at : new Date().toISOString(),
      updatedAt: row.updated_at ? row.updated_at : row.created_at,
      roles: uniqueRoles,
      mustChangePassword: row.must_change_password === 1 || row.must_change_password === '1' || row.must_change_password === true
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
        const normalized = this._normalizeDisplayName(candidate);
        if(normalized){
          displayName = normalized;
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

  _normalizeNameList(list = [], options = {}){
    const {sort = false} = options;
    const seen = new Set();
    const result = [];
    list.forEach(name =>{
      const value = typeof name === 'string' ? name.trim() : '';
      if(!value){
        return;
      }
      const key = value.toLowerCase();
      if(seen.has(key)){
        return;
      }
      seen.add(key);
      result.push(value);
    });
    if(sort){
      result.sort((a,b)=> a.localeCompare(b, undefined, {sensitivity: 'base'}));
    }
    return result;
  }

  _tableExists(name){
    const row = this._selectOne("SELECT name FROM sqlite_master WHERE type='table' AND name = ?", [name]);
    return Boolean(row);
  }

  _columnExists(table, column){
    if(!table || !column){
      return false;
    }
    const rows = this._select(`PRAGMA table_info(${table})`);
    return rows.some(row => row.name === column);
  }

  async _persist(show){
    const payload = JSON.stringify(show);
    const updated = new Date(show.updatedAt || Date.now()).toISOString();
    this._run(`
      INSERT INTO shows (id, data, updated_at) VALUES (?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET data = excluded.data, updated_at = excluded.updated_at
    `, [show.id, payload, updated]);
    await this._persistDatabase();
  }

  _saveArchiveRow(show, archivedAt, deletedAt){
    const archiveTimestamp = this._getTimestamp(archivedAt) ?? Date.now();
    const deletedTimestamp = this._getTimestamp(deletedAt);
    show.archivedAt = archiveTimestamp;
    if(deletedTimestamp !== null){
      show.deletedAt = deletedTimestamp;
    }else{
      delete show.deletedAt;
    }
    const payload = JSON.stringify(show);
    const showDate = typeof show.date === 'string' ? show.date.trim() : '';
    this._run(`
      INSERT INTO show_archive (id, data, show_date, created_at, archived_at, deleted_at)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET data = excluded.data, show_date = excluded.show_date, created_at = excluded.created_at, archived_at = excluded.archived_at, deleted_at = excluded.deleted_at
    `, [
      show.id,
      payload,
      showDate,
      this._stringifyTimestamp(this._getTimestamp(show.createdAt)),
      this._stringifyTimestamp(archiveTimestamp),
      this._stringifyTimestamp(deletedTimestamp)
    ]);
  }

  _select(query, params = []){
    const stmt = this.db.prepare(query);
    try {
      stmt.bind(params);
      const rows = [];
      while(stmt.step()){
        rows.push(stmt.getAsObject());
      }
      return rows;
    } finally {
      stmt.free();
    }
  }

  _selectOne(query, params = []){
    const rows = this._select(query, params);
    return rows.length ? rows[0] : null;
  }

  _run(query, params = []){
    const stmt = this.db.prepare(query);
    try {
      stmt.bind(params);
      while(stmt.step()){
        // Exhaust the statement so sqlite finalizes it
      }
    } finally {
      stmt.free();
    }
  }

  async _persistDatabase(){
    if(!this.db){
      return;
    }
    const data = this.db.export();
    const buffer = Buffer.from(data);
    await fs.promises.writeFile(this.filename, buffer);
  }

  async _refreshArchive(){
    if(!this.db){
      return;
    }
    let mutated = false;
    mutated = (await this._archiveDailyShows()) || mutated;
    mutated = (await this._purgeExpiredArchives()) || mutated;
    if(mutated){
      await this._persistDatabase();
    }
  }

  async _archiveDailyShows(){
    const rows = this._select('SELECT id, data FROM shows');
    if(!rows.length){
      return false;
    }
    const groups = new Map();
    rows.forEach(row => {
      let show;
      try{
        show = JSON.parse(row.data);
      }catch(err){
        show = null;
      }
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
    let changed = false;
    for(const [key, list] of groups.entries()){
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
        const archiveTime = Date.now();
        list.forEach(item =>{
          const normalized = this._normalizeShow(item.show);
          this._saveArchiveRow(normalized, archiveTime, null);
          this._run('DELETE FROM shows WHERE id = ?', [normalized.id]);
          changed = true;
        });
      }
    }
    return changed;
  }

  async _purgeExpiredArchives(){
    const rows = this._select('SELECT id, data, created_at FROM show_archive');
    if(!rows.length){
      return false;
    }
    const now = Date.now();
    const expiredIds = [];
    rows.forEach(row =>{
      let show;
      try{
        show = JSON.parse(row.data);
      }catch(err){
        show = null;
      }
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
    expiredIds.forEach(id => this._run('DELETE FROM show_archive WHERE id = ?', [id]));
    return true;
  }

  _mapArchiveRow(row){
    if(!row){
      return null;
    }
    let show;
    try{
      show = JSON.parse(row.data);
    }catch(err){
      return null;
    }
    if(!show || typeof show !== 'object'){
      return null;
    }
    const archivedAt = this._getTimestamp(row.archived_at) ?? this._getTimestamp(show.archivedAt);
    const storedCreated = this._getTimestamp(row.created_at);
    const createdAt = this._getTimestamp(show.createdAt) ?? storedCreated;
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

  _getTimestamp(value){
    if(typeof value === 'number' && Number.isFinite(value)){
      return value;
    }
    const numeric = Number(value);
    if(Number.isFinite(numeric)){
      return numeric;
    }
    if(typeof value === 'string'){
      const parsed = Date.parse(value);
      if(Number.isFinite(parsed)){
        return parsed;
      }
    }
    return null;
  }

  _stringifyTimestamp(value){
    return Number.isFinite(value) ? String(value) : null;
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

  async _fileExists(filePath){
    try {
      await fs.promises.access(filePath, fs.constants.F_OK);
      return true;
    } catch (err) {
      return false;
    }
  }
}

module.exports = SqlProvider;
