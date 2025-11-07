const fs = require('fs');
const path = require('path');
const initSqlJs = require('sql.js');
const { v4: uuidv4 } = require('uuid');

const { dispatchShowEvent } = require('../webhookDispatcher');

const AUTO_ARCHIVE_WINDOW_MS = 12 * 60 * 60 * 1000;
const ARCHIVE_RETENTION_MONTHS = 2;
const DEFAULT_PILOTS = ['Alex','Nick','John Henery','James','Robert','Nazar'];
const DEFAULT_CREW = ['Alex','Nick','John Henery','James','Robert','Nazar'];
const DEFAULT_MONKEY_LEADS = ['Cleo','Bret','Leslie','Dallas'];

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

    if(this._seedDefaultStaff()){
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
    return {
      crew: this._listStaffByRole('crew'),
      pilots: this._listStaffByRole('pilot'),
      monkeyLeads: this._listMonkeyLeads()
    };
  }

  async replaceStaff(staff = {}){
    const crew = this._normalizeNameList(staff.crew || [], {sort: true});
    const pilots = this._normalizeNameList(staff.pilots || [], {sort: true});
    const monkeyLeads = this._normalizeNameList(staff.monkeyLeads || [], {sort: true});
    this._replaceStaffRole('crew', crew);
    this._replaceStaffRole('pilot', pilots);
    this._replaceMonkeyLeads(monkeyLeads);
    await this._persistDatabase();
    return {crew, pilots, monkeyLeads};
  }

  _assertRequiredShowFields(raw = {}){
    const required = [
      {key: 'date', label: 'Date'},
      {key: 'time', label: 'Show start time'},
      {key: 'label', label: 'Show label'},
      {key: 'leadPilot', label: 'Lead pilot'},
      {key: 'monkeyLead', label: 'Monkey lead'}
    ];
    required.forEach(field =>{
      const value = typeof raw[field.key] === 'string' ? raw[field.key].trim() : '';
      if(!value){
        const err = new Error(`${field.label} is required`);
        err.status = 400;
        throw err;
      }
    });
  }

  _normalizeShow(raw){
    const createdAt = typeof raw.createdAt === 'number' ? raw.createdAt : Number(raw.createdAt);
    const updatedAt = typeof raw.updatedAt === 'number' ? raw.updatedAt : Number(raw.updatedAt);
    return {
      id: raw.id,
      date: typeof raw.date === 'string' ? raw.date.trim() : '',
      time: typeof raw.time === 'string' ? raw.time.trim() : '',
      label: typeof raw.label === 'string' ? raw.label.trim() : '',
      crew: Array.isArray(raw.crew) ? this._normalizeNameList(raw.crew, {sort: true}) : [],
      leadPilot: typeof raw.leadPilot === 'string' ? raw.leadPilot.trim() : '',
      monkeyLead: typeof raw.monkeyLead === 'string' ? raw.monkeyLead.trim() : '',
      notes: typeof raw.notes === 'string' ? raw.notes.trim() : '',
      entries: Array.isArray(raw.entries) ? raw.entries.map(e => this._normalizeEntry(e)) : [],
      createdAt: Number.isFinite(createdAt) ? createdAt : Date.now(),
      updatedAt: Number.isFinite(updatedAt) ? updatedAt : Date.now()
    };
  }

  _normalizeEntry(raw){
    const ts = typeof raw.ts === 'number' ? raw.ts : Number(raw.ts);
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
      operator: typeof raw.operator === 'string' ? raw.operator.trim() : '',
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
    const normalized = (entry.operator || '').trim().toLowerCase();
    if(!normalized){
      return;
    }
    const hasDuplicate = (show.entries || []).some(existing => {
      if(!existing){
        return false;
      }
      if(existing.id === entry.id){
        return false;
      }
      const existingPilot = (existing.operator || '').trim().toLowerCase();
      return existingPilot === normalized;
    });
    if(hasDuplicate){
      const err = new Error('Pilot already has an entry for this show.');
      err.status = 400;
      throw err;
    }
  }

  _ensureSchema(){
    let mutated = false;
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

  _seedDefaultStaff(){
    let mutated = false;
    if(this._listStaffByRole('pilot').length === 0){
      this._replaceStaffRole('pilot', this._normalizeNameList(DEFAULT_PILOTS, {sort: true}));
      mutated = true;
    }
    if(this._listStaffByRole('crew').length === 0){
      this._replaceStaffRole('crew', this._normalizeNameList(DEFAULT_CREW, {sort: true}));
      mutated = true;
    }
    if(this._listMonkeyLeads().length === 0){
      this._replaceMonkeyLeads(this._normalizeNameList(DEFAULT_MONKEY_LEADS, {sort: true}));
      mutated = true;
    }
    return mutated;
  }

  _listStaffByRole(role){
    const rows = this._select('SELECT name FROM staff WHERE role = ? ORDER BY name COLLATE NOCASE', [role]);
    return rows.map(row => row.name);
  }

  _listMonkeyLeads(){
    const rows = this._select('SELECT name FROM monkey_leads ORDER BY name COLLATE NOCASE');
    return rows.map(row => row.name);
  }

  _replaceStaffRole(role, names){
    this._run('DELETE FROM staff WHERE role = ?', [role]);
    if(!Array.isArray(names) || names.length === 0){
      return;
    }
    const timestamp = new Date().toISOString();
    names.forEach(name =>{
      this._run('INSERT INTO staff (id, name, role, created_at) VALUES (?, ?, ?, ?)', [uuidv4(), name, role, timestamp]);
    });
  }

  _replaceMonkeyLeads(names){
    this._run('DELETE FROM monkey_leads');
    if(!Array.isArray(names) || names.length === 0){
      return;
    }
    const timestamp = new Date().toISOString();
    names.forEach(name =>{
      this._run('INSERT INTO monkey_leads (id, name, created_at) VALUES (?, ?, ?)', [uuidv4(), name, timestamp]);
    });
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
    const archivedShows = [];
    let changed = false;
    for(const list of groups.values()){
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
      if(now - earliest >= AUTO_ARCHIVE_WINDOW_MS){
        const archiveTime = Date.now();
        for(const item of list){
          const normalized = this._normalizeShow(item.show);
          this._saveArchiveRow(normalized, archiveTime, null);
          this._run('DELETE FROM shows WHERE id = ?', [normalized.id]);
          const prepared = this._prepareArchivedShowForDispatch(normalized);
          if(prepared){
            archivedShows.push(prepared);
          }
          changed = true;
        }
      }
    }
    if(archivedShows.length){
      await this._dispatchArchivedShows(archivedShows);
    }
    return changed;
  }

  _prepareArchivedShowForDispatch(show){
    if(!show || typeof show !== 'object'){
      return null;
    }
    const entries = Array.isArray(show.entries)
      ? show.entries.map(entry => ({
        ...entry,
        actions: Array.isArray(entry?.actions) ? [...entry.actions] : []
      }))
      : [];
    return {
      ...show,
      entries
    };
  }

  async _dispatchArchivedShows(shows){
    if(!Array.isArray(shows) || !shows.length){
      return;
    }
    const triggeredAt = new Date().toISOString();
    const totalShows = shows.length;
    for(let index = 0; index < totalShows; index += 1){
      const show = shows[index];
      if(!show){
        continue;
      }
      const meta = {
        automation: {
          source: 'daily-archive',
          triggeredAt,
          totalShows,
          showIndex: index,
          showId: show.id || null
        }
      };
      try{
        await dispatchShowEvent('show.archived', show, meta);
      }catch(err){
        const label = show?.id || '(unknown)';
        console.error(`[sqlProvider] Failed to dispatch archive webhook for show ${label}`, err);
      }
    }
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
