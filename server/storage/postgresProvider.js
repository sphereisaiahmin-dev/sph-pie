const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');

const { dispatchShowEvent } = require('../webhookDispatcher');
const { fetchCalendarFeed, getCalendarCutoffTimestamp } = require('../calendarFeed');

const AUTO_ARCHIVE_WINDOW_MS = 12 * 60 * 60 * 1000;
const ARCHIVE_RETENTION_MONTHS = 2;
const DEFAULT_PILOTS = ['Alex','Nick','John Henery','James','Robert','Nazar'];
const DEFAULT_CREW = ['Alex','Nick','John Henery','James','Robert','Nazar'];
const DEFAULT_MONKEY_LEADS = ['Cleo','Bret','Leslie','Dallas'];

const IDENTIFIER_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;

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
    await this._seedDefaultStaff();
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
    this._assertOperatorUnique(show, entry);
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
    this._assertOperatorUnique(show, entry);
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

  async listCalendarEvents(){
    await this._pruneCalendarEvents();
    const rows = await this._select(`SELECT data FROM ${this._table('calendar_events')} ORDER BY start_ts ASC`);
    return rows.map(row => this._mapCalendarRow(row)).filter(Boolean);
  }

  async syncCalendarEvents(feedUrl){
    const cutoff = getCalendarCutoffTimestamp();
    await this._pruneCalendarEvents(cutoff);
    const events = await fetchCalendarFeed(feedUrl);
    const filtered = (Array.isArray(events) ? events : []).filter(event => Number.isFinite(event.startTs) && event.startTs >= cutoff);
    const seen = new Set();
    for(const event of filtered){
      if(!event || !event.id || seen.has(event.id)){
        continue;
      }
      seen.add(event.id);
      await this._saveCalendarEvent(event);
    }
    return this.listCalendarEvents();
  }

  async getStaff(){
    return {
      crew: await this._listStaffByRole('crew'),
      pilots: await this._listStaffByRole('pilot'),
      monkeyLeads: await this._listMonkeyLeads()
    };
  }

  async replaceStaff(staff = {}){
    const crew = this._normalizeNameList(staff.crew || [], {sort: true});
    const pilots = this._normalizeNameList(staff.pilots || [], {sort: true});
    const monkeyLeads = this._normalizeNameList(staff.monkeyLeads || [], {sort: true});
    await this._withClient(async client =>{
      await this._replaceStaffRole('crew', crew, client);
      await this._replaceStaffRole('pilot', pilots, client);
      await this._replaceMonkeyLeads(monkeyLeads, client);
    });
    return {crew, pilots, monkeyLeads};
  }

  _assertRequiredShowFields(raw = {}){
    const required = [
      {key: 'date', label: 'Date'},
      {key: 'time', label: 'Show start time'},
      {key: 'label', label: 'Show label'},
      {key: 'leadPilot', label: 'Lead pilot'},
      {key: 'monkeyLead', label: 'Crew lead'}
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
      disciplineId: typeof raw.disciplineId === 'string' ? raw.disciplineId.trim().toLowerCase() : '',
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

  _assertOperatorUnique(show, entry){
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
      const err = new Error('Operator already has an entry for this show.');
      err.status = 400;
      throw err;
    }
  }

  async _ensureSchema(){
    const showsTable = this._table('shows');
    const staffTable = this._table('staff');
    const monkeyTable = this._table('monkey_leads');
    const archiveTable = this._table('show_archive');
    const calendarTable = this._table('calendar_events');
    await this._run(`
      CREATE TABLE IF NOT EXISTS ${showsTable} (
        id UUID PRIMARY KEY,
        data JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
      )
    `);
    await this._run(`
      CREATE TABLE IF NOT EXISTS ${staffTable} (
        id UUID PRIMARY KEY,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
      )
    `);
    await this._run(`
      CREATE TABLE IF NOT EXISTS ${monkeyTable} (
        id UUID PRIMARY KEY,
        name TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
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
    await this._run(`CREATE INDEX IF NOT EXISTS ${this._indexName('show_archive_archived_at_idx')} ON ${archiveTable} (archived_at DESC)`);
    await this._run(`CREATE INDEX IF NOT EXISTS ${this._indexName('staff_role_name_idx')} ON ${staffTable} (role, name)`);
    await this._run(`
      CREATE TABLE IF NOT EXISTS ${calendarTable} (
        id TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        start_ts BIGINT,
        end_ts BIGINT,
        created_at TIMESTAMPTZ NOT NULL
      )
    `);
    await this._run(`CREATE INDEX IF NOT EXISTS ${this._indexName('calendar_events_start_idx')} ON ${calendarTable} (start_ts)`);
  }

  async _seedDefaultStaff(){
    let mutated = false;
    if((await this._listStaffByRole('pilot')).length === 0){
      await this._replaceStaffRole('pilot', this._normalizeNameList(DEFAULT_PILOTS, {sort: true}));
      mutated = true;
    }
    if((await this._listStaffByRole('crew')).length === 0){
      await this._replaceStaffRole('crew', this._normalizeNameList(DEFAULT_CREW, {sort: true}));
      mutated = true;
    }
    if((await this._listMonkeyLeads()).length === 0){
      await this._replaceMonkeyLeads(this._normalizeNameList(DEFAULT_MONKEY_LEADS, {sort: true}));
      mutated = true;
    }
    return mutated;
  }

  async _listStaffByRole(role){
    const rows = await this._select(`SELECT name FROM ${this._table('staff')} WHERE role = $1 ORDER BY lower(name), name`, [role]);
    return rows.map(row => row.name);
  }

  async _listMonkeyLeads(){
    const rows = await this._select(`SELECT name FROM ${this._table('monkey_leads')} ORDER BY lower(name), name`);
    return rows.map(row => row.name);
  }

  async _replaceStaffRole(role, names, client = null){
    const executor = client || this.pool;
    await executor.query(`DELETE FROM ${this._table('staff')} WHERE role = $1`, [role]);
    if(!Array.isArray(names) || names.length === 0){
      return;
    }
    const timestamp = new Date();
    for(const name of names){
      await executor.query(`INSERT INTO ${this._table('staff')} (id, name, role, created_at) VALUES ($1, $2, $3, $4)`, [uuidv4(), name, role, timestamp]);
    }
  }

  async _replaceMonkeyLeads(names, client = null){
    const executor = client || this.pool;
    await executor.query(`DELETE FROM ${this._table('monkey_leads')}`);
    if(!Array.isArray(names) || names.length === 0){
      return;
    }
    const timestamp = new Date();
    for(const name of names){
      await executor.query(`INSERT INTO ${this._table('monkey_leads')} (id, name, created_at) VALUES ($1, $2, $3)`, [uuidv4(), name, timestamp]);
    }
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
        list.forEach(item => showsToArchive.push(item.show));
      }
    }
    if(!showsToArchive.length){
      return false;
    }
    const archivedShows = [];
    await this._withClient(async client =>{
      for(const show of showsToArchive){
        const normalized = this._normalizeShow(show);
        const archiveTime = Date.now();
        await this._saveArchiveRow(normalized, archiveTime, null, client);
        await client.query(`DELETE FROM ${showsTable} WHERE id = $1`, [normalized.id]);
        const prepared = this._prepareArchivedShowForDispatch(normalized);
        if(prepared){
          archivedShows.push(prepared);
        }
      }
    });
    if(archivedShows.length){
      await this._dispatchArchivedShows(archivedShows);
    }
    return true;
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
        console.error(`[postgresProvider] Failed to dispatch archive webhook for show ${label}`, err);
      }
    }
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

  _mapCalendarRow(row){
    if(!row){
      return null;
    }
    return this._parseRowData(row.data);
  }

  async _saveCalendarEvent(event){
    const table = this._table('calendar_events');
    const payload = event || {};
    await this._run(
      `INSERT INTO ${table} (id, data, start_ts, end_ts, created_at)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT(id) DO UPDATE SET data = EXCLUDED.data, start_ts = EXCLUDED.start_ts, end_ts = EXCLUDED.end_ts, created_at = EXCLUDED.created_at`,
      [
        event.id,
        payload,
        this._getTimestamp(event.startTs),
        this._getTimestamp(event.endTs),
        new Date(event.startTs || Date.now()).toISOString()
      ]
    );
  }

  async _pruneCalendarEvents(cutoffTs){
    const cutoff = Number.isFinite(cutoffTs) ? cutoffTs : getCalendarCutoffTimestamp();
    const rows = await this._select(`SELECT id FROM ${this._table('calendar_events')} WHERE start_ts < $1`, [cutoff]);
    if(rows.length){
      const ids = rows.map(row => row.id);
      await this._run(`DELETE FROM ${this._table('calendar_events')} WHERE id = ANY($1)`, [ids]);
      return true;
    }
    return false;
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
      poolConfig.database = 'pie';
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
