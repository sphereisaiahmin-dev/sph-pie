const PostgresProvider = require('../server/storage/postgresProvider');

function getDatabaseNameFromConfig(config){
  if(config.database){
    return config.database;
  }
  if(config.connectionString){
    try{
      const url = new URL(config.connectionString);
      const pathname = url.pathname || '';
      const dbName = decodeURIComponent(pathname.replace(/^\//, ''));
      return dbName || null;
    }catch(err){
      console.error('Failed to parse connection string in simulation', err);
    }
  }
  return null;
}

class StubPool{
  constructor(config, state){
    this.config = config;
    this.state = state;
    this.database = getDatabaseNameFromConfig(config) || state.targetDatabase;
    if(this.database === state.targetDatabase){
      this.role = state.databaseCreated ? 'ready' : 'missing';
    }else{
      this.role = 'admin';
    }
  }

  async query(sql, params = []){
    const trimmed = typeof sql === 'string' ? sql.trim() : '';
    this.state.logs.push({database: this.database, role: this.role, sql: trimmed, params});
    if(this.role === 'missing' && /^SELECT\s+1\b/i.test(trimmed)){
      const err = new Error(`database "${this.database}" does not exist`);
      err.code = '3D000';
      throw err;
    }
    if(/^CREATE\s+SCHEMA\b/i.test(trimmed)){
      const schemaMatch = trimmed.match(/CREATE\s+SCHEMA(?:\s+IF\s+NOT\s+EXISTS)?\s+"([^"]+)"/i);
      if(schemaMatch){
        this.state.schemas.add(schemaMatch[1]);
      }
      return {rows: [], rowCount: 0};
    }
    if(this.role === 'admin' && /^CREATE\s+DATABASE\b/i.test(trimmed)){
      this.state.databaseCreated = true;
      return {rows: [], rowCount: 0};
    }
    if(/^SELECT\s+name\s+FROM/i.test(trimmed) && /"staff"/i.test(trimmed)){
      const role = params?.[0];
      const rows = this.state.staff
        .filter(item => !role || item.role === role)
        .map(item => ({name: item.name}));
      return {rows};
    }
    if(/^SELECT\s+name\s+FROM/i.test(trimmed) && /"monkey_leads"/i.test(trimmed)){
      const rows = this.state.monkeyLeads.map(name => ({name}));
      return {rows};
    }
    if(/^DELETE\s+FROM/i.test(trimmed) && /"staff"/i.test(trimmed)){
      const role = params?.[0];
      if(role){
        this.state.staff = this.state.staff.filter(item => item.role !== role);
      }else{
        this.state.staff = [];
      }
      return {rows: [], rowCount: 0};
    }
    if(/^DELETE\s+FROM/i.test(trimmed) && /"monkey_leads"/i.test(trimmed)){
      this.state.monkeyLeads = [];
      return {rows: [], rowCount: 0};
    }
    if(/^INSERT\s+INTO/i.test(trimmed) && /"staff"/i.test(trimmed)){
      const [, name, role] = params;
      this.state.staff.push({name, role});
      return {rows: [], rowCount: 1};
    }
    if(/^INSERT\s+INTO/i.test(trimmed) && /"monkey_leads"/i.test(trimmed)){
      const [, name] = params;
      this.state.monkeyLeads.push(name);
      return {rows: [], rowCount: 1};
    }
    if(/^SELECT\s+data\s+FROM/i.test(trimmed) && /"shows"/i.test(trimmed) && !/WHERE/i.test(trimmed)){
      const rows = Array.from(this.state.shows.values())
        .map(record => ({data: record.data}))
        .sort((a, b) => {
          const aUpdated = JSON.parse(a.data).updatedAt ?? 0;
          const bUpdated = JSON.parse(b.data).updatedAt ?? 0;
          return bUpdated - aUpdated;
        });
      return {rows};
    }
    if(/^SELECT\s+id\s*,\s*data\s+FROM/i.test(trimmed) && /"shows"/i.test(trimmed)){
      const rows = Array.from(this.state.shows.values()).map(record => ({id: record.id, data: record.data}));
      return {rows};
    }
    if(/^SELECT\s+data\s+FROM/i.test(trimmed) && /"shows"/i.test(trimmed) && /WHERE/i.test(trimmed)){
      const id = params?.[0];
      const record = id ? this.state.shows.get(id) : null;
      return {rows: record ? [{data: record.data}] : []};
    }
    if(/^SELECT\s+data\s+FROM/i.test(trimmed) && /"show_archive"/i.test(trimmed)){
      const rows = Array.from(this.state.archives.values()).map(record => ({data: record.data}));
      return {rows};
    }
    if(/^SELECT\s+id\s*,\s*data\s*,\s*created_at\s+FROM/i.test(trimmed) && /"show_archive"/i.test(trimmed)){
      const rows = Array.from(this.state.archives.values()).map(record => ({id: record.id, data: record.data, created_at: record.created_at}));
      return {rows};
    }
    if(/^INSERT\s+INTO/i.test(trimmed) && /"shows"/i.test(trimmed)){
      const [id, data] = params;
      this.state.shows.set(id, {id, data});
      return {rows: [], rowCount: 1};
    }
    if(/^INSERT\s+INTO/i.test(trimmed) && /"show_archive"/i.test(trimmed)){
      const [id, data, , createdAt] = params;
      this.state.archives.set(id, {id, data, created_at: createdAt});
      return {rows: [], rowCount: 1};
    }
    if(/^DELETE\s+FROM/i.test(trimmed) && /"shows"/i.test(trimmed)){
      const id = params?.[0];
      if(id){
        this.state.shows.delete(id);
      }
      return {rows: [], rowCount: 1};
    }
    if(/^DELETE\s+FROM/i.test(trimmed) && /"show_archive"/i.test(trimmed)){
      const ids = params?.[0];
      if(Array.isArray(ids)){
        ids.forEach(id => this.state.archives.delete(id));
      }
      return {rows: [], rowCount: Array.isArray(ids) ? ids.length : 0};
    }
    if(/^SELECT\s+1\b/i.test(trimmed)){
      return {rows: [{'?column?': 1}]};
    }
    return {rows: [], rowCount: 0};
  }

  async connect(){
    const pool = this;
    return {
      async query(sql, params){
        if(/^BEGIN\b/i.test((sql || '').trim())){
          pool.state.logs.push({database: pool.database, role: pool.role, sql: 'BEGIN', params});
          return {rows: [], rowCount: 0};
        }
        if(/^COMMIT\b/i.test((sql || '').trim())){
          pool.state.logs.push({database: pool.database, role: pool.role, sql: 'COMMIT', params});
          return {rows: [], rowCount: 0};
        }
        if(/^ROLLBACK\b/i.test((sql || '').trim())){
          pool.state.logs.push({database: pool.database, role: pool.role, sql: 'ROLLBACK', params});
          return {rows: [], rowCount: 0};
        }
        return pool.query(sql, params);
      },
      release(){
        pool.state.logs.push({database: pool.database, role: pool.role, sql: '<release>'});
      }
    };
  }

  async end(){
    this.state.logs.push({database: this.database, role: this.role, sql: '<end>'});
  }
}

async function runScenario({label, databaseCreated, schema}){
  const state = {
    databaseCreated,
    targetDatabase: 'pie',
    logs: [],
    staff: [],
    monkeyLeads: [],
    shows: new Map(),
    archives: new Map(),
    schemas: new Set()
  };

  const provider = new PostgresProvider({
    database: state.targetDatabase,
    user: 'postgres',
    password: 'postgres',
    schema
  });
  provider._createPool = config => new StubPool(config, state);

  await provider.init();
  const staff = await provider.getStaff();
  const createdShow = await provider.createShow({
    date: '2024-12-01',
    time: '10:00',
    label: `${label} Flight`,
    crew: staff.crew.slice(0, 2),
    leadPilot: staff.pilots[0] || 'Alex',
    monkeyLead: staff.monkeyLeads[0] || 'Cleo',
    notes: `${label} simulation`
  });
  await provider.listShows();
  await provider.deleteShow(createdShow.id);
  await provider.dispose();

  return {
    label,
    databaseCreated: state.databaseCreated,
    schemas: Array.from(state.schemas),
    staffCounts: {
      crew: staff.crew.length,
      pilots: staff.pilots.length,
      monkeyLeads: staff.monkeyLeads.length
    },
    showArchived: state.archives.has(createdShow.id),
    queryLog: state.logs
  };
}

async function main(){
  const scenarios = [
    {label: 'auto-create', databaseCreated: false, schema: 'ops'},
    {label: 'existing-db', databaseCreated: true, schema: 'ops'}
  ];
  for(const scenario of scenarios){
    const result = await runScenario(scenario);
    console.log(`Scenario: ${result.label}`);
    console.log(`  Database created during init: ${scenario.databaseCreated ? 'already present' : 'created by provider'}`);
    console.log(`  Schema bootstrap: ${result.schemas.join(', ') || 'none'}`);
    console.log(`  Staff counts -> crew: ${result.staffCounts.crew}, pilots: ${result.staffCounts.pilots}, monkey leads: ${result.staffCounts.monkeyLeads}`);
    console.log(`  Show archived after deletion: ${result.showArchived}`);
    const creationQueries = result.queryLog.filter(entry => /^CREATE DATABASE/i.test(entry.sql || ''));
    console.log(`  CREATE DATABASE executed: ${creationQueries.length > 0}`);
    console.log('  Total queries executed:', result.queryLog.length);
    console.log('');
  }
}

main().catch(err => {
  console.error('Simulation failed', err);
  process.exitCode = 1;
});
