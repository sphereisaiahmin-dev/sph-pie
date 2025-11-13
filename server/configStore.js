const fs = require('fs');
const path = require('path');

const CONFIG_FILE = path.join(__dirname, '..', 'config', 'app-config.json');
const DEFAULT_HOST = process.env.HOST || process.env.LISTEN_HOST || '10.241.211.120';
const DEFAULT_PORT = Number.isFinite(parseInt(process.env.PORT, 10)) ? parseInt(process.env.PORT, 10) : 3000;
const DEFAULT_POSTGRES_URL = process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5432/pie';
const DEFAULT_STORAGE_PROVIDER = (process.env.STORAGE_PROVIDER || process.env.DB_PROVIDER || 'sqljs').toLowerCase();
const DEFAULT_CONFIG = {
  host: DEFAULT_HOST,
  port: DEFAULT_PORT,
  unitLabel: 'Drone',
  storageProvider: DEFAULT_STORAGE_PROVIDER === 'postgres' || DEFAULT_STORAGE_PROVIDER === 'postgresql' ? 'postgres' : 'sqljs',
  sql: {
    filename: path.join(process.cwd(), 'data', 'pie.sqlite')
  },
  postgres: {
    connectionString: DEFAULT_POSTGRES_URL,
    ssl: false,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
    schema: process.env.DATABASE_SCHEMA || null
  },
  webhook: {
    enabled: false,
    url: '',
    method: 'POST',
    secret: '',
    headers: []
  }
};

function ensureConfigFile(){
  const dir = path.dirname(CONFIG_FILE);
  if(!fs.existsSync(dir)){
    fs.mkdirSync(dir, {recursive: true});
  }
  if(!fs.existsSync(CONFIG_FILE)){
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(DEFAULT_CONFIG, null, 2));
  }
}

function loadConfig(){
  ensureConfigFile();
  try{
    const raw = fs.readFileSync(CONFIG_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    const {provider: legacyProvider, storage: legacyStorage, ...cleanParsed} = parsed;
    const storageProvider = (cleanParsed.storageProvider || legacyStorage?.provider || legacyProvider || DEFAULT_CONFIG.storageProvider || 'sqljs');
    const normalizedProvider = typeof storageProvider === 'string' ? storageProvider.toLowerCase() : 'sqljs';
    const mergedPostgres = {
      ...DEFAULT_CONFIG.postgres,
      ...(legacyStorage?.postgres || {}),
      ...(cleanParsed.postgres || {})
    };
    return {
      ...DEFAULT_CONFIG,
      ...cleanParsed,
      storageProvider: normalizedProvider === 'postgresql' ? 'postgres' : normalizedProvider,
      sql: {...DEFAULT_CONFIG.sql, ...(cleanParsed.sql || {})},
      postgres: mergedPostgres,
      webhook: {...DEFAULT_CONFIG.webhook, ...(cleanParsed.webhook || {})},
      host: cleanParsed.host || DEFAULT_CONFIG.host,
      port: Number.isFinite(parseInt(cleanParsed.port, 10)) ? parseInt(cleanParsed.port, 10) : DEFAULT_CONFIG.port
    };
  }catch(err){
    console.error('Failed to load config. Falling back to defaults.', err);
    return {...DEFAULT_CONFIG};
  }
}

function saveConfig(config){
  ensureConfigFile();
  const merged = {
    ...DEFAULT_CONFIG,
    ...config,
    sql: {...DEFAULT_CONFIG.sql, ...(config.sql || {})},
    postgres: {...DEFAULT_CONFIG.postgres, ...(config.postgres || {})},
    webhook: {...DEFAULT_CONFIG.webhook, ...(config.webhook || {})}
  };
  delete merged.provider;
  if(merged.storage && typeof merged.storage === 'object'){
    if(typeof merged.storage.provider === 'string'){
      merged.storageProvider = merged.storage.provider;
    }
    if(merged.storage.sql){
      merged.sql = {...merged.sql, ...merged.storage.sql};
    }
    if(merged.storage.postgres){
      merged.postgres = {...merged.postgres, ...merged.storage.postgres};
    }
    delete merged.storage;
  }
  const provider = typeof merged.storageProvider === 'string' ? merged.storageProvider.toLowerCase() : DEFAULT_CONFIG.storageProvider;
  merged.storageProvider = provider === 'postgresql' ? 'postgres' : provider;
  merged.host = merged.host || DEFAULT_CONFIG.host;
  merged.port = Number.isFinite(parseInt(merged.port, 10)) ? parseInt(merged.port, 10) : DEFAULT_CONFIG.port;
  fs.writeFileSync(CONFIG_FILE, JSON.stringify(merged, null, 2));
  return merged;
}

module.exports = {
  loadConfig,
  saveConfig,
  defaultConfig: DEFAULT_CONFIG
};
