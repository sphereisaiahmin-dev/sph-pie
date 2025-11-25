const SqlProvider = require('./sqlProvider');
const PostgresProvider = require('./postgresProvider');

let providerInstance = null;
let providerType = 'sqljs';

function resolveProviderSelection(config = {}){
  const explicit = typeof config.storageProvider === 'string' ? config.storageProvider
    : typeof config.provider === 'string' ? config.provider
      : typeof config.storage?.provider === 'string' ? config.storage.provider : 'sqljs';
  const normalized = explicit.toLowerCase();
  if(normalized === 'postgres' || normalized === 'postgresql'){
    const postgresConfig = {
      ...(config.postgres || {}),
      ...(config.storage?.postgres || {})
    };
    return {type: 'postgres', Provider: PostgresProvider, options: postgresConfig};
  }
  const sqlConfig = config.storage?.sql ? {...config.sql, ...config.storage.sql} : config.sql;
  return {type: 'sqljs', Provider: SqlProvider, options: sqlConfig};
}

async function initProvider(config){
  if(providerInstance && typeof providerInstance.dispose === 'function'){
    await providerInstance.dispose();
  }
  const {type, Provider, options} = resolveProviderSelection(config);
  providerInstance = new Provider(options);
  providerType = type;
  await providerInstance.init();
  return providerInstance;
}

function getProvider(){
  if(!providerInstance){
    throw new Error('Storage provider not initialized');
  }
  return providerInstance;
}

function getActiveProviderType(){
  return providerType;
}

module.exports = {
  initProvider,
  getProvider,
  getActiveProviderType
};
