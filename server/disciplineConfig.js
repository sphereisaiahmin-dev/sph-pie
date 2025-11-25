const fs = require('fs');
const path = require('path');

const CONFIG_PATH = path.join(__dirname, '..', 'config', 'disciplines.json');

function loadConfig(){
  const raw = fs.readFileSync(CONFIG_PATH, 'utf8');
  const parsed = JSON.parse(raw);
  const roles = Array.isArray(parsed.roles) ? parsed.roles.map(normalizeKey).filter(Boolean) : [];
  const disciplines = Array.isArray(parsed.disciplines) ? parsed.disciplines.map(normalizeDiscipline).filter(Boolean) : [];
  return {roles, disciplines};
}

function normalizeKey(value){
  return typeof value === 'string' ? value.trim().toLowerCase() : '';
}

function normalizeDiscipline(raw){
  if(!raw || typeof raw !== 'object'){
    return null;
  }
  const id = typeof raw.id === 'string' ? raw.id.trim().toLowerCase() : '';
  const name = typeof raw.name === 'string' ? raw.name.trim() : '';
  if(!id || !name){
    return null;
  }
  return {
    id,
    name,
    default: Boolean(raw.default),
    forms: Boolean(raw.forms)
  };
}

const {roles: ROLE_LEVELS, disciplines: DISCIPLINES} = loadConfig();

const DEFAULT_DISCIPLINE = DISCIPLINES.find(discipline => discipline.default) || DISCIPLINES.find(Boolean) || null;

function getRoleKey(disciplineId, level){
  const discipline = findDiscipline(disciplineId);
  const normalizedLevel = normalizeKey(level);
  if(!discipline || !ROLE_LEVELS.includes(normalizedLevel)){
    return null;
  }
  return `${discipline.id}.${normalizedLevel}`;
}

function listRoleKeys(){
  const keys = [];
  for(const discipline of DISCIPLINES){
    for(const level of ROLE_LEVELS){
      keys.push(`${discipline.id}.${level}`);
    }
  }
  return keys;
}

const ROLE_ALIASES = new Map([
  ['lead', () => getRoleKey(DEFAULT_DISCIPLINE?.id, 'lead')],
  ['operator', () => getRoleKey(DEFAULT_DISCIPLINE?.id, 'operator')],
  ['stagecrew', () => getRoleKey(DEFAULT_DISCIPLINE?.id, 'crew')],
  ['crew', () => getRoleKey(DEFAULT_DISCIPLINE?.id, 'crew')]
]);

function normalizeRole(role){
  if(typeof role !== 'string'){
    return null;
  }
  const trimmed = role.trim();
  if(!trimmed){
    return null;
  }
  const lower = trimmed.toLowerCase();
  if(lower === 'admin'){
    return 'admin';
  }
  if(ROLE_ALIASES.has(lower)){
    return ROLE_ALIASES.get(lower)() || null;
  }
  if(!trimmed.includes('.')){
    return null;
  }
  const [disciplineId, level] = trimmed.split('.');
  const key = getRoleKey(disciplineId, level);
  return key;
}

function findDiscipline(id){
  if(typeof id !== 'string'){
    return null;
  }
  const normalized = id.trim().toLowerCase();
  if(!normalized){
    return null;
  }
  return DISCIPLINES.find(discipline => discipline.id === normalized) || null;
}

function getDisplayName(roleKey){
  if(roleKey === 'admin'){
    return 'Admin';
  }
  const parsed = parseRoleKey(roleKey);
  if(!parsed){
    return roleKey;
  }
  const discipline = findDiscipline(parsed.disciplineId);
  const levelName = parsed.level.charAt(0).toUpperCase() + parsed.level.slice(1);
  return discipline ? `${discipline.name} ${levelName}` : `${parsed.disciplineId} ${levelName}`;
}

function parseRoleKey(roleKey){
  if(typeof roleKey !== 'string'){
    return null;
  }
  const trimmed = roleKey.trim().toLowerCase();
  if(!trimmed){
    return null;
  }
  if(trimmed === 'admin'){
    return {disciplineId: null, level: 'admin'};
  }
  const parts = trimmed.split('.');
  if(parts.length !== 2){
    return null;
  }
  const [disciplineId, level] = parts;
  if(!ROLE_LEVELS.includes(level)){
    return null;
  }
  const discipline = findDiscipline(disciplineId);
  if(!discipline){
    return null;
  }
  return {disciplineId: discipline.id, level};
}

function roleMatchesLevel(roleKey, level){
  const parsed = parseRoleKey(roleKey);
  return Boolean(parsed && parsed.level === level);
}

function roleMatchesDiscipline(roleKey, disciplineId){
  const parsed = parseRoleKey(roleKey);
  return Boolean(parsed && parsed.disciplineId === disciplineId);
}

module.exports = {
  ROLE_LEVELS,
  DISCIPLINES,
  DEFAULT_DISCIPLINE,
  getRoleKey,
  listRoleKeys,
  normalizeRole,
  parseRoleKey,
  roleMatchesLevel,
  roleMatchesDiscipline,
  getDisplayName,
  findDiscipline
};
