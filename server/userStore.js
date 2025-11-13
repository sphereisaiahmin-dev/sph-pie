const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');

const {
  DISCIPLINES,
  ROLE_LEVELS,
  DEFAULT_DISCIPLINE,
  listRoleKeys,
  normalizeRole,
  getRoleKey,
  getDisplayName,
  roleMatchesLevel,
  roleMatchesDiscipline
} = require('./disciplineConfig');

const USERS_FILE = path.join(process.cwd(), 'data', 'users.json');
const SUPPORTED_ROLES = ['admin', ...listRoleKeys()];
const DEFAULT_TEMP_PASSWORD = 'adminsphere1';
const SCRYPT_PARAMS = {N: 16384, r: 8, p: 1, keylen: 64};

const DEFAULT_DISCIPLINE_ID = DEFAULT_DISCIPLINE?.id || 'drones';
const DEFAULT_LEAD_ROLE = getRoleKey(DEFAULT_DISCIPLINE_ID, 'lead') || `${DEFAULT_DISCIPLINE_ID}.lead`;
const DEFAULT_OPERATOR_ROLE = getRoleKey(DEFAULT_DISCIPLINE_ID, 'operator') || `${DEFAULT_DISCIPLINE_ID}.operator`;
const DEFAULT_CREW_ROLE = getRoleKey(DEFAULT_DISCIPLINE_ID, 'crew') || `${DEFAULT_DISCIPLINE_ID}.crew`;

const DEFAULT_USER_SEED = [
  {name: 'Alex Brodnik', email: 'alexander.brodnik@thesphere.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'John Henry', email: 'john.henry@thesphere.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'James Johnson', email: 'james.johnson@thesphere.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'Nazar Vasylyk', email: 'nazar.vasylyk@thesphere.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'Nicholas Aquino', email: 'nicholas.aquino@thesphere.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'Robert Ontell', email: 'robert.ontell@thesphere.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'Ben Ellingson', email: 'benellingson@mac.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'Alaz Szabo', email: 'alanszabojr@me.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'John Graham', email: 'john@vigilantaerialsystems.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'Daniel Perrier', email: 'dnlperrier08@gmail.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'Jevin Williams', email: 'jevinwilliams@gmail.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'Gregory Ryan', email: 'gregorywryan@gmail.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'Jordan Schroeder', email: 'jtsschroeder7@gmail.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'Danny Szabo', email: 'dannyszabo@gmail.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'Ben Storick', email: 'bestorick@gmail.com', roles: [DEFAULT_LEAD_ROLE, DEFAULT_OPERATOR_ROLE]},
  {name: 'Isaiah Mincher', email: 'isaiah.mincher@thesphere.com', roles: ['admin']},
  {name: 'Zach Harvest', email: 'zach.harvest@thesphere.com', roles: ['admin']},
  {name: 'Bret Tuttle', email: 'bret.tuttle@thesphere.com', roles: [DEFAULT_CREW_ROLE]},
  {name: 'Cleo Kelley', email: 'cleo.kelley@thesphere.com', roles: [DEFAULT_CREW_ROLE]},
  {name: 'Dallas Howerton', email: 'dallas.howerton@thesphere.com', roles: [DEFAULT_CREW_ROLE]},
  {name: 'Daisy Serratos Gomez', email: 'daisy.serratosgomez@thesphere.com', roles: [DEFAULT_CREW_ROLE]}
];

let userRecords = [];
let initialized = false;

async function initUserStore(){
  if(initialized){
    return;
  }
  await fs.promises.mkdir(path.dirname(USERS_FILE), {recursive: true});
  if(await fileExists(USERS_FILE)){
    await loadUsersFromFile();
  }else{
    userRecords = seedDefaultUsers();
    await persistUsers();
  }
  if(userRecords.length === 0){
    userRecords = seedDefaultUsers();
    await persistUsers();
  }
  initialized = true;
}

async function loadUsersFromFile(){
  try{
    const raw = await fs.promises.readFile(USERS_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    if(Array.isArray(parsed.users)){
      userRecords = parsed.users.map(normalizeStoredUser).filter(Boolean);
    }else{
      userRecords = [];
    }
  }catch(err){
    console.warn('Failed to parse user store. Recreating seed data.', err);
    userRecords = seedDefaultUsers();
    await persistUsers();
  }
}

function seedDefaultUsers(){
  const now = new Date().toISOString();
  return DEFAULT_USER_SEED.map(seed =>{
    const password = hashPassword(DEFAULT_TEMP_PASSWORD);
    return {
      id: uuidv4(),
      name: seed.name,
      email: normalizeEmail(seed.email),
      roles: normalizeRoles(seed.roles),
      password,
      passwordResetRequired: true,
      createdAt: now,
      updatedAt: now
    };
  });
}

async function persistUsers(){
  const payload = {users: userRecords};
  await fs.promises.writeFile(USERS_FILE, JSON.stringify(payload, null, 2));
}

function normalizeStoredUser(raw){
  if(!raw || typeof raw !== 'object'){
    return null;
  }
  const normalizedRoles = normalizeRoles(raw.roles);
  const password = typeof raw.password === 'object'
    ? {
        hash: String(raw.password.hash || ''),
        salt: raw.password.salt || raw.passwordSalt || '',
        algorithm: raw.password.algorithm || 'scrypt',
        params: raw.password.params || {N: raw.password.N || SCRYPT_PARAMS.N, r: raw.password.r || SCRYPT_PARAMS.r, p: raw.password.p || SCRYPT_PARAMS.p, keylen: raw.password.keylen || SCRYPT_PARAMS.keylen}
      }
    : hashPassword(DEFAULT_TEMP_PASSWORD);
  return {
    id: raw.id || uuidv4(),
    name: typeof raw.name === 'string' ? raw.name.trim() : 'User',
    email: normalizeEmail(raw.email),
    roles: normalizedRoles,
    password,
    passwordResetRequired: Boolean(raw.passwordResetRequired),
    createdAt: raw.createdAt || new Date().toISOString(),
    updatedAt: raw.updatedAt || raw.createdAt || new Date().toISOString()
  };
}

function hashPassword(password, salt){
  if(typeof salt !== 'string' || !salt){
    salt = crypto.randomBytes(16).toString('hex');
  }
  const hash = crypto.scryptSync(password, salt, SCRYPT_PARAMS.keylen, {N: SCRYPT_PARAMS.N, r: SCRYPT_PARAMS.r, p: SCRYPT_PARAMS.p}).toString('hex');
  return {
    hash,
    salt,
    algorithm: 'scrypt',
    params: {...SCRYPT_PARAMS}
  };
}

function verifyPassword(record, password){
  if(!record || !password || typeof password !== 'string'){
    return false;
  }
  const stored = record.password;
  if(!stored || !stored.hash || !stored.salt){
    return false;
  }
  try{
    const hash = crypto.scryptSync(password, stored.salt, stored.params?.keylen || SCRYPT_PARAMS.keylen, {
      N: stored.params?.N || SCRYPT_PARAMS.N,
      r: stored.params?.r || SCRYPT_PARAMS.r,
      p: stored.params?.p || SCRYPT_PARAMS.p
    }).toString('hex');
    return crypto.timingSafeEqual(Buffer.from(hash, 'hex'), Buffer.from(stored.hash, 'hex'));
  }catch(err){
    return false;
  }
}

function normalizeRoles(input){
  const set = new Set();
  const roles = Array.isArray(input)
    ? input
    : (typeof input === 'string' ? input.split(',') : []);
  roles.forEach(role =>{
    const value = normalizeRole(typeof role === 'string' ? role : '');
    if(value && SUPPORTED_ROLES.includes(value)){
      set.add(value);
    }
  });
  return Array.from(set);
}

function normalizeEmail(email){
  return typeof email === 'string' ? email.trim().toLowerCase() : '';
}

function sanitizeName(name){
  return typeof name === 'string' && name.trim() ? name.trim() : 'Unnamed user';
}

function sanitizeUser(record){
  const roles = Array.isArray(record.roles) ? record.roles.slice() : [];
  const sortedRoles = roles.sort((a, b)=> getDisplayName(a).localeCompare(getDisplayName(b), undefined, {sensitivity: 'base'}));
  return {
    id: record.id,
    name: record.name,
    email: record.email,
    roles: sortedRoles,
    needsPasswordReset: Boolean(record.passwordResetRequired),
    createdAt: record.createdAt,
    updatedAt: record.updatedAt
  };
}

function listUsers(){
  return userRecords
    .slice()
    .sort((a, b)=> a.name.localeCompare(b.name, undefined, {sensitivity: 'base'}))
    .map(sanitizeUser);
}

function findUserByEmail(email){
  const normalized = normalizeEmail(email);
  return userRecords.find(user => user.email === normalized) || null;
}

function findUserById(id){
  return userRecords.find(user => user.id === id) || null;
}

function ensureUniqueEmail(candidateEmail, ignoreUserId){
  const normalized = normalizeEmail(candidateEmail);
  if(!normalized){
    const err = new Error('Email is required');
    err.status = 400;
    throw err;
  }
  const existing = findUserByEmail(normalized);
  if(existing && existing.id !== ignoreUserId){
    const err = new Error('Email already exists');
    err.status = 409;
    throw err;
  }
  return normalized;
}

async function createUser(input){
  const name = sanitizeName(input?.name);
  const email = ensureUniqueEmail(input?.email);
  const roles = normalizeRoles(input?.roles);
  if(!roles.length){
    const err = new Error('Select at least one role');
    err.status = 400;
    throw err;
  }
  const now = new Date().toISOString();
  const password = hashPassword(DEFAULT_TEMP_PASSWORD);
  const record = {
    id: uuidv4(),
    name,
    email,
    roles,
    password,
    passwordResetRequired: true,
    createdAt: now,
    updatedAt: now
  };
  userRecords.push(record);
  await persistUsers();
  return sanitizeUser(record);
}

async function updateUser(id, updates){
  const record = findUserById(id);
  if(!record){
    const err = new Error('User not found');
    err.status = 404;
    throw err;
  }
  const nextEmail = updates?.email ? ensureUniqueEmail(updates.email, record.id) : record.email;
  const nextName = typeof updates?.name === 'string' && updates.name.trim() ? updates.name.trim() : record.name;
  const nextRoles = Array.isArray(updates?.roles)
    ? normalizeRoles(updates.roles)
    : record.roles;
  if(nextRoles.length === 0){
    const err = new Error('Select at least one role');
    err.status = 400;
    throw err;
  }
  record.name = nextName;
  record.email = nextEmail;
  record.roles = nextRoles;
  record.updatedAt = new Date().toISOString();
  await persistUsers();
  return sanitizeUser(record);
}

async function setUserPassword(id, password, options = {}){
  const record = findUserById(id);
  if(!record){
    const err = new Error('User not found');
    err.status = 404;
    throw err;
  }
  validatePasswordStrength(password);
  record.password = hashPassword(password);
  record.passwordResetRequired = Boolean(options.requireReset);
  record.updatedAt = new Date().toISOString();
  await persistUsers();
  return sanitizeUser(record);
}

async function resetUserPassword(id){
  const record = findUserById(id);
  if(!record){
    const err = new Error('User not found');
    err.status = 404;
    throw err;
  }
  record.password = hashPassword(DEFAULT_TEMP_PASSWORD);
  record.passwordResetRequired = true;
  record.updatedAt = new Date().toISOString();
  await persistUsers();
  return sanitizeUser(record);
}

function validatePasswordStrength(password){
  if(typeof password !== 'string' || password.length < 12){
    const err = new Error('Password must be at least 12 characters long');
    err.status = 400;
    throw err;
  }
  if(!/[a-z]/.test(password) || !/[A-Z]/.test(password) || !/[0-9]/.test(password) || !/[^A-Za-z0-9]/.test(password)){
    const err = new Error('Password must include upper, lower, number and special characters');
    err.status = 400;
    throw err;
  }
}

function getRoleDirectory(){
  const directory = {};
  for(const discipline of DISCIPLINES){
    const levels = {};
    for(const level of ROLE_LEVELS){
      const roleKey = getRoleKey(discipline.id, level);
      levels[level] = userRecords
        .filter(user => Array.isArray(user.roles) && user.roles.includes(roleKey))
        .map(user => user.name)
        .sort((a, b)=> a.localeCompare(b, undefined, {sensitivity: 'base'}));
    }
    directory[discipline.id] = levels;
  }
  return directory;
}

async function deleteUser(id){
  const index = userRecords.findIndex(user => user.id === id);
  if(index === -1){
    const err = new Error('User not found');
    err.status = 404;
    throw err;
  }
  const [removed] = userRecords.splice(index, 1);
  await persistUsers();
  return sanitizeUser(removed);
}

async function fileExists(file){
  try{
    await fs.promises.access(file, fs.constants.F_OK);
    return true;
  }catch(err){
    return false;
  }
}

module.exports = {
  initUserStore,
  listUsers,
  createUser,
  updateUser,
  deleteUser,
  setUserPassword,
  resetUserPassword,
  validatePasswordStrength,
  verifyPassword: (record, password) => verifyPassword(record, password),
  findUserByEmail,
  findUserById,
  sanitizeUser,
  getRoleDirectory,
  DEFAULT_TEMP_PASSWORD
};
