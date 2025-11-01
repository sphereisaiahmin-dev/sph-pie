const { getProvider } = require('../storage');
const { normalizeRoles, normalizeRoleName, VALID_ROLES } = require('./roles');
const { hashPassword, verifyPassword } = require('./password');

function sanitizeUser(user){
  if(!user){
    return null;
  }
  const {passwordHash, password_hash, ...rest} = user;
  if(Array.isArray(rest.roles)){
    rest.roles = [...new Set(rest.roles.map(role => role.toLowerCase()))];
  }else{
    rest.roles = [];
  }
  return rest;
}

async function listUsers(){
  const provider = getProvider();
  const users = await provider.listUsers();
  return users.map(sanitizeUser);
}

async function getUserById(id){
  const provider = getProvider();
  const user = await provider.getUser(id);
  return sanitizeUser(user);
}

async function getUserByEmail(email){
  const provider = getProvider();
  const user = await provider.getUserByEmail(email);
  return user ? {...user} : null;
}

async function authenticate(email, password){
  if(typeof email !== 'string' || typeof password !== 'string'){
    return null;
  }
  const provider = getProvider();
  const user = await provider.getUserByEmail(email);
  if(!user || !user.isActive){
    return null;
  }
  if(!user.passwordHash){
    return null;
  }
  const valid = await verifyPassword(password, user.passwordHash);
  if(!valid){
    return null;
  }
  return sanitizeUser(user);
}

function ensureValidRoles(roles){
  const normalized = normalizeRoles(roles, {dedupe: true});
  if(normalized.length === 0){
    const err = new Error('At least one role is required.');
    err.status = 400;
    throw err;
  }
  for(const role of normalized){
    if(!VALID_ROLES.includes(role)){
      const err = new Error(`Unsupported role: ${role}`);
      err.status = 400;
      throw err;
    }
  }
  return normalized;
}

async function createUser({name, email, password, roles, isActive = true}){
  if(typeof name !== 'string' || !name.trim()){
    const err = new Error('Name is required');
    err.status = 400;
    throw err;
  }
  if(typeof email !== 'string' || !email.trim()){
    const err = new Error('Email is required');
    err.status = 400;
    throw err;
  }
  const provider = getProvider();
  const normalizedRoles = ensureValidRoles(roles);
  const existing = await provider.getUserByEmail(email.trim().toLowerCase());
  if(existing){
    const err = new Error('Email already registered');
    err.status = 409;
    throw err;
  }
  let passwordHash = null;
  if(password){
    passwordHash = await hashPassword(password);
  }
  const created = await provider.createUser({
    name: name.trim(),
    email: email.trim().toLowerCase(),
    passwordHash,
    roles: normalizedRoles,
    isActive: Boolean(isActive)
  });
  return sanitizeUser(created);
}

async function updateUser(id, updates){
  const provider = getProvider();
  const existing = await provider.getUser(id);
  if(!existing){
    const err = new Error('User not found');
    err.status = 404;
    throw err;
  }
  const payload = {};
  if(updates.name !== undefined){
    if(typeof updates.name !== 'string' || !updates.name.trim()){
      const err = new Error('Name is required');
      err.status = 400;
      throw err;
    }
    payload.name = updates.name.trim();
  }
  if(updates.email !== undefined){
    if(typeof updates.email !== 'string' || !updates.email.trim()){
      const err = new Error('Email is required');
      err.status = 400;
      throw err;
    }
    const normalizedEmail = updates.email.trim().toLowerCase();
    const duplicate = await provider.getUserByEmail(normalizedEmail);
    if(duplicate && duplicate.id !== id){
      const err = new Error('Email already registered');
      err.status = 409;
      throw err;
    }
    payload.email = normalizedEmail;
  }
  if(updates.isActive !== undefined){
    payload.isActive = Boolean(updates.isActive);
  }
  if(updates.roles !== undefined){
    payload.roles = ensureValidRoles(updates.roles);
  }
  const updated = await provider.updateUser(id, payload);
  return sanitizeUser(updated);
}

async function setUserPassword(id, password){
  const provider = getProvider();
  const existing = await provider.getUser(id);
  if(!existing){
    const err = new Error('User not found');
    err.status = 404;
    throw err;
  }
  const passwordHash = await hashPassword(password);
  const updated = await provider.setUserPassword(id, passwordHash);
  return sanitizeUser(updated);
}

async function changePassword(id, currentPassword, nextPassword){
  const provider = getProvider();
  const existing = await provider.getUser(id);
  if(!existing){
    const err = new Error('User not found');
    err.status = 404;
    throw err;
  }
  if(!existing.passwordHash){
    const err = new Error('Password is not set for this account.');
    err.status = 400;
    throw err;
  }
  const valid = await verifyPassword(currentPassword, existing.passwordHash);
  if(!valid){
    const err = new Error('Current password is incorrect.');
    err.status = 400;
    throw err;
  }
  const passwordHash = await hashPassword(nextPassword);
  const updated = await provider.setUserPassword(id, passwordHash);
  return sanitizeUser(updated);
}

function legacyRoleForKey(key){
  const normalized = normalizeRoleName(key);
  return normalized;
}

module.exports = {
  listUsers,
  getUserById,
  getUserByEmail,
  authenticate,
  createUser,
  updateUser,
  setUserPassword,
  changePassword,
  sanitizeUser,
  legacyRoleForKey
};
