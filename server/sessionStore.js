const crypto = require('crypto');

const SESSION_TTL_MS = 12 * 60 * 60 * 1000;
const SESSION_COOKIE_NAME = 'mt_session';

const sessions = new Map();

function hashToken(token){
  return crypto.createHash('sha256').update(token).digest('hex');
}

function createSession(userId){
  const token = crypto.randomBytes(48).toString('hex');
  const tokenHash = hashToken(token);
  const now = Date.now();
  const expiresAt = now + SESSION_TTL_MS;
  sessions.set(tokenHash, {userId, createdAt: now, expiresAt});
  return {token, expiresAt};
}

function getSession(token){
  if(!token){
    return null;
  }
  const tokenHash = hashToken(token);
  const session = sessions.get(tokenHash);
  if(!session){
    return null;
  }
  if(session.expiresAt <= Date.now()){
    sessions.delete(tokenHash);
    return null;
  }
  return {...session, tokenHash};
}

function touchSession(token){
  const existing = getSession(token);
  if(!existing){
    return null;
  }
  const newExpires = Date.now() + SESSION_TTL_MS;
  sessions.set(existing.tokenHash, {userId: existing.userId, createdAt: existing.createdAt, expiresAt: newExpires});
  return {userId: existing.userId, expiresAt: newExpires};
}

function deleteSession(token){
  if(!token){
    return;
  }
  const tokenHash = hashToken(token);
  sessions.delete(tokenHash);
}

function deleteSessionsForUser(userId){
  if(!userId){
    return;
  }
  for(const [tokenHash, session] of sessions.entries()){
    if(session.userId === userId){
      sessions.delete(tokenHash);
    }
  }
}

function purgeExpiredSessions(){
  const now = Date.now();
  for(const [tokenHash, session] of sessions.entries()){
    if(session.expiresAt <= now){
      sessions.delete(tokenHash);
    }
  }
}

module.exports = {
  createSession,
  getSession,
  touchSession,
  deleteSession,
  deleteSessionsForUser,
  purgeExpiredSessions,
  SESSION_TTL_MS,
  SESSION_COOKIE_NAME
};
