const crypto = require('crypto');

const DEFAULT_SESSION_TTL_MS = Number.isFinite(Number(process.env.SESSION_TTL_MS))
  ? Math.max(5 * 60 * 1000, Number(process.env.SESSION_TTL_MS))
  : 8 * 60 * 60 * 1000; // 8 hours

class SessionStore {
  constructor(ttlMs = DEFAULT_SESSION_TTL_MS){
    this.ttlMs = ttlMs;
    this.sessions = new Map();
  }

  createSession(userSnapshot){
    const token = crypto.randomBytes(48).toString('base64url');
    const now = Date.now();
    const expiresAt = now + this.ttlMs;
    const session = {
      token,
      userId: userSnapshot.id,
      roles: Array.isArray(userSnapshot.roles) ? [...userSnapshot.roles] : [],
      userVersion: userSnapshot.updatedAt || userSnapshot.updated_at || null,
      createdAt: now,
      expiresAt
    };
    this.sessions.set(token, session);
    return session;
  }

  get(token){
    if(!token){
      return null;
    }
    const session = this.sessions.get(token);
    if(!session){
      return null;
    }
    if(session.expiresAt <= Date.now()){
      this.sessions.delete(token);
      return null;
    }
    return session;
  }

  touch(token){
    const session = this.get(token);
    if(!session){
      return null;
    }
    session.expiresAt = Date.now() + this.ttlMs;
    return session;
  }

  revoke(token){
    if(!token){
      return;
    }
    this.sessions.delete(token);
  }

  revokeUserSessions(userId){
    if(!userId){
      return 0;
    }
    let removed = 0;
    for(const [token, session] of this.sessions.entries()){
      if(session.userId === userId){
        this.sessions.delete(token);
        removed += 1;
      }
    }
    return removed;
  }
}

module.exports = SessionStore;
