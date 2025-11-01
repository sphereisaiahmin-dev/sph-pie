const cookie = require('cookie');
const { getProvider } = require('../storage');
const { hasRole } = require('./roles');

const SESSION_COOKIE = 'mt_session';

function serializeCookie(name, value, options = {}){
  const defaultOptions = {
    httpOnly: true,
    sameSite: 'lax',
    path: '/',
    secure: process.env.NODE_ENV === 'production'
  };
  const finalOptions = {...defaultOptions, ...options};
  if(typeof finalOptions.maxAge === 'number' && Number.isFinite(finalOptions.maxAge)){
    if(finalOptions.maxAge <= 0){
      finalOptions.maxAge = 0;
    }else{
      finalOptions.maxAge = Math.max(1, Math.floor(finalOptions.maxAge / 1000));
    }
  }
  return cookie.serialize(name, value, finalOptions);
}

function attachCookieHelpers(res){
  if(typeof res.setSessionCookie === 'function'){
    return;
  }
  res.setSessionCookie = (token, options = {}) =>{
    const cookieValue = serializeCookie(SESSION_COOKIE, token, options);
    res.append('Set-Cookie', cookieValue);
  };
  res.clearSessionCookie = () =>{
    const cookieValue = serializeCookie(SESSION_COOKIE, '', {maxAge: 0});
    res.append('Set-Cookie', cookieValue);
  };
}

function sessionMiddleware(sessionStore){
  return async function sessionHandler(req, res, next){
    attachCookieHelpers(res);
    req.session = null;
    req.user = null;
    const header = req.headers?.cookie;
    if(!header){
      return next();
    }
    let cookies = {};
    try{
      cookies = cookie.parse(header);
    }catch(err){
      return next();
    }
    const token = cookies[SESSION_COOKIE];
    if(!token){
      return next();
    }
    const session = sessionStore.get(token);
    if(!session){
      res.clearSessionCookie();
      return next();
    }
    try{
      const provider = getProvider();
      const user = await provider.getUser(session.userId);
      if(!user || !user.isActive){
        sessionStore.revoke(token);
        res.clearSessionCookie();
        return next();
      }
      const userVersion = user.updatedAt || user.updated_at || null;
      if(session.userVersion && userVersion && session.userVersion !== userVersion){
        sessionStore.revoke(token);
        res.clearSessionCookie();
        return next();
      }
      sessionStore.touch(token);
      req.session = session;
      req.user = sanitizeUser(user);
      return next();
    }catch(err){
      console.error('Failed to hydrate session', err);
      sessionStore.revoke(token);
      res.clearSessionCookie();
      return next();
    }
  };
}

function sanitizeUser(user){
  if(!user){
    return null;
  }
  const {passwordHash, password_hash, ...rest} = user;
  if(Array.isArray(rest.roles)){
    rest.roles = [...rest.roles];
  }else{
    rest.roles = [];
  }
  return rest;
}

function requireAuth(){
  return function requireAuthHandler(req, res, next){
    if(!req.user){
      res.status(401).json({error: 'Authentication required'});
      return;
    }
    next();
  };
}

function requireRoles(roles){
  const expected = Array.isArray(roles) ? roles : [roles];
  return function requireRolesHandler(req, res, next){
    if(!req.user){
      res.status(401).json({error: 'Authentication required'});
      return;
    }
    if(!hasRole(req.user.roles, expected)){
      res.status(403).json({error: 'Insufficient permissions'});
      return;
    }
    next();
  };
}

module.exports = {
  SESSION_COOKIE,
  sanitizeUser,
  sessionMiddleware,
  requireAuth,
  requireRoles
};
