const express = require('express');
const SessionStore = require('./sessionStore');
const { sessionMiddleware, requireAuth, requireRoles, sanitizeUser } = require('./middleware');
const { authenticate, changePassword, setUserPassword, listUsers, createUser, updateUser, getUserById } = require('./userService');

function createAuthRouter(sessionStore){
  const router = express.Router();

  router.post('/login', async (req, res, next)=>{
    try{
      const {email, password} = req.body || {};
      const user = await authenticate(email, password);
      if(!user){
        res.status(401).json({error: 'Invalid credentials'});
        return;
      }
      const session = sessionStore.createSession(user);
      res.setSessionCookie(session.token, {maxAge: sessionStore.ttlMs || undefined});
      res.json({user, session: {...session, token: undefined}});
    }catch(err){
      next(err);
    }
  });

  router.post('/logout', requireAuth(), (req, res)=>{
    if(req.session?.token){
      sessionStore.revoke(req.session.token);
    }
    res.clearSessionCookie();
    res.status(204).end();
  });

  router.post('/change-password', requireAuth(), async (req, res, next)=>{
    try{
      const {currentPassword, newPassword} = req.body || {};
      const updated = await changePassword(req.user.id, currentPassword, newPassword);
      sessionStore.revokeUserSessions(req.user.id);
      res.status(200).json({user: updated});
    }catch(err){
      next(err);
    }
  });

  return router;
}

function createUserRouter(){
  const router = express.Router();

  router.get('/', async (req, res, next)=>{
    try{
      const users = await listUsers();
      res.json({users});
    }catch(err){
      next(err);
    }
  });

  router.post('/', async (req, res, next)=>{
    try{
      const user = await createUser(req.body || {});
      res.status(201).json({user});
    }catch(err){
      next(err);
    }
  });

  router.get('/:id', async (req, res, next)=>{
    try{
      const user = await getUserById(req.params.id);
      if(!user){
        res.status(404).json({error: 'User not found'});
        return;
      }
      res.json({user});
    }catch(err){
      next(err);
    }
  });

  router.put('/:id', async (req, res, next)=>{
    try{
      const user = await updateUser(req.params.id, req.body || {});
      res.json({user});
    }catch(err){
      next(err);
    }
  });

  router.post('/:id/password', async (req, res, next)=>{
    try{
      const {password, requirePasswordChange} = req.body || {};
      if(typeof password !== 'string' || password.length < 8){
        const err = new Error('Password must be at least 8 characters.');
        err.status = 400;
        throw err;
      }
      const user = await setUserPassword(req.params.id, password, {requireChange: Boolean(requirePasswordChange)});
      res.json({user});
    }catch(err){
      next(err);
    }
  });

  return router;
}

module.exports = {
  SessionStore,
  createAuthRouter,
  createUserRouter,
  sessionMiddleware,
  requireAuth,
  requireRoles,
  sanitizeUser
};
