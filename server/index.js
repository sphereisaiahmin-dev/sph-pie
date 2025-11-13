const path = require('path');
const express = require('express');
const morgan = require('morgan');
const { loadConfig, saveConfig } = require('./configStore');
const { initProvider, getProvider } = require('./storage');
const { setWebhookConfig, getWebhookStatus, dispatchShowEvent } = require('./webhookDispatcher');
const {
  initUserStore,
  listUsers,
  createUser,
  updateUser,
  setUserPassword,
  resetUserPassword,
  verifyPassword,
  findUserByEmail,
  findUserById,
  sanitizeUser,
  getRoleDirectory,
  DEFAULT_TEMP_PASSWORD
} = require('./userStore');
const {
  createSession,
  getSession,
  deleteSession,
  deleteSessionsForUser,
  SESSION_COOKIE_NAME
} = require('./sessionStore');

const DAY_IN_MS = 24 * 60 * 60 * 1000;
const PASSWORD_RESET_ALLOW = new Set([
  'GET:/api/auth/session',
  'POST:/api/auth/password',
  'POST:/api/auth/logout',
  'GET:/api/health'
]);

async function bootstrap(){
  const app = express();
  let config = loadConfig();
  let configuredHost = config.host || '10.241.211.120';
  let configuredPort = config.port || 3000;
  const envPort = Number.parseInt(process.env.PORT, 10);
  const envHost = process.env.HOST || process.env.LISTEN_HOST;
  let boundPort = Number.isFinite(envPort) ? envPort : configuredPort;
  let boundHost = envHost || configuredHost;
  let serverInstance = null;
  await initProvider(config);
  await initUserStore();
  await setWebhookConfig(config.webhook);

  app.use(express.json({limit: '2mb'}));
  app.use(morgan('dev'));
  app.use(express.static(path.join(__dirname, '..', 'public')));

  function asyncHandler(fn){
    return (req, res, next)=>{
      Promise.resolve(fn(req, res, next)).catch(next);
    };
  }

  app.use(asyncHandler(async (req, res, next)=>{
    const token = readSessionToken(req);
    if(!token){
      return next();
    }
    const session = getSession(token);
    if(!session){
      return next();
    }
    const userRecord = findUserById(session.userId);
    if(!userRecord){
      deleteSession(token);
      return next();
    }
    req.sessionToken = token;
    req.userRecord = userRecord;
    req.user = sanitizeUser(userRecord);
    return next();
  }));

  app.use((req, res, next)=>{
    if(!req.path.startsWith('/api/')){
      return next();
    }
    if(!req.user || !req.user.needsPasswordReset){
      return next();
    }
    const key = `${req.method.toUpperCase()}:${req.path}`;
    if(PASSWORD_RESET_ALLOW.has(key)){
      return next();
    }
    return res.status(423).json({error: 'Password reset required'});
  });

  function getStorageMetadata(){
    try{
      const provider = getProvider();
      if(provider && typeof provider.getStorageMetadata === 'function'){
        const meta = provider.getStorageMetadata();
        if(meta && typeof meta === 'object'){
          return {
            label: typeof meta.label === 'string' && meta.label ? meta.label : (provider.getStorageLabel?.() || 'SQL.js v2'),
            ...meta
          };
        }
      }
      const label = provider?.getStorageLabel?.() || 'SQL.js v2';
      return {label};
    }catch(err){
      return {label: 'SQL.js v2'};
    }
  }

  app.get('/api/health', (req, res)=>{
    const storageMeta = getStorageMetadata();
    res.json({
      status: 'ok',
      storage: storageMeta.label,
      storageMeta,
      webhook: getWebhookStatus(),
      host: configuredHost,
      port: configuredPort,
      boundHost,
      boundPort
    });
  });

  app.get('/api/auth/session', (req, res)=>{
    if(!req.user){
      res.json({authenticated: false});
      return;
    }
    res.json({authenticated: true, user: req.user});
  });

  app.post('/api/auth/login', asyncHandler(async (req, res)=>{
    const email = typeof req.body?.email === 'string' ? req.body.email.trim().toLowerCase() : '';
    const password = typeof req.body?.password === 'string' ? req.body.password : '';
    if(!email || !password){
      res.status(400).json({error: 'Email and password are required'});
      return;
    }
    const userRecord = findUserByEmail(email);
    if(!userRecord || !verifyPassword(userRecord, password)){
      res.status(401).json({error: 'Invalid email or password'});
      return;
    }
    const {token, expiresAt} = createSession(userRecord.id);
    setSessionCookie(res, token, expiresAt);
    res.json({authenticated: true, user: sanitizeUser(userRecord)});
  }));

  app.post('/api/auth/logout', requireAuth, (req, res)=>{
    if(req.sessionToken){
      deleteSession(req.sessionToken);
    }
    clearSessionCookie(res);
    res.json({ok: true});
  });

  app.post('/api/auth/password', requireAuth, asyncHandler(async (req, res)=>{
    const {currentPassword, newPassword} = req.body || {};
    const userRecord = findUserById(req.user.id);
    if(!userRecord){
      res.status(404).json({error: 'User not found'});
      return;
    }
    if(!verifyPassword(userRecord, typeof currentPassword === 'string' ? currentPassword : '')){
      res.status(400).json({error: 'Current password is incorrect'});
      return;
    }
    await setUserPassword(userRecord.id, typeof newPassword === 'string' ? newPassword : '', {requireReset: false});
    deleteSessionsForUser(userRecord.id);
    const updatedRecord = findUserById(userRecord.id);
    const {token, expiresAt} = createSession(updatedRecord.id);
    setSessionCookie(res, token, expiresAt);
    res.json({user: sanitizeUser(updatedRecord)});
  }));

  app.get('/api/users', requireRoles('admin'), (req, res)=>{
    res.json({users: listUsers(), defaultPassword: DEFAULT_TEMP_PASSWORD});
  });

  app.post('/api/users', requireRoles('admin'), asyncHandler(async (req, res)=>{
    const body = req.body || {};
    const roles = normalizeRolesInput(body.roles);
    const user = await createUser({
      name: body.name,
      email: body.email,
      roles: roles === undefined ? [] : roles
    });
    res.status(201).json({user, defaultPassword: DEFAULT_TEMP_PASSWORD});
  }));

  app.put('/api/users/:id', requireRoles('admin'), asyncHandler(async (req, res)=>{
    const roles = normalizeRolesInput(req.body?.roles);
    const payload = {
      name: req.body?.name,
      email: req.body?.email
    };
    if(roles !== undefined){
      payload.roles = roles;
    }
    const updated = await updateUser(req.params.id, payload);
    res.json({user: updated});
  }));

  app.post('/api/users/:id/reset-password', requireRoles('admin'), asyncHandler(async (req, res)=>{
    const updated = await resetUserPassword(req.params.id);
    deleteSessionsForUser(req.params.id);
    res.json({user: updated, defaultPassword: DEFAULT_TEMP_PASSWORD});
  }));

  app.get('/api/config', requireAuth, (req, res)=>{
    const storageMeta = getStorageMetadata();
    res.json({...config, storageMeta, webhookStatus: getWebhookStatus()});
  });

  app.put('/api/config', requireRoles('admin'), asyncHandler(async (req, res)=>{
    const nextConfig = saveConfig(req.body || {});
    await initProvider(nextConfig);
    await setWebhookConfig(nextConfig.webhook);
    config = nextConfig;
    configuredHost = config.host || configuredHost;
    configuredPort = config.port || configuredPort;
    if(!envHost && configuredHost !== boundHost){
      console.warn(`Configured host updated to ${configuredHost}. Restart the server to bind to the new address.`);
    }
    if(!Number.isFinite(envPort) && configuredPort !== boundPort){
      console.warn(`Configured port updated to ${configuredPort}. Restart the server to bind to the new port.`);
    }
    const storageMeta = getStorageMetadata();
    res.json({...config, storageMeta, webhookStatus: getWebhookStatus()});
  }));

  app.get('/api/staff', requireAuth, (req, res)=>{
    const directory = getRoleDirectory();
    res.json({
      leads: directory.leads,
      operators: directory.operators,
      stagecrew: directory.stagecrew,
      pilots: directory.operators,
      crew: directory.stagecrew,
      monkeyLeads: directory.leads
    });
  });

  app.put('/api/staff', requireRoles('admin'), (req, res)=>{
    res.status(410).json({error: 'Manual staff editing disabled. Manage users instead.'});
  });

  app.get('/api/shows', requireRoles('lead', 'operator', 'stagecrew'), asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const shows = await provider.listShows();
    const storageMeta = getStorageMetadata();
    res.json({storage: storageMeta.label, storageMeta, webhook: getWebhookStatus(), shows});
  }));

  app.get('/api/shows/archive', requireRoles('lead', 'operator', 'stagecrew'), asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const shows = await provider.listArchivedShows();
    res.json({shows});
  }));

  app.post('/api/shows', requireRoles('lead'), asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const show = await provider.createShow(req.body || {});
    res.status(201).json(show);
  }));

  app.get('/api/shows/:id', requireRoles('lead', 'operator', 'stagecrew'), asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const show = await provider.getShow(req.params.id);
    if(!show){
      res.status(404).json({error: 'Show not found'});
      return;
    }
    res.json(show);
  }));

  app.put('/api/shows/:id', requireRoles('lead'), asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const show = await provider.updateShow(req.params.id, req.body || {});
    if(!show){
      res.status(404).json({error: 'Show not found'});
      return;
    }
    res.json(show);
  }));

  app.delete('/api/shows/:id', requireRoles('lead'), asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const archived = await provider.deleteShow(req.params.id);
    if(!archived){
      res.status(404).json({error: 'Show not found'});
      return;
    }
    await dispatchShowEvent('show.deleted', archived);
    res.json(archived);
  }));

  app.post('/api/shows/:id/archive', requireRoles('lead'), asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const archived = await provider.archiveShowNow(req.params.id);
    if(!archived){
      res.status(404).json({error: 'Show not found'});
      return;
    }
    await dispatchShowEvent('show.archived', archived);
    res.json(archived);
  }));

  app.post('/api/webhook/simulate-month', requireRoles('admin'), asyncHandler(async (req, res)=>{
    const provider = getProvider();
    if(!provider){
      res.status(503).json({error: 'Storage provider not ready'});
      return;
    }
    const now = Date.now();
    const windowStart = now - (30 * DAY_IN_MS);

    function getTimestamp(show){
      if(!show || typeof show !== 'object'){
        return null;
      }
      const candidates = [show.archivedAt, show.updatedAt, show.createdAt];
      for(const value of candidates){
        if(value === null || value === undefined){
          continue;
        }
        const num = Number(value);
        if(Number.isFinite(num)){
          return num;
        }
        if(typeof value === 'string'){
          const parsed = Date.parse(value);
          if(Number.isFinite(parsed)){
            return parsed;
          }
        }
      }
      return null;
    }

    function selectRecentShows(list = []){
      const normalized = Array.isArray(list) ? list : [];
      const recent = normalized.filter(show =>{
        const ts = getTimestamp(show);
        return ts !== null && ts >= windowStart;
      });
      if(recent.length){
        return recent;
      }
      return normalized.slice(0, 30);
    }

    let shows = [];
    if(typeof provider.listArchivedShows === 'function'){
      const archivedShows = await provider.listArchivedShows();
      shows = selectRecentShows(archivedShows);
    }
    if(!shows.length && typeof provider.listShows === 'function'){
      const activeShows = await provider.listShows();
      shows = selectRecentShows(activeShows);
    }

    const limitedShows = shows.slice(0, 90);
    const requestedAt = new Date().toISOString();
    const SHOW_LIMIT = 3;
    const ENTRY_LIMIT = 6;
    const selectedShows = [];
    const entryLimitErrors = [];

    for(let index = 0; index < limitedShows.length && selectedShows.length < SHOW_LIMIT; index += 1){
      const show = limitedShows[index];
      const entries = Array.isArray(show?.entries) ? show.entries : [];
      if(entries.length < ENTRY_LIMIT){
        entryLimitErrors.push({
          showId: show?.id || null,
          error: `Show requires at least ${ENTRY_LIMIT} operator entries for simulation`
        });
        continue;
      }
      selectedShows.push({
        ...show,
        entries: entries.slice(0, ENTRY_LIMIT)
      });
    }

    const requested = selectedShows.length;
    if(requested === 0){
      res.json({
        requested: 0,
        dispatched: 0,
        skipped: 0,
        errors: entryLimitErrors,
        webhook: getWebhookStatus()
      });
      return;
    }

    let dispatched = 0;
    let skipped = 0;
    let entryPayloads = 0;
    let entryFailures = 0;
    const errors = requested < SHOW_LIMIT ? [...entryLimitErrors] : [];

    for(let index = 0; index < selectedShows.length; index += 1){
      const show = selectedShows[index];
      const meta = {
        simulation: {
          source: 'admin-settings',
          requestedAt,
          showIndex: index,
          totalShows: selectedShows.length,
          rangeDays: 30,
          entryLimit: ENTRY_LIMIT
        }
      };
      const result = await dispatchShowEvent('show.archived', show, meta);
      if(result?.skipped){
        skipped += 1;
        continue;
      }
      entryPayloads += Number(result?.dispatched || 0);
      entryFailures += Number(result?.failed || 0);
      if(result?.success === false){
        errors.push({
          showId: show?.id || null,
          error: result.error || 'Unknown dispatch error',
          failedEntries: Number.isFinite(result?.failed) ? result.failed : undefined
        });
      }else{
        dispatched += 1;
      }
    }

    res.json({
      requested,
      dispatched,
      skipped,
      entryPayloads,
      entryFailures,
      errors,
      webhook: getWebhookStatus()
    });
  }));

  app.post('/api/shows/:id/entries', requireRoles('lead', 'operator'), asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const entry = await provider.addEntry(req.params.id, req.body || {});
    if(!entry){
      res.status(404).json({error: 'Show not found'});
      return;
    }
    res.status(201).json(entry);
  }));

  app.put('/api/shows/:id/entries/:entryId', requireRoles('lead', 'operator'), asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const entry = await provider.updateEntry(req.params.id, req.params.entryId, req.body || {});
    if(!entry){
      res.status(404).json({error: 'Entry not found'});
      return;
    }
    res.json(entry);
  }));

  app.delete('/api/shows/:id/entries/:entryId', requireRoles('lead', 'operator'), asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const result = await provider.deleteEntry(req.params.id, req.params.entryId);
    if(!result){
      res.status(404).json({error: 'Entry not found'});
      return;
    }
    res.status(204).end();
  }));

  app.get(/^(?!\/api\/).*/, (req, res)=>{
    res.sendFile(path.join(__dirname, '..', 'public', 'index.html'));
  });

  app.use((err, req, res, next)=>{ // eslint-disable-line no-unused-vars
    console.error(err);
    const status = Number.isInteger(err.status) ? err.status : 500;
    const payload = {
      error: status === 500 ? 'Internal server error' : (err.message || 'Request failed')
    };
    if(status === 500 && err.message){
      payload.detail = err.message;
    }
    res.status(status).json(payload);
  });

  function handleListenError(err){
    if(err.code === 'EADDRNOTAVAIL' && !envHost && boundHost !== '0.0.0.0'){
      console.warn(`Address ${boundHost} is not available on this machine. Falling back to 0.0.0.0.`);
      serverInstance?.off('error', handleListenError);
      boundHost = '0.0.0.0';
      startListening(boundHost);
      return;
    }
    console.error('Failed to bind server', err);
    process.exit(1);
  }

  function startListening(targetHost){
    serverInstance = app.listen(boundPort, targetHost, ()=>{
      console.log(`Server listening on http://${targetHost}:${boundPort}`);
      if(targetHost !== configuredHost){
        console.log(`Configured LAN URL: http://${configuredHost}:${configuredPort}`);
      }
      console.log('Press Ctrl+C to stop the server.');
    });
    serverInstance.on('error', handleListenError);
  }

  startListening(boundHost);
}

function normalizeRolesInput(input){
  if(input === undefined || input === null){
    return undefined;
  }
  if(Array.isArray(input)){
    return input;
  }
  if(typeof input === 'string'){
    return input.split(',').map(part => part.trim()).filter(Boolean);
  }
  return [];
}

function readSessionToken(req){
  const header = req.headers?.cookie;
  if(!header){
    return null;
  }
  const cookies = header.split(';');
  for(const cookie of cookies){
    const trimmed = cookie.trim();
    if(!trimmed){
      continue;
    }
    if(trimmed.startsWith(`${SESSION_COOKIE_NAME}=`)){
      return decodeURIComponent(trimmed.slice(SESSION_COOKIE_NAME.length + 1));
    }
  }
  return null;
}

function setSessionCookie(res, token, expiresAt){
  const maxAge = Math.max(0, Math.floor((expiresAt - Date.now()) / 1000));
  const parts = [
    `${SESSION_COOKIE_NAME}=${token}`,
    'Path=/',
    'HttpOnly',
    'SameSite=Lax',
    `Max-Age=${maxAge}`
  ];
  if(process.env.NODE_ENV === 'production'){
    parts.push('Secure');
  }
  res.setHeader('Set-Cookie', parts.join('; '));
}

function clearSessionCookie(res){
  const parts = [
    `${SESSION_COOKIE_NAME}=`,
    'Path=/',
    'HttpOnly',
    'SameSite=Lax',
    'Max-Age=0'
  ];
  if(process.env.NODE_ENV === 'production'){
    parts.push('Secure');
  }
  res.setHeader('Set-Cookie', parts.join('; '));
}

function requireAuth(req, res, next){
  if(!req.user){
    res.status(401).json({error: 'Authentication required'});
    return;
  }
  next();
}

function requireRoles(...roles){
  const roleSet = new Set(roles);
  return (req, res, next)=>{
    if(!req.user){
      res.status(401).json({error: 'Authentication required'});
      return;
    }
    const userRoles = Array.isArray(req.user.roles) ? req.user.roles : [];
    if(userRoles.includes('admin')){
      next();
      return;
    }
    const allowed = userRoles.some(role => roleSet.has(role));
    if(!allowed){
      res.status(403).json({error: 'Insufficient permissions'});
      return;
    }
    next();
  };
}

bootstrap().catch(err=>{
  console.error('Failed to start server', err);
  process.exit(1);
});
