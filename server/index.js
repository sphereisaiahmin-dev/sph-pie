const path = require('path');
const express = require('express');
const morgan = require('morgan');
const { loadConfig, saveConfig } = require('./configStore');
const { initProvider, getProvider } = require('./storage');
const { setWebhookConfig, getWebhookStatus, dispatchEntryEvent, dispatchShowEvent } = require('./webhookDispatcher');
const { SessionStore, createAuthRouter, createUserRouter, sessionMiddleware, requireAuth, requireRoles, sanitizeUser } = require('./auth');
const { seedDefaultUsers } = require('./auth/userSeeder');

const DAY_IN_MS = 24 * 60 * 60 * 1000;

async function bootstrap(){
  const app = express();
  let config = loadConfig();
  const sessionStore = new SessionStore();
  let configuredHost = config.host || '10.241.211.120';
  let configuredPort = config.port || 3000;
  const envPort = Number.parseInt(process.env.PORT, 10);
  const envHost = process.env.HOST || process.env.LISTEN_HOST;
  let boundPort = Number.isFinite(envPort) ? envPort : configuredPort;
  let boundHost = envHost || configuredHost;
  let serverInstance = null;
  await initProvider(config);
  const defaultSeedPassword = process.env.MONKEY_TRACKER_SEED_PASSWORD || process.env.SEED_USER_PASSWORD || 'sphere';
  await seedDefaultUsers({defaultPassword: defaultSeedPassword});
  await setWebhookConfig(config.webhook);

  app.use(express.json({limit: '2mb'}));
  app.use(morgan('dev'));
  app.use(express.static(path.join(__dirname, '..', 'public')));
  app.use(sessionMiddleware(sessionStore));

  const requireAuthenticated = requireAuth();
  const requireAdmin = requireRoles('admin');
  const requireCrewOrHigher = requireRoles(['crew', 'operator', 'lead', 'admin']);
  const requireOperatorOrLead = requireRoles(['operator', 'lead', 'admin']);
  const requireLeadOrAdmin = requireRoles(['lead', 'admin']);

  app.use('/api/auth', createAuthRouter(sessionStore));
  app.use('/api/users', requireAuthenticated, requireAdmin, createUserRouter());

  app.get('/api/session', requireAuthenticated, (req, res)=>{
    res.json({user: sanitizeUser(req.user), session: req.session ? {...req.session, token: undefined} : null});
  });

  function asyncHandler(fn){
    return (req, res, next)=>{
      Promise.resolve(fn(req, res, next)).catch(next);
    };
  }

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

  app.get('/api/config', requireAuthenticated, requireAdmin, (req, res)=>{
    const storageMeta = getStorageMetadata();
    res.json({...config, storageMeta, webhookStatus: getWebhookStatus()});
  });

  app.put('/api/config', requireAuthenticated, requireAdmin, asyncHandler(async (req, res)=>{
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

  app.get('/api/staff', requireAuthenticated, asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const staff = await provider.getStaff();
    res.json(staff);
  }));

  app.put('/api/staff', requireAuthenticated, requireAdmin, asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const staff = await provider.replaceStaff(req.body || {});
    res.json(staff);
  }));

  app.get('/api/shows', requireAuthenticated, requireCrewOrHigher, asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const shows = await provider.listShows();
    const storageMeta = getStorageMetadata();
    res.json({storage: storageMeta.label, storageMeta, webhook: getWebhookStatus(), shows});
  }));

  app.get('/api/shows/archive', requireAuthenticated, requireCrewOrHigher, asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const shows = await provider.listArchivedShows();
    res.json({shows});
  }));

  app.post('/api/shows', requireAuthenticated, requireLeadOrAdmin, asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const show = await provider.createShow(req.body || {});
    res.status(201).json(show);
  }));

  app.get('/api/shows/:id', requireAuthenticated, requireCrewOrHigher, asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const show = await provider.getShow(req.params.id);
    if(!show){
      res.status(404).json({error: 'Show not found'});
      return;
    }
    res.json(show);
  }));

  app.put('/api/shows/:id', requireAuthenticated, requireLeadOrAdmin, asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const show = await provider.updateShow(req.params.id, req.body || {});
    if(!show){
      res.status(404).json({error: 'Show not found'});
      return;
    }
    res.json(show);
  }));

  app.delete('/api/shows/:id', requireAuthenticated, requireAdmin, asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const archived = await provider.deleteShow(req.params.id);
    if(!archived){
      res.status(404).json({error: 'Show not found'});
      return;
    }
    await dispatchShowEvent('show.deleted', archived);
    res.json(archived);
  }));

  app.post('/api/shows/:id/archive', requireAuthenticated, requireLeadOrAdmin, asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const archived = await provider.archiveShowNow(req.params.id);
    if(!archived){
      res.status(404).json({error: 'Show not found'});
      return;
    }
    await dispatchShowEvent('show.archived', archived);
    res.json(archived);
  }));

  app.post('/api/webhook/simulate-month', requireAuthenticated, requireAdmin, asyncHandler(async (req, res)=>{
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
    const requested = limitedShows.length;
    const requestedAt = new Date().toISOString();
    let dispatched = 0;
    let skipped = 0;
    const errors = [];
    for(let index = 0; index < limitedShows.length; index += 1){
      const show = limitedShows[index];
      const meta = {
        simulation: {
          source: 'admin-settings',
          requestedAt,
          showIndex: index,
          totalShows: requested,
          rangeDays: 30
        }
      };
      const result = await dispatchShowEvent('show.archived', show, meta);
      if(result?.skipped){
        skipped += 1;
      }else if(result?.success === false){
        errors.push({showId: show?.id || null, error: result.error});
      }else{
        dispatched += 1;
      }
    }

    res.json({requested, dispatched, skipped, errors, webhook: getWebhookStatus()});
  }));

  app.post('/api/shows/:id/entries', requireAuthenticated, requireOperatorOrLead, asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const entry = await provider.addEntry(req.params.id, req.body || {});
    if(!entry){
      res.status(404).json({error: 'Show not found'});
      return;
    }
    const show = await provider.getShow(req.params.id);
    await dispatchEntryEvent('entry.created', show, entry);
    res.status(201).json(entry);
  }));

  app.put('/api/shows/:id/entries/:entryId', requireAuthenticated, requireOperatorOrLead, asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const entry = await provider.updateEntry(req.params.id, req.params.entryId, req.body || {});
    if(!entry){
      res.status(404).json({error: 'Entry not found'});
      return;
    }
    const show = await provider.getShow(req.params.id);
    await dispatchEntryEvent('entry.updated', show, entry);
    res.json(entry);
  }));

  app.delete('/api/shows/:id/entries/:entryId', requireAuthenticated, requireLeadOrAdmin, asyncHandler(async (req, res)=>{
    const provider = getProvider();
    const result = await provider.deleteEntry(req.params.id, req.params.entryId);
    if(!result){
      res.status(404).json({error: 'Entry not found'});
      return;
    }
    res.status(204).end();
  }));

  // Serve index.html for any non-API request (client-side routing support)
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

bootstrap().catch(err=>{
  console.error('Failed to start server', err);
  process.exit(1);
});
