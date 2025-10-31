const axios = require('axios');

const DEFAULT_WEBHOOK_CONFIG = {
  enabled: false,
  url: '',
  method: 'POST',
  secret: '',
  headers: [],
  timeoutMs: 8000
};

const HANDSHAKE_METHODS = ['HEAD', 'OPTIONS', 'GET'];
const DEFAULT_HANDSHAKE_TIMEOUT = 5000;

const EXPORT_COLUMNS = [
  'showId','showDate','showTime','showLabel','crew','leadPilot','monkeyLead','showNotes',
  'entryId','unitId','planned','launched','status','primaryIssue','subIssue','otherDetail',
  'severity','rootCause','actions','operator','batteryId','delaySec','commandRx','notes'
];

let activeConfig = {...DEFAULT_WEBHOOK_CONFIG};
let verificationState = {
  status: 'disabled',
  targetUrl: '',
  verifiedAt: null,
  handshakeMethod: null,
  httpStatus: null,
  durationMs: null,
  error: null,
  errorCode: null
};
let lastSkipReason = null;

function isObject(value){
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function toBoolean(value){
  if(value === true){
    return true;
  }
  if(value === false){
    return false;
  }
  if(typeof value === 'string'){
    const normalized = value.trim().toLowerCase();
    if(['true', '1', 'yes', 'on'].includes(normalized)){
      return true;
    }
    if(['false', '0', 'no', 'off', ''].includes(normalized)){
      return false;
    }
  }
  if(typeof value === 'number'){
    return value !== 0;
  }
  return Boolean(value);
}

function normalizeTimeoutMs(value){
  const parsed = Number(value);
  if(Number.isFinite(parsed) && parsed > 0){
    return Math.min(parsed, 60000);
  }
  return DEFAULT_WEBHOOK_CONFIG.timeoutMs;
}

function formatUrlForLog(url){
  if(!url){
    return '(none)';
  }
  try{
    const parsed = new URL(url);
    return `${parsed.protocol}//${parsed.host}${parsed.pathname}`;
  }catch(err){
    return url;
  }
}

function updateVerificationState(patch = {}){
  verificationState = {
    status: patch.status || verificationState.status || 'unknown',
    targetUrl: activeConfig.url || '',
    verifiedAt: patch.verifiedAt || new Date().toISOString(),
    handshakeMethod: patch.handshakeMethod ?? null,
    httpStatus: patch.httpStatus ?? null,
    durationMs: patch.durationMs ?? null,
    error: patch.error ?? null,
    errorCode: patch.errorCode ?? null
  };
  return verificationState;
}

function normalizeHeaderList(headers){
  if(!headers){
    return [];
  }
  if(!Array.isArray(headers) && typeof headers === 'object'){
    return Object.entries(headers).map(([name, value])=>({name, value}));
  }
  return (Array.isArray(headers) ? headers : [])
    .map(header => {
      if(!header){
        return null;
      }
      if(typeof header === 'string'){
        const idx = header.indexOf(':');
        if(idx === -1){
          return null;
        }
        const name = header.slice(0, idx).trim();
        const value = header.slice(idx + 1).trim();
        return name ? {name, value} : null;
      }
      if(typeof header === 'object'){
        const name = String(header.name || header.key || '').trim();
        if(!name){
          return null;
        }
        const value = header.value !== undefined ? String(header.value) : '';
        return {name, value};
      }
      return null;
    })
    .filter(Boolean);
}

async function verifyWebhookConnection(options = {}){
  if(!activeConfig.enabled || !activeConfig.url){
    const reason = activeConfig.enabled ? 'Missing webhook URL' : 'Webhook disabled in configuration';
    if(lastSkipReason !== reason){
      console.info(`[webhook] Skipping verification: ${reason}.`);
      lastSkipReason = reason;
    }
    return updateVerificationState({
      status: 'disabled',
      error: reason,
      handshakeMethod: null,
      httpStatus: null,
      durationMs: null
    });
  }

  const timeout = Math.min(
    normalizeTimeoutMs(options.timeoutMs ?? activeConfig.timeoutMs ?? DEFAULT_WEBHOOK_CONFIG.timeoutMs),
    60000
  );
  const headers = buildRequestHeaders();
  let lastError = null;

  for(const method of HANDSHAKE_METHODS){
    const started = Date.now();
    try{
      const response = await axios({
        method,
        url: activeConfig.url,
        headers,
        timeout: Math.min(timeout, DEFAULT_HANDSHAKE_TIMEOUT),
        maxRedirects: 3,
        validateStatus: () => true
      });
      const status = response.status || 0;
      const duration = Date.now() - started;
      const success = status >= 200 && status < 400;
      const authChallenge = status === 401 || status === 403;
      const methodUnsupported = status === 405 || status === 501;
      const reachable = status >= 200 && status < 500;

      if(success || authChallenge){
        console.info(`[webhook] Handshake succeeded via ${method} ${formatUrlForLog(activeConfig.url)} (status=${status}, ${duration}ms).`);
        lastSkipReason = null;
        return updateVerificationState({
          status: 'ok',
          handshakeMethod: method,
          httpStatus: status,
          durationMs: duration,
          error: null,
          errorCode: null
        });
      }

      if(methodUnsupported){
        console.info(`[webhook] Handshake method ${method} not allowed (status=${status}). Trying next method.`);
        lastError = new Error(`HTTP ${status}`);
        lastError.response = {status};
        continue;
      }

      if(reachable){
        console.info(`[webhook] Handshake reached target via ${method} ${formatUrlForLog(activeConfig.url)} (status=${status}, ${duration}ms).`);
        lastSkipReason = null;
        return updateVerificationState({
          status: 'ok',
          handshakeMethod: method,
          httpStatus: status,
          durationMs: duration,
          error: null,
          errorCode: null
        });
      }

      lastError = new Error(`HTTP ${status}`);
      lastError.response = {status};
      console.warn(`[webhook] Handshake ${method} ${formatUrlForLog(activeConfig.url)} returned status ${status}.`);
    }catch(error){
      const duration = Date.now() - started;
      lastError = error;
      const status = error?.response?.status;
      const code = error?.code;
      const detail = status ? `HTTP ${status}` : (code || error.message);
      console.warn(`[webhook] Handshake failed via ${method} ${formatUrlForLog(activeConfig.url)} after ${duration}ms: ${detail}`);
    }
  }

  const failure = {
    status: 'error',
    handshakeMethod: null,
    httpStatus: lastError?.response?.status ?? null,
    durationMs: null,
    error: lastError?.message || 'Unable to verify webhook target',
    errorCode: lastError?.code || null
  };
  console.warn(`[webhook] Unable to verify webhook target ${formatUrlForLog(activeConfig.url)}: ${failure.error}`);
  return updateVerificationState(failure);
}

async function setWebhookConfig(config = {}){
  const normalized = {
    ...DEFAULT_WEBHOOK_CONFIG,
    ...(isObject(config) ? config : {})
  };
  normalized.enabled = toBoolean(normalized.enabled);
  normalized.url = typeof normalized.url === 'string' ? normalized.url.trim() : '';
  normalized.method = String(normalized.method || 'POST').toUpperCase();
  normalized.secret = typeof normalized.secret === 'string' ? normalized.secret : '';
  normalized.timeoutMs = normalizeTimeoutMs(normalized.timeoutMs);
  normalized.headers = normalizeHeaderList(normalized.headers);
  activeConfig = normalized;

  console.info(`[webhook] Configuration applied (enabled=${activeConfig.enabled ? 'yes' : 'no'}, method=${activeConfig.method}, url=${formatUrlForLog(activeConfig.url)}, headers=${activeConfig.headers.length}).`);

  lastSkipReason = null;
  return verifyWebhookConnection({timeoutMs: activeConfig.timeoutMs});
}

function getWebhookStatus(){
  return {
    enabled: Boolean(activeConfig.enabled && activeConfig.url),
    method: activeConfig.method,
    hasSecret: Boolean(activeConfig.secret),
    headerCount: Array.isArray(activeConfig.headers) ? activeConfig.headers.length : 0,
    timeoutMs: activeConfig.timeoutMs,
    verification: {...verificationState}
  };
}

function buildTableRow(show = {}, entry = {}){
  const crewList = Array.isArray(show.crew) ? show.crew : [];
  const actionsList = Array.isArray(entry.actions) ? entry.actions : [];
  return {
    showId: show.id || '',
    showDate: show.date || '',
    showTime: show.time || '',
    showLabel: show.label || '',
    crew: crewList.join('|'),
    leadPilot: show.leadPilot || '',
    monkeyLead: show.monkeyLead || '',
    showNotes: show.notes || '',
    entryId: entry.id || '',
    unitId: entry.unitId || '',
    planned: entry.planned || '',
    launched: entry.launched || '',
    status: entry.status || '',
    primaryIssue: entry.status === 'Completed' ? '' : (entry.primaryIssue || ''),
    subIssue: entry.status === 'Completed' ? '' : (entry.subIssue || ''),
    otherDetail: entry.status === 'Completed' ? '' : (entry.otherDetail || ''),
    severity: entry.status === 'Completed' ? '' : (entry.severity || ''),
    rootCause: entry.status === 'Completed' ? '' : (entry.rootCause || ''),
    actions: actionsList.join('|'),
    operator: entry.operator || '',
    batteryId: entry.batteryId || '',
    delaySec: entry.delaySec === null || entry.delaySec === undefined ? '' : entry.delaySec,
    commandRx: entry.commandRx || '',
    notes: entry.notes || ''
  };
}

function toFiniteOrNull(value){
  if(value === null || value === undefined){
    return null;
  }
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function buildEntrySnapshot(entry = {}){
  const actionsList = Array.isArray(entry.actions)
    ? entry.actions.filter(action => action !== undefined && action !== null && String(action).trim() !== '')
    : [];
  const statusKey = String(entry.status || '').trim().toLowerCase();
  const includeIssueDetails = statusKey !== 'completed';
  return {
    id: entry.id || '',
    unitId: entry.unitId || '',
    planned: entry.planned || '',
    launched: entry.launched || '',
    status: entry.status || '',
    primaryIssue: includeIssueDetails ? (entry.primaryIssue || '') : '',
    subIssue: includeIssueDetails ? (entry.subIssue || '') : '',
    severity: includeIssueDetails ? (entry.severity || '') : '',
    rootCause: includeIssueDetails ? (entry.rootCause || '') : '',
    actions: actionsList,
    batteryId: entry.batteryId || '',
    delaySec: toFiniteOrNull(entry.delaySec),
    commandRx: entry.commandRx || '',
    timestamp: entry.ts || null
  };
}

function pickTopValue(countMap, totalEntries){
  let topValue = null;
  let topCount = 0;
  for(const [value, count] of countMap.entries()){
    if(count > topCount){
      topValue = value;
      topCount = count;
    }
  }
  if(!topCount){
    return null;
  }
  return {
    value: topValue,
    count: topCount,
    average: totalEntries ? count / totalEntries : null
  };
}

function computeShowAnalytics(show = {}){
  const entries = normalizeEntryList(show);
  const totalEntries = entries.length;
  let launchedCount = 0;
  let issueEntryCount = 0;
  const primaryCounts = new Map();
  const subIssueCounts = new Map();

  for(const entry of entries){
    if(String(entry?.launched || '').trim().toLowerCase() === 'yes'){
      launchedCount += 1;
    }
    const primary = typeof entry?.primaryIssue === 'string' ? entry.primaryIssue.trim() : '';
    if(primary){
      issueEntryCount += 1;
      primaryCounts.set(primary, (primaryCounts.get(primary) || 0) + 1);
    }
    const subIssue = typeof entry?.subIssue === 'string' ? entry.subIssue.trim() : '';
    const otherDetail = typeof entry?.otherDetail === 'string' ? entry.otherDetail.trim() : '';
    const detail = subIssue || otherDetail;
    if(detail){
      subIssueCounts.set(detail, (subIssueCounts.get(detail) || 0) + 1);
    }
  }

  const averageLaunches = totalEntries ? launchedCount / totalEntries : null;
  const averageIssues = totalEntries ? issueEntryCount / totalEntries : null;

  return {
    totalEntries,
    launchedCount,
    issueEntryCount,
    averageLaunches,
    averageIssues,
    topPrimaryIssue: pickTopValue(primaryCounts, totalEntries),
    topSubIssue: pickTopValue(subIssueCounts, totalEntries)
  };
}

function roundAverage(value){
  if(value === null || value === undefined){
    return null;
  }
  if(!Number.isFinite(value)){
    return null;
  }
  return Number(value.toFixed(2));
}

function buildSummaryFromAnalytics(analytics = null){
  const data = analytics || {totalEntries: 0};
  const primary = data.topPrimaryIssue || null;
  const sub = data.topSubIssue || null;
  return {
    totalEntries: data.totalEntries || 0,
    averageLaunches: roundAverage(data.averageLaunches ?? null),
    averageIssues: roundAverage(data.averageIssues ?? null),
    primaryIssue: primary?.value || '',
    primaryIssueCount: primary?.count ?? 0,
    primaryIssueAverage: roundAverage(primary?.average ?? null),
    subIssue: sub?.value || '',
    subIssueCount: sub?.count ?? 0,
    subIssueAverage: roundAverage(sub?.average ?? null)
  };
}

function buildMessagePayload(show = {}, entry = null, analytics = null){
  const normalizedShow = {
    ...show,
    entries: normalizeEntryList(show)
  };
  const computed = analytics || computeShowAnalytics(normalizedShow);
  const summary = buildSummaryFromAnalytics(computed);
  const showSummary = buildShowSummary(normalizedShow);

  if(entry){
    return {
      show: showSummary,
      entry: buildEntrySnapshot(entry),
      summary
    };
  }

  return {
    show: showSummary,
    summary,
    entries: normalizedShow.entries.map(buildEntrySnapshot)
  };
}

function csvEscape(value){
  const str = value === null || value === undefined ? '' : String(value);
  if(str.includes('"') || str.includes(',') || /[\n\r]/.test(str)){
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function buildCsvRow(rowObject){
  return EXPORT_COLUMNS.map(column => csvEscape(rowObject[column] ?? '')).join(',');
}

function buildRequestHeaders(){
  const headers = {'Content-Type': 'application/json'};
  const customAuthHeader = Array.isArray(activeConfig.headers)
    ? activeConfig.headers.find(header => header?.name && header.name.toLowerCase() === 'authorization')
    : null;
  if(activeConfig.secret && !customAuthHeader){
    headers.Authorization = `Bearer ${activeConfig.secret}`;
  }
  if(Array.isArray(activeConfig.headers)){
    activeConfig.headers.forEach(({name, value})=>{
      if(name){
        headers[name] = value;
      }
    });
  }
  return headers;
}

async function sendWebhookPayload(payload, meta = {}){
  const started = Date.now();
  const eventName = meta.event || payload?.event || 'unknown';
  try{
    const response = await axios({
      method: activeConfig.method || 'POST',
      url: activeConfig.url,
      data: payload,
      headers: buildRequestHeaders(),
      timeout: activeConfig.timeoutMs,
      validateStatus: () => true
    });
    const duration = Date.now() - started;
    const status = response.status || 0;
    if(status >= 200 && status < 400){
      console.info(`[webhook] Dispatched ${eventName} payload (status=${status}, ${duration}ms).`);
      updateVerificationState({
        status: 'ok',
        handshakeMethod: verificationState.handshakeMethod,
        httpStatus: status,
        durationMs: duration,
        error: null,
        errorCode: null
      });
      return {success: true, status, durationMs: duration};
    }
    const detail = `HTTP ${status}`;
    console.warn(`[webhook] Dispatch ${eventName} returned ${detail} after ${duration}ms.`);
    return {success: false, status, error: detail, durationMs: duration};
  }catch(error){
    const duration = Date.now() - started;
    const status = error?.response?.status ?? null;
    const code = error?.code || null;
    const message = status ? `HTTP ${status}` : (code || error.message || 'Webhook dispatch failed');
    console.warn(`[webhook] Dispatch ${eventName} failed after ${duration}ms: ${message}`);
    updateVerificationState({
      status: 'error',
      handshakeMethod: verificationState.handshakeMethod,
      httpStatus: status,
      durationMs: duration,
      error: error.message,
      errorCode: code
    });
    return {success: false, error: error.message, status, durationMs: duration, errorCode: code};
  }
}

async function dispatchEntryEvent(event, show, entry){
  if(!activeConfig.enabled || !activeConfig.url){
    const reason = !activeConfig.enabled ? 'disabled in configuration' : 'missing URL';
    if(lastSkipReason !== reason){
      console.info(`[webhook] Skipping ${event} dispatch because webhook is ${reason}.`);
      lastSkipReason = reason;
    }
    updateVerificationState({
      status: 'disabled',
      error: `Webhook ${reason}`,
      handshakeMethod: null,
      httpStatus: null,
      durationMs: null
    });
    return {skipped: true};
  }
  const baseEntries = normalizeEntryList(show);
  const normalizedShow = {
    ...show,
    crew: Array.isArray(show?.crew) ? show.crew : [],
    entries: baseEntries
  };
  const includesEntry = entry
    ? baseEntries.some(item => item?.id && item.id === entry.id)
    : false;
  const analyticsEntries = entry && !includesEntry
    ? normalizeEntryList({...normalizedShow, entries: [...baseEntries, entry]})
    : baseEntries;
  const analyticsShow = {
    ...normalizedShow,
    entries: analyticsEntries
  };
  const rowObject = buildTableRow(normalizedShow, entry);
  const analytics = computeShowAnalytics(analyticsShow);
  const summary = buildSummaryFromAnalytics(analytics);
  const message = buildMessagePayload(analyticsShow, entry, analytics);
  const showSummary = buildShowSummary(normalizedShow);
  const entrySnapshot = buildEntrySnapshot(entry);
  const payload = {
    event,
    schemaVersion: 3,
    dispatchedAt: new Date().toISOString(),
    target: {
      url: activeConfig.url,
      method: activeConfig.method
    },
    summary,
    table: {
      columns: EXPORT_COLUMNS,
      row: EXPORT_COLUMNS.map(column => rowObject[column] ?? '')
    },
    csv: {
      header: EXPORT_COLUMNS,
      row: buildCsvRow(rowObject)
    },
    message,
    show: showSummary,
    entry: entrySnapshot
  };

  return sendWebhookPayload(payload, {event, kind: 'entry'});
}

function normalizeEntryList(show){
  if(!show){
    return [];
  }
  return Array.isArray(show.entries)
    ? show.entries.map(entry => ({
      ...entry,
      actions: Array.isArray(entry?.actions) ? entry.actions : []
    }))
    : [];
}

function buildShowSummary(show = {}){
  return {
    label: show.label || '',
    date: show.date || '',
    time: show.time || '',
    leadPilot: show.leadPilot || '',
    monkeyLead: show.monkeyLead || '',
    createdAt: show.createdAt ?? null,
    updatedAt: show.updatedAt ?? null,
    archivedAt: show.archivedAt ?? null,
    deletedAt: show.deletedAt ?? null
  };
}

function normalizeMeta(meta){
  if(!isObject(meta)){
    return null;
  }
  const clone = {...meta};
  return Object.keys(clone).length ? clone : null;
}

async function dispatchShowEvent(event, show, meta){
  if(!activeConfig.enabled || !activeConfig.url){
    const reason = !activeConfig.enabled ? 'disabled in configuration' : 'missing URL';
    if(lastSkipReason !== reason){
      console.info(`[webhook] Skipping ${event} dispatch because webhook is ${reason}.`);
      lastSkipReason = reason;
    }
    updateVerificationState({
      status: 'disabled',
      error: `Webhook ${reason}`,
      handshakeMethod: null,
      httpStatus: null,
      durationMs: null
    });
    return {skipped: true};
  }
  const normalizedShow = {
    ...show,
    crew: Array.isArray(show?.crew) ? show.crew : [],
    entries: normalizeEntryList(show)
  };
  const showSummary = buildShowSummary(normalizedShow);
  const tableRows = normalizedShow.entries.map(entry => buildTableRow(normalizedShow, entry));
  const analytics = computeShowAnalytics(normalizedShow);
  const summary = buildSummaryFromAnalytics(analytics);
  const message = buildMessagePayload(normalizedShow, null, analytics);
  const sanitizedEntries = message.entries || [];
  const payload = {
    event,
    schemaVersion: 3,
    dispatchedAt: new Date().toISOString(),
    target: {
      url: activeConfig.url,
      method: activeConfig.method
    },
    summary,
    table: {
      columns: EXPORT_COLUMNS,
      rows: tableRows.map(row => EXPORT_COLUMNS.map(column => row[column] ?? ''))
    },
    csv: {
      header: EXPORT_COLUMNS,
      rows: tableRows.map(row => buildCsvRow(row))
    },
    message,
    show: showSummary,
    entries: sanitizedEntries
  };
  const normalizedMeta = normalizeMeta(meta);
  if(normalizedMeta){
    payload.meta = normalizedMeta;
  }
  return sendWebhookPayload(payload, {event, kind: 'show'});
}

module.exports = {
  setWebhookConfig,
  verifyWebhookConnection,
  getWebhookStatus,
  dispatchEntryEvent,
  dispatchShowEvent,
  buildTableRow,
  buildCsvRow,
  buildMessagePayload,
  buildSummaryFromAnalytics,
  computeShowAnalytics,
  buildEntrySnapshot,
  EXPORT_COLUMNS
};
