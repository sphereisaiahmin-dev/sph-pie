const axios = require('axios');

const DEFAULT_WEBHOOK_CONFIG = {
  enabled: false,
  url: '',
  method: 'POST',
  secret: '',
  headers: [],
  timeoutMs: 8000
};

const EXPORT_COLUMNS = [
  'showId','showDate','showTime','showLabel','crew','leadPilot','monkeyLead','showNotes',
  'entryId','unitId','planned','launched','status','primaryIssue','subIssue','otherDetail',
  'severity','rootCause','actions','operator','batteryId','delaySec','commandRx','notes'
];

let activeConfig = {...DEFAULT_WEBHOOK_CONFIG};

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

function setWebhookConfig(config = {}){
  const normalized = {
    ...DEFAULT_WEBHOOK_CONFIG,
    ...config
  };
  normalized.method = String(normalized.method || 'POST').toUpperCase();
  normalized.headers = normalizeHeaderList(normalized.headers);
  activeConfig = normalized;
}

function getWebhookStatus(){
  return {
    enabled: Boolean(activeConfig.enabled && activeConfig.url),
    method: activeConfig.method,
    hasSecret: Boolean(activeConfig.secret),
    headerCount: Array.isArray(activeConfig.headers) ? activeConfig.headers.length : 0
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

function buildMessagePayload(rowObject = {}){
  return EXPORT_COLUMNS.reduce((acc, column)=>{
    const value = rowObject[column];
    acc[column] = value === undefined || value === null ? '' : value;
    return acc;
  }, {});
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

async function dispatchEntryEvent(event, show, entry){
  if(!activeConfig.enabled || !activeConfig.url){
    return {skipped: true};
  }
  const rowObject = buildTableRow(show, entry);
  const message = buildMessagePayload(rowObject);
  const payload = {
    event,
    schemaVersion: 2,
    dispatchedAt: new Date().toISOString(),
    target: {
      url: activeConfig.url,
      method: activeConfig.method
    },
    table: {
      columns: EXPORT_COLUMNS,
      row: EXPORT_COLUMNS.map(column => rowObject[column] ?? '')
    },
    csv: {
      header: EXPORT_COLUMNS,
      row: buildCsvRow(rowObject)
    },
    message,
    show: {
      id: show?.id || '',
      label: show?.label || '',
      date: show?.date || '',
      time: show?.time || '',
      crew: Array.isArray(show?.crew) ? show.crew : []
    },
    entry: {
      ...entry,
      actions: Array.isArray(entry?.actions) ? entry.actions : []
    }
  };

  const headers = {'Content-Type': 'application/json'};
  if(activeConfig.secret){
    headers['X-Drone-Webhook-Secret'] = activeConfig.secret;
  }
  if(Array.isArray(activeConfig.headers)){
    activeConfig.headers.forEach(({name, value})=>{
      if(name){
        headers[name] = value;
      }
    });
  }

  try{
    const response = await axios({
      method: activeConfig.method || 'POST',
      url: activeConfig.url,
      data: payload,
      headers,
      timeout: activeConfig.timeoutMs
    });
    return {success: true, status: response.status};
  }catch(error){
    console.warn('Webhook dispatch failed', error.message);
    return {success: false, error: error.message};
  }
}

module.exports = {
  setWebhookConfig,
  getWebhookStatus,
  dispatchEntryEvent,
  buildTableRow,
  buildCsvRow,
  buildMessagePayload,
  EXPORT_COLUMNS
};
