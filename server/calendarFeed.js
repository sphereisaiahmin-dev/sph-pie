const axios = require('axios');
const ical = require('node-ical');
const { HttpsProxyAgent } = require('https-proxy-agent');

const { DISCIPLINES } = require('./disciplineConfig');

const CALENDAR_FEED_URL = process.env.CALENDAR_FEED_URL
  || process.env.TEAMUP_CALENDAR_URL
  || 'https://ics.teamup.com/feed/8orye2s63sbb3virutab5ny6beycko/12007214.ics';

const PROXY_URL = process.env.HTTPS_PROXY || process.env.https_proxy || process.env.HTTP_PROXY || process.env.http_proxy || null;

const DISCIPLINE_NAME_MAP = buildDisciplineLookup();

function buildDisciplineLookup(){
  const map = new Map();
  DISCIPLINES.forEach(discipline => {
    const id = String(discipline.id || '').trim().toLowerCase();
    const name = String(discipline.name || '').trim().toLowerCase();
    if(id){
      map.set(id, id);
    }
    if(name){
      map.set(name, id || name);
    }
  });
  return map;
}

function createHttpConfig(){
  const config = {
    responseType: 'text',
    timeout: 15000,
    maxRedirects: 5
  };
  if(PROXY_URL){
    const agent = new HttpsProxyAgent(PROXY_URL);
    config.httpAgent = agent;
    config.httpsAgent = agent;
    config.proxy = false;
  }
  return config;
}

function normalizeString(value){
  return typeof value === 'string' ? value.trim() : '';
}

function toTimestamp(value){
  if(value instanceof Date){
    const time = value.getTime();
    return Number.isFinite(time) ? time : null;
  }
  if(typeof value === 'number' && Number.isFinite(value)){
    return value;
  }
  if(typeof value === 'string' && value){
    const parsed = Date.parse(value);
    if(Number.isFinite(parsed)){
      return parsed;
    }
  }
  return null;
}

function resolveDisciplineId(event){
  if(!event){
    return null;
  }
  const categories = Array.isArray(event.categories)
    ? event.categories
    : typeof event.categories === 'string'
      ? event.categories.split(',')
      : [];
  for(const rawCategory of categories){
    const category = normalizeString(rawCategory).toLowerCase();
    if(!category){
      continue;
    }
    if(DISCIPLINE_NAME_MAP.has(category)){
      return DISCIPLINE_NAME_MAP.get(category) || null;
    }
  }
  const summary = normalizeString(event.summary).toLowerCase();
  if(summary){
    for(const [key, value] of DISCIPLINE_NAME_MAP.entries()){
      if(!key || !value){
        continue;
      }
      if(summary.includes(key)){
        return value;
      }
    }
  }
  return null;
}

function normalizeAttachments(raw){
  if(!raw){
    return [];
  }
  const list = Array.isArray(raw) ? raw : [raw];
  const attachments = [];
  list.forEach(item => {
    if(!item){
      return;
    }
    if(typeof item === 'string'){
      const url = item.trim();
      if(url){
        attachments.push({url});
      }
      return;
    }
    if(typeof item === 'object'){
      const url = normalizeString(item.val || item.url || '');
      if(url){
        attachments.push({
          url,
          mimeType: normalizeString(item.params?.FMTTYPE || item.params?.['FMTTYPE']),
          label: normalizeString(item.params?.LABEL || item.params?.['X-LABEL'] || '')
        });
      }
    }
  });
  return attachments;
}

function normalizeCalendarEvent(raw){
  if(!raw || typeof raw !== 'object'){
    return null;
  }
  const uid = normalizeString(raw.uid || raw.id);
  if(!uid){
    return null;
  }
  const startTs = toTimestamp(raw.start);
  const endTs = toTimestamp(raw.end);
  const description = normalizeString(raw.description || raw.summary || '');
  const urlValue = typeof raw.url === 'string' ? raw.url : raw.url?.val;
  const allDay = Boolean(raw.start?.dateOnly || raw.datetype === 'date' || String(raw['MICROSOFT-CDO-ALLDAYEVENT']).toUpperCase() === 'TRUE');
  const createdAt = toTimestamp(raw.created);
  const updatedAt = toTimestamp(raw.lastmodified || raw.dtstamp);
  const disciplineId = resolveDisciplineId(raw);
  return {
    id: uid,
    uid,
    title: normalizeString(raw.summary) || 'Untitled event',
    description,
    location: normalizeString(raw.location || ''),
    url: normalizeString(urlValue || ''),
    start: Number.isFinite(startTs) ? new Date(startTs).toISOString() : null,
    end: Number.isFinite(endTs) ? new Date(endTs).toISOString() : null,
    startTs: Number.isFinite(startTs) ? startTs : null,
    endTs: Number.isFinite(endTs) ? endTs : null,
    allDay,
    disciplineId,
    category: normalizeString(Array.isArray(raw.categories) && raw.categories.length ? raw.categories[0] : ''),
    who: normalizeString(raw['TEAMUP-WHO'] || raw.who || ''),
    attachments: normalizeAttachments(raw.attach),
    createdAt,
    updatedAt
  };
}

async function fetchCalendarEvents(options = {}){
  const url = normalizeString(options.url || '') || CALENDAR_FEED_URL;
  if(!url){
    throw new Error('Calendar feed URL not configured');
  }
  const httpConfig = createHttpConfig();
  const response = await axios.get(url, httpConfig);
  const parsed = await ical.async.parseICS(response.data);
  const events = [];
  Object.values(parsed || {}).forEach(value => {
    if(!value || value.type !== 'VEVENT'){
      return;
    }
    const event = normalizeCalendarEvent(value);
    if(event){
      events.push(event);
    }
  });
  events.sort((a, b) => {
    const aTime = Number.isFinite(a.startTs) ? a.startTs : Number.isFinite(a.endTs) ? a.endTs : Infinity;
    const bTime = Number.isFinite(b.startTs) ? b.startTs : Number.isFinite(b.endTs) ? b.endTs : Infinity;
    return aTime - bTime;
  });
  return events;
}

module.exports = {
  CALENDAR_FEED_URL,
  fetchCalendarEvents
};
