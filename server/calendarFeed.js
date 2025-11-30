const ical = require('node-ical');

const COLOR_MAP = {
  woz: '#22c55e',
  eagles: '#3b82f6',
  zac: '#ef4444',
  bsb: '#a855f7',
  illenium: '#f59e0b'
};

const SPECIAL_TITLE_COLORS = [
  {match: 'zac brown band: love and fear', color: '#ef4444', label: 'ZAC'}
];

function parseCalendarMetadata(summary = ''){
  const normalizedTitle = summary.toLowerCase();
  const special = SPECIAL_TITLE_COLORS.find(entry => normalizedTitle.includes(entry.match));
  const firstWordMatch = summary.match(/^([A-Za-z]+)/);
  const eventName = (special?.label)
    ? special.label
    : (firstWordMatch ? firstWordMatch[1].toUpperCase() : '');
  const numberMatch = summary.match(/#\s*(\d+)/);
  const fallbackNumberMatch = !numberMatch ? summary.match(/\b(\d+)\b/) : null;
  const showNumber = numberMatch
    ? Number(numberMatch[1])
    : (fallbackNumberMatch ? Number(fallbackNumberMatch[1]) : null);
  const color = (special?.color)
    || COLOR_MAP[eventName.toLowerCase()]
    || '';
  return {eventName, showNumber, color};
}

function getCalendarCutoffTimestamp(monthsBack = 2){
  const now = new Date();
  now.setHours(0, 0, 0, 0);
  now.setMonth(now.getMonth() - monthsBack);
  return now.getTime();
}

async function fetchCalendarFeed(feedUrl){
  if(!feedUrl || typeof feedUrl !== 'string'){
    return [];
  }
  let data;
  try{
    data = await ical.async.fromURL(feedUrl);
  }catch(err){
    console.error('[calendarFeed] Failed to fetch calendar feed', err);
    return [];
  }
  const events = [];
  Object.values(data || {}).forEach(entry => {
    if(!entry || entry.type !== 'VEVENT'){
      return;
    }
    const start = entry.start instanceof Date ? entry.start : null;
    const end = entry.end instanceof Date ? entry.end : null;
    if(!start){
      return;
    }
    const id = typeof entry.uid === 'string' && entry.uid
      ? entry.uid
      : (typeof entry.id === 'string' && entry.id ? entry.id : `${entry.summary || 'event'}-${start.getTime()}`);
    const allDay = entry.datetype === 'date' || (start.getUTCHours?.() === 0 && start.getUTCMinutes?.() === 0 && (!end || end.getUTCHours?.() === 0));
    const meta = parseCalendarMetadata(typeof entry.summary === 'string' ? entry.summary : '');
    events.push({
      id,
      title: typeof entry.summary === 'string' ? entry.summary : 'Untitled event',
      description: typeof entry.description === 'string' ? entry.description : '',
      location: typeof entry.location === 'string' ? entry.location : '',
      start: start.toISOString(),
      end: end instanceof Date ? end.toISOString() : '',
      startTs: start.getTime(),
      endTs: end instanceof Date ? end.getTime() : null,
      allDay,
      eventName: meta.eventName,
      showNumber: meta.showNumber,
      color: meta.color
    });
  });
  return events;
}

module.exports = {
  fetchCalendarFeed,
  getCalendarCutoffTimestamp
};
