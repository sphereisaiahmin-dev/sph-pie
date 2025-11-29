const ical = require('node-ical');

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
    events.push({
      id,
      title: typeof entry.summary === 'string' ? entry.summary : 'Untitled event',
      description: typeof entry.description === 'string' ? entry.description : '',
      location: typeof entry.location === 'string' ? entry.location : '',
      start: start.toISOString(),
      end: end instanceof Date ? end.toISOString() : '',
      startTs: start.getTime(),
      endTs: end instanceof Date ? end.getTime() : null,
      allDay
    });
  });
  return events;
}

module.exports = {
  fetchCalendarFeed,
  getCalendarCutoffTimestamp
};
