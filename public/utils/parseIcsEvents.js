function unfoldLines(input){
  const raw = typeof input === 'string' ? input.replace(/\r\n/g, '\n').replace(/\r/g, '\n') : '';
  const lines = raw.split('\n');
  const unfolded = [];
  for(const line of lines){
    if(!line){
      unfolded.push('');
      continue;
    }
    if(line.startsWith(' ') || line.startsWith('\t')){
      const previous = unfolded.length ? unfolded[unfolded.length - 1] : '';
      unfolded[unfolded.length - 1] = previous + line.slice(1);
    }else{
      unfolded.push(line);
    }
  }
  return unfolded;
}

function decodeIcsText(text){
  if(typeof text !== 'string'){ return ''; }
  return text
    .replace(/\\n/g, '\n')
    .replace(/\\, /g, ', ')
    .replace(/\\,/g, ',')
    .replace(/\\;/g, ';')
    .replace(/\\\\/g, '\\')
    .trim();
}

function readFirst(record, key){
  const entry = readEntry(record, key);
  return entry ? decodeIcsText(entry.value) : '';
}

function readEntry(record, key){
  if(!record){ return null; }
  const normalized = key.toUpperCase();
  const list = record[normalized];
  if(!Array.isArray(list) || !list.length){
    return null;
  }
  return list[0];
}

function isDateOnly(entry){
  if(!entry || !entry.value){ return false; }
  if(entry.params && entry.params.VALUE === 'DATE'){ return true; }
  return /^\d{8}$/.test(entry.value.trim());
}

function parseIcsDate(entry){
  if(!entry || !entry.value){ return null; }
  const raw = entry.value.trim();
  if(!raw){ return null; }
  const dateOnly = isDateOnly(entry);
  const match = raw.match(/^(\d{4})(\d{2})(\d{2})(?:T(\d{2})(\d{2})(\d{2})(Z)?)?$/);
  if(match){
    const year = Number(match[1]);
    const month = Number(match[2]) - 1;
    const day = Number(match[3]);
    if(match[4] == null || dateOnly){
      return new Date(year, month, day);
    }
    const hour = Number(match[4] || 0);
    const minute = Number(match[5] || 0);
    const second = Number(match[6] || 0);
    if(match[7] === 'Z'){
      return new Date(Date.UTC(year, month, day, hour, minute, second));
    }
    return new Date(year, month, day, hour, minute, second);
  }
  const fallback = new Date(raw);
  return Number.isNaN(fallback.getTime()) ? null : fallback;
}

function toDate(date){
  return date instanceof Date ? new Date(date.getTime()) : null;
}

function normalizeDescription(value){
  if(!value){ return ''; }
  return value.replace(/\n{3,}/g, '\n\n').trim();
}

export function parseIcsEvents(icsText){
  const lines = unfoldLines(icsText || '');
  const events = [];
  let current = null;

  for(const rawLine of lines){
    const line = rawLine ? rawLine.trimEnd() : '';
    if(line === 'BEGIN:VEVENT'){
      current = {};
      continue;
    }
    if(line === 'END:VEVENT'){
      if(current){
        const event = buildEvent(current);
        if(event){
          events.push(event);
        }
      }
      current = null;
      continue;
    }
    if(!current || !line){
      continue;
    }
    const colonIndex = line.indexOf(':');
    if(colonIndex === -1){
      continue;
    }
    const rawKey = line.slice(0, colonIndex);
    const value = line.slice(colonIndex + 1);
    const segments = rawKey.split(';');
    const key = segments[0].trim().toUpperCase();
    const params = {};
    for(let i=1;i<segments.length;i+=1){
      const [paramKey, paramValue] = segments[i].split('=');
      if(paramKey){
        params[paramKey.trim().toUpperCase()] = (paramValue || '').trim();
      }
    }
    if(!current[key]){
      current[key] = [];
    }
    current[key].push({ value, params });
  }

  return events;
}

function buildEvent(record){
  const startEntry = readEntry(record, 'DTSTART');
  const endEntry = readEntry(record, 'DTEND');
  const start = parseIcsDate(startEntry);
  let end = parseIcsDate(endEntry);
  if(!start){
    return null;
  }
  const allDay = isDateOnly(startEntry);
  if(!end){
    end = allDay ? new Date(start.getFullYear(), start.getMonth(), start.getDate() + 1) : new Date(start.getTime() + 60 * 60 * 1000);
  }
  const summary = readFirst(record, 'SUMMARY');
  const description = normalizeDescription(readFirst(record, 'DESCRIPTION'));
  const location = readFirst(record, 'LOCATION');
  const uid = readFirst(record, 'UID') || `${start.getTime()}-${summary || 'event'}`;
  const rrule = readFirst(record, 'RRULE');
  return {
    id: uid,
    title: summary || 'Untitled event',
    description,
    location,
    start: toDate(start),
    end: toDate(end),
    allDay,
    recurrence: rrule || ''
  };
}

export default parseIcsEvents;
