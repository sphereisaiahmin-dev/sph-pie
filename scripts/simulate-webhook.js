const http = require('http');
const {
  setWebhookConfig,
  dispatchEntryEvent,
  EXPORT_COLUMNS,
  buildTableRow,
  buildMessagePayload
} = require('../server/webhookDispatcher');

async function run(){
  const port = 4101;
  let capturedPayload = null;

  const server = http.createServer((req, res)=>{
    if(req.method === 'POST' && req.url === '/hooks'){
      let body = '';
      req.on('data', chunk=>{ body += chunk; });
      req.on('end', ()=>{
        try{
          capturedPayload = JSON.parse(body || '{}');
        }catch(err){
          capturedPayload = {error: err.message, raw: body};
        }
        res.writeHead(200, {'Content-Type': 'application/json'});
        res.end(JSON.stringify({ok: true}));
      });
    }else{
      res.writeHead(404);
      res.end();
    }
  });

  await new Promise(resolve=> server.listen(port, '127.0.0.1', resolve));

  setWebhookConfig({
    enabled: true,
    url: `http://127.0.0.1:${port}/hooks`,
    method: 'POST',
    headers: [{name: 'X-Test-Webhook', value: 'yes'}]
  });

  const show = {
    id: 'simulation-show',
    date: '2024-07-04',
    time: '21:00',
    label: 'Independence Demo',
    crew: ['Alex', 'Nazar'],
    leadPilot: 'Alex',
    monkeyLead: 'Nazar',
    notes: 'Verification run'
  };

  const entry = {
    id: 'entry-001',
    unitId: 'Drone-01',
    planned: 'Yes',
    launched: 'Yes',
    status: 'Completed',
    actions: ['Logged only'],
    operator: 'Alex',
    batteryId: 'B-12',
    delaySec: 0,
    commandRx: 'Yes',
    notes: 'Green across the board'
  };

  const result = await dispatchEntryEvent('entry.test', show, entry);
  await new Promise(resolve => setTimeout(resolve, 200));
  await new Promise(resolve => server.close(resolve));

  if(!capturedPayload){
    throw new Error('Webhook simulation failed: no payload received');
  }

  const expectedRowMap = buildTableRow(show, entry);
  const expectedRow = EXPORT_COLUMNS.map(column => expectedRowMap[column] ?? '');
  const actualRow = (capturedPayload.table && capturedPayload.table.row) || [];

  const matches = JSON.stringify(actualRow) === JSON.stringify(expectedRow);
  if(!matches){
    throw new Error('Webhook table row does not match CSV export order');
  }

  const expectedMessage = buildMessagePayload(expectedRowMap);
  const actualMessage = capturedPayload.message || {};
  if(JSON.stringify(actualMessage) !== JSON.stringify(expectedMessage)){
    throw new Error('Webhook message payload does not mirror expected column mapping');
  }

  if(capturedPayload.csv && capturedPayload.csv.header){
    const headerMatches = JSON.stringify(capturedPayload.csv.header) === JSON.stringify(EXPORT_COLUMNS);
    if(!headerMatches){
      throw new Error('CSV header in webhook payload differs from expected columns');
    }
  }

  console.log('Webhook simulation succeeded.', {
    status: result.status || 'unknown',
    method: capturedPayload.target?.method,
    columns: capturedPayload.table?.columns?.length
  });
}

run().catch(err=>{
  console.error(err.message);
  process.exit(1);
});
