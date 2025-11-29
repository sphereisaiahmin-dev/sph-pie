const ISSUE_MAP = {
  'Tracking lost': ['occlusion','calibration','marker loss','software','unknown'],
  'Failed to launch': ['mechanical','arming','safety','unknown'],
  'Command delay': ['network latency','controller queue','unknown'],
  'RF link': ['TX fault','RX fault','interference','antenna','unknown'],
  'Battery': ['low voltage','BMS fault','poor contact','swelling','unknown'],
  'Motor or prop': ['no spin','desync','damage','unknown'],
  'Sensor or IMU': ['bias','calibration','saturation','unknown'],
  'Software or show control': ['cue timing','state desync','crash','unknown'],
  'Operator input': ['incorrect mode','early abort','missed cue','unknown'],
  Other: []
};
const PRIMARY_ISSUES = Object.keys(ISSUE_MAP);
const ACTIONS = ['Reboot','Swap battery','Swap drone','Retry launch','Abort segment','Logged only'];
const STATUS = ['Completed','No-launch','Abort'];
const EXPORT_COLUMNS = [
  'showId','showDate','showTime','showLabel','crew','leadPilot','monkeyLead','showNotes',
  'entryId','unitId','planned','launched','status','primaryIssue','subIssue','otherDetail',
  'severity','rootCause','actions','operator','batteryId','delaySec','commandRx','notes'
];
const ARCHIVE_METRIC_DEFS = {
  entriesCount: {
    label: 'Entries logged',
    getValue: stats => stats.totalEntries,
    decimals: 0,
    min: 0,
    chartable: true
  },
  completedCount: {
    label: 'Completed flights',
    getValue: stats => stats.completedCount,
    decimals: 0
  },
  noLaunchCount: {
    label: 'No-launch events',
    getValue: stats => stats.noLaunchCount,
    decimals: 0
  },
  abortCount: {
    label: 'Abort events',
    getValue: stats => stats.abortCount,
    decimals: 0
  },
  avgDelaySec: {
    label: 'Average delay (s)',
    getValue: stats => stats.avgDelaySec,
    decimals: 1,
    min: 0,
    chartable: true,
    suffix: ' s'
  },
  maxDelaySec: {
    label: 'Max delay (s)',
    getValue: stats => stats.maxDelaySec,
    decimals: 1,
    min: 0,
    suffix: ' s'
  },
  completionRate: {
    label: 'Completion rate (%)',
    getValue: stats => stats.completionRate,
    decimals: 0,
    suffix: '%',
    min: 0,
    max: 100,
    chartable: true
  },
  launchRate: {
    label: 'Launch rate (%)',
    getValue: stats => stats.launchRate,
    decimals: 0,
    suffix: '%',
    min: 0,
    max: 100,
    chartable: true
  },
  abortRate: {
    label: 'Abort rate (%)',
    getValue: stats => stats.abortRate,
    decimals: 0,
    suffix: '%',
    min: 0,
    max: 100,
    chartable: true
  }
};
const ARCHIVE_SUMMARY_KEYS = [
  'entriesCount',
  'completedCount',
  'noLaunchCount',
  'abortCount',
  'avgDelaySec',
  'maxDelaySec',
  'launchRate',
  'completionRate'
];

const ISSUE_METRIC_PREFIX = 'issue:';
const issueMetricDefCache = new Map();

function createEmptyShowDraft(){
  return {
    date: '',
    time: '',
    label: '',
    leadPilot: '',
    monkeyLead: '',
    notes: '',
    disciplineId: getActiveDisciplineId()
  };
}

const state = {
  session: null,
  appReady: false,
  config: null,
  unitLabel: 'Drone',
  shows: [],
  currentShowId: null,
  currentView: 'discipline',
  editingEntryRef: null,
  serverHost: '10.241.211.120',
  serverPort: 3000,
  storageLabel: 'SQL.js storage v2',
  storageMeta: null,
  newShowDraft: null,
  showHeaderShowErrors: false,
  isCreatingShow: false,
  archivedShows: [],
  currentArchivedShowId: null,
  selectedArchiveChartShows: null,
  selectedArchiveMetrics: ['launchRate', 'avgDelaySec'],
  archiveChartFilters: {
    startDate: null,
    endDate: null,
    operator: null,
    discipline: ''
  },
  archiveSelectionMode: 'calendar',
  archiveDailyGroups: [],
  archiveDailyGroupsByKey: {},
  activeArchiveDayKey: null,
  calendarEvents: [],
  calendarMonth: null,
  activeCalendarDayKey: null,
  calendarLoaded: false,
  webhookConfig: {
    enabled: false,
    url: '',
    method: 'POST',
    secret: '',
    headersText: ''
  },
  webhookStatus: {
    enabled: false,
    method: 'POST',
    hasSecret: false,
    headerCount: 0
  },
  disciplines: [],
  roleLevels: ['lead', 'operator', 'crew'],
  defaultDisciplineId: null,
  selectedDisciplineId: null,
  staffDirectory: {},
  staff: {
    stagecrew: [],
    operators: [],
    leads: []
  },
  users: [],
  userFilters: {
    query: '',
    role: ''
  },
  defaultTempPassword: 'adminsphere1'
};

state.newShowDraft = createEmptyShowDraft();

const syncState = {
  channel: null,
  id: (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function')
    ? crypto.randomUUID()
    : `sync-${Date.now()}-${Math.random().toString(16).slice(2)}`
};

const ARCHIVE_CHART_COLORS = ['#16a34a', '#f97316', '#38bdf8', '#a855f7', '#facc15', '#f472b6', '#22d3ee'];
let archiveChartInstance = null;
let uiInitialized = false;

const SYNC_CHANNEL_NAME = 'pie-sync';
const IDLE_LOGOUT_MS = 5 * 60 * 1000;

const appTitle = el('appTitle');
const titleSubPrefix = el('titleSubPrefix');
const titleSubSuffix = el('titleSubSuffix');
const loginScreen = el('loginScreen');
const passwordResetScreen = el('passwordResetScreen');
const appShell = el('appShell');
const loginForm = el('loginForm');
const loginEmailInput = el('loginEmail');
const loginPasswordInput = el('loginPassword');
const loginError = el('loginError');
const passwordResetForm = el('passwordResetForm');
const resetCurrentInput = el('resetCurrent');
const resetNewInput = el('resetNew');
const resetConfirmInput = el('resetConfirm');
const passwordResetError = el('passwordResetError');
const passwordResetLogoutBtn = el('passwordResetLogout');
const sessionUserEl = el('sessionUser');
const sessionNameEl = el('sessionName');
const sessionRolesEl = el('sessionRoles');
const logoutBtn = el('logoutBtn');
const userDirectoryEl = el('userDirectory');
const userForm = el('userForm');
const disciplineView = el('disciplineView');
const disciplineList = el('disciplineList');
const workspaceView = el('workspaceView');
const workspaceMessage = el('workspaceMessage');
const workspaceList = el('workspaceList');
const disciplineTitle = el('disciplineTitle');
const landingTitle = el('landingTitle');
const landingSubtitle = el('landingSubtitle');
const workspaceTitle = el('workspaceTitle');
const userRoleGrid = el('userRoleGrid');
const userIdInput = el('userId');
const userNameInput = el('userName');
const userEmailInput = el('userEmail');
const userFormStatus = el('userFormStatus');
const userFormCancelBtn = el('userFormCancel');
const userFormSubmitBtn = el('userFormSubmit');
const newUserBtn = el('newUserBtn');
const userSearchInput = el('userSearch');
const userRoleFilter = el('userRoleFilter');
const userModal = el('userModal');
const closeUserModalBtn = el('closeUserModal');
const userModalTitle = el('userModalTitle');
const unitLabelEl = el('unitLabel');
const showDate = el('showDate');
const showTime = el('showTime');
const showLabel = el('showLabel');
const showNotes = el('showNotes');
const showCrewSelect = el('showCrew');
const leadPilotSelect = el('leadPilot');
const monkeyLeadSelect = el('monkeyLead');
const newShowBtn = el('newShow');
const unitId = el('unitId');
const planned = el('planned');
const launched = el('launched');
const stCompleted = el('stCompleted');
const stNoLaunch = el('stNoLaunch');
const stAbort = el('stAbort');
const primaryIssue = el('primaryIssue');
const subIssue = el('subIssue');
const otherDetail = el('otherDetail');
const otherDetailWrap = el('otherDetailWrap');
const severity = el('severity');
const rootCause = el('rootCause');
const actionsChips = el('actionsChips');
const operator = el('operator');
const batteryId = el('batteryId');
const delaySec = el('delaySec');
const commandRx = el('commandRx');
const entryNotes = el('entryNotes');
const addLineBtn = el('addLine');
const operatorDisplay = el('operatorDisplay');
const operatorEntryNotice = el('operatorEntryNotice');
const groupsContainer = el('groups');
const issueBlocks = qsa('.issue-block');
const toastEl = el('toast');
const editModal = el('editModal');
const editForm = el('editForm');
const configBtn = el('configBtn');
const configPanel = el('configPanel');
const cancelConfigBtn = el('cancelConfig');
const configForm = el('configForm');
const configMessage = el('configMessage');
const menuUserName = el('menuUserName');
const menuUserEmail = el('menuUserEmail');
const menuUserRoles = el('menuUserRoles');
const menuDateTime = el('menuDateTime');
const configNavButtons = qsa('[data-config-target]');
const adminWorkspaceNavBtn = el('adminWorkspaceNav');
const configSections = qsa('[data-config-section]');
const unitLabelSelect = el('unitLabelSelect');
const webhookEnabled = el('webhookEnabled');
const webhookUrl = el('webhookUrl');
const webhookMethod = el('webhookMethod');
const webhookSecret = el('webhookSecret');
const webhookHeaders = el('webhookHeaders');
const webhookPreview = el('webhookPreview');
const webhookConfigureBtn = el('webhookConfigure');
const webhookSimulateBtn = el('webhookSimulateMonth');
const webhookModal = el('webhookModal');
const closeWebhookModalBtn = el('closeWebhookModal');
const webhookForm = el('webhookForm');
const webhookCancelBtn = el('webhookCancel');
const roleHomeBtn = el('roleHome');
const viewBadge = el('viewBadge');
const welcomeBanner = el('welcomeBanner');
const landingDisciplineShortcuts = el('landingDisciplineShortcuts');
const chooseLeadBtn = el('chooseLead');
const chooseOperatorBtn = el('chooseOperator');
const chooseArchiveBtn = el('chooseArchive');
const openCalendarBtn = el('openCalendar');
const entryShowSelect = el('entryShowSelect');
const operatorShowSummary = el('operatorShowSummary');
const archiveShowSelect = el('archiveShowSelect');
const archiveDetails = el('archiveDetails');
const archiveMeta = el('archiveMeta');
const archiveEmpty = el('archiveEmpty');
const archiveExportCsvBtn = el('archiveExportCsv');
const archiveExportJsonBtn = el('archiveExportJson');
const archiveStats = el('archiveStats');
const archiveMetricButtons = el('archiveMetricButtons');
const archiveIssueButtons = el('archiveIssueButtons');
const archiveModeControls = el('archiveModeControls');
let archiveStatShowSelect = el('archiveStatShowSelect');
let archiveShowFilterStart = el('archiveShowFilterStart');
let archiveShowFilterEnd = el('archiveShowFilterEnd');
let archiveSelectAllShowsBtn = el('archiveSelectAllShows');
let archiveClearShowSelectionBtn = el('archiveClearShowSelection');
const archiveOperatorFilter = el('archiveOperatorFilter');
const archiveModeCalendarBtn = el('archiveModeCalendar');
const archiveModeShowsBtn = el('archiveModeShows');
const archiveStatCanvas = el('archiveStatCanvas');
const archiveStatEmpty = el('archiveStatEmpty');
const archiveDayDetail = el('archiveDayDetail');
const archiveDayDetailTitle = el('archiveDayDetailTitle');
const archiveDayDetailContent = el('archiveDayDetailContent');
let archiveDayDetailCloseTimer = null;
const closeArchiveDayDetailBtn = el('closeArchiveDayDetail');
const refreshArchiveBtn = el('refreshArchive');
const archiveDisciplineFilter = el('archiveDisciplineFilter');
const connectionStatusEl = el('connectionStatus');
const providerBadge = el('providerBadge');
const webhookBadge = el('webhookBadge');
const refreshShowsBtn = el('refreshShows');
const lanAddressEl = el('lanAddress');
const calendarView = el('calendarView');
const calendarLayout = el('calendarLayout');
const calendarGrid = el('calendarGrid');
const calendarMonthLabel = el('calendarMonthLabel');
const calendarDayDetails = el('calendarDayDetails');
const calendarDayTitle = el('calendarDayTitle');
const calendarDaySubtitle = el('calendarDaySubtitle');
const calendarEventList = el('calendarEventList');
const calendarPrevBtn = el('calendarPrev');
const calendarNextBtn = el('calendarNext');
const calendarRefreshBtn = el('calendarRefresh');

let currentConfigSection = 'admin';
let webhookModalSnapshot = null;
let idleTimerId = null;
let idleListenersRegistered = false;
let unloadHandlerRegistered = false;
let suppressUnloadLogout = false;
let menuClockInterval = null;
const addLineBtnDefaultText = addLineBtn ? addLineBtn.textContent : 'Add line';

if(configPanel){
  configPanel.setAttribute('aria-hidden', 'true');
}

bootstrap().catch(err=>{
  console.error(err);
  toast('Failed to initialise application', true);
});

async function bootstrap(){
  initAuthUI();
  await refreshSession();
}

async function init(){
  await loadConfig();
  await loadDisciplineConfig();
  updateConnectionIndicator('loading');
  await loadStaff();
  if(isAdmin()){
    await loadUsers();
  }else{
    state.users = [];
  }
  await loadShows();
  state.calendarMonth = state.calendarMonth || getMonthStart(new Date());
  state.activeCalendarDayKey = state.activeCalendarDayKey || formatDayKey(new Date());
  initUI();
  setupSyncChannel();
  populateUnitOptions();
  populateIssues();
  renderActionsChips(actionsChips, []);
  setCurrentShow(state.currentShowId || null);
  setView('discipline');
  state.appReady = true;
}

async function ensureAppReady(){
  if(state.appReady){
    return;
  }
  await init();
  state.appReady = true;
}

function initAuthUI(){
  if(loginForm){
    loginForm.addEventListener('submit', onLoginSubmit);
  }
  if(passwordResetForm){
    passwordResetForm.addEventListener('submit', onPasswordResetSubmit);
  }
  if(passwordResetLogoutBtn){
    passwordResetLogoutBtn.addEventListener('click', ()=> logout());
  }
  if(logoutBtn){
    logoutBtn.addEventListener('click', ()=> logout());
  }
  if(userDirectoryEl){
    userDirectoryEl.addEventListener('click', onUserDirectoryClick);
  }
  if(userFormCancelBtn){
    userFormCancelBtn.addEventListener('click', event=>{
      event.preventDefault();
      resetUserForm();
    });
  }
  if(userFormSubmitBtn){
    userFormSubmitBtn.addEventListener('click', onUserFormSubmit);
  }
  if(userForm){
    userForm.addEventListener('keydown', handleUserFormKeydown);
  }
  if(disciplineList){
    disciplineList.addEventListener('click', onDisciplineListClick);
  }
  if(landingDisciplineShortcuts){
    landingDisciplineShortcuts.addEventListener('click', onDisciplineListClick);
  }
  setupIdleActivityTracking();
  setupUnloadLogoutHandler();
  startMenuClock();
}

async function refreshSession(){
  try{
    const data = await apiRequest('/api/auth/session', {method: 'GET', skipAuthHandlers: true});
    if(data?.authenticated){
      await handleSessionAuthenticated(data.user);
    }else{
      state.session = null;
      updateSessionUi();
      showLoginScreen();
    }
  }catch(err){
    console.error('Failed to fetch session', err);
    state.session = null;
    updateSessionUi();
    showLoginScreen();
  }
}

async function handleSessionAuthenticated(user){
  state.session = user || null;
  if(!isAdmin()){
    state.users = [];
  }
  updateSessionUi();
  resetIdleTimer();
  if(state.session?.needsPasswordReset){
    clearPasswordResetForm();
    showPasswordResetScreen();
    return;
  }
  await ensureAppReady();
  if(isAdmin() && !state.users.length){
    await loadUsers();
  }
  showAppShell();
}

function showLoginScreen(){
  clearIdleTimer();
  if(loginScreen){
    loginScreen.hidden = false;
  }
  if(passwordResetScreen){
    passwordResetScreen.hidden = true;
  }
  if(appShell){
    appShell.hidden = true;
  }
}

function showPasswordResetScreen(){
  if(loginScreen){
    loginScreen.hidden = true;
  }
  if(passwordResetScreen){
    passwordResetScreen.hidden = false;
  }
  if(appShell){
    appShell.hidden = true;
  }
}

function showAppShell(){
  if(loginScreen){
    loginScreen.hidden = true;
  }
  if(passwordResetScreen){
    passwordResetScreen.hidden = true;
  }
  if(appShell){
    appShell.hidden = false;
  }
}

function updateSessionUi(){
  if(sessionUserEl){
    if(state.session){
      sessionUserEl.hidden = false;
      sessionNameEl.textContent = state.session.name || state.session.email || 'Account';
      sessionRolesEl.textContent = formatRoleList(state.session.roles || []);
    }else{
      sessionUserEl.hidden = true;
    }
  }
  if(menuUserName){
    if(state.session){
      menuUserName.textContent = state.session.name || state.session.email || 'Account';
      if(menuUserEmail){
        menuUserEmail.textContent = state.session.email || '';
      }
      if(menuUserRoles){
        const rolesText = formatRoleList(state.session.roles || []);
        menuUserRoles.textContent = rolesText ? `Roles: ${rolesText}` : '';
      }
    }else{
      menuUserName.textContent = 'Not signed in';
      if(menuUserEmail){
        menuUserEmail.textContent = '';
      }
      if(menuUserRoles){
        menuUserRoles.textContent = '';
      }
    }
  }
  if(welcomeBanner){
    const firstName = getSessionFirstName();
    if(firstName){
      welcomeBanner.textContent = `Welcome, ${firstName}`;
      welcomeBanner.hidden = false;
    }else{
      welcomeBanner.hidden = true;
    }
  }
  if(configBtn){
    configBtn.hidden = false;
    configBtn.disabled = false;
  }
  if(cancelConfigBtn){
    cancelConfigBtn.disabled = false;
  }
  if(adminWorkspaceNavBtn){
    const admin = isAdmin();
    adminWorkspaceNavBtn.hidden = !admin;
    adminWorkspaceNavBtn.disabled = !admin;
  }
  if(isAdmin()){
    renderUserDirectory();
  }else if(userDirectoryEl){
    userDirectoryEl.innerHTML = '<p class="help">Admin access required.</p>';
  }
  updateWorkspaceAvailability();
  syncOperatorIdentity();
}

function formatRoleKey(role){
  if(typeof role !== 'string'){
    return '';
  }
  const normalized = role.trim().toLowerCase();
  if(!normalized){
    return '';
  }
  if(normalized === 'admin'){
    return 'Admin';
  }
  const [disciplineId, level] = normalized.split('.');
  if(!level){
    return normalized.charAt(0).toUpperCase() + normalized.slice(1);
  }
  const discipline = state.disciplines.find(entry => entry.id === disciplineId);
  const disciplineName = discipline ? discipline.name : (disciplineId ? disciplineId.charAt(0).toUpperCase() + disciplineId.slice(1) : '');
  const levelName = level.charAt(0).toUpperCase() + level.slice(1);
  return `${disciplineName} ${levelName}`.trim();
}

function formatRoleLevel(level){
  if(typeof level !== 'string'){
    return '';
  }
  const normalized = level.trim().toLowerCase();
  if(!normalized){
    return '';
  }
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function formatRoleList(roles = []){
  if(!Array.isArray(roles) || !roles.length){
    return '';
  }
  return roles.map(formatRoleKey).filter(Boolean).join(', ');
}

function getSessionFirstName(){
  const full = typeof state.session?.name === 'string' && state.session.name.trim()
    ? state.session.name.trim()
    : (typeof state.session?.email === 'string' ? state.session.email.trim() : '');
  if(!full){
    return '';
  }
  const [first] = full.split(/\s+/);
  return first || '';
}

function updateWorkspaceAvailability(){
  const hasWorkspaces = disciplineHasForms(getActiveDisciplineId());
  if(chooseLeadBtn){
    const allowed = hasWorkspaces && userHasRole('lead');
    chooseLeadBtn.disabled = !allowed;
    chooseLeadBtn.classList.toggle('is-disabled', !allowed);
  }
  if(chooseOperatorBtn){
    const allowed = hasWorkspaces && userHasRole('operator');
    chooseOperatorBtn.disabled = !allowed;
    chooseOperatorBtn.classList.toggle('is-disabled', !allowed);
  }
  if(chooseArchiveBtn){
    const allowed = hasWorkspaces && (userHasRole('lead') || userHasRole('operator') || userHasRole('crew'));
    chooseArchiveBtn.disabled = !allowed;
    chooseArchiveBtn.classList.toggle('is-disabled', !allowed);
  }
}

async function onLoginSubmit(event){
  event.preventDefault();
  const email = loginEmailInput?.value ? loginEmailInput.value.trim() : '';
  const password = loginPasswordInput?.value || '';
  if(!email || !password){
    if(loginError){
      loginError.textContent = 'Email and password are required';
      loginError.hidden = false;
    }
    return;
  }
  try{
    if(loginError){ loginError.hidden = true; }
    const result = await apiRequest('/api/auth/login', {
      method: 'POST',
      body: {email, password},
      skipAuthHandlers: true
    });
    if(result?.user){
      await handleSessionAuthenticated(result.user);
      loginPasswordInput.value = '';
    }
  }catch(err){
    console.error('Login failed', err);
    if(loginError){
      loginError.textContent = err.message || 'Login failed';
      loginError.hidden = false;
    }
  }
}

async function onPasswordResetSubmit(event){
  event.preventDefault();
  const currentPassword = resetCurrentInput?.value || '';
  const nextPassword = resetNewInput?.value || '';
  const confirmPassword = resetConfirmInput?.value || '';
  if(!currentPassword || !nextPassword){
    if(passwordResetError){
      passwordResetError.textContent = 'Enter your current and new password.';
      passwordResetError.hidden = false;
    }
    return;
  }
  if(nextPassword !== confirmPassword){
    if(passwordResetError){
      passwordResetError.textContent = 'Passwords do not match';
      passwordResetError.hidden = false;
    }
    return;
  }
  try{
    if(passwordResetError){ passwordResetError.hidden = true; }
    const result = await apiRequest('/api/auth/password', {
      method: 'POST',
      body: {currentPassword, newPassword: nextPassword}
    });
    if(result?.user){
      state.session = result.user;
      updateSessionUi();
      toast('Password updated');
      showAppShell();
      await ensureAppReady();
    }
  }catch(err){
    console.error('Password reset failed', err);
    if(passwordResetError){
      passwordResetError.textContent = err.message || 'Password update failed';
      passwordResetError.hidden = false;
    }
  }
}

async function logout(){
  suppressUnloadLogout = true;
  clearIdleTimer();
  try{
    await apiRequest('/api/auth/logout', {method: 'POST', skipAuthHandlers: true});
  }catch(err){
    console.error('Failed to log out', err);
  }finally{
    window.location.reload();
  }
}

function clearPasswordResetForm(){
  if(resetCurrentInput){ resetCurrentInput.value = ''; }
  if(resetNewInput){ resetNewInput.value = ''; }
  if(resetConfirmInput){ resetConfirmInput.value = ''; }
  if(passwordResetError){ passwordResetError.hidden = true; }
}

function handleSessionExpired(){
  clearIdleTimer();
  state.session = null;
  state.appReady = false;
  state.users = [];
  updateSessionUi();
  showLoginScreen();
}

function handlePasswordResetRequired(){
  if(state.session){
    state.session.needsPasswordReset = true;
  }
  showPasswordResetScreen();
}

function resolveRoleKey(role, disciplineId){
  if(typeof role !== 'string'){
    return '';
  }
  const normalizedRole = role.trim().toLowerCase();
  if(!normalizedRole){
    return '';
  }
  if(normalizedRole === 'admin'){
    return 'admin';
  }
  if(normalizedRole.includes('.')){
    return normalizedRole;
  }
  const targetDiscipline = disciplineId || getActiveDisciplineId();
  if(!targetDiscipline){
    return normalizedRole;
  }
  const mappedRole = normalizedRole === 'stagecrew' ? 'crew' : normalizedRole;
  return `${targetDiscipline}.${mappedRole}`;
}

function userHasRole(role, disciplineId){
  if(!role){
    return false;
  }
  const roles = Array.isArray(state.session?.roles) ? state.session.roles : [];
  if(roles.includes('admin')){
    return true;
  }
  const key = resolveRoleKey(role, disciplineId);
  if(!key){
    return false;
  }
  return roles.some(entry => typeof entry === 'string' && entry.trim().toLowerCase() === key);
}

function isAdmin(){
  const roles = Array.isArray(state.session?.roles) ? state.session.roles : [];
  return roles.includes('admin');
}

async function loadUsers(){
  if(!isAdmin()){
    state.users = [];
    renderUserDirectory();
    return;
  }
  try{
    const data = await apiRequest('/api/users');
    state.users = Array.isArray(data.users) ? data.users : [];
    if(data?.defaultPassword){
      state.defaultTempPassword = data.defaultPassword;
    }
    renderUserDirectory();
  }catch(err){
    console.error('Failed to load users', err);
    if(userFormStatus){
      userFormStatus.textContent = err.message || 'Failed to load users';
      userFormStatus.classList.add('error');
    }
  }
}

function renderUserDirectory(){
  if(!userDirectoryEl){
    return;
  }
  const filters = state.userFilters || {query: '', role: ''};
  if(userSearchInput && userSearchInput.value !== (filters.query || '')){
    userSearchInput.value = filters.query || '';
  }
  if(userRoleFilter && userRoleFilter.value !== (filters.role || '')){
    userRoleFilter.value = filters.role || '';
  }
  if(!isAdmin()){
    userDirectoryEl.innerHTML = '<p class="help">Admin access required.</p>';
    return;
  }
  const users = Array.isArray(state.users) ? state.users.slice().sort((a, b)=> (a.name || '').localeCompare(b.name || '', undefined, {sensitivity: 'base'})) : [];
  if(!users.length){
    userDirectoryEl.innerHTML = '<p class="help">No user accounts yet.</p>';
    return;
  }
  const query = (filters.query || '').toLowerCase();
  const roleFilter = filters.role || '';
  const filtered = users.filter(user =>{
    const name = (user.name || '').toLowerCase();
    const email = (user.email || '').toLowerCase();
    const matchesQuery = !query || name.includes(query) || email.includes(query);
    const roles = Array.isArray(user.roles) ? user.roles : [];
    const matchesRole = !roleFilter || roles.includes(roleFilter);
    return matchesQuery && matchesRole;
  });
  if(!filtered.length){
    userDirectoryEl.innerHTML = '<p class="help">No users match your filters.</p>';
    return;
  }
  const rows = filtered.map(user =>{
    const roles = formatRoleList(user.roles || []);
    const resetBadge = user.needsPasswordReset ? '<span class="badge warn">Reset required</span>' : '';
    return `<div class="user-row">
      <div class="user-row-info">
        <strong>${escapeHtml(user.name || user.email || 'User')}</strong>
        <span>${escapeHtml(user.email || '')}</span>
        <span class="help">${escapeHtml(roles || 'No roles')}</span>
        ${resetBadge}
      </div>
      <div class="user-row-actions">
        <button type="button" class="btn ghost small" data-edit-user="${user.id}">Edit</button>
        <button type="button" class="btn ghost small" data-reset-user="${user.id}">Reset password</button>
      </div>
    </div>`;
  }).join('');
  userDirectoryEl.innerHTML = rows;
}

function onUserDirectoryClick(event){
  const editBtn = event.target.closest('[data-edit-user]');
  if(editBtn){
    startUserEdit(editBtn.dataset.editUser);
    return;
  }
  const resetBtn = event.target.closest('[data-reset-user]');
  if(resetBtn){
    resetUserPasswordFor(resetBtn.dataset.resetUser);
  }
}

function startUserEdit(userId){
  if(!isAdmin() || !userForm){
    return;
  }
  const user = state.users.find(u => u.id === userId);
  if(!user){
    return;
  }
  userIdInput.value = user.id;
  userNameInput.value = user.name || '';
  userEmailInput.value = user.email || '';
  renderUserRoleGrid(user.roles || []);
  openUserModal('edit', user.name || user.email || 'user');
  if(userFormStatus){
    userFormStatus.textContent = 'Editing existing user';
    userFormStatus.classList.remove('error');
  }
}

function resetUserForm(options = {}){
  const {closeModal = true} = options;
  if(!userForm){
    return;
  }
  userIdInput.value = '';
  userNameInput.value = '';
  userEmailInput.value = '';
  renderUserRoleGrid([]);
  if(userFormStatus){
    userFormStatus.textContent = '';
    userFormStatus.classList.remove('error');
  }
  if(closeModal){
    closeUserModal();
  }
}

function onNewUserClick(){
  if(!isAdmin()){
    toast('Admin access required', true);
    return;
  }
  resetUserForm({closeModal: false});
  if(userFormStatus){
    userFormStatus.textContent = 'Create new user';
    userFormStatus.classList.remove('error');
  }
  openUserModal('create');
}

function onUserSearchInput(){
  if(!isAdmin()){
    return;
  }
  state.userFilters.query = userSearchInput?.value ? userSearchInput.value.trim() : '';
  renderUserDirectory();
}

function onUserRoleFilterChange(){
  if(!isAdmin()){
    return;
  }
  state.userFilters.role = userRoleFilter?.value || '';
  renderUserDirectory();
}

function openUserModal(mode = 'create', userName = ''){
  if(!userModal){
    return;
  }
  if(userModalTitle){
    userModalTitle.textContent = mode === 'edit' && userName ? `Edit ${userName}` : 'Add user';
  }
  userModal.classList.add('open');
  userModal.setAttribute('aria-hidden', 'false');
}

function closeUserModal(){
  if(!userModal){
    return;
  }
  userModal.classList.remove('open');
  userModal.setAttribute('aria-hidden', 'true');
}

function getSelectedUserRoles(){
  if(!userForm){
    return [];
  }
  const roleInputs = userForm.querySelectorAll('input[name="userRole"]:checked');
  return Array.from(roleInputs).map(input => input.value);
}

function handleUserFormKeydown(event){
  if(event.key !== 'Enter' || event.shiftKey){
    return;
  }
  const target = event.target;
  if(!target){
    return;
  }
  const tag = target.tagName;
  if(tag !== 'INPUT' && tag !== 'SELECT'){
    return;
  }
  event.preventDefault();
  onUserFormSubmit();
}

async function onUserFormSubmit(event){
  if(event && typeof event.preventDefault === 'function'){
    event.preventDefault();
  }
  if(!isAdmin()){
    toast('Admin access required', true);
    return;
  }
  const name = userNameInput?.value ? userNameInput.value.trim() : '';
  const email = userEmailInput?.value ? userEmailInput.value.trim() : '';
  const roles = getSelectedUserRoles();
  if(!name || !email || !roles.length){
    if(userFormStatus){
      userFormStatus.textContent = 'Name, email and at least one role are required';
      userFormStatus.classList.add('error');
    }
    return;
  }
  const payload = {name, email, roles};
  const userId = userIdInput?.value ? userIdInput.value.trim() : '';
  try{
    if(userId){
      await apiRequest(`/api/users/${userId}`, {method: 'PUT', body: payload});
      toast('User updated');
    }else{
      await apiRequest('/api/users', {method: 'POST', body: payload});
      toast('User created');
    }
    resetUserForm();
    await loadUsers();
    await loadStaff();
    notifyStaffChanged();
  }catch(err){
    console.error('Failed to save user', err);
    if(userFormStatus){
      userFormStatus.textContent = err.message || 'Failed to save user';
      userFormStatus.classList.add('error');
    }
  }
}

async function resetUserPasswordFor(userId){
  if(!isAdmin() || !userId){
    return;
  }
  try{
    await apiRequest(`/api/users/${userId}/reset-password`, {method: 'POST'});
    toast(`Password reset. New temp password: ${state.defaultTempPassword}`);
    await loadUsers();
  }catch(err){
    console.error('Failed to reset password', err);
    toast(err.message || 'Failed to reset password', true);
  }
}

function initUI(){
  if(uiInitialized){
    return;
  }
  [stCompleted, stNoLaunch, stAbort].forEach(btn=>{
    btn.addEventListener('click', ()=>{
      setStatus(btn.dataset.status);
      updateIssueVisibility();
    });
  });
  planned.addEventListener('change', onPlanLaunchChange);
  launched.addEventListener('change', onPlanLaunchChange);
  primaryIssue.addEventListener('change', ()=>{
    populateSubIssues(primaryIssue.value);
    updateIssueVisibility();
  });

  showDate.addEventListener('change', ()=> handleShowHeaderChange('date', showDate.value));
  showTime.addEventListener('change', ()=> handleShowHeaderChange('time', showTime.value));
  showLabel.addEventListener('input', ()=> handleShowHeaderChange('label', showLabel.value));
  showNotes.addEventListener('input', ()=> handleShowHeaderChange('notes', showNotes.value));
  if(leadPilotSelect){
    leadPilotSelect.addEventListener('change', ()=> handleShowHeaderChange('leadPilot', leadPilotSelect.value));
  }
  if(monkeyLeadSelect){
    monkeyLeadSelect.addEventListener('change', ()=> handleShowHeaderChange('monkeyLead', monkeyLeadSelect.value));
  }

  if(newShowBtn){ newShowBtn.addEventListener('click', onNewShow); }
  if(addLineBtn){ addLineBtn.addEventListener('click', onAddLine); }

  if(entryShowSelect){
    entryShowSelect.addEventListener('change', ()=>{
      setCurrentShow(entryShowSelect.value || null);
    });
  }
  if(chooseLeadBtn){
    chooseLeadBtn.addEventListener('click', ()=>{
      if(!userHasRole('lead')){
        toast('Lead workspace requires Lead role', true);
        return;
      }
      setView('lead');
      setCurrentShow(state.currentShowId || (state.shows[0]?.id ?? null));
    });
  }
  if(chooseOperatorBtn){
    chooseOperatorBtn.addEventListener('click', ()=>{
      if(!userHasRole('operator')){
        toast('Operator workspace requires Operator role', true);
        return;
      }
      setView('operator');
    });
  }
  if(chooseArchiveBtn){
    chooseArchiveBtn.addEventListener('click', ()=>{
      if(!userHasRole('lead') && !userHasRole('operator') && !userHasRole('crew')){
        toast('Archive workspace requires a workspace role', true);
        return;
      }
      openArchiveWorkspace();
    });
  }
  if(openCalendarBtn){
    openCalendarBtn.addEventListener('click', ()=>{
      if(!userHasRole('lead') && !userHasRole('operator') && !userHasRole('crew')){
        toast('Calendar requires a workspace role', true);
        return;
      }
      openCalendarWorkspace();
    });
  }
  if(archiveShowSelect){
    archiveShowSelect.addEventListener('change', ()=>{
      setCurrentArchivedShow(archiveShowSelect.value || null);
    });
  }
  if(archiveExportCsvBtn){
    archiveExportCsvBtn.addEventListener('click', ()=> exportSelectedArchive('csv'));
  }
  if(archiveExportJsonBtn){
    archiveExportJsonBtn.addEventListener('click', ()=> exportSelectedArchive('json'));
  }
  if(archiveMetricButtons){
    archiveMetricButtons.addEventListener('click', onArchiveMetricButtonsClick);
  }
  if(archiveIssueButtons){
    archiveIssueButtons.addEventListener('click', onArchiveMetricButtonsClick);
  }
  if(archiveModeCalendarBtn){
    archiveModeCalendarBtn.addEventListener('click', ()=> setArchiveSelectionMode('calendar'));
  }
  if(archiveModeShowsBtn){
    archiveModeShowsBtn.addEventListener('click', ()=> setArchiveSelectionMode('shows'));
  }
  if(archiveDisciplineFilter){
    archiveDisciplineFilter.addEventListener('change', onArchiveDisciplineFilterChange);
  }
  if(archiveOperatorFilter){
    archiveOperatorFilter.addEventListener('change', onArchiveOperatorFilterChange);
  }
  if(closeArchiveDayDetailBtn){
    closeArchiveDayDetailBtn.addEventListener('click', closeArchiveDayDetail);
  }
  if(roleHomeBtn){
    roleHomeBtn.addEventListener('click', ()=> setView('discipline'));
  }
  if(newUserBtn){
    newUserBtn.addEventListener('click', onNewUserClick);
  }
  if(closeUserModalBtn){
    closeUserModalBtn.addEventListener('click', closeUserModal);
  }
  if(userModal){
    userModal.addEventListener('click', event=>{
      if(event.target === userModal){
        closeUserModal();
      }
    });
  }
  if(userSearchInput){
    userSearchInput.addEventListener('input', onUserSearchInput);
  }
  if(userRoleFilter){
    userRoleFilter.addEventListener('change', onUserRoleFilterChange);
  }

  el('closeEdit').addEventListener('click', closeEditModal);
  el('saveEdit').addEventListener('click', saveEditEntry);

  if(configBtn){
    configBtn.addEventListener('click', ()=> toggleConfig());
  }
  if(cancelConfigBtn){
    cancelConfigBtn.addEventListener('click', ()=> toggleConfig(false));
  }
  if(configNavButtons.length){
    configNavButtons.forEach(btn=>{
      btn.setAttribute('aria-pressed', 'false');
      btn.addEventListener('click', ()=>{
        const target = btn.dataset.configTarget || 'landing';
        if(target === 'admin'){
          if(!isAdmin()){
            toast('Admin access required', true);
            return;
          }
          setView('admin');
        }else{
          setView(target);
        }
        toggleConfig(false);
      });
    });
  }
  document.addEventListener('keydown', e=>{
    if(e.key === 'Escape'){
      closeAllShowMenus();
      toggleConfig(false);
      closeEditModal();
      closeWebhookModal({restore: true});
    }
  });

  window.addEventListener('resize', refreshDrawerOffset);
  configForm.addEventListener('submit', onConfigSubmit);
  setConfigSection(state.currentView || 'landing');
  refreshDrawerOffset();
  closeAdminPinPrompt();
  if(webhookEnabled){
    webhookEnabled.addEventListener('change', ()=>{
      syncWebhookFields();
      updateWebhookPreview();
      updateWebhookConfigureVisibility();
      if(webhookEnabled.checked){
        openWebhookModal();
      }else{
        closeWebhookModal();
      }
    });
  }
  if(webhookUrl){
    webhookUrl.addEventListener('input', ()=>{
      updateWebhookPreview();
    });
  }
  if(webhookMethod){
    webhookMethod.addEventListener('change', ()=>{
      updateWebhookPreview();
    });
  }
  if(webhookSecret){
    webhookSecret.addEventListener('input', ()=>{
      updateWebhookPreview();
    });
  }
  if(webhookHeaders){
    webhookHeaders.addEventListener('input', ()=>{
      updateWebhookPreview();
    });
  }
  if(webhookConfigureBtn){
    webhookConfigureBtn.addEventListener('click', ()=> openWebhookModal());
  }
  if(webhookSimulateBtn){
    webhookSimulateBtn.dataset.label = webhookSimulateBtn.textContent;
    webhookSimulateBtn.addEventListener('click', onSimulateWebhookMonth);
    updateWebhookSimulationButton();
  }
  if(closeWebhookModalBtn){
    closeWebhookModalBtn.addEventListener('click', ()=> closeWebhookModal({restore: true}));
  }
  if(webhookCancelBtn){
    webhookCancelBtn.addEventListener('click', ()=> closeWebhookModal({restore: true}));
  }
  if(webhookForm){
    webhookForm.addEventListener('submit', event=>{
      event.preventDefault();
      saveWebhookModal();
    });
  }
  if(refreshShowsBtn){
    refreshShowsBtn.dataset.label = refreshShowsBtn.textContent;
    refreshShowsBtn.addEventListener('click', onRefreshShows);
  }
  if(refreshArchiveBtn){
    refreshArchiveBtn.dataset.label = refreshArchiveBtn.textContent;
    refreshArchiveBtn.addEventListener('click', onRefreshArchiveList);
  }
  if(calendarPrevBtn){
    calendarPrevBtn.addEventListener('click', ()=> changeCalendarMonth(-1));
  }
  if(calendarNextBtn){
    calendarNextBtn.addEventListener('click', ()=> changeCalendarMonth(1));
  }
  if(calendarGrid){
    calendarGrid.addEventListener('click', onCalendarGridClick);
  }
  if(calendarRefreshBtn){
    calendarRefreshBtn.dataset.label = calendarRefreshBtn.textContent;
    calendarRefreshBtn.addEventListener('click', ()=> loadCalendarEvents({force: true}));
  }
  window.addEventListener('resize', ()=> positionCalendarDayDetails(state.activeCalendarDayKey));

  document.addEventListener('click', event=>{
    if(!event.target.closest('.show-menu-wrap')){
      closeAllShowMenus();
    }
  });

  renderShowHeaderDraft();
}

async function loadConfig(){
  const data = await apiRequest('/api/config');
  state.config = data;
  state.serverHost = data.host || state.serverHost;
  const portFromConfig = Number.parseInt(data.port, 10);
  state.serverPort = Number.isFinite(portFromConfig) ? portFromConfig : state.serverPort;
  state.unitLabel = data.unitLabel || 'Drone';
  const storageMeta = (data.storageMeta && typeof data.storageMeta === 'object')
    ? data.storageMeta
    : (typeof data.storage === 'object' ? data.storage : null);
  state.storageMeta = storageMeta || (typeof data.storage === 'string' ? {label: data.storage} : null);
  state.storageLabel = resolveStorageLabel(state.storageMeta || data.storage || state.storageLabel);
  state.webhookConfig = {
    enabled: Boolean(data.webhook?.enabled),
    url: data.webhook?.url || '',
    method: (data.webhook?.method || 'POST').toUpperCase(),
    secret: data.webhook?.secret || '',
    headersText: formatHeadersText(data.webhook?.headers)
  };
  state.webhookStatus = normalizeWebhookStatus(data.webhookStatus, data.webhook);
  updateDisciplineHeader();
  unitLabelEl.textContent = state.unitLabel;
  unitLabelSelect.value = state.unitLabel;
  setLanAddress();
  setProviderBadge(state.storageMeta || state.storageLabel);
  setWebhookBadge(state.webhookStatus);
  refreshWebhookUi();
}

async function loadDisciplineConfig(){
  try{
    const data = await apiRequest('/api/disciplines');
    const roles = Array.isArray(data?.roles) ? data.roles : [];
    state.roleLevels = roles.map(role => typeof role === 'string' ? role.trim().toLowerCase() : '').filter(Boolean);
    const disciplines = Array.isArray(data?.disciplines) ? data.disciplines : [];
    state.disciplines = disciplines.map(item => ({
      id: typeof item.id === 'string' ? item.id.trim().toLowerCase() : '',
      name: typeof item.name === 'string' ? item.name.trim() : '',
      default: Boolean(item.default),
      forms: Boolean(item.forms)
    })).filter(entry => entry.id && entry.name);
    const defaultId = typeof data?.defaultDiscipline === 'string' && data.defaultDiscipline
      ? data.defaultDiscipline.trim().toLowerCase()
      : (state.disciplines.find(discipline => discipline.default)?.id || state.disciplines[0]?.id || null);
    state.defaultDisciplineId = defaultId;
    if(!state.selectedDisciplineId){
      state.selectedDisciplineId = defaultId;
    }
    const selectedRoles = getSelectedUserRoles();
    renderDisciplineOptions();
    updateActiveDisciplineUi();
    renderUserRoleGrid(selectedRoles);
    renderUserRoleFilterOptions();
  }catch(err){
    console.error('Failed to load disciplines', err);
    if(!state.disciplines.length){
      state.disciplines = [{id: 'drones', name: 'Drones', default: true, forms: true}];
      state.defaultDisciplineId = 'drones';
      if(!state.selectedDisciplineId){
        state.selectedDisciplineId = 'drones';
      }
    }
    const selectedRoles = getSelectedUserRoles();
    renderDisciplineOptions();
    updateActiveDisciplineUi();
    renderUserRoleGrid(selectedRoles);
    renderUserRoleFilterOptions();
  }
}

async function loadStaff(){
  try{
    const data = await apiRequest('/api/staff');
    const directory = {};
    const disciplines = Array.isArray(data?.disciplines) ? data.disciplines : [];
    disciplines.forEach(entry =>{
      const id = typeof entry?.id === 'string' ? entry.id.trim().toLowerCase() : '';
      if(!id){
        return;
      }
      const roleMap = {};
      if(Array.isArray(entry.roles)){
        entry.roles.forEach(roleEntry =>{
          const levelId = typeof roleEntry?.id === 'string' ? roleEntry.id.trim().toLowerCase() : '';
          if(!levelId){
            return;
          }
          roleMap[levelId] = normalizeNameList(roleEntry?.users || [], {sort: true});
        });
      }
      directory[id] = roleMap;
    });
    state.staffDirectory = directory;
    applyActiveDisciplineRoster();
  }catch(err){
    console.error('Failed to load staff', err);
    state.staffDirectory = state.staffDirectory || {};
    state.staff = state.staff || {stagecrew: [], operators: [], leads: []};
    state.staff.stagecrew = [];
    state.staff.operators = [];
    state.staff.leads = [];
    toast('Failed to load staff directory', true);
    applyActiveDisciplineRoster();
  }
}

function getActiveDisciplineId(){
  return state.selectedDisciplineId || state.defaultDisciplineId || state.disciplines[0]?.id || 'drones';
}

function getDisciplineRoster(disciplineId){
  const roster = state.staffDirectory[disciplineId] || {};
  return {
    lead: normalizeNameList(roster.lead || roster.leads || [], {sort: true}),
    operator: normalizeNameList(roster.operator || roster.operators || [], {sort: true}),
    crew: normalizeNameList(roster.crew || roster.stagecrew || [], {sort: true})
  };
}

function applyActiveDisciplineRoster(){
  const disciplineId = getActiveDisciplineId();
  const roster = getDisciplineRoster(disciplineId);
  state.staff = {
    stagecrew: roster.crew.slice(),
    operators: roster.operator.slice(),
    leads: roster.lead.slice()
  };
  renderOperatorOptions();
  renderCrewOptions(getCurrentShow()?.crew || []);
  renderPilotAssignments(getCurrentShow());
  renderShowHeaderDraft();
  updateActiveDisciplineUi();
}

function getDisciplineDisplayName(disciplineId){
  const normalized = typeof disciplineId === 'string' ? disciplineId.trim().toLowerCase() : '';
  if(!normalized){
    return '';
  }
  const entry = state.disciplines.find(item => item.id === normalized);
  if(entry){
    return entry.name || normalized.charAt(0).toUpperCase() + normalized.slice(1);
  }
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function disciplineHasForms(disciplineId){
  const normalized = typeof disciplineId === 'string' ? disciplineId.trim().toLowerCase() : '';
  if(!normalized){
    return false;
  }
  const entry = state.disciplines.find(item => item.id === normalized);
  if(entry){
    return Boolean(entry.forms);
  }
  return normalized === 'drones';
}

function updateDisciplineHeader(){
  const disciplineId = getActiveDisciplineId();
  const name = getDisciplineDisplayName(disciplineId) || state.unitLabel || 'Discipline';
  const hasForms = disciplineHasForms(disciplineId);
  if(appTitle){
    appTitle.textContent = hasForms ? state.unitLabel : name;
  }
  if(titleSubPrefix){
    titleSubPrefix.textContent = hasForms ? 'Tracking' : 'Preparing';
  }
  if(titleSubSuffix){
    titleSubSuffix.textContent = hasForms ? 'activity' : 'workspaces';
  }
}

function renderDisciplineOptions(){
  const options = state.disciplines.length ? state.disciplines : [{id: 'drones', name: 'Drones', forms: true}];
  const activeId = getActiveDisciplineId();
  const markup = options.map(option =>{
    const isActive = option.id === activeId;
    const classes = ['btn', 'primary', 'role-btn'];
    if(isActive){
      classes.push('is-active');
    }
    const label = escapeHtml(option.name || getDisciplineDisplayName(option.id));
    return `<button type="button" class="${classes.join(' ')}" data-discipline-id="${escapeHtml(option.id)}" aria-pressed="${isActive ? 'true' : 'false'}">${label}</button>`;
  }).join('');
  if(disciplineList){
    disciplineList.innerHTML = markup;
  }
  if(landingDisciplineShortcuts){
    landingDisciplineShortcuts.innerHTML = markup;
  }
}

function onDisciplineListClick(event){
  const target = event.target.closest('[data-discipline-id]');
  if(!target){
    return;
  }
  const id = target.dataset.disciplineId;
  if(typeof id !== 'string'){
    return;
  }
  selectDiscipline(id);
}

function selectDiscipline(disciplineId){
  const normalized = typeof disciplineId === 'string' ? disciplineId.trim().toLowerCase() : '';
  if(!normalized){
    return;
  }
  state.selectedDisciplineId = normalized;
  applyActiveDisciplineRoster();
  renderDisciplineOptions();
  updateActiveDisciplineUi();
  if(disciplineHasForms(normalized)){
    setView('landing');
  }else{
    setView('workspace');
  }
}

function renderWorkspacePlaceholder(){
  if(!workspaceMessage){
    return;
  }
  const disciplineId = getActiveDisciplineId();
  const name = getDisciplineDisplayName(disciplineId) || 'this discipline';
  workspaceMessage.textContent = `Workspaces for ${name} are coming soon.`;
  if(workspaceList){
    workspaceList.innerHTML = '';
  }
}

function updateActiveDisciplineUi(){
  const disciplineId = getActiveDisciplineId();
  const name = getDisciplineDisplayName(disciplineId) || 'this discipline';
  if(landingTitle){
    landingTitle.textContent = `Choose workspace for ${name}`;
  }
  if(landingSubtitle){
    landingSubtitle.textContent = `Select the role you need for the ${name} team.`;
  }
  if(workspaceTitle){
    workspaceTitle.textContent = `Choose your workspace for ${name}`;
  }
  if(disciplineTitle){
    disciplineTitle.textContent = 'Choose discipline';
  }
  updateDisciplineHeader();
}

function renderUserRoleGrid(selectedRoles = []){
  if(!userRoleGrid){
    return;
  }
  const selectedSet = new Set((selectedRoles || []).map(role => typeof role === 'string' ? role.trim().toLowerCase() : ''));
  const disciplines = state.disciplines.length ? state.disciplines : [{id: 'drones', name: 'Drones', forms: true}];
  const roleLevels = state.roleLevels.length ? state.roleLevels : ['lead', 'operator', 'crew'];
  const groups = disciplines.map(discipline =>{
    const roleItems = roleLevels.map(level =>{
      const value = `${discipline.id}.${level}`;
      const checked = selectedSet.has(value);
      const label = formatRoleLevel(level);
      return `<label><input type="checkbox" name="userRole" value="${escapeHtml(value)}"${checked ? ' checked' : ''} /> ${escapeHtml(label)}</label>`;
    }).join('');
    return `<div class="user-role-group"><h4>${escapeHtml(discipline.name)}</h4>${roleItems}</div>`;
  });
  const adminChecked = selectedSet.has('admin');
  groups.push(`<div class="user-role-group"><h4>Global</h4><label><input type="checkbox" name="userRole" value="admin"${adminChecked ? ' checked' : ''} /> Admin</label></div>`);
  userRoleGrid.innerHTML = groups.join('');
}

function renderUserRoleFilterOptions(){
  if(!userRoleFilter){
    return;
  }
  const currentValue = state.userFilters?.role || userRoleFilter.value || '';
  const disciplines = state.disciplines.length ? state.disciplines : [{id: 'drones', name: 'Drones', forms: true}];
  const roleLevels = state.roleLevels.length ? state.roleLevels : ['lead', 'operator', 'crew'];
  const options = [];
  disciplines.forEach(discipline =>{
    roleLevels.forEach(level =>{
      const value = `${discipline.id}.${level}`;
      const label = `${discipline.name} ${formatRoleLevel(level)}`;
      options.push({value, label});
    });
  });
  options.push({value: 'admin', label: 'Admin'});
  const existing = new Set(options.map(option => option.value));
  let nextValue = existing.has(currentValue) ? currentValue : '';
  const optionMarkup = options.map(option => `<option value="${escapeHtml(option.value)}">${escapeHtml(option.label)}</option>`).join('');
  userRoleFilter.innerHTML = `<option value="">All roles</option>${optionMarkup}`;
  userRoleFilter.value = nextValue;
  state.userFilters.role = nextValue;
}

async function loadShows(){
  try{
    const previousId = state.currentShowId;
    const data = await apiRequest('/api/shows');
    const storageMeta = (data.storageMeta && typeof data.storageMeta === 'object') ? data.storageMeta : null;
    state.storageMeta = storageMeta || (typeof data.storage === 'string' ? {label: data.storage} : state.storageMeta);
    state.storageLabel = resolveStorageLabel(state.storageMeta || data.storage || state.storageLabel);
    state.webhookStatus = normalizeWebhookStatus(data.webhook, state.webhookConfig);
    state.shows = Array.isArray(data.shows) ? data.shows.map(normalizeActiveShow) : [];
    sortShows();
    const fallbackId = state.shows[0]?.id || null;
    state.currentShowId = previousId && state.shows.some(show=>show.id===previousId) ? previousId : fallbackId;
    updateConnectionIndicator();
    updateWebhookPreview();
    await loadArchivedShows({silent: true, preserveSelection: true});
  }catch(err){
    console.error('Failed to load shows', err);
    state.shows = [];
    state.currentShowId = null;
    toast('Failed to load shows', true);
    updateConnectionIndicator('error');
  }
}

async function openArchiveWorkspace(){
  setView('archive');
  renderArchiveSelect();
  await loadArchivedShows({silent: true, preserveSelection: true});
}

async function openCalendarWorkspace(){
  setView('calendar');
  if(!state.calendarMonth){
    state.calendarMonth = getMonthStart(new Date());
  }
  if(!state.activeCalendarDayKey){
    state.activeCalendarDayKey = formatDayKey(new Date());
  }
  await loadCalendarEvents({force: !state.calendarLoaded});
  renderCalendar();
}

async function loadArchivedShows(options = {}){
  const {silent = false, preserveSelection = false} = options;
  try{
    const data = await apiRequest('/api/shows/archive');
    const shows = Array.isArray(data.shows) ? data.shows.map(normalizeArchivedShow) : [];
    shows.sort((a, b)=> (Number.isFinite(b.archivedAt) ? b.archivedAt : 0) - (Number.isFinite(a.archivedAt) ? a.archivedAt : 0));
    state.archivedShows = shows;
    syncArchiveChartSelection();
    if(preserveSelection){
      const hasCurrent = state.currentArchivedShowId && state.archivedShows.some(show=>show.id === state.currentArchivedShowId);
      if(!hasCurrent){
        state.currentArchivedShowId = state.archivedShows[0]?.id || null;
      }
    }else{
      state.currentArchivedShowId = state.archivedShows[0]?.id || null;
    }
    renderArchiveSelect();
    renderArchiveDisciplineFilter();
    renderArchiveChartControls();
    renderArchiveChart();
    return true;
  }catch(err){
    console.error('Failed to load archive', err);
    if(!silent){
      toast('Failed to load archive', true);
    }
    if(!preserveSelection){
      state.archivedShows = [];
      state.currentArchivedShowId = null;
      state.selectedArchiveChartShows = [];
      renderArchiveSelect();
      renderArchiveDisciplineFilter();
      renderArchiveChartControls();
      renderArchiveChart();
    }
    return false;
  }
}

async function loadCalendarEvents(options = {}){
  const {force = false} = options;
  if(state.calendarLoaded && !force){
    renderCalendar();
    return;
  }
  if(calendarRefreshBtn){
    calendarRefreshBtn.disabled = true;
    calendarRefreshBtn.textContent = 'Syncing…';
  }
  try{
    const data = await apiRequest('/api/calendar');
    const events = Array.isArray(data?.events) ? data.events.map(normalizeCalendarEvent) : [];
    state.calendarEvents = events.filter(event => Number.isFinite(event.startTs));
    state.calendarLoaded = true;
    renderCalendar();
  }catch(err){
    console.error('Failed to load calendar', err);
    toast('Failed to load calendar feed', true);
  }finally{
    if(calendarRefreshBtn){
      calendarRefreshBtn.disabled = false;
      calendarRefreshBtn.textContent = calendarRefreshBtn.dataset.label || 'Refresh calendar';
    }
  }
}

async function onRefreshShows(){
  let originalLabel = '';
  if(refreshShowsBtn){
    originalLabel = refreshShowsBtn.dataset.label || refreshShowsBtn.textContent;
    refreshShowsBtn.disabled = true;
    refreshShowsBtn.textContent = 'Refreshing…';
  }
  updateConnectionIndicator('loading');
  try{
    await loadShows();
    setCurrentShow(state.currentShowId || null);
    toast('Data refreshed');
  }catch(err){
    console.error('Failed to refresh shows', err);
    toast('Failed to refresh data', true);
  }finally{
    if(refreshShowsBtn){
      refreshShowsBtn.disabled = false;
      refreshShowsBtn.textContent = originalLabel || 'Refresh data';
    }
  }
}

async function onRefreshArchiveList(){
  let originalLabel = '';
  if(refreshArchiveBtn){
    originalLabel = refreshArchiveBtn.dataset.label || refreshArchiveBtn.textContent;
    refreshArchiveBtn.disabled = true;
    refreshArchiveBtn.textContent = 'Refreshing…';
  }
  try{
    const success = await loadArchivedShows({silent: false, preserveSelection: true});
    if(success){
      toast('Archive refreshed');
    }
  }catch(err){
    console.error('Failed to refresh archive', err);
  }finally{
    if(refreshArchiveBtn){
      refreshArchiveBtn.disabled = false;
      refreshArchiveBtn.textContent = originalLabel || 'Refresh archive';
    }
  }
}

async function onSimulateWebhookMonth(){
  if(!webhookSimulateBtn){
    return;
  }
  const originalLabel = webhookSimulateBtn.dataset.label || webhookSimulateBtn.textContent;
  webhookSimulateBtn.disabled = true;
  webhookSimulateBtn.textContent = 'Simulating…';
  try{
    const result = await apiRequest('/api/webhook/simulate-month', {method: 'POST'});
    if(result?.webhook){
      state.webhookStatus = normalizeWebhookStatus(result.webhook, state.webhookConfig);
      updateConnectionIndicator();
    }
    const dispatched = Number(result?.dispatched) || 0;
    const skipped = Number(result?.skipped) || 0;
    const requested = Number(result?.requested) || 0;
    const errors = Array.isArray(result?.errors) ? result.errors : [];
    if(errors.length){
      console.warn('Webhook simulation completed with errors', errors);
    }
    if(dispatched > 0){
      const skippedNote = skipped ? ` (${skipped} skipped)` : '';
      const errorNote = errors.length ? ` • ${errors.length} error${errors.length === 1 ? '' : 's'}` : '';
      toast(`Simulated ${dispatched} show${dispatched === 1 ? '' : 's'}${skippedNote}${errorNote}.`);
    }else if(skipped > 0 && requested > 0){
      toast('Webhook disabled. Simulation skipped.');
    }else if(requested === 0){
      toast('No shows available to simulate', true);
    }else if(errors.length){
      toast('Simulation attempted but webhook returned errors', true);
    }else{
      toast('Simulation complete');
    }
  }catch(err){
    console.error('Failed to simulate webhook month', err);
    toast(err.message || 'Failed to simulate webhook delivery', true);
  }finally{
    if(webhookSimulateBtn){
      webhookSimulateBtn.textContent = originalLabel || 'Simulate month delivery';
      updateWebhookSimulationButton();
    }
  }
}

function setupSyncChannel(){
  if(typeof BroadcastChannel !== 'function'){
    return;
  }
  if(syncState.channel){
    return;
  }
  try{
    syncState.channel = new BroadcastChannel(SYNC_CHANNEL_NAME);
    syncState.channel.addEventListener('message', handleSyncMessage);
    window.addEventListener('beforeunload', closeSyncChannel, {once: true});
    window.addEventListener('pagehide', closeSyncChannel, {once: true});
  }catch(err){
    console.warn('Failed to initialize sync channel', err);
    syncState.channel = null;
  }
}

function closeSyncChannel(){
  if(!syncState.channel){
    return;
  }
  try{
    syncState.channel.close();
  }catch(err){
    console.warn('Failed to close sync channel', err);
  }finally{
    syncState.channel = null;
  }
}

function broadcastMessage(type, detail = {}){
  if(!syncState.channel){
    return;
  }
  try{
    syncState.channel.postMessage({
      source: syncState.id,
      type,
      detail: detail || {}
    });
  }catch(err){
    console.warn('Failed to broadcast sync message', err);
  }
}

function notifyShowsChanged(detail = {}){
  broadcastMessage('shows:changed', detail);
}

function notifyStaffChanged(){
  broadcastMessage('staff:changed');
}

function notifyConfigChanged(detail = {}){
  broadcastMessage('config:changed', detail);
}

async function handleSyncMessage(event){
  const data = event?.data;
  if(!data || typeof data !== 'object' || data.source === syncState.id){
    return;
  }
  try{
    switch(data.type){
      case 'shows:changed':
        await refreshShowsFromSync(data.detail);
        break;
      case 'staff:changed':
        await refreshStaffFromSync();
        break;
      case 'config:changed':
        await refreshConfigFromSync(data.detail);
        break;
      default:
        break;
    }
  }catch(err){
    console.error('Sync message handling failed', err);
  }
}

async function refreshShowsFromSync(detail = {}){
  const previousId = state.currentShowId;
  try{
    await loadShows();
  }catch(err){
    console.error('Failed to sync shows', err);
    return;
  }
  let targetId = null;
  const preferredId = detail && typeof detail === 'object' ? detail.showId : null;
  const hasPrevious = previousId && state.shows.some(show => show.id === previousId);
  const hasPreferred = preferredId && state.shows.some(show => show.id === preferredId);
  if(hasPrevious){
    targetId = previousId;
  }else if(hasPreferred){
    targetId = preferredId;
  }else{
    targetId = state.shows[0]?.id || null;
  }
  setCurrentShow(targetId, {skipOperatorSync: false});
}

async function refreshStaffFromSync(){
  try{
    await loadStaff();
  }catch(err){
    console.error('Failed to sync staff roster', err);
  }
}

async function refreshConfigFromSync(){
  try{
    await loadConfig();
    populateUnitOptions();
    updateConnectionIndicator('loading');
    await loadShows();
    setCurrentShow(state.currentShowId || null);
  }catch(err){
    console.error('Failed to sync configuration', err);
  }
}

function getCurrentShow(){
  if(!state.currentShowId){
    return null;
  }
  return state.shows.find(s=>s.id===state.currentShowId) || null;
}

function setCurrentShow(showId, options = {}){
  const {skipOperatorSync = false, skipRender = false} = options;
  state.currentShowId = showId || null;
  renderOperatorOptions();
  updateIssueVisibility();
  if(!skipRender){
    renderGroups();
  }
  updateWebhookPreview();
  if(!skipOperatorSync){
    syncOperatorShowSelect();
  }else{
    updateOperatorSummary();
  }
  updateOperatorEntryState();
}

function syncOperatorShowSelect(){
  if(!entryShowSelect){
    updateOperatorSummary();
    return;
  }
  const shows = state.shows.slice();
  if(!shows.length){
    entryShowSelect.innerHTML = '<option value="">No shows available</option>';
    entryShowSelect.disabled = true;
    entryShowSelect.value = '';
    updateOperatorSummary();
    updateOperatorEntryState();
    return;
  }
  entryShowSelect.disabled = false;
  entryShowSelect.innerHTML = shows.map(show=>{
    const date = formatDateUS(show.date) || 'MM-DD-YYYY';
    const time = formatTime12Hour(show.time) || 'HH:mm';
    const label = show.label ? ` • ${show.label}` : '';
    return `<option value="${show.id}">${escapeHtml(`${date} • ${time}${label}`)}</option>`;
  }).join('');
  const hasCurrent = state.currentShowId && shows.some(show=>show.id===state.currentShowId);
  const selectedId = hasCurrent ? state.currentShowId : shows[0].id;
  entryShowSelect.value = selectedId;
  if(!hasCurrent){
    setCurrentShow(selectedId, {skipOperatorSync: true});
  }else{
    updateOperatorSummary();
    updateOperatorEntryState();
  }
}

function updateOperatorSummary(){
  if(!operatorShowSummary){
    return;
  }
  const show = getCurrentShow();
  if(!show){
    operatorShowSummary.textContent = 'Lead must create a show before logging entries.';
    return;
  }
  const date = formatDateUS(show.date) || 'Date TBD';
  const time = formatTime12Hour(show.time) || 'Time TBD';
  const parts = [`Logging to ${date} • ${time}`];
  if(show.label){ parts.push(show.label); }
  if(show.leadPilot){ parts.push(`Lead: ${show.leadPilot}`); }
  if(show.monkeyLead){ parts.push(`Crew lead: ${show.monkeyLead}`); }
  operatorShowSummary.textContent = parts.join(' • ');
}

function renderArchiveSelect(){
  if(!archiveShowSelect){
    return;
  }
  const shows = getFilteredArchivedShows(state.archivedShows, {includeDateFilter: false, includeOperatorFilter: false});
  if(!shows.length){
    archiveShowSelect.innerHTML = '<option value="">No archived shows</option>';
    archiveShowSelect.disabled = true;
    if(archiveMeta){
      const totalShows = Array.isArray(state.archivedShows) ? state.archivedShows.length : 0;
      archiveMeta.textContent = totalShows
        ? 'No archived shows for this discipline yet.'
        : 'Shows archive will populate once daily records are archived.';
    }
    renderArchiveStats(null);
    renderArchiveDetails(null);
    renderArchiveChartControls();
    renderArchiveChart();
    return;
  }
  archiveShowSelect.disabled = false;
  archiveShowSelect.innerHTML = shows.map(show=>{
    const date = formatDateUS(show.date) || 'MM-DD-YYYY';
    const time = formatTime12Hour(show.time) || 'HH:mm';
    const label = show.label ? ` • ${show.label}` : '';
    const status = show.deletedAt ? ' (deleted)' : '';
    return `<option value="${show.id}">${escapeHtml(`${date} • ${time}${label}${status}`)}</option>`;
  }).join('');
  const hasCurrent = state.currentArchivedShowId && shows.some(show=>show.id === state.currentArchivedShowId);
  const selectedId = hasCurrent ? state.currentArchivedShowId : shows[0].id;
  archiveShowSelect.value = selectedId;
  setCurrentArchivedShow(selectedId, {skipSelectUpdate: true});
}

function renderArchiveDisciplineFilter(){
  if(!archiveDisciplineFilter){
    return;
  }
  const options = state.disciplines.length ? state.disciplines : [{id: 'drones', name: 'Drones'}];
  const currentRaw = state.archiveChartFilters?.discipline;
  const current = typeof currentRaw === 'string' ? currentRaw.toLowerCase() : '';
  const optionMarkup = ['<option value="">All disciplines</option>']
    .concat(options.map(option => `<option value="${escapeHtml(option.id)}">${escapeHtml(option.name)}</option>`))
    .join('');
  archiveDisciplineFilter.innerHTML = optionMarkup;
  const match = options.find(option => option.id === current);
  const shouldFallback = currentRaw === undefined || currentRaw === null;
  const fallback = shouldFallback && getActiveDisciplineId() ? getActiveDisciplineId() : '';
  const nextValue = match
    ? match.id
    : (fallback && options.some(option => option.id === fallback) ? fallback : (current || ''));
  archiveDisciplineFilter.value = nextValue;
  state.archiveChartFilters.discipline = nextValue;
}

function setCurrentArchivedShow(showId, options = {}){
  const {skipSelectUpdate = false} = options;
  const availableShows = getFilteredArchivedShows(state.archivedShows, {includeDateFilter: false, includeOperatorFilter: false});
  const preferredId = showId && availableShows.some(show => show.id === showId)
    ? showId
    : (availableShows[0]?.id || null);
  state.currentArchivedShowId = preferredId;
  if(!skipSelectUpdate && archiveShowSelect){
    if(preferredId){
      archiveShowSelect.value = preferredId;
    }else if(availableShows[0]){
      archiveShowSelect.value = availableShows[0].id;
    }else{
      archiveShowSelect.value = '';
    }
  }
  const show = getArchivedShow(state.currentArchivedShowId);
  renderArchiveStats(show);
  renderArchiveDetails(show);
}

function getArchivedShow(showId){
  if(!showId){
    return null;
  }
  return state.archivedShows.find(show=>show.id === showId) || null;
}

function renderArchiveStats(show){
  if(!archiveStats){
    return;
  }
  if(!show){
    archiveStats.innerHTML = '<p class="help">Select an archived show to view summary statistics.</p>';
    return;
  }
  const stats = computeArchiveShowStats(show);
  const items = ARCHIVE_SUMMARY_KEYS.map(key => {
    const def = getArchiveMetricDef(key);
    if(!def){
      return '';
    }
    const value = def.getValue(stats, show);
    const formatted = formatMetricValue(def, value);
    return `<div><dt>${escapeHtml(def.label)}</dt><dd>${escapeHtml(formatted)}</dd></div>`;
  }).filter(Boolean);
  if(items.length){
    archiveStats.innerHTML = `
      <h3>Show statistics</h3>
      <dl>${items.join('')}</dl>
    `;
  }else{
    archiveStats.innerHTML = `
      <h3>Show statistics</h3>
      <p class="help">Statistics will populate once entries are recorded for this show.</p>
    `;
  }
}

function getArchiveSelectionMode(){
  return state.archiveSelectionMode === 'shows' ? 'shows' : 'calendar';
}

function setArchiveSelectionMode(mode){
  const nextMode = mode === 'shows' ? 'shows' : 'calendar';
  if(state.archiveSelectionMode === nextMode){
    return;
  }
  state.archiveSelectionMode = nextMode;
  if(nextMode === 'calendar'){
    const filtered = getFilteredArchivedShows();
    state.selectedArchiveChartShows = filtered.map(show => show.id);
  }else{
    const shows = Array.isArray(state.archivedShows) ? state.archivedShows : [];
    if(Array.isArray(state.selectedArchiveChartShows)){
      const availableIds = new Set(shows.map(show => show.id));
      const filtered = state.selectedArchiveChartShows.filter(id => availableIds.has(id));
      state.selectedArchiveChartShows = filtered.length ? filtered : (shows.length ? null : []);
    }else{
      state.selectedArchiveChartShows = shows.length ? null : [];
    }
  }
  closeArchiveDayDetail();
  renderArchiveChartControls();
  renderArchiveChart();
}

function renderArchiveSelectionMode(){
  const mode = getArchiveSelectionMode();
  if(archiveModeCalendarBtn){
    archiveModeCalendarBtn.classList.toggle('is-active', mode === 'calendar');
    archiveModeCalendarBtn.setAttribute('aria-pressed', mode === 'calendar' ? 'true' : 'false');
  }
  if(archiveModeShowsBtn){
    archiveModeShowsBtn.classList.toggle('is-active', mode === 'shows');
    archiveModeShowsBtn.setAttribute('aria-pressed', mode === 'shows' ? 'true' : 'false');
  }
  if(archiveModeControls){
    archiveModeControls.className = `control-group archive-mode-${mode}`;
    archiveModeControls.innerHTML = mode === 'calendar'
      ? getArchiveCalendarControlsMarkup()
      : getArchiveShowControlsMarkup();
    refreshArchiveModeControlRefs();
  }
}

function getArchiveCalendarControlsMarkup(){
  return `
    <span class="control-label">Date range</span>
    <div class="date-range" role="group" aria-label="Archive date range">
      <label class="sr-only" for="archiveShowFilterStart">Start date</label>
      <input id="archiveShowFilterStart" type="date" />
      <span class="date-range-sep" aria-hidden="true">→</span>
      <label class="sr-only" for="archiveShowFilterEnd">End date</label>
      <input id="archiveShowFilterEnd" type="date" />
    </div>
    <p class="help small">Filter the show list using calendar dates.</p>
  `;
}

function getArchiveShowControlsMarkup(){
  return `
    <label for="archiveStatShowSelect">Shows to plot</label>
    <select id="archiveStatShowSelect" multiple size="8" aria-describedby="archiveShowHelp"></select>
    <div class="control-actions">
      <button id="archiveSelectAllShows" type="button" class="btn ghost small">Select all</button>
      <button id="archiveClearShowSelection" type="button" class="btn ghost small">Clear</button>
    </div>
    <p id="archiveShowHelp" class="help small">Use Shift or Ctrl/Cmd click to choose multiple shows.</p>
  `;
}

function refreshArchiveModeControlRefs(){
  archiveStatShowSelect = el('archiveStatShowSelect');
  archiveShowFilterStart = el('archiveShowFilterStart');
  archiveShowFilterEnd = el('archiveShowFilterEnd');
  archiveSelectAllShowsBtn = el('archiveSelectAllShows');
  archiveClearShowSelectionBtn = el('archiveClearShowSelection');

  if(archiveShowFilterStart){
    archiveShowFilterStart.addEventListener('change', ()=> onArchiveDateFilterChange('startDate', archiveShowFilterStart.value));
  }
  if(archiveShowFilterEnd){
    archiveShowFilterEnd.addEventListener('change', ()=> onArchiveDateFilterChange('endDate', archiveShowFilterEnd.value));
  }
  if(archiveStatShowSelect){
    archiveStatShowSelect.addEventListener('change', onArchiveShowSelectChange);
  }
  if(archiveSelectAllShowsBtn){
    archiveSelectAllShowsBtn.addEventListener('click', selectAllFilteredArchiveShows);
  }
  if(archiveClearShowSelectionBtn){
    archiveClearShowSelectionBtn.addEventListener('click', clearFilteredArchiveSelection);
  }
}

function renderArchiveChartControls(){
  renderArchiveSelectionMode();
  const shows = Array.isArray(state.archivedShows) ? state.archivedShows : [];
  const filters = typeof state.archiveChartFilters === 'object' && state.archiveChartFilters
    ? Object.assign({startDate: null, endDate: null, operator: null, discipline: ''}, state.archiveChartFilters)
    : {startDate: null, endDate: null, operator: null, discipline: ''};
  state.archiveChartFilters = filters;
  renderArchiveDisciplineFilter();
  if(archiveShowFilterStart){
    archiveShowFilterStart.value = filters.startDate || '';
  }
  if(archiveShowFilterEnd){
    archiveShowFilterEnd.value = filters.endDate || '';
  }

  const mode = getArchiveSelectionMode();
  const filteredShows = getFilteredArchivedShows(shows);
  const operatorFilteredShows = getFilteredArchivedShows(shows, {includeDateFilter: false});
  if(mode === 'calendar'){
    state.selectedArchiveChartShows = filteredShows.map(show => show.id);
  }else{
    const availableIds = new Set(operatorFilteredShows.map(show => show.id));
    const isArraySelection = Array.isArray(state.selectedArchiveChartShows);
    const currentSelection = isArraySelection
      ? state.selectedArchiveChartShows.filter(id => availableIds.has(id))
      : [];
    const shouldAutoPopulate = !isArraySelection;
    if(!currentSelection.length && operatorFilteredShows.length && shouldAutoPopulate){
      currentSelection.push(...operatorFilteredShows.slice(0, Math.min(5, operatorFilteredShows.length)).map(show => show.id));
    }
    state.selectedArchiveChartShows = Array.from(new Set(currentSelection));
  }

  if(archiveStatShowSelect){
    const optionShows = mode === 'calendar' ? filteredShows : operatorFilteredShows;
    if(optionShows.length){
      const optionsMarkup = optionShows.map(show => {
        const label = buildArchiveShowLabel(show);
        const id = escapeHtml(show.id || '');
        return `<option value="${id}">${escapeHtml(label)}</option>`;
      }).join('');
      archiveStatShowSelect.innerHTML = optionsMarkup;
      const selectedSet = new Set(state.selectedArchiveChartShows || []);
      Array.from(archiveStatShowSelect.options).forEach(option => {
        option.selected = selectedSet.has(option.value);
      });
      archiveStatShowSelect.disabled = mode !== 'shows';
    }else{
      archiveStatShowSelect.innerHTML = '';
      archiveStatShowSelect.disabled = true;
    }
  }

  if(archiveOperatorFilter){
    let operatorSource = [];
    if(mode === 'calendar'){
      operatorSource = getFilteredArchivedShows(shows, {includeOperatorFilter: false});
    }else{
      const selectedIds = new Set(Array.isArray(state.selectedArchiveChartShows) ? state.selectedArchiveChartShows : []);
      const selectedShows = shows.filter(show => selectedIds.has(show.id));
      operatorSource = getFilteredArchivedShows(selectedShows, {includeDateFilter: false, includeOperatorFilter: false});
    }
    const operatorNames = getArchiveOperatorNames(operatorSource);
    if(operatorNames.length){
      const operatorOptions = [''].concat(operatorNames).map(name => {
        if(!name){
          return '<option value="">All operators</option>';
        }
        const safeName = escapeHtml(name);
        return `<option value="${safeName}">${safeName}</option>`;
      }).join('');
      archiveOperatorFilter.innerHTML = operatorOptions;
      const selectedOperator = filters.operator || '';
      const operatorMatch = selectedOperator
        ? operatorNames.find(name => name.toLowerCase() === selectedOperator.toLowerCase())
        : '';
      archiveOperatorFilter.value = operatorMatch || '';
      archiveOperatorFilter.disabled = false;
      if(operatorMatch){
        state.archiveChartFilters.operator = operatorMatch;
      }
      if(selectedOperator && !operatorMatch){
        state.archiveChartFilters.operator = null;
      }
    }else{
      archiveOperatorFilter.innerHTML = '<option value="">All operators</option>';
      archiveOperatorFilter.value = '';
      archiveOperatorFilter.disabled = true;
      state.archiveChartFilters.operator = null;
    }
  }

  const chartableMetrics = getChartableMetricKeys();
  const issueMetricKeys = getChartableIssueMetricKeys();
  const selectedMetrics = Array.isArray(state.selectedArchiveMetrics)
    ? state.selectedArchiveMetrics.filter(key => chartableMetrics.includes(key) || issueMetricKeys.includes(key))
    : [];
  const effectiveSelection = selectedMetrics.length
    ? Array.from(new Set(selectedMetrics))
    : chartableMetrics.slice(0, Math.min(2, chartableMetrics.length));
  state.selectedArchiveMetrics = effectiveSelection;

  if(archiveMetricButtons){
    renderMetricToggleButtons(archiveMetricButtons, chartableMetrics, 'Archive metrics will appear once data is available.');
  }
  if(archiveIssueButtons){
    renderMetricToggleButtons(archiveIssueButtons, issueMetricKeys, 'Primary issues will populate automatically.');
  }

  if(archiveStatEmpty){
    if(!shows.length){
      archiveStatEmpty.hidden = false;
      archiveStatEmpty.textContent = 'Archive data will appear once shows are archived.';
    }else if(mode === 'calendar' && !filteredShows.length){
      archiveStatEmpty.hidden = false;
      archiveStatEmpty.textContent = 'No archived shows match the selected filters.';
    }else if(mode === 'shows' && !operatorFilteredShows.length){
      archiveStatEmpty.hidden = false;
      archiveStatEmpty.textContent = 'No archived shows match the selected operator.';
    }else if(mode === 'shows' && !state.selectedArchiveChartShows.length){
      archiveStatEmpty.hidden = false;
      archiveStatEmpty.textContent = 'Use the show picker to select one or more shows to render the chart.';
    }else if(!getActiveArchiveMetricKeys().length){
      archiveStatEmpty.hidden = false;
      archiveStatEmpty.textContent = 'Select one or more metrics to render the chart.';
    }
  }
}

function renderMetricToggleButtons(container, metricKeys, emptyMessage){
  if(!container){
    return;
  }
  const selectedSet = new Set(Array.isArray(state.selectedArchiveMetrics) ? state.selectedArchiveMetrics : []);
  const buttons = (metricKeys || []).map(metricKey => {
    const def = getArchiveMetricDef(metricKey);
    if(!def || !def.chartable){
      return '';
    }
    const isSelected = selectedSet.has(metricKey);
    const label = def.buttonLabel || def.label || metricKey;
    const classes = `btn metric-toggle${isSelected ? ' is-selected' : ''}`;
    const safeKey = escapeHtml(metricKey);
    const safeLabel = escapeHtml(label);
    return `<button type="button" class="${classes}" data-metric-key="${safeKey}" aria-pressed="${isSelected ? 'true' : 'false'}">${safeLabel}</button>`;
  }).filter(Boolean);
  if(buttons.length){
    container.innerHTML = buttons.join('');
  }else{
    const message = emptyMessage || 'Metrics will appear once data is available.';
    container.innerHTML = `<p class="help small">${escapeHtml(message)}</p>`;
  }
}

function renderArchiveChart(){
  if(!archiveStatCanvas || typeof Chart === 'undefined'){
    return;
  }
  const ctx = archiveStatCanvas.getContext('2d');
  if(!ctx){
    return;
  }
  const metrics = getActiveArchiveMetricKeys();
  const shows = getSelectedArchiveChartShows();
  if(!shows.length || !metrics.length){
    if(archiveChartInstance){
      archiveChartInstance.destroy();
      archiveChartInstance = null;
    }
    if(archiveStatEmpty){
      const hasShows = Array.isArray(state.archivedShows) && state.archivedShows.length > 0;
      const message = !hasShows
        ? 'Archive data will appear once shows are archived.'
        : (!shows.length
          ? 'Select one or more shows to render the chart.'
          : 'Select one or more metrics to render the chart.');
      archiveStatEmpty.hidden = false;
      archiveStatEmpty.textContent = message;
    }
    closeArchiveDayDetail();
    return;
  }

  const chartData = buildArchiveChartData(shows, metrics);
  if(!chartData.datasets.length || !chartData.dailyGroups.length){
    if(archiveChartInstance){
      archiveChartInstance.destroy();
      archiveChartInstance = null;
    }
    if(archiveStatEmpty){
      archiveStatEmpty.hidden = false;
      archiveStatEmpty.textContent = 'Selected shows do not have data for the chosen metrics yet.';
    }
    state.archiveDailyGroups = [];
    state.archiveDailyGroupsByKey = {};
    closeArchiveDayDetail();
    return;
  }

  if(archiveStatEmpty){
    archiveStatEmpty.hidden = true;
  }

  state.archiveDailyGroups = chartData.dailyGroups;
  state.archiveDailyGroupsByKey = Object.fromEntries((chartData.dailyGroups || []).map(group => [group.dateKey, group]));
  if(state.activeArchiveDayKey){
    const activeDay = getArchiveDayByKey(state.activeArchiveDayKey);
    if(activeDay){
      renderArchiveDayDetail(activeDay);
    }else{
      closeArchiveDayDetail();
    }
  }

  const data = { datasets: chartData.datasets };
  const options = buildArchiveChartOptions(chartData.axes);
  options.onClick = (event, elements, chart)=> handleArchiveChartClick(event, elements, chart);
  options.onHover = (event, elements)=> handleArchiveChartHover(event, elements);

  if(archiveChartInstance){
    archiveChartInstance.data = data;
    archiveChartInstance.options = options;
    archiveChartInstance.update();
  }else{
    archiveChartInstance = new Chart(ctx, {
      type: 'line',
      data,
      options
    });
  }

  const metricLabels = chartData.datasets.map(dataset => dataset.label).join(', ');
  archiveStatCanvas.setAttribute('aria-label', `${metricLabels} over time`);
}

function handleArchiveChartClick(event, elements, chart){
  if(!elements || !elements.length){
    closeArchiveDayDetail();
    return;
  }
  const element = elements[0];
  if(!chart || !chart.data || !chart.data.datasets){
    closeArchiveDayDetail();
    return;
  }
  const dataset = chart.data.datasets[element.datasetIndex];
  const point = dataset?.data?.[element.index];
  if(!point || !point.dayKey){
    closeArchiveDayDetail();
    return;
  }
  if(state.activeArchiveDayKey === point.dayKey){
    closeArchiveDayDetail();
    return;
  }
  const day = getArchiveDayByKey(point.dayKey);
  if(day){
    openArchiveDayDetail(day);
  }else{
    closeArchiveDayDetail();
  }
}

function handleArchiveChartHover(event, elements){
  const target = event?.native?.target || archiveStatCanvas;
  if(target && target.style){
    target.style.cursor = elements && elements.length ? 'pointer' : 'default';
  }
}

function openArchiveDayDetail(day){
  if(!day){
    closeArchiveDayDetail();
    return;
  }
  state.activeArchiveDayKey = day.dateKey || null;
  renderArchiveDayDetail(day);
  if(archiveDayDetail){
    if(archiveDayDetailCloseTimer){
      clearTimeout(archiveDayDetailCloseTimer);
      archiveDayDetailCloseTimer = null;
    }
    archiveDayDetail.hidden = false;
    archiveDayDetail.classList.remove('closing');
    archiveDayDetail.classList.remove('showing');
    // Trigger reflow so the entrance animation plays when re-opening quickly
    void archiveDayDetail.offsetWidth;
    archiveDayDetail.classList.add('showing');
  }
}

function renderArchiveDayDetail(day){
  if(!day || !archiveDayDetailContent){
    return;
  }
  const metrics = getActiveArchiveMetricKeys();
  const showCount = Array.isArray(day.shows) ? day.shows.length : 0;
  const summaryLabel = showCount === 1 ? 'show' : 'shows';
  const summaryLine = showCount
    ? `Averages from ${showCount} ${summaryLabel}.`
    : 'No shows recorded for this date yet.';
  if(archiveDayDetailTitle){
    archiveDayDetailTitle.textContent = day.displayDate || formatArchiveDayLabel(day.timestamp) || 'Day breakdown';
  }

  const metricCards = metrics.map(metricKey => {
    const def = getArchiveMetricDef(metricKey);
    if(!def){
      return '';
    }
    const summary = day.metrics?.[metricKey] || getOrCreateGroupMetricSummary(day, metricKey, def);
    const averageText = summary ? formatMetricValue(def, summary.average) : '—';
    const metaParts = [];
    if(summary){
      if(typeof summary.count === 'number'){
        metaParts.push(`${summary.count} ${summary.count === 1 ? 'value' : 'values'}`);
      }
      if(Number.isFinite(summary.min) && Number.isFinite(summary.max) && summary.min !== summary.max){
        const minText = formatMetricValue(def, summary.min);
        const maxText = formatMetricValue(def, summary.max);
        metaParts.push(`Range ${minText} – ${maxText}`);
      }
    }
    const metaText = metaParts.length ? metaParts.join(' • ') : '';
    return `
      <div class="archive-day-metric">
        <h5>${escapeHtml(def.label)}</h5>
        <div class="value">${escapeHtml(averageText)}</div>
        ${metaText ? `<div class="meta">${escapeHtml(metaText)}</div>` : ''}
      </div>
    `;
  }).filter(Boolean);

  const metricSection = metricCards.length
    ? `<div class="archive-day-metrics">${metricCards.join('')}</div>`
    : `<p class="help small">Select one or more metrics to compare shows.</p>`;

  const showListLimit = 3;
  const showItems = showCount
    ? day.shows.slice(0, showListLimit).map(item => {
        const show = item?.show;
        if(!show){
          return '';
        }
        const showLabel = escapeHtml(buildArchiveShowLabel(show));
        const metricSnippets = metrics.map(metricKey => {
          const def = getArchiveMetricDef(metricKey);
          if(!def){
            return '';
          }
          const summary = day.metrics?.[metricKey] || getOrCreateGroupMetricSummary(day, metricKey, def);
          const entry = summary?.valueMap?.[show.id];
          const valueText = entry ? entry.formatted : '—';
          const shortLabel = (def.label || metricKey).replace(/\s*\([^)]*\)/g, '').trim() || metricKey;
          return `<span class="metric"><span class="metric-label">${escapeHtml(shortLabel)}</span><span class="metric-value">${escapeHtml(valueText)}</span></span>`;
        }).filter(Boolean).join('');
        return `
          <li class="archive-day-show">
            <span class="archive-day-show-name">${showLabel}</span>
            ${metricSnippets ? `<span class="archive-day-show-metrics">${metricSnippets}</span>` : ''}
          </li>
        `;
      }).filter(Boolean)
    : [];
  const remainingShows = Math.max(0, showCount - showItems.length);
  const showSection = showItems.length
    ? `
      <div class="archive-day-shows">
        <ul class="archive-day-show-list">${showItems.join('')}</ul>
        ${remainingShows ? `<p class="archive-day-more">+${remainingShows} more ${remainingShows === 1 ? 'show' : 'shows'} logged this day</p>` : ''}
      </div>
    `
    : `<p class="help small">Shows for this date will appear here once recorded.</p>`;

  archiveDayDetailContent.innerHTML = `
    <p class="archive-day-summary">${escapeHtml(summaryLine)}</p>
    ${metricSection}
    ${showSection}
  `;
}

function closeArchiveDayDetail(){
  state.activeArchiveDayKey = null;
  if(archiveDayDetail){
    archiveDayDetail.classList.remove('showing');
    archiveDayDetail.classList.add('closing');
    if(archiveDayDetailCloseTimer){
      clearTimeout(archiveDayDetailCloseTimer);
    }
    archiveDayDetailCloseTimer = setTimeout(()=>{
      finalizeArchiveDayDetailHide();
    }, 180);
  }else{
    finalizeArchiveDayDetailHide();
  }
}

function finalizeArchiveDayDetailHide(){
  if(archiveDayDetail){
    archiveDayDetail.hidden = true;
    archiveDayDetail.classList.remove('closing');
    archiveDayDetail.classList.remove('showing');
  }
  if(archiveDayDetailContent){
    archiveDayDetailContent.innerHTML = '';
  }
  if(archiveDayDetailTitle){
    archiveDayDetailTitle.textContent = 'Day breakdown';
  }
  if(archiveDayDetailCloseTimer){
    clearTimeout(archiveDayDetailCloseTimer);
    archiveDayDetailCloseTimer = null;
  }
}

function onArchiveMetricButtonsClick(event){
  const target = event?.target ? event.target.closest('button[data-metric-key]') : null;
  if(!target){
    return;
  }
  const metricKey = target.getAttribute('data-metric-key');
  if(!metricKey){
    return;
  }
  toggleArchiveMetric(metricKey);
}

function toggleArchiveMetric(metricKey){
  if(!metricKey){
    return;
  }
  const availableMetrics = new Set([
    ...getChartableMetricKeys(),
    ...getChartableIssueMetricKeys()
  ]);
  if(!availableMetrics.has(metricKey)){
    return;
  }
  const selection = Array.isArray(state.selectedArchiveMetrics)
    ? state.selectedArchiveMetrics.slice()
    : [];
  const index = selection.indexOf(metricKey);
  if(index >= 0){
    selection.splice(index, 1);
  }else{
    selection.push(metricKey);
  }
  state.selectedArchiveMetrics = selection;
  closeArchiveDayDetail();
  renderArchiveChartControls();
  renderArchiveChart();
}

function onArchiveShowSelectChange(){
  if(!archiveStatShowSelect){
    return;
  }
  const selected = Array.from(archiveStatShowSelect.selectedOptions || []).map(option => option.value);
  state.selectedArchiveChartShows = selected;
  renderArchiveChart();
}

function onArchiveDateFilterChange(field, value){
  if(!field){
    return;
  }
  if(!state.archiveChartFilters || typeof state.archiveChartFilters !== 'object'){
    state.archiveChartFilters = {startDate: null, endDate: null, operator: null, discipline: ''};
  }
  state.archiveChartFilters[field] = value || null;
  renderArchiveChartControls();
  renderArchiveChart();
}

function onArchiveOperatorFilterChange(){
  if(!state.archiveChartFilters || typeof state.archiveChartFilters !== 'object'){
    state.archiveChartFilters = {startDate: null, endDate: null, operator: null, discipline: ''};
  }
  const value = archiveOperatorFilter?.value || '';
  state.archiveChartFilters.operator = value || null;
  closeArchiveDayDetail();
  renderArchiveChartControls();
  renderArchiveChart();
}

function onArchiveDisciplineFilterChange(){
  if(!state.archiveChartFilters || typeof state.archiveChartFilters !== 'object'){
    state.archiveChartFilters = {startDate: null, endDate: null, operator: null, discipline: ''};
  }
  const value = archiveDisciplineFilter?.value || '';
  state.archiveChartFilters.discipline = value.trim().toLowerCase();
  const filtered = getFilteredArchivedShows(state.archivedShows, {includeDateFilter: false, includeOperatorFilter: false});
  if(!filtered.some(show => show.id === state.currentArchivedShowId)){
    state.currentArchivedShowId = filtered[0]?.id || null;
  }
  closeArchiveDayDetail();
  renderArchiveSelect();
  renderArchiveChartControls();
  renderArchiveChart();
}

function selectAllFilteredArchiveShows(){
  if(getArchiveSelectionMode() === 'calendar'){
    const filtered = getFilteredArchivedShows();
    state.selectedArchiveChartShows = filtered.map(show => show.id);
  }else{
    const shows = Array.isArray(state.archivedShows) ? state.archivedShows : [];
    const filtered = getFilteredArchivedShows(shows, {includeDateFilter: false});
    state.selectedArchiveChartShows = filtered.map(show => show.id);
  }
  closeArchiveDayDetail();
  renderArchiveChartControls();
  renderArchiveChart();
}

function clearFilteredArchiveSelection(){
  state.selectedArchiveChartShows = [];
  if(getArchiveSelectionMode() === 'calendar'){
    const filtered = getFilteredArchivedShows();
    state.selectedArchiveChartShows = filtered.map(show => show.id);
  }
  closeArchiveDayDetail();
  renderArchiveChartControls();
  renderArchiveChart();
}


function getChartableMetricKeys(){
  return Object.keys(ARCHIVE_METRIC_DEFS).filter(key => ARCHIVE_METRIC_DEFS[key]?.chartable);
}

function getChartableIssueMetricKeys(){
  return PRIMARY_ISSUES.map(issue => getIssueMetricKey(issue));
}

function getActiveArchiveMetricKeys(){
  const metrics = Array.isArray(state.selectedArchiveMetrics) ? state.selectedArchiveMetrics : [];
  return metrics.filter(key => getArchiveMetricDef(key)?.chartable);
}

function getIssueMetricKey(issue){
  return `${ISSUE_METRIC_PREFIX}${issue}`;
}

function isIssueMetricKey(key){
  return typeof key === 'string' && key.startsWith(ISSUE_METRIC_PREFIX);
}

function getIssueFromMetricKey(key){
  if(!isIssueMetricKey(key)){
    return null;
  }
  return key.slice(ISSUE_METRIC_PREFIX.length);
}

function getFilteredArchivedShows(shows = state.archivedShows, options = {}){
  const list = Array.isArray(shows) ? shows.slice() : [];
  const filters = state.archiveChartFilters || {};
  const includeDateFilter = options.includeDateFilter !== false;
  const includeOperatorFilter = options.includeOperatorFilter !== false;
  const includeDisciplineFilter = options.includeDisciplineFilter !== false;
  const startTs = includeDateFilter ? parseFilterDate(filters.startDate, false) : null;
  const endTs = includeDateFilter ? parseFilterDate(filters.endDate, true) : null;
  const operatorFilter = includeOperatorFilter && typeof filters.operator === 'string' && filters.operator
    ? filters.operator.toLowerCase()
    : null;
  const disciplineFilter = includeDisciplineFilter && typeof filters.discipline === 'string' && filters.discipline
    ? filters.discipline.toLowerCase()
    : '';
  return list.filter(show => {
    const timestamp = getShowTimestamp(show);
    if(timestamp === null){
      return false;
    }
    if(includeDateFilter && startTs !== null && timestamp < startTs){
      return false;
    }
    if(includeDateFilter && endTs !== null && timestamp > endTs){
      return false;
    }
    if(disciplineFilter){
      const showDiscipline = typeof show.disciplineId === 'string' ? show.disciplineId.trim().toLowerCase() : '';
      if(showDiscipline !== disciplineFilter){
        return false;
      }
    }
    if(operatorFilter){
      if(!showIncludesOperator(show, operatorFilter)){
        return false;
      }
    }
    return true;
  });
}

function getCalendarMonthStart(){
  const month = state.calendarMonth instanceof Date ? state.calendarMonth : new Date();
  return getMonthStart(month);
}

function getMonthStart(value){
  const date = value instanceof Date ? new Date(value) : new Date();
  date.setHours(0,0,0,0);
  date.setDate(1);
  return date;
}

function formatDayKey(value){
  if(value instanceof Date){
    const iso = value.toISOString();
    return iso.slice(0, 10);
  }
  if(typeof value === 'string'){
    return value.slice(0, 10);
  }
  return '';
}

function parseDayKey(key){
  if(!key){
    return null;
  }
  const date = new Date(key);
  return Number.isNaN(date.getTime()) ? null : date;
}

function changeCalendarMonth(delta){
  const start = getCalendarMonthStart();
  start.setMonth(start.getMonth() + delta);
  state.calendarMonth = start;
  const activeDate = parseDayKey(state.activeCalendarDayKey) || new Date();
  if(activeDate.getMonth() !== start.getMonth() || activeDate.getFullYear() !== start.getFullYear()){
    const nextDate = new Date(start);
    nextDate.setDate(Math.min(activeDate.getDate() || 1, daysInMonth(start)));
    state.activeCalendarDayKey = formatDayKey(nextDate);
  }
  renderCalendar();
}

function daysInMonth(date){
  const working = date instanceof Date ? new Date(date) : new Date();
  return new Date(working.getFullYear(), working.getMonth() + 1, 0).getDate();
}

function buildCalendarDayMap(events = state.calendarEvents){
  const map = new Map();
  (Array.isArray(events) ? events : []).forEach(event => {
    const key = event.dayKey || formatDayKey(event.startDate || event.start);
    if(!key){
      return;
    }
    if(!map.has(key)){
      map.set(key, []);
    }
    map.get(key).push(event);
  });
  map.forEach(list => list.sort((a, b)=> (a.startTs || 0) - (b.startTs || 0)));
  return map;
}

function renderCalendar(){
  if(!calendarGrid){
    return;
  }
  const monthStart = getCalendarMonthStart();
  state.calendarMonth = monthStart;
  const todayKey = formatDayKey(new Date());
  const activeDay = state.activeCalendarDayKey || todayKey;
  if(calendarMonthLabel){
    calendarMonthLabel.textContent = monthStart.toLocaleDateString(undefined, {month: 'long', year: 'numeric'});
  }
  const dayMap = buildCalendarDayMap();
  const firstDay = new Date(monthStart);
  const leading = firstDay.getDay();
  const totalDays = daysInMonth(monthStart);
  const cells = [];
  for(let i = 0; i < leading; i += 1){
    cells.push('<div class="calendar-cell is-empty" aria-hidden="true"></div>');
  }
  for(let day = 1; day <= totalDays; day += 1){
    const currentDate = new Date(monthStart);
    currentDate.setDate(day);
    const key = formatDayKey(currentDate);
    const events = dayMap.get(key) || [];
    const isToday = key === todayKey;
    const isActive = key === activeDay;
    const classes = ['calendar-cell'];
    if(isToday){ classes.push('is-today'); }
    if(isActive){ classes.push('is-active'); }
    const chips = events.slice(0, 3).map(event => {
      const label = escapeHtml(event.title || 'Event');
      const time = event.allDay ? 'All day' : formatTimeLabel(event.startDate || event.start);
      return `<span class="calendar-chip" title="${label}${time ? ` • ${escapeHtml(time)}` : ''}">${label}</span>`;
    }).join('');
    const moreLabel = events.length > 3 ? `<span class="calendar-chip more">+${events.length - 3} more</span>` : '';
    cells.push(`
      <button type="button" class="${classes.join(' ')}" data-day-key="${key}" aria-pressed="${isActive ? 'true' : 'false'}">
        <span class="calendar-date">${day}</span>
        <div class="calendar-chip-group">${chips}${moreLabel}</div>
      </button>
    `);
  }
  calendarGrid.innerHTML = cells.join('');
  renderCalendarDayDetails(activeDay, dayMap);
}

function renderCalendarDayDetails(dayKey, dayMap = buildCalendarDayMap()){
  if(!calendarDayDetails){
    return;
  }
  const key = dayKey || formatDayKey(new Date());
  const date = parseDayKey(key);
  const events = dayMap.get(key) || [];
  const targetCell = calendarGrid ? calendarGrid.querySelector(`[data-day-key="${key}"]`) : null;
  calendarDayDetails.classList.toggle('is-visible', Boolean(targetCell));
  if(calendarDayTitle){
    calendarDayTitle.textContent = date ? date.toLocaleDateString(undefined, {weekday: 'long', month: 'long', day: 'numeric'}) : 'Selected day';
  }
  if(calendarDaySubtitle){
    calendarDaySubtitle.textContent = events.length ? `${events.length} event${events.length === 1 ? '' : 's'}` : 'No events scheduled';
  }
  if(calendarEventList){
    if(!events.length){
      calendarEventList.innerHTML = '<p class="help">No events for this day.</p>';
    }else{
      calendarEventList.innerHTML = events.map(event => {
        const timeLabel = event.allDay ? 'All day' : formatTimeRange(event.startDate, event.endDate);
        const location = event.location ? `<span class="calendar-meta">${escapeHtml(event.location)}</span>` : '';
        const description = event.description ? `<p class="help">${escapeHtml(event.description)}</p>` : '';
        return `
          <article class="calendar-event">
            <h4>${escapeHtml(event.title || 'Event')}</h4>
            <div class="calendar-meta">${escapeHtml(timeLabel || '')}</div>
            ${location}
            ${description}
          </article>
        `;
      }).join('');
    }
  }
  state.activeCalendarDayKey = key;
  positionCalendarDayDetails(key, targetCell);
}

function positionCalendarDayDetails(dayKey, targetCell){
  if(!calendarDayDetails || !calendarGrid){
    return;
  }
  const cell = targetCell || calendarGrid.querySelector(`[data-day-key="${dayKey}"]`);
  calendarDayDetails.classList.toggle('is-visible', Boolean(cell));
  if(!cell){
    return;
  }
  const shell = calendarLayout || calendarGrid;
  const shellRect = shell.getBoundingClientRect();
  const cellRect = cell.getBoundingClientRect();
  const detailWidth = calendarDayDetails.offsetWidth || 0;
  const desiredLeft = cellRect.left - shellRect.left + (cellRect.width / 2) - (detailWidth / 2);
  const maxLeft = (shell.clientWidth || shellRect.width) - detailWidth - 12;
  const left = Math.max(12, Math.min(desiredLeft, maxLeft));
  const top = cellRect.bottom - shellRect.top + 12;
  const arrowOffset = (cellRect.left - shellRect.left + (cellRect.width / 2)) - left;
  calendarDayDetails.style.left = `${left}px`;
  calendarDayDetails.style.top = `${top}px`;
  calendarDayDetails.style.setProperty('--popover-arrow-left', `${Math.max(18, Math.min(arrowOffset, Math.max(detailWidth - 18, 18)))}px`);
}

function onCalendarGridClick(event){
  const target = event.target.closest('[data-day-key]');
  if(!target){
    return;
  }
  const key = target.dataset.dayKey;
  if(!key){
    return;
  }
  state.activeCalendarDayKey = key;
  renderCalendar();
}

function formatTimeLabel(date){
  if(!(date instanceof Date)){
    const parsed = parseDayKey(date);
    if(!parsed){
      return '';
    }
    return parsed.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
  }
  if(Number.isNaN(date.getTime())){
    return '';
  }
  return date.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
}

function formatTimeRange(startDate, endDate){
  const startLabel = formatTimeLabel(startDate);
  const endLabel = formatTimeLabel(endDate);
  if(startLabel && endLabel){
    return `${startLabel} – ${endLabel}`;
  }
  return startLabel || endLabel || '';
}

function getArchiveOperatorNames(shows = []){
  const collections = [];
  (Array.isArray(shows) ? shows : []).forEach(show => {
    const lookup = collectShowOperatorMap(show);
    if(lookup.size){
      collections.push(lookup);
    }
  });
  if(!collections.length){
    return [];
  }
  let intersection = null;
  collections.forEach(map => {
    const keys = Array.from(map.keys());
    if(intersection === null){
      intersection = new Set(keys);
      return;
    }
    intersection = new Set(keys.filter(key => intersection.has(key)));
  });
  if(!intersection || !intersection.size){
    return [];
  }
  const canonical = new Map();
  collections.forEach(map => {
    intersection.forEach(key => {
      if(!canonical.has(key) && map.has(key)){
        canonical.set(key, map.get(key));
      }
    });
  });
  return Array.from(intersection)
    .map(key => canonical.get(key) || key)
    .sort((a, b)=> a.localeCompare(b, undefined, {sensitivity: 'base'}));
}

function collectShowOperatorMap(show){
  const map = new Map();
  const entries = Array.isArray(show?.entries) ? show.entries : [];
  entries.forEach(entry => {
    const name = typeof entry?.operator === 'string' ? entry.operator.trim() : '';
    if(!name){
      return;
    }
    const key = name.toLowerCase();
    if(!map.has(key)){
      map.set(key, name);
    }
  });
  return map;
}

function showIncludesOperator(show, operatorKey){
  if(!operatorKey){
    return true;
  }
  const lookup = collectShowOperatorMap(show);
  return lookup.has(operatorKey);
}

function parseFilterDate(value, endOfDay){
  if(typeof value !== 'string' || !value){
    return null;
  }
  const date = new Date(`${value}T00:00:00`);
  if(Number.isNaN(date.getTime())){
    return null;
  }
  if(endOfDay){
    date.setHours(23, 59, 59, 999);
  }
  return date.getTime();
}

function getSelectedArchiveChartShows(){
  const mode = getArchiveSelectionMode();
  const allShows = Array.isArray(state.archivedShows) ? state.archivedShows.slice() : [];
  if(mode === 'calendar'){
    const filtered = getFilteredArchivedShows(allShows);
    filtered.sort((a, b)=> (getShowTimestamp(a) ?? 0) - (getShowTimestamp(b) ?? 0));
    return filtered;
  }
  const selectedIds = new Set(Array.isArray(state.selectedArchiveChartShows) ? state.selectedArchiveChartShows : []);
  const selected = allShows.filter(show => selectedIds.has(show.id));
  const operatorFiltered = getFilteredArchivedShows(selected, {includeDateFilter: false});
  operatorFiltered.sort((a, b)=> (getShowTimestamp(a) ?? 0) - (getShowTimestamp(b) ?? 0));
  return operatorFiltered;
}

function buildArchiveChartData(shows, metrics){
  const axes = {};
  const datasets = [];
  const dailyGroups = buildArchiveDailyGroups(shows);

  metrics.forEach((metricKey, index) => {
    const metricDef = getArchiveMetricDef(metricKey);
    if(!metricDef || !metricDef.chartable){
      return;
    }
    const axisId = getMetricAxisId(metricKey, metricDef);
    if(!axes[axisId]){
      axes[axisId] = createAxisDescriptor(metricDef);
    }else{
      extendAxisDescriptor(axes[axisId], metricDef);
    }
    const color = ARCHIVE_CHART_COLORS[index % ARCHIVE_CHART_COLORS.length];
    const dataset = {
      label: metricDef.label,
      yAxisID: axisId,
      borderColor: color,
      backgroundColor: applyAlphaToColor(color, 0.25),
      tension: 0.28,
      borderWidth: 2,
      pointRadius: 4,
      pointHoverRadius: 6,
      pointBackgroundColor: color,
      pointBorderColor: '#0f172a',
      fill: false,
      spanGaps: true,
      parsing: false,
      archiveMetricDef: metricDef,
      archiveMetricKey: metricKey,
      data: dailyGroups.map(group => {
        const summary = getOrCreateGroupMetricSummary(group, metricKey, metricDef);
        const average = summary ? summary.average : null;
        return {
          x: group.midpoint,
          y: isValidMetricValue(average) ? Number(average) : null,
          dayKey: group.dateKey
        };
      })
    };
    updateAxisDataExtents(axes[axisId], dataset.data);
    datasets.push(dataset);
  });

  return {datasets, axes, dailyGroups};
}

function buildArchiveDailyGroups(shows){
  const groupsByKey = new Map();
  const list = Array.isArray(shows) ? shows : [];
  list.forEach(show => {
    if(!show){
      return;
    }
    const timestamp = getShowTimestamp(show);
    if(!Number.isFinite(timestamp)){
      return;
    }
    const dayStart = new Date(timestamp);
    dayStart.setHours(0, 0, 0, 0);
    const startTs = dayStart.getTime();
    const dateKey = dayStart.toISOString().slice(0, 10);
    let group = groupsByKey.get(dateKey);
    if(!group){
      group = {
        dateKey,
        timestamp: startTs,
        midpoint: startTs + 12 * 60 * 60 * 1000,
        shows: [],
        metrics: {},
        displayDate: formatArchiveDayLabel(startTs),
        totalShows: 0
      };
      groupsByKey.set(dateKey, group);
    }
    group.shows.push({
      show,
      stats: computeArchiveShowStats(show)
    });
  });
  const groups = Array.from(groupsByKey.values());
  groups.sort((a, b)=> a.timestamp - b.timestamp);
  groups.forEach(group => {
    group.totalShows = group.shows.length;
    if(!group.displayDate){
      group.displayDate = formatArchiveDayLabel(group.timestamp);
    }
  });
  return groups;
}

function getOrCreateGroupMetricSummary(group, metricKey, metricDef){
  if(!group){
    return null;
  }
  if(!group.metrics){
    group.metrics = {};
  }
  if(group.metrics[metricKey]){
    return group.metrics[metricKey];
  }
  const showValues = [];
  const numericValues = [];
  (group.shows || []).forEach(item => {
    const show = item?.show || null;
    const stats = item?.stats || null;
    if(!show){
      return;
    }
    const value = metricDef?.getValue ? metricDef.getValue(stats, show) : null;
    const numeric = isValidMetricValue(value) ? Number(value) : null;
    const formatted = formatMetricValue(metricDef, numeric);
    const label = buildArchiveShowLabel(show);
    const shortLabel = buildArchiveShowShortLabel(show);
    const entry = {
      showId: show.id,
      label,
      shortLabel,
      value: numeric,
      formatted
    };
    showValues.push(entry);
    if(Number.isFinite(numeric)){
      numericValues.push(numeric);
    }
  });
  const average = numericValues.length
    ? numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length
    : null;
  const min = numericValues.length ? Math.min(...numericValues) : null;
  const max = numericValues.length ? Math.max(...numericValues) : null;
  const valueMap = showValues.reduce((acc, entry) => {
    if(entry?.showId){
      acc[entry.showId] = entry;
    }
    return acc;
  }, {});
  const summary = {
    average,
    min,
    max,
    count: numericValues.length,
    totalShows: group.shows?.length || 0,
    showValues,
    valueMap
  };
  group.metrics[metricKey] = summary;
  return summary;
}

function getArchiveDayByKey(dayKey){
  if(!dayKey){
    return null;
  }
  const lookup = state.archiveDailyGroupsByKey || {};
  return lookup[dayKey] || null;
}

function updateAxisDataExtents(descriptor, data){
  if(!descriptor || !Array.isArray(data)){
    return;
  }
  const values = data
    .map(point => Number.isFinite(point?.y) ? Number(point.y) : null)
    .filter(value => value !== null);
  if(!values.length){
    return;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  descriptor.dataMin = typeof descriptor.dataMin === 'number' ? Math.min(descriptor.dataMin, min) : min;
  descriptor.dataMax = typeof descriptor.dataMax === 'number' ? Math.max(descriptor.dataMax, max) : max;
}

function buildArchiveChartOptions(axisDescriptors){
  const scales = {
    x: {
      type: 'time',
      grid: { color: 'rgba(148, 163, 184, 0.18)' },
      ticks: {
        color: 'rgba(226, 232, 240, 0.85)',
        maxRotation: 0,
        autoSkipPadding: 16
      },
      time: {
        tooltipFormat: 'PP p',
        displayFormats: {
          hour: 'MMM d ha',
          day: 'MMM d'
        }
      }
    }
  };
  const axisIds = Object.keys(axisDescriptors);
  axisIds.forEach((axisId, index) => {
    const descriptor = axisDescriptors[axisId];
    const position = axisId === 'y-seconds' ? 'right' : 'left';
    const drawGrid = index === 0;
    const suggestedMin = collectAxisBound(descriptor, 'min');
    const suggestedMax = collectAxisBound(descriptor, 'max');
    scales[axisId] = {
      type: 'linear',
      position,
      grid: {
        drawOnChartArea: drawGrid,
        color: drawGrid ? 'rgba(148, 163, 184, 0.18)' : 'rgba(148, 163, 184, 0.08)'
      },
      ticks: {
        color: 'rgba(226, 232, 240, 0.85)',
        padding: 8,
        callback: value => formatChartAxisTick(descriptor, value)
      },
      suggestedMin,
      suggestedMax,
      beginAtZero: descriptor?.min === 0,
      offset: position === 'left' && index > 0
    };
    if(position === 'right'){
      scales[axisId].grid.drawOnChartArea = false;
    }
  });

  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: {
      duration: 700,
      easing: 'easeOutQuart'
    },
    interaction: {
      mode: 'index',
      intersect: false
    },
    plugins: {
      legend: {
        labels: {
          color: '#f8fafc',
          usePointStyle: true,
          boxWidth: 12
        }
      },
      tooltip: {
        backgroundColor: 'rgba(15, 23, 42, 0.92)',
        borderColor: 'rgba(59, 130, 246, 0.45)',
        borderWidth: 1,
        displayColors: false,
        padding: 10,
        bodySpacing: 6,
        titleSpacing: 4,
        boxPadding: 4,
        caretSize: 6,
        callbacks: {
          title: items => formatArchiveTooltipTitleFromItems(items),
          label: context => formatArchiveTooltipLabel(context),
          afterBody: items => formatArchiveTooltipBreakdown(items)
        }
      }
    },
    scales
  };
}

function collectAxisBound(descriptor, key){
  if(!descriptor){
    return undefined;
  }
  const bounds = [];
  if(typeof descriptor[key] === 'number'){
    bounds.push(descriptor[key]);
  }
  const dataKey = key === 'min' ? 'dataMin' : 'dataMax';
  if(typeof descriptor[dataKey] === 'number'){
    bounds.push(descriptor[dataKey]);
  }
  if(!bounds.length){
    return undefined;
  }
  return key === 'min' ? Math.min(...bounds) : Math.max(...bounds);
}

function formatChartAxisTick(descriptor, value){
  if(!descriptor){
    return value;
  }
  return formatChartAxisValue({suffix: descriptor.suffix, decimals: descriptor.decimals}, value);
}

function formatArchiveTooltipTitle(value){
  if(!Number.isFinite(value)){
    return '';
  }
  try{
    return new Date(value).toLocaleString(undefined, {
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit'
    });
  }catch(err){
    return '';
  }
}

function formatArchiveTooltipLabel(context){
  if(!context || !context.dataset){
    return '';
  }
  const dataset = context.dataset;
  const def = dataset.archiveMetricDef || null;
  const dayKey = context.raw?.dayKey;
  if(dayKey && dataset.archiveMetricKey && def){
    const day = getArchiveDayByKey(dayKey);
    if(day){
      const summary = day.metrics?.[dataset.archiveMetricKey]
        || getOrCreateGroupMetricSummary(day, dataset.archiveMetricKey, def);
      if(summary){
        const formattedAvg = formatMetricValue(def, summary.average);
        return `${dataset.label}: ${formattedAvg}`;
      }
    }
  }
  const value = context.parsed?.y;
  const formatted = def ? formatMetricValue(def, value) : (Number.isFinite(value) ? value : '—');
  return `${dataset.label}: ${formatted}`;
}

function formatArchiveTooltipTitleFromItems(items){
  const first = items?.[0];
  if(first?.raw?.dayKey){
    const day = getArchiveDayByKey(first.raw.dayKey);
    if(day){
      return day.displayDate || formatArchiveDayLabel(day.timestamp);
    }
  }
  return formatArchiveTooltipTitle(first?.parsed?.x);
}

function formatArchiveTooltipBreakdown(items){
  const first = items?.[0];
  if(!first?.raw?.dayKey){
    return [];
  }
  const day = getArchiveDayByKey(first.raw.dayKey);
  if(!day){
    return [];
  }
  const showCount = Array.isArray(day.shows) ? day.shows.length : 0;
  const lines = [`${showCount} ${showCount === 1 ? 'show' : 'shows'} logged`];
  const metrics = getActiveArchiveMetricKeys();
  metrics.forEach(metricKey => {
    const def = getArchiveMetricDef(metricKey);
    if(!def){
      return;
    }
    const summary = day.metrics?.[metricKey] || getOrCreateGroupMetricSummary(day, metricKey, def);
    if(!summary){
      return;
    }
    const averageText = formatMetricValue(def, summary.average);
    const countText = typeof summary.count === 'number' ? ` • n=${summary.count}` : '';
    lines.push(`${def.label}: ${averageText}${countText}`);
  });
  return lines;
}

function getMetricAxisId(metricKey, metricDef){
  const suffix = typeof metricDef?.suffix === 'string' ? metricDef.suffix.trim() : '';
  if(suffix === '%'){
    return 'y-percent';
  }
  if(suffix.toLowerCase().includes('s')){
    return 'y-seconds';
  }
  return `y-${metricKey}`;
}

function createAxisDescriptor(metricDef){
  return {
    suffix: typeof metricDef?.suffix === 'string' ? metricDef.suffix : '',
    min: typeof metricDef?.min === 'number' ? metricDef.min : undefined,
    max: typeof metricDef?.max === 'number' ? metricDef.max : undefined,
    decimals: typeof metricDef?.decimals === 'number' ? metricDef.decimals : 0,
    dataMin: undefined,
    dataMax: undefined
  };
}

function extendAxisDescriptor(descriptor, metricDef){
  if(!descriptor){
    return;
  }
  if(typeof metricDef?.min === 'number'){
    descriptor.min = typeof descriptor.min === 'number' ? Math.min(descriptor.min, metricDef.min) : metricDef.min;
  }
  if(typeof metricDef?.max === 'number'){
    descriptor.max = typeof descriptor.max === 'number' ? Math.max(descriptor.max, metricDef.max) : metricDef.max;
  }
  const decimals = typeof metricDef?.decimals === 'number' ? metricDef.decimals : 0;
  descriptor.decimals = Math.max(descriptor.decimals || 0, decimals);
}

function applyAlphaToColor(color, alpha){
  if(typeof color !== 'string'){
    return `rgba(34, 197, 94, ${Math.max(0, Math.min(1, alpha || 0))})`;
  }
  const hex = color.replace('#', '');
  if(hex.length !== 6){
    const normalized = Math.max(0, Math.min(1, alpha || 0));
    return `rgba(37, 99, 235, ${normalized})`;
  }
  const r = Number.parseInt(hex.slice(0, 2), 16);
  const g = Number.parseInt(hex.slice(2, 4), 16);
  const b = Number.parseInt(hex.slice(4, 6), 16);
  const normalized = Math.max(0, Math.min(1, Number(alpha) || 0));
  return `rgba(${r}, ${g}, ${b}, ${normalized})`;
}

function renderArchiveDetails(show){
  if(!archiveDetails){
    return;
  }
  if(!show){
    archiveDetails.innerHTML = '<p class="help">Select an archived show to review its summary.</p>';
    if(archiveMeta){
      archiveMeta.textContent = 'Shows move here automatically 12 hours after creation and remain for two months.';
    }
    if(archiveEmpty){
      archiveEmpty.hidden = !(Array.isArray(state.archivedShows) && state.archivedShows.length === 0);
    }
    if(archiveExportCsvBtn){ archiveExportCsvBtn.disabled = true; }
    if(archiveExportJsonBtn){ archiveExportJsonBtn.disabled = true; }
    return;
  }
  if(archiveEmpty){
    archiveEmpty.hidden = true;
  }
  const crewList = Array.isArray(show.crew) && show.crew.length ? show.crew.join(', ') : '—';
  const entries = Array.isArray(show.entries) ? show.entries : [];
  const deletedOn = show.deletedAt ? (formatDateTime(show.deletedAt) || '—') : '';
  const rows = [
    ['Status', show.deletedAt ? 'Deleted' : 'Archived'],
    ['Date', formatDateUS(show.date) || show.date || '—'],
    ['Time', formatTime12Hour(show.time) || show.time || '—'],
    ['Label', show.label || '—'],
    ['Lead', show.leadPilot || '—'],
    ['Crew lead', show.monkeyLead || '—'],
    ['Crew', crewList],
    ['Entries logged', entries.length]
  ];
  if(show.deletedAt){
    rows.splice(1, 0, ['Deleted on', deletedOn]);
  }
  const entryCountLabel = entries.length
    ? `${entries.length} ${entries.length === 1 ? 'entry' : 'entries'} logged`
    : 'No entries logged';
  const entriesMarkup = entries.length
    ? entries.map((entry, index)=> renderArchiveEntry(entry, index)).join('')
    : '<p class="archive-empty-msg">No entries recorded for this show yet.</p>';
  archiveDetails.innerHTML = `
    <div class="archive-card">
      <dl class="archive-info">
        ${rows.map(([label, value])=> renderArchiveMeta(label, value)).join('')}
      </dl>
      ${show.notes ? `<div class="archive-notes"><h3>Show notes</h3><p>${escapeHtml(show.notes)}</p></div>` : ''}
    </div>
    <div class="archive-entries">
      <div class="archive-entries-header">
        <h3>Entries</h3>
        <span class="archive-entries-count">${escapeHtml(entryCountLabel)}</span>
      </div>
      ${entriesMarkup}
    </div>
  `;
  if(archiveMeta){
    const archived = formatDateTime(show.archivedAt);
    const created = formatDateTime(show.createdAt);
    const metaParts = [];
    const deleted = formatDateTime(show.deletedAt);
    if(deleted){ metaParts.push(`Deleted ${deleted}`); }
    if(archived){ metaParts.push(`Archived ${archived}`); }
    if(created){ metaParts.push(`Created ${created}`); }
    archiveMeta.textContent = metaParts.join(' • ');
  }
  if(archiveExportCsvBtn){ archiveExportCsvBtn.disabled = false; }
  if(archiveExportJsonBtn){ archiveExportJsonBtn.disabled = false; }
}

function renderArchiveMeta(label, value){
  const text = value === undefined || value === null || value === '' ? '—' : String(value);
  return `<div><dt>${escapeHtml(label)}</dt><dd>${escapeHtml(text)}</dd></div>`;
}

function renderArchiveEntry(entry, index){
  const statusKey = String(entry.status || '').toLowerCase();
  let statusClass = 'status-completed';
  if(statusKey === 'no-launch'){
    statusClass = 'status-no-launch';
  }else if(statusKey === 'abort'){
    statusClass = 'status-abort';
  }
  const planned = entry.planned || '—';
  const launched = entry.launched || '—';
  const operatorName = entry.operator || '—';
  const battery = entry.batteryId || '—';
  const delayValue = (typeof entry.delaySec === 'number' && Number.isFinite(entry.delaySec))
    ? `${entry.delaySec} s`
    : '—';
  const primaryIssue = statusClass === 'status-completed' ? '—' : (entry.primaryIssue || '—');
  const issueDetail = statusClass === 'status-completed' ? '—' : (entry.subIssue || entry.otherDetail || '—');
  const severity = statusClass === 'status-completed' ? '—' : (entry.severity || '—');
  const rootCause = statusClass === 'status-completed' ? '—' : (entry.rootCause || '—');
  const actionsList = Array.isArray(entry.actions) && entry.actions.length ? entry.actions.join(', ') : '—';
  const commandRx = entry.commandRx || '—';
  const timestamp = formatDateTime(entry.ts);
  const unitLabel = entry.unitId || `Entry ${index + 1}`;
  const notesSection = entry.notes ? `<div class="archive-entry-notes"><h4>Notes</h4><p>${escapeHtml(entry.notes)}</p></div>` : '';
  const headerTimestamp = timestamp ? `<span class="archive-entry-timestamp">${escapeHtml(timestamp)}</span>` : '';
  return `
    <article class="archive-entry ${statusClass}">
      <header class="archive-entry-header">
        <div class="archive-entry-heading">
          <span class="archive-entry-unit">${escapeHtml(unitLabel)}</span>
          <span class="archive-entry-badge ${statusClass}">${escapeHtml(entry.status || '—')}</span>
        </div>
        ${headerTimestamp}
      </header>
      <dl class="archive-entry-grid">
        ${renderArchiveMeta('Planned', planned)}
        ${renderArchiveMeta('Launched', launched)}
        ${renderArchiveMeta('Operator', operatorName)}
        ${renderArchiveMeta('Battery', battery)}
        ${renderArchiveMeta('Delay', delayValue)}
        ${renderArchiveMeta('Primary issue', primaryIssue)}
        ${renderArchiveMeta('Issue detail', issueDetail)}
        ${renderArchiveMeta('Severity', severity)}
        ${renderArchiveMeta('Root cause', rootCause)}
        ${renderArchiveMeta('Actions', actionsList)}
        ${renderArchiveMeta('Command RX', commandRx)}
      </dl>
      ${notesSection}
    </article>
  `;
}

function computeArchiveShowStats(show){
  const entries = Array.isArray(show?.entries) ? show.entries : [];
  let completedCount = 0;
  let noLaunchCount = 0;
  let abortCount = 0;
  let launchedCount = 0;
  const delayValues = [];
  const issueCounts = {};
  for(const entry of entries){
    const status = String(entry?.status || '').toLowerCase();
    if(status === 'completed'){
      completedCount += 1;
    }else if(status === 'no-launch'){
      noLaunchCount += 1;
    }else if(status === 'abort'){
      abortCount += 1;
    }
    if(String(entry?.launched || '').toLowerCase() === 'yes'){
      launchedCount += 1;
    }
    if(Number.isFinite(entry?.delaySec)){
      delayValues.push(entry.delaySec);
    }
    const issue = typeof entry?.primaryIssue === 'string' ? entry.primaryIssue.trim() : '';
    if(issue){
      const normalized = PRIMARY_ISSUES.includes(issue) ? issue : 'Other';
      issueCounts[normalized] = (issueCounts[normalized] || 0) + 1;
    }
  }
  const totalEntries = entries.length;
  const delaySum = delayValues.reduce((sum, value)=> sum + value, 0);
  const avgDelaySec = delayValues.length ? delaySum / delayValues.length : null;
  const maxDelaySec = delayValues.length ? Math.max(...delayValues) : null;
  const completionRate = totalEntries ? (completedCount / totalEntries) * 100 : null;
  const launchRate = totalEntries ? (launchedCount / totalEntries) * 100 : null;
  const abortRate = totalEntries ? (abortCount / totalEntries) * 100 : null;
  const issueRates = {};
  PRIMARY_ISSUES.forEach(issue => {
    const count = issueCounts[issue] || 0;
    issueRates[issue] = totalEntries ? (count / totalEntries) * 100 : null;
  });
  return {
    totalEntries,
    completedCount,
    noLaunchCount,
    abortCount,
    launchedCount,
    avgDelaySec,
    maxDelaySec,
    completionRate,
    launchRate,
    abortRate,
    issueCounts,
    issueRates
  };
}

function getArchiveMetricDef(key){
  if(!key){
    return null;
  }
  if(ARCHIVE_METRIC_DEFS[key]){
    return ARCHIVE_METRIC_DEFS[key];
  }
  if(isIssueMetricKey(key)){
    if(issueMetricDefCache.has(key)){
      return issueMetricDefCache.get(key);
    }
    const issue = getIssueFromMetricKey(key);
    if(!issue || !PRIMARY_ISSUES.includes(issue)){
      return null;
    }
    const def = {
      label: `${issue} frequency (%)`,
      buttonLabel: issue,
      suffix: '%',
      decimals: 0,
      min: 0,
      max: 100,
      chartable: true,
      getValue: stats => {
        if(!stats){
          return null;
        }
        const rates = stats.issueRates || null;
        if(rates && Object.prototype.hasOwnProperty.call(rates, issue)){
          const value = rates[issue];
          return Number.isFinite(value) ? value : (value === 0 ? 0 : null);
        }
        return null;
      }
    };
    issueMetricDefCache.set(key, def);
    return def;
  }
  return null;
}

function formatMetricValue(def, value){
  if(value === null || value === undefined || Number.isNaN(value)){
    return '—';
  }
  const decimals = typeof def?.decimals === 'number' ? def.decimals : 0;
  const number = Number(value);
  if(!Number.isFinite(number)){
    return '—';
  }
  const formatted = number.toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals
  });
  return def?.suffix ? `${formatted}${def.suffix}` : formatted;
}

function formatChartAxisValue(def, value){
  if(value === null || value === undefined || Number.isNaN(value)){
    return '';
  }
  const decimals = typeof def?.decimals === 'number' ? Math.min(def.decimals, 2) : 0;
  const number = Number(value);
  if(!Number.isFinite(number)){
    return '';
  }
  const formatted = number.toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals
  });
  const suffix = typeof def?.suffix === 'string' ? def.suffix.trim() : '';
  if(!suffix){
    return formatted;
  }
  if(suffix === '%'){
    return `${formatted}%`;
  }
  return `${formatted} ${suffix}`;
}

function formatArchiveChartDate(timestamp){
  if(!Number.isFinite(timestamp)){
    return '';
  }
  try{
    return new Date(timestamp).toLocaleDateString(undefined, {
      month: 'short',
      day: 'numeric'
    });
  }catch(err){
    return '';
  }
}

function formatArchiveDayLabel(timestamp){
  if(!Number.isFinite(timestamp)){
    return '';
  }
  try{
    return new Date(timestamp).toLocaleDateString(undefined, {
      month: 'long',
      day: 'numeric',
      year: 'numeric'
    });
  }catch(err){
    return '';
  }
}

function buildArchiveShowLabel(show){
  if(!show){
    return '';
  }
  const date = formatDateUS(show.date) || show.date || 'Unknown date';
  const time = formatTime12Hour(show.time) || '';
  const label = show.label ? ` • ${show.label}` : '';
  return `${date}${time ? ` • ${time}` : ''}${label}`;
}

function buildArchiveShowShortLabel(show){
  if(!show){
    return '';
  }
  const label = typeof show.label === 'string' ? show.label.trim() : '';
  const time = formatTime12Hour(show.time) || '';
  if(label && time){
    return `${time} • ${label}`;
  }
  if(label){
    return label;
  }
  if(time){
    return time;
  }
  return 'Show';
}

function getShowTimestamp(show){
  if(!show){
    return null;
  }
  if(Number.isFinite(show.createdAt)){
    return show.createdAt;
  }
  const parsed = parseShowDateTime(show.date, show.time);
  if(parsed !== null){
    return parsed;
  }
  if(Number.isFinite(show.archivedAt)){
    return show.archivedAt;
  }
  if(Array.isArray(show.entries) && show.entries.length){
    const sorted = show.entries
      .map(entry => Number.isFinite(entry.ts) ? entry.ts : null)
      .filter(ts => ts !== null)
      .sort((a, b)=> a - b);
    if(sorted.length){
      return sorted[0];
    }
  }
  return null;
}

function parseShowDateTime(dateStr, timeStr){
  if(typeof dateStr !== 'string' || !dateStr){
    return null;
  }
  const time = (typeof timeStr === 'string' && timeStr) ? timeStr : '00:00';
  const iso = `${dateStr}T${time}`;
  const ts = Date.parse(iso);
  return Number.isFinite(ts) ? ts : null;
}

function isValidMetricValue(value){
  if(value === null || value === undefined){
    return false;
  }
  const number = Number(value);
  return Number.isFinite(number);
}

function syncArchiveChartSelection(){
  const mode = getArchiveSelectionMode();
  const shows = Array.isArray(state.archivedShows) ? state.archivedShows : [];
  if(mode === 'calendar'){
    const filtered = getFilteredArchivedShows(shows);
    state.selectedArchiveChartShows = filtered.map(show => show.id);
    return;
  }
  const operatorFiltered = getFilteredArchivedShows(shows, {includeDateFilter: false});
  if(!Array.isArray(state.selectedArchiveChartShows)){
    state.selectedArchiveChartShows = operatorFiltered.length
      ? operatorFiltered.slice(0, Math.min(5, operatorFiltered.length)).map(show => show.id)
      : [];
    return;
  }
  const available = new Set(operatorFiltered.map(show => show.id));
  const nextSelection = state.selectedArchiveChartShows.filter(id => available.has(id));
  state.selectedArchiveChartShows = nextSelection;
}

function exportSelectedArchive(format){
  const show = getArchivedShow(state.currentArchivedShowId);
  if(!show){
    toast('Select an archived show first', true);
    return;
  }
  if(format === 'csv'){
    exportShowAsCsv(show);
  }else{
    exportShowAsJson(show);
  }
}

function upsertShow(show){
  const normalized = normalizeActiveShow(show);
  const idx = state.shows.findIndex(s=>s.id===normalized.id);
  if(idx >= 0){
    state.shows[idx] = normalized;
  }else{
    state.shows.unshift(normalized);
  }
  sortShows();
}

function sortShows(){
  state.shows.sort((a,b)=>{
    const au = a.updatedAt || a.createdAt || 0;
    const bu = b.updatedAt || b.createdAt || 0;
    return bu - au;
  });
}

function populateUnitOptions(){
  const units = getDefaultUnits();
  const currentValue = unitId.value;
  unitId.innerHTML = '<option value="">Select</option>' + units.map(u=>`<option ${currentValue===u?'selected':''}>${u}</option>`).join('');
  unitLabelEl.textContent = state.unitLabel;
  updateDisciplineHeader();
}

function populateIssues(){
  primaryIssue.innerHTML = '<option value="">Select</option>' + PRIMARY_ISSUES.map(issue=>`<option value="${issue}">${issue}</option>`).join('');
  populateSubIssues(primaryIssue.value);
}

function populateSubIssues(primary){
  const options = ISSUE_MAP[primary] || [];
  subIssue.innerHTML = '<option value="">N/A</option>' + options.map(opt=>`<option value="${opt}">${opt}</option>`).join('');
}

function renderActionsChips(container, selected){
  container.innerHTML = '';
  ACTIONS.forEach(action=>{
    const chip = document.createElement('button');
    chip.type = 'button';
    chip.className = 'action-chip';
    chip.textContent = action;
    const isSelected = selected.includes(action);
    chip.setAttribute('aria-pressed', isSelected ? 'true' : 'false');
    chip.addEventListener('click', ()=>{
      const pressed = chip.getAttribute('aria-pressed') === 'true';
      chip.setAttribute('aria-pressed', pressed ? 'false' : 'true');
    });
    container.appendChild(chip);
  });
}

function getSelectedActions(container){
  return qsa('.action-chip', container).filter(chip=>chip.getAttribute('aria-pressed') === 'true').map(chip=>chip.textContent);
}

function updateIssueVisibility(){
  const st = getStatus();
  const showIssues = st && st !== 'Completed';
  issueBlocks.forEach(block=> block.classList.toggle('hidden', !showIssues));
  const isOther = primaryIssue.value === 'Other';
  otherDetailWrap.classList.toggle('hidden', !showIssues || !isOther);
}

function getStatus(){
  const selected = [stCompleted, stNoLaunch, stAbort].find(btn=>btn.getAttribute('aria-pressed') === 'true');
  return selected ? selected.dataset.status : '';
}

function setStatus(status){
  [stCompleted, stNoLaunch, stAbort].forEach(btn=>{
    btn.setAttribute('aria-pressed', btn.dataset.status === status ? 'true' : 'false');
  });
}

function onPlanLaunchChange(){
  const st = getStatus();
  if(planned.value === 'No' && st && st !== 'No-launch'){
    setStatus('No-launch');
  }
  if(launched.value === 'No' && st && st !== 'No-launch'){
    setStatus('No-launch');
  }
  if(launched.value === 'Yes' && st === 'No-launch'){
    setStatus('Completed');
  }
  updateIssueVisibility();
}

function collectShowHeaderValues(){
  return {
    date: showDate?.value || '',
    time: showTime?.value || '',
    label: showLabel?.value.trim() || '',
    leadPilot: leadPilotSelect?.value?.trim() || '',
    monkeyLead: monkeyLeadSelect?.value?.trim() || '',
    notes: showNotes?.value.trim() || '',
    disciplineId: getActiveDisciplineId()
  };
}

function getNewShowDraft(){
  if(!state.newShowDraft){
    state.newShowDraft = createEmptyShowDraft();
  }
  return state.newShowDraft;
}

function renderShowHeaderDraft(){
  const draft = getNewShowDraft();
  if(showDate){ showDate.value = draft.date || ''; }
  if(showTime){ showTime.value = draft.time || ''; }
  if(showLabel){ showLabel.value = draft.label || ''; }
  if(showNotes){ showNotes.value = draft.notes || ''; }
  renderPilotAssignments(draft);
  ensureShowHeaderValid();
}

function resetShowHeaderDraft(){
  state.newShowDraft = createEmptyShowDraft();
  state.showHeaderShowErrors = false;
  renderShowHeaderDraft();
}

function updateNewShowDraft(field, value){
  const draft = getNewShowDraft();
  draft[field] = value;
}

function handleShowHeaderChange(field, value){
  updateNewShowDraft(field, value);
  ensureShowHeaderValid();
}

function ensureShowHeaderValid(values, options = {}){
  const {showErrors = false} = options;
  const headerValues = values || collectShowHeaderValues();
  if(showErrors){
    state.showHeaderShowErrors = true;
  }
  const shouldShowErrors = state.showHeaderShowErrors || showErrors;
  const requiredFields = [
    {key: 'date', label: 'Date', element: showDate},
    {key: 'time', label: 'Show start time', element: showTime},
    {key: 'label', label: 'Show label', element: showLabel},
    {key: 'leadPilot', label: 'Lead', element: leadPilotSelect},
    {key: 'monkeyLead', label: 'Crew lead', element: monkeyLeadSelect}
  ];
  let firstInvalid = null;
  requiredFields.forEach(field =>{
    const rawValue = headerValues[field.key];
    const normalized = typeof rawValue === 'string' ? rawValue.trim() : rawValue;
    const isValid = Boolean(normalized);
    setFieldValidity(field.element, isValid, shouldShowErrors);
    if(!isValid && !firstInvalid){
      firstInvalid = field;
    }
  });
  const isValid = !firstInvalid;
  if(newShowBtn){
    if(state.isCreatingShow){
      newShowBtn.disabled = true;
    }else{
      newShowBtn.disabled = !isValid;
    }
  }
  if(showErrors && firstInvalid){
    toast(`${firstInvalid.label} is required`, true);
    if(firstInvalid.element && typeof firstInvalid.element.focus === 'function'){
      firstInvalid.element.focus();
    }
  }
  return isValid;
}

function setFieldValidity(element, isValid, showError){
  if(!element){
    return;
  }
  if(!showError){
    element.classList.remove('is-invalid');
    element.removeAttribute('aria-invalid');
    return;
  }
  if(isValid){
    element.classList.remove('is-invalid');
    element.removeAttribute('aria-invalid');
  }else{
    element.classList.add('is-invalid');
    element.setAttribute('aria-invalid', 'true');
  }
}

function setShowHeaderDisabled(disabled){
  const controls = [showDate, showTime, showLabel, showNotes, leadPilotSelect, monkeyLeadSelect];
  controls.forEach(control =>{
    if(!control){
      return;
    }
    if(disabled){
      control.dataset.prevDisabled = control.disabled ? 'true' : 'false';
      control.disabled = true;
    }else{
      if(control.dataset.prevDisabled === 'true'){
        control.disabled = true;
      }else{
        control.disabled = false;
      }
      delete control.dataset.prevDisabled;
    }
  });
  if(!disabled){
    renderShowHeaderDraft();
  }
}

function setNewShowButtonBusy(busy){
  if(!newShowBtn){
    return;
  }
  if(!newShowBtn.dataset.originalLabel){
    newShowBtn.dataset.originalLabel = newShowBtn.textContent || 'Add show';
  }
  newShowBtn.textContent = busy ? 'Adding…' : newShowBtn.dataset.originalLabel;
  if(busy){
    newShowBtn.disabled = true;
  }else{
    ensureShowHeaderValid();
  }
}

async function onNewShow(){
  closeAllShowMenus();
  if(state.isCreatingShow){
    return;
  }
  const headerValues = collectShowHeaderValues();
  const isValid = ensureShowHeaderValid(headerValues, {showErrors: true});
  if(!isValid){
    return;
  }
  const previousId = state.currentShowId;
  state.isCreatingShow = true;
  setShowHeaderDisabled(true);
  setNewShowButtonBusy(true);
  try{
    const payload = await apiRequest('/api/shows', {method:'POST', body: JSON.stringify(headerValues)});
    upsertShow(payload);
    setCurrentShow(payload.id);
    notifyShowsChanged({showId: payload.id});
    clearEntryForm();
    toast('New show created');
    resetShowHeaderDraft();
  }catch(err){
    console.error(err);
    toast('Failed to create show', true);
    const fallbackId = previousId && state.shows.some(show => show.id === previousId)
      ? previousId
      : (state.shows[0]?.id || null);
    setCurrentShow(fallbackId);
  }finally{
    state.isCreatingShow = false;
    setShowHeaderDisabled(false);
    setNewShowButtonBusy(false);
  }
}

async function duplicateShow(showId){
  closeAllShowMenus();
  if(state.isCreatingShow){
    return;
  }
  const source = state.shows.find(show => show.id === showId);
  if(!source){
    toast('Show not found', true);
    return;
  }
  const previousId = state.currentShowId;
  state.isCreatingShow = true;
  setShowHeaderDisabled(true);
  setNewShowButtonBusy(true);
  try{
    const dupPayload = {
      date: source.date,
      time: source.time,
      label: source.label,
      crew: [...(source.crew||[])],
      leadPilot: source.leadPilot || '',
      monkeyLead: source.monkeyLead || '',
      notes: source.notes || ''
    };
    const payload = await apiRequest('/api/shows', {method:'POST', body: JSON.stringify(dupPayload)});
    upsertShow(payload);
    setCurrentShow(payload.id);
    notifyShowsChanged({showId: payload.id});
    clearEntryForm();
    toast('Show duplicated');
  }catch(err){
    console.error(err);
    toast(err.message || 'Failed to duplicate show', true);
    const fallbackId = previousId && state.shows.some(show => show.id === previousId)
      ? previousId
      : (state.shows[0]?.id || null);
    setCurrentShow(fallbackId);
  }finally{
    state.isCreatingShow = false;
    setShowHeaderDisabled(false);
    setNewShowButtonBusy(false);
  }
}

async function archiveShowNow(showId){
  closeAllShowMenus();
  const show = state.shows.find(s => s.id === showId);
  if(!show){
    toast('Show not found', true);
    return;
  }
  const confirmed = confirm('Archive this show now? It will move to the archive workspace.');
  if(!confirmed){
    return;
  }
  let archivedPayload = null;
  try{
    archivedPayload = await apiRequest(`/api/shows/${showId}/archive`, {method:'POST'});
  }catch(err){
    console.error('Failed to archive show', err);
    toast(err.message || 'Failed to archive show', true);
    return;
  }
  const wasCurrent = state.currentShowId === showId;
  state.shows = state.shows.filter(s => s.id !== showId);
  if(wasCurrent){
    const fallbackId = state.shows[0]?.id || null;
    setCurrentShow(fallbackId);
  }else{
    renderGroups();
    syncOperatorShowSelect();
  }
  await loadArchivedShows({silent: true, preserveSelection: true});
  if(archivedPayload && archivedPayload.id){
    setCurrentArchivedShow(archivedPayload.id);
  }
  notifyShowsChanged({showId: state.currentShowId || null});
  toast('Show archived');
}

async function deleteShow(showId){
  closeAllShowMenus();
  const show = state.shows.find(s => s.id === showId);
  if(!show){
    toast('Show not found', true);
    return;
  }
  const confirmed = confirm('Delete this show? It will move to the archive and cannot be undone.');
  if(!confirmed){
    return;
  }
  let archivedPayload = null;
  try{
    archivedPayload = await apiRequest(`/api/shows/${showId}`, {method: 'DELETE'});
  }catch(err){
    console.error('Failed to delete show', err);
    toast(err.message || 'Failed to delete show', true);
    return;
  }
  const wasCurrent = state.currentShowId === showId;
  state.shows = state.shows.filter(s => s.id !== showId);
  if(wasCurrent){
    const fallbackId = state.shows[0]?.id || null;
    setCurrentShow(fallbackId);
  }else{
    renderGroups();
    syncOperatorShowSelect();
  }
  await loadArchivedShows({silent: true, preserveSelection: true});
  if(archivedPayload && archivedPayload.id){
    setCurrentArchivedShow(archivedPayload.id);
  }
  notifyShowsChanged({showId: state.currentShowId || null});
  toast('Show deleted');
}

async function onAddLine(){
  if(state.currentView !== 'operator'){
    toast('Switch to the Operator workspace to log entries', true);
    return;
  }
  const show = getCurrentShow();
  if(!show){
    toast('Select or create a show first', true);
    return;
  }
  const operatorName = getOperatorIdentity();
  if(operator){
    operator.value = operatorName;
  }
  if(!operatorName){
    showError('errOperator');
    toast('Operator credentials missing. Please sign in again.', true);
    updateOperatorEntryState();
    return;
  }
  if(operatorHasEntry(show, operatorName)){
    toast('You already submitted an entry for this show.', true);
    updateOperatorEntryState();
    return;
  }
  clearErrors();
  let ok = true;
  if(!show.date){ showError('errDate'); ok=false; }
  if(!show.time){ showError('errTime'); ok=false; }
  if(!unitId.value){ showError('errUnit'); ok=false; }
  if(!planned.value){ showError('errPlanned'); ok=false; }
  if(!launched.value){ showError('errLaunched'); ok=false; }
  const st = getStatus();
  if(!st){ showError('errStatus'); ok=false; }
  if(planned.value === 'No' && st !== 'No-launch'){ showError('errStatus'); toast('If Planned is No, Status must be No-launch', true); ok=false; }
  if(launched.value === 'No' && st !== 'No-launch'){ showError('errStatus'); toast('If Launched is No, Status must be No-launch', true); ok=false; }
  if(launched.value === 'Yes' && st === 'No-launch'){ showError('errStatus'); toast('If Launched is Yes, Status cannot be No-launch', true); ok=false; }
  if(st !== 'Completed'){
    if(!primaryIssue.value){ showError('errPrimary'); ok=false; }
    if(!severity.value){ showError('errSeverity'); ok=false; }
    if(primaryIssue.value === 'Other' && !otherDetail.value.trim()){ showError('errOther'); ok=false; }
  }
  if(!operator.value){ showError('errOperator'); ok=false; }
  if(delaySec.value){
    const v = Number(delaySec.value);
    if(!Number.isFinite(v) || v < 0){ showError('errDelay'); ok=false; }
  }
  if(!ok){ return; }

  const entry = {
    unitId: unitId.value,
    planned: planned.value,
    launched: launched.value,
    status: st,
    primaryIssue: st === 'Completed' ? '' : primaryIssue.value,
    subIssue: st === 'Completed' ? '' : (subIssue.value || ''),
    otherDetail: st === 'Completed' ? '' : (primaryIssue.value === 'Other' ? otherDetail.value.trim() : ''),
    severity: st === 'Completed' ? '' : (severity.value || ''),
    rootCause: st === 'Completed' ? '' : (rootCause.value || ''),
    actions: st === 'Completed' ? [] : getSelectedActions(actionsChips),
    operator: operatorName || '',
    batteryId: batteryId.value.trim(),
    delaySec: delaySec.value ? Number(delaySec.value) : null,
    commandRx: commandRx.value || '',
    notes: entryNotes.value.trim()
  };

  try{
    await apiRequest(`/api/shows/${show.id}/entries`, {method:'POST', body: JSON.stringify(entry)});
    const updatedShow = await apiRequest(`/api/shows/${show.id}`, {method:'GET'});
    upsertShow(updatedShow);
    setCurrentShow(updatedShow.id);
    notifyShowsChanged({showId: updatedShow.id});
    clearEntryForm();
    toast('Line added');
  }catch(err){
    console.error(err);
    toast(err.message || 'Failed to add entry', true);
  }
}

function clearEntryForm(){
  unitId.value = '';
  planned.value = '';
  launched.value = '';
  setStatus('');
  primaryIssue.value = '';
  subIssue.innerHTML = '<option value="">N/A</option>';
  otherDetail.value = '';
  severity.value = '';
  rootCause.value = '';
  renderActionsChips(actionsChips, []);
  batteryId.value = '';
  delaySec.value = '';
  commandRx.value = '';
  entryNotes.value = '';
  updateIssueVisibility();
  syncOperatorIdentity();
}

function renderGroups(){
  sortShows();
  closeAllShowMenus();
  groupsContainer.innerHTML = '';
  state.shows.forEach(show=>{
    const isOpen = show.id === state.currentShowId;
    const group = document.createElement('details');
    group.className = 'group';
    group.open = isOpen;
    const summary = document.createElement('summary');
    const summaryContent = document.createElement('div');
    summaryContent.className = 'group-summary';
    const titleEl = document.createElement('div');
    titleEl.className = 'group-title';
    titleEl.textContent = groupTitle(show);
    const meta = document.createElement('div');
    meta.className = 'group-summary-meta';
    const badge = document.createElement('div');
    badge.className = 'badge';
    badge.textContent = `${show.entries?.length || 0} entries`;
    meta.appendChild(badge);
    meta.appendChild(createShowMenu(show));
    summaryContent.appendChild(titleEl);
    summaryContent.appendChild(meta);
    summary.appendChild(summaryContent);
    summary.addEventListener('click', ()=>{
      setTimeout(()=>{
        if(group.open){
          setCurrentShow(show.id, {skipRender: true});
        }
      }, 0);
    });
    summary.addEventListener('click', closeAllShowMenus);
    const metricsDiv = document.createElement('div');
    const metrics = computeMetrics(show);
    metricsDiv.className = 'metrics';
    metricsDiv.innerHTML = `
      <div class="metric">Launch success: <b>${metrics.successRate}%</b></div>
      <div class="metric">Completed: <b>${metrics.countCompleted}</b></div>
      <div class="metric">No-launch: <b>${metrics.countNoLaunch}</b></div>
      <div class="metric">Abort: <b>${metrics.countAbort}</b></div>
      <div class="metric">Avg delay: <b>${metrics.avgDelay}</b> s</div>
      <div class="metric">Top issues: <b>${metrics.topIssues.join(', ') || 'n/a'}</b></div>
    `;
    const rows = document.createElement('div');
    rows.className = 'rows';
    const header = document.createElement('div');
    header.className = 'rowcard';
    header.style.background = '#1a1d26';
    header.style.fontWeight = '700';
    header.style.color = 'var(--text-dim)';
    header.style.borderBottom = '2px solid var(--border)';
    const unitPrefix = (state.unitLabel || '').trim();
    const idHeader = unitPrefix ? `${unitPrefix.charAt(0).toUpperCase()}#` : 'U#';
    header.innerHTML = `
      <div><b>${idHeader}</b></div>
      <div><b>Planned</b></div>
      <div><b>Launched</b></div>
      <div><b>Status</b></div>
      <div><b>Issue</b></div>
      <div><b>Operator</b></div>
      <div><b>Notes</b></div>
      <div></div>
    `;
    rows.appendChild(header);
    (show.entries || []).slice().sort((a,b)=> (b.ts||0) - (a.ts||0)).forEach(entry=>{
      rows.appendChild(renderRow(show, entry));
    });
    group.appendChild(summary);
    group.appendChild(metricsDiv);
    group.appendChild(rows);
    groupsContainer.appendChild(group);
  });
  updateWebhookPreview();
}

function groupTitle(show){
  const d = formatDateUS(show.date) || 'MM-DD-YYYY';
  const t = formatTime12Hour(show.time) || 'HH:mm';
  const label = show.label ? ` • ${show.label}` : '';
  return `${d} • ${t}${label}`;
}

function createShowMenu(show){
  const wrap = document.createElement('div');
  wrap.className = 'show-menu-wrap';
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'btn icon-btn show-menu-btn';
  btn.setAttribute('aria-haspopup', 'true');
  btn.setAttribute('aria-expanded', 'false');
  btn.title = 'Show options';
  btn.setAttribute('aria-label', 'Show options');
  btn.innerHTML = '⋮';
  btn.addEventListener('click', event=>{
    event.preventDefault();
    event.stopPropagation();
    const isOpen = wrap.classList.contains('open');
    closeAllShowMenus();
    if(!isOpen){
      wrap.classList.add('open');
      btn.setAttribute('aria-expanded', 'true');
    }
  });
  const menu = document.createElement('div');
  menu.className = 'show-menu';
  const duplicateBtn = document.createElement('button');
  duplicateBtn.type = 'button';
  duplicateBtn.className = 'menu-item';
  duplicateBtn.textContent = 'Duplicate show';
  duplicateBtn.addEventListener('click', async event=>{
    event.preventDefault();
    event.stopPropagation();
    closeAllShowMenus();
    await duplicateShow(show.id);
  });
  menu.appendChild(duplicateBtn);
  const archiveBtn = document.createElement('button');
  archiveBtn.type = 'button';
  archiveBtn.className = 'menu-item';
  archiveBtn.textContent = 'Archive show';
  archiveBtn.addEventListener('click', async event=>{
    event.preventDefault();
    event.stopPropagation();
    await archiveShowNow(show.id);
  });
  menu.appendChild(archiveBtn);
  const deleteBtn = document.createElement('button');
  deleteBtn.type = 'button';
  deleteBtn.className = 'menu-item danger';
  deleteBtn.textContent = 'Delete show';
  deleteBtn.addEventListener('click', async event=>{
    event.preventDefault();
    event.stopPropagation();
    await deleteShow(show.id);
  });
  menu.appendChild(deleteBtn);
  wrap.appendChild(btn);
  wrap.appendChild(menu);
  return wrap;
}

function closeAllShowMenus(){
  qsa('.show-menu-wrap.open').forEach(wrap=>{
    wrap.classList.remove('open');
    const toggle = qs('.show-menu-btn', wrap);
    if(toggle){
      toggle.setAttribute('aria-expanded', 'false');
    }
  });
}

function renderRow(show, entry){
  const row = document.createElement('div');
  row.className = 'rowcard';
  const colorDot = entry.status === 'Completed' ? 'dot-success' : entry.status === 'Abort' ? 'dot-warn' : 'dot-danger';
  const issueTxt = entry.status === 'Completed' ? '' : [entry.primaryIssue, entry.subIssue || entry.otherDetail].filter(Boolean).join(' / ');
  row.innerHTML = `
    <div><b>${escapeHtml(entry.unitId || '')}</b></div>
    <div>${escapeHtml(entry.planned || '')}</div>
    <div>${escapeHtml(entry.launched || '')}</div>
    <div><span class="status-dot ${colorDot}"></span>${escapeHtml(entry.status || '')}</div>
    <div>${escapeHtml(issueTxt)}</div>
    <div>${escapeHtml(entry.operator || '')}</div>
    <div>${escapeHtml(entry.notes || '')}</div>
    <div class="menu" data-menu>
      <button class="menu-btn" title="Row menu" aria-haspopup="true" aria-expanded="false">⋯</button>
      <div class="menu-list" role="menu">
        <button class="menu-item" role="menuitem" data-edit>Edit</button>
        <button class="menu-item" role="menuitem" data-delete>Delete</button>
      </div>
    </div>
  `;
  const menu = qs('[data-menu]', row);
  const btn = qs('.menu-btn', menu);
  btn.addEventListener('click', e=>{
    e.stopPropagation();
    const open = menu.hasAttribute('open');
    closeAllMenus();
    if(!open){
      menu.setAttribute('open', '');
    }
    btn.setAttribute('aria-expanded', String(!open));
    document.addEventListener('click', closeAllMenus, {once:true});
  });
  qs('[data-edit]', row).addEventListener('click', ()=>{
    menu.removeAttribute('open');
    openEditModal(show.id, entry.id);
  });
  qs('[data-delete]', row).addEventListener('click', async ()=>{
    menu.removeAttribute('open');
    if(confirm('Delete this entry?')){
      await deleteEntry(show.id, entry.id);
    }
  });
  return row;
}

async function deleteEntry(showId, entryId){
  try{
    await apiRequest(`/api/shows/${showId}/entries/${entryId}`, {method:'DELETE'});
    const updatedShow = await apiRequest(`/api/shows/${showId}`, {method:'GET'});
    upsertShow(updatedShow);
    setCurrentShow(updatedShow.id);
    notifyShowsChanged({showId: updatedShow.id});
    toast('Entry deleted');
  }catch(err){
    console.error(err);
    toast('Failed to delete entry', true);
  }
}

function closeAllMenus(){
  qsa('[data-menu]').forEach(m=>m.removeAttribute('open'));
}

function computeMetrics(show){
  const plannedYes = (show.entries||[]).filter(e=>e.planned==='Yes').length;
  const completed = (show.entries||[]).filter(e=>e.status==='Completed').length;
  const noLaunch = (show.entries||[]).filter(e=>e.status==='No-launch').length;
  const abort = (show.entries||[]).filter(e=>e.status==='Abort').length;
  const delays = (show.entries||[]).map(e=>e.delaySec).filter(v=>typeof v === 'number');
  const avgDelay = delays.length ? (delays.reduce((a,b)=>a+b,0)/delays.length).toFixed(2) : '0.00';
  const issues = {};
  (show.entries||[]).forEach(e=>{
    if(e.status !== 'Completed' && e.primaryIssue){
      issues[e.primaryIssue] = (issues[e.primaryIssue] || 0) + 1;
    }
  });
  const topIssues = Object.entries(issues).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([key])=>key);
  const successRate = plannedYes ? Math.round((completed/plannedYes)*100) : 0;
  return {
    successRate,
    countCompleted: completed,
    countNoLaunch: noLaunch,
    countAbort: abort,
    avgDelay,
    topIssues
  };
}

function openEditModal(showId, entryId){
  const show = state.shows.find(s=>s.id===showId);
  const entry = show?.entries.find(e=>e.id===entryId);
  if(!entry){
    return;
  }
  state.editingEntryRef = {showId, entryId};
  editForm.innerHTML = '';
  const fields = buildEntryFieldsClone(entry, show);
  fields.forEach(f=> editForm.appendChild(f));
  editModal.classList.add('open');
}

function closeEditModal(){
  editModal.classList.remove('open');
  state.editingEntryRef = null;
}

async function saveEditEntry(){
  if(!state.editingEntryRef){
    return;
  }
  const show = state.shows.find(s=>s.id===state.editingEntryRef.showId);
  if(!show){
    toast('Show not found', true);
    return;
  }
  const form = editForm;
  const get = id => qs(`#${id}`, form);
  const status = pillGet(form);
  if(!get('edit_unitId').value || !get('edit_planned').value || !get('edit_launched').value || !status){
    toast('Missing required fields', true);
    return;
  }
  if(get('edit_planned').value === 'No' && status !== 'No-launch'){ toast('If Planned is No, Status must be No-launch', true); return; }
  if(get('edit_launched').value === 'No' && status !== 'No-launch'){ toast('If Launched is No, Status must be No-launch', true); return; }
  if(get('edit_launched').value === 'Yes' && status === 'No-launch'){ toast('If Launched is Yes, Status cannot be No-launch', true); return; }
  const prim = get('edit_primaryIssue').value;
  const sev = get('edit_severity').value;
  const other = get('edit_otherDetail').value.trim();
  if(status !== 'Completed'){
    if(!prim || !sev){ toast('Issue and Severity required when not Completed', true); return; }
    if(prim === 'Other' && !other){ toast('Other detail required', true); return; }
  }
  const operatorValue = get('edit_operator').value.trim();
  if(!operatorValue){ toast('Operator required', true); return; }
  const entryUpdate = {
    unitId: get('edit_unitId').value,
    planned: get('edit_planned').value,
    launched: get('edit_launched').value,
    status,
    primaryIssue: status === 'Completed' ? '' : prim,
    subIssue: status === 'Completed' ? '' : (get('edit_subIssue').value || ''),
    otherDetail: status === 'Completed' ? '' : (prim === 'Other' ? other : ''),
    severity: status === 'Completed' ? '' : sev,
    rootCause: status === 'Completed' ? '' : get('edit_rootCause').value,
    actions: status === 'Completed' ? [] : getSelectedActions(qs('#edit_actionsChips', form)),
    operator: operatorValue,
    batteryId: get('edit_batteryId').value.trim(),
    delaySec: get('edit_delaySec').value ? Number(get('edit_delaySec').value) : null,
    commandRx: get('edit_commandRx').value || '',
    notes: get('edit_entryNotes').value.trim()
  };
  try{
    await apiRequest(`/api/shows/${show.id}/entries/${state.editingEntryRef.entryId}`, {method:'PUT', body: JSON.stringify(entryUpdate)});
    const updatedShow = await apiRequest(`/api/shows/${show.id}`, {method:'GET'});
    upsertShow(updatedShow);
    setCurrentShow(updatedShow.id);
    notifyShowsChanged({showId: updatedShow.id});
    closeEditModal();
    toast('Entry updated');
  }catch(err){
    console.error(err);
    toast(err.message || 'Failed to update entry', true);
  }
}

function buildEntryFieldsClone(entry, show){
  const fields = [];
  const wrap = (node, cls='col-3')=>{
    const div = document.createElement('div');
    div.className = cls;
    div.appendChild(node);
    return div;
  };
  const createLabelWrap = (id, labelText, node)=>{
    const label = document.createElement('label');
    label.setAttribute('for', id);
    label.textContent = labelText;
    const wrapper = document.createElement('div');
    wrapper.appendChild(label);
    node.id = id;
    node.style.width = '100%';
    if(node.tagName !== 'TEXTAREA'){
      node.style.minHeight = 'var(--tap-min)';
    }
    wrapper.appendChild(node);
    return wrapper;
  };
  const unit = document.createElement('select');
  const units = getDefaultUnits();
  if(entry.unitId && !units.includes(entry.unitId)){
    units.push(entry.unitId);
  }
  unit.innerHTML = '<option value="">Select</option>' + units.map(u=>`<option ${entry.unitId===u?'selected':''}>${u}</option>`).join('');
  fields.push(wrap(createLabelWrap('edit_unitId', `${state.unitLabel} ID`, unit)));

  const plannedSelect = document.createElement('select');
  plannedSelect.innerHTML = optionsForYesNo(entry.planned);
  fields.push(wrap(createLabelWrap('edit_planned', 'Planned to fly', plannedSelect)));

  const launchedSelect = document.createElement('select');
  launchedSelect.innerHTML = optionsForYesNo(entry.launched);
  fields.push(wrap(createLabelWrap('edit_launched', 'Launched', launchedSelect)));

  const pills = pillBuild(entry.status);
  fields.push(wrap(pills, 'col-4'));

  const prim = document.createElement('select');
  prim.innerHTML = '<option value="">Select</option>' + PRIMARY_ISSUES.map(issue=>`<option ${entry.primaryIssue===issue?'selected':''}>${issue}</option>`).join('');
  fields.push(wrap(createLabelWrap('edit_primaryIssue', 'Primary issue', prim), 'col-4'));

  const sub = document.createElement('select');
  const options = ISSUE_MAP[entry.primaryIssue] || [];
  sub.innerHTML = '<option value="">N/A</option>' + options.map(opt=>`<option ${entry.subIssue===opt?'selected':''}>${opt}</option>`).join('');
  fields.push(wrap(createLabelWrap('edit_subIssue', 'Sub-issue', sub), 'col-4'));

  const other = document.createElement('input');
  other.type = 'text';
  other.value = entry.otherDetail || '';
  fields.push(wrap(createLabelWrap('edit_otherDetail', 'Other detail', other), 'col-4'));

  const sev = document.createElement('select');
  sev.innerHTML = '<option value="">Select</option>' + ['Critical show stop','Major visible','Minor contained'].map(opt=>`<option ${entry.severity===opt?'selected':''}>${opt}</option>`).join('');
  fields.push(wrap(createLabelWrap('edit_severity', 'Severity', sev), 'col-4'));

  const root = document.createElement('select');
  root.innerHTML = '<option value="">Select</option>' + ['Hardware','Software','Ops','Environment','Unknown'].map(opt=>`<option ${entry.rootCause===opt?'selected':''}>${opt}</option>`).join('');
  fields.push(wrap(createLabelWrap('edit_rootCause', 'Root cause draft', root), 'col-4'));

  const actionsWrap = document.createElement('div');
  actionsWrap.className = 'actions-chips';
  actionsWrap.id = 'edit_actionsChips';
  renderActionsChips(actionsWrap, entry.actions || []);
  const actionsContainer = document.createElement('div');
  actionsContainer.className = 'col-12';
  const actionsLabel = document.createElement('label');
  actionsLabel.textContent = 'Actions taken';
  actionsContainer.appendChild(actionsLabel);
  actionsContainer.appendChild(actionsWrap);
  fields.push(actionsContainer);

  const operatorSelect = document.createElement('select');
  const operatorNames = getOperatorNames([entry.operator, show?.leadPilot]);
  if(operatorNames.length){
    operatorSelect.innerHTML = '<option value="">Select</option>' + operatorNames.map(name=>`<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`).join('');
    operatorSelect.disabled = false;
    const match = operatorNames.find(name => name.toLowerCase() === (entry.operator || '').toLowerCase());
    operatorSelect.value = match || '';
  }else{
    operatorSelect.innerHTML = '<option value="">Add operators via admin settings</option>';
    operatorSelect.disabled = true;
  }
  fields.push(wrap(createLabelWrap('edit_operator', 'Operator', operatorSelect)));

  const battery = document.createElement('input');
  battery.type = 'text';
  battery.value = entry.batteryId || '';
  fields.push(wrap(createLabelWrap('edit_batteryId', 'Battery ID', battery)));

  const delay = document.createElement('input');
  delay.type = 'number';
  delay.step = '0.1';
  delay.min = '0';
  delay.value = entry.delaySec ?? '';
  fields.push(wrap(createLabelWrap('edit_delaySec', 'Launch delay seconds', delay)));

  const cmdRx = document.createElement('select');
  cmdRx.innerHTML = '<option value="">Select</option>' + ['Yes','No'].map(opt=>`<option ${entry.commandRx===opt?'selected':''}>${opt}</option>`).join('');
  fields.push(wrap(createLabelWrap('edit_commandRx', 'Command received', cmdRx)));

  const notes = document.createElement('textarea');
  notes.value = entry.notes || '';
  fields.push(wrap(createLabelWrap('edit_entryNotes', 'Notes', notes), 'col-12'));

  return fields;
}

function optionsForYesNo(selected){
  return ['','Yes','No'].map(opt=> opt ? `<option ${selected===opt?'selected':''}>${opt}</option>` : '<option value="">Select</option>').join('');
}

function pillBuild(current){
  const wrapper = document.createElement('div');
  wrapper.className = 'pills';
  STATUS.forEach(status=>{
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'pill';
    btn.dataset.status = status;
    btn.textContent = status;
    btn.setAttribute('aria-pressed', status === current ? 'true' : 'false');
    btn.addEventListener('click', ()=>{
      qsa('.pill', wrapper).forEach(b=> b.setAttribute('aria-pressed', 'false'));
      btn.setAttribute('aria-pressed', 'true');
    });
    if(status === 'Completed'){ btn.classList.add('completed'); }
    if(status === 'No-launch'){ btn.classList.add('nolaunch'); }
    if(status === 'Abort'){ btn.classList.add('abort'); }
    wrapper.appendChild(btn);
  });
  return wrapper;
}

function pillGet(formRoot){
  const btn = qsa('.pill', formRoot).find(b=>b.getAttribute('aria-pressed')==='true');
  return btn ? btn.dataset.status : '';
}

function renderOperatorOptions(){
  syncOperatorIdentity();
}

function getOperatorIdentity(){
  if(!state.session){
    return '';
  }
  const name = typeof state.session.name === 'string' ? state.session.name.trim() : '';
  if(name){
    return name;
  }
  return typeof state.session.email === 'string' ? state.session.email.trim() : '';
}

function syncOperatorIdentity(){
  if(!operator){
    return;
  }
  const identity = getOperatorIdentity();
  operator.value = identity;
  if(operatorDisplay){
    operatorDisplay.value = identity || '';
  }
  updateOperatorEntryState();
}

function operatorHasEntry(show, operatorName){
  if(!show || !operatorName){
    return false;
  }
  const normalized = operatorName.trim().toLowerCase();
  if(!normalized){
    return false;
  }
  return Array.isArray(show.entries) && show.entries.some(entry =>{
    if(!entry){
      return false;
    }
    const existing = typeof entry.operator === 'string' ? entry.operator.trim().toLowerCase() : '';
    return existing === normalized;
  });
}

function updateOperatorEntryState(){
  if(!addLineBtn){
    return;
  }
  if(state.currentView !== 'operator'){
    addLineBtn.disabled = false;
    addLineBtn.textContent = addLineBtnDefaultText;
    if(operatorEntryNotice){
      operatorEntryNotice.hidden = true;
    }
    return;
  }
  const show = getCurrentShow();
  const identity = getOperatorIdentity();
  let message = '';
  let disabled = false;
  if(!identity){
    disabled = true;
    message = 'Sign in again to log entries.';
  }else if(!show){
    disabled = true;
    message = 'Lead must create a show before logging entries.';
  }else if(operatorHasEntry(show, identity)){
    disabled = true;
    message = 'You already submitted an entry for this show.';
  }
  addLineBtn.disabled = disabled;
  addLineBtn.textContent = disabled ? 'Entry locked' : addLineBtnDefaultText;
  if(operatorEntryNotice){
    operatorEntryNotice.textContent = message;
    operatorEntryNotice.hidden = !message;
  }
}

function setView(view){
  const normalizedView = view || 'discipline';
  if(normalizedView === 'lead' && !userHasRole('lead')){
    toast('Lead workspace requires Lead role', true);
    return;
  }
  if(normalizedView === 'operator' && !userHasRole('operator')){
    toast('Operator workspace requires Operator role', true);
    return;
  }
  if(normalizedView === 'archive' && !userHasRole('crew') && !userHasRole('operator') && !userHasRole('lead')){
    toast('Archive workspace requires a workspace role', true);
    return;
  }
  if(normalizedView === 'calendar' && !userHasRole('crew') && !userHasRole('operator') && !userHasRole('lead')){
    toast('Calendar requires a workspace role', true);
    return;
  }
  if(normalizedView === 'admin' && !isAdmin()){
    toast('Admin workspace requires Admin role', true);
    return;
  }
  state.currentView = normalizedView;
  const knownViews = ['discipline','landing','lead','operator','archive','admin','workspace','calendar'];
  document.body.classList.remove(...knownViews.map(value => `view-${value}`));
  document.body.classList.add(`view-${normalizedView}`);
  setConfigSection(normalizedView);
  if(viewBadge){
    viewBadge.hidden = false;
    viewBadge.classList.remove('view-badge-operator');
    if(normalizedView === 'operator'){
      viewBadge.textContent = 'Operator workspace';
      viewBadge.classList.add('view-badge-operator');
    }else if(normalizedView === 'archive'){
      viewBadge.textContent = 'Archive workspace';
    }else if(normalizedView === 'calendar'){
      viewBadge.textContent = 'Calendar workspace';
    }else if(normalizedView === 'admin'){
      viewBadge.textContent = 'Admin workspace';
    }else if(normalizedView === 'landing'){
      viewBadge.textContent = 'Choose workspace';
    }else if(normalizedView === 'discipline'){
      viewBadge.textContent = 'Select discipline';
    }else if(normalizedView === 'workspace'){
      viewBadge.textContent = 'Workspace overview';
    }else{
      viewBadge.textContent = 'Lead workspace';
    }
  }
  if(roleHomeBtn){
    roleHomeBtn.hidden = normalizedView === 'discipline';
  }
  if(normalizedView === 'discipline'){
    toggleConfig(false);
    renderDisciplineOptions();
  }else if(normalizedView === 'workspace'){
    toggleConfig(false);
    renderWorkspacePlaceholder();
  }else if(normalizedView === 'landing'){
    toggleConfig(false);
  }
  if(normalizedView === 'operator'){
    syncOperatorShowSelect();
  }else{
    updateOperatorSummary();
  }
  if(normalizedView === 'archive'){
    renderArchiveSelect();
  }
  if(normalizedView === 'calendar'){
    renderCalendar();
    if(!state.calendarLoaded){
      loadCalendarEvents();
    }
  }
  if(normalizedView === 'admin' && isAdmin()){
    loadUsers();
  }
  updateOperatorEntryState();
  updateWorkspaceAvailability();
}

function toggleConfig(force){
  const shouldOpen = typeof force === 'boolean'
    ? force
    : !document.body.classList.contains('menu-open');
  if(configBtn){
    configBtn.setAttribute('aria-expanded', String(shouldOpen));
    configBtn.classList.toggle('is-active', shouldOpen);
  }
  if(configPanel){
    configPanel.classList.toggle('open', shouldOpen);
    configPanel.setAttribute('aria-hidden', shouldOpen ? 'false' : 'true');
  }
  document.body.classList.toggle('menu-open', shouldOpen);
  if(!shouldOpen){
    document.body.style.setProperty('--drawer-active-width', '0px');
  }
  requestAnimationFrame(refreshDrawerOffset);
  const nextSection = state.currentView || 'landing';
  if(shouldOpen){
    configMessage.textContent = '';
    setConfigSection(nextSection);
    if(isAdmin()){
      loadUsers();
    }
  }else{
    setConfigSection(nextSection);
  }
}

function refreshDrawerOffset(){
  if(!configPanel){
    document.body.style.setProperty('--drawer-active-width', '0px');
    return;
  }
  const isOpen = document.body.classList.contains('menu-open') && configPanel.classList.contains('open');
  const measured = isOpen ? configPanel.getBoundingClientRect().width : 0;
  const clamped = Math.max(0, Math.min(measured, window.innerWidth));
  document.body.style.setProperty('--drawer-active-width', `${Math.round(clamped)}px`);
}

function setConfigSection(section){
  currentConfigSection = section;
  if(configSections.length){
    configSections.forEach(sec=>{
      const isActive = sec.dataset.configSection === section;
      sec.classList.toggle('is-active', isActive);
      sec.setAttribute('aria-hidden', isActive ? 'false' : 'true');
    });
  }
  if(configNavButtons.length){
    configNavButtons.forEach(btn=>{
      const isActive = btn.dataset.configTarget === section;
      btn.classList.toggle('is-active', isActive);
      btn.setAttribute('aria-pressed', isActive ? 'true' : 'false');
    });
  }
}

function closeAdminPinPrompt(){
  const prompt = el('adminPinPrompt');
  if(prompt){
    prompt.hidden = true;
  }
}




async function onConfigSubmit(event){
  event.preventDefault();
  if(!isAdmin()){
    toast('Admin access required', true);
    return;
  }
  const payload = {
    unitLabel: unitLabelSelect.value,
    webhook: {
      enabled: webhookEnabled ? webhookEnabled.checked : false,
      url: webhookUrl ? webhookUrl.value.trim() : '',
      method: webhookMethod ? webhookMethod.value.toUpperCase() : 'POST',
      secret: webhookSecret ? webhookSecret.value.trim() : '',
      headers: parseHeadersText(webhookHeaders ? webhookHeaders.value : '')
    }
  };
  try{
    const updated = await apiRequest('/api/config', {method:'PUT', body: JSON.stringify(payload)});
    state.config = updated;
    state.unitLabel = updated.unitLabel || 'Drone';
    state.serverHost = updated.host || state.serverHost;
    const nextPort = Number.parseInt(updated.port, 10);
    state.serverPort = Number.isFinite(nextPort) ? nextPort : state.serverPort;
    const updatedStorageMeta = (updated.storageMeta && typeof updated.storageMeta === 'object')
      ? updated.storageMeta
      : (typeof updated.storage === 'object' ? updated.storage : null);
    state.storageMeta = updatedStorageMeta || (typeof updated.storage === 'string' ? {label: updated.storage} : state.storageMeta);
    state.storageLabel = resolveStorageLabel(state.storageMeta || updated.storage || state.storageLabel);
    state.webhookConfig = {
      enabled: Boolean(updated.webhook?.enabled),
      url: updated.webhook?.url || '',
      method: (updated.webhook?.method || 'POST').toUpperCase(),
      secret: updated.webhook?.secret || '',
      headersText: formatHeadersText(updated.webhook?.headers)
    };
    state.webhookStatus = normalizeWebhookStatus(updated.webhookStatus || updated.webhook, updated.webhook);
    unitLabelSelect.value = state.unitLabel;
    updateDisciplineHeader();
    unitLabelEl.textContent = state.unitLabel;
    setLanAddress();
    setProviderBadge(state.storageMeta || state.storageLabel);
    setWebhookBadge(state.webhookStatus);
    populateUnitOptions();
    refreshWebhookUi();
    updateConnectionIndicator('loading');
    await loadShows();
    setCurrentShow(state.currentShowId || null);
    notifyConfigChanged({unitLabel: state.unitLabel});
    configMessage.textContent = 'Settings saved. Storage restarted.';
    toggleConfig(false);
    toast('Settings updated');
  }catch(err){
    console.error(err);
    configMessage.textContent = err.message || 'Failed to save settings';
    toast('Failed to save settings', true);
  }
}

function exportShowAsCsv(show){
  if(!show){
    toast('No show selected', true);
    return;
  }
  const rows = (show.entries||[]).map(entry=>{
    const row = buildWebhookRow(show, entry);
    return EXPORT_COLUMNS.map(column => row[column] ?? '');
  });
  const csv = [EXPORT_COLUMNS.map(csvEscape).join(','), ...rows.map(row=>row.map(csvEscape).join(','))].join('\n');
  downloadFile(csv, `${show.id}.csv`, 'text/csv');
  toast('CSV exported');
}

function exportShowAsJson(show){
  if(!show){
    toast('No show selected', true);
    return;
  }
  const json = JSON.stringify(show, null, 2);
  downloadFile(json, `${show.id}.json`, 'application/json');
  toast('JSON exported');
}

function buildWebhookRow(show = {}, entry = {}){
  const status = entry.status || '';
  const crewList = Array.isArray(show.crew) ? show.crew : [];
  const actions = Array.isArray(entry.actions) ? entry.actions : [];
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
    status,
    primaryIssue: status === 'Completed' ? '' : (entry.primaryIssue || ''),
    subIssue: status === 'Completed' ? '' : (entry.subIssue || ''),
    otherDetail: status === 'Completed' ? '' : (entry.otherDetail || ''),
    severity: status === 'Completed' ? '' : (entry.severity || ''),
    rootCause: status === 'Completed' ? '' : (entry.rootCause || ''),
    actions: actions.join('|'),
    operator: entry.operator || '',
    batteryId: entry.batteryId || '',
    delaySec: entry.delaySec === null || entry.delaySec === undefined ? '' : entry.delaySec,
    commandRx: entry.commandRx || '',
    notes: entry.notes || ''
  };
}

function buildWebhookMessage(row = {}){
  const payload = {};
  EXPORT_COLUMNS.forEach(column => {
    const value = row[column];
    payload[column] = value === undefined || value === null ? '' : value;
  });
  return payload;
}

function buildSampleWebhookRow(){
  return {
    showId: 'sample-show',
    showDate: '2024-07-01',
    showTime: '19:00',
    showLabel: 'Evening Showcase',
    crew: 'Alex|Nazar',
    leadPilot: 'Alex',
    monkeyLead: 'Nazar',
    showNotes: 'Preview row for webhook payload',
    entryId: 'sample-entry',
    unitId: 'Drone-01',
    planned: 'Yes',
    launched: 'Yes',
    status: 'Completed',
    primaryIssue: '',
    subIssue: '',
    otherDetail: '',
    severity: '',
    rootCause: '',
    actions: 'Logged only',
    operator: 'Alex',
    batteryId: 'B-12',
    delaySec: '0',
    commandRx: 'Yes',
    notes: 'Nominal flight'
  };
}

function clearErrors(){
  qsa('.error').forEach(e=> e.hidden = true);
}

function showError(id){
  const el = document.getElementById(id);
  if(el){
    el.hidden = false;
  }
}

function setLanAddress(){
  if(!lanAddressEl){ return; }
  const host = state.serverHost || '10.241.211.120';
  const port = state.serverPort || 3000;
  lanAddressEl.textContent = `http://${host}:${port}`;
}

function resolveStorageLabel(source, fallback = 'SQL.js storage v2'){
  if(source === null || source === undefined){
    return fallback;
  }
  if(typeof source === 'string'){
    const trimmed = source.trim();
    return trimmed || fallback;
  }
  if(typeof source === 'object'){
    if(typeof source.label === 'string' && source.label.trim()){
      return source.label.trim();
    }
    if(typeof source.name === 'string' && source.name.trim()){
      return source.name.trim();
    }
  }
  return fallback;
}

function setProviderBadge(label){
  if(!providerBadge){ return; }
  const meta = label && typeof label === 'object' ? label : null;
  const text = resolveStorageLabel(meta || label);
  const driverHint = meta?.driver ? String(meta.driver).toLowerCase() : '';
  let badgeClass = 'provider-sql';
  if(driverHint.includes('postgres')){
    badgeClass = 'provider-pg';
  }else if(driverHint.includes('sql')){
    badgeClass = 'provider-sql';
  }else if(text.toLowerCase().includes('postgres')){
    badgeClass = 'provider-pg';
  }
  Array.from(providerBadge.classList)
    .filter(cls => cls.startsWith('provider-'))
    .forEach(cls => providerBadge.classList.remove(cls));
  providerBadge.classList.add(badgeClass);
  providerBadge.textContent = text;
  providerBadge.setAttribute('aria-label', `Active storage provider: ${text}`);
}

function setWebhookBadge(status){
  if(!webhookBadge){ return; }
  const verification = status?.verification && typeof status.verification === 'object'
    ? status.verification
    : null;
  const handshakeError = Boolean(verification?.status === 'error' || verification?.error);
  const enabled = Boolean(status?.enabled && !handshakeError);
  webhookBadge.classList.toggle('badge-webhook-on', enabled);
  webhookBadge.classList.toggle('badge-webhook-off', !enabled);
  const method = (status?.method || 'POST').toUpperCase();
  const baseText = enabled ? `Webhook ${method}` : (handshakeError ? 'Webhook error' : 'Webhook disabled');
  webhookBadge.textContent = baseText;
  const parts = [`Webhook status: ${baseText}`];
  if(verification){
    const statusLabel = verification.status || (handshakeError ? 'error' : 'ok');
    const httpLabel = verification.httpStatus ? ` (HTTP ${verification.httpStatus})` : '';
    parts.push(`Handshake ${statusLabel}${httpLabel}`);
    if(verification.verifiedAt){
      const verifiedDate = new Date(verification.verifiedAt);
      if(!Number.isNaN(verifiedDate.valueOf())){
        parts.push(`Last check ${verifiedDate.toLocaleString()}`);
      }
    }
    if(verification.error){
      parts.push(`Last error: ${verification.error}`);
    }
  }
  const ariaLabel = parts.join('. ');
  webhookBadge.setAttribute('aria-label', ariaLabel);
  webhookBadge.setAttribute('title', parts.join('\n'));
}

function updateConnectionIndicator(status){
  if(!connectionStatusEl){ return; }
  const host = state.serverHost || '10.241.211.120';
  const port = state.serverPort || 3000;
  const storageLabel = resolveStorageLabel(state.storageLabel);
  const verification = state.webhookStatus?.verification;
  const handshakeError = Boolean(verification && (verification.status === 'error' || verification.error));
  const webhookLabel = handshakeError
    ? 'Webhook error'
    : (state.webhookStatus?.enabled ? `Webhook ${state.webhookStatus.method || 'POST'}` : 'Webhook disabled');
  setLanAddress();
  setProviderBadge(state.storageMeta || storageLabel);
  setWebhookBadge(state.webhookStatus);
  connectionStatusEl.classList.remove('is-error', 'is-pending');
  let message = '';
  if(status === 'loading'){
    message = `Refreshing data from ${host}:${port}…`;
    connectionStatusEl.classList.add('is-pending');
  }else if(status === 'error'){
    message = `Unable to reach ${host}:${port}`;
    connectionStatusEl.classList.add('is-error');
  }else if(status === 'ready'){
    message = `Listening on ${host}:${port}`;
    connectionStatusEl.classList.add('is-pending');
  }else{
    message = `Connected to ${host}:${port}`;
  }
  connectionStatusEl.textContent = `${message} • ${storageLabel} • ${webhookLabel}`;
}

function normalizeWebhookStatus(status, fallback){
  const fallbackConfig = (fallback && typeof fallback === 'object') ? fallback : {};
  const fallbackMethod = (fallbackConfig.method || fallbackConfig.httpMethod || 'POST').toUpperCase();
  const fallbackHeaderCount = Number.isFinite(fallbackConfig.headerCount)
    ? fallbackConfig.headerCount
    : (Array.isArray(fallbackConfig.headers) ? fallbackConfig.headers.length : 0);
  const fallbackEnabled = Boolean(fallbackConfig.enabled && (fallbackConfig.url || fallbackConfig.targetUrl));
  const fallbackSecret = fallbackConfig.hasSecret ?? Boolean(fallbackConfig.secret);
  const fallbackVerification = (fallbackConfig.verification && typeof fallbackConfig.verification === 'object')
    ? {...fallbackConfig.verification}
    : null;
  const candidate = (status && typeof status === 'object') ? status : {};
  const verification = (candidate.verification && typeof candidate.verification === 'object')
    ? {...candidate.verification}
    : fallbackVerification;
  return {
    enabled: Boolean(candidate.enabled ?? fallbackEnabled),
    method: (candidate.method || fallbackMethod || 'POST').toUpperCase(),
    hasSecret: Boolean(candidate.hasSecret ?? fallbackSecret),
    headerCount: Number.isFinite(candidate.headerCount) ? candidate.headerCount : fallbackHeaderCount,
    verification
  };
}

function formatHeadersText(headers){
  if(!headers){
    return '';
  }
  if(Array.isArray(headers)){
    return headers
      .map(header => {
        if(typeof header === 'string'){ return header.trim(); }
        if(header && (header.name || header.key)){
          const name = String(header.name || header.key).trim();
          const value = header.value !== undefined ? String(header.value) : '';
          return name ? `${name}: ${value}`.trim() : '';
        }
        return '';
      })
      .filter(Boolean)
      .join('\n');
  }
  if(typeof headers === 'object'){
    return Object.entries(headers)
      .map(([name, value])=>`${name}: ${value}`)
      .join('\n');
  }
  return '';
}

function parseHeadersText(value){
  if(!value){
    return [];
  }
  return value.split(/\n+/)
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => {
      const idx = line.indexOf(':');
      if(idx === -1){
        return null;
      }
      const name = line.slice(0, idx).trim();
      const headerValue = line.slice(idx + 1).trim();
      return name ? {name, value: headerValue} : null;
    })
    .filter(Boolean);
}

function updateWebhookConfigureVisibility(){
  if(!webhookConfigureBtn){
    return;
  }
  const shouldShow = Boolean(webhookEnabled && webhookEnabled.checked);
  webhookConfigureBtn.hidden = !shouldShow;
  if(shouldShow){
    webhookConfigureBtn.setAttribute('aria-hidden', 'false');
  }else{
    webhookConfigureBtn.setAttribute('aria-hidden', 'true');
  }
}

function updateWebhookSimulationButton(){
  if(!webhookSimulateBtn){
    return;
  }
  const enabled = Boolean(webhookEnabled && webhookEnabled.checked);
  const hasUrl = Boolean(webhookUrl && webhookUrl.value && webhookUrl.value.trim());
  const shouldDisable = !enabled || !hasUrl;
  webhookSimulateBtn.disabled = shouldDisable;
}

function cloneWebhookConfig(config){
  return {
    enabled: Boolean(config?.enabled),
    url: config?.url || '',
    method: (config?.method || 'POST').toUpperCase(),
    secret: config?.secret || '',
    headersText: config?.headersText || ''
  };
}

function refreshWebhookUi(){
  const config = state.webhookConfig || {};
  if(webhookEnabled){ webhookEnabled.checked = Boolean(config.enabled); }
  if(webhookUrl){ webhookUrl.value = config.url || ''; }
  if(webhookMethod){ webhookMethod.value = (config.method || 'POST').toUpperCase(); }
  if(webhookSecret){ webhookSecret.value = config.secret || ''; }
  if(webhookHeaders){ webhookHeaders.value = config.headersText || ''; }
  syncWebhookFields();
  updateWebhookPreview();
  updateWebhookConfigureVisibility();
}

function openWebhookModal(){
  if(!webhookModal){
    return;
  }
  if(webhookModal.classList.contains('open')){
    return;
  }
  refreshWebhookUi();
  webhookModalSnapshot = cloneWebhookConfig(state.webhookConfig);
  webhookModal.classList.add('open');
  webhookModal.setAttribute('aria-hidden', 'false');
  requestAnimationFrame(()=>{
    if(webhookUrl && !webhookUrl.disabled){
      webhookUrl.focus();
      webhookUrl.select?.();
    }
  });
}

function closeWebhookModal(options){
  if(!webhookModal){
    return;
  }
  const restore = Boolean(options?.restore);
  const snapshot = webhookModalSnapshot;
  webhookModalSnapshot = null;
  if(restore && snapshot){
    state.webhookConfig = cloneWebhookConfig(snapshot);
    refreshWebhookUi();
  }
  webhookModal.classList.remove('open');
  webhookModal.setAttribute('aria-hidden', 'true');
  updateWebhookConfigureVisibility();
}

function saveWebhookModal(){
  updateWebhookPreview();
  closeWebhookModal();
  toast('Webhook settings staged. Save admin settings to apply.');
}

function syncWebhookFields(){
  if(!webhookEnabled){ return; }
  const enabled = webhookEnabled.checked;
  [webhookUrl, webhookMethod, webhookSecret, webhookHeaders].forEach(input=>{
    if(input){
      input.disabled = !enabled;
      if(!enabled){
        input.classList.add('is-disabled');
      }else{
        input.classList.remove('is-disabled');
      }
    }
  });
  if(webhookPreview){
    webhookPreview.classList.toggle('is-disabled', !enabled || !(webhookUrl?.value.trim()));
  }
  updateWebhookSimulationButton();
}

function updateWebhookPreview(){
  if(!webhookPreview){
    return;
  }
  state.webhookConfig.enabled = webhookEnabled ? webhookEnabled.checked : false;
  state.webhookConfig.url = webhookUrl ? webhookUrl.value.trim() : '';
  state.webhookConfig.method = webhookMethod ? webhookMethod.value.toUpperCase() : 'POST';
  state.webhookConfig.secret = webhookSecret ? webhookSecret.value.trim() : '';
  state.webhookConfig.headersText = webhookHeaders ? webhookHeaders.value : '';

  const enabled = state.webhookConfig.enabled;
  const url = state.webhookConfig.url;
  const method = state.webhookConfig.method;
  const show = getCurrentShow();
  const entry = show?.entries?.[0];
  let row = buildWebhookRow(show || {}, entry || {});
  const emptyRow = EXPORT_COLUMNS.every(col => row[col] === '' || row[col] === null || row[col] === undefined);
  if(emptyRow){
    row = buildSampleWebhookRow();
  }
  const message = buildWebhookMessage(row);
  const messageJson = JSON.stringify(message, null, 2);
  const headerCells = EXPORT_COLUMNS.map(column=>`<th>${escapeHtml(column)}</th>`).join('');
  const rowCells = EXPORT_COLUMNS.map(column=>`<td>${escapeHtml(row[column] ?? '')}</td>`).join('');
  const secret = webhookSecret ? webhookSecret.value.trim() : '';
  const additionalHeaders = parseHeadersText(webhookHeaders ? webhookHeaders.value : '');
  const hasCustomAuthorization = additionalHeaders.some(header => header && header.name && header.name.toLowerCase() === 'authorization');
  const combinedHeaders = [
    {name: 'Content-Type', value: 'application/json'},
    ...(!hasCustomAuthorization && secret ? [{name: 'Authorization', value: `Bearer ${secret}`}]: []),
    ...additionalHeaders
  ];
  const headersListHtml = combinedHeaders.length
    ? combinedHeaders.map(({name, value}) => `<li><code>${escapeHtml(name)}: ${escapeHtml(value)}</code></li>`).join('')
    : '<li class="webhook-headers-empty">No headers configured</li>';
  const statusMessage = !enabled
    ? 'Webhook disabled. Enable the toggle to deliver entries automatically.'
    : (url ? `Entries will ${escapeHtml(method)} to ${escapeHtml(url)}.` : 'Provide a webhook URL to activate delivery.');
  webhookPreview.innerHTML = `
    <div class="webhook-status ${enabled && url ? 'is-on' : 'is-off'}">${statusMessage}</div>
    <div class="webhook-headers">
      <div class="webhook-json-label">HTTP headers</div>
      <ul class="webhook-headers-list">${headersListHtml}</ul>
    </div>
    <div class="webhook-table-wrap">
      <table class="webhook-table">
        <thead><tr>${headerCells}</tr></thead>
        <tbody><tr>${rowCells}</tr></tbody>
      </table>
    </div>
    <div class="webhook-json-wrap">
      <div class="webhook-json-label">JSON payload (.message)</div>
      <pre class="webhook-json"><code>${escapeHtml(messageJson)}</code></pre>
    </div>
  `;
  webhookPreview.classList.toggle('is-disabled', !enabled || !url);
  updateWebhookSimulationButton();
}

function toast(message, isError){
  if(!toastEl){ return; }
  toastEl.textContent = message;
  toastEl.classList.add('show');
  toastEl.style.borderColor = isError ? 'var(--danger)' : 'var(--border)';
  setTimeout(()=> toastEl.classList.remove('show'), 2200);
}

function downloadFile(content, filename, type){
  const blob = new Blob([content], {type: type || 'application/octet-stream'});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(()=> URL.revokeObjectURL(url), 500);
}

function csvEscape(value){
  if(value == null){
    return '';
  }
  const str = String(value);
  if(/[",\r\n]/.test(str)){
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function getDefaultUnits(){
  const label = (state.unitLabel || '').trim();
  if(!label){
    return Array.from({length: 12}, (_, i)=> `U${i+1}`);
  }
  if(label.toLowerCase() === 'drone'){
    return Array.from({length: 12}, (_, i)=> `D${i+1}`);
  }
  const prefix = label.charAt(0).toUpperCase();
  return Array.from({length: 12}, (_, i)=> `${prefix}${i+1}`);
}

function formatDateUS(dateStr){
  if(!dateStr || !dateStr.includes('-')){
    return dateStr || '';
  }
  const [y,m,d] = dateStr.split('-');
  return `${m}-${d}-${y}`;
}

function formatTime12Hour(timeStr){
  if(!timeStr || !timeStr.includes(':')){
    return timeStr || '';
  }
  const [h, m] = timeStr.split(':');
  let hour = parseInt(h, 10);
  const suffix = hour >= 12 ? 'PM' : 'AM';
  if(hour === 0){ hour = 12; }
  if(hour > 12){ hour -= 12; }
  return `${hour}:${m} ${suffix}`;
}

function formatDateTime(value){
  const timestamp = toNumber(value);
  if(timestamp === null){
    return '';
  }
  try{
    return new Date(timestamp).toLocaleString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  }catch(err){
    return '';
  }
}

function escapeHtml(str){
  return String(str || '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
}

function normalizeArchivedShow(raw = {}){
  const crew = Array.isArray(raw.crew) ? normalizeNameList(raw.crew || [], {sort: false}) : [];
  const entries = Array.isArray(raw.entries)
    ? raw.entries.map(normalizeArchivedEntry).sort((a, b)=> (b.ts || 0) - (a.ts || 0))
    : [];
  const show = {
    id: raw.id,
    date: typeof raw.date === 'string' ? raw.date : '',
    time: typeof raw.time === 'string' ? raw.time : '',
    label: typeof raw.label === 'string' ? raw.label : '',
    leadPilot: typeof raw.leadPilot === 'string' ? raw.leadPilot : '',
    monkeyLead: typeof raw.monkeyLead === 'string' ? raw.monkeyLead : '',
    notes: typeof raw.notes === 'string' ? raw.notes : '',
    crew,
    entries,
    disciplineId: typeof raw.disciplineId === 'string' ? raw.disciplineId.trim().toLowerCase() : (state.defaultDisciplineId || 'drones'),
    createdAt: toNumber(raw.createdAt),
    archivedAt: toNumber(raw.archivedAt),
    deletedAt: toNumber(raw.deletedAt)
  };
  return show;
}

function normalizeActiveShow(raw = {}){
  const crew = Array.isArray(raw.crew) ? normalizeNameList(raw.crew || [], {sort: false}) : [];
  const entries = Array.isArray(raw.entries)
    ? raw.entries.map(normalizeArchivedEntry)
    : [];
  return {
    id: raw.id,
    date: typeof raw.date === 'string' ? raw.date : '',
    time: typeof raw.time === 'string' ? raw.time : '',
    label: typeof raw.label === 'string' ? raw.label : '',
    leadPilot: typeof raw.leadPilot === 'string' ? raw.leadPilot : '',
    monkeyLead: typeof raw.monkeyLead === 'string' ? raw.monkeyLead : '',
    notes: typeof raw.notes === 'string' ? raw.notes : '',
    crew,
    entries,
    disciplineId: typeof raw.disciplineId === 'string' ? raw.disciplineId.trim().toLowerCase() : (state.defaultDisciplineId || 'drones'),
    createdAt: toNumber(raw.createdAt),
    updatedAt: toNumber(raw.updatedAt)
  };
}

function normalizeCalendarEvent(raw = {}){
  const start = raw.start instanceof Date ? raw.start : parseDayKey(raw.start);
  const end = raw.end instanceof Date ? raw.end : parseDayKey(raw.end);
  const startTs = Number.isFinite(raw.startTs) ? raw.startTs : start?.getTime();
  const endTs = Number.isFinite(raw.endTs) ? raw.endTs : end?.getTime();
  const startDate = startTs ? new Date(startTs) : start || null;
  const endDate = endTs ? new Date(endTs) : end || null;
  return {
    id: raw.id,
    title: typeof raw.title === 'string' ? raw.title : 'Event',
    description: typeof raw.description === 'string' ? raw.description : '',
    location: typeof raw.location === 'string' ? raw.location : '',
    start: startDate ? startDate.toISOString() : '',
    end: endDate ? endDate.toISOString() : '',
    startTs: startDate ? startDate.getTime() : null,
    endTs: endDate ? endDate.getTime() : null,
    startDate,
    endDate,
    dayKey: startDate ? formatDayKey(startDate) : '',
    allDay: Boolean(raw.allDay)
  };
}

function normalizeArchivedEntry(raw = {}){
  const entry = {
    id: typeof raw.id === 'string' ? raw.id : '',
    ts: toNumber(raw.ts),
    unitId: typeof raw.unitId === 'string' ? raw.unitId : '',
    planned: typeof raw.planned === 'string' ? raw.planned : '',
    launched: typeof raw.launched === 'string' ? raw.launched : '',
    status: typeof raw.status === 'string' ? raw.status : '',
    primaryIssue: typeof raw.primaryIssue === 'string' ? raw.primaryIssue : '',
    subIssue: typeof raw.subIssue === 'string' ? raw.subIssue : '',
    otherDetail: typeof raw.otherDetail === 'string' ? raw.otherDetail : '',
    severity: typeof raw.severity === 'string' ? raw.severity : '',
    rootCause: typeof raw.rootCause === 'string' ? raw.rootCause : '',
    actions: normalizeNameList(Array.isArray(raw.actions) ? raw.actions : []),
    operator: typeof raw.operator === 'string' ? raw.operator : '',
    batteryId: typeof raw.batteryId === 'string' ? raw.batteryId : '',
    delaySec: null,
    commandRx: typeof raw.commandRx === 'string' ? raw.commandRx : '',
    notes: typeof raw.notes === 'string' ? raw.notes : ''
  };
  const delay = toNumber(raw.delaySec);
  entry.delaySec = Number.isFinite(delay) ? delay : null;
  return entry;
}

function toNumber(value){
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

async function apiRequest(url, options){
  const opts = options ? {...options} : {};
  const skipAuthHandlers = Boolean(opts.skipAuthHandlers);
  delete opts.skipAuthHandlers;
  if(opts.body && typeof opts.body !== 'string'){
    opts.body = JSON.stringify(opts.body);
  }
  opts.headers = Object.assign({'Content-Type': 'application/json'}, opts.headers || {});
  const res = await fetch(url, opts);
  if(res.status === 204){
    return null;
  }
  let data = null;
  try{
    data = await res.json();
  }catch(err){
    data = null;
  }
  if(!res.ok){
    const message = data && data.error ? data.error : `Request failed (${res.status})`;
    if(!skipAuthHandlers){
      if(res.status === 401){
        handleSessionExpired();
      }else if(res.status === 423){
        handlePasswordResetRequired();
      }
    }
    throw new Error(message);
  }
  return data;
}

function setupIdleActivityTracking(){
  if(idleListenersRegistered){
    return;
  }
  ['mousemove','keydown','mousedown','touchstart','scroll'].forEach(eventName =>{
    document.addEventListener(eventName, handleIdleActivity, {passive: true});
  });
  document.addEventListener('visibilitychange', handleIdleVisibilityChange);
  idleListenersRegistered = true;
}

function handleIdleActivity(){
  resetIdleTimer();
}

function handleIdleVisibilityChange(){
  if(document.visibilityState === 'visible'){
    resetIdleTimer();
  }
}

function resetIdleTimer(){
  if(!state.session){
    clearIdleTimer();
    return;
  }
  clearIdleTimer();
  idleTimerId = window.setTimeout(handleIdleLogout, IDLE_LOGOUT_MS);
}

function clearIdleTimer(){
  if(idleTimerId){
    clearTimeout(idleTimerId);
    idleTimerId = null;
  }
}

function handleIdleLogout(){
  toast('Logging out due to inactivity');
  logout();
}

function setupUnloadLogoutHandler(){
  if(unloadHandlerRegistered){
    return;
  }
  window.addEventListener('beforeunload', handleBeforeUnloadLogout);
  unloadHandlerRegistered = true;
}

function handleBeforeUnloadLogout(){
  if(suppressUnloadLogout || !state.session){
    return;
  }
  sendLogoutBeacon();
}

function sendLogoutBeacon(){
  try{
    const body = new Blob([JSON.stringify({reason: 'unload'})], {type: 'application/json'});
    if(typeof navigator !== 'undefined' && typeof navigator.sendBeacon === 'function'){
      navigator.sendBeacon('/api/auth/logout', body);
      return;
    }
  }catch(err){
    console.warn('Beacon setup failed', err);
  }
  if(typeof fetch === 'function'){
    fetch('/api/auth/logout', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: '{}',
      keepalive: true
    }).catch(()=>{});
  }
}

function startMenuClock(){
  if(menuClockInterval){
    return;
  }
  updateMenuClock();
  menuClockInterval = window.setInterval(updateMenuClock, 1000);
}

function updateMenuClock(){
  if(!menuDateTime){
    return;
  }
  const now = new Date();
  menuDateTime.textContent = now.toLocaleString(undefined, {
    weekday: 'short',
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit'
  });
}



function getOperatorNames(additional = []){
  return normalizeNameList([state.staff?.operators || [], additional], {sort: true});
}

function getStagecrewNames(additional = []){
  return normalizeNameList([state.staff?.stagecrew || [], additional], {sort: true});
}

function getLeadNames(additional = []){
  return normalizeNameList([state.staff?.leads || [], additional], {sort: true});
}

function renderCrewOptions(selected = []){
  if(!showCrewSelect){
    return;
  }
  const selectedList = normalizeNameList(selected);
  const crewNames = getStagecrewNames([selectedList]);
  if(!crewNames.length){
    showCrewSelect.innerHTML = '<option value="">Add crew via admin settings</option>';
    showCrewSelect.disabled = true;
    return;
  }
  showCrewSelect.disabled = false;
  showCrewSelect.innerHTML = crewNames.map(name=>`<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`).join('');
  const selectedSet = new Set(selectedList.map(name => name.toLowerCase()));
  Array.from(showCrewSelect.options).forEach(option =>{
    option.selected = selectedSet.has(option.value.toLowerCase());
  });
}

function renderPilotAssignments(show){
  if(leadPilotSelect){
    const leadNames = getLeadNames([show?.leadPilot]);
    if(!leadNames.length){
      leadPilotSelect.innerHTML = '<option value="">Add leads via admin settings</option>';
      leadPilotSelect.disabled = true;
    }else{
      const leadOptions = [''].concat(leadNames).map(name=>{
        if(!name){
          return '<option value="">Select</option>';
        }
        return `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`;
      }).join('');
      leadPilotSelect.innerHTML = leadOptions;
      leadPilotSelect.disabled = false;
      const leadValue = show?.leadPilot || '';
      const leadMatch = leadNames.find(name => name.toLowerCase() === leadValue.toLowerCase());
      leadPilotSelect.value = leadMatch || '';
    }
  }
  if(monkeyLeadSelect){
    const monkeyNames = getStagecrewNames([show?.monkeyLead]);
    if(!monkeyNames.length){
      monkeyLeadSelect.innerHTML = '<option value="">Add crew via admin settings</option>';
      monkeyLeadSelect.disabled = true;
    }else{
      const monkeyOptions = [''].concat(monkeyNames).map(name=>{
        if(!name){
          return '<option value="">Select</option>';
        }
        return `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`;
      }).join('');
      monkeyLeadSelect.innerHTML = monkeyOptions;
      monkeyLeadSelect.disabled = false;
      const monkeyValue = show?.monkeyLead || '';
      const monkeyMatch = monkeyNames.find(name => name.toLowerCase() === monkeyValue.toLowerCase());
      monkeyLeadSelect.value = monkeyMatch || '';
    }
  }
}

function normalizeNameList(list = [], options = {}){
  const {sort = false} = options;
  const seen = new Set();
  const result = [];
  const queue = Array.isArray(list) ? list.slice() : [list];
  while(queue.length){
    const value = queue.shift();
    if(Array.isArray(value)){
      queue.push(...value);
      continue;
    }
    const name = typeof value === 'string' ? value.trim() : '';
    if(!name){
      continue;
    }
    const key = name.toLowerCase();
    if(seen.has(key)){
      continue;
    }
    seen.add(key);
    result.push(name);
  }
  if(sort){
    result.sort((a,b)=> a.localeCompare(b, undefined, {sensitivity: 'base'}));
  }
  return result;
}

function getShowById(id){
  return state.shows.find(s=>s.id===id) || null;
}

function el(id){
  return document.getElementById(id);
}

function qs(selector, root=document){
  return root.querySelector(selector);
}

function qsa(selector, root=document){
  return Array.from(root.querySelectorAll(selector));
}
