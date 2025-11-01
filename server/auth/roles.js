const VALID_ROLES = ['admin', 'lead', 'operator', 'crew'];

function normalizeRoleName(role){
  if(typeof role !== 'string'){
    return null;
  }
  const trimmed = role.trim().toLowerCase();
  if(!trimmed){
    return null;
  }
  if(trimmed === 'pilots' || trimmed === 'pilot'){
    return 'operator';
  }
  if(trimmed === 'monkeylead' || trimmed === 'monkey-lead' || trimmed === 'monkey_lead'){
    return 'crew';
  }
  if(trimmed === 'leads' || trimmed === 'leadpilot' || trimmed === 'lead-pilot' || trimmed === 'lead_pilot'){
    return 'lead';
  }
  if(VALID_ROLES.includes(trimmed)){
    return trimmed;
  }
  return null;
}

function normalizeRoles(input, {dedupe = true} = {}){
  const values = Array.isArray(input) ? input : [input];
  const normalized = [];
  values.forEach(value =>{
    const role = normalizeRoleName(value);
    if(!role){
      return;
    }
    if(dedupe && normalized.includes(role)){
      return;
    }
    normalized.push(role);
  });
  return normalized;
}

function hasRole(userRoles = [], requiredRoles = []){
  if(!Array.isArray(requiredRoles) || requiredRoles.length === 0){
    return true;
  }
  const normalizedUserRoles = normalizeRoles(userRoles, {dedupe: true});
  const normalizedRequiredRoles = normalizeRoles(requiredRoles, {dedupe: true});
  if(normalizedUserRoles.includes('admin')){
    return true;
  }
  return normalizedRequiredRoles.some(role => normalizedUserRoles.includes(role));
}

module.exports = {
  VALID_ROLES,
  normalizeRoleName,
  normalizeRoles,
  hasRole
};
