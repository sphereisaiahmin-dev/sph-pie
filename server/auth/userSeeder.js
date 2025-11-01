const { hashPassword } = require('./password');
const { normalizeRoles } = require('./roles');
const { getProvider } = require('../storage');

const DEFAULT_USERS = [
  {name: 'Isaiah Mincher', email: 'isaiah.mincher@thesphere.com', roles: ['admin']},
  {name: 'Zach Harvest', email: 'zach.harvest@thesphere.com', roles: ['admin']},
  {name: 'Nazar Vasylyk', email: 'nazar.vasylyk@thesphere.com', roles: ['lead', 'operator']},
  {name: 'Nick Aquuino', email: 'nicholas.aquino@thesphere.com', roles: ['lead', 'operator']},
  {name: 'Alexandar Brodnik', email: 'alexandar.brodnik@thesphere.com', roles: ['lead', 'operator']},
  {name: 'Cleo Kelley', email: 'cleo.kelley@thesphere.com', roles: ['crew']},
  {name: 'Dallas Howerton', email: 'dallas.howerton@thesphere.com', roles: ['crew']}
];

async function seedDefaultUsers({defaultPassword} = {}){
  const provider = getProvider();
  const passwordHash = defaultPassword ? await hashPassword(defaultPassword, {allowShort: true}) : null;
  const requireChange = Boolean(passwordHash);
  for(const user of DEFAULT_USERS){
    const existing = await provider.getUserByEmail(user.email);
    if(existing){
      const mergedRoles = mergeRoles(existing.roles, user.roles);
      const needsRoleUpdate = mergedRoles.length !== existing.roles.length
        || mergedRoles.some(role => !existing.roles.includes(role));
      if(needsRoleUpdate){
        await provider.updateUser(existing.id, {roles: mergedRoles});
      }
      if(passwordHash && !existing.passwordHash){
        await provider.setUserPassword(existing.id, passwordHash, {requireChange});
      }
      continue;
    }
    await provider.createUser({
      name: user.name,
      email: user.email,
      passwordHash,
      roles: user.roles,
      isActive: true,
      mustChangePassword: requireChange
    });
  }
}

function mergeRoles(existing = [], incoming = []){
  const normalizedExisting = normalizeRoles(existing, {dedupe: true});
  const normalizedIncoming = normalizeRoles(incoming, {dedupe: true});
  const merged = new Set([...normalizedExisting, ...normalizedIncoming]);
  return Array.from(merged);
}

module.exports = {
  DEFAULT_USERS,
  seedDefaultUsers,
  mergeRoles
};
