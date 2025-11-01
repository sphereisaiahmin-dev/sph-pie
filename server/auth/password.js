const bcrypt = require('bcryptjs');

const DEFAULT_ROUNDS = Number.isFinite(Number(process.env.BCRYPT_ROUNDS))
  ? Math.max(10, Number(process.env.BCRYPT_ROUNDS))
  : 12;

async function hashPassword(password, options = {}){
  const opts = options || {};
  if(typeof password !== 'string' || password.length === 0){
    const err = new Error('Password is required.');
    err.status = 400;
    throw err;
  }
  if(!opts.allowShort && password.length < 8){
    const err = new Error('Password must be at least 8 characters.');
    err.status = 400;
    throw err;
  }
  return bcrypt.hash(password, DEFAULT_ROUNDS);
}

async function verifyPassword(password, hash){
  if(!hash){
    return false;
  }
  if(typeof password !== 'string' || !password){
    return false;
  }
  try{
    return await bcrypt.compare(password, hash);
  }catch(err){
    return false;
  }
}

module.exports = {
  hashPassword,
  verifyPassword
};
