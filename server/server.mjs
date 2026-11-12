// Investa API (graduation-project simulation). No money moves; recorded agreements are not legal equity.
// Runs on node:sqlite (local file) or PostgreSQL (DATABASE_URL, via the optional `pg` package), same code and SQL.
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { AsyncLocalStorage } from 'node:async_hooks';
import { scryptSync, randomBytes, timingSafeEqual, createHash } from 'node:crypto';

const SCHEMA = `

  CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, email TEXT UNIQUE NOT NULL, name TEXT NOT NULL, role TEXT NOT NULL CHECK(role IN('investor','founder')), pw TEXT NOT NULL, created INTEGER NOT NULL);
  CREATE TABLE IF NOT EXISTS sessions(token_hash TEXT PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), expires INTEGER NOT NULL);
  CREATE TABLE IF NOT EXISTS resets(token_hash TEXT PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), expires INTEGER NOT NULL, used INTEGER NOT NULL DEFAULT 0);
  CREATE TABLE IF NOT EXISTS startups(id INTEGER PRIMARY KEY, founder_id INTEGER NOT NULL REFERENCES users(id), name TEXT NOT NULL, pitch TEXT NOT NULL DEFAULT '', offered_pct REAL NOT NULL CHECK(offered_pct>0 AND offered_pct<=100));
  CREATE TABLE IF NOT EXISTS products(id INTEGER PRIMARY KEY, startup_id INTEGER NOT NULL REFERENCES startups(id), name TEXT NOT NULL, price REAL NOT NULL CHECK(price>=0), cost REAL CHECK(cost IS NULL OR cost>=0), available INTEGER NOT NULL DEFAULT 0, sold INTEGER NOT NULL DEFAULT 0);
  CREATE TABLE IF NOT EXISTS interests(id INTEGER PRIMARY KEY, startup_id INTEGER NOT NULL REFERENCES startups(id), investor_id INTEGER NOT NULL REFERENCES users(id), status TEXT NOT NULL CHECK(status IN('pending','accepted','declined')), UNIQUE(startup_id,investor_id));
  CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY, interest_id INTEGER NOT NULL REFERENCES interests(id), sender_id INTEGER NOT NULL REFERENCES users(id), body TEXT NOT NULL, created INTEGER NOT NULL);
  CREATE TABLE IF NOT EXISTS offers(id INTEGER PRIMARY KEY, interest_id INTEGER NOT NULL REFERENCES interests(id), amount REAL NOT NULL CHECK(amount>0), pct REAL NOT NULL CHECK(pct>0 AND pct<=100), status TEXT NOT NULL CHECK(status IN('pending','countered','recorded','declined','withdrawn','expired')), proposed_by TEXT NOT NULL CHECK(proposed_by IN('investor','founder')), parent_id INTEGER, expires INTEGER NOT NULL, created INTEGER NOT NULL);
  CREATE TABLE IF NOT EXISTS agreements(id INTEGER PRIMARY KEY, offer_id INTEGER UNIQUE NOT NULL REFERENCES offers(id), startup_id INTEGER NOT NULL, investor_id INTEGER NOT NULL, amount REAL NOT NULL, pct REAL NOT NULL, created INTEGER NOT NULL);
  CREATE TABLE IF NOT EXISTS ledger(id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), kind TEXT NOT NULL CHECK(kind IN('income','expense')), note TEXT NOT NULL, amount REAL NOT NULL CHECK(amount>0), d TEXT NOT NULL);
  CREATE TABLE IF NOT EXISTS invoices(id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), customer TEXT NOT NULL, item TEXT NOT NULL, amount REAL NOT NULL CHECK(amount>0), status TEXT NOT NULL DEFAULT 'Due', d TEXT NOT NULL);
  CREATE TABLE IF NOT EXISTS login_fails(email TEXT PRIMARY KEY, n INTEGER NOT NULL, t INTEGER NOT NULL);
  CREATE TABLE IF NOT EXISTS rate_hits(k TEXT PRIMARY KEY, w INTEGER NOT NULL, n INTEGER NOT NULL);`;
const SQLITE_EXTRA = `
  CREATE TRIGGER IF NOT EXISTS agreements_cap BEFORE INSERT ON agreements BEGIN
    SELECT RAISE(ABORT, 'cap_exceeded') WHERE (SELECT COALESCE(SUM(pct),0) FROM agreements WHERE startup_id=NEW.startup_id) + NEW.pct > (SELECT offered_pct FROM startups WHERE id=NEW.startup_id) + 1e-9;
  END;`;
const PG_EXTRA = `
  CREATE OR REPLACE FUNCTION agreements_cap() RETURNS trigger AS $$
  DECLARE cap DOUBLE PRECISION; cur DOUBLE PRECISION;
  BEGIN
    SELECT offered_pct INTO cap FROM startups WHERE id = NEW.startup_id FOR UPDATE;
    SELECT COALESCE(SUM(pct),0) INTO cur FROM agreements WHERE startup_id = NEW.startup_id;
    IF cur + NEW.pct > cap + 1e-9 THEN RAISE EXCEPTION 'cap_exceeded'; END IF;
    RETURN NEW;
  END $$ LANGUAGE plpgsql;
  DROP TRIGGER IF EXISTS agreements_cap ON agreements;
  CREATE TRIGGER agreements_cap BEFORE INSERT ON agreements FOR EACH ROW EXECUTE FUNCTION agreements_cap();`;
const toPg = (ddl) => ddl.replace(/INTEGER PRIMARY KEY/g, 'BIGSERIAL PRIMARY KEY').replace(/\bREAL\b/g, 'DOUBLE PRECISION').replace(/\bINTEGER\b/g, 'BIGINT');
const numbered = (sql) => { let n = 0; return sql.replace(/\?/g, () => '$' + (++n)); };

// Drivers share one tiny async interface: q (rows), one (first row), run, ins (insert returning id), tx, exec, close.
async function makeDriver(spec) {
  if (spec && spec.pglite) { // tests only: in-process PostgreSQL
    const db = spec.pglite; const als = new AsyncLocalStorage();
    const exec = (sql, a = []) => db.query(numbered(sql), a);
    return { kind: 'pglite', serial: true, q: async (s, ...a) => (await exec(s, a)).rows, one: async (s, ...a) => (await exec(s, a)).rows[0],
      run: async (s, ...a) => { await exec(s, a); }, ins: async (s, ...a) => (await exec(s + ' RETURNING id', a)).rows[0].id,
      tx: async (fn) => { if (als.getStore()) return fn(); await db.exec('BEGIN'); try { const r = await als.run(true, fn); await db.exec('COMMIT'); return r; } catch (e) { await db.exec('ROLLBACK'); throw e; } },
      exec: async (ddl) => { await db.exec(toPg(ddl)); await db.exec(PG_EXTRA); }, close: async () => db.close() };
  }
  if (typeof spec === 'string' && /^postgres(ql)?:\/\//.test(spec)) {
    const { default: pg } = await import('pg'); pg.types.setTypeParser(20, Number); // BIGINT as number (values stay well below 2^53)
    const pool = new pg.Pool({ connectionString: spec, max: Number(process.env.PG_POOL || 5), ssl: process.env.PG_SSL === 'off' ? false : { rejectUnauthorized: false } });
    const als = new AsyncLocalStorage(); const c = () => als.getStore() || pool;
    const exec = (sql, a = []) => c().query(numbered(sql), a);
    return { kind: 'pg', serial: false, q: async (s, ...a) => (await exec(s, a)).rows, one: async (s, ...a) => (await exec(s, a)).rows[0],
      run: async (s, ...a) => { await exec(s, a); }, ins: async (s, ...a) => (await exec(s + ' RETURNING id', a)).rows[0].id,
      tx: async (fn) => { if (als.getStore()) return fn(); const cl = await pool.connect(); try { await cl.query('BEGIN'); const r = await als.run(cl, fn); await cl.query('COMMIT'); return r; } catch (e) { await cl.query('ROLLBACK').catch(() => {}); throw e; } finally { cl.release(); } },
      exec: async (ddl) => { await pool.query(toPg(ddl)); await pool.query(PG_EXTRA); }, close: () => pool.end() };
  }
  const { DatabaseSync } = await import('node:sqlite'); const db = new DatabaseSync(spec || ':memory:'); db.exec('PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;');
  let depth = 0;
  return { kind: 'sqlite', serial: true, raw: db, q: async (s, ...a) => db.prepare(s).all(...a), one: async (s, ...a) => db.prepare(s).get(...a),
    run: async (s, ...a) => { db.prepare(s).run(...a); }, ins: async (s, ...a) => Number(db.prepare(s + ' RETURNING id').get(...a).id),
    tx: async (fn) => { if (depth) return fn(); depth++; db.exec('BEGIN IMMEDIATE'); try { const r = await fn(); db.exec('COMMIT'); return r; } catch (e) { db.exec('ROLLBACK'); throw e; } finally { depth--; } },
    exec: async (ddl) => { db.exec(ddl); db.exec(SQLITE_EXTRA); }, close: async () => db.close() };
}

export function createApp(dbSpec = ':memory:') {
  let driver; const ready = (async () => { driver = await makeDriver(dbSpec); await driver.exec(SCHEMA); app.db = driver; })();
  const q = (...a) => driver.q(...a), one = (...a) => driver.one(...a), run = (...a) => driver.run(...a), ins = (...a) => driver.ins(...a), tx = (fn) => driver.tx(fn);
  let serial = true; ready.then(() => { serial = driver.serial; });
  let chain = Promise.resolve(); const withLock = (fn) => { const p = chain.then(fn, fn); chain = p.catch(() => {}); return p; };
  const app = {};
  const now = () => Date.now();
  const DAY = 86400000;
  // Policy values are configurable and uncommitted until the user decides (offer expiry proposal: 7 days).
  const OFFER_TTL = Number(process.env.OFFER_TTL_DAYS || 7) * DAY, SESSION_TTL = 7 * DAY, RESET_TTL = 30 * 60000;
  // Login lockout is stored in the database (survives restarts and serverless instances): 5 failures per email lock it for 15 minutes.
  const MAX_FAILS = 5, LOCK_MS = 15 * 60000;
  const sha = (s) => createHash('sha256').update(s).digest('hex');
  const hashPw = (p) => { const salt = randomBytes(16); return `s1$${salt.toString('hex')}$${scryptSync(p, salt, 32).toString('hex')}`; };
  const checkPw = (p, stored) => { const [, s, h] = stored.split('$'); const a = scryptSync(p, Buffer.from(s, 'hex'), 32), b = Buffer.from(h, 'hex'); return a.length === b.length && timingSafeEqual(a, b); };
  class HttpError extends Error { constructor(code, msg) { super(msg); this.code = code; } }
  const bad = (m) => new HttpError(400, m), forbid = (m) => new HttpError(403, m), nf = (m) => new HttpError(404, m), conflict = (m) => new HttpError(409, m);
  const num = (v, name) => { const n = Number(v); if (!Number.isFinite(n)) throw bad(`${name} must be a number`); return n; };
  const str = (v, name, max = 200) => { if (typeof v !== 'string' || !v.trim() || v.length > max) throw bad(`${name} is required`); return v.trim(); };

  // Rule: recorded + still-open pending offers do not need to be reserved, but a record can never push total over offered_pct.
  const recordedPct = async (startupId) => (await one('SELECT COALESCE(SUM(pct),0) t FROM agreements WHERE startup_id=?', startupId)).t;
  const expireOffers = async () => run("UPDATE offers SET status='expired' WHERE status IN('pending','countered') AND expires<?", now());

  const publicUser = (u) => ({ id: u.id, email: u.email, name: u.name, role: u.role });
  const interestFor = async (id, user) => {
    const i = await one('SELECT i.*, s.founder_id, s.offered_pct FROM interests i JOIN startups s ON s.id=i.startup_id WHERE i.id=?', id);
    if (!i || (i.investor_id !== user.id && i.founder_id !== user.id)) throw nf('Not found');
    return i;
  };
  const side = (i, user) => (i.investor_id === user.id ? 'investor' : 'founder');

  const routes = [];
  const route = (method, pattern, auth, fn) => routes.push({ method, re: new RegExp('^' + pattern.replace(/:(\w+)/g, '(?<$1>[^/]+)') + '$'), auth, fn });

  route('POST', '/auth/register', false, async ({ body }) => {
    const email = str(body.email, 'email').toLowerCase(), name = str(body.name, 'name'), pw = body.password;
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) throw bad('Enter a valid email');
    if (typeof pw !== 'string' || pw.length < 8) throw bad('Password must be at least 8 characters');
    if (!['investor', 'founder'].includes(body.role)) throw bad('role must be investor or founder');
    if (await one('SELECT 1 x FROM users WHERE email=?', email)) throw conflict('An account with this email already exists');
    const rid = await ins('INSERT INTO users(email,name,role,pw,created) VALUES(?,?,?,?,?)', email, name, body.role, hashPw(pw), now());
    return [201, publicUser(await one('SELECT * FROM users WHERE id=?', rid))];
  });
  route('POST', '/auth/login', false, async ({ body }) => {
    const email = String(body.email || '').toLowerCase();
    const f = await one('SELECT * FROM login_fails WHERE email=?', email); if (f && f.n >= MAX_FAILS && now() - f.t < LOCK_MS) throw new HttpError(429, 'Too many attempts, try again later');
    const u = await one('SELECT * FROM users WHERE email=?', email);
    if (!u || !checkPw(String(body.password || ''), u.pw)) { const g = f && now() - f.t < LOCK_MS ? f : { n: 0 }; await run('INSERT INTO login_fails(email,n,t) VALUES(?,?,?) ON CONFLICT(email) DO UPDATE SET n=excluded.n, t=excluded.t', email, g.n + 1, now()); throw new HttpError(401, 'Wrong email or password'); }
    await run('DELETE FROM login_fails WHERE email=?', email);
    const token = randomBytes(32).toString('hex');
    await run('INSERT INTO sessions VALUES(?,?,?)', sha(token), u.id, now() + SESSION_TTL);
    return { token, user: publicUser(u) };
  });
  route('POST', '/auth/logout', true, async ({ token }) => { await run('DELETE FROM sessions WHERE token_hash=?', sha(token)); return { ok: true }; });
  route('POST', '/auth/forgot', false, async ({ body, mailer }) => {
    if (!mailerReady()) throw new HttpError(503, 'Password reset by email is not set up on this server');
    const u = await one('SELECT * FROM users WHERE email=?', String(body.email || '').toLowerCase());
    if (u) { const t = randomBytes(24).toString('hex'); await run('INSERT INTO resets VALUES(?,?,?,0)', sha(t), u.id, now() + RESET_TTL); mailer(u.email, t); }
    return { ok: true, message: 'If that email has an account, a reset link was sent.' }; // same answer either way
  });
  route('POST', '/auth/reset', false, async ({ body }) => {
    if (typeof body.password !== 'string' || body.password.length < 8) throw bad('Password must be at least 8 characters');
    const r = await one('SELECT * FROM resets WHERE token_hash=? AND used=0 AND expires>?', sha(String(body.token || '')), now());
    if (!r) throw bad('This reset link is invalid or expired');
    await tx(async () => { await run('UPDATE users SET pw=? WHERE id=?', hashPw(body.password), r.user_id); await run('UPDATE resets SET used=1 WHERE token_hash=?', r.token_hash); await run('DELETE FROM sessions WHERE user_id=?', r.user_id); });
    return { ok: true };
  });
  route('GET', '/config', false, async () => ({ reset_email: mailerReady() }));
  route('GET', '/me', true, async ({ user }) => publicUser(user));

  // Startups and products
  route('POST', '/startups', true, async ({ user, body }) => {
    if (user.role !== 'founder') throw forbid('Only founders create startups');
    const pct = num(body.offeredPct, 'offeredPct'); if (pct <= 0 || pct > 100) throw bad('offeredPct must be between 0 and 100');
    const rid = await ins('INSERT INTO startups(founder_id,name,pitch,offered_pct) VALUES(?,?,?,?)', user.id, str(body.name, 'name'), String(body.pitch || '').slice(0, 2000), pct);
    return [201, await one('SELECT * FROM startups WHERE id=?', rid)];
  });
  route('GET', '/startups', true, async () => await q('SELECT s.id,s.founder_id,s.name,s.pitch,s.offered_pct, COALESCE((SELECT SUM(pct) FROM agreements a WHERE a.startup_id=s.id),0) agreed_pct FROM startups s'));
  route('GET', '/startups/:id/cap-table', true, async ({ user, params }) => {
    const s = await one('SELECT * FROM startups WHERE id=?', params.id); if (!s) throw nf('Not found');
    if (s.founder_id !== user.id) throw forbid('Only the founder sees the cap table');
    const rows = await q('SELECT a.pct,a.amount,a.created,u.name investor FROM agreements a JOIN users u ON u.id=a.investor_id WHERE a.startup_id=? ORDER BY a.id', s.id);
    const agreed = rows.reduce((t, r) => t + r.pct, 0);
    return { startup: s.name, offered_pct: s.offered_pct, agreed_pct: agreed, remaining_of_offer_pct: s.offered_pct - agreed, holders: rows, simulation: true };
  });
  const ownStartup = async (user, id) => { const s = await one('SELECT * FROM startups WHERE id=?', id); if (!s) throw nf('Not found'); if (s.founder_id !== user.id) throw forbid('Not your startup'); return s; };
  route('GET', '/startups/:id/products', true, async ({ params }) => (await q('SELECT * FROM products WHERE startup_id=?', params.id)).map(productOut));
  route('POST', '/startups/:id/products', true, async ({ user, params, body }) => {
    await ownStartup(user, params.id);
    const cost = body.cost === undefined || body.cost === null || body.cost === '' ? null : num(body.cost, 'cost');
    const rid = await ins('INSERT INTO products(startup_id,name,price,cost,available,sold) VALUES(?,?,?,?,?,?)', params.id, str(body.name, 'name'), num(body.price, 'price'), cost, Math.trunc(num(body.available ?? 0, 'available')), Math.trunc(num(body.sold ?? 0, 'sold')));
    return [201, productOut(await one('SELECT * FROM products WHERE id=?', rid))];
  });
  // Derived, honestly named. Not an investment return. Null when cost is missing or zero.
  const productOut = (p) => ({ ...p, gross_sales: p.price * p.sold, markup_on_unit_cost_pct: p.cost > 0 ? ((p.price - p.cost) / p.cost) * 100 : null });

  // Interest, chat
  route('POST', '/startups/:id/interest', true, async ({ user, params }) => {
    if (user.role !== 'investor') throw forbid('Only investors send interest requests');
    if (!await one('SELECT 1 x FROM startups WHERE id=?', params.id)) throw nf('Not found');
    if (await one('SELECT 1 x FROM interests WHERE startup_id=? AND investor_id=?', params.id, user.id)) throw conflict('You already sent a request');
    const rid = await ins("INSERT INTO interests(startup_id,investor_id,status) VALUES(?,?,'pending')", params.id, user.id);
    return [201, await one('SELECT * FROM interests WHERE id=?', rid)];
  });
  route('POST', '/interests/:id/respond', true, async ({ user, params, body }) => {
    const i = await interestFor(params.id, user); if (side(i, user) !== 'founder') throw forbid('Only the founder responds');
    if (i.status !== 'pending') throw conflict('Already answered');
    if (!['accepted', 'declined'].includes(body.status)) throw bad('status must be accepted or declined');
    await run('UPDATE interests SET status=? WHERE id=?', body.status, i.id); return { ...i, status: body.status };
  });
  route('GET', '/interests', true, async ({ user }) => await q('SELECT i.*, s.name startup, u.name investor FROM interests i JOIN startups s ON s.id=i.startup_id JOIN users u ON u.id=i.investor_id WHERE i.investor_id=? OR s.founder_id=?', user.id, user.id));
  route('GET', '/interests/:id/messages', true, async ({ user, params }) => { const i = await interestFor(params.id, user); if (i.status !== 'accepted') throw forbid('Chat opens when the founder accepts'); return await q('SELECT * FROM messages WHERE interest_id=? ORDER BY id', i.id); });
  route('POST', '/interests/:id/messages', true, async ({ user, params, body }) => {
    const i = await interestFor(params.id, user); if (i.status !== 'accepted') throw forbid('Chat opens when the founder accepts');
    const rid = await ins('INSERT INTO messages(interest_id,sender_id,body,created) VALUES(?,?,?,?)', i.id, user.id, str(body.body, 'body', 2000), now());
    return [201, await one('SELECT * FROM messages WHERE id=?', rid)];
  });

  // Offers: investor proposes. Founder accepts, declines or counters. Counter needs investor confirm.
  // A share is recorded only when both sides approved the identical terms (the last proposer's terms + the other side's accept).
  const checkTerms = async (i, amount, pct) => {
    if (pct <= 0 || pct > 100) throw bad('pct must be between 0 and 100');
    if (amount <= 0) throw bad('amount must be above 0');
    const remaining = i.offered_pct - await recordedPct(i.startup_id);
    if (pct > remaining + 1e-9) throw conflict(`Only ${remaining}% of the company is still on offer`);
  };
  const openOffer = async (interestId) => await one("SELECT * FROM offers WHERE interest_id=? AND status IN('pending','countered') ORDER BY id DESC", interestId);
  route('POST', '/interests/:id/offers', true, async ({ user, params, body }) => await tx(async () => {
    await expireOffers();
    const i = await interestFor(params.id, user); if (side(i, user) !== 'investor') throw forbid('Only the investor sends the first offer');
    if (i.status !== 'accepted') throw forbid('Chat must be open first');
    if (await openOffer(i.id)) throw conflict('There is already an open offer');
    const amount = num(body.amount, 'amount'), pct = num(body.pct, 'pct'); await checkTerms(i, amount, pct);
    const rid = await ins("INSERT INTO offers(interest_id,amount,pct,status,proposed_by,expires,created) VALUES(?,?,?,'pending','investor',?,?)", i.id, amount, pct, now() + OFFER_TTL, now());
    return [201, await one('SELECT * FROM offers WHERE id=?', rid)];
  }));
  route('GET', '/interests/:id/offers', true, async ({ user, params }) => { await expireOffers(); const i = await interestFor(params.id, user); return await q('SELECT * FROM offers WHERE interest_id=? ORDER BY id', i.id); });
  route('POST', '/offers/:id/respond', true, async ({ user, params, body }) => await tx(async () => {
    await expireOffers();
    const o = await one('SELECT * FROM offers WHERE id=?', params.id); if (!o) throw nf('Not found');
    const i = await interestFor(o.interest_id, user); const me = side(i, user);
    if (o.status !== 'pending' && o.status !== 'countered') throw conflict(`Offer is ${o.status}`);
    if (o.status === 'countered') throw conflict('This offer was replaced by a counter');
    if (me === o.proposed_by) { // proposer may only withdraw
      if (body.action !== 'withdraw') throw forbid('Wait for the other side to respond, or withdraw');
      await run("UPDATE offers SET status='withdrawn' WHERE id=?", o.id); return await one('SELECT * FROM offers WHERE id=?', o.id);
    }
    if (body.action === 'decline') { await run("UPDATE offers SET status='declined' WHERE id=?", o.id); return await one('SELECT * FROM offers WHERE id=?', o.id); }
    if (body.action === 'counter') {
      const amount = num(body.amount, 'amount'), pct = num(body.pct, 'pct'); await checkTerms(i, amount, pct);
      await run("UPDATE offers SET status='countered' WHERE id=?", o.id);
      const rid = await ins("INSERT INTO offers(interest_id,amount,pct,status,proposed_by,parent_id,expires,created) VALUES(?,?,?,'pending',?,?,?,?)", i.id, amount, pct, me, o.id, now() + OFFER_TTL, now());
      return [201, await one('SELECT * FROM offers WHERE id=?', rid)];
    }
    if (body.action === 'accept') {
      // Identical terms: the accepter must restate what they see; mismatch (a stale view) is rejected.
      if (num(body.amount, 'amount') !== o.amount || num(body.pct, 'pct') !== o.pct) throw conflict('Terms changed. Review the current offer and accept again');
      await checkTerms(i, o.amount, o.pct); // re-check cap at the moment of recording
      await run("UPDATE offers SET status='recorded' WHERE id=?", o.id);
      await run('INSERT INTO agreements(offer_id,startup_id,investor_id,amount,pct,created) VALUES(?,?,?,?,?,?)', o.id, i.startup_id, i.investor_id, o.amount, o.pct, now());
      return { ...(await one('SELECT * FROM offers WHERE id=?', o.id)), simulation: true };
    }
    throw bad('action must be accept, decline, counter or withdraw');
  }));
  route('GET', '/portfolio', true, async ({ user }) => {
    if (user.role !== 'investor') throw forbid('Investors only');
    const rows = await q('SELECT a.pct,a.amount,a.created,s.name startup FROM agreements a JOIN startups s ON s.id=a.startup_id WHERE a.investor_id=?', user.id);
    return { holdings: rows, total_amount: rows.reduce((t, r) => t + r.amount, 0), simulation: true }; // no valuation, no returns
  });

  // Finances ledger and invoices, per user. Analytics are sums of ledger entries only.
  route('GET', '/ledger', true, async ({ user }) => await q('SELECT * FROM ledger WHERE user_id=? ORDER BY id DESC', user.id));
  route('POST', '/ledger', true, async ({ user, body }) => {
    if (!['income', 'expense'].includes(body.kind)) throw bad('kind must be income or expense');
    const rid = await ins('INSERT INTO ledger(user_id,kind,note,amount,d) VALUES(?,?,?,?,?)', user.id, body.kind, str(body.note, 'note'), num(body.amount, 'amount'), str(body.d, 'd', 10));
    return [201, await one('SELECT * FROM ledger WHERE id=?', rid)];
  });
  route('GET', '/ledger/summary', true, async ({ user }) => { const r = await one("SELECT COALESCE(SUM(CASE WHEN kind='income' THEN amount END),0) income, COALESCE(SUM(CASE WHEN kind='expense' THEN amount END),0) expense FROM ledger WHERE user_id=?", user.id); return { ...r, net: r.income - r.expense }; });
  route('GET', '/invoices', true, async ({ user }) => await q('SELECT * FROM invoices WHERE user_id=? ORDER BY id DESC', user.id));
  route('POST', '/invoices', true, async ({ user, body }) => {
    const rid = await ins('INSERT INTO invoices(user_id,customer,item,amount,d) VALUES(?,?,?,?,?)', user.id, str(body.customer, 'customer'), str(body.item, 'item'), num(body.amount, 'amount'), str(body.d, 'd', 10));
    return [201, await one('SELECT * FROM invoices WHERE id=?', rid)];
  });

  // Mailer: Brevo HTTP API when BREVO_API_KEY and MAIL_FROM are set (key never logged). Without one, reset by email is disabled and says so.
  // DEV_CONSOLE_MAILER=1 prints the link to the server log for local testing only.
  const publicUrl = (process.env.PUBLIC_URL || 'http://127.0.0.1:8787').replace(/\/$/, '');
  let customMailer = null;
  const mailerReady = () => !!(customMailer || (process.env.BREVO_API_KEY && process.env.MAIL_FROM) || process.env.DEV_CONSOLE_MAILER);
  const mailer = (email, token) => {
    if (customMailer) return customMailer(email, token);
    const link = `${publicUrl}/app/?reset=${token}`;
    if (!process.env.BREVO_API_KEY || !process.env.MAIL_FROM) { console.log(`[dev mailer] reset link for ${email}: ${link}`); return; }
    fetch('https://api.brevo.com/v3/smtp/email', { method: 'POST', headers: { 'api-key': process.env.BREVO_API_KEY, 'content-type': 'application/json', accept: 'application/json' },
      body: JSON.stringify({ sender: { email: process.env.MAIL_FROM, name: 'Investa' }, to: [{ email }], subject: 'Reset your Investa password / إعادة تعيين كلمة المرور', htmlContent: `<p>Use this link within 30 minutes to choose a new password:</p><p><a href="${link}">${link}</a></p><p>استخدم هذا الرابط خلال ٣٠ دقيقة لاختيار كلمة مرور جديدة.</p><p>If you did not ask for this, ignore this email.</p>` }) })
      .then((r) => { if (!r.ok) console.error('mailer failed with status', r.status); }).catch(() => console.error('mailer request failed'));
  };
  // Per-IP rate limit stored in the database: 10 auth calls and 240 other calls per minute. Behind a proxy set TRUST_PROXY=1 to use x-forwarded-for.
  const limited = async (req, isAuth) => {
    const ip = (process.env.TRUST_PROXY ? String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() : '') || req.socket.remoteAddress || '?';
    const w = Math.floor(Date.now() / 60000);
    const r = await one('INSERT INTO rate_hits(k,w,n) VALUES(?,?,1) ON CONFLICT(k) DO UPDATE SET n = CASE WHEN rate_hits.w = excluded.w THEN rate_hits.n + 1 ELSE 1 END, w = excluded.w RETURNING n', ip + (isAuth ? '|a' : '|g'), w);
    return r.n > (isAuth ? 10 : 240);
  };
  async function handle(req) {
    const url = new URL(req.url, 'http://x'); const path = url.pathname.replace(/^\/api(?=\/|$)/, '').replace(/\/+$/, '') || '/';
    for (const r of routes) {
      if (r.method !== req.method) continue; const m = r.re.exec(path); if (!m) continue;
      let user = null, token = null;
      if (r.auth) {
        token = (req.headers.authorization || '').replace(/^Bearer /, '');
        const s = token && await one('SELECT * FROM sessions WHERE token_hash=? AND expires>?', sha(token), now());
        if (!s) throw new HttpError(401, 'Sign in required');
        user = await one('SELECT * FROM users WHERE id=?', s.user_id);
      }
      let body = {};
      if (req.method === 'POST') { const chunks = []; let n = 0; for await (const c of req) { n += c.length; if (n > 100000) throw bad('Body too large'); chunks.push(c); } try { body = JSON.parse(Buffer.concat(chunks).toString() || '{}'); } catch { throw bad('Invalid JSON'); } }
      const out = await r.fn({ user, token, params: m.groups || {}, body, mailer: (...a) => mailer(...a) });
      return Array.isArray(out) && typeof out[0] === 'number' ? out : [200, out];
    }
    throw nf('Not found');
  }
  const handler = async (req, res) => {
    await ready;
    // CORS: only the configured local client origin(s); no wildcard.
    const origins = (process.env.CLIENT_ORIGIN || 'http://127.0.0.1:5173,http://localhost:5173').split(',');
    const cors = origins.includes(req.headers.origin) ? { 'access-control-allow-origin': req.headers.origin, 'vary': 'Origin', 'access-control-allow-headers': 'content-type, authorization', 'access-control-allow-methods': 'GET, POST, OPTIONS', 'access-control-allow-private-network': 'true' } : {};
    const send = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json', 'x-content-type-options': 'nosniff', 'x-frame-options': 'DENY', ...(publicUrl.startsWith('https') ? { 'strict-transport-security': 'max-age=15552000' } : {}), 'cache-control': 'no-store', 'referrer-policy': 'no-referrer', ...cors }); res.end(JSON.stringify(obj)); };
    // Optional static hosting of the built local client at /app/ (set STATIC_DIR). Same origin as the API, so no CORS.
    if (process.env.STATIC_DIR && req.method === 'GET' && (req.url === '/app' || req.url.startsWith('/app/'))) {
      const rel = decodeURIComponent(new URL(req.url, 'http://x').pathname.replace(/^\/app\/?/, '')) || 'index.html';
      const file = path.resolve(process.env.STATIC_DIR, rel);
      if (!file.startsWith(path.resolve(process.env.STATIC_DIR) + path.sep) || !fs.existsSync(file)) { res.writeHead(404); return res.end('Not found'); }
      const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png' };
      res.writeHead(200, { 'content-type': types[path.extname(file)] || 'application/octet-stream', 'x-content-type-options': 'nosniff' }); return res.end(fs.readFileSync(file));
    }
    if (req.method === 'OPTIONS') { res.writeHead(204, { ...cors }); return res.end(); }
    try { if (!process.env.NO_RATE_LIMIT && await limited(req, /^(\/api)?\/auth\//.test(req.url))) throw new HttpError(429, 'Too many requests, wait a minute'); const [c, o] = await (serial ? withLock(() => handle(req)) : handle(req)); send(c, o); }
    catch (e) { if (/cap_exceeded/.test(String(e && e.message))) send(409, { error: 'Share cap exceeded', code: 'share_cap_exceeded', params: [] }); else if (e instanceof HttpError) send(e.code, { error: e.message, code: e.message.toLowerCase().replace(/[\d.]+/g, 'n').replace(/[^a-z]+/g, '_').replace(/^_|_$/g, '') , params: (e.message.match(/\d+(?:\.\d+)?/g) || [])}); else { console.error(e); send(500, { error: 'Server error' }); } }
  };
  const server = http.createServer(handler);
  Object.assign(app, { server, handler, ready, setMailer: (f) => { customMailer = f; }, expireOffers });
  return app;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { server } = createApp(process.env.DATABASE_URL || process.env.DB || './investa.db');
  server.listen(Number(process.env.PORT || 8787), process.env.HOST || '127.0.0.1', () => console.log(`Investa API on ${process.env.HOST || '127.0.0.1'}:${process.env.PORT || 8787}`));
}
