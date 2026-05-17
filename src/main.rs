//! `stryke-redis-helper` — Redis / Valkey bridge binary.
//!
//! Single-shot subprocess: every invocation opens a fresh Redis
//! connection, runs one command, prints JSON, exits.

use std::io::{self, BufWriter, Write};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use redis::{Client, Commands, Connection, ConnectionInfo, IntoConnectionInfo};
use serde_json::{json, Value};

#[derive(Parser, Debug)]
#[command(
    name = "stryke-redis-helper",
    version,
    about = "Redis / Valkey client for the stryke `redis` package"
)]
struct Cli {
    #[command(flatten)]
    conn: Conn,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Debug, Clone)]
struct Conn {
    /// `redis://[user:pass@]host[:port]/[db]` (or `rediss://…` for TLS).
    #[arg(long, short = 'u', env = "REDIS_URL", global = true)]
    url: Option<String>,

    #[arg(long, short = 'H', env = "REDIS_HOST", global = true)]
    host: Option<String>,

    #[arg(long, short = 'P', env = "REDIS_PORT", global = true)]
    port: Option<u16>,

    #[arg(
        long,
        short = 'p',
        env = "REDIS_PASSWORD",
        global = true,
        hide_env_values = true
    )]
    password: Option<String>,

    #[arg(long, env = "REDIS_USERNAME", global = true)]
    username: Option<String>,

    #[arg(long, short = 'D', env = "REDIS_DB", global = true)]
    db: Option<i64>,

    /// Force TLS even when URL is `redis://`.
    #[arg(long, global = true)]
    tls: bool,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    // ---- KV ----
    Get { key: String },
    Set {
        key: String,
        value: String,
        #[arg(long, value_name = "SECONDS")]
        ex: Option<u64>,
        #[arg(long, value_name = "MS")]
        px: Option<u64>,
        /// Set only if key doesn't exist.
        #[arg(long)]
        nx: bool,
        /// Set only if key already exists.
        #[arg(long)]
        xx: bool,
    },
    Del { keys: Vec<String> },
    Exists { keys: Vec<String> },
    Expire { key: String, seconds: i64 },
    Ttl { key: String },
    Type { key: String },
    Incr {
        key: String,
        #[arg(long, default_value_t = 1)]
        by: i64,
    },
    Decr {
        key: String,
        #[arg(long, default_value_t = 1)]
        by: i64,
    },
    Mget { keys: Vec<String> },
    Mset {
        /// `key value key value …`
        kv: Vec<String>,
    },
    /// Glob-style `KEYS PATTERN`. Use sparingly on prod databases; prefer `scan`.
    Keys { pattern: String },
    /// Streaming SCAN — emits NDJSON keys without blocking the server.
    Scan {
        #[arg(long, default_value = "*")]
        match_: String,
        #[arg(long, default_value_t = 100)]
        count: usize,
        #[arg(long)]
        limit: Option<usize>,
    },

    // ---- Lists ----
    Lpush { key: String, values: Vec<String> },
    Rpush { key: String, values: Vec<String> },
    Lpop {
        key: String,
        #[arg(long)]
        count: Option<usize>,
    },
    Rpop {
        key: String,
        #[arg(long)]
        count: Option<usize>,
    },
    Lrange {
        key: String,
        #[arg(default_value_t = 0, allow_hyphen_values = true)]
        start: isize,
        #[arg(default_value_t = -1, allow_hyphen_values = true)]
        stop: isize,
    },
    Llen { key: String },

    // ---- Sets ----
    Sadd { key: String, members: Vec<String> },
    Srem { key: String, members: Vec<String> },
    Smembers { key: String },
    Sismember { key: String, member: String },
    Scard { key: String },

    // ---- Hashes ----
    Hset { key: String, field: String, value: String },
    Hget { key: String, field: String },
    Hdel { key: String, fields: Vec<String> },
    Hgetall { key: String },
    Hkeys { key: String },
    Hvals { key: String },
    Hmget { key: String, fields: Vec<String> },
    Hmset {
        key: String,
        /// `field value field value …`
        fv: Vec<String>,
    },

    // ---- Sorted sets ----
    Zadd {
        key: String,
        /// `score member score member …`
        sm: Vec<String>,
    },
    Zrange {
        key: String,
        #[arg(default_value_t = 0, allow_hyphen_values = true)]
        start: isize,
        #[arg(default_value_t = -1, allow_hyphen_values = true)]
        stop: isize,
        #[arg(long)]
        withscores: bool,
        #[arg(long)]
        rev: bool,
    },
    Zrem { key: String, members: Vec<String> },
    Zcard { key: String },
    Zscore { key: String, member: String },

    // ---- Pub/sub publish (sub is streaming, out of scope for v1) ----
    Publish { channel: String, message: String },

    // ---- Server ----
    Ping,
    Info {
        #[arg(default_value = "")]
        section: String,
    },
    Dbsize,
    Flushdb {
        #[arg(long)]
        confirm: bool,
    },

    /// Run an arbitrary command. `args` is the raw arg vector (first item
    /// is the command name). Use sparingly — no type validation.
    Raw { args: Vec<String> },
}

fn main() {
    if let Err(e) = run() {
        eprintln!("stryke-redis-helper: {e:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let info = build_conn_info(&cli.conn)?;
    let client = Client::open(info).context("opening Redis client")?;
    let mut con = client.get_connection().context("connecting to Redis")?;
    match cli.cmd {
        // KV
        Cmd::Get { key } => cmd_get(&mut con, &key),
        Cmd::Set { key, value, ex, px, nx, xx } => cmd_set(&mut con, &key, &value, ex, px, nx, xx),
        Cmd::Del { keys } => emit_int(redis_del(&mut con, &keys)?),
        Cmd::Exists { keys } => emit_int(redis_exists(&mut con, &keys)?),
        Cmd::Expire { key, seconds } => emit_bool(redis_expire(&mut con, &key, seconds)?),
        Cmd::Ttl { key } => emit_int(redis_ttl(&mut con, &key)?),
        Cmd::Type { key } => emit_json(&json!({ "type": redis_type(&mut con, &key)? })),
        Cmd::Incr { key, by } => emit_int(redis_incr(&mut con, &key, by)?),
        Cmd::Decr { key, by } => emit_int(redis_incr(&mut con, &key, -by)?),
        Cmd::Mget { keys } => cmd_mget(&mut con, &keys),
        Cmd::Mset { kv } => cmd_mset(&mut con, &kv),
        Cmd::Keys { pattern } => cmd_keys(&mut con, &pattern),
        Cmd::Scan { match_, count, limit } => cmd_scan(&mut con, &match_, count, limit),

        // Lists
        Cmd::Lpush { key, values } => emit_int(redis_lpush(&mut con, &key, &values)?),
        Cmd::Rpush { key, values } => emit_int(redis_rpush(&mut con, &key, &values)?),
        Cmd::Lpop { key, count } => cmd_lpop(&mut con, &key, count),
        Cmd::Rpop { key, count } => cmd_rpop(&mut con, &key, count),
        Cmd::Lrange { key, start, stop } => cmd_lrange(&mut con, &key, start, stop),
        Cmd::Llen { key } => emit_int(redis_llen(&mut con, &key)?),

        // Sets
        Cmd::Sadd { key, members } => emit_int(redis_sadd(&mut con, &key, &members)?),
        Cmd::Srem { key, members } => emit_int(redis_srem(&mut con, &key, &members)?),
        Cmd::Smembers { key } => cmd_smembers(&mut con, &key),
        Cmd::Sismember { key, member } => emit_bool(redis_sismember(&mut con, &key, &member)?),
        Cmd::Scard { key } => emit_int(redis_scard(&mut con, &key)?),

        // Hashes
        Cmd::Hset { key, field, value } => emit_int(redis_hset(&mut con, &key, &field, &value)?),
        Cmd::Hget { key, field } => cmd_hget(&mut con, &key, &field),
        Cmd::Hdel { key, fields } => emit_int(redis_hdel(&mut con, &key, &fields)?),
        Cmd::Hgetall { key } => cmd_hgetall(&mut con, &key),
        Cmd::Hkeys { key } => cmd_hkeys(&mut con, &key),
        Cmd::Hvals { key } => cmd_hvals(&mut con, &key),
        Cmd::Hmget { key, fields } => cmd_hmget(&mut con, &key, &fields),
        Cmd::Hmset { key, fv } => cmd_hmset(&mut con, &key, &fv),

        // Sorted sets
        Cmd::Zadd { key, sm } => cmd_zadd(&mut con, &key, &sm),
        Cmd::Zrange { key, start, stop, withscores, rev } => {
            cmd_zrange(&mut con, &key, start, stop, withscores, rev)
        }
        Cmd::Zrem { key, members } => emit_int(redis_zrem(&mut con, &key, &members)?),
        Cmd::Zcard { key } => emit_int(redis_zcard(&mut con, &key)?),
        Cmd::Zscore { key, member } => cmd_zscore(&mut con, &key, &member),

        // Pub/sub
        Cmd::Publish { channel, message } => emit_int(redis_publish(&mut con, &channel, &message)?),

        // Server
        Cmd::Ping => cmd_ping(&mut con),
        Cmd::Info { section } => cmd_info(&mut con, &section),
        Cmd::Dbsize => emit_int(redis_dbsize(&mut con)?),
        Cmd::Flushdb { confirm } => cmd_flushdb(&mut con, confirm),

        Cmd::Raw { args } => cmd_raw(&mut con, &args),
    }
}

/* ------------------------------------------------------------------------- */
/* connection                                                                */
/* ------------------------------------------------------------------------- */

fn build_conn_info(c: &Conn) -> Result<ConnectionInfo> {
    // Always build via a URL so flag-level overrides compose cleanly with
    // env defaults. The redis crate's URL parser handles auth, db, TLS.
    let info = if let Some(url) = &c.url {
        url.clone().into_connection_info().context("parsing --url")?
    } else {
        let scheme = if c.tls { "rediss" } else { "redis" };
        let host = c.host.clone().unwrap_or_else(|| "127.0.0.1".to_string());
        let port = c.port.unwrap_or(6379);
        let auth = match (&c.username, &c.password) {
            (Some(u), Some(p)) => format!("{u}:{p}@"),
            (None, Some(p)) => format!(":{p}@"),
            _ => String::new(),
        };
        let db = c.db.map(|d| format!("/{d}")).unwrap_or_default();
        format!("{scheme}://{auth}{host}:{port}{db}")
            .into_connection_info()
            .context("building URL")?
    };
    Ok(info)
}

/* ------------------------------------------------------------------------- */
/* helpers                                                                   */
/* ------------------------------------------------------------------------- */

fn emit_json<T: serde::Serialize>(v: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

fn emit_ndjson<T: serde::Serialize, W: Write>(w: &mut W, v: &T) -> Result<()> {
    serde_json::to_writer(&mut *w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

fn emit_int(n: i64) -> Result<()> {
    emit_json(&json!({ "value": n }))
}

fn emit_bool(b: bool) -> Result<()> {
    emit_json(&json!({ "value": b }))
}

fn bytes_to_jsonish(b: Vec<u8>) -> Value {
    match String::from_utf8(b) {
        Ok(s) => Value::String(s),
        Err(e) => {
            use base64::engine::general_purpose::STANDARD as B64;
            use base64::Engine as _;
            let mut out = String::from("base64:");
            out.push_str(&B64.encode(e.into_bytes()));
            Value::String(out)
        }
    }
}

/* ------------------------------------------------------------------------- */
/* KV                                                                        */
/* ------------------------------------------------------------------------- */

fn cmd_get(con: &mut Connection, key: &str) -> Result<()> {
    let raw: Option<Vec<u8>> = con.get(key)?;
    match raw {
        Some(b) => emit_json(&json!({ "value": bytes_to_jsonish(b) })),
        None => emit_json(&json!({ "value": Value::Null })),
    }
}

fn cmd_set(
    con: &mut Connection,
    key: &str,
    value: &str,
    ex: Option<u64>,
    px: Option<u64>,
    nx: bool,
    xx: bool,
) -> Result<()> {
    let mut cmd = redis::cmd("SET");
    cmd.arg(key).arg(value);
    if let Some(s) = ex {
        cmd.arg("EX").arg(s);
    }
    if let Some(ms) = px {
        cmd.arg("PX").arg(ms);
    }
    if nx {
        cmd.arg("NX");
    }
    if xx {
        cmd.arg("XX");
    }
    let resp: Option<String> = cmd.query(con)?;
    emit_json(&json!({ "ok": resp.as_deref() == Some("OK") }))
}

fn redis_del(con: &mut Connection, keys: &[String]) -> Result<i64> {
    Ok(con.del(keys)?)
}

fn redis_exists(con: &mut Connection, keys: &[String]) -> Result<i64> {
    Ok(con.exists(keys)?)
}

fn redis_expire(con: &mut Connection, key: &str, seconds: i64) -> Result<bool> {
    Ok(con.expire(key, seconds)?)
}

fn redis_ttl(con: &mut Connection, key: &str) -> Result<i64> {
    Ok(con.ttl(key)?)
}

fn redis_type(con: &mut Connection, key: &str) -> Result<String> {
    let t: String = redis::cmd("TYPE").arg(key).query(con)?;
    Ok(t)
}

fn redis_incr(con: &mut Connection, key: &str, by: i64) -> Result<i64> {
    Ok(con.incr(key, by)?)
}

fn cmd_mget(con: &mut Connection, keys: &[String]) -> Result<()> {
    if keys.is_empty() {
        return emit_json(&json!({ "values": [] }));
    }
    let raw: Vec<Option<Vec<u8>>> = con.mget(keys)?;
    let vals: Vec<Value> = raw
        .into_iter()
        .map(|o| match o {
            Some(b) => bytes_to_jsonish(b),
            None => Value::Null,
        })
        .collect();
    emit_json(&json!({ "values": vals }))
}

fn cmd_mset(con: &mut Connection, kv: &[String]) -> Result<()> {
    if kv.len() % 2 != 0 {
        bail!("mset takes pairs of `key value …` — got {} args", kv.len());
    }
    let pairs: Vec<(&str, &str)> = kv
        .chunks(2)
        .map(|c| (c[0].as_str(), c[1].as_str()))
        .collect();
    let _: () = con.mset(&pairs)?;
    emit_json(&json!({ "ok": true, "set": pairs.len() }))
}

fn cmd_keys(con: &mut Connection, pattern: &str) -> Result<()> {
    let keys: Vec<String> = con.keys(pattern)?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for k in keys {
        emit_ndjson(&mut out, &json!({ "key": k }))?;
    }
    Ok(())
}

fn cmd_scan(con: &mut Connection, pat: &str, batch: usize, limit: Option<usize>) -> Result<()> {
    let iter: redis::Iter<'_, String> = redis::cmd("SCAN")
        .cursor_arg(0)
        .arg("MATCH")
        .arg(pat)
        .arg("COUNT")
        .arg(batch)
        .clone()
        .iter(con)?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let mut emitted = 0usize;
    for item in iter {
        let k = item?;
        emit_ndjson(&mut out, &json!({ "key": k }))?;
        emitted += 1;
        if let Some(l) = limit {
            if emitted >= l {
                break;
            }
        }
    }
    Ok(())
}

/* ------------------------------------------------------------------------- */
/* Lists                                                                     */
/* ------------------------------------------------------------------------- */

fn redis_lpush(con: &mut Connection, key: &str, values: &[String]) -> Result<i64> {
    Ok(con.lpush(key, values)?)
}

fn redis_rpush(con: &mut Connection, key: &str, values: &[String]) -> Result<i64> {
    Ok(con.rpush(key, values)?)
}

fn cmd_lpop(con: &mut Connection, key: &str, count: Option<usize>) -> Result<()> {
    pop_helper(con, "LPOP", key, count)
}

fn cmd_rpop(con: &mut Connection, key: &str, count: Option<usize>) -> Result<()> {
    pop_helper(con, "RPOP", key, count)
}

fn pop_helper(con: &mut Connection, cmd_name: &str, key: &str, count: Option<usize>) -> Result<()> {
    let mut cmd = redis::cmd(cmd_name);
    cmd.arg(key);
    match count {
        None => {
            let v: Option<Vec<u8>> = cmd.query(con)?;
            emit_json(&json!({
                "value": v.map(bytes_to_jsonish).unwrap_or(Value::Null),
            }))
        }
        Some(n) => {
            cmd.arg(n);
            let v: Option<Vec<Vec<u8>>> = cmd.query(con)?;
            let arr: Vec<Value> = v
                .unwrap_or_default()
                .into_iter()
                .map(bytes_to_jsonish)
                .collect();
            emit_json(&json!({ "values": arr }))
        }
    }
}

fn cmd_lrange(con: &mut Connection, key: &str, start: isize, stop: isize) -> Result<()> {
    let raw: Vec<Vec<u8>> = con.lrange(key, start, stop)?;
    let arr: Vec<Value> = raw.into_iter().map(bytes_to_jsonish).collect();
    emit_json(&json!({ "values": arr }))
}

fn redis_llen(con: &mut Connection, key: &str) -> Result<i64> {
    Ok(con.llen(key)?)
}

/* ------------------------------------------------------------------------- */
/* Sets                                                                      */
/* ------------------------------------------------------------------------- */

fn redis_sadd(con: &mut Connection, key: &str, m: &[String]) -> Result<i64> {
    Ok(con.sadd(key, m)?)
}
fn redis_srem(con: &mut Connection, key: &str, m: &[String]) -> Result<i64> {
    Ok(con.srem(key, m)?)
}

fn cmd_smembers(con: &mut Connection, key: &str) -> Result<()> {
    let raw: Vec<Vec<u8>> = con.smembers(key)?;
    let arr: Vec<Value> = raw.into_iter().map(bytes_to_jsonish).collect();
    emit_json(&json!({ "members": arr }))
}

fn redis_sismember(con: &mut Connection, key: &str, m: &str) -> Result<bool> {
    Ok(con.sismember(key, m)?)
}

fn redis_scard(con: &mut Connection, key: &str) -> Result<i64> {
    Ok(con.scard(key)?)
}

/* ------------------------------------------------------------------------- */
/* Hashes                                                                    */
/* ------------------------------------------------------------------------- */

fn redis_hset(con: &mut Connection, key: &str, field: &str, value: &str) -> Result<i64> {
    Ok(con.hset(key, field, value)?)
}

fn cmd_hget(con: &mut Connection, key: &str, field: &str) -> Result<()> {
    let raw: Option<Vec<u8>> = con.hget(key, field)?;
    emit_json(&json!({
        "value": raw.map(bytes_to_jsonish).unwrap_or(Value::Null),
    }))
}

fn redis_hdel(con: &mut Connection, key: &str, fields: &[String]) -> Result<i64> {
    Ok(con.hdel(key, fields)?)
}

fn cmd_hgetall(con: &mut Connection, key: &str) -> Result<()> {
    let raw: Vec<(String, Vec<u8>)> = con.hgetall(key)?;
    let mut obj = serde_json::Map::new();
    for (k, v) in raw {
        obj.insert(k, bytes_to_jsonish(v));
    }
    emit_json(&Value::Object(obj))
}

fn cmd_hkeys(con: &mut Connection, key: &str) -> Result<()> {
    let v: Vec<String> = con.hkeys(key)?;
    emit_json(&json!({ "keys": v }))
}

fn cmd_hvals(con: &mut Connection, key: &str) -> Result<()> {
    let raw: Vec<Vec<u8>> = con.hvals(key)?;
    let arr: Vec<Value> = raw.into_iter().map(bytes_to_jsonish).collect();
    emit_json(&json!({ "values": arr }))
}

fn cmd_hmget(con: &mut Connection, key: &str, fields: &[String]) -> Result<()> {
    if fields.is_empty() {
        return emit_json(&json!({ "values": [] }));
    }
    let mut cmd = redis::cmd("HMGET");
    cmd.arg(key);
    for f in fields {
        cmd.arg(f);
    }
    let raw: Vec<Option<Vec<u8>>> = cmd.query(con)?;
    let arr: Vec<Value> = raw
        .into_iter()
        .map(|o| o.map(bytes_to_jsonish).unwrap_or(Value::Null))
        .collect();
    emit_json(&json!({ "values": arr }))
}

fn cmd_hmset(con: &mut Connection, key: &str, fv: &[String]) -> Result<()> {
    if fv.len() % 2 != 0 {
        bail!("hmset takes pairs of `field value …` — got {} args", fv.len());
    }
    let pairs: Vec<(&str, &str)> = fv.chunks(2).map(|c| (c[0].as_str(), c[1].as_str())).collect();
    let _: () = con.hset_multiple(key, &pairs)?;
    emit_json(&json!({ "ok": true, "set": pairs.len() }))
}

/* ------------------------------------------------------------------------- */
/* Sorted sets                                                               */
/* ------------------------------------------------------------------------- */

fn cmd_zadd(con: &mut Connection, key: &str, sm: &[String]) -> Result<()> {
    if sm.len() % 2 != 0 {
        bail!("zadd takes pairs of `score member …` — got {} args", sm.len());
    }
    let mut cmd = redis::cmd("ZADD");
    cmd.arg(key);
    for pair in sm.chunks(2) {
        cmd.arg(&pair[0]).arg(&pair[1]);
    }
    let n: i64 = cmd.query(con)?;
    emit_int(n)
}

fn cmd_zrange(
    con: &mut Connection,
    key: &str,
    start: isize,
    stop: isize,
    withscores: bool,
    rev: bool,
) -> Result<()> {
    let mut cmd = redis::cmd(if rev { "ZREVRANGE" } else { "ZRANGE" });
    cmd.arg(key).arg(start).arg(stop);
    if withscores {
        cmd.arg("WITHSCORES");
        let raw: Vec<(Vec<u8>, f64)> = cmd.query(con)?;
        let arr: Vec<Value> = raw
            .into_iter()
            .map(|(m, s)| {
                json!({ "member": bytes_to_jsonish(m), "score": s })
            })
            .collect();
        emit_json(&json!({ "values": arr }))
    } else {
        let raw: Vec<Vec<u8>> = cmd.query(con)?;
        let arr: Vec<Value> = raw.into_iter().map(bytes_to_jsonish).collect();
        emit_json(&json!({ "values": arr }))
    }
}

fn redis_zrem(con: &mut Connection, key: &str, m: &[String]) -> Result<i64> {
    Ok(con.zrem(key, m)?)
}

fn redis_zcard(con: &mut Connection, key: &str) -> Result<i64> {
    Ok(con.zcard(key)?)
}

fn cmd_zscore(con: &mut Connection, key: &str, member: &str) -> Result<()> {
    let s: Option<f64> = con.zscore(key, member)?;
    emit_json(&json!({ "score": s }))
}

/* ------------------------------------------------------------------------- */
/* Pub/sub                                                                   */
/* ------------------------------------------------------------------------- */

fn redis_publish(con: &mut Connection, channel: &str, message: &str) -> Result<i64> {
    Ok(con.publish(channel, message)?)
}

/* ------------------------------------------------------------------------- */
/* Server                                                                    */
/* ------------------------------------------------------------------------- */

fn cmd_ping(con: &mut Connection) -> Result<()> {
    let r: String = redis::cmd("PING").query(con)?;
    emit_json(&json!({ "pong": r }))
}

fn cmd_info(con: &mut Connection, section: &str) -> Result<()> {
    let raw: String = if section.is_empty() {
        redis::cmd("INFO").query(con)?
    } else {
        redis::cmd("INFO").arg(section).query(con)?
    };
    // Convert key:value lines to a JSON object grouped by section.
    let mut groups: serde_json::Map<String, Value> = serde_json::Map::new();
    let mut cur_section = "default".to_string();
    let mut cur_obj = serde_json::Map::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(sec) = line.strip_prefix("# ") {
            if !cur_obj.is_empty() {
                groups.insert(std::mem::take(&mut cur_section), Value::Object(std::mem::take(&mut cur_obj)));
            }
            cur_section = sec.to_string();
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            cur_obj.insert(k.to_string(), Value::String(v.to_string()));
        }
    }
    if !cur_obj.is_empty() {
        groups.insert(cur_section, Value::Object(cur_obj));
    }
    emit_json(&Value::Object(groups))
}

fn redis_dbsize(con: &mut Connection) -> Result<i64> {
    let n: i64 = redis::cmd("DBSIZE").query(con)?;
    Ok(n)
}

fn cmd_flushdb(con: &mut Connection, confirm: bool) -> Result<()> {
    if !confirm {
        return Err(anyhow!(
            "flushdb requires --confirm to prevent accidental data loss"
        ));
    }
    let _: () = redis::cmd("FLUSHDB").query(con)?;
    emit_json(&json!({ "ok": true }))
}

fn cmd_raw(con: &mut Connection, args: &[String]) -> Result<()> {
    if args.is_empty() {
        bail!("raw requires at least one argument");
    }
    let mut cmd = redis::cmd(&args[0]);
    for a in &args[1..] {
        cmd.arg(a);
    }
    let v: redis::Value = cmd.query(con)?;
    emit_json(&redis_value_to_json(&v))
}

fn redis_value_to_json(v: &redis::Value) -> Value {
    use redis::Value as R;
    match v {
        R::Nil => Value::Null,
        R::Int(i) => json!(i),
        R::SimpleString(s) => Value::String(s.clone()),
        R::Okay => Value::String("OK".into()),
        R::BulkString(b) => bytes_to_jsonish(b.clone()),
        R::Array(arr) => Value::Array(arr.iter().map(redis_value_to_json).collect()),
        R::ServerError(e) => Value::String(format!("error: {:?}", e)),
        R::Boolean(b) => json!(b),
        R::Double(d) => json!(d),
        R::BigNumber(n) => Value::String(n.to_string()),
        R::Map(pairs) => {
            let mut obj = serde_json::Map::new();
            for (k, val) in pairs {
                let key = match k {
                    R::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                    R::SimpleString(s) => s.clone(),
                    other => format!("{:?}", other),
                };
                obj.insert(key, redis_value_to_json(val));
            }
            Value::Object(obj)
        }
        R::Set(arr) => Value::Array(arr.iter().map(redis_value_to_json).collect()),
        R::Attribute { data, attributes: _ } => redis_value_to_json(data),
        R::Push { kind: _, data } => Value::Array(data.iter().map(redis_value_to_json).collect()),
        R::VerbatimString { format: _, text } => Value::String(text.clone()),
        _ => Value::String(format!("{:?}", v)),
    }
}
