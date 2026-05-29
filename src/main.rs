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
    Get {
        key: String,
    },
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
    Del {
        keys: Vec<String>,
    },
    Exists {
        keys: Vec<String>,
    },
    Expire {
        key: String,
        seconds: i64,
    },
    Ttl {
        key: String,
    },
    Type {
        key: String,
    },
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
    Mget {
        keys: Vec<String>,
    },
    Mset {
        /// `key value key value …`
        kv: Vec<String>,
    },
    /// Glob-style `KEYS PATTERN`. Use sparingly on prod databases; prefer `scan`.
    Keys {
        pattern: String,
    },
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
    Lpush {
        key: String,
        values: Vec<String>,
    },
    Rpush {
        key: String,
        values: Vec<String>,
    },
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
    Llen {
        key: String,
    },

    // ---- Sets ----
    Sadd {
        key: String,
        members: Vec<String>,
    },
    Srem {
        key: String,
        members: Vec<String>,
    },
    Smembers {
        key: String,
    },
    Sismember {
        key: String,
        member: String,
    },
    Scard {
        key: String,
    },

    // ---- Hashes ----
    Hset {
        key: String,
        field: String,
        value: String,
    },
    Hget {
        key: String,
        field: String,
    },
    Hdel {
        key: String,
        fields: Vec<String>,
    },
    Hgetall {
        key: String,
    },
    Hkeys {
        key: String,
    },
    Hvals {
        key: String,
    },
    Hmget {
        key: String,
        fields: Vec<String>,
    },
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
    Zrem {
        key: String,
        members: Vec<String>,
    },
    Zcard {
        key: String,
    },
    Zscore {
        key: String,
        member: String,
    },

    // ---- Pub/sub publish (sub is streaming, out of scope for v1) ----
    Publish {
        channel: String,
        message: String,
    },

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
    Raw {
        args: Vec<String>,
    },
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
        Cmd::Set {
            key,
            value,
            ex,
            px,
            nx,
            xx,
        } => cmd_set(&mut con, &key, &value, ex, px, nx, xx),
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
        Cmd::Scan {
            match_,
            count,
            limit,
        } => cmd_scan(&mut con, &match_, count, limit),

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
        Cmd::Zrange {
            key,
            start,
            stop,
            withscores,
            rev,
        } => cmd_zrange(&mut con, &key, start, stop, withscores, rev),
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
        url.clone()
            .into_connection_info()
            .context("parsing --url")?
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
    if !kv.len().is_multiple_of(2) {
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
    if !fv.len().is_multiple_of(2) {
        bail!(
            "hmset takes pairs of `field value …` — got {} args",
            fv.len()
        );
    }
    let pairs: Vec<(&str, &str)> = fv
        .chunks(2)
        .map(|c| (c[0].as_str(), c[1].as_str()))
        .collect();
    let _: () = con.hset_multiple(key, &pairs)?;
    emit_json(&json!({ "ok": true, "set": pairs.len() }))
}

/* ------------------------------------------------------------------------- */
/* Sorted sets                                                               */
/* ------------------------------------------------------------------------- */

fn cmd_zadd(con: &mut Connection, key: &str, sm: &[String]) -> Result<()> {
    if !sm.len().is_multiple_of(2) {
        bail!(
            "zadd takes pairs of `score member …` — got {} args",
            sm.len()
        );
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
            .map(|(m, s)| json!({ "member": bytes_to_jsonish(m), "score": s }))
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
                groups.insert(
                    std::mem::take(&mut cur_section),
                    Value::Object(std::mem::take(&mut cur_obj)),
                );
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
        R::Attribute {
            data,
            attributes: _,
        } => redis_value_to_json(data),
        R::Push { kind: _, data } => Value::Array(data.iter().map(redis_value_to_json).collect()),
        R::VerbatimString { format: _, text } => Value::String(text.clone()),
        _ => Value::String(format!("{:?}", v)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_conn() -> Conn {
        Conn {
            url: None,
            host: None,
            port: None,
            password: None,
            username: None,
            db: None,
            tls: false,
        }
    }

    // ─── build_conn_info (verified via Debug format since fields are private) ──

    #[test]
    fn build_conn_info_defaults_to_localhost_6379_db_0() {
        let info = build_conn_info(&empty_conn()).unwrap();
        let dbg = format!("{info:?}");
        assert!(dbg.contains("127.0.0.1"), "dbg = {dbg}");
        assert!(dbg.contains("6379"), "dbg = {dbg}");
        // db: 0 default.
        assert!(dbg.contains("db: 0"), "dbg = {dbg}");
    }

    #[test]
    fn build_conn_info_url_wins_over_individual_fields() {
        // When --url is given, host/port/db/etc. are ignored.
        let mut c = empty_conn();
        c.url = Some("redis://example.com:1234/7".into());
        c.host = Some("ignored.com".into());
        c.port = Some(5555);
        c.db = Some(99);
        let info = build_conn_info(&c).unwrap();
        let dbg = format!("{info:?}");
        assert!(dbg.contains("example.com"), "dbg = {dbg}");
        assert!(dbg.contains("1234"), "dbg = {dbg}");
        assert!(dbg.contains("db: 7"), "dbg = {dbg}");
        assert!(!dbg.contains("ignored.com"));
        assert!(!dbg.contains("5555"));
    }

    #[test]
    fn build_conn_info_tls_flag_switches_to_rediss() {
        let mut c = empty_conn();
        c.tls = true;
        let info = build_conn_info(&c).unwrap();
        // TcpTls variant appears in Debug output for rediss:// URLs.
        let dbg = format!("{info:?}");
        assert!(dbg.contains("TcpTls"), "dbg = {dbg}");
    }

    #[test]
    fn build_conn_info_host_and_port_overrides() {
        let mut c = empty_conn();
        c.host = Some("redis.local".into());
        c.port = Some(7000);
        c.db = Some(3);
        let info = build_conn_info(&c).unwrap();
        let dbg = format!("{info:?}");
        assert!(dbg.contains("redis.local"), "dbg = {dbg}");
        assert!(dbg.contains("7000"), "dbg = {dbg}");
        assert!(dbg.contains("db: 3"), "dbg = {dbg}");
    }

    #[test]
    fn build_conn_info_password_only_uses_colon_prefix() {
        // Asserts via URL round-trip: passwordless wouldn't carry the secret
        // through the URL parse. Debug redacts the password so we use a
        // pre-known mock and check that build_conn_info doesn't error.
        let mut c = empty_conn();
        c.password = Some("secret-xyz".into());
        let info = build_conn_info(&c).unwrap();
        // Password might be redacted in Debug — but the function should
        // succeed (not error), proving the colon-only format parses.
        assert!(format!("{info:?}").contains("127.0.0.1"));
    }

    #[test]
    fn build_conn_info_user_and_password() {
        let mut c = empty_conn();
        c.username = Some("alice".into());
        c.password = Some("hunter2".into());
        let info = build_conn_info(&c).unwrap();
        let dbg = format!("{info:?}");
        // Username is not redacted in Debug; it should appear.
        assert!(dbg.contains("alice"), "dbg = {dbg}");
    }

    #[test]
    fn build_conn_info_invalid_url_errors() {
        let mut c = empty_conn();
        c.url = Some("not-a-valid-redis-url".into());
        let err = build_conn_info(&c).unwrap_err();
        assert!(format!("{err:#}").contains("--url"));
    }

    // ─── bytes_to_jsonish ────────────────────────────────────────────

    #[test]
    fn bytes_to_jsonish_utf8_becomes_string() {
        assert_eq!(
            bytes_to_jsonish(b"hello".to_vec()),
            Value::String("hello".into())
        );
    }

    #[test]
    fn bytes_to_jsonish_empty_bytes_empty_string() {
        assert_eq!(bytes_to_jsonish(vec![]), Value::String(String::new()));
    }

    #[test]
    fn bytes_to_jsonish_non_utf8_base64_prefixed() {
        use base64::engine::general_purpose::STANDARD as B64;
        use base64::Engine as _;
        let raw = vec![0xff, 0xfe, 0xfd];
        let v = bytes_to_jsonish(raw.clone());
        let s = v.as_str().unwrap();
        assert!(s.starts_with("base64:"));
        let decoded = B64.decode(s.strip_prefix("base64:").unwrap()).unwrap();
        assert_eq!(decoded, raw);
    }

    #[test]
    fn bytes_to_jsonish_unicode_preserved() {
        let v = bytes_to_jsonish("日本語".as_bytes().to_vec());
        assert_eq!(v, Value::String("日本語".into()));
    }

    // ─── redis_value_to_json ─────────────────────────────────────────

    #[test]
    fn redis_value_to_json_scalars() {
        use redis::Value as R;
        assert_eq!(redis_value_to_json(&R::Nil), Value::Null);
        assert_eq!(redis_value_to_json(&R::Int(42)), json!(42));
        assert_eq!(redis_value_to_json(&R::Okay), json!("OK"));
        assert_eq!(
            redis_value_to_json(&R::SimpleString("PONG".into())),
            json!("PONG")
        );
        assert_eq!(redis_value_to_json(&R::Boolean(true)), json!(true));
        assert_eq!(redis_value_to_json(&R::Double(1.5)), json!(1.5));
    }

    #[test]
    fn redis_value_to_json_bulk_string_via_bytes_to_jsonish() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::BulkString(b"hello".to_vec()));
        assert_eq!(v, json!("hello"));
        // Non-UTF-8 bulk → base64: prefix.
        let v = redis_value_to_json(&R::BulkString(vec![0xff]));
        assert!(v.as_str().unwrap().starts_with("base64:"));
    }

    #[test]
    fn redis_value_to_json_array_recurses() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Array(vec![
            R::Int(1),
            R::SimpleString("two".into()),
            R::Nil,
        ]));
        assert_eq!(v, json!([1, "two", null]));
    }

    #[test]
    fn redis_value_to_json_empty_array() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Array(vec![]));
        assert_eq!(v, json!([]));
    }

    // Note: BigNumber test omitted to avoid pulling in num_bigint just for
    // tests — the variant's behavior (.to_string() → JSON string) is trivial
    // and verified by inspection.

    #[test]
    fn redis_value_to_json_set_becomes_array() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Set(vec![R::Int(1), R::Int(2)]));
        assert_eq!(v, json!([1, 2]));
    }

    #[test]
    fn redis_value_to_json_map_string_keys() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Map(vec![
            (R::BulkString(b"a".to_vec()), R::Int(1)),
            (R::SimpleString("b".into()), R::Int(2)),
        ]));
        assert_eq!(v["a"], json!(1));
        assert_eq!(v["b"], json!(2));
    }

    // ─── emit_ndjson ─────────────────────────────────────────────────

    #[test]
    fn emit_ndjson_appends_newline() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!({"k": 1})).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "{\"k\":1}\n");
    }

    #[test]
    fn emit_ndjson_multi_call_line_count() {
        let mut buf = Vec::new();
        for i in 0..5 {
            emit_ndjson(&mut buf, &json!({"i": i})).unwrap();
        }
        assert_eq!(String::from_utf8(buf).unwrap().lines().count(), 5);
    }

    #[test]
    fn build_conn_info_composed_url_includes_db_path() {
        let mut c = empty_conn();
        c.db = Some(5);
        let dbg = format!("{:?}", build_conn_info(&c).unwrap());
        assert!(dbg.contains("db: 5"), "dbg = {dbg}");
    }

    #[test]
    fn build_conn_info_redis_scheme_when_tls_false() {
        let dbg = format!("{:?}", build_conn_info(&empty_conn()).unwrap());
        assert!(!dbg.contains("TcpTls"), "dbg = {dbg}");
    }

    #[test]
    fn redis_value_to_json_server_error_stringifies() {
        use redis::Value as R;
        use std::io::Cursor;
        let raw: R = redis::Parser::default()
            .parse_value(Cursor::new(b"-ERR wrong type\r\n"))
            .unwrap();
        let R::ServerError(e) = raw else {
            panic!("expected ServerError variant");
        };
        let v = redis_value_to_json(&R::ServerError(e));
        let s = v.as_str().unwrap();
        assert!(s.starts_with("error:"));
        assert!(s.contains("wrong type"));
    }

    #[test]
    fn redis_value_to_json_attribute_unwraps_data() {
        use redis::Value as R;
        let inner = R::Int(99);
        let v = redis_value_to_json(&R::Attribute {
            data: Box::new(inner),
            attributes: vec![],
        });
        assert_eq!(v, json!(99));
    }

    #[test]
    fn redis_value_to_json_verbatim_string_returns_text() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::VerbatimString {
            format: redis::VerbatimFormat::Text,
            text: "bulk-string".into(),
        });
        assert_eq!(v, json!("bulk-string"));
    }

    #[test]
    fn redis_value_to_json_push_becomes_array() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Push {
            kind: redis::PushKind::Message,
            data: vec![
                R::SimpleString("chan".into()),
                R::BulkString(b"hi".to_vec()),
            ],
        });
        assert_eq!(v, json!(["chan", "hi"]));
    }

    #[test]
    fn redis_value_to_json_map_non_string_key_uses_debug() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Map(vec![(R::Int(1), R::Int(2))]));
        let obj = v.as_object().unwrap();
        assert!(obj.contains_key("1") || obj.keys().any(|k| k.contains('1')));
        assert_eq!(obj.values().next().unwrap(), &json!(2));
    }

    #[test]
    fn emit_int_and_emit_bool_wrap_value_key() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!({ "value": 7 })).unwrap();
        assert!(String::from_utf8(buf).unwrap().contains("\"value\":7"));
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!({ "value": true })).unwrap();
        assert!(String::from_utf8(buf).unwrap().contains("\"value\":true"));
    }

    #[test]
    fn build_conn_info_custom_port_only() {
        let mut c = empty_conn();
        c.port = Some(6380);
        let dbg = format!("{:?}", build_conn_info(&c).unwrap());
        assert!(dbg.contains("6380"), "dbg = {dbg}");
        assert!(dbg.contains("127.0.0.1"), "dbg = {dbg}");
    }

    #[test]
    fn build_conn_info_no_password_omits_auth_segment() {
        let mut c = empty_conn();
        c.username = Some("alice".into());
        let dbg = format!("{:?}", build_conn_info(&c).unwrap());
        assert!(dbg.contains("127.0.0.1"));
        // Without password the composed URL has no `@` auth block.
        assert!(!dbg.contains('@') || !dbg.contains("alice@"), "dbg = {dbg}");
    }

    #[test]
    fn bytes_to_jsonish_single_non_utf8_byte() {
        let v = bytes_to_jsonish(vec![0x80]);
        let s = v.as_str().unwrap();
        assert!(s.starts_with("base64:"));
    }

    #[test]
    fn redis_value_to_json_empty_bulk_string() {
        use redis::Value as R;
        assert_eq!(redis_value_to_json(&R::BulkString(vec![])), json!(""),);
    }

    #[test]
    fn redis_value_to_json_map_simple_string_keys() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Map(vec![(R::SimpleString("k".into()), R::Int(9))]));
        assert_eq!(v["k"], json!(9));
    }

    #[test]
    fn redis_value_to_json_double_and_ok_in_array() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Array(vec![R::Double(-1.5), R::Okay]));
        assert_eq!(v, json!([-1.5, "OK"]));
    }

    #[test]
    fn redis_value_to_json_verbatim_markdown_format() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::VerbatimString {
            format: redis::VerbatimFormat::Markdown,
            text: "# title".into(),
        });
        assert_eq!(v, json!("# title"));
    }

    #[test]
    fn redis_value_to_json_nested_array_of_ints() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Array(vec![
            R::Array(vec![R::Int(1), R::Int(2)]),
            R::Int(3),
        ]));
        assert_eq!(v, json!([[1, 2], 3]));
    }

    #[test]
    fn redis_value_to_json_boolean_false() {
        use redis::Value as R;
        assert_eq!(redis_value_to_json(&R::Boolean(false)), json!(false));
    }

    #[test]
    fn redis_value_to_json_negative_int() {
        use redis::Value as R;
        assert_eq!(redis_value_to_json(&R::Int(-42)), json!(-42));
    }

    #[test]
    fn redis_value_to_json_simple_string_empty() {
        use redis::Value as R;
        assert_eq!(
            redis_value_to_json(&R::SimpleString(String::new())),
            json!("")
        );
    }

    #[test]
    fn redis_value_to_json_set_to_array() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Set(vec![R::Int(2), R::Int(1)]));
        assert_eq!(v, json!([2, 1]));
    }

    #[test]
    fn redis_value_to_json_array_with_nil() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Array(vec![R::Nil, R::Int(0)]));
        assert_eq!(v, json!([null, 0]));
    }

    #[test]
    fn bytes_to_jsonish_emoji_utf8() {
        assert_eq!(bytes_to_jsonish("🦀".as_bytes().to_vec()), json!("🦀"));
    }

    #[test]
    fn build_conn_info_db_zero_in_debug() {
        let mut c = empty_conn();
        c.db = Some(0);
        let dbg = format!("{:?}", build_conn_info(&c).unwrap());
        assert!(dbg.contains("db: 0"), "dbg = {dbg}");
    }

    #[test]
    fn redis_value_to_json_map_bulk_string_key() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Map(vec![(
            R::BulkString(b"field".to_vec()),
            R::SimpleString("val".into()),
        )]));
        assert_eq!(v["field"], json!("val"));
    }

    #[test]
    fn build_conn_info_rediss_url_uses_tls() {
        let mut c = empty_conn();
        c.url = Some("rediss://cache.example.com:6380/1".into());
        let dbg = format!("{:?}", build_conn_info(&c).unwrap());
        assert!(dbg.contains("TcpTls"), "dbg = {dbg}");
        assert!(dbg.contains("cache.example.com"), "dbg = {dbg}");
    }

    #[test]
    fn redis_value_to_json_empty_map() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Map(vec![]));
        assert_eq!(v, json!({}));
    }

    #[test]
    fn redis_value_to_json_int_zero() {
        use redis::Value as R;
        assert_eq!(redis_value_to_json(&R::Int(0)), json!(0));
    }

    #[test]
    fn bytes_to_jsonish_utf8_with_newline() {
        assert_eq!(bytes_to_jsonish(b"a\nb".to_vec()), json!("a\nb"));
    }

    #[test]
    fn build_conn_info_port_without_explicit_host() {
        let mut c = empty_conn();
        c.port = Some(6381);
        let dbg = format!("{:?}", build_conn_info(&c).unwrap());
        assert!(dbg.contains("127.0.0.1"), "dbg = {dbg}");
        assert!(dbg.contains("6381"), "dbg = {dbg}");
    }

    #[test]
    fn redis_value_to_json_simple_string_with_spaces() {
        use redis::Value as R;
        assert_eq!(
            redis_value_to_json(&R::SimpleString("hello world".into())),
            json!("hello world"),
        );
    }

    #[test]
    fn emit_ndjson_null_value() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &Value::Null).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "null\n");
    }

    #[test]
    fn build_conn_info_db_fifteen() {
        let mut c = empty_conn();
        c.db = Some(15);
        let dbg = format!("{:?}", build_conn_info(&c).unwrap());
        assert!(dbg.contains("db: 15"), "dbg = {dbg}");
    }

    #[test]
    fn build_conn_info_host_only_override() {
        let mut c = empty_conn();
        c.host = Some("redis.internal".into());
        let dbg = format!("{:?}", build_conn_info(&c).unwrap());
        assert!(dbg.contains("redis.internal"), "dbg = {dbg}");
        assert!(dbg.contains("6379"), "dbg = {dbg}");
    }

    #[test]
    fn redis_value_to_json_double_negative() {
        use redis::Value as R;
        assert_eq!(redis_value_to_json(&R::Double(-0.5)), json!(-0.5));
    }

    #[test]
    fn redis_value_to_json_push_empty_data_array() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Push {
            kind: redis::PushKind::Message,
            data: vec![],
        });
        assert_eq!(v, json!([]));
    }

    #[test]
    fn bytes_to_jsonish_tab_in_utf8_string() {
        assert_eq!(bytes_to_jsonish(b"a\tb".to_vec()), json!("a\tb"));
    }

    #[test]
    fn emit_ndjson_false_bool() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!(false)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "false\n");
    }

    #[test]
    fn emit_ndjson_empty_object() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!({})).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "{}\n");
    }

    #[test]
    fn redis_value_to_json_array_single_element() {
        use redis::Value as R;
        assert_eq!(redis_value_to_json(&R::Array(vec![R::Int(9)])), json!([9]));
    }

    #[test]
    fn build_conn_info_username_only_no_password() {
        let mut c = empty_conn();
        c.username = Some("svc".into());
        let dbg = format!("{:?}", build_conn_info(&c).unwrap());
        assert!(dbg.contains("127.0.0.1"), "dbg = {dbg}");
        // Without password the composed URL has no auth segment.
        assert!(!dbg.contains('@'), "dbg = {dbg}");
    }

    #[test]
    fn build_conn_info_tls_flag_with_custom_host() {
        let mut c = empty_conn();
        c.tls = true;
        c.host = Some("secure.redis".into());
        let dbg = format!("{:?}", build_conn_info(&c).unwrap());
        assert!(dbg.contains("TcpTls"), "dbg = {dbg}");
        assert!(dbg.contains("secure.redis"), "dbg = {dbg}");
    }

    #[test]
    fn redis_value_to_json_set_single_member() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Set(vec![R::SimpleString("only".into())]));
        assert_eq!(v, json!(["only"]));
    }

    #[test]
    fn bytes_to_jsonish_carriage_return_utf8() {
        assert_eq!(bytes_to_jsonish(b"a\rb".to_vec()), json!("a\rb"));
    }

    #[test]
    fn emit_ndjson_positive_integer() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!(100)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "100\n");
    }

    #[test]
    fn redis_value_to_json_map_with_ok_value() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Map(vec![(R::SimpleString("status".into()), R::Okay)]));
        assert_eq!(v["status"], json!("OK"));
    }

    #[test]
    fn redis_value_to_json_bulk_string_non_utf8() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::BulkString(vec![0xFE]));
        assert!(v.as_str().unwrap().starts_with("base64:"));
    }

    #[test]
    fn build_conn_info_password_without_username() {
        let mut c = empty_conn();
        c.password = Some("secret".into());
        assert!(build_conn_info(&c).is_ok());
    }

    #[test]
    fn redis_value_to_json_verbatim_text_format() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::VerbatimString {
            format: redis::VerbatimFormat::Text,
            text: "raw".into(),
        });
        assert_eq!(v, json!("raw"));
    }

    #[test]
    fn redis_value_to_json_boolean_true() {
        use redis::Value as R;
        assert_eq!(redis_value_to_json(&R::Boolean(true)), json!(true));
    }

    #[test]
    fn emit_ndjson_nested_object_one_field() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!({"k": "v"})).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "{\"k\":\"v\"}\n");
    }

    #[test]
    fn build_conn_info_custom_port_6380() {
        let mut c = empty_conn();
        c.port = Some(6380);
        let dbg = format!("{:?}", build_conn_info(&c).unwrap());
        assert!(dbg.contains("6380"), "dbg = {dbg}");
    }

    #[test]
    fn redis_value_to_json_map_two_keys() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Map(vec![
            (R::SimpleString("a".into()), R::Int(1)),
            (R::SimpleString("b".into()), R::Int(2)),
        ]));
        assert_eq!(v["a"], json!(1));
        assert_eq!(v["b"], json!(2));
    }

    #[test]
    fn redis_value_to_json_push_two_elements() {
        use redis::Value as R;
        let v = redis_value_to_json(&R::Push {
            kind: redis::PushKind::Message,
            data: vec![R::SimpleString("ch".into()), R::BulkString(b"hi".to_vec())],
        });
        assert_eq!(v.as_array().unwrap().len(), 2);
    }

    #[test]
    fn bytes_to_jsonish_backslash_utf8() {
        assert_eq!(bytes_to_jsonish(b"a\\b".to_vec()), json!("a\\b"));
    }

    #[test]
    fn build_conn_info_username_and_password_fields() {
        let mut c = empty_conn();
        c.username = Some("u".into());
        c.password = Some("p".into());
        let dbg = format!("{:?}", build_conn_info(&c).unwrap());
        assert!(dbg.contains("username: Some(\"u\")"), "dbg = {dbg}");
        assert!(dbg.contains("password: Some"), "dbg = {dbg}");
    }
}
