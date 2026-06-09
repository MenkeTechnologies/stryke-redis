//! stryke-redis — Redis / Valkey cdylib loaded in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn redis__*` is a JSON-string-in /
//! JSON-string-out wrapper around the `redis` crate's sync API. stryke's FFI
//! bridge (`rust_ffi.rs::load_cdylib`) resolves these symbols at first
//! `use Redis`, registers each one as a stryke-callable function, and on each
//! call passes a JSON-encoded args dict and copies the returned JSON into a
//! stryke string. The cdylib's `stryke_free_cstring` export plugs the
//! returned-allocation leak the inline-FFI v1 had.
//!
//! Persistent state: `CONNS` caches one `redis::Connection` per
//! `(url, host:port, db, tls, auth)` tuple for the life of the stryke
//! process. The predecessor `stryke-redis-helper` binary opened a fresh TCP
//! connection per call — every `Redis::get(...)` paid full connect + auth.
//! With this cdylib, the connection survives across calls and `Redis::get`
//! reduces to one RESP roundtrip.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use redis::{Commands, Connection};
use serde_json::{json, Value};

// ── connection cache ────────────────────────────────────────────────────────

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct ConnKey {
    url: String,
    db: i64,
    tls: bool,
    username: String,
    password: String,
}

type ConnHandle = Arc<Mutex<Connection>>;

static CONNS: OnceCell<Mutex<HashMap<ConnKey, ConnHandle>>> = OnceCell::new();

fn conns() -> &'static Mutex<HashMap<ConnKey, ConnHandle>> {
    CONNS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Build a connection URL from either the explicit `url` field or
/// `host`/`port`/`db`/`username`/`password`/`tls` parts. Matches the
/// connection flags the v1 helper binary accepted.
fn url_from_opts(opts: &Value) -> ConnKey {
    if let Some(u) = opts.get("url").and_then(|v| v.as_str()) {
        return ConnKey {
            url: u.to_string(),
            db: opts.get("db").and_then(|v| v.as_i64()).unwrap_or(-1),
            tls: opts.get("tls").and_then(|v| v.as_bool()).unwrap_or(false),
            username: opts
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            password: opts
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        };
    }
    let host = opts
        .get("host")
        .and_then(|v| v.as_str())
        .unwrap_or("127.0.0.1");
    let port = opts.get("port").and_then(|v| v.as_i64()).unwrap_or(6379);
    let db = opts.get("db").and_then(|v| v.as_i64()).unwrap_or(0);
    let tls = opts.get("tls").and_then(|v| v.as_bool()).unwrap_or(false);
    let username = opts
        .get("username")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let password = opts
        .get("password")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let scheme = if tls { "rediss" } else { "redis" };
    let auth = match (username.as_str(), password.as_str()) {
        ("", "") => String::new(),
        ("", p) => format!(":{}@", p),
        (u, p) => format!("{}:{}@", u, p),
    };
    let url = format!("{}://{}{}:{}/{}", scheme, auth, host, port, db);
    ConnKey {
        url,
        db,
        tls,
        username,
        password,
    }
}

/// Get or open the cached connection for this opts dict, then run `f` against it.
fn with_conn<F, R>(opts: &Value, f: F) -> Result<R>
where
    F: FnOnce(&mut Connection) -> Result<R>,
{
    let key = url_from_opts(opts);
    let handle = {
        let mut map = conns().lock();
        if let Some(h) = map.get(&key) {
            Arc::clone(h)
        } else {
            let client = redis::Client::open(key.url.as_str())
                .map_err(|e| anyhow!("connect {}: {}", key.url, e))?;
            let conn = client
                .get_connection()
                .map_err(|e| anyhow!("get_connection {}: {}", key.url, e))?;
            let h = Arc::new(Mutex::new(conn));
            map.insert(key.clone(), Arc::clone(&h));
            h
        }
    };
    let mut conn = handle.lock();
    f(&mut conn)
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call<F>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Result<Value>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| handler(input)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-redis handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib. stryke's FFI
/// bridge calls this immediately after copying the returned bytes into a
/// stryke string.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── version + ping ─────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__version(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| {
        Ok(json!({"version": env!("CARGO_PKG_VERSION")}))
    })
}

#[no_mangle]
pub extern "C" fn redis__ping(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            let pong: String = redis::cmd("PING").query(c)?;
            Ok(json!({"value": pong}))
        })
    })
}

// ── string KV ───────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        with_conn(&v, |c| {
            let val: Option<String> = c.get(key)?;
            Ok(json!({"value": val}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__set(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let value = v["value"].as_str().ok_or_else(|| anyhow!("missing value"))?;
        let ex = v["ex"].as_i64();
        let px = v["px"].as_i64();
        let nx = v["nx"].as_bool().unwrap_or(false);
        let xx = v["xx"].as_bool().unwrap_or(false);
        with_conn(&v, |c| {
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
            let r: Option<String> = cmd.query(c)?;
            Ok(json!({"ok": r.as_deref() == Some("OK")}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__del(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let keys = string_vec(&v["keys"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("DEL").arg(&keys).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__exists(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let keys = string_vec(&v["keys"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("EXISTS").arg(&keys).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__expire(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let secs = v["seconds"].as_i64().ok_or_else(|| anyhow!("missing seconds"))?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("EXPIRE").arg(key).arg(secs).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__ttl(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("TTL").arg(key).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__type(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        with_conn(&v, |c| {
            let t: String = redis::cmd("TYPE").arg(key).query(c)?;
            Ok(json!({"type": t}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__incr(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let by = v["by"].as_i64();
        with_conn(&v, |c| {
            let n: i64 = match by {
                Some(d) => redis::cmd("INCRBY").arg(key).arg(d).query(c)?,
                None => redis::cmd("INCR").arg(key).query(c)?,
            };
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__decr(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let by = v["by"].as_i64();
        with_conn(&v, |c| {
            let n: i64 = match by {
                Some(d) => redis::cmd("DECRBY").arg(key).arg(d).query(c)?,
                None => redis::cmd("DECR").arg(key).query(c)?,
            };
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__mget(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let keys = string_vec(&v["keys"])?;
        with_conn(&v, |c| {
            let vals: Vec<Option<String>> = redis::cmd("MGET").arg(&keys).query(c)?;
            Ok(json!({"values": vals}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__mset(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let flat = string_vec(&v["pairs"])?;
        if flat.len() % 2 != 0 {
            return Err(anyhow!("mset pairs must be even-length"));
        }
        with_conn(&v, |c| {
            let r: String = redis::cmd("MSET").arg(&flat).query(c)?;
            Ok(json!({"ok": r == "OK"}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__keys(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let pattern = v["pattern"].as_str().unwrap_or("*");
        with_conn(&v, |c| {
            let keys: Vec<String> = redis::cmd("KEYS").arg(pattern).query(c)?;
            Ok(json!({"keys": keys}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__scan(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let match_pat = v["match"].as_str();
        let count = v["count"].as_i64();
        let limit = v["limit"].as_i64().map(|n| n as usize);
        with_conn(&v, |c| {
            let mut cur: u64 = 0;
            let mut out: Vec<String> = Vec::new();
            loop {
                let mut cmd = redis::cmd("SCAN");
                cmd.arg(cur);
                if let Some(p) = match_pat {
                    cmd.arg("MATCH").arg(p);
                }
                if let Some(n) = count {
                    cmd.arg("COUNT").arg(n);
                }
                let (next, batch): (u64, Vec<String>) = cmd.query(c)?;
                out.extend(batch);
                if let Some(l) = limit {
                    if out.len() >= l {
                        out.truncate(l);
                        break;
                    }
                }
                cur = next;
                if cur == 0 {
                    break;
                }
            }
            Ok(json!({"keys": out}))
        })
    })
}

// ── lists ───────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__lpush(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let vals = string_vec(&v["values"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("LPUSH").arg(key).arg(&vals).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__rpush(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let vals = string_vec(&v["values"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("RPUSH").arg(key).arg(&vals).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__lpop(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let count = v["count"].as_i64();
        with_conn(&v, |c| match count {
            Some(n) => {
                let vals: Vec<String> = redis::cmd("LPOP").arg(key).arg(n).query(c)?;
                Ok(json!({"values": vals}))
            }
            None => {
                let val: Option<String> = redis::cmd("LPOP").arg(key).query(c)?;
                Ok(json!({"value": val}))
            }
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__rpop(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let count = v["count"].as_i64();
        with_conn(&v, |c| match count {
            Some(n) => {
                let vals: Vec<String> = redis::cmd("RPOP").arg(key).arg(n).query(c)?;
                Ok(json!({"values": vals}))
            }
            None => {
                let val: Option<String> = redis::cmd("RPOP").arg(key).query(c)?;
                Ok(json!({"value": val}))
            }
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__lrange(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let start = v["start"].as_i64().ok_or_else(|| anyhow!("missing start"))?;
        let stop = v["stop"].as_i64().ok_or_else(|| anyhow!("missing stop"))?;
        with_conn(&v, |c| {
            let vals: Vec<String> = redis::cmd("LRANGE")
                .arg(key)
                .arg(start)
                .arg(stop)
                .query(c)?;
            Ok(json!({"values": vals}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__llen(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("LLEN").arg(key).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

// ── sets ────────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__sadd(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let members = string_vec(&v["members"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("SADD").arg(key).arg(&members).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__srem(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let members = string_vec(&v["members"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("SREM").arg(key).arg(&members).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__smembers(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        with_conn(&v, |c| {
            let vals: Vec<String> = redis::cmd("SMEMBERS").arg(key).query(c)?;
            Ok(json!({"members": vals}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__sismember(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let m = v["member"].as_str().ok_or_else(|| anyhow!("missing member"))?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("SISMEMBER").arg(key).arg(m).query(c)?;
            Ok(json!({"value": n != 0}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__scard(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("SCARD").arg(key).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

// ── hashes ──────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__hset(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let field = v["field"].as_str().ok_or_else(|| anyhow!("missing field"))?;
        let value = v["value"].as_str().ok_or_else(|| anyhow!("missing value"))?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("HSET").arg(key).arg(field).arg(value).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__hget(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let field = v["field"].as_str().ok_or_else(|| anyhow!("missing field"))?;
        with_conn(&v, |c| {
            let val: Option<String> = redis::cmd("HGET").arg(key).arg(field).query(c)?;
            Ok(json!({"value": val}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__hdel(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let fields = string_vec(&v["fields"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("HDEL").arg(key).arg(&fields).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__hgetall(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        with_conn(&v, |c| {
            let pairs: Vec<(String, String)> = redis::cmd("HGETALL").arg(key).query(c)?;
            let map: serde_json::Map<String, Value> = pairs
                .into_iter()
                .map(|(k, v)| (k, Value::String(v)))
                .collect();
            Ok(json!({"hash": map}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__hkeys(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        with_conn(&v, |c| {
            let keys: Vec<String> = redis::cmd("HKEYS").arg(key).query(c)?;
            Ok(json!({"keys": keys}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__hvals(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        with_conn(&v, |c| {
            let vals: Vec<String> = redis::cmd("HVALS").arg(key).query(c)?;
            Ok(json!({"values": vals}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__hmget(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let fields = string_vec(&v["fields"])?;
        with_conn(&v, |c| {
            let vals: Vec<Option<String>> = redis::cmd("HMGET").arg(key).arg(&fields).query(c)?;
            Ok(json!({"values": vals}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__hmset(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let flat = string_vec(&v["pairs"])?;
        if flat.len() % 2 != 0 {
            return Err(anyhow!("hmset pairs must be even-length"));
        }
        with_conn(&v, |c| {
            let r: String = redis::cmd("HMSET").arg(key).arg(&flat).query(c)?;
            Ok(json!({"ok": r == "OK"}))
        })
    })
}

// ── sorted sets ─────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__zadd(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        // pairs: [score, member, score, member, ...] — strings on the wire.
        let arr = v["pairs"]
            .as_array()
            .ok_or_else(|| anyhow!("missing pairs"))?;
        if arr.len() % 2 != 0 {
            return Err(anyhow!("zadd pairs must be even-length (score, member, ...)"));
        }
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("ZADD");
            cmd.arg(key);
            for chunk in arr.chunks(2) {
                let score = chunk[0]
                    .as_f64()
                    .or_else(|| chunk[0].as_str().and_then(|s| s.parse::<f64>().ok()))
                    .ok_or_else(|| anyhow!("bad zadd score"))?;
                let member = chunk[1]
                    .as_str()
                    .ok_or_else(|| anyhow!("bad zadd member"))?;
                cmd.arg(score).arg(member);
            }
            let n: i64 = cmd.query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__zrange(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let start = v["start"].as_i64().ok_or_else(|| anyhow!("missing start"))?;
        let stop = v["stop"].as_i64().ok_or_else(|| anyhow!("missing stop"))?;
        let with_scores = v["with_scores"].as_bool().unwrap_or(false);
        let rev = v["rev"].as_bool().unwrap_or(false);
        with_conn(&v, |c| {
            let mut cmd = redis::cmd(if rev { "ZREVRANGE" } else { "ZRANGE" });
            cmd.arg(key).arg(start).arg(stop);
            if with_scores {
                cmd.arg("WITHSCORES");
                let pairs: Vec<(String, f64)> = cmd.query(c)?;
                Ok(json!({"pairs": pairs}))
            } else {
                let vals: Vec<String> = cmd.query(c)?;
                Ok(json!({"values": vals}))
            }
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__zrem(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let members = string_vec(&v["members"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("ZREM").arg(key).arg(&members).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__zcard(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("ZCARD").arg(key).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__zscore(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
        let member = v["member"].as_str().ok_or_else(|| anyhow!("missing member"))?;
        with_conn(&v, |c| {
            let s: Option<f64> = redis::cmd("ZSCORE").arg(key).arg(member).query(c)?;
            Ok(json!({"value": s}))
        })
    })
}

// ── pub/sub publish ─────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__publish(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let channel = v["channel"].as_str().ok_or_else(|| anyhow!("missing channel"))?;
        let message = v["message"].as_str().ok_or_else(|| anyhow!("missing message"))?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("PUBLISH").arg(channel).arg(message).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

// ── server admin ────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__info(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let section = v["section"].as_str();
        with_conn(&v, |c| {
            let info: String = match section {
                Some(s) => redis::cmd("INFO").arg(s).query(c)?,
                None => redis::cmd("INFO").query(c)?,
            };
            Ok(json!({"info": info}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__dbsize(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("DBSIZE").query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__flushdb(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let asyncronous = v["asynchronous"].as_bool().unwrap_or(false);
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("FLUSHDB");
            if asyncronous {
                cmd.arg("ASYNC");
            }
            let r: String = cmd.query(c)?;
            Ok(json!({"ok": r == "OK"}))
        })
    })
}

// ── raw command escape hatch ────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__raw(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let argv = string_vec(&v["argv"])?;
        if argv.is_empty() {
            return Err(anyhow!("raw: argv must be non-empty"));
        }
        with_conn(&v, |c| {
            let mut cmd = redis::cmd(&argv[0]);
            for a in &argv[1..] {
                cmd.arg(a);
            }
            // Use redis::Value to handle any reply shape, then re-encode as JSON.
            let val: redis::Value = cmd.query(c)?;
            Ok(json!({"value": redis_value_to_json(val)}))
        })
    })
}

// ── helpers ─────────────────────────────────────────────────────────────────

/// Accept either a JSON array of strings or a single string, return Vec<String>.
fn string_vec(v: &Value) -> Result<Vec<String>> {
    match v {
        Value::Array(a) => a
            .iter()
            .map(|x| x.as_str().map(String::from).ok_or_else(|| anyhow!("non-string in array")))
            .collect(),
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Null => Ok(Vec::new()),
        _ => Err(anyhow!("expected string or array of strings")),
    }
}

/// Generic RESP value → serde_json::Value for the `raw` escape hatch.
fn redis_value_to_json(v: redis::Value) -> Value {
    match v {
        redis::Value::Nil => Value::Null,
        redis::Value::Int(n) => json!(n),
        redis::Value::BulkString(b) => match String::from_utf8(b) {
            Ok(s) => Value::String(s),
            Err(e) => Value::String(format!("<binary {} bytes>", e.into_bytes().len())),
        },
        redis::Value::SimpleString(s) => Value::String(s),
        redis::Value::Okay => Value::String("OK".to_string()),
        redis::Value::Array(arr) => Value::Array(arr.into_iter().map(redis_value_to_json).collect()),
        redis::Value::Map(pairs) => {
            let m: serde_json::Map<String, Value> = pairs
                .into_iter()
                .map(|(k, v)| {
                    let key = match k {
                        redis::Value::SimpleString(s) => s,
                        redis::Value::BulkString(b) => {
                            String::from_utf8(b).unwrap_or_else(|_| "<binary>".to_string())
                        }
                        other => format!("{:?}", other),
                    };
                    (key, redis_value_to_json(v))
                })
                .collect();
            Value::Object(m)
        }
        other => Value::String(format!("{:?}", other)),
    }
}
