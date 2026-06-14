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
    ffi_call(args, |_| Ok(json!({"version": env!("CARGO_PKG_VERSION")})))
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
        let value = v["value"]
            .as_str()
            .ok_or_else(|| anyhow!("missing value"))?;
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
        let secs = v["seconds"]
            .as_i64()
            .ok_or_else(|| anyhow!("missing seconds"))?;
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
        let start = v["start"]
            .as_i64()
            .ok_or_else(|| anyhow!("missing start"))?;
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
        let m = v["member"]
            .as_str()
            .ok_or_else(|| anyhow!("missing member"))?;
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
        let field = v["field"]
            .as_str()
            .ok_or_else(|| anyhow!("missing field"))?;
        let value = v["value"]
            .as_str()
            .ok_or_else(|| anyhow!("missing value"))?;
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
        let field = v["field"]
            .as_str()
            .ok_or_else(|| anyhow!("missing field"))?;
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
            return Err(anyhow!(
                "zadd pairs must be even-length (score, member, ...)"
            ));
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
        let start = v["start"]
            .as_i64()
            .ok_or_else(|| anyhow!("missing start"))?;
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
        let member = v["member"]
            .as_str()
            .ok_or_else(|| anyhow!("missing member"))?;
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
        let channel = v["channel"]
            .as_str()
            .ok_or_else(|| anyhow!("missing channel"))?;
        let message = v["message"]
            .as_str()
            .ok_or_else(|| anyhow!("missing message"))?;
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

// ── key management ───────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__rename(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let newkey = need_str(&v, "newkey")?;
        with_conn(&v, |c| {
            let r: String = redis::cmd("RENAME").arg(key).arg(newkey).query(c)?;
            Ok(json!({"ok": r == "OK"}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__renamenx(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let newkey = need_str(&v, "newkey")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("RENAMENX").arg(key).arg(newkey).query(c)?;
            Ok(json!({"value": n != 0}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__persist(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("PERSIST").arg(key).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__pexpire(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let ms = need_i64(&v, "millis")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("PEXPIRE").arg(key).arg(ms).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__pttl(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("PTTL").arg(key).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__expireat(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let ts = need_i64(&v, "timestamp")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("EXPIREAT").arg(key).arg(ts).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__pexpireat(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let ts = need_i64(&v, "millis_timestamp")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("PEXPIREAT").arg(key).arg(ts).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__expiretime(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("EXPIRETIME").arg(key).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__randomkey(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            let k: Option<String> = redis::cmd("RANDOMKEY").query(c)?;
            Ok(json!({"value": k}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__touch(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let keys = string_vec(&v["keys"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("TOUCH").arg(&keys).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__unlink(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let keys = string_vec(&v["keys"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("UNLINK").arg(&keys).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__copy(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let source = need_str(&v, "source")?;
        let dest = need_str(&v, "destination")?;
        let replace = v["replace"].as_bool().unwrap_or(false);
        let db = v["destination_db"].as_i64();
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("COPY");
            cmd.arg(source).arg(dest);
            if let Some(d) = db {
                cmd.arg("DB").arg(d);
            }
            if replace {
                cmd.arg("REPLACE");
            }
            let n: i64 = cmd.query(c)?;
            Ok(json!({"value": n != 0}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__object_encoding(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        with_conn(&v, |c| {
            let e: Option<String> = redis::cmd("OBJECT").arg("ENCODING").arg(key).query(c)?;
            Ok(json!({"value": e}))
        })
    })
}

// ── string extras ────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__getset(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let value = need_str(&v, "value")?;
        with_conn(&v, |c| {
            let old: Option<String> = redis::cmd("GETSET").arg(key).arg(value).query(c)?;
            Ok(json!({"value": old}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__getdel(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        with_conn(&v, |c| {
            let old: Option<String> = redis::cmd("GETDEL").arg(key).query(c)?;
            Ok(json!({"value": old}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__append(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let value = need_str(&v, "value")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("APPEND").arg(key).arg(value).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__strlen(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("STRLEN").arg(key).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__setex(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let value = need_str(&v, "value")?;
        let secs = need_i64(&v, "seconds")?;
        let px = v["px"].as_bool().unwrap_or(false);
        with_conn(&v, |c| {
            let r: String = redis::cmd(if px { "PSETEX" } else { "SETEX" })
                .arg(key)
                .arg(secs)
                .arg(value)
                .query(c)?;
            Ok(json!({"ok": r == "OK"}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__setnx(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let value = need_str(&v, "value")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("SETNX").arg(key).arg(value).query(c)?;
            Ok(json!({"value": n != 0}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__getrange(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let start = need_i64(&v, "start")?;
        let end = need_i64(&v, "end")?;
        with_conn(&v, |c| {
            let s: String = redis::cmd("GETRANGE")
                .arg(key)
                .arg(start)
                .arg(end)
                .query(c)?;
            Ok(json!({"value": s}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__setrange(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let offset = need_i64(&v, "offset")?;
        let value = need_str(&v, "value")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("SETRANGE")
                .arg(key)
                .arg(offset)
                .arg(value)
                .query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__incrbyfloat(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let by = v["by"].as_f64().ok_or_else(|| anyhow!("missing by"))?;
        with_conn(&v, |c| {
            let n: f64 = redis::cmd("INCRBYFLOAT").arg(key).arg(by).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

// ── bitmaps ──────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__setbit(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let offset = need_i64(&v, "offset")?;
        let bit = need_i64(&v, "bit")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("SETBIT")
                .arg(key)
                .arg(offset)
                .arg(bit)
                .query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__getbit(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let offset = need_i64(&v, "offset")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("GETBIT").arg(key).arg(offset).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__bitcount(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let start = v["start"].as_i64();
        let end = v["end"].as_i64();
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("BITCOUNT");
            cmd.arg(key);
            if let (Some(s), Some(e)) = (start, end) {
                cmd.arg(s).arg(e);
            }
            let n: i64 = cmd.query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__bitop(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let op = need_str(&v, "operation")?;
        let dest = need_str(&v, "destination")?;
        let keys = string_vec(&v["keys"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("BITOP").arg(op).arg(dest).arg(&keys).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

// ── list extras ──────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__lindex(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let index = need_i64(&v, "index")?;
        with_conn(&v, |c| {
            let val: Option<String> = redis::cmd("LINDEX").arg(key).arg(index).query(c)?;
            Ok(json!({"value": val}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__lset(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let index = need_i64(&v, "index")?;
        let value = need_str(&v, "value")?;
        with_conn(&v, |c| {
            let r: String = redis::cmd("LSET").arg(key).arg(index).arg(value).query(c)?;
            Ok(json!({"ok": r == "OK"}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__linsert(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let before = v["before"].as_bool().unwrap_or(false);
        let pivot = need_str(&v, "pivot")?;
        let value = need_str(&v, "value")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("LINSERT")
                .arg(key)
                .arg(if before { "BEFORE" } else { "AFTER" })
                .arg(pivot)
                .arg(value)
                .query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__lrem(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let count = need_i64(&v, "count")?;
        let value = need_str(&v, "value")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("LREM").arg(key).arg(count).arg(value).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__ltrim(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let start = need_i64(&v, "start")?;
        let stop = need_i64(&v, "stop")?;
        with_conn(&v, |c| {
            let r: String = redis::cmd("LTRIM").arg(key).arg(start).arg(stop).query(c)?;
            Ok(json!({"ok": r == "OK"}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__rpoplpush(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let source = need_str(&v, "source")?;
        let dest = need_str(&v, "destination")?;
        with_conn(&v, |c| {
            let val: Option<String> = redis::cmd("RPOPLPUSH").arg(source).arg(dest).query(c)?;
            Ok(json!({"value": val}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__lmove(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let source = need_str(&v, "source")?;
        let dest = need_str(&v, "destination")?;
        let from = if v["from"].as_str() == Some("RIGHT") {
            "RIGHT"
        } else {
            "LEFT"
        };
        let to = if v["to"].as_str() == Some("RIGHT") {
            "RIGHT"
        } else {
            "LEFT"
        };
        with_conn(&v, |c| {
            let val: Option<String> = redis::cmd("LMOVE")
                .arg(source)
                .arg(dest)
                .arg(from)
                .arg(to)
                .query(c)?;
            Ok(json!({"value": val}))
        })
    })
}

// ── hash extras ──────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__hexists(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let field = need_str(&v, "field")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("HEXISTS").arg(key).arg(field).query(c)?;
            Ok(json!({"value": n != 0}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__hincrby(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let field = need_str(&v, "field")?;
        let by = need_i64(&v, "by")?;
        let float = v["float"].as_bool().unwrap_or(false);
        with_conn(&v, |c| {
            if float {
                let by_f = v["by"].as_f64().unwrap_or(by as f64);
                let n: f64 = redis::cmd("HINCRBYFLOAT")
                    .arg(key)
                    .arg(field)
                    .arg(by_f)
                    .query(c)?;
                Ok(json!({"value": n}))
            } else {
                let n: i64 = redis::cmd("HINCRBY").arg(key).arg(field).arg(by).query(c)?;
                Ok(json!({"value": n}))
            }
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__hlen(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("HLEN").arg(key).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__hsetnx(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let field = need_str(&v, "field")?;
        let value = need_str(&v, "value")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("HSETNX")
                .arg(key)
                .arg(field)
                .arg(value)
                .query(c)?;
            Ok(json!({"value": n != 0}))
        })
    })
}

// ── set extras ───────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__spop(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let count = v["count"].as_i64();
        with_conn(&v, |c| match count {
            Some(n) => {
                let vals: Vec<String> = redis::cmd("SPOP").arg(key).arg(n).query(c)?;
                Ok(json!({"members": vals}))
            }
            None => {
                let val: Option<String> = redis::cmd("SPOP").arg(key).query(c)?;
                Ok(json!({"value": val}))
            }
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__srandmember(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let count = v["count"].as_i64();
        with_conn(&v, |c| match count {
            Some(n) => {
                let vals: Vec<String> = redis::cmd("SRANDMEMBER").arg(key).arg(n).query(c)?;
                Ok(json!({"members": vals}))
            }
            None => {
                let val: Option<String> = redis::cmd("SRANDMEMBER").arg(key).query(c)?;
                Ok(json!({"value": val}))
            }
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__smove(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let source = need_str(&v, "source")?;
        let dest = need_str(&v, "destination")?;
        let member = need_str(&v, "member")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("SMOVE")
                .arg(source)
                .arg(dest)
                .arg(member)
                .query(c)?;
            Ok(json!({"value": n != 0}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__sinter(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| set_combine(&v, "SINTER"))
}

#[no_mangle]
pub extern "C" fn redis__sunion(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| set_combine(&v, "SUNION"))
}

#[no_mangle]
pub extern "C" fn redis__sdiff(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| set_combine(&v, "SDIFF"))
}

// ── sorted set extras ────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__zincrby(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let by = v["by"].as_f64().ok_or_else(|| anyhow!("missing by"))?;
        let member = need_str(&v, "member")?;
        with_conn(&v, |c| {
            let n: f64 = redis::cmd("ZINCRBY")
                .arg(key)
                .arg(by)
                .arg(member)
                .query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__zrank(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let member = need_str(&v, "member")?;
        let rev = v["rev"].as_bool().unwrap_or(false);
        with_conn(&v, |c| {
            let n: Option<i64> = redis::cmd(if rev { "ZREVRANK" } else { "ZRANK" })
                .arg(key)
                .arg(member)
                .query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__zcount(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let min = arg_str(&v["min"]).unwrap_or_else(|| "-inf".to_string());
        let max = arg_str(&v["max"]).unwrap_or_else(|| "+inf".to_string());
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("ZCOUNT").arg(key).arg(min).arg(max).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__zrangebyscore(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let min = arg_str(&v["min"]).unwrap_or_else(|| "-inf".to_string());
        let max = arg_str(&v["max"]).unwrap_or_else(|| "+inf".to_string());
        let with_scores = v["with_scores"].as_bool().unwrap_or(false);
        let rev = v["rev"].as_bool().unwrap_or(false);
        let limit_offset = v["limit_offset"].as_i64();
        let limit_count = v["limit_count"].as_i64();
        with_conn(&v, |c| {
            let mut cmd = redis::cmd(if rev {
                "ZREVRANGEBYSCORE"
            } else {
                "ZRANGEBYSCORE"
            });
            if rev {
                cmd.arg(key).arg(max).arg(min);
            } else {
                cmd.arg(key).arg(min).arg(max);
            }
            if with_scores {
                cmd.arg("WITHSCORES");
            }
            if let (Some(o), Some(n)) = (limit_offset, limit_count) {
                cmd.arg("LIMIT").arg(o).arg(n);
            }
            if with_scores {
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
pub extern "C" fn redis__zpopmin(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| zpop(&v, "ZPOPMIN"))
}

#[no_mangle]
pub extern "C" fn redis__zpopmax(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| zpop(&v, "ZPOPMAX"))
}

#[no_mangle]
pub extern "C" fn redis__zremrangebyrank(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let start = need_i64(&v, "start")?;
        let stop = need_i64(&v, "stop")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("ZREMRANGEBYRANK")
                .arg(key)
                .arg(start)
                .arg(stop)
                .query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__zremrangebyscore(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let min = arg_str(&v["min"]).unwrap_or_else(|| "-inf".to_string());
        let max = arg_str(&v["max"]).unwrap_or_else(|| "+inf".to_string());
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("ZREMRANGEBYSCORE")
                .arg(key)
                .arg(min)
                .arg(max)
                .query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__zmscore(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let members = string_vec(&v["members"])?;
        with_conn(&v, |c| {
            let scores: Vec<Option<f64>> = redis::cmd("ZMSCORE").arg(key).arg(&members).query(c)?;
            Ok(json!({"values": scores}))
        })
    })
}

// ── HyperLogLog ──────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__pfadd(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let elements = string_vec(&v["elements"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("PFADD").arg(key).arg(&elements).query(c)?;
            Ok(json!({"value": n != 0}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__pfcount(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let keys = string_vec(&v["keys"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("PFCOUNT").arg(&keys).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__pfmerge(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let dest = need_str(&v, "destination")?;
        let sources = string_vec(&v["sources"])?;
        with_conn(&v, |c| {
            let r: String = redis::cmd("PFMERGE").arg(dest).arg(&sources).query(c)?;
            Ok(json!({"ok": r == "OK"}))
        })
    })
}

// ── streams ──────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__xadd(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let id = v["id"].as_str().unwrap_or("*");
        let fields = v["fields"]
            .as_object()
            .ok_or_else(|| anyhow!("missing fields object"))?;
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("XADD");
            cmd.arg(key).arg(id);
            for (f, val) in fields {
                let s = val
                    .as_str()
                    .map(String::from)
                    .unwrap_or_else(|| val.to_string());
                cmd.arg(f).arg(s);
            }
            let new_id: String = cmd.query(c)?;
            Ok(json!({"id": new_id}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__xlen(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("XLEN").arg(key).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__xrange(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let start = v["start"].as_str().unwrap_or("-");
        let end = v["end"].as_str().unwrap_or("+");
        let count = v["count"].as_i64();
        let rev = v["rev"].as_bool().unwrap_or(false);
        with_conn(&v, |c| {
            let mut cmd = redis::cmd(if rev { "XREVRANGE" } else { "XRANGE" });
            if rev {
                cmd.arg(key).arg(end).arg(start);
            } else {
                cmd.arg(key).arg(start).arg(end);
            }
            if let Some(n) = count {
                cmd.arg("COUNT").arg(n);
            }
            let raw: redis::Value = cmd.query(c)?;
            Ok(json!({"entries": redis_value_to_json(raw)}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__xdel(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let ids = string_vec(&v["ids"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("XDEL").arg(key).arg(&ids).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__xtrim(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let maxlen = v["maxlen"].as_i64();
        let minid = v["minid"].as_str();
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("XTRIM");
            cmd.arg(key);
            if let Some(n) = maxlen {
                cmd.arg("MAXLEN").arg(n);
            } else if let Some(id) = minid {
                cmd.arg("MINID").arg(id);
            } else {
                return Err(anyhow!("xtrim needs maxlen or minid"));
            }
            let n: i64 = cmd.query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__xread(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let streams = v["streams"]
            .as_object()
            .ok_or_else(|| anyhow!("missing streams object (key->id)"))?;
        let count = v["count"].as_i64();
        let block = v["block"].as_i64();
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("XREAD");
            if let Some(n) = count {
                cmd.arg("COUNT").arg(n);
            }
            if let Some(ms) = block {
                cmd.arg("BLOCK").arg(ms);
            }
            cmd.arg("STREAMS");
            for k in streams.keys() {
                cmd.arg(k.as_str());
            }
            for id in streams.values() {
                cmd.arg(id.as_str().unwrap_or("$"));
            }
            let raw: redis::Value = cmd.query(c)?;
            Ok(json!({"streams": redis_value_to_json(raw)}))
        })
    })
}

// ── geospatial ───────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__geoadd(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let members = v["members"]
            .as_array()
            .ok_or_else(|| anyhow!("missing members [[lon,lat,name],...]"))?;
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("GEOADD");
            cmd.arg(key);
            for m in members {
                let t = m
                    .as_array()
                    .ok_or_else(|| anyhow!("member must be [lon,lat,name]"))?;
                if t.len() != 3 {
                    return Err(anyhow!("member must be [lon,lat,name]"));
                }
                let lon = t[0].as_f64().ok_or_else(|| anyhow!("bad lon"))?;
                let lat = t[1].as_f64().ok_or_else(|| anyhow!("bad lat"))?;
                let name = t[2].as_str().ok_or_else(|| anyhow!("bad member name"))?;
                cmd.arg(lon).arg(lat).arg(name);
            }
            let n: i64 = cmd.query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__geopos(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let members = string_vec(&v["members"])?;
        with_conn(&v, |c| {
            let raw: redis::Value = redis::cmd("GEOPOS").arg(key).arg(&members).query(c)?;
            Ok(json!({"positions": redis_value_to_json(raw)}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__geodist(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let m1 = need_str(&v, "member1")?;
        let m2 = need_str(&v, "member2")?;
        let unit = v["unit"].as_str().unwrap_or("m");
        with_conn(&v, |c| {
            let d: Option<f64> = redis::cmd("GEODIST")
                .arg(key)
                .arg(m1)
                .arg(m2)
                .arg(unit)
                .query(c)?;
            Ok(json!({"value": d}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__geosearch(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("GEOSEARCH");
            cmd.arg(key);
            if let Some(m) = v["from_member"].as_str() {
                cmd.arg("FROMMEMBER").arg(m);
            } else if let (Some(lon), Some(lat)) = (v["lon"].as_f64(), v["lat"].as_f64()) {
                cmd.arg("FROMLONLAT").arg(lon).arg(lat);
            } else {
                return Err(anyhow!("geosearch needs from_member or lon/lat"));
            }
            if let Some(r) = v["radius"].as_f64() {
                cmd.arg("BYRADIUS")
                    .arg(r)
                    .arg(v["unit"].as_str().unwrap_or("m"));
            } else if let (Some(w), Some(h)) = (v["width"].as_f64(), v["height"].as_f64()) {
                cmd.arg("BYBOX")
                    .arg(w)
                    .arg(h)
                    .arg(v["unit"].as_str().unwrap_or("m"));
            } else {
                return Err(anyhow!("geosearch needs radius or width/height"));
            }
            if let Some(n) = v["count"].as_i64() {
                cmd.arg("COUNT").arg(n);
            }
            if v["with_coord"].as_bool().unwrap_or(false) {
                cmd.arg("WITHCOORD");
            }
            if v["with_dist"].as_bool().unwrap_or(false) {
                cmd.arg("WITHDIST");
            }
            let raw: redis::Value = cmd.query(c)?;
            Ok(json!({"results": redis_value_to_json(raw)}))
        })
    })
}

// ── scripting ────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__eval(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| eval_impl(&v, "EVAL", "script"))
}

#[no_mangle]
pub extern "C" fn redis__evalsha(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| eval_impl(&v, "EVALSHA", "sha"))
}

#[no_mangle]
pub extern "C" fn redis__script_load(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let script = need_str(&v, "script")?;
        with_conn(&v, |c| {
            let sha: String = redis::cmd("SCRIPT").arg("LOAD").arg(script).query(c)?;
            Ok(json!({"sha": sha}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__script_exists(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let shas = string_vec(&v["shas"])?;
        with_conn(&v, |c| {
            let exists: Vec<i64> = redis::cmd("SCRIPT").arg("EXISTS").arg(&shas).query(c)?;
            Ok(json!({"values": exists.iter().map(|n| *n != 0).collect::<Vec<bool>>()}))
        })
    })
}

// ── pub/sub introspection ────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__pubsub_channels(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let pattern = v["pattern"].as_str();
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("PUBSUB");
            cmd.arg("CHANNELS");
            if let Some(p) = pattern {
                cmd.arg(p);
            }
            let chans: Vec<String> = cmd.query(c)?;
            Ok(json!({"channels": chans}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__pubsub_numsub(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let channels = string_vec(&v["channels"])?;
        with_conn(&v, |c| {
            let pairs: Vec<(String, i64)> =
                redis::cmd("PUBSUB").arg("NUMSUB").arg(&channels).query(c)?;
            let map: serde_json::Map<String, Value> =
                pairs.into_iter().map(|(k, n)| (k, json!(n))).collect();
            Ok(json!({"counts": map}))
        })
    })
}

// ── server admin extras ──────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__flushall(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let asynchronous = v["asynchronous"].as_bool().unwrap_or(false);
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("FLUSHALL");
            if asynchronous {
                cmd.arg("ASYNC");
            }
            let r: String = cmd.query(c)?;
            Ok(json!({"ok": r == "OK"}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__time(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            let t: (i64, i64) = redis::cmd("TIME").query(c)?;
            Ok(json!({"seconds": t.0, "microseconds": t.1}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__config_get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let parameter = need_str(&v, "parameter")?;
        with_conn(&v, |c| {
            let pairs: Vec<(String, String)> =
                redis::cmd("CONFIG").arg("GET").arg(parameter).query(c)?;
            let map: serde_json::Map<String, Value> = pairs
                .into_iter()
                .map(|(k, val)| (k, Value::String(val)))
                .collect();
            Ok(json!({"config": map}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__config_set(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let parameter = need_str(&v, "parameter")?;
        let value = need_str(&v, "value")?;
        with_conn(&v, |c| {
            let r: String = redis::cmd("CONFIG")
                .arg("SET")
                .arg(parameter)
                .arg(value)
                .query(c)?;
            Ok(json!({"ok": r == "OK"}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__memory_usage(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        with_conn(&v, |c| {
            let n: Option<i64> = redis::cmd("MEMORY").arg("USAGE").arg(key).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__echo(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let message = need_str(&v, "message")?;
        with_conn(&v, |c| {
            let r: String = redis::cmd("ECHO").arg(message).query(c)?;
            Ok(json!({"value": r}))
        })
    })
}

// ── pipeline / transaction ───────────────────────────────────────────────────

/// Run a batch of commands in one round-trip. `commands` is an array of argv
/// arrays (`[["SET","k","v"],["GET","k"]]`). When `transaction` is true the
/// batch runs inside MULTI/EXEC (atomic). Returns one JSON result per command.
#[no_mangle]
pub extern "C" fn redis__pipeline(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let cmds = v["commands"]
            .as_array()
            .ok_or_else(|| anyhow!("missing commands array of argv arrays"))?;
        let atomic = v["transaction"].as_bool().unwrap_or(false);
        let mut pipe = redis::pipe();
        if atomic {
            pipe.atomic();
        }
        for entry in cmds {
            let argv = string_vec(entry)?;
            if argv.is_empty() {
                return Err(anyhow!(
                    "pipeline: each command must be a non-empty argv array"
                ));
            }
            let cmd = pipe.cmd(&argv[0]);
            for a in &argv[1..] {
                cmd.arg(a);
            }
        }
        with_conn(&v, |c| {
            let results: Vec<redis::Value> = pipe.query(c)?;
            let out: Vec<Value> = results.into_iter().map(redis_value_to_json).collect();
            Ok(json!({"results": out}))
        })
    })
}

// ── server admin + introspection ─────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__wait(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let numreplicas = need_i64(&v, "numreplicas")?;
        let timeout = v["timeout"].as_i64().unwrap_or(0);
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("WAIT").arg(numreplicas).arg(timeout).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__lastsave(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("LASTSAVE").query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__slowlog_get(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let count = v["count"].as_i64();
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("SLOWLOG");
            cmd.arg("GET");
            if let Some(n) = count {
                cmd.arg(n);
            }
            let raw: redis::Value = cmd.query(c)?;
            Ok(json!({"entries": redis_value_to_json(raw)}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__slowlog_reset(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            let r: String = redis::cmd("SLOWLOG").arg("RESET").query(c)?;
            Ok(json!({"ok": r == "OK"}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__client_list(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            let s: String = redis::cmd("CLIENT").arg("LIST").query(c)?;
            Ok(json!({"value": s}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__client_info(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            let s: String = redis::cmd("CLIENT").arg("INFO").query(c)?;
            Ok(json!({"value": s}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__acl_whoami(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            let s: String = redis::cmd("ACL").arg("WHOAMI").query(c)?;
            Ok(json!({"value": s}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__acl_list(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            let rules: Vec<String> = redis::cmd("ACL").arg("LIST").query(c)?;
            Ok(json!({"rules": rules}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__acl_cat(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let category = v["category"].as_str();
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("ACL");
            cmd.arg("CAT");
            if let Some(cat) = category {
                cmd.arg(cat);
            }
            let cats: Vec<String> = cmd.query(c)?;
            Ok(json!({"categories": cats}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__object_idletime(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        with_conn(&v, |c| {
            let n: Option<i64> = redis::cmd("OBJECT").arg("IDLETIME").arg(key).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__object_refcount(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        with_conn(&v, |c| {
            let n: Option<i64> = redis::cmd("OBJECT").arg("REFCOUNT").arg(key).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

// ── redis 6.2 / 7.x commands ─────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__getex(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let ex = v["ex"].as_i64();
        let px = v["px"].as_i64();
        let persist = v["persist"].as_bool().unwrap_or(false);
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("GETEX");
            cmd.arg(key);
            if let Some(s) = ex {
                cmd.arg("EX").arg(s);
            } else if let Some(ms) = px {
                cmd.arg("PX").arg(ms);
            } else if persist {
                cmd.arg("PERSIST");
            }
            let val: Option<String> = cmd.query(c)?;
            Ok(json!({"value": val}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__smismember(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let members = string_vec(&v["members"])?;
        with_conn(&v, |c| {
            let bits: Vec<i64> = redis::cmd("SMISMEMBER").arg(key).arg(&members).query(c)?;
            Ok(json!({"values": bits.iter().map(|n| *n != 0).collect::<Vec<bool>>()}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__sintercard(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let keys = string_vec(&v["keys"])?;
        let limit = v["limit"].as_i64();
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("SINTERCARD");
            cmd.arg(keys.len()).arg(&keys);
            if let Some(l) = limit {
                cmd.arg("LIMIT").arg(l);
            }
            let n: i64 = cmd.query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__lpos(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let element = need_str(&v, "element")?;
        let rank = v["rank"].as_i64();
        let count = v["count"].as_i64();
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("LPOS");
            cmd.arg(key).arg(element);
            if let Some(r) = rank {
                cmd.arg("RANK").arg(r);
            }
            if let Some(n) = count {
                // COUNT returns an array of positions (0 = all matches).
                cmd.arg("COUNT").arg(n);
                let positions: Vec<i64> = cmd.query(c)?;
                Ok(json!({"values": positions}))
            } else {
                let pos: Option<i64> = cmd.query(c)?;
                Ok(json!({"value": pos}))
            }
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__lmpop(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let keys = string_vec(&v["keys"])?;
        let from = if v["from"].as_str() == Some("RIGHT") {
            "RIGHT"
        } else {
            "LEFT"
        };
        let count = v["count"].as_i64();
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("LMPOP");
            cmd.arg(keys.len()).arg(&keys).arg(from);
            if let Some(n) = count {
                cmd.arg("COUNT").arg(n);
            }
            // Reply: [key, [elem, ...]] or nil.
            let raw: redis::Value = cmd.query(c)?;
            Ok(json!({"value": redis_value_to_json(raw)}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__zmpop(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let keys = string_vec(&v["keys"])?;
        let from = if v["from"].as_str() == Some("MAX") {
            "MAX"
        } else {
            "MIN"
        };
        let count = v["count"].as_i64();
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("ZMPOP");
            cmd.arg(keys.len()).arg(&keys).arg(from);
            if let Some(n) = count {
                cmd.arg("COUNT").arg(n);
            }
            let raw: redis::Value = cmd.query(c)?;
            Ok(json!({"value": redis_value_to_json(raw)}))
        })
    })
}

// ── helpers ─────────────────────────────────────────────────────────────────

/// Fetch a required string field, erroring with the field name when absent.
fn need_str<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v[key].as_str().ok_or_else(|| anyhow!("missing {}", key))
}

/// Fetch a required integer field, erroring with the field name when absent.
fn need_i64(v: &Value, key: &str) -> Result<i64> {
    v[key].as_i64().ok_or_else(|| anyhow!("missing {}", key))
}

/// Render a JSON scalar as a Redis argument string. Accepts strings verbatim
/// and stringifies numbers — used for score bounds like `"(1"`, `"-inf"`, `5`.
fn arg_str(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// SINTER / SUNION / SDIFF share one shape: keys in, member array out.
fn set_combine(v: &Value, op: &str) -> Result<Value> {
    let keys = string_vec(&v["keys"])?;
    with_conn(v, |c| {
        let members: Vec<String> = redis::cmd(op).arg(&keys).query(c)?;
        Ok(json!({ "members": members }))
    })
}

/// ZPOPMIN / ZPOPMAX share one shape: optional count, returns [member, score] pairs.
fn zpop(v: &Value, op: &str) -> Result<Value> {
    let key = need_str(v, "key")?;
    let count = v["count"].as_i64();
    with_conn(v, |c| {
        let mut cmd = redis::cmd(op);
        cmd.arg(key);
        if let Some(n) = count {
            cmd.arg(n);
        }
        // Reply is a flat [member, score, member, score, ...] array.
        let flat: Vec<String> = cmd.query(c)?;
        let pairs: Vec<(String, String)> = flat
            .chunks(2)
            .filter(|ch| ch.len() == 2)
            .map(|ch| (ch[0].clone(), ch[1].clone()))
            .collect();
        Ok(json!({ "pairs": pairs }))
    })
}

/// EVAL / EVALSHA share one shape: script-or-sha, keys[], args[], dynamic reply.
fn eval_impl(v: &Value, op: &str, script_field: &str) -> Result<Value> {
    let script = need_str(v, script_field)?;
    let keys = string_vec(&v["keys"])?;
    let argv = string_vec(&v["args"])?;
    with_conn(v, |c| {
        let mut cmd = redis::cmd(op);
        cmd.arg(script).arg(keys.len());
        for k in &keys {
            cmd.arg(k);
        }
        for a in &argv {
            cmd.arg(a);
        }
        let raw: redis::Value = cmd.query(c)?;
        Ok(json!({ "value": redis_value_to_json(raw) }))
    })
}

/// Accept either a JSON array of strings or a single string, return Vec<String>.
fn string_vec(v: &Value) -> Result<Vec<String>> {
    match v {
        Value::Array(a) => a
            .iter()
            .map(|x| {
                x.as_str()
                    .map(String::from)
                    .ok_or_else(|| anyhow!("non-string in array"))
            })
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
        redis::Value::Array(arr) => {
            Value::Array(arr.into_iter().map(redis_value_to_json).collect())
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── url_from_opts / ConnKey ──

    #[test]
    fn url_explicit_url_field_kept_verbatim() {
        let k = url_from_opts(&json!({"url": "redis://h:6380/2"}));
        assert_eq!(k.url, "redis://h:6380/2");
    }

    #[test]
    fn url_default_built_with_redis_scheme() {
        let k = url_from_opts(&json!({}));
        assert_eq!(k.url, "redis://127.0.0.1:6379/0");
        assert!(!k.tls);
    }

    #[test]
    fn url_tls_picks_rediss_scheme() {
        let k = url_from_opts(&json!({"tls": true}));
        assert!(k.url.starts_with("rediss://"), "{}", k.url);
        assert!(k.tls);
    }

    #[test]
    fn url_password_only_uses_colon_prefix() {
        // Redis ACL: `:password@host` means default-user auth.
        let k = url_from_opts(&json!({"password": "hunter2"}));
        assert!(k.url.contains(":hunter2@"), "{}", k.url);
        assert!(!k.url.contains("@:"), "{}", k.url);
    }

    #[test]
    fn url_username_and_password() {
        let k = url_from_opts(&json!({"username": "ada", "password": "hunter2"}));
        assert!(k.url.contains("ada:hunter2@"), "{}", k.url);
    }

    #[test]
    fn url_no_auth_segment_when_both_blank() {
        let k = url_from_opts(&json!({}));
        assert!(!k.url.contains('@'), "{}", k.url);
    }

    #[test]
    fn url_host_port_db_overrides() {
        let k = url_from_opts(&json!({"host": "h.example", "port": 16379, "db": 7}));
        assert_eq!(k.url, "redis://h.example:16379/7");
        assert_eq!(k.db, 7);
    }

    #[test]
    fn conn_key_eq_distinguishes_db() {
        let a = url_from_opts(&json!({"db": 0}));
        let b = url_from_opts(&json!({"db": 1}));
        assert_ne!(a, b);
    }

    #[test]
    fn conn_key_eq_distinguishes_credentials() {
        let a = url_from_opts(&json!({"password": "x"}));
        let b = url_from_opts(&json!({"password": "y"}));
        assert_ne!(a, b);
    }

    // ── string_vec ──

    #[test]
    fn sv_array_of_strings() {
        let v = json!(["a", "b", "c"]);
        assert_eq!(string_vec(&v).unwrap(), vec!["a", "b", "c"]);
    }

    #[test]
    fn sv_single_string_wraps_in_singleton_vec() {
        let v = json!("only");
        assert_eq!(string_vec(&v).unwrap(), vec!["only"]);
    }

    #[test]
    fn sv_null_yields_empty_vec() {
        assert!(string_vec(&Value::Null).unwrap().is_empty());
    }

    #[test]
    fn sv_array_with_non_string_errors() {
        let v = json!(["a", 42, "c"]);
        let err = string_vec(&v).unwrap_err().to_string();
        assert!(err.contains("non-string"), "{err}");
    }

    #[test]
    fn sv_number_or_bool_errors() {
        assert!(string_vec(&json!(42)).is_err());
        assert!(string_vec(&json!(true)).is_err());
        assert!(string_vec(&json!({"k":"v"})).is_err());
    }

    // ── redis_value_to_json ──

    #[test]
    fn rv2j_nil() {
        assert_eq!(redis_value_to_json(redis::Value::Nil), Value::Null);
    }

    #[test]
    fn rv2j_int() {
        assert_eq!(redis_value_to_json(redis::Value::Int(42)), json!(42));
        assert_eq!(redis_value_to_json(redis::Value::Int(-7)), json!(-7));
    }

    #[test]
    fn rv2j_bulk_utf8_string() {
        let v = redis_value_to_json(redis::Value::BulkString(b"hello".to_vec()));
        assert_eq!(v, json!("hello"));
    }

    #[test]
    fn rv2j_bulk_non_utf8_marker() {
        let v = redis_value_to_json(redis::Value::BulkString(vec![0xFF, 0xFE, 0xFD]));
        assert_eq!(v, json!("<binary 3 bytes>"));
    }

    #[test]
    fn rv2j_simple_string_and_okay() {
        assert_eq!(
            redis_value_to_json(redis::Value::SimpleString("PONG".into())),
            json!("PONG")
        );
        assert_eq!(redis_value_to_json(redis::Value::Okay), json!("OK"));
    }

    #[test]
    fn rv2j_array_recurses() {
        let v = redis_value_to_json(redis::Value::Array(vec![
            redis::Value::Int(1),
            redis::Value::BulkString(b"two".to_vec()),
        ]));
        assert_eq!(v, json!([1, "two"]));
    }

    #[test]
    fn rv2j_map_string_keys() {
        let v = redis_value_to_json(redis::Value::Map(vec![
            (
                redis::Value::SimpleString("k1".into()),
                redis::Value::Int(1),
            ),
            (
                redis::Value::BulkString(b"k2".to_vec()),
                redis::Value::BulkString(b"v2".to_vec()),
            ),
        ]));
        assert_eq!(v["k1"], json!(1));
        assert_eq!(v["k2"], json!("v2"));
    }

    // ── new hand-crafted bug-class catchers ──

    /// `redis_value_to_json` is the only path the `raw` escape hatch has for
    /// reflecting arbitrary RESP replies. A Map → Array → BulkString chain is
    /// the exact shape returned by HGETALL via RAW, by XRANGE entries, and by
    /// many Stream / cluster commands. A future "fast path" refactor that
    /// flattens or collapses any single layer would silently corrupt the JSON
    /// the stryke caller sees. Test pins that each layer is preserved
    /// AND that the inner BulkString bytes survive recursion intact.
    #[test]
    fn rv2j_three_deep_map_array_bulk_preserves_structure() {
        let v = redis_value_to_json(redis::Value::Map(vec![(
            redis::Value::SimpleString("entries".into()),
            redis::Value::Array(vec![
                redis::Value::BulkString(b"a".to_vec()),
                redis::Value::Array(vec![
                    redis::Value::Int(7),
                    redis::Value::BulkString(b"deep".to_vec()),
                ]),
            ]),
        )]));
        // Map preserved as object, array preserved as array, bulk preserved as string,
        // recursion descends into the inner array (not stringified via Debug).
        assert_eq!(v["entries"][0], json!("a"));
        assert_eq!(v["entries"][1], json!([7, "deep"]));
        // Defensive: if a future refactor falls through to the `other =>` Debug
        // arm at any layer, the result would contain the literal "Array(" or
        // "BulkString(" tokens. Reject that.
        let s = serde_json::to_string(&v).unwrap();
        assert!(!s.contains("Array("), "Debug-formatted leak: {s}");
        assert!(!s.contains("BulkString("), "Debug-formatted leak: {s}");
        assert!(!s.contains("SimpleString("), "Debug-formatted leak: {s}");
    }

    /// Redis strings are binary-safe: keys and values can contain NUL (`0x00`)
    /// bytes. Because this cdylib hands strings back to stryke through C
    /// strings via `CString::new` in `ffi_call`, any path that converts a Redis
    /// bulk reply via `String::from_utf8` and then re-serializes must NOT lose
    /// or truncate at the embedded NUL — the NUL only matters at the final
    /// `CString::new` boundary, which is responsible for rejecting NULs. This
    /// test pins that the JSON-encoded form of a bulk string with an embedded
    /// NUL is a faithful JSON string (length preserved, NUL JSON-escaped as
    /// ` `), not the `<binary N bytes>` fallback (which would mean a
    /// regression replaced `from_utf8` with stricter validation).
    #[test]
    fn rv2j_bulk_with_embedded_nul_is_round_tripped_as_json_string() {
        let v = redis_value_to_json(redis::Value::BulkString(b"a\0b".to_vec()));
        // Must round-trip via JSON; serde_json will escape   in the output.
        let s = serde_json::to_string(&v).unwrap();
        assert_eq!(s, "\"a\\u0000b\"", "actual: {s}");
        // And the decoded value must be the original 3-byte string.
        assert_eq!(v.as_str().map(str::len), Some(3));
        assert!(!s.contains("<binary"), "fell through to non-utf8 fallback");
    }

    /// `string_vec` must not silently flatten or stringify nested arrays —
    /// stryke callers passing `[["a","b"]]` (one bad layer of wrapping) should
    /// get an error, not the surprise singleton `[\"[\\\"a\\\",\\\"b\\\"]\"]` or
    /// the silent flattening `[\"a\",\"b\"]`. Without this pin, a future "be
    /// lenient about input shape" refactor could introduce a write-amplification
    /// bug where `DEL [["k1","k2"]]` deletes one key named `["k1","k2"]`
    /// instead of two keys, or vice versa.
    #[test]
    fn sv_nested_array_errors_not_flattens_or_stringifies() {
        let v = json!([["a", "b"]]);
        let err = string_vec(&v).unwrap_err().to_string();
        assert!(err.contains("non-string"), "{err}");
    }

    /// ConnKey participates in connection-cache lookup. Two opts dicts that
    /// differ only in host MUST map to different ConnKeys — otherwise stryke
    /// would reuse a localhost connection for a remote target (or vice versa).
    /// `db`/`credentials` are already pinned; `host` and `port` are not —
    /// they're folded into the derived URL string but a future refactor that
    /// adds explicit `host`/`port` fields to ConnKey and forgets to fall back
    /// to them when the `url` field is present (or vice versa) would silently
    /// break cache isolation. Pin both at once.
    #[test]
    fn conn_key_distinguishes_host_and_port_independently() {
        let base = url_from_opts(&json!({"host": "h1", "port": 6379}));
        let other_host = url_from_opts(&json!({"host": "h2", "port": 6379}));
        let other_port = url_from_opts(&json!({"host": "h1", "port": 6380}));
        assert_ne!(
            base, other_host,
            "host collision: {base:?} vs {other_host:?}"
        );
        assert_ne!(
            base, other_port,
            "port collision: {base:?} vs {other_port:?}"
        );
        // And differing url-field strings must also produce distinct keys.
        let url_a = url_from_opts(&json!({"url": "redis://h1:6379/0"}));
        let url_b = url_from_opts(&json!({"url": "redis://h2:6379/0"}));
        assert_ne!(url_a, url_b);
    }

    /// `string_vec` accepts 3 shapes the callers exercise: a JSON array of
    /// strings, a single bare string (auto-wrapped), and null (empty).
    /// Anything else errors. Pin so a refactor that "helpfully" stringifies
    /// numbers or strips nulls from arrays gets caught.
    #[test]
    fn string_vec_array_of_strings_round_trips() {
        let v = string_vec(&json!(["a", "b", "c"])).unwrap();
        assert_eq!(v, vec!["a", "b", "c"]);
    }

    #[test]
    fn string_vec_bare_string_auto_wraps_to_single_element() {
        // Caller convenience: `del("foo")` works the same as `del(["foo"])`.
        let v = string_vec(&json!("foo")).unwrap();
        assert_eq!(v, vec!["foo"]);
    }

    #[test]
    fn string_vec_null_yields_empty() {
        let v = string_vec(&Value::Null).unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn string_vec_array_with_non_string_errors() {
        let err = string_vec(&json!(["a", 42, "b"])).unwrap_err().to_string();
        assert!(err.contains("non-string"), "got: {err}");
    }

    #[test]
    fn string_vec_non_array_non_string_errors() {
        let err = string_vec(&json!({"k": "v"})).unwrap_err().to_string();
        assert!(err.contains("expected string"), "got: {err}");
    }

    // ── arg_str / need_str / need_i64 (new helpers) ──

    /// Score-bound args reach Redis as strings. A JSON number like `5` must
    /// stringify to `"5"` (not `"5.0"`), and a string bound like `"(1"` or
    /// `"-inf"` must pass through verbatim — these encode exclusive/open ranges
    /// for ZRANGEBYSCORE/ZCOUNT. A regression that formatted integers as floats
    /// would make `ZCOUNT k 5 10` send `5.0`, which Redis rejects.
    #[test]
    fn arg_str_number_stringifies_without_decimal_point() {
        assert_eq!(arg_str(&json!(5)).as_deref(), Some("5"));
        assert_eq!(arg_str(&json!(-3)).as_deref(), Some("-3"));
        assert_eq!(arg_str(&json!("(1")).as_deref(), Some("(1"));
        assert_eq!(arg_str(&json!("-inf")).as_deref(), Some("-inf"));
    }

    /// Non-scalar JSON has no Redis-arg representation; `arg_str` returns None
    /// so callers fall back to their default bound rather than sending garbage.
    #[test]
    fn arg_str_rejects_non_scalar() {
        assert_eq!(arg_str(&json!(["a"])), None);
        assert_eq!(arg_str(&json!({"k": "v"})), None);
        assert_eq!(arg_str(&Value::Null), None);
    }

    /// The error must name the missing field so a stryke-side `Redis::*` die
    /// message points at the right argument, not a generic "missing".
    #[test]
    fn need_str_error_names_the_field() {
        let err = need_str(&json!({}), "destination").unwrap_err().to_string();
        assert!(err.contains("destination"), "got: {err}");
        assert_eq!(need_str(&json!({"key": "k"}), "key").unwrap(), "k");
    }

    #[test]
    fn need_i64_error_names_the_field_and_parses_present() {
        let err = need_i64(&json!({}), "offset").unwrap_err().to_string();
        assert!(err.contains("offset"), "got: {err}");
        assert_eq!(need_i64(&json!({"seconds": 42}), "seconds").unwrap(), 42);
    }

    /// A JSON float in an integer slot must NOT silently truncate — `as_i64`
    /// returns None for `3.5`, so `need_i64` errors rather than sending `3`.
    /// Pins that PEXPIRE/SETEX millis can't be quietly corrupted by a float.
    #[test]
    fn need_i64_rejects_non_integer_number() {
        assert!(need_i64(&json!({"seconds": 3.5}), "seconds").is_err());
    }
}
