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

// ── cursor scans (hash / set / zset) ─────────────────────────────────────────

#[no_mangle]
pub extern "C" fn redis__hscan(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let match_pat = v["match"].as_str();
        let count = v["count"].as_i64();
        with_conn(&v, |c| {
            let mut cur: u64 = 0;
            let mut map = serde_json::Map::new();
            loop {
                let mut cmd = redis::cmd("HSCAN");
                cmd.arg(key).arg(cur);
                if let Some(p) = match_pat {
                    cmd.arg("MATCH").arg(p);
                }
                if let Some(n) = count {
                    cmd.arg("COUNT").arg(n);
                }
                // Reply: (next_cursor, [field, value, field, value, ...]).
                let (next, flat): (u64, Vec<String>) = cmd.query(c)?;
                for pair in flat.chunks(2) {
                    if pair.len() == 2 {
                        map.insert(pair[0].clone(), Value::String(pair[1].clone()));
                    }
                }
                cur = next;
                if cur == 0 {
                    break;
                }
            }
            Ok(json!({ "hash": map }))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__sscan(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let match_pat = v["match"].as_str();
        let count = v["count"].as_i64();
        with_conn(&v, |c| {
            let mut cur: u64 = 0;
            let mut out: Vec<String> = Vec::new();
            loop {
                let mut cmd = redis::cmd("SSCAN");
                cmd.arg(key).arg(cur);
                if let Some(p) = match_pat {
                    cmd.arg("MATCH").arg(p);
                }
                if let Some(n) = count {
                    cmd.arg("COUNT").arg(n);
                }
                let (next, batch): (u64, Vec<String>) = cmd.query(c)?;
                out.extend(batch);
                cur = next;
                if cur == 0 {
                    break;
                }
            }
            Ok(json!({ "members": out }))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__zscan(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let match_pat = v["match"].as_str();
        let count = v["count"].as_i64();
        with_conn(&v, |c| {
            let mut cur: u64 = 0;
            let mut pairs: Vec<(String, String)> = Vec::new();
            loop {
                let mut cmd = redis::cmd("ZSCAN");
                cmd.arg(key).arg(cur);
                if let Some(p) = match_pat {
                    cmd.arg("MATCH").arg(p);
                }
                if let Some(n) = count {
                    cmd.arg("COUNT").arg(n);
                }
                // Reply: (next, [member, score, member, score, ...]).
                let (next, flat): (u64, Vec<String>) = cmd.query(c)?;
                for ch in flat.chunks(2) {
                    if ch.len() == 2 {
                        pairs.push((ch[0].clone(), ch[1].clone()));
                    }
                }
                cur = next;
                if cur == 0 {
                    break;
                }
            }
            Ok(json!({ "pairs": pairs }))
        })
    })
}

// ── stream consumer groups (non-blocking parts) ──────────────────────────────

#[no_mangle]
pub extern "C" fn redis__xgroup_create(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let group = need_str(&v, "group")?;
        let id = v["id"].as_str().unwrap_or("$");
        let mkstream = v["mkstream"].as_bool().unwrap_or(false);
        with_conn(&v, |c| {
            let mut cmd = redis::cmd("XGROUP");
            cmd.arg("CREATE").arg(key).arg(group).arg(id);
            if mkstream {
                cmd.arg("MKSTREAM");
            }
            let r: String = cmd.query(c)?;
            Ok(json!({"ok": r == "OK"}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__xack(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        let group = need_str(&v, "group")?;
        let ids = string_vec(&v["ids"])?;
        with_conn(&v, |c| {
            let n: i64 = redis::cmd("XACK").arg(key).arg(group).arg(&ids).query(c)?;
            Ok(json!({"value": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn redis__xinfo_stream(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = need_str(&v, "key")?;
        with_conn(&v, |c| {
            let raw: redis::Value = redis::cmd("XINFO").arg("STREAM").arg(key).query(c)?;
            Ok(json!({"info": redis_value_to_json(raw)}))
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

// ── pure helpers (no connection) ─────────────────────────────────────────────

/// Parse a Redis connection URL `redis[s]://[user[:pass]@]host[:port][/db]`
/// into `{scheme, tls, user, password, host, port, db}`. `rediss` sets `tls`.
/// Pure — opens no connection.
fn op_parse_url(v: Value) -> Result<Value> {
    let url = v["url"].as_str().ok_or_else(|| anyhow!("missing url"))?;
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| anyhow!("not a Redis URL (missing `://`): {url}"))?;
    let tls = match scheme {
        "redis" => false,
        "rediss" => true,
        other => return Err(anyhow!("unsupported scheme `{other}` (want redis|rediss)")),
    };
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (rest, None),
    };
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    let (user, password) = match userinfo {
        Some(ui) => match ui.split_once(':') {
            Some((u, p)) => (if u.is_empty() { Value::Null } else { json!(u) }, json!(p)),
            None => (json!(ui), Value::Null),
        },
        None => (Value::Null, Value::Null),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) if !h.is_empty() => (h.to_string(), p.parse::<u32>().ok()),
        _ => (hostport.to_string(), None),
    };
    let host = if host.is_empty() {
        "127.0.0.1".to_string()
    } else {
        host
    };
    let db = path
        .filter(|p| !p.is_empty())
        .and_then(|p| p.parse::<u32>().ok());
    Ok(json!({
        "scheme": scheme,
        "tls": tls,
        "user": user,
        "password": password,
        "host": host,
        "port": port,
        "db": db,
    }))
}

/// Build a Redis connection URL from parts — the inverse of `parse_url`. opts:
/// host (default `127.0.0.1`), port, db, user, password, and `tls` (true →
/// `rediss://`, default `redis://`). Userinfo is emitted as `user:password@`,
/// `:password@`, or `user@` depending on which are present. Pure.
fn op_build_url(v: Value) -> Result<Value> {
    // stryke serializes a truthy flag as the JSON number 1, not a bool, so accept
    // bool true, a nonzero number, or "1"/"true".
    let tls = match v.get("tls") {
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_i64().map(|i| i != 0).unwrap_or(false),
        Some(Value::String(s)) => s == "1" || s.eq_ignore_ascii_case("true"),
        _ => false,
    };
    let scheme = if tls { "rediss" } else { "redis" };
    let host = v.get("host").and_then(Value::as_str).unwrap_or("127.0.0.1");
    let host = if host.is_empty() { "127.0.0.1" } else { host };
    let user = v
        .get("user")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    // Accept password as string or omit; an empty string still counts as set so
    // `:@host` round-trips, but None means no userinfo.
    let password = v.get("password").and_then(Value::as_str);
    let userinfo = match (user, password) {
        (Some(u), Some(p)) => format!("{u}:{p}@"),
        (Some(u), None) => format!("{u}@"),
        (None, Some(p)) => format!(":{p}@"),
        (None, None) => String::new(),
    };
    let port = v.get("port").and_then(Value::as_u64);
    let portseg = port.map(|p| format!(":{p}")).unwrap_or_default();
    let db = v.get("db").and_then(Value::as_u64);
    let dbseg = db.map(|d| format!("/{d}")).unwrap_or_default();
    let url = format!("{scheme}://{userinfo}{host}{portseg}{dbseg}");
    Ok(json!({"url": url}))
}

/// Redact the password in a Redis connection URL so it is safe to log — the
/// password component of the userinfo (`user:password@` or `:password@`) is
/// replaced with `***` while the scheme, user, host, port and db path are kept
/// intact. A URL with no password (or no userinfo) is returned unchanged, as is a
/// value with no `://`. opts: `url` (required). Returns `{url, redacted}`. Pure.
fn op_redact_url(v: Value) -> Result<Value> {
    let url = v["url"].as_str().ok_or_else(|| anyhow!("missing url"))?;
    let redacted = match url.split_once("://") {
        Some((scheme, rest)) => {
            // Only the authority (up to the first `/`) can hold userinfo.
            let (authority, path) = match rest.split_once('/') {
                Some((a, p)) => (a, Some(p)),
                None => (rest, None),
            };
            let new_authority = match authority.rsplit_once('@') {
                Some((userinfo, hostport)) => {
                    let masked = match userinfo.split_once(':') {
                        Some((user, _pass)) => format!("{user}:***"),
                        None => userinfo.to_string(), // user only, no password
                    };
                    format!("{masked}@{hostport}")
                }
                None => authority.to_string(),
            };
            match path {
                Some(p) => format!("{scheme}://{new_authority}/{p}"),
                None => format!("{scheme}://{new_authority}"),
            }
        }
        None => url.to_string(),
    };
    Ok(json!({"url": url, "redacted": redacted}))
}

/// Match a string against a Redis glob pattern — a faithful port of Redis's
/// `stringmatchlen`: `*` (any run), `?` (one char), `[...]` classes with
/// ranges and `[^…]` negation, and `\` escapes. Case-sensitive.
fn glob_match(pat: &[u8], s: &[u8]) -> bool {
    if pat.is_empty() {
        return s.is_empty();
    }
    match pat[0] {
        b'*' => {
            let mut rest = &pat[1..];
            while rest.first() == Some(&b'*') {
                rest = &rest[1..];
            }
            if rest.is_empty() {
                return true;
            }
            (0..=s.len()).any(|i| glob_match(rest, &s[i..]))
        }
        b'?' => !s.is_empty() && glob_match(&pat[1..], &s[1..]),
        b'[' => {
            if s.is_empty() {
                return false;
            }
            let mut i = 1;
            let neg = pat.get(1) == Some(&b'^');
            if neg {
                i = 2;
            }
            let mut matched = false;
            while i < pat.len() && pat[i] != b']' {
                if pat[i] == b'\\' && i + 1 < pat.len() {
                    i += 1;
                    if pat[i] == s[0] {
                        matched = true;
                    }
                    i += 1;
                } else if i + 2 < pat.len() && pat[i + 1] == b'-' && pat[i + 2] != b']' {
                    let (lo, hi) = (pat[i].min(pat[i + 2]), pat[i].max(pat[i + 2]));
                    if s[0] >= lo && s[0] <= hi {
                        matched = true;
                    }
                    i += 3;
                } else {
                    if pat[i] == s[0] {
                        matched = true;
                    }
                    i += 1;
                }
            }
            if i < pat.len() {
                i += 1; // consume the closing ]
            }
            (matched != neg) && glob_match(&pat[i..], &s[1..])
        }
        b'\\' if pat.len() >= 2 => {
            !s.is_empty() && pat[1] == s[0] && glob_match(&pat[2..], &s[1..])
        }
        c => !s.is_empty() && c == s[0] && glob_match(&pat[1..], &s[1..]),
    }
}

/// Test whether `key` matches a Redis `pattern` (KEYS/SCAN glob syntax),
/// client-side. Pure.
fn op_glob_match(v: Value) -> Result<Value> {
    let pattern = v["pattern"]
        .as_str()
        .ok_or_else(|| anyhow!("missing pattern"))?;
    let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
    Ok(
        json!({"pattern": pattern, "key": key, "match": glob_match(pattern.as_bytes(), key.as_bytes())}),
    )
}

/// Escape a literal string so `glob_match` (KEYS/SCAN syntax) matches it
/// verbatim — each glob metacharacter (`*`, `?`, `[`, `]`, `\`) is backslash-
/// prefixed. Use it to build a safe pattern around a key fragment that may
/// itself contain glob characters. `glob_match(glob_escape(s), s)` is always
/// true. Pure.
fn op_glob_escape(v: Value) -> Result<Value> {
    let value = v["value"]
        .as_str()
        .ok_or_else(|| anyhow!("missing value"))?;
    let mut escaped = String::with_capacity(value.len());
    for c in value.chars() {
        if matches!(c, '*' | '?' | '[' | ']' | '\\') {
            escaped.push('\\');
        }
        escaped.push(c);
    }
    Ok(json!({"escaped": escaped}))
}

/// Decode the glob escapes `\*` `\?` `\[` `\]` `\\` back to their literal
/// characters — the inverse of `glob_escape`. A single left-to-right scan, so a
/// `\\` next to a metacharacter is handled correctly; a backslash not
/// introducing one of those escapes is left literal. opts: `value` (or
/// `escaped`). Returns `{value}`. Pure.
fn op_glob_unescape(v: Value) -> Result<Value> {
    let value = v["value"]
        .as_str()
        .or_else(|| v["escaped"].as_str())
        .ok_or_else(|| anyhow!("missing value"))?;
    let chars: Vec<char> = value.chars().collect();
    let mut out = String::with_capacity(value.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '\\'
            && i + 1 < chars.len()
            && matches!(chars[i + 1], '*' | '?' | '[' | ']' | '\\')
        {
            out.push(chars[i + 1]);
            i += 2;
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    Ok(json!({"value": out}))
}

/// Convert a Redis glob-style `pattern` (the form `KEYS`/`SCAN MATCH` use) into an
/// anchored regular expression with the same semantics — for filtering keys
/// client-side, or understanding what a pattern accepts. `*` → `.*`, `?` → `.`,
/// `[…]` becomes a regex character class (negation `[^…]`, `a-z` ranges and `\`
/// escapes are preserved), a `\x` escape outside a class becomes the literal `x`,
/// and every other character is regex-escaped. The result is wrapped in `^…$` so
/// it matches the whole key, mirroring `glob_match`. opts: `pattern` (required).
/// Returns `{pattern, regex}`. Pure.
fn op_glob_to_regex(v: Value) -> Result<Value> {
    let pattern = v["pattern"]
        .as_str()
        .ok_or_else(|| anyhow!("missing pattern"))?;
    let chars: Vec<char> = pattern.chars().collect();
    let n = chars.len();
    let esc_lit = |c: char, out: &mut String| {
        if "\\^$.|?*+()[]{}".contains(c) {
            out.push('\\');
        }
        out.push(c);
    };
    let mut out = String::from("^");
    let mut i = 0;
    while i < n {
        match chars[i] {
            '*' => {
                out.push_str(".*");
                while i + 1 < n && chars[i + 1] == '*' {
                    i += 1;
                }
                i += 1;
            }
            '?' => {
                out.push('.');
                i += 1;
            }
            '\\' if i + 1 < n => {
                esc_lit(chars[i + 1], &mut out);
                i += 2;
            }
            '[' => {
                out.push('[');
                i += 1;
                if i < n && chars[i] == '^' {
                    out.push('^');
                    i += 1;
                }
                while i < n && chars[i] != ']' {
                    if chars[i] == '\\' && i + 1 < n {
                        let c = chars[i + 1];
                        // Re-escape only the chars that are class-significant in regex.
                        if matches!(c, '\\' | ']' | '^' | '-') {
                            out.push('\\');
                        }
                        out.push(c);
                        i += 2;
                    } else {
                        let c = chars[i];
                        if matches!(c, '\\' | ']' | '^') {
                            out.push('\\');
                        }
                        out.push(c);
                        i += 1;
                    }
                }
                if i >= n {
                    return Err(anyhow!("unterminated `[` in glob pattern: {pattern}"));
                }
                out.push(']');
                i += 1; // consume the closing `]`
            }
            c => {
                esc_lit(c, &mut out);
                i += 1;
            }
        }
    }
    out.push('$');
    Ok(json!({ "pattern": pattern, "regex": out }))
}

/// CRC16-CCITT in the XMODEM variant (poly 0x1021, init 0x0000, no reflection),
/// exactly as Redis `crc16.c` computes it. Bitwise rather than table-driven for
/// readability — key hashing is never hot. The standard CRC-16/XMODEM check
/// value `crc16("123456789") == 0x31C3` pins the implementation.
fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// The substring Redis Cluster actually hashes for a key: if the key contains a
/// `{`, and a `}` follows it with at least one character between, only that
/// inner substring is hashed (the "hash tag"); otherwise the whole key. Mirrors
/// `keyHashSlot` in Redis `cluster.c`.
fn hash_tag(key: &[u8]) -> &[u8] {
    if let Some(open) = key.iter().position(|&b| b == b'{') {
        if let Some(rel) = key[open + 1..].iter().position(|&b| b == b'}') {
            if rel > 0 {
                return &key[open + 1..open + 1 + rel];
            }
        }
    }
    key
}

/// Compute the Redis Cluster hash slot for a key — the `CLUSTER KEYSLOT`
/// algorithm: `crc16(hash_tag(key)) % 16384`. Honors `{…}` hash tags so related
/// keys (`{user1000}.following`, `foo{user1000}bar`) co-locate on one slot.
/// opts: key (required). Returns `{key, slot, hash_tag}`. Pure.
fn op_cluster_keyslot(v: Value) -> Result<Value> {
    let key = v["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
    let tag = hash_tag(key.as_bytes());
    let slot = crc16_xmodem(tag) % 16384;
    Ok(json!({
        "key": key,
        "slot": slot,
        "hash_tag": std::str::from_utf8(tag).unwrap_or(key),
    }))
}

/// Whether two keys map to the same Redis Cluster hash slot — the condition a
/// multi-key command (MGET/MSET, transactions, Lua with multiple keys) needs to
/// avoid a CROSSSLOT error. Computes each key's slot via the same
/// `crc16(hash_tag(key)) % 16384` as `cluster_keyslot`, honoring `{…}` hash
/// tags. opts: `a`, `b` (keys). Returns `{a, b, slot_a, slot_b, same_slot}`.
/// Pure.
fn op_same_slot(v: Value) -> Result<Value> {
    let a = v["a"]
        .as_str()
        .or_else(|| v["key_a"].as_str())
        .ok_or_else(|| anyhow!("missing a"))?;
    let b = v["b"]
        .as_str()
        .or_else(|| v["key_b"].as_str())
        .ok_or_else(|| anyhow!("missing b"))?;
    let slot_a = crc16_xmodem(hash_tag(a.as_bytes())) % 16384;
    let slot_b = crc16_xmodem(hash_tag(b.as_bytes())) % 16384;
    Ok(json!({
        "a": a,
        "b": b,
        "slot_a": slot_a,
        "slot_b": slot_b,
        "same_slot": slot_a == slot_b,
    }))
}

/// Parse a Redis Streams entry ID `<ms>-<seq>` into its parts. A full ID gives
/// both `ms` (milliseconds time) and `seq` (sequence number); a partial ID
/// (`<ms>` with no `-<seq>`) gives `ms` with a null `seq`. The special IDs map
/// to a `special` tag: `-` → `min`, `+` → `max`, `$` → `last`, `*` → `auto`
/// (with `ms`/`seq` null). opts: `id` (required). Returns `{ms, seq, special}`.
/// Pure.
fn op_parse_stream_id(v: Value) -> Result<Value> {
    let id = v["id"].as_str().ok_or_else(|| anyhow!("missing id"))?;
    let special = match id {
        "-" => Some("min"),
        "+" => Some("max"),
        "$" => Some("last"),
        "*" => Some("auto"),
        _ => None,
    };
    if let Some(tag) = special {
        return Ok(json!({"ms": Value::Null, "seq": Value::Null, "special": tag}));
    }
    let (ms_str, seq) = match id.split_once('-') {
        Some((m, s)) => {
            let seq: u64 = s
                .parse()
                .map_err(|_| anyhow!("invalid stream id sequence `{s}`"))?;
            (m, json!(seq))
        }
        None => (id, Value::Null),
    };
    let ms: u64 = ms_str
        .parse()
        .map_err(|_| anyhow!("invalid stream id milliseconds `{ms_str}`"))?;
    Ok(json!({"ms": ms, "seq": seq, "special": Value::Null}))
}

/// Build a Redis Streams entry ID from parts — the inverse of `parse_stream_id`.
/// A `special` tag (`min`/`max`/`last`/`auto`) yields `-`/`+`/`$`/`*`; otherwise
/// `ms` is required and an optional `seq` produces `<ms>-<seq>` (a partial
/// `<ms>` when `seq` is omitted). opts: `special`, or `ms` (+ optional `seq`).
/// Returns `{id}`. Pure.
fn op_build_stream_id(v: Value) -> Result<Value> {
    if let Some(tag) = v.get("special").and_then(Value::as_str) {
        let id = match tag {
            "min" => "-",
            "max" => "+",
            "last" => "$",
            "auto" => "*",
            other => {
                return Err(anyhow!(
                    "unknown special stream id `{other}` (min|max|last|auto)"
                ))
            }
        };
        return Ok(json!({ "id": id }));
    }
    let ms = v
        .get("ms")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing ms (or special)"))?;
    let id = match v.get("seq").and_then(Value::as_u64) {
        Some(seq) => format!("{ms}-{seq}"),
        None => ms.to_string(),
    };
    Ok(json!({ "id": id }))
}

/// The inclusive stream-id pair for an `XRANGE` over a `[start_ms, end_ms]` time
/// window — the idiomatic "read a stream by time". `start` is `{start_ms}-0` (the
/// first id possible in that millisecond) and `end` is `{end_ms}-<u64::MAX>` (the
/// last, so every entry produced during `end_ms` is included). Both bounds are
/// inclusive; `end_ms` must be `>= start_ms`. opts: `start_ms` (or `start`),
/// `end_ms` (or `end`), required non-negative millisecond timestamps. Returns
/// `{start, end, start_ms, end_ms}`. Pure.
fn op_stream_id_range(opts: Value) -> Result<Value> {
    let start_ms = opts
        .get("start_ms")
        .or_else(|| opts.get("start"))
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing start_ms (a non-negative millisecond timestamp)"))?;
    let end_ms = opts
        .get("end_ms")
        .or_else(|| opts.get("end"))
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing end_ms (a non-negative millisecond timestamp)"))?;
    if end_ms < start_ms {
        return Err(anyhow!(
            "end_ms ({end_ms}) must be >= start_ms ({start_ms})"
        ));
    }
    Ok(json!({
        "start": format!("{start_ms}-0"),
        "end": format!("{end_ms}-{}", u64::MAX),
        "start_ms": start_ms,
        "end_ms": end_ms,
    }))
}

/// A stream id resolved to a comparison rank. `-`/`+` sort below/above every
/// concrete id; a concrete `<ms>[-<seq>]` ranks by `(ms, seq)` with a bare
/// `<ms>` taking `seq` 0; `$`/`*` have no absolute position and are rejected.
fn resolve_stream_rank(id: &str) -> Result<(u8, u64, u64)> {
    match id {
        "-" => Ok((0, 0, 0)),
        "+" => Ok((2, u64::MAX, u64::MAX)),
        "$" | "*" => Err(anyhow!(
            "stream id `{id}` has no absolute position (not comparable)"
        )),
        _ => {
            let (ms_str, seq) = match id.split_once('-') {
                Some((m, s)) => (
                    m,
                    s.parse::<u64>()
                        .map_err(|_| anyhow!("invalid stream id sequence `{s}`"))?,
                ),
                None => (id, 0),
            };
            let ms = ms_str
                .parse::<u64>()
                .map_err(|_| anyhow!("invalid stream id milliseconds `{ms_str}`"))?;
            Ok((1, ms, seq))
        }
    }
}

/// Compare two Redis Streams entry IDs by their total order — entries are
/// ordered by `(ms, seq)`. Resolves each id (a bare `<ms>` takes `seq` 0; `-`/`+`
/// sort below/above everything), then compares. `$`/`*` have no absolute position
/// and are rejected. opts: `a`, `b`. Returns `{a, b, cmp}` with `cmp` -1 (a<b),
/// 0 (equal), or 1 (a>b). Pure.
fn op_compare_stream_id(v: Value) -> Result<Value> {
    let a = v["a"].as_str().ok_or_else(|| anyhow!("missing a"))?;
    let b = v["b"].as_str().ok_or_else(|| anyhow!("missing b"))?;
    let cmp = match resolve_stream_rank(a)?.cmp(&resolve_stream_rank(b)?) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    };
    Ok(json!({ "a": a, "b": b, "cmp": cmp }))
}

/// The smallest Redis Streams entry ID strictly greater than `id` — its
/// successor — for building an exclusive lower bound when paging `XRANGE`/`XREAD`
/// (everything after the last id you read). Increments the sequence; when the
/// sequence is at `u64::MAX` it rolls over to `<ms+1>-0`. opts: `id` (a concrete
/// `<ms>[-<seq>]`; a bare `<ms>` takes `seq` 0). `-`/`+`/`$`/`*` have no successor
/// and are rejected, as is the absolute maximum (`ms` and `seq` both `u64::MAX`).
/// Returns `{id}`. Pure.
fn op_next_stream_id(v: Value) -> Result<Value> {
    let id = v["id"].as_str().ok_or_else(|| anyhow!("missing id"))?;
    let (rank, ms, seq) = resolve_stream_rank(id)?;
    if rank != 1 {
        return Err(anyhow!("stream id `{id}` is not a concrete entry id"));
    }
    let (nms, nseq) = if seq < u64::MAX {
        (ms, seq + 1)
    } else if ms < u64::MAX {
        (ms + 1, 0)
    } else {
        return Err(anyhow!(
            "stream id `{id}` is the maximum entry id; it has no successor"
        ));
    };
    Ok(json!({ "id": format!("{nms}-{nseq}") }))
}

/// The largest Redis Streams entry ID strictly less than `id` — its predecessor,
/// the mirror of `next_stream_id` — for building an exclusive upper bound when
/// paging `XREVRANGE` (everything before the last id you read). Decrements the
/// sequence; when the sequence is `0` it borrows down to `<ms-1>-<u64::MAX>`.
/// opts: `id` (a concrete `<ms>[-<seq>]`; a bare `<ms>` takes `seq` 0).
/// `-`/`+`/`$`/`*` have no predecessor and are rejected, as is the absolute
/// minimum (`0-0`). Returns `{id}`. Pure.
fn op_prev_stream_id(v: Value) -> Result<Value> {
    let id = v["id"].as_str().ok_or_else(|| anyhow!("missing id"))?;
    let (rank, ms, seq) = resolve_stream_rank(id)?;
    if rank != 1 {
        return Err(anyhow!("stream id `{id}` is not a concrete entry id"));
    }
    let (pms, pseq) = if seq > 0 {
        (ms, seq - 1)
    } else if ms > 0 {
        (ms - 1, u64::MAX)
    } else {
        return Err(anyhow!(
            "stream id `{id}` is the minimum entry id; it has no predecessor"
        ));
    };
    Ok(json!({ "id": format!("{pms}-{pseq}") }))
}

/// Parse the text reply of the `INFO` command into structured maps. The reply is
/// `# Section` headers followed by `field:value` lines (CRLF-terminated, blank
/// lines between sections). Returns `sections` — `{Section: {field: value}}` —
/// plus a flat `fields` map of every field across sections and `names`, the
/// section names in reply order. Field values are kept as raw strings (a complex
/// `k=v,k2=v2` value is not further split, since not every `=` is a sub-map).
/// Lines before the first header land in an empty-named (`""`) section. opts:
/// `info` (or `value`) required. Pure.
fn op_parse_info(v: Value) -> Result<Value> {
    let text = v
        .get("info")
        .or_else(|| v.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing info"))?;
    let mut sections = serde_json::Map::new();
    let mut fields = serde_json::Map::new();
    let mut names: Vec<Value> = Vec::new();
    let mut section = String::new();
    for raw in text.split('\n') {
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        if line.is_empty() {
            continue;
        }
        if let Some(header) = line.strip_prefix('#') {
            section = header.trim().to_string();
            if !sections.contains_key(&section) {
                names.push(json!(section));
                sections.insert(section.clone(), json!({}));
            }
            continue;
        }
        let Some((key, val)) = line.split_once(':') else {
            continue;
        };
        let entry = sections.entry(section.clone()).or_insert_with(|| json!({}));
        if names.iter().all(|n| n.as_str() != Some(section.as_str())) {
            names.push(json!(section.clone()));
        }
        entry
            .as_object_mut()
            .expect("section is an object")
            .insert(key.to_string(), json!(val));
        fields.insert(key.to_string(), json!(val));
    }
    Ok(json!({ "sections": sections, "fields": fields, "names": names }))
}

/// Parse a `CLIENT INFO` / `CLIENT LIST` reply — distinct from the `INFO` format
/// (`op_parse_info`): each connection is one line of space-separated `field=value`
/// pairs (`id=3 addr=127.0.0.1:50858 name= db=0 cmd=client|info …`). Returns
/// `clients` — one `{field: value}` map per non-empty line — so `CLIENT INFO`
/// (one line) yields a single-element list and `CLIENT LIST` (many) yields one
/// entry per connection. Values are kept as raw strings (empty values like
/// `name=` survive). opts: `info` (or `value`) required. Pure.
fn op_parse_client_info(v: Value) -> Result<Value> {
    let text = v
        .get("info")
        .or_else(|| v.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing info"))?;
    let mut clients: Vec<Value> = Vec::new();
    for raw in text.split('\n') {
        let line = raw.strip_suffix('\r').unwrap_or(raw).trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = serde_json::Map::new();
        for token in line.split_whitespace() {
            if let Some((k, val)) = token.split_once('=') {
                fields.insert(k.to_string(), json!(val));
            }
        }
        clients.push(Value::Object(fields));
    }
    Ok(json!({ "clients": clients }))
}

/// Parse the output of `CLUSTER NODES` — one node per line, distinct from
/// `parse_info` (sectioned `key:value`) and `parse_client_info` (space-separated
/// `field=value`). Each line is `<id> <ip:port@cport[,hostname]> <flags> <master>
/// <ping-sent> <pong-recv> <config-epoch> <link-state> <slot>…`. The address is
/// split into `host`/`port`/`cport`/`hostname`; `flags` becomes an array (with
/// `myself`/`role` lifted); `master` `-` becomes null; the numeric fields are
/// parsed; and each trailing slot token is decoded as a `[start,end]` range
/// (a single slot is `[n,n]`), while `[slot-<-node]`/`[slot->-node]` go to
/// `importing`/`migrating`. opts: `nodes` (or `value`). Returns `{nodes:[…]}`.
/// Pure.
fn op_parse_cluster_nodes(v: Value) -> Result<Value> {
    let text = v
        .get("nodes")
        .or_else(|| v.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing nodes"))?;
    let mut nodes: Vec<Value> = Vec::new();
    for raw in text.split('\n') {
        let line = raw.strip_suffix('\r').unwrap_or(raw).trim();
        if line.is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split_whitespace().collect();
        if cols.len() < 8 {
            return Err(anyhow!("cluster node line has fewer than 8 fields: {line}"));
        }
        // Address: ip:port@cport[,hostname]
        let addr = cols[1];
        let (ipport, cport_host) = addr.split_once('@').unwrap_or((addr, ""));
        let (host, port) = match ipport.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse::<u32>().ok()),
            None => (ipport.to_string(), None),
        };
        let (cport_str, hostname) = match cport_host.split_once(',') {
            Some((c, h)) => (c, Some(h.to_string())),
            None => (cport_host, None),
        };
        let cport = cport_str.parse::<u32>().ok();
        // Flags.
        let flags: Vec<&str> = if cols[2] == "noflags" {
            Vec::new()
        } else {
            cols[2].split(',').collect()
        };
        let myself = flags.contains(&"myself");
        let role = if flags.contains(&"master") {
            Some("master")
        } else if flags.contains(&"slave") {
            Some("slave")
        } else {
            None
        };
        let master = if cols[3] == "-" {
            Value::Null
        } else {
            json!(cols[3])
        };
        let num = |s: &str| s.parse::<u64>().map(|n| json!(n)).unwrap_or(json!(s));
        // Slots (fields 8+).
        let mut slots: Vec<Value> = Vec::new();
        let mut migrating: Vec<Value> = Vec::new();
        let mut importing: Vec<Value> = Vec::new();
        for tok in &cols[8..] {
            if let Some(inner) = tok.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
                if let Some((slot, node)) = inner.split_once("-<-") {
                    importing.push(json!({"slot": num(slot), "node": node}));
                } else if let Some((slot, node)) = inner.split_once("->-") {
                    migrating.push(json!({"slot": num(slot), "node": node}));
                }
            } else if let Some((a, b)) = tok.split_once('-') {
                slots.push(json!([num(a), num(b)]));
            } else {
                slots.push(json!([num(tok), num(tok)]));
            }
        }
        nodes.push(json!({
            "id": cols[0],
            "addr": addr,
            "host": host,
            "port": port,
            "cport": cport,
            "hostname": hostname,
            "flags": flags,
            "myself": myself,
            "role": role,
            "master": master,
            "ping_sent": num(cols[4]),
            "pong_recv": num(cols[5]),
            "config_epoch": num(cols[6]),
            "link_state": cols[7],
            "slots": slots,
            "migrating": migrating,
            "importing": importing,
        }));
    }
    Ok(json!({ "nodes": nodes }))
}

#[no_mangle]
pub extern "C" fn redis__parse_info(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_info)
}

#[no_mangle]
pub extern "C" fn redis__parse_cluster_nodes(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_cluster_nodes)
}

#[no_mangle]
pub extern "C" fn redis__parse_client_info(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_client_info)
}

#[no_mangle]
pub extern "C" fn redis__parse_url(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_url)
}

#[no_mangle]
pub extern "C" fn redis__build_url(args: *const c_char) -> *const c_char {
    ffi_call(args, op_build_url)
}

#[no_mangle]
pub extern "C" fn redis__redact_url(args: *const c_char) -> *const c_char {
    ffi_call(args, op_redact_url)
}

#[no_mangle]
pub extern "C" fn redis__glob_match(args: *const c_char) -> *const c_char {
    ffi_call(args, op_glob_match)
}

#[no_mangle]
pub extern "C" fn redis__glob_escape(args: *const c_char) -> *const c_char {
    ffi_call(args, op_glob_escape)
}

#[no_mangle]
pub extern "C" fn redis__glob_unescape(args: *const c_char) -> *const c_char {
    ffi_call(args, op_glob_unescape)
}

#[no_mangle]
pub extern "C" fn redis__glob_to_regex(args: *const c_char) -> *const c_char {
    ffi_call(args, op_glob_to_regex)
}

#[no_mangle]
pub extern "C" fn redis__cluster_keyslot(args: *const c_char) -> *const c_char {
    ffi_call(args, op_cluster_keyslot)
}

#[no_mangle]
pub extern "C" fn redis__same_slot(args: *const c_char) -> *const c_char {
    ffi_call(args, op_same_slot)
}

#[no_mangle]
pub extern "C" fn redis__parse_stream_id(args: *const c_char) -> *const c_char {
    ffi_call(args, op_parse_stream_id)
}

#[no_mangle]
pub extern "C" fn redis__build_stream_id(args: *const c_char) -> *const c_char {
    ffi_call(args, op_build_stream_id)
}

#[no_mangle]
pub extern "C" fn redis__stream_id_range(args: *const c_char) -> *const c_char {
    ffi_call(args, op_stream_id_range)
}

#[no_mangle]
pub extern "C" fn redis__compare_stream_id(args: *const c_char) -> *const c_char {
    ffi_call(args, op_compare_stream_id)
}

#[no_mangle]
pub extern "C" fn redis__next_stream_id(args: *const c_char) -> *const c_char {
    ffi_call(args, op_next_stream_id)
}

#[no_mangle]
pub extern "C" fn redis__prev_stream_id(args: *const c_char) -> *const c_char {
    ffi_call(args, op_prev_stream_id)
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

    // ── pure helpers (no connection) ─────────────────────────────────────────

    #[test]
    fn parse_url_full_with_auth_and_db() {
        let v =
            op_parse_url(json!({"url": "redis://alice:s3cret@cache.example.com:6380/2"})).unwrap();
        assert_eq!(v["scheme"], json!("redis"));
        assert_eq!(v["tls"], json!(false));
        assert_eq!(v["user"], json!("alice"));
        assert_eq!(v["password"], json!("s3cret"));
        assert_eq!(v["host"], json!("cache.example.com"));
        assert_eq!(v["port"], json!(6380));
        assert_eq!(v["db"], json!(2));
    }

    #[test]
    fn parse_url_rediss_and_password_only_and_defaults() {
        let tls = op_parse_url(json!({"url": "rediss://:pw@host"})).unwrap();
        assert_eq!(tls["tls"], json!(true), "rediss → tls");
        assert_eq!(tls["user"], Value::Null, "empty user before colon → null");
        assert_eq!(tls["password"], json!("pw"));
        assert_eq!(tls["port"], Value::Null, "no port → null");
        assert!(op_parse_url(json!({"url": "http://x"})).is_err());
    }

    #[test]
    fn build_url_is_inverse_of_parse_url() {
        // Full round-trip: parts → URL → parts.
        let built = op_build_url(json!({
            "user": "alice", "password": "s3cret",
            "host": "cache.example.com", "port": 6380, "db": 2
        }))
        .unwrap()["url"]
            .clone();
        assert_eq!(
            built,
            json!("redis://alice:s3cret@cache.example.com:6380/2")
        );
        let back = op_parse_url(json!({"url": built})).unwrap();
        assert_eq!(back["user"], json!("alice"));
        assert_eq!(back["password"], json!("s3cret"));
        assert_eq!(back["port"], json!(6380));
        assert_eq!(back["db"], json!(2));
        // tls flag → rediss, password-only userinfo. stryke passes the flag as
        // the JSON number 1, not a bool, so both forms must select rediss.
        assert_eq!(
            op_build_url(json!({"tls": true, "password": "pw", "host": "h"})).unwrap()["url"],
            json!("rediss://:pw@h")
        );
        assert_eq!(
            op_build_url(json!({"tls": 1, "password": "pw", "host": "h"})).unwrap()["url"],
            json!("rediss://:pw@h"),
            "numeric truthy flag (stryke serialization) also selects rediss"
        );
        // Bare host defaults; no userinfo when neither user nor password set.
        assert_eq!(
            op_build_url(json!({})).unwrap()["url"],
            json!("redis://127.0.0.1")
        );
        // user-only userinfo.
        assert_eq!(
            op_build_url(json!({"user": "u", "host": "h", "db": 0})).unwrap()["url"],
            json!("redis://u@h/0")
        );
    }

    #[test]
    fn redact_url_masks_only_the_password() {
        let r = |u: &str| {
            op_redact_url(json!({ "url": u })).unwrap()["redacted"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // user:password → user:*** , the rest of the URL is preserved.
        assert_eq!(
            r("redis://alice:s3cret@cache.example.com:6380/2"),
            "redis://alice:***@cache.example.com:6380/2"
        );
        // Password-only userinfo (the common requirepass form).
        assert_eq!(r("rediss://:hunter2@h:6379"), "rediss://:***@h:6379");
        // No password → unchanged (user-only, no userinfo, bare host).
        assert_eq!(r("redis://alice@h/0"), "redis://alice@h/0");
        assert_eq!(r("redis://h:6379/1"), "redis://h:6379/1");
        // A query/path after the db is preserved.
        assert_eq!(
            r("rediss://u:p@h/0?ssl_cert_reqs=required"),
            "rediss://u:***@h/0?ssl_cert_reqs=required"
        );
        // Not a recognizable URL → unchanged; missing arg errors.
        assert_eq!(r("not-a-url"), "not-a-url");
        assert!(op_redact_url(json!({})).is_err());
    }

    #[test]
    fn glob_match_star_question_and_classes() {
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h*llo", b"heeello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(!glob_match(b"h[ae]llo", b"hillo"));
        assert!(glob_match(b"h[^e]llo", b"hallo"));
        assert!(
            !glob_match(b"h[^e]llo", b"hello"),
            "negated class excludes e"
        );
        assert!(glob_match(b"key:[0-9]", b"key:7"), "range");
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"user:*:session", b"user:42:session"));
        assert!(!glob_match(b"user:*:session", b"user:42:other"));
        // backslash escapes the metacharacter.
        assert!(glob_match(b"a\\*b", b"a*b"));
        assert!(!glob_match(b"a\\*b", b"axb"));
    }

    #[test]
    fn op_glob_match_returns_bool() {
        assert_eq!(
            op_glob_match(json!({"pattern": "user:*", "key": "user:1"})).unwrap()["match"],
            json!(true)
        );
        assert_eq!(
            op_glob_match(json!({"pattern": "user:*", "key": "admin:1"})).unwrap()["match"],
            json!(false)
        );
    }

    #[test]
    fn glob_escape_makes_metachars_literal_and_round_trips_through_match() {
        // Each metacharacter is backslash-prefixed.
        assert_eq!(
            op_glob_escape(json!({"value": "a*b?c[d]e\\f"})).unwrap()["escaped"],
            json!("a\\*b\\?c\\[d\\]e\\\\f")
        );
        // Invariant: an escaped literal matches itself...
        let lit = "key:[v1]*?";
        let pat = op_glob_escape(json!({"value": lit})).unwrap()["escaped"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(
            glob_match(pat.as_bytes(), lit.as_bytes()),
            "escaped pattern matches the literal"
        );
        // ...but not a different string the raw glob would have matched.
        assert!(!glob_match(pat.as_bytes(), b"key:v1xy"));
        assert!(op_glob_escape(json!({})).is_err());
    }

    #[test]
    fn glob_unescape_inverts_glob_escape() {
        let un = |s: &str| {
            op_glob_unescape(json!({ "value": s })).unwrap()["value"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // Each escape decodes back.
        assert_eq!(un("a\\*b\\?c"), "a*b?c");
        assert_eq!(un("\\[d\\]"), "[d]");
        assert_eq!(un("c\\\\d"), "c\\d");
        // A `\\` adjacent to a metacharacter must not be mis-parsed (left-to-right
        // scan): `\\*` is a literal backslash followed by an unescaped `*`.
        assert_eq!(un("\\\\*"), "\\*");
        // A backslash not introducing a glob escape stays literal.
        assert_eq!(un("a\\nb"), "a\\nb");
        // Round-trips glob_escape for arbitrary input, including every metachar.
        for raw in ["a*b?c[d]e\\f", "key:[v1]*?", "plain", "\\*?[]\\"] {
            let esc = op_glob_escape(json!({ "value": raw })).unwrap()["escaped"]
                .as_str()
                .unwrap()
                .to_string();
            assert_eq!(un(&esc), raw, "round-trip for {raw:?}");
        }
        // `escaped` is accepted as an alias for `value`.
        assert_eq!(
            op_glob_unescape(json!({"escaped": "x\\*y"})).unwrap()["value"],
            json!("x*y")
        );
        assert!(op_glob_unescape(json!({})).is_err());
    }

    #[test]
    fn glob_to_regex_translates_glob_metacharacters() {
        let r = |p: &str| {
            op_glob_to_regex(json!({ "pattern": p })).unwrap()["regex"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // `*` and `?`, anchored to the whole key.
        assert_eq!(r("user:*"), "^user:.*$");
        assert_eq!(r("h?llo"), "^h.llo$");
        // Consecutive `*` collapse to a single `.*`.
        assert_eq!(r("a**b"), "^a.*b$");
        // Regex metacharacters in literals are escaped.
        assert_eq!(r("a.b+c"), "^a\\.b\\+c$");
        // A glob `\x` escape becomes the literal x (regex-escaped if special).
        assert_eq!(r("a\\*b"), "^a\\*b$");
        assert_eq!(r("a\\.b"), "^a\\.b$");
        // Character classes: ranges, negation, and the literal-dot-in-class case.
        assert_eq!(r("[a-z]"), "^[a-z]$");
        assert_eq!(r("[^0-9]"), "^[^0-9]$");
        assert_eq!(r("key:[12]"), "^key:[12]$");
        // An unterminated class is an error.
        assert!(op_glob_to_regex(json!({ "pattern": "ab[cd" })).is_err());
        assert!(op_glob_to_regex(json!({})).is_err());
    }

    #[test]
    fn crc16_matches_standard_xmodem_check_value() {
        // The canonical CRC-16/XMODEM check value for "123456789" is 0x31C3.
        assert_eq!(crc16_xmodem(b"123456789"), 0x31C3);
        assert_eq!(crc16_xmodem(b""), 0);
    }

    #[test]
    fn cluster_keyslot_uses_hash_tag_and_stays_in_range() {
        // crc16("123456789") = 0x31C3 = 12739, which is < 16384 so it is the slot.
        assert_eq!(
            op_cluster_keyslot(json!({"key": "123456789"})).unwrap()["slot"],
            json!(12739)
        );
        // Hash-tag equivalence: a `{tag}` collapses the key to its inner tag, so
        // these three keys all land on the same slot as the bare tag.
        let bare = op_cluster_keyslot(json!({"key": "user1000"})).unwrap()["slot"].clone();
        for key in ["{user1000}.following", "foo{user1000}bar", "{user1000}"] {
            assert_eq!(
                op_cluster_keyslot(json!({ "key": key })).unwrap()["slot"],
                bare,
                "{key} co-locates with its hash tag"
            );
        }
        // Empty braces and an unclosed `{` fall back to hashing the whole key.
        let whole = op_cluster_keyslot(json!({"key": "foo{}bar"})).unwrap();
        assert_eq!(whole["hash_tag"], json!("foo{}bar"));
        assert_eq!(
            op_cluster_keyslot(json!({"key": "foo{bar"})).unwrap()["hash_tag"],
            json!("foo{bar")
        );
        // Every slot is within the 16384-slot space.
        for key in ["", "a", "{x}", "a very long :: redis key with spaces"] {
            let slot = op_cluster_keyslot(json!({ "key": key })).unwrap()["slot"]
                .as_u64()
                .unwrap();
            assert!(slot < 16384, "slot for {key:?} in range");
        }
        assert!(op_cluster_keyslot(json!({})).is_err());
    }

    #[test]
    fn same_slot_tracks_hash_tag_colocation() {
        // Keys sharing a `{tag}` co-locate, so MGET/MSET/transactions are safe.
        let v = op_same_slot(json!({"a": "{user1000}.following", "b": "{user1000}.followers"}))
            .unwrap();
        assert_eq!(v["same_slot"], json!(true));
        assert_eq!(v["slot_a"], v["slot_b"]);
        // Without a shared tag, two arbitrary keys land on different slots.
        let diff = op_same_slot(json!({"a": "user1000", "b": "user2000"})).unwrap();
        assert_eq!(diff["same_slot"], json!(false));
        // A bare tag matches a key embedding that tag.
        assert_eq!(
            op_same_slot(json!({"a": "user1000", "b": "foo{user1000}bar"})).unwrap()["same_slot"],
            json!(true)
        );
        // Slots agree with cluster_keyslot.
        let ks =
            op_cluster_keyslot(json!({"key": "{user1000}.following"})).unwrap()["slot"].clone();
        assert_eq!(v["slot_a"], ks);
        // Missing keys reject.
        assert!(op_same_slot(json!({"a": "x"})).is_err());
        assert!(op_same_slot(json!({})).is_err());
    }

    #[test]
    fn parse_stream_id_full_partial_and_special() {
        // Full ID → both parts.
        let full = op_parse_stream_id(json!({"id": "1526919030474-55"})).unwrap();
        assert_eq!(full["ms"], json!(1_526_919_030_474u64));
        assert_eq!(full["seq"], json!(55));
        assert_eq!(full["special"], Value::Null);
        // Partial ID (no `-seq`) → ms with a null seq.
        let partial = op_parse_stream_id(json!({"id": "1526919030474"})).unwrap();
        assert_eq!(partial["ms"], json!(1_526_919_030_474u64));
        assert_eq!(partial["seq"], Value::Null);
        // The four special IDs.
        for (id, tag) in [("-", "min"), ("+", "max"), ("$", "last"), ("*", "auto")] {
            let v = op_parse_stream_id(json!({ "id": id })).unwrap();
            assert_eq!(v["special"], json!(tag), "{id} → {tag}");
            assert_eq!(v["ms"], Value::Null);
        }
        // Non-numeric components reject.
        assert!(op_parse_stream_id(json!({"id": "abc-1"})).is_err());
        assert!(op_parse_stream_id(json!({"id": "100-xyz"})).is_err());
        assert!(op_parse_stream_id(json!({})).is_err());
    }

    #[test]
    fn build_stream_id_inverts_parse_stream_id() {
        // ms + seq → full ID; ms alone → partial.
        assert_eq!(
            op_build_stream_id(json!({"ms": 1_526_919_030_474u64, "seq": 55})).unwrap()["id"],
            json!("1526919030474-55")
        );
        assert_eq!(
            op_build_stream_id(json!({"ms": 1_526_919_030_474u64})).unwrap()["id"],
            json!("1526919030474")
        );
        // Special tags → their sigils.
        for (tag, id) in [("min", "-"), ("max", "+"), ("last", "$"), ("auto", "*")] {
            assert_eq!(
                op_build_stream_id(json!({ "special": tag })).unwrap()["id"],
                json!(id),
                "{tag} → {id}"
            );
        }
        // Round-trips parse_stream_id (full, partial, and each special).
        for id in ["1526919030474-55", "1526919030474", "-", "+", "$", "*"] {
            let p = op_parse_stream_id(json!({ "id": id })).unwrap();
            let rebuilt = op_build_stream_id(json!({
                "ms": p["ms"],
                "seq": p["seq"],
                "special": p["special"],
            }))
            .unwrap()["id"]
                .clone();
            assert_eq!(rebuilt, json!(id), "round-trip for {id}");
        }
        // Unknown special and missing ms reject.
        assert!(op_build_stream_id(json!({"special": "bogus"})).is_err());
        assert!(op_build_stream_id(json!({"seq": 1})).is_err());
    }

    #[test]
    fn stream_id_range_builds_the_inclusive_xrange_window() {
        let v = op_stream_id_range(json!({"start_ms": 1000u64, "end_ms": 2000u64})).unwrap();
        // start is the first id of start_ms; end is the last id of end_ms.
        assert_eq!(v["start"], json!("1000-0"));
        assert_eq!(v["end"], json!("2000-18446744073709551615"));
        assert_eq!(v["start_ms"], json!(1000));
        assert_eq!(v["end_ms"], json!(2000));
        // A single-millisecond window covers the whole sequence space of that ms.
        let one = op_stream_id_range(json!({"start": 500u64, "end": 500u64})).unwrap();
        assert_eq!(one["start"], json!("500-0"));
        assert_eq!(one["end"], json!("500-18446744073709551615"));
        // The window end actually orders at-or-above any real id in end_ms, and the
        // start at-or-below — verified against compare_stream_id.
        let mid = "2000-42";
        let lo = op_compare_stream_id(json!({"a": "1000-0", "b": mid})).unwrap();
        let hi = op_compare_stream_id(json!({"a": "2000-18446744073709551615", "b": mid})).unwrap();
        assert_eq!(
            lo["cmp"],
            json!(-1),
            "range start is <= an id inside the window"
        );
        assert_eq!(
            hi["cmp"],
            json!(1),
            "range end is >= an id inside the window"
        );
        // end_ms < start_ms and missing bounds reject.
        assert!(op_stream_id_range(json!({"start_ms": 2000u64, "end_ms": 1000u64})).is_err());
        assert!(op_stream_id_range(json!({"start_ms": 1000u64})).is_err());
        assert!(op_stream_id_range(json!({})).is_err());
    }

    #[test]
    fn compare_stream_id_orders_by_ms_then_seq() {
        let cmp = |a: &str, b: &str| {
            op_compare_stream_id(json!({"a": a, "b": b})).unwrap()["cmp"]
                .as_i64()
                .unwrap()
        };
        // ms dominates seq.
        assert_eq!(cmp("5-0", "10-0"), -1);
        assert_eq!(cmp("10-9", "5-99"), 1);
        // equal ms → seq breaks the tie.
        assert_eq!(cmp("5-1", "5-2"), -1);
        assert_eq!(cmp("5-2", "5-1"), 1);
        assert_eq!(cmp("5-1", "5-1"), 0);
        // a bare <ms> takes seq 0.
        assert_eq!(cmp("5", "5-0"), 0);
        assert_eq!(cmp("5", "5-1"), -1);
        // `-`/`+` sort below/above every concrete id and each other.
        assert_eq!(cmp("-", "0-0"), -1);
        assert_eq!(cmp("+", "18446744073709551615-18446744073709551615"), 1);
        assert_eq!(cmp("-", "+"), -1);
        assert_eq!(cmp("+", "-"), 1);
        assert_eq!(cmp("-", "-"), 0);
        assert_eq!(cmp("+", "+"), 0);
        // `$`/`*` have no absolute position → reject on either side.
        assert!(op_compare_stream_id(json!({"a": "$", "b": "5-0"})).is_err());
        assert!(op_compare_stream_id(json!({"a": "5-0", "b": "*"})).is_err());
        // garbage rejects.
        assert!(op_compare_stream_id(json!({"a": "x-1", "b": "5-0"})).is_err());
        assert!(op_compare_stream_id(json!({"a": "5-z", "b": "5-0"})).is_err());
    }

    #[test]
    fn next_stream_id_is_the_successor_for_exclusive_ranges() {
        let next = |id: &str| {
            op_next_stream_id(json!({ "id": id })).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // Ordinary increment of the sequence.
        assert_eq!(next("5-0"), "5-1");
        assert_eq!(next("1526919030474-55"), "1526919030474-56");
        // A bare `<ms>` takes seq 0, so its successor is `<ms>-1`.
        assert_eq!(next("5"), "5-1");
        // Sequence at u64::MAX rolls over to the next millisecond, seq 0.
        assert_eq!(next("5-18446744073709551615"), "6-0");
        // The successor always compares strictly greater than the input.
        for id in ["0-0", "5-0", "5", "5-18446744073709551615"] {
            let n = next(id);
            assert_eq!(
                op_compare_stream_id(json!({"a": &n, "b": id})).unwrap()["cmp"],
                json!(1),
                "next({id}) = {n} must be > {id}"
            );
        }
        // Special ids have no successor; neither does the absolute maximum.
        assert!(op_next_stream_id(json!({"id": "-"})).is_err());
        assert!(op_next_stream_id(json!({"id": "+"})).is_err());
        assert!(op_next_stream_id(json!({"id": "$"})).is_err());
        assert!(op_next_stream_id(json!({"id": "*"})).is_err());
        assert!(op_next_stream_id(json!({
            "id": "18446744073709551615-18446744073709551615"
        }))
        .is_err());
        // Garbage and missing id reject.
        assert!(op_next_stream_id(json!({"id": "x-1"})).is_err());
        assert!(op_next_stream_id(json!({})).is_err());
    }

    #[test]
    fn prev_stream_id_is_the_predecessor_and_mirrors_next() {
        let prev = |id: &str| {
            op_prev_stream_id(json!({ "id": id })).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_string()
        };
        let next = |id: &str| {
            op_next_stream_id(json!({ "id": id })).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // Ordinary decrement of the sequence.
        assert_eq!(prev("5-1"), "5-0");
        assert_eq!(prev("1526919030474-56"), "1526919030474-55");
        // A bare `<ms>` takes seq 0, so its predecessor borrows: `<ms-1>-MAX`.
        assert_eq!(prev("5"), "4-18446744073709551615");
        // Seq 0 borrows down to the previous millisecond at u64::MAX.
        assert_eq!(prev("6-0"), "5-18446744073709551615");
        // The predecessor always compares strictly less than the input.
        for id in ["5-1", "6-0", "1-0", "18446744073709551615-0"] {
            let p = prev(id);
            assert_eq!(
                op_compare_stream_id(json!({"a": &p, "b": id})).unwrap()["cmp"],
                json!(-1),
                "prev({id}) = {p} must be < {id}"
            );
        }
        // prev and next are exact inverses on concrete ids.
        for id in ["5-0", "5-1", "6-0", "1526919030474-55"] {
            assert_eq!(prev(&next(id)), id, "prev(next({id})) must round-trip");
            assert_eq!(next(&prev(id)), id, "next(prev({id})) must round-trip");
        }
        // Special ids have no predecessor; neither does the absolute minimum 0-0.
        assert!(op_prev_stream_id(json!({"id": "-"})).is_err());
        assert!(op_prev_stream_id(json!({"id": "+"})).is_err());
        assert!(op_prev_stream_id(json!({"id": "$"})).is_err());
        assert!(op_prev_stream_id(json!({"id": "*"})).is_err());
        assert!(op_prev_stream_id(json!({"id": "0-0"})).is_err());
        // Garbage and missing id reject.
        assert!(op_prev_stream_id(json!({"id": "x-1"})).is_err());
        assert!(op_prev_stream_id(json!({})).is_err());
    }

    #[test]
    fn parse_info_groups_fields_by_section() {
        // A realistic CRLF reply with two sections and a complex value.
        let text = "# Server\r\nredis_version:7.4.0\r\nredis_mode:standalone\r\n\r\n# Keyspace\r\ndb0:keys=2,expires=1,avg_ttl=0\r\n";
        let v = op_parse_info(json!({ "info": text })).unwrap();
        // Section order preserved.
        assert_eq!(v["names"], json!(["Server", "Keyspace"]));
        // Grouped per section.
        assert_eq!(v["sections"]["Server"]["redis_version"], json!("7.4.0"));
        assert_eq!(v["sections"]["Server"]["redis_mode"], json!("standalone"));
        // A complex `k=v,…` value is kept raw (not split).
        assert_eq!(
            v["sections"]["Keyspace"]["db0"],
            json!("keys=2,expires=1,avg_ttl=0")
        );
        // Flat field map spans all sections.
        assert_eq!(v["fields"]["redis_version"], json!("7.4.0"));
        assert_eq!(v["fields"]["db0"], json!("keys=2,expires=1,avg_ttl=0"));
        // A value may itself contain a colon (split on the first only).
        let v2 =
            op_parse_info(json!({ "info": "# Server\r\nexecutable:/usr/bin/redis:server\r\n" }))
                .unwrap();
        assert_eq!(v2["fields"]["executable"], json!("/usr/bin/redis:server"));
        // Bare `\n` line endings are tolerated; an empty reply yields empty maps.
        assert_eq!(
            op_parse_info(json!({ "info": "# X\na:1\n" })).unwrap()["sections"]["X"]["a"],
            json!("1")
        );
        assert_eq!(
            op_parse_info(json!({ "info": "" })).unwrap()["names"],
            json!([])
        );
        assert!(op_parse_info(json!({})).is_err());
    }

    #[test]
    fn parse_client_info_splits_space_separated_pairs_per_line() {
        // CLIENT INFO: a single line of `field=value` pairs.
        let one = "id=3 addr=127.0.0.1:50858 name= db=0 cmd=client|info";
        let v = op_parse_client_info(json!({ "info": one })).unwrap();
        let clients = v["clients"].as_array().unwrap();
        assert_eq!(clients.len(), 1, "one line → one client");
        assert_eq!(clients[0]["id"], json!("3"));
        assert_eq!(clients[0]["addr"], json!("127.0.0.1:50858"));
        // An empty value (`name=`) is preserved.
        assert_eq!(clients[0]["name"], json!(""));
        // A `|`-containing command value survives.
        assert_eq!(clients[0]["cmd"], json!("client|info"));
        // CLIENT LIST: one client per line.
        let many = "id=1 addr=10.0.0.1:6379 db=0\nid=2 addr=10.0.0.2:6379 db=1\n";
        let lv = op_parse_client_info(json!({ "info": many })).unwrap();
        let lc = lv["clients"].as_array().unwrap();
        assert_eq!(lc.len(), 2);
        assert_eq!(lc[1]["id"], json!("2"));
        assert_eq!(lc[1]["db"], json!("1"));
        // `value` alias; an empty reply yields no clients.
        assert_eq!(
            op_parse_client_info(json!({ "value": "id=9 db=0" })).unwrap()["clients"][0]["id"],
            json!("9")
        );
        assert_eq!(
            op_parse_client_info(json!({ "info": "" })).unwrap()["clients"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert!(op_parse_client_info(json!({})).is_err());
    }

    #[test]
    fn parse_cluster_nodes_decomposes_each_node_line() {
        let text = "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001,host1 myself,master - 0 1700000000000 1 connected 0-5460\n\
                    07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30002@31002 master - 0 1700000000050 2 connected 5461-10922 [77->-e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca]\n\
                    abcabcabcabcabcabcabcabcabcabcabcabcabc1 127.0.0.1:30003@31003 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1700000000070 3 connected\n";
        let v = op_parse_cluster_nodes(json!({ "nodes": text })).unwrap();
        let nodes = v["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 3, "three node lines");
        // Node 0: myself master, address split, a slot range.
        let n0 = &nodes[0];
        assert_eq!(n0["id"], json!("e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca"));
        assert_eq!(n0["host"], json!("127.0.0.1"));
        assert_eq!(n0["port"], json!(30001));
        assert_eq!(n0["cport"], json!(31001));
        assert_eq!(n0["hostname"], json!("host1"));
        assert_eq!(n0["myself"], json!(true));
        assert_eq!(n0["role"], json!("master"));
        assert_eq!(n0["master"], Value::Null, "a master has `-` → null");
        assert_eq!(n0["link_state"], json!("connected"));
        assert_eq!(n0["config_epoch"], json!(1));
        assert_eq!(
            n0["slots"],
            json!([[0, 5460]]),
            "a range becomes [start,end]"
        );
        // Node 1: a migrating slot is lifted out of `slots`.
        let n1 = &nodes[1];
        assert_eq!(n1["hostname"], Value::Null, "no hostname → null");
        assert_eq!(n1["slots"], json!([[5461, 10922]]));
        assert_eq!(n1["migrating"][0]["slot"], json!(77));
        assert_eq!(
            n1["migrating"][0]["node"],
            json!("e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca")
        );
        // Node 2: a slave with a master id and no slots.
        let n2 = &nodes[2];
        assert_eq!(n2["role"], json!("slave"));
        assert_eq!(
            n2["master"],
            json!("e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca")
        );
        assert_eq!(n2["slots"].as_array().unwrap().len(), 0);
        // Importing notation, a bare single slot, `value` alias, errors.
        let imp = op_parse_cluster_nodes(json!({
            "value": "id1 1.2.3.4:7000@17000 master - 0 0 5 connected 42 [93-<-id2]"
        }))
        .unwrap();
        assert_eq!(
            imp["nodes"][0]["slots"],
            json!([[42, 42]]),
            "single slot → [n,n]"
        );
        assert_eq!(imp["nodes"][0]["importing"][0]["slot"], json!(93));
        assert_eq!(imp["nodes"][0]["importing"][0]["node"], json!("id2"));
        assert!(op_parse_cluster_nodes(json!({"nodes": "too few fields here"})).is_err());
        assert!(op_parse_cluster_nodes(json!({})).is_err());
    }
}
