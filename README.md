```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ r e d i s ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-redis/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[REDIS / VALKEY CLIENT FOR STRYKE // KV + LISTS + SETS + HASHES + ZSETS + PUB/SUB + SCAN]`

> *"In-memory state, one pipe away."*

Redis / Valkey client for stryke. KV, lists, sets, hashes, sorted sets,
pub/sub publish, scan, server admin. Opt-in package tier.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-kafka`](https://github.com/MenkeTechnologies/stryke-kafka) · [`stryke-mongo`](https://github.com/MenkeTechnologies/stryke-mongo) · [`stryke-postgres`](https://github.com/MenkeTechnologies/stryke-postgres) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Why this is one of the most useful stryke packages](#0x00-why-this-is-one-of-the-most-useful-stryke-packages)
- [\[0x01\] Install](#0x01-install)
- [\[0x02\] Quick start](#0x02-quick-start)
- [\[0x03\] Connection options](#0x03-connection-options)
- [\[0x04\] API reference (selected)](#0x04-api-reference-selected)
- [\[0x05\] FFI layer](#0x05-ffi-layer)
- [\[0x06\] Tests](#0x06-tests)
- [\[0x07\] Dev workflow](#0x07-dev-workflow)
- [\[0x08\] Layout](#0x08-layout)
- [\[0x09\] Roadmap](#0x09-roadmap)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Why this is one of the most useful stryke packages

A Redis client is the single highest-leverage missing daily-use
integration. Caches, queues, deduplication sets, leaderboards, rate
limiters, pub/sub fanout — all of it lives in Redis or a Redis-compatible
key-value store (Valkey, KeyDB, Dragonfly). This package gives stryke
direct access in one syscall per operation:

```stryke
use Redis
Redis::set "rate-limit:user-42", "1", ex => 60, nx => 1
```

## [0x01] Install

From a release (no rustc needed on the consumer machine — works from
any directory, no project needed):

```sh
s pkg install -g github.com/MenkeTechnologies/stryke-redis
```

From a local checkout (publisher / contributor workflow — builds the
cdylib via cargo, then installs into `~/.stryke/store/redis@<version>/`):

```sh
cd ~/projects/stryke-redis
cargo build --release
s pkg install -g .
```

Or:

```sh
make install
```

`-g` installs globally with no project requirement. `s pkg install`
without `-g` resolves the current project's `stryke.toml` deps and is
unrelated to installing this package itself.

## [0x02] Quick start

```stryke
use Redis

# Connection is detected from $REDIS_URL or defaults to 127.0.0.1:6379.
$ENV{REDIS_URL} = "redis://localhost:6379/0"

Redis::set "greeting", "hello from stryke"
p Redis::get "greeting"

Redis::mset { a => 1, b => 2, c => 3 }
p Redis::mget ["a", "b", "c"]   # ("1","2","3")

# TTL.
Redis::set "session:42", "...", ex => 3600          # 1h
p Redis::ttl "session:42"

# Counters.
my $n = Redis::incr "page-views", by => 5

# Lists / queues.
Redis::lpush "events", ["a", "b", "c"]
my @recent = Redis::lrange "events", 0, 9
my $next   = Redis::rpop  "events"                  # FIFO consumption

# Sets / dedup.
Redis::sadd "seen-users", ["alice", "bob"]
p Redis::sismember "seen-users", "alice"            # 1

# Hash = JSON-friendly objects.
Redis::hmset "user:42", { name => "alice", role => "admin", score => 100 }
my $u = Redis::hgetall "user:42"
p "$u->{name} ($u->{role})"

# Sorted set = leaderboard.
Redis::zadd "leaderboard", { alice => 100, bob => 200 }
my @top = Redis::zrange "leaderboard", 0, 9, withscores => 1, rev => 1

# Pub/sub publish (subscribe is streaming — coming in v2 once stryke has
# a unix-socket reader builtin).
Redis::publish "events", "new user signed up"

# SCAN — non-blocking iteration.
my @keys = Redis::scan match => "session:*", count => 100

# Server.
p to_json Redis::info section => "memory"
p Redis::dbsize
Redis::flushdb confirm => 1   # destructive — must pass confirm flag
```

Connection overrides on every public fn (`%opts`):

```stryke
my %prod = (
    url => "rediss://default:secret@prod.example.com:6379/0",
)
Redis::get "foo", %prod
```

Or compose from parts: `host`, `port`, `password`, `username`, `db`, `tls`.

## [0x03] Connection options

Every `Redis::*` op accepts `%opts` as its final argument. Connection
fields the cdylib understands (matching the v1 helper-binary flags):

```
url       → redis://… or rediss://… (TLS)
host      → 127.0.0.1
port      → 6379
db        → 0
username  → ""
password  → ""
tls       → 1 / 0 — force TLS without redis:// → rediss:// rewrite
```

Inline:

```stryke
Redis::set "rate-limit:user-42", "1",
    url => "rediss://prod:secret@host:6380/0", ex => 60, nx => 1
```

Or set once in the environment:

```sh
export REDIS_URL=redis://localhost:6379/0
```

```stryke
use Redis
$ENV{REDIS_URL} = "redis://localhost:6379/0"
my %conn = (url => $ENV{REDIS_URL})
Redis::set "k", "v", %conn
```

The cdylib caches one `redis::Connection` per `(url, db, auth)` tuple
for the life of the process — back-to-back calls with the same connection
options reuse the same TCP socket. No handshake-per-call.

## [0x04] API reference (selected)

```stryke
# KV
Redis::get        $key, %opts → $value | undef
Redis::set        $key, $value, %opts → 1 | ""    # opts: ex, px, nx, xx
Redis::del        $keys_or_aref, %opts → $count
Redis::exists     $keys_or_aref, %opts → $count
Redis::expire     $key, $seconds, %opts → 1 | 0
Redis::ttl        $key, %opts → $seconds          # -2 missing, -1 no ttl
Redis::type       $key, %opts → "string"|"list"|"set"|"zset"|"hash"|"stream"|"none"
Redis::incr       $key, %opts → $new              # opts: by
Redis::decr       $key, %opts → $new              # opts: by
Redis::mget       \@keys, %opts → @values
Redis::mset       \%pairs_or_aref, %opts → \%resp
Redis::keys       $pattern, %opts → @keys         # ⚠️ blocking
Redis::scan       %opts → @keys                   # opts: match, count, limit

# Lists
Redis::lpush      $key, $val_or_aref, %opts → $len
Redis::rpush      $key, $val_or_aref, %opts → $len
Redis::lpop       $key, %opts → $value | @values  # opts: count
Redis::rpop       $key, %opts → $value | @values
Redis::lrange     $key, $start, $stop, %opts → @values
Redis::llen       $key, %opts → $len

# Sets
Redis::sadd       $key, $m_or_aref, %opts → $added
Redis::srem       $key, $m_or_aref, %opts → $removed
Redis::smembers   $key, %opts → @members
Redis::sismember  $key, $member, %opts → 1 | 0
Redis::scard      $key, %opts → $size

# Hashes
Redis::hset       $key, $field, $value, %opts → 1 | 0
Redis::hget       $key, $field, %opts → $value | undef
Redis::hdel       $key, $fields_or_aref, %opts → $count
Redis::hgetall    $key, %opts → \%hash
Redis::hkeys      $key, %opts → @fields
Redis::hvals      $key, %opts → @values
Redis::hmget      $key, \@fields, %opts → @values
Redis::hmset      $key, \%pairs_or_aref, %opts → \%resp

# Sorted sets
Redis::zadd       $key, \%pairs_or_aref, %opts → $added
                                                   # { member => score, … } or [score, member, …]
Redis::zrange     $key, $start, $stop, %opts → @values | @{ {member, score}, … }
                                                   # opts: withscores, rev
Redis::zrem       $key, $m_or_aref, %opts → $removed
Redis::zcard      $key, %opts → $size
Redis::zscore     $key, $member, %opts → $score | undef

# Pub/sub + server
Redis::publish    $channel, $message, %opts → $subscriber_count
Redis::info       %opts → \%info                  # opts: section
Redis::dbsize     %opts → $count
Redis::flushdb    %opts → \%resp                  # require confirm => 1
Redis::ping       %opts → 1 | ""
Redis::raw        \@argv, %opts → \%resp          # arbitrary command
```

## [0x05] FFI layer

Each `Redis::*` wrapper builds a JSON args dict and calls a sibling
`redis__*` symbol resolved out of `libstryke_redis.{dylib,so}`. The
cdylib is dlopened in-process on first `use Redis` (via stryke's
`pkg::commands::try_load_ffi_for` resolver hook) and caches one
`redis::Connection` per `(url, db, auth)` tuple in
`OnceCell<Mutex<HashMap>>` for the life of the stryke process.

Wire shape:

* Scalars → `{"value": …}`
* Lists / sets / values → `{"values": [...]}` or `{"members": [...]}`
* Hashes → `{"hash": {...}}`
* Zset rows with scores → `{"pairs": [[member, score], ...]}`
* Errors → `{"error": "<msg>"}` — the wrapper `die`s with it

Set `STRYKE_REDIS_DEBUG=1` to log every call's request JSON to stderr.

## [0x06] Tests

```sh
cargo test                                    # compiles, no live calls
REDIS_URL=redis://localhost s test t/         # live round-trip suite
```

Test keys are scoped under `stryke:test:$$:` and cleaned at exit.

Local test server:

```sh
brew install redis        # macOS
brew services start redis
# or
docker run --rm -p 6379:6379 redis:7
```

## [0x07] Dev workflow

```sh
make             # release build
make debug
make test
make install
make clean
```

## [0x08] Layout

```
stryke-redis/
  stryke.toml                      # stryke package manifest ([ffi] table)
  Cargo.toml                       # cdylib crate manifest
  Makefile
  src/lib.rs                       # cdylib — redis__* extern "C" exports + persistent conn cache
  lib/
    Redis.stk                      # `use Redis` — thin wrapper around the FFI symbols
  t/
    test_redis.stk                 # live round-trip test suite
  tests/
    contract_cli_round4.rs         # Rust contract tests
  examples/
    kv.stk
    structures.stk
    publish.stk
  docs/
    index.html                     # docs site
    report.html
  .github/workflows/
    ci.yml                         # redis:7 service + live round-trip
    release.yml                    # cross-compile + GH release on tag push
```

## [0x09] Roadmap

| v1 (this release) | v2+ |
|---|---|
| All core data types + admin | Streaming `SUBSCRIBE` once stryke has Unix-socket builtin |
| Connection pooling per call | Pipelined ops + transactions (MULTI/EXEC) |
| Single-shot subprocess | Persistent serve-mode daemon |
| String/binary values | RESP3 + RedisJSON / RedisSearch / RedisGraph |
| `redis` 1.x sync API | Redis 8 Vector Sets + Bloom |

## [0xFF] License

MIT.
