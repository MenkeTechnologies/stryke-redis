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

### `[REDIS / VALKEY CLIENT FOR STRYKE // KV + LISTS + SETS + HASHES + ZSETS + STREAMS + GEO + SCRIPTING + BITMAPS + HLL + PUB/SUB + PIPELINE + SCAN]`

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

# Key management
Redis::rename     $key, $newkey, %opts → 1        # renamenx → 1|0
Redis::persist    $key, %opts → 1 | 0
Redis::pexpire    $key, $millis, %opts → 1 | 0    # pttl → $ms; expiretime → $unix_s
Redis::expireat   $key, $unix_seconds, %opts → 1  # pexpireat → ms variant
Redis::touch      $keys_or_aref, %opts → $count   # unlink → async del
Redis::copy       $src, $dst, %opts → 1 | 0       # opts: replace, destination_db
Redis::randomkey  %opts → $key | undef
Redis::object_encoding $key, %opts → $encoding | undef

# String extras + bitmaps
Redis::getset     $key, $value, %opts → $old      # getdel → get+del
Redis::append     $key, $value, %opts → $len      # strlen → $len
Redis::setex      $key, $seconds, $value, %opts → 1   # opts: px (ms)
Redis::setnx      $key, $value, %opts → 1 | 0
Redis::getrange   $key, $start, $end, %opts → $substr  # setrange → $len
Redis::incrbyfloat $key, $by, %opts → $new
Redis::setbit     $key, $offset, $bit, %opts → $prior  # getbit → bit
Redis::bitcount   $key, %opts → $n                # opts: start, end
Redis::bitop      $op, $dst, $keys_or_aref, %opts → $len  # AND|OR|XOR|NOT

# List extras
Redis::lindex     $key, $index, %opts → $value | undef
Redis::lset       $key, $index, $value, %opts → 1
Redis::linsert    $key, $pivot, $value, %opts → $len   # opts: before
Redis::lrem       $key, $count, $value, %opts → $removed
Redis::ltrim      $key, $start, $stop, %opts → 1
Redis::rpoplpush  $src, $dst, %opts → $value      # lmove → opts: from, to

# Hash extras
Redis::hexists    $key, $field, %opts → 1 | 0
Redis::hincrby    $key, $field, $by, %opts → $new # opts: float
Redis::hlen       $key, %opts → $count
Redis::hsetnx     $key, $field, $value, %opts → 1 | 0

# Set algebra
Redis::spop       $key, %opts → $member | @members    # opts: count
Redis::srandmember $key, %opts → $member | @members   # opts: count
Redis::smove      $src, $dst, $member, %opts → 1 | 0
Redis::sinter     $keys_or_aref, %opts → @members     # sunion, sdiff

# Sorted set extras
Redis::zincrby    $key, $by, $member, %opts → $new
Redis::zrank      $key, $member, %opts → $rank | undef # opts: rev
Redis::zcount     $key, %opts → $n                     # opts: min, max
Redis::zrangebyscore $key, %opts → @values | @pairs    # opts: min,max,with_scores,rev,limit_*
Redis::zpopmin    $key, %opts → @pairs                 # zpopmax; opts: count
Redis::zremrangebyrank  $key, $start, $stop, %opts → $removed
Redis::zremrangebyscore $key, %opts → $removed         # opts: min, max
Redis::zmscore    $key, $members_or_aref, %opts → @scores

# HyperLogLog
Redis::pfadd      $key, $elements_or_aref, %opts → 1 | 0
Redis::pfcount    $keys_or_aref, %opts → $approx_card
Redis::pfmerge    $dst, $sources_or_aref, %opts → 1

# Streams
Redis::xadd       $key, \%fields, %opts → $id          # opts: id (default "*")
Redis::xlen       $key, %opts → $count
Redis::xrange     $key, %opts → @entries               # opts: start,end,count,rev
Redis::xdel       $key, $ids_or_aref, %opts → $removed
Redis::xtrim      $key, %opts → $removed               # opts: maxlen | minid
Redis::xread      \%streams, %opts → \%resp            # {key=>id}; opts: count, block

# Geospatial
Redis::geoadd     $key, [[lon,lat,name],…], %opts → $added
Redis::geopos     $key, $members_or_aref, %opts → @[lon,lat]
Redis::geodist    $key, $m1, $m2, %opts → $dist        # opts: unit (m/km/mi/ft)
Redis::geosearch  $key, %opts → @results               # from_member|lon+lat; radius|width+height

# Scripting
Redis::eval       $script, %opts → $result             # opts: keys, args (arefs)
Redis::evalsha    $sha, %opts → $result                # opts: keys, args
Redis::script_load $script, %opts → $sha1
Redis::script_exists $shas_or_aref, %opts → @bools

# Pub/sub + server
Redis::publish    $channel, $message, %opts → $subscriber_count
Redis::pubsub_channels %opts → @channels          # opts: pattern
Redis::pubsub_numsub   $channels_or_aref, %opts → \%counts
Redis::info       %opts → \%info                  # opts: section
Redis::dbsize     %opts → $count
Redis::flushdb    %opts → \%resp                  # require confirm => 1
Redis::flushall   %opts → 1                        # opts: async
Redis::time       %opts → ($seconds, $micros)
Redis::config_get $parameter, %opts → \%config    # config_set → 1
Redis::memory_usage $key, %opts → $bytes | undef
Redis::echo       $message, %opts → $message
Redis::ping       %opts → 1 | ""
Redis::raw        \@argv, %opts → \%resp          # arbitrary command

# Cursor scans (iterate internally; return everything)
Redis::hscan      $key, %opts → %hash             # opts: match, count
Redis::sscan      $key, %opts → @members          # opts: match, count
Redis::zscan      $key, %opts → @pairs            # opts: match, count

# Stream consumer groups
Redis::xgroup_create $key, $group, %opts → 1      # opts: id (default $), mkstream
Redis::xack          $key, $group, $ids_or_aref, %opts → $count
Redis::xinfo_stream  $key, %opts → \%info

# Pipeline / transaction
Redis::pipeline   [[CMD,…],…], %opts → @results  # opts: transaction (MULTI/EXEC)

# Server admin + introspection
Redis::wait            $numreplicas, %opts → $acked   # opts: timeout (ms)
Redis::lastsave        %opts → $unix_time
Redis::slowlog_get     %opts → \@entries              # opts: count
Redis::slowlog_reset   %opts → 1
Redis::client_list     %opts → $text                  # one line per client
Redis::client_info     %opts → $text
Redis::acl_whoami      %opts → $username
Redis::acl_list        %opts → @rules
Redis::acl_cat         %opts → @categories            # opts: category
Redis::object_idletime $key, %opts → $seconds | undef
Redis::object_refcount $key, %opts → $n | undef

# Redis 6.2 / 7.x
Redis::getex      $key, %opts → $value | undef        # opts: ex, px, persist
Redis::smismember $key, $members_or_aref, %opts → @bools
Redis::sintercard $keys_or_aref, %opts → $n           # opts: limit
Redis::lpos       $key, $element, %opts → $idx | @idxs # opts: rank, count
Redis::lmpop      $keys_or_aref, %opts → [key, [elems]] | undef  # opts: from (LEFT|RIGHT), count
Redis::zmpop      $keys_or_aref, %opts → [key, [[m,score]]] | undef  # opts: from (MIN|MAX), count
```

### Pure helpers (no connection)

```stryke
Redis::parse_url($url)            → { scheme, tls, user, password, host, port, db }   # redis[s]://…
Redis::build_url(%opts)           → $url      # parts → redis[s]:// URL; inverse of parse_url (opts: host, port, db, user, password, tls)
Redis::glob_match($pattern, $key) → 1 | ""   # Redis KEYS/SCAN glob (* ? [a-z] [^…] \), matched client-side
Redis::glob_escape($value)        → $escaped # backslash-escape * ? [ ] \ so glob_match treats $value literally
Redis::glob_unescape($escaped)    → $value   # inverse of glob_escape: \* \? \[ \] \\ → literal chars (single left-to-right scan)
Redis::cluster_keyslot($key)      → { key, slot, hash_tag }   # CLUSTER KEYSLOT: crc16(hash_tag(key)) % 16384, honors {…} hash tags
Redis::same_slot($a, $b)          → { a, b, slot_a, slot_b, same_slot }   # do two keys co-locate (avoid CROSSSLOT in multi-key ops)?
Redis::parse_stream_id($id)       → { ms, seq, special }   # stream entry id <ms>-<seq>; special - + $ * → min/max/last/auto
Redis::build_stream_id(%opts)     → $id   # { ms, seq? } or { special } → stream entry id; inverse of parse_stream_id
Redis::compare_stream_id($a, $b)  → -1|0|1   # order two stream ids by (ms, seq); bare <ms> → seq 0; - + below/above; $ * die
Redis::next_stream_id($id)        → $id   # successor of a concrete entry id (exclusive XRANGE bound); seq+1, rolls <ms>-MAX → <ms+1>-0
Redis::prev_stream_id($id)        → $id   # predecessor (exclusive XREVRANGE bound); seq-1, borrows <ms>-0 → <ms-1>-MAX; mirror of next_stream_id
```

`glob_match` is a faithful port of Redis's `stringmatchlen` — use it to filter
keys locally without a round trip.

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
