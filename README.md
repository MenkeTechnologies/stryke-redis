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
- [\[0x03\] CLI: `redis`](#0x03-cli-redis)
- [\[0x04\] API reference (selected)](#0x04-api-reference-selected)
- [\[0x05\] Helper protocol](#0x05-helper-protocol)
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

```sh
cd ~/projects/stryke-redis
cargo build --release
s pkg install -g .
```

Or:

```sh
make install
```

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

## [0x03] CLI: `redis`

```sh
redis ping
redis set    foo bar --ex=60 --nx
redis get    foo
redis mset   a 1 b 2 c 3
redis mget   a b c
redis incr   counter --by=5
redis del    a b c counter
redis exists foo bar
redis ttl    session:42

redis lpush  events a b c
redis lrange events 0 -1
redis lpop   events --count=3

redis sadd   tags rust stryke
redis smembers tags

redis hmset  user:42 name alice age 30
redis hgetall user:42
redis hmget  user:42 name age

redis zadd   leaderboard 100 alice 200 bob
redis zrange leaderboard 0 -1 --withscores --rev

redis publish my-channel "hello"

redis scan   --match='session:*' --count=100 --limit=1000
redis keys   'session:*'          # ⚠️ blocking; use scan in prod
redis type   foo

redis info   memory
redis dbsize
redis flushdb --confirm
redis raw    EVAL "return KEYS[1]" 1 hello       # arbitrary command

redis build                                       # cargo build --release
redis version
```

Global flags (also via env vars):

```
-u, --url URL              $REDIS_URL          (redis://… or rediss://…)
-H, --host HOST            $REDIS_HOST
-P, --port PORT            $REDIS_PORT
-p, --password PW          $REDIS_PASSWORD
    --username U           $REDIS_USERNAME
-D, --db N                 $REDIS_DB
    --tls                  force TLS connection
```

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

## [0x05] Helper protocol

```sh
stryke-redis-helper set foo bar --ex=60
stryke-redis-helper mget a b c
stryke-redis-helper -u rediss://prod:secret@host/0 zadd lb 100 alice 200 bob
stryke-redis-helper scan --match='session:*' --count=100 --limit=1000
stryke-redis-helper raw EVAL 'return KEYS[1]' 1 hello
```

Output:

* Scalars → `{"value": …}`
* Lists/sets/values → `{"values": [...]}` or `{"members": [...]}`
* Hashes → JSON object directly
* Zset rows with scores → `{"values": [{"member","score"}, …]}`
* Streaming (scan, keys) → NDJSON `{"key": ...}` per line

Binary values that aren't valid UTF-8 come back as `"base64:..."` strings.

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
  stryke.toml                      # stryke package manifest
  Cargo.toml                       # Rust helper crate manifest
  Makefile
  src/main.rs                      # single-file helper binary
  lib/
    Redis.stk                      # `use Redis`
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
