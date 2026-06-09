//! Round 4 integration tests pinned the v1 helper-binary CLI contract
//! (`stryke-redis-helper --help`, exit codes, subcommand routing).
//!
//! v0.2.0 retired that binary in favor of an in-process cdylib loaded by
//! stryke via dlopen — there is no longer a CLI surface to contract-test.
//! The exports are exercised end-to-end by:
//!   * `t/test_redis.stk` — live round-trip against a Redis server.
//!   * The `Redis::*` `.stk` wrappers themselves — calling a missing
//!     export fails loud at `redis__<op>(...)` resolution.
//!
//! This file is preserved (per repo convention: never delete test files)
//! and replaced with a single sanity test so `cargo test` stays green.

#[test]
fn cdylib_replacement_for_helper_binary_compiles() {
    // If this file compiles, the rest of the crate compiled too — meaning
    // the cdylib's `extern "C"` exports passed type-check. That's the
    // minimum substitute contract for what the helper binary's
    // `--help`/`--version` tests used to assert about the v1 entry point.
}
