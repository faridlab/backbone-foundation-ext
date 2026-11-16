## Rules

- **MUST** read and follow the target repo's own `CLAUDE.md` when working across repos — before editing in another repo, read its rules; the more local `CLAUDE.md` always wins.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).

<!-- rust-rules:start (one identical copy in every Rust repo; full text: metaphora docs/handbook/rust-rules.md) -->
## Rust rules (NS01 to NS12)

Every Rust change in this repo follows the twelve Rust rules. The full text, with the reason and
the check for each rule, is the metaphora handbook page
[`docs/handbook/rust-rules.md`](https://github.com/faridlab/metaphora/blob/main/docs/handbook/rust-rules.md).
Cite the code (for example `NS04`) in reviews and commits.

- **NS01 Types carry the meaning:** newtypes and enums for ids, money and states; parse input once, at the boundary.
- **NS02 Never panic on input:** no `unwrap`, `expect`, indexing or unchecked arithmetic on request, file, env, row or message data; return a typed error.
- **NS03 Errors are typed, never swallowed:** `thiserror` in libraries, `anyhow` only in binaries; no `let _ =`, `.ok()` or `unwrap_or_default()` that hides a failure.
- **NS04 Ownership is designed, not appeased:** never add `clone`, `to_vec`, `to_owned`, `Rc`/`Arc`, `RefCell`/`Cell`, `Box`, indices, a split, a lifetime or `unsafe` only to make it compile. `Arc` for state shared across tasks is design, not appeasement.
- **NS05 Unsafe stays in one place:** `#![forbid(unsafe_code)]` by default; any exception lives in one module with `// SAFETY:` on every block.
- **NS06 Pure core, effects at the edge:** domain logic does no database, network, clock or env access; the edge passes those in.
- **NS07 Structure:** one concept per module, private or `pub(crate)` by default, dependencies point inward, generated code and hand code (`*.ext.rs`) kept apart.
- **NS08 Comments and naming:** domain names, comments say why, every public item documented, no session jargon.
- **NS09 Reuse before writing:** search std, current dependencies and the framework first; promote shared code upstream, never copy it.
- **NS10 Dependencies:** each new crate justified, unused default features off, `cargo deny` clean.
- **NS11 Proof, tests, speed:** a fix starts with a test that fails on the old code; a speed claim comes with a measurement.
- **NS12 The CI gate:** `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings -W clippy::pedantic`, `cargo test` and `cargo deny check` all pass; every `#[allow]` says why. Adopted repo by repo: until this repo runs it, reviewers enforce it by hand.
<!-- rust-rules:end -->
