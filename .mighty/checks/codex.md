---
name: codex
agent: codex
---
Review the pull request for correctness, regressions, and missing verification
of changed behavior. Read the repository's instructions and governing contracts
before judging the implementation. Trace each finding to the changed code and
its observable consequence. Report actionable findings with file and line
references, explaining the conditions under which the problem occurs.

When the prompt names a governing graph branch, read the contracts and
evidence there, passing that branch as `scope` to `get_document` and
`list_documents` (and as `branch` to `list_branches`). A specification or
evidence update on that branch counts as present; do not ask for it on main.

Work through this checklist before writing the review. Each item names what
should be true of the change and says when it applies. In the review body, add
a `## Checklist` section with one line per item: `pass` when it is true,
`fail` when it is not, or `n/a` when it does not apply, with the file and line
or the reason. A failed item is also reported as a finding.

1. **In-flight work and the previous release keep working.** Applies when the
   change alters a Restate handler's sequence of journaled calls or sends, a
   wire struct, enum or WIT variant, a required field, a persisted key or id,
   a UI contribution or URL id, or a stored cursor. Trace an invocation
   journaled by the old code through the new code, the new code running beside
   the previous release, and the old code running after a rollback. A journal
   mismatch, a decode error, a reader that ignores the old key, a result that
   replays differently, or a cursor the previous release still needs being
   deleted fails.
2. **Each error is retried or made terminal according to its kind.** Applies
   when the change adds or edits error handling, retry policy or terminal
   states. For each error the call can return, confirm whether it is retryable
   or permanent. A permanent refusal retried forever (wedging a per-PR object
   or command queue), a transient failure stored as a terminal result, a retry
   loop with no backoff, or an error reported as the wrong kind fails.
3. **Each check resolves the caller's live principal.** Applies when the
   change authenticates a request, checks membership, picks a credential or
   profile, or lists data across Spaces. Confirm the check resolves the live
   principal (live root membership, not a cached or self row), keeps the
   principal it authenticated, scopes listings to the caller's Space, and
   sends each credential only to its own origin. Treating a token's presence
   as authentication, or a grant hardcoded on, fails.
4. **Earlier side effects stay safe when a later step fails.** Applies when
   the change persists a marker, claim, identity or reservation, or releases
   capacity, and then runs something that can fail. Confirm each later failure
   rolls the earlier effect back or leaves it safe to retry. A delivered
   marker written before delivery, a reservation released before a retry that
   still uses it, or a claim left active after a stop fails.
5. **Concurrent writers cannot race or deadlock.** Applies when the change
   takes locks, compares-and-swaps a ledger, or reads, modifies and writes a
   whole record. Confirm every path takes locks in the same order, every CAS
   fences the state it depends on, concurrent same-id requests replay rather
   than conflict, and no whole-record write can drop a concurrent update.
6. **Cost grows with the request, not with all stored data.** Applies when the
   change adds a poll, heartbeat, listing, cleanup or per-request walk. Check
   whether it rewrites a shared ledger or an ever-growing array, or lists or
   walks all stored data on every call or idle tick. Work that grows with
   total stored data on a hot or periodic path fails.
7. **Companion registrations and built artifacts change with the code.**
   Applies when the change adds a plugin, migration, route, dependency, script
   or UI source. Confirm the static plugin manifests and deploy-time migration
   registration, production route wiring, rebuilt `dist` bundles,
   `Cargo.lock`, executable bits, and release-tool companions all change with
   it.
8. **Every new or changed query is served by an index.** Applies when the
   change adds or edits a SQL query, a state-store or kernel key-value
   listing, or a migration that adds or changes an index. Trace the query's
   filter and sort columns against the table's primary key and indexes, and
   note how often it runs. A filter the key cannot serve (a function applied
   to a key column, or a match on a column the index does not lead with), a
   query issued per row, or an index test that checks only the index's name or
   SQL text instead of checking its columns against the production query,
   fails.
