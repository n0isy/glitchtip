# n0isy GlitchTip fork

Based on upstream MIT-licensed GlitchTip **v6.2.6**,
`d3039088e913a591a8e6544a2d2fffc865a9711a`, with upstream history and license retained.
Source: <https://gitlab.com/glitchtip/glitchtip-backend>.
This patch was authored and tested with OpenAI Codex assistance. It is a downstream
fork, not a submitted upstream contribution.

## Self-hosted ingest quotas

Quotas work with billing disabled. Configure every web replica identically:

```ini
GLITCHTIP_INGEST_RATE_LIMIT=600
GLITCHTIP_INGEST_GLOBAL_RATE_LIMIT=1200
GLITCHTIP_INGEST_RATE_WINDOW=60
GLITCHTIP_RUST_INGEST=false
```

- `GLITCHTIP_INGEST_RATE_LIMIT`: maximum admitted items per project per window;
  all DSNs and endpoints share this budget. Default `0` disables this limit.
- `GLITCHTIP_INGEST_GLOBAL_RATE_LIMIT`: maximum items for the whole installation;
  default `0` disables this limit.
- `GLITCHTIP_INGEST_RATE_WINDOW`: positive window duration in seconds; default `60`.
- Existing project-key `rate_limit_count` and `rate_limit_window` also apply.
  A missing/zero key window uses the configured default window.
- An active shared limit requires `VALKEY_URL`. All web replicas must use the
  same Valkey database/cache prefix. The fork rejects `GLITCHTIP_RUST_INGEST=true`
  at startup because that optional Rust fast path does not enforce these quotas.
  The normal Rust decompression and sourcemap machinery remain unchanged.

One atomic Lua operation checks all applicable counters, then charges them only
if all budgets permit the **whole** request. Windows start at the first admitted
item and expire after the configured interval. Rejected requests do not extend
the window or consume another scope's budget. Replies are `429` with integer
`Retry-After`; failure of the quota-counter operation returns `503` with
`Retry-After: 5`. Browser CORS exposes the retry headers.

The budget counts envelope events, transactions, feedback, user reports and
enabled log records; a log batch costs its number of records. A minidump-only
envelope costs one. `/store/`, `/security/`, `/minidump/` and native OTLP log
ingest use the same counters. Empty/unsupported/client-report-only envelopes
are ignored as before. Duplicate or malformed admitted payloads can consume
quota; this bounds attempted work and prevents refunds becoming an abuse path.
Sourcemap uploads, releases and management APIs are outside this ingest budget.
If an envelope exceeds the whole window's limit, split it or raise the limit:
retrying an oversized batch will continue to return `429`.

Valkey counters are temporary operational protection, not billing records.
Restarting or evicting their database can reset a window. Use a dedicated Valkey
with persistence and `maxmemory-policy noeviction` for deployment.

## Alert scheduler boundary

Upstream checks events in the last N minutes measured from scheduler execution.
A one-minute rule can lose an event received just after the previous pass if
the following pass runs a few seconds late. Pending issue IDs were then removed
from Valkey, so no later check could notify that issue.

For pending issues this fork measures the same N-minute interval from their
latest **server receipt time** (`IssueIndex.last_seen`), not from delayed poll
time. It does not widen the rate interval. Batches of at most 200 issue windows
bound each SQL query. Existing once-per-issue notification and regression
behavior remain in place. The cache-free fallback retains upstream behavior;
production should use Valkey. This is not a durable alert queue: upstream's
one-hour pending-set expiry and notification delivery semantics are unchanged.

## Build and upgrade

```sh
docker build -f Dockerfile.n0isy \
  --build-arg REVISION="$(git rev-parse HEAD)" \
  -t ghcr.io/n0isy/glitchtip:6.2.6-n0isy.1 .
```

The derivative uses the released image pinned by digest and copies only six
changed Python source files. It retains the released frontend, dependencies,
Rust extension, SDK wire formats and sourcemap implementation. No database
schema change or new dependency is introduced; update web/worker to the same
image, preserve volumes, and keep the standard upstream migration entrypoint.
Rollback is the original `glitchtip/glitchtip:6.2.6` image; counters may remain
in Valkey until their normal expiry, but upstream ignores them.

## Regression validation

Tests use isolated PostgreSQL 18 and Valkey 9, plus the exact v6.2.6 runtime.
The additional tests exercise concurrent atomic reservation, project/global/key
limits with billing disabled, DSN and endpoint switching, compressed multi-event
envelopes, whole-batch rejection, expiry, failure handling, and scheduler drift
without widening the configured rate window. Existing ingest, alerts, files,
release and sourcemap tests verify compatibility.

The test Valkey address is `redis://valkey:6379/15`; run in an isolated Compose
network. No production credentials or deployment configuration are included.

Validation on 2026-10-17: 13 new regression tests and 257 existing tests passed
(270 distinct tests across the runs). Four targeted regression scenarios fail
when the three affected upstream endpoint/scheduler files are restored, then
pass with the patch. `ruff check apps/ glitchtip/` passes. Repository-wide
`ruff check .` reports the same 105 pre-existing findings as v6.2.6; Vulture
reports the same two pre-existing findings (`sortBy`, `sender`). No unrelated
upstream lint cleanup is included.
