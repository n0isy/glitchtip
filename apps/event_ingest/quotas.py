"""Atomic self-hosted ingest budgets, independent of billing and SDK sampling."""

import logging

import orjson
from django.conf import settings
from django.core.cache import caches
from django.http import HttpResponse

from apps.projects.models import ProjectKey

from .authentication import auth_from_request

logger = logging.getLogger(__name__)

# All limits are checked before any counter is charged. Redis executes the
# script atomically, so workers, DSNs and ingest endpoints share one budget.
# Windows start on the first accepted event; rejected requests never extend it.
RESERVE_LUA = """
local cost = tonumber(ARGV[1])
local retry_ms = 0
for i, key in ipairs(KEYS) do
    local limit = tonumber(ARGV[i * 2])
    local window_ms = tonumber(ARGV[i * 2 + 1])
    local used = tonumber(redis.call('GET', key) or '0')
    if used + cost > limit then
        local remaining = redis.call('PTTL', key)
        if remaining < 1 then remaining = window_ms end
        retry_ms = math.max(retry_ms, remaining)
    end
end
if retry_ms > 0 then return math.ceil(retry_ms / 1000) end
for i, key in ipairs(KEYS) do
    local window_ms = tonumber(ARGV[i * 2 + 1])
    local used = redis.call('INCRBY', key, cost)
    if used == cost then redis.call('PEXPIRE', key, window_ms) end
end
return 0
"""


def quota_response(status, retry_after):
    response = HttpResponse(
        "Too Many Requests" if status == 429 else "Ingest quota service unavailable",
        status=status,
    )
    response["Retry-After"] = str(retry_after)
    return response


async def reserve_ingest_quota(request, project, count=1):
    """Reserve the complete request before enqueueing anything, or return 429/503.

    Count admitted items, including duplicates/invalid payloads that passed
    framing. Refunding these would let a hostile sender evade the budget.
    Empty/client-report-only envelopes do not spend the event budget.
    """
    if count <= 0:
        return None

    window = settings.GLITCHTIP_INGEST_RATE_WINDOW
    limits = []
    if settings.GLITCHTIP_INGEST_GLOBAL_RATE_LIMIT:
        limits.append(("global", settings.GLITCHTIP_INGEST_GLOBAL_RATE_LIMIT, window))
    if settings.GLITCHTIP_INGEST_RATE_LIMIT:
        limits.append(
            (f"project:{project.id}", settings.GLITCHTIP_INGEST_RATE_LIMIT, window)
        )

    # Read the current DSN quota so UI changes take effect immediately. Project
    # authentication has already checked validity and activity before this call.
    key = (
        await ProjectKey.objects.filter(
            project_id=project.id, public_key=auth_from_request(request)
        )
        .values("pk", "rate_limit_count", "rate_limit_window")
        .aget()
    )
    if key["rate_limit_count"]:
        limits.append(
            (
                f"key:{key['pk']}",
                key["rate_limit_count"],
                key["rate_limit_window"] or window,
            )
        )
    if not limits:
        return None
    if not settings.CACHE_IS_VALKEY:
        return quota_response(503, 5)

    # Hash tag makes all scopes land in one Redis Cluster slot. Cache prefix
    # separates deployments that share a Valkey database. Window is in the key
    # so changing its duration cannot inherit an incompatible expiry.
    cache = caches["default"]
    keys = [
        cache.make_key(f"{{ingest-quota}}:{scope}:{seconds}")
        for scope, _, seconds in limits
    ]
    args = [str(count)]
    for _, limit, seconds in limits:
        args.extend((str(limit), str(seconds * 1000)))
    try:
        retry_after = int(
            await cache.get_raw_client().eval(
                RESERVE_LUA, keys, [arg.encode() for arg in args]
            )
        )
    except Exception:
        logger.exception("Unable to reserve ingest quota")
        return quota_response(503, 5)
    if retry_after:
        return quota_response(429, retry_after)
    return None


def envelope_cost(envelope):
    """Count work before dispatch, including multi-event and batched-log envelopes."""
    count = 0
    has_event = False
    has_minidump = False
    for item in envelope.items:
        try:
            header = orjson.loads(item.header)
        except orjson.JSONDecodeError:
            continue
        if not isinstance(header, dict):
            continue
        item_type = header.get("type")
        if item_type in ("event", "transaction"):
            has_event = True
            count += 1
        elif (
            item_type in ("feedback", "user_report")
            or item_type == "otel_log"
            and settings.GLITCHTIP_ENABLE_LOGS
        ):
            count += 1
        elif item_type == "log" and settings.GLITCHTIP_ENABLE_LOGS:
            try:
                payload = orjson.loads(item.payload)
                count += max(1, len(payload.get("items", [])))
            except (orjson.JSONDecodeError, TypeError, AttributeError):
                count += 1
        elif (
            item_type == "attachment"
            and header.get("attachment_type") == "event.minidump"
        ):
            has_minidump = True
    return count + int(has_minidump and not has_event)
