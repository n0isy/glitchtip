from datetime import timedelta

from django.conf import settings
from django.core.cache import caches
from django.db.models import Count, Q
from django.tasks import task
from django.utils import timezone

from apps.issue_events.models import Issue
from glitchtip.partition_manager import UUID7Helper

from .constants import ISSUE_IDS_KEY
from .models import Notification, ProjectAlert

# Lua script for atomic smembers + del
LUA_SCRIPT = """
local members = redis.call('SMEMBERS', KEYS[1])
redis.call('DEL', KEYS[1])
return members
"""


async def process_alert(project_alert_id: int, issue_ids: list[int]):
    notification = await Notification.objects.acreate(project_alert_id=project_alert_id)
    await notification.issues.aset(issue_ids)
    await send_notification.aenqueue(notification.pk)


@task
async def process_event_alerts():
    """Inspect alerts and determine if new notifications need sent"""
    now = timezone.now()

    issue_ids: list[int] | None = None
    # Support not having valkey, in theory
    if settings.CACHE_IS_VALKEY:
        # Note all recent issue_ids at ingest time. Then we can filter by them here.
        driver = caches["default"].get_raw_client()
        issue_ids = [int(x) for x in await driver.eval(LUA_SCRIPT, [ISSUE_IDS_KEY], [])]

    project_alerts = ProjectAlert.objects.filter(
        quantity__isnull=False, timespan_minutes__isnull=False
    ).select_related("project")
    if issue_ids == []:
        return  # There are no new issues, no work to do

    if issue_ids:
        project_alerts = project_alerts.filter(
            project__issues__id__in=issue_ids
        ).distinct()

    async for alert in project_alerts:
        start_time = now - timedelta(minutes=alert.timespan_minutes)
        # Pruning partition optimization
        start_uuid = UUID7Helper._uuid7_for_timestamp(start_time, min_random=True)
        quantity_in_timespan = alert.quantity
        issues = (
            Issue.objects.filter(
                project_id=alert.project_id,
                # Filter by UUIDv7 which encodes timestamp (enables partition pruning)
                issueevent__id__gte=start_uuid,
                issueevent__organization_id=alert.project.organization_id,
            )
            .exclude(notification__project_alert=alert)
            .annotate(num_events=Count("issueevent"))
            .filter(num_events__gte=quantity_in_timespan)
            .values_list("id", flat=True)
        )
        if issue_ids:
            # The minute scheduler can run a few seconds late. Measuring back
            # from execution time silently loses an event received immediately
            # after the previous check (especially a one-minute/one-event rule).
            # For pending issues, evaluate the actual event-rate window ending
            # at server receipt time, without widening the configured timespan.
            candidates = Issue.objects.filter(
                project_id=alert.project_id, id__in=issue_ids
            ).exclude(notification__project_alert=alert)
            issue_id_list = []
            batch = []
            async for issue_id, last_seen in candidates.values_list(
                "id", "index__last_seen"
            ):
                batch.append((issue_id, last_seen))
                if len(batch) == 200:
                    issue_id_list.extend(await pending_alert_issues(alert, batch))
                    batch = []
            if batch:
                issue_id_list.extend(await pending_alert_issues(alert, batch))
        else:
            issue_id_list = [i async for i in issues]
        if issue_id_list:
            notification = await alert.notification_set.acreate()
            await notification.issues.aset(issue_id_list)
            await send_notification.aenqueue(notification.pk)


async def pending_alert_issues(alert, candidates):
    windows = Q()
    for issue_id, last_seen in candidates:
        start = last_seen - timedelta(minutes=alert.timespan_minutes)
        windows |= Q(
            id=issue_id,
            issueevent__id__gte=UUID7Helper._uuid7_for_timestamp(
                start, min_random=True
            ),
            issueevent__id__lte=UUID7Helper._uuid7_for_timestamp(last_seen),
        )
    eligible = (
        Issue.objects.filter(
            windows,
            project_id=alert.project_id,
            issueevent__organization_id=alert.project.organization_id,
        )
        .exclude(notification__project_alert=alert)
        .annotate(num_events=Count("issueevent"))
        .filter(num_events__gte=alert.quantity)
        .values_list("id", flat=True)
    )
    return [issue_id async for issue_id in eligible]


@task
async def send_notification(notification_id: int):
    notification = await Notification.objects.select_related("project_alert").aget(
        pk=notification_id
    )
    await notification.send_notifications()
