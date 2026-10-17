from datetime import timedelta

from django.core.cache import cache, caches
from django.test import override_settings
from django.utils import timezone
from freezegun import freeze_time
from model_bakery import baker

from apps.event_ingest.tests.test_quotas import VALKEY_TEST_CACHES
from apps.event_ingest.tests.utils import run_async_closing
from apps.issue_events.models import IssueIndex
from glitchtip.partition_manager import UUID7Helper
from glitchtip.test_utils.test_case import GlitchTipTransactionTestCase

from ..constants import ISSUE_IDS_KEY
from ..models import Notification
from ..tasks import process_event_alerts


@override_settings(CACHES=VALKEY_TEST_CACHES, CACHE_IS_VALKEY=True)
class SchedulerBoundaryTestCase(GlitchTipTransactionTestCase):
    def setUp(self):
        super().setUp()
        self.create_user_and_project()
        cache.clear()
        self.now = timezone.now()
        self.alert = baker.make(
            "alerts.ProjectAlert", project=self.project, timespan_minutes=1, quantity=1
        )

    def record(self, times):
        issue = baker.make("issue_events.Issue", project=self.project)
        for received in times:
            baker.make(
                "issue_events.IssueEvent",
                issue=issue,
                organization=self.organization,
                id=UUID7Helper.from_datetime(received),
            )
        IssueIndex.objects.filter(issue=issue).update(last_seen=max(times))

        async def pending():
            await (
                caches["default"]
                .get_raw_client()
                .eval(
                    "return redis.call('SADD', KEYS[1], ARGV[1])",
                    [ISSUE_IDS_KEY],
                    [str(issue.pk).encode()],
                )
            )

        run_async_closing(pending)
        return issue

    def test_event_just_after_previous_check_survives_scheduler_drift(self):
        with freeze_time(self.now):
            process_event_alerts.call()
        issue = self.record([self.now + timedelta(milliseconds=200)])
        with freeze_time(self.now + timedelta(seconds=62)):
            process_event_alerts.call()
        self.assertEqual(Notification.objects.count(), 1)
        self.assertEqual(
            list(Notification.objects.get().issues.values_list("id", flat=True)),
            [issue.pk],
        )
        process_event_alerts.call()
        self.assertEqual(Notification.objects.count(), 1)

    def test_delayed_poll_preserves_rate_window_without_widening(self):
        self.alert.quantity = 2
        self.alert.save()
        qualifying = self.record([self.now, self.now + timedelta(seconds=59)])
        too_sparse = self.record([self.now, self.now + timedelta(seconds=61)])
        with freeze_time(self.now + timedelta(seconds=125)):
            process_event_alerts.call()
        notification = Notification.objects.get()
        self.assertTrue(notification.issues.filter(pk=qualifying.pk).exists())
        self.assertFalse(notification.issues.filter(pk=too_sparse.pk).exists())
