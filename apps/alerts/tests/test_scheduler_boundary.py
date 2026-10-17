from datetime import timedelta
from uuid import uuid4

from django.core.cache import cache, caches
from django.tasks import task_backends
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

    def test_client_clock_does_not_change_server_receipt_window(self):
        received = self.now + timedelta(milliseconds=200)
        key = self.project.projectkey_set.first()
        with freeze_time(received):
            for clock_offset in (-365, 365):
                response = self.client.post(
                    f"/api/{self.project.pk}/store/?sentry_key={key.public_key}",
                    {
                        "event_id": uuid4().hex,
                        "timestamp": (
                            self.now + timedelta(days=clock_offset)
                        ).timestamp(),
                        "message": f"client clock offset {clock_offset}",
                        "fingerprint": [str(clock_offset)],
                    },
                    content_type="application/json",
                )
                self.assertEqual(response.status_code, 200)
            task_backends["default"].flush_batches()
        self.assertEqual(
            list(self.project.issues.values_list("index__last_seen", flat=True)),
            [received, received],
        )
        with freeze_time(self.now + timedelta(seconds=62)):
            process_event_alerts.call()
        self.assertEqual(Notification.objects.get().issues.count(), 2)

    def test_batch_boundary_and_multiple_rules_keep_all_pending_issues(self):
        second = baker.make(
            "alerts.ProjectAlert", project=self.project, timespan_minutes=1, quantity=2
        )
        for _ in range(201):
            self.record([self.now, self.now + timedelta(seconds=59)])
        with freeze_time(self.now + timedelta(seconds=125)):
            process_event_alerts.call()
        self.assertEqual(Notification.objects.count(), 2)
        for alert in (self.alert, second):
            self.assertEqual(
                Notification.objects.get(project_alert=alert).issues.count(), 201
            )
        process_event_alerts.call()
        self.assertEqual(Notification.objects.count(), 2)
