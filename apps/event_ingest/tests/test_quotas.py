import asyncio
import gzip
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from django.core.cache import cache, caches
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import RequestFactory, override_settings
from model_bakery import baker

from apps.event_ingest.quotas import reserve_ingest_quota

from .utils import EventIngestTestCase, run_async_closing

# Isolated Valkey database: exercise the real atomic script, not a mock counter.
VALKEY_TEST_CACHES = {
    "default": {
        "BACKEND": "django_vcache.backend.ValkeyCache",
        "LOCATION": "redis://valkey:6379/15",
        "OPTIONS": {"DRIVER_CLASS": "gt_rust.valkey.RustValkeyDriver"},
    }
}


@override_settings(
    CACHES=VALKEY_TEST_CACHES,
    CACHE_IS_VALKEY=True,
    GLITCHTIP_INGEST_RATE_LIMIT=3,
    GLITCHTIP_INGEST_GLOBAL_RATE_LIMIT=0,
    GLITCHTIP_INGEST_RATE_WINDOW=60,
    BILLING_ENABLED=False,
)
class IngestQuotaTestCase(EventIngestTestCase):
    def setUp(self):
        super().setUp()
        cache.clear()

    def envelope(self, count=1, key=None, compressed=False):
        lines = [{}]
        for _ in range(count):
            lines.extend(
                [
                    {"type": "event"},
                    {"event_id": uuid4().hex, "message": "quota regression"},
                ]
            )
        body = ("\n".join(json.dumps(line) for line in lines) + "\n").encode()
        extra = {}
        if compressed:
            body = gzip.compress(body)
            extra["HTTP_CONTENT_ENCODING"] = "gzip"
        return self.client.post(
            f"/api/{self.project.pk}/envelope/?sentry_key={key or self.projectkey.public_key}",
            body,
            content_type="application/x-sentry-envelope",
            **extra,
        )

    def store(self):
        return self.client.post(
            f"/api/{self.project.pk}/store/{self.params}",
            {"event_id": uuid4().hex, "message": "quota regression"},
            content_type="application/json",
        )

    def request(self, key=None):
        return RequestFactory().post(
            f"/?sentry_key={key or self.projectkey.public_key}"
        )

    def test_envelope_and_store_share_project_budget_across_dsns(self):
        second_key = baker.make("projects.ProjectKey", project=self.project)
        with patch(
            "apps.event_ingest.views.ingest_event", new_callable=AsyncMock
        ) as enqueue:
            self.assertEqual(self.envelope(2).status_code, 200)
            self.assertEqual(self.store().status_code, 200)
            rejected = self.envelope(key=second_key.public_key)
            self.assertEqual(rejected.status_code, 429)
            self.assertTrue(1 <= int(rejected["Retry-After"]) <= 60)
            self.assertEqual(self.store().status_code, 429)
            self.assertEqual(enqueue.aenqueue.await_count, 2)

    def test_rejects_entire_batch_without_spending_partial_quota(self):
        with patch(
            "apps.event_ingest.views.ingest_event", new_callable=AsyncMock
        ) as enqueue:
            self.assertEqual(self.envelope(4).status_code, 429)
            enqueue.aenqueue.assert_not_awaited()
            self.assertEqual(self.envelope(3, compressed=True).status_code, 200)
            self.assertEqual(enqueue.aenqueue.await_count, 3)

    def test_invalid_key_cannot_spend_quota(self):
        self.assertEqual(self.envelope(key=uuid4()).status_code, 403)
        with patch("apps.event_ingest.views.ingest_event", new_callable=AsyncMock):
            self.assertEqual(self.envelope(3).status_code, 200)

    @override_settings(GLITCHTIP_INGEST_RATE_LIMIT=0)
    def test_existing_key_quota_is_enforced_without_billing(self):
        self.projectkey.rate_limit_count = 1
        self.projectkey.rate_limit_window = 30
        self.projectkey.save()
        with patch("apps.event_ingest.views.ingest_event", new_callable=AsyncMock):
            self.assertEqual(self.store().status_code, 200)
            response = self.envelope()
        self.assertEqual(response.status_code, 429)
        self.assertTrue(1 <= int(response["Retry-After"]) <= 30)

    @override_settings(GLITCHTIP_INGEST_RATE_LIMIT=7)
    def test_concurrent_reservations_never_exceed_budget(self):
        async def run():
            results = await asyncio.gather(
                *[
                    reserve_ingest_quota(
                        self.request(), SimpleNamespace(id=self.project.pk)
                    )
                    for _ in range(40)
                ]
            )
            self.assertEqual(sum(response is None for response in results), 7)
            self.assertEqual(
                sum(
                    response is not None and response.status_code == 429
                    for response in results
                ),
                33,
            )

        run_async_closing(run)

    @override_settings(GLITCHTIP_INGEST_GLOBAL_RATE_LIMIT=1)
    def test_global_budget_is_shared_and_rejection_does_not_charge_project(self):
        other = baker.make("projects.Project")
        other_key = other.projectkey_set.first()

        async def run():
            first = await reserve_ingest_quota(self.request(), self.project)
            second = await reserve_ingest_quota(
                self.request(other_key.public_key), other
            )
            self.assertIsNone(first)
            self.assertEqual(second.status_code, 429)
            with override_settings(GLITCHTIP_INGEST_GLOBAL_RATE_LIMIT=0):
                self.assertIsNone(
                    await reserve_ingest_quota(
                        self.request(other_key.public_key), other, 3
                    )
                )

        run_async_closing(run)

    @override_settings(GLITCHTIP_INGEST_RATE_LIMIT=1, GLITCHTIP_INGEST_RATE_WINDOW=1)
    def test_quota_recovers_after_expiry(self):
        async def run():
            self.assertIsNone(await reserve_ingest_quota(self.request(), self.project))
            self.assertEqual(
                (await reserve_ingest_quota(self.request(), self.project)).status_code,
                429,
            )
            await asyncio.sleep(1.1)
            self.assertIsNone(await reserve_ingest_quota(self.request(), self.project))

        run_async_closing(run)

    def test_unavailable_counter_fails_closed(self):
        async def run():
            with patch.object(
                caches["default"], "get_raw_client", side_effect=ConnectionError
            ):
                response = await reserve_ingest_quota(self.request(), self.project)
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response["Retry-After"], "5")

        run_async_closing(run)

    def test_empty_envelopes_do_not_spend_budget(self):
        self.assertEqual(self.envelope(0).status_code, 200)
        with patch("apps.event_ingest.views.ingest_event", new_callable=AsyncMock):
            self.assertEqual(self.envelope(3).status_code, 200)

    def test_security_and_minidump_cannot_bypass_project_budget(self):
        async def exhaust():
            self.assertIsNone(
                await reserve_ingest_quota(self.request(), self.project, 3)
            )

        run_async_closing(exhaust)
        csp = self.get_json_data(
            "apps/event_ingest/tests/test_data/csp/mozilla_example.json"
        )
        response = self.client.post(
            f"/api/{self.project.pk}/security/{self.params}",
            csp,
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 429)
        response = self.client.post(
            f"/api/{self.project.pk}/minidump/{self.params}",
            {
                "upload_file_minidump": SimpleUploadedFile(
                    "crash.dmp", b"MDMP" + b"\0" * 32
                )
            },
        )
        self.assertEqual(response.status_code, 429)

    @override_settings(GLITCHTIP_ENABLE_LOGS=True)
    def test_otlp_records_share_envelope_budget(self):
        from .test_otlp_logs import _build_batch_request

        with patch(
            "apps.event_ingest.otlp_views.ingest_logs", new_callable=AsyncMock
        ) as task:
            response = self.client.post(
                "/v1/logs",
                _build_batch_request(3),
                content_type="application/x-protobuf",
                HTTP_AUTHORIZATION=f"Bearer {self.projectkey.public_key}",
            )
        self.assertEqual(response.status_code, 200)
        task.aenqueue.assert_awaited_once()
        self.assertEqual(self.envelope().status_code, 429)
