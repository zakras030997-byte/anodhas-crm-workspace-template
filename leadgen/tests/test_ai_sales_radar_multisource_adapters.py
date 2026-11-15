from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import traceback
import unittest

from pydantic import SecretStr, ValidationError

from ai_sales_radar.multisource.adapters import (
    AdapterAuthorizationError,
    AdapterConfigurationError,
    AdapterPayloadError,
    AdapterRateLimitError,
    AdapterRedirectError,
    AdapterResponseError,
    AdapterTimeoutError,
    AdapterTransientError,
    FNSAccessMode,
    FNSRecordRequest,
    FNSVerifierAdapter,
    HttpHeader,
    HttpRequest,
    HttpResponse,
    OfficialFNSRecord,
    OfficialWebsiteReader,
    ProviderEntitlement,
    ProviderName,
    RequestBudget,
    RequestBudgetExceeded,
    TWO_GIS_PLACES_ENDPOINT,
    TwoGisConfig,
    TwoGisPlacesAdapter,
    WebsiteReadRequest,
    WebsiteReaderConfig,
    WebsiteRedirectPolicy,
    YANDEX_MAPS_SEARCH_ENDPOINT,
    YandexMapsAdapter,
    YandexMapsConfig,
)


NOW = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)
PROJECT_ROOT = Path(__file__).resolve().parents[1]


class FakeHttpTransport:
    def __init__(self, *responses: HttpResponse | BaseException) -> None:
        self.responses = list(responses)
        self.requests: list[HttpRequest] = []

    async def request(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        if not self.responses:
            raise AssertionError("unexpected transport call")
        value = self.responses.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value


class FakePinnedHttpTransport(FakeHttpTransport):
    def __init__(self, *responses: HttpResponse | BaseException) -> None:
        super().__init__(*responses)
        self.connect_ips: list[str] = []

    async def request_to_validated_ip(
        self,
        request: HttpRequest,
        *,
        connect_ip: str,
    ) -> HttpResponse:
        self.connect_ips.append(connect_ip)
        return await self.request(request)


class FakeResolver:
    def __init__(self, *addresses: str) -> None:
        self.addresses = tuple(addresses)
        self.calls: list[str] = []

    async def resolve(self, host: str) -> tuple[str, ...]:
        self.calls.append(host)
        return self.addresses


class FakeFNSRecordTransport:
    def __init__(self, record: OfficialFNSRecord | None) -> None:
        self.record = record
        self.requests: list[FNSRecordRequest] = []

    async def fetch(self, request: FNSRecordRequest) -> OfficialFNSRecord | None:
        self.requests.append(request)
        return self.record


def json_response(payload: object, *, status: int = 200) -> HttpResponse:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return HttpResponse(
        status_code=status,
        headers=(
            HttpHeader(name="Content-Type", value="application/json; charset=utf-8"),
            HttpHeader(name="Content-Length", value=str(len(body))),
        ),
        body=body,
    )


def yandex_payload(
    *,
    found: int = 1,
    provider_id: str = "yandex-secret-id",
    item_count: int = 1,
) -> dict:
    return {
        "type": "FeatureCollection",
        "properties": {"ResponseMetaData": {"SearchResponse": {"found": found}}},
        "features": [
            {
                "id": f"{provider_id}-{index}",
                "properties": {
                    "CompanyMetaData": {
                        "id": f"{provider_id}-{index}",
                        "name": "Кофейня Тест",
                        "address": "Санкт-Петербург, Невский проспект, 1",
                        "url": "https://coffee.example.org/",
                        "Categories": [{"name": "Кофейня"}],
                        "Phones": [{"formatted": "+70000000002"}],
                        "email": "contact010@example.invalid",
                        "manager": "must-not-be-copied",
                    }
                },
            }
            for index in range(item_count)
        ],
    }


def two_gis_payload(
    *,
    total: int = 1,
    provider_id: str = "2gis-secret-id",
    itin: str | None = None,
) -> dict:
    payload = {
        "meta": {"code": 200},
        "result": {
            "total": total,
            "items": [
                {
                    "id": provider_id,
                    "name": "Автосервис Тест",
                    "full_name": "Санкт-Петербург, Лиговский проспект, 10",
                    "rubrics": [{"name": "Автосервис"}],
                    "contact_groups": [
                        {
                            "contacts": [
                                {"type": "website", "value": "https://auto.example.org/"},
                                {"type": "phone", "text": "+70000000003"},
                            ]
                        }
                    ],
                    "private_note": "must-not-be-copied",
                }
            ],
        },
    }
    if itin is not None:
        payload["result"]["items"][0]["itin"] = itin
    return payload


class DirectoryAdaptersTest(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def yandex_adapter(
        transport: FakeHttpTransport,
        *,
        key: str | None = "fixture-yandex-key",
        allowed: bool = True,
        licensed: bool = True,
    ) -> YandexMapsAdapter:
        return YandexMapsAdapter(
            transport=transport,
            entitlement=ProviderEntitlement(
                provider=ProviderName.YANDEX_MAPS,
                allowed=allowed,
                license_confirmed=licensed,
            ),
            config=YandexMapsConfig(
                api_key=None if key is None else SecretStr(key),
            ),
        )

    async def test_yandex_success_uses_exact_endpoint_and_drops_provider_id(self):
        provider_id = "provider-place-id-should-not-persist"
        transport = FakeHttpTransport(json_response(yandex_payload(provider_id=provider_id)))
        result = await self.yandex_adapter(transport).discover(
            _query(),
            budget=RequestBudget(max_requests=1),
            collected_at=NOW,
        )

        self.assertEqual(transport.requests[0].url, YANDEX_MAPS_SEARCH_ENDPOINT)
        self.assertFalse(transport.requests[0].follow_redirects)
        self.assertEqual(result.receipt.endpoint, YANDEX_MAPS_SEARCH_ENDPOINT)
        self.assertEqual(result.receipt.request_count, 1)
        self.assertEqual(result.observations[0].name, "Кофейня Тест")
        serialized = result.model_dump_json()
        self.assertNotIn(provider_id, serialized)
        self.assertNotIn("must-not-be-copied", serialized)

    async def test_two_gis_success_uses_exact_endpoint_and_allowlisted_fields(self):
        provider_id = "2gis-place-id-should-not-persist"
        transport = FakeHttpTransport(json_response(two_gis_payload(provider_id=provider_id)))
        adapter = TwoGisPlacesAdapter(
            transport=transport,
            entitlement=ProviderEntitlement(
                provider=ProviderName.TWO_GIS,
                allowed=True,
                license_confirmed=True,
            ),
            config=TwoGisConfig(api_key=SecretStr("fixture-two-gis-key")),
        )
        result = await adapter.discover(
            _query(),
            budget=RequestBudget(max_requests=1),
            collected_at=NOW,
        )

        self.assertEqual(transport.requests[0].url, TWO_GIS_PLACES_ENDPOINT)
        self.assertEqual(result.observations[0].category, "Автосервис")
        requested_fields = dict(
            (item.name, item.value) for item in transport.requests[0].query
        )["fields"]
        self.assertNotIn("items.contact_groups", requested_fields)
        self.assertIsNone(result.observations[0].website_url)
        self.assertEqual(result.observations[0].public_phones, ())
        serialized = result.model_dump_json()
        self.assertNotIn(provider_id, serialized)
        self.assertNotIn("private_note", serialized)

    async def test_two_gis_protected_contacts_require_explicit_field_permission(self):
        name_only_transport = FakeHttpTransport(json_response(two_gis_payload()))
        name_only_adapter = TwoGisPlacesAdapter(
            transport=name_only_transport,
            entitlement=ProviderEntitlement(
                provider=ProviderName.TWO_GIS,
                allowed=True,
                license_confirmed=True,
                permitted_fields=("name",),
            ),
            config=TwoGisConfig(api_key=SecretStr("fixture-two-gis-key")),
        )
        name_only = await name_only_adapter.discover(
            _query(),
            budget=RequestBudget(),
            collected_at=NOW,
        )

        name_only_fields = dict(
            (item.name, item.value) for item in name_only_transport.requests[0].query
        )["fields"]
        self.assertEqual(name_only_fields, "items.name")
        self.assertIsNone(name_only.observations[0].website_url)
        self.assertEqual(name_only.observations[0].public_phones, ())

        contacts_transport = FakeHttpTransport(json_response(two_gis_payload()))
        contacts_adapter = TwoGisPlacesAdapter(
            transport=contacts_transport,
            entitlement=ProviderEntitlement(
                provider=ProviderName.TWO_GIS,
                allowed=True,
                license_confirmed=True,
                permitted_fields=("name", "public_phones", "website_url"),
            ),
            config=TwoGisConfig(api_key=SecretStr("fixture-two-gis-key")),
        )
        contacts = await contacts_adapter.discover(
            _query(),
            budget=RequestBudget(),
            collected_at=NOW,
        )

        contact_fields = dict(
            (item.name, item.value) for item in contacts_transport.requests[0].query
        )["fields"]
        self.assertEqual(contact_fields, "items.name,items.contact_groups")
        self.assertEqual(contacts.observations[0].website_url, "https://auto.example.org/")
        self.assertEqual(contacts.observations[0].public_phones, ("+70000000003",))

    async def test_two_gis_itin_requires_explicit_inn_permission(self):
        documented_itin = "7707083893"
        explicit_transport = FakeHttpTransport(
            json_response(two_gis_payload(itin=documented_itin))
        )
        explicit_adapter = TwoGisPlacesAdapter(
            transport=explicit_transport,
            entitlement=ProviderEntitlement(
                provider=ProviderName.TWO_GIS,
                allowed=True,
                license_confirmed=True,
                permitted_fields=("name", "inn"),
            ),
            config=TwoGisConfig(api_key=SecretStr("fixture-two-gis-key")),
        )
        explicit = await explicit_adapter.discover(
            _query(),
            budget=RequestBudget(),
            collected_at=NOW,
        )

        explicit_fields = dict(
            (item.name, item.value) for item in explicit_transport.requests[0].query
        )["fields"]
        self.assertEqual(explicit_fields, "items.name,items.itin")
        self.assertNotIn("items.inn", explicit_fields)
        self.assertEqual(explicit.observations[0].inn, documented_itin)

        for label, permitted_fields in (
            ("safe_default", ()),
            ("name_only", ("name",)),
        ):
            with self.subTest(label=label):
                safe_transport = FakeHttpTransport(
                    json_response(two_gis_payload(itin=documented_itin))
                )
                safe_adapter = TwoGisPlacesAdapter(
                    transport=safe_transport,
                    entitlement=ProviderEntitlement(
                        provider=ProviderName.TWO_GIS,
                        allowed=True,
                        license_confirmed=True,
                        permitted_fields=permitted_fields,
                    ),
                    config=TwoGisConfig(api_key=SecretStr("fixture-two-gis-key")),
                )
                safe = await safe_adapter.discover(
                    _query(),
                    budget=RequestBudget(),
                    collected_at=NOW,
                )

                safe_fields = dict(
                    (item.name, item.value) for item in safe_transport.requests[0].query
                )["fields"]
                self.assertNotIn("items.itin", safe_fields)
                self.assertNotIn("items.inn", safe_fields)
                self.assertIsNone(safe.observations[0].inn)

    async def test_missing_key_and_license_gate_stop_before_transport(self):
        missing_key_transport = FakeHttpTransport()
        with self.assertRaises(AdapterConfigurationError):
            await self.yandex_adapter(missing_key_transport, key=None).discover(
                _query(),
                budget=RequestBudget(),
                collected_at=NOW,
            )
        self.assertEqual(missing_key_transport.requests, [])

        denied_transport = FakeHttpTransport()
        with self.assertRaises(Exception) as caught:
            await self.yandex_adapter(denied_transport, licensed=False).discover(
                _query(),
                budget=RequestBudget(),
                collected_at=NOW,
            )
        self.assertIn("licensed access is not confirmed", str(caught.exception))
        self.assertEqual(denied_transport.requests, [])

    async def test_entitlement_defaults_to_deny(self):
        transport = FakeHttpTransport()
        adapter = YandexMapsAdapter(
            transport=transport,
            entitlement=ProviderEntitlement(provider=ProviderName.YANDEX_MAPS),
            config=YandexMapsConfig(api_key=SecretStr("fixture-yandex-key")),
        )
        with self.assertRaises(Exception) as caught:
            await adapter.discover(_query(), budget=RequestBudget(), collected_at=NOW)
        self.assertIn("access is not entitled", str(caught.exception))
        self.assertEqual(transport.requests, [])

    async def test_malformed_json_content_type_and_size_fail_closed(self):
        malformed = HttpResponse(
            status_code=200,
            headers=(HttpHeader(name="content-type", value="application/json"),),
            body=b"{not-json",
        )
        with self.assertRaises(AdapterPayloadError):
            await self.yandex_adapter(FakeHttpTransport(malformed)).discover(
                _query(), budget=RequestBudget(), collected_at=NOW
            )

        wrong_type = HttpResponse(
            status_code=200,
            headers=(HttpHeader(name="content-type", value="text/html"),),
            body=b"{}",
        )
        with self.assertRaises(AdapterResponseError):
            await self.yandex_adapter(FakeHttpTransport(wrong_type)).discover(
                _query(), budget=RequestBudget(), collected_at=NOW
            )

        too_large = HttpResponse(
            status_code=200,
            headers=(
                HttpHeader(name="content-type", value="application/json"),
                HttpHeader(name="content-length", value="1000001"),
            ),
            body=b"{}",
        )
        with self.assertRaises(AdapterResponseError):
            await self.yandex_adapter(FakeHttpTransport(too_large)).discover(
                _query(), budget=RequestBudget(), collected_at=NOW
            )

    async def test_http_error_classes_are_safe(self):
        expected = {
            401: AdapterAuthorizationError,
            403: AdapterAuthorizationError,
            429: AdapterRateLimitError,
            500: AdapterTransientError,
            503: AdapterTransientError,
        }
        for status, exception_type in expected.items():
            with self.subTest(status=status):
                with self.assertRaises(exception_type):
                    await self.yandex_adapter(
                        FakeHttpTransport(json_response({}, status=status))
                    ).discover(_query(), budget=RequestBudget(), collected_at=NOW)

    async def test_timeout_and_key_are_redacted_from_full_traceback(self):
        secret = "super-secret-api-key"
        transport = FakeHttpTransport(TimeoutError(f"url?apikey={secret}"))
        with self.assertRaises(AdapterTimeoutError) as caught:
            await self.yandex_adapter(transport, key=secret).discover(
                _query(), budget=RequestBudget(), collected_at=NOW
            )

        rendered = "".join(
            traceback.format_exception(type(caught.exception), caught.exception, caught.exception.__traceback__)
        )
        self.assertNotIn(secret, rendered)
        self.assertNotIn(secret, repr(transport.requests[0]))
        self.assertNotIn(secret, json.dumps(transport.requests[0].model_dump(), default=str))
        self.assertEqual(dict(transport.requests[0].transport_query())["apikey"], secret)

    async def test_transport_adapter_error_is_replaced_without_secret_chain(self):
        secret = "credential-bearing-transport-error"
        transport = FakeHttpTransport(
            AdapterTransientError(f"transport leaked apikey={secret}")
        )
        with self.assertRaises(AdapterTransientError) as caught:
            await self.yandex_adapter(transport, key=secret).discover(
                _query(), budget=RequestBudget(), collected_at=NOW
            )

        rendered = "".join(
            traceback.format_exception(
                type(caught.exception),
                caught.exception,
                caught.exception.__traceback__,
            )
        )
        self.assertNotIn(secret, str(caught.exception))
        self.assertNotIn(secret, rendered)
        self.assertIsNone(caught.exception.__cause__)
        self.assertIsNone(caught.exception.__context__)

    async def test_yandex_keeps_fixed_page_size_and_trims_final_page_locally(self):
        transport = FakeHttpTransport(
            json_response(yandex_payload(found=23, item_count=20)),
            json_response(yandex_payload(found=23, item_count=20)),
        )
        result = await self.yandex_adapter(transport).discover(
            _query(limit=23, page_size=20),
            budget=RequestBudget(max_requests=2),
            collected_at=NOW,
        )

        pagination = []
        for request in transport.requests:
            parameters = dict((item.name, item.value) for item in request.query)
            pagination.append((parameters["skip"], parameters["results"]))
        self.assertEqual(pagination, [("0", "20"), ("20", "20")])
        self.assertEqual(len(result.observations), 23)

    async def test_pagination_cannot_exceed_request_budget(self):
        transport = FakeHttpTransport(json_response(yandex_payload(found=2)))
        with self.assertRaises(RequestBudgetExceeded):
            await self.yandex_adapter(transport).discover(
                _query(limit=2, page_size=1),
                budget=RequestBudget(max_requests=1),
                collected_at=NOW,
            )
        self.assertEqual(len(transport.requests), 1)


class FNSAdapterTest(unittest.IsolatedAsyncioTestCase):
    async def test_manual_official_record_transport_succeeds_without_scraping(self):
        record = OfficialFNSRecord(
            mode=FNSAccessMode.MANUAL,
            source_url="https://egrul.nalog.ru/record",
            observed_at=NOW,
            legal_name="ООО Тест",
            legal_status="active",
            inn="7707083893",
            ogrn="1027700132195",
            content_sha256="a" * 64,
        )
        transport = FakeFNSRecordTransport(record)
        adapter = FNSVerifierAdapter(
            transport=transport,
            entitlement=ProviderEntitlement(provider=ProviderName.FNS, allowed=True),
        )
        result = await adapter.verify(
            FNSRecordRequest(mode=FNSAccessMode.MANUAL, inn="7707083893"),
            budget=RequestBudget(),
            collected_at=NOW,
        )

        self.assertEqual(result.observation.inn, "7707083893")
        self.assertEqual(result.receipt.request_count, 1)
        self.assertEqual(len(transport.requests), 1)

    async def test_licensed_modes_require_explicit_license_before_transport(self):
        transport = FakeFNSRecordTransport(None)
        adapter = FNSVerifierAdapter(
            transport=transport,
            entitlement=ProviderEntitlement(provider=ProviderName.FNS, allowed=True),
        )
        with self.assertRaises(Exception) as caught:
            await adapter.verify(
                FNSRecordRequest(
                    mode=FNSAccessMode.LICENSED_EGRUL_FTP,
                    inn="7707083893",
                ),
                budget=RequestBudget(),
                collected_at=NOW,
            )
        self.assertIn("licensed access is not confirmed", str(caught.exception))
        self.assertEqual(transport.requests, [])

    def test_non_provider_fns_modes_require_official_nalog_source(self):
        with self.assertRaises(ValidationError):
            OfficialFNSRecord(
                mode=FNSAccessMode.OPEN_DATA_SNAPSHOT,
                source_url="https://unofficial.example.org/fns",
                observed_at=NOW,
                legal_name="ООО Тест",
                legal_status="active",
                inn="7707083893",
                content_sha256="b" * 64,
            )

        official = OfficialFNSRecord(
            mode=FNSAccessMode.OPEN_DATA_SNAPSHOT,
            source_url="https://www.nalog.gov.ru/opendata/",
            observed_at=NOW,
            legal_name="ООО Тест",
            legal_status="active",
            inn="7707083893",
            content_sha256="b" * 64,
        )
        self.assertEqual(official.provider_name, "fns")


class WebsiteAdapterTest(unittest.IsolatedAsyncioTestCase):
    async def test_public_explicit_domain_success_pins_resolved_ip(self):
        body = "<html><body>Публичный бизнес-сайт</body></html>".encode()
        transport = FakePinnedHttpTransport(
            HttpResponse(
                status_code=200,
                headers=(HttpHeader(name="content-type", value="text/html; charset=utf-8"),),
                body=body,
            )
        )
        resolver = FakeResolver("8.8.8.8")
        reader = _website_reader(transport, resolver)
        result = await reader.read(
            WebsiteReadRequest(url="https://example.org/", official_domain="example.org"),
            budget=RequestBudget(),
            collected_at=NOW,
        )

        self.assertEqual(transport.requests[0].resolved_ips, ("8.8.8.8",))
        self.assertEqual(transport.connect_ips, ["8.8.8.8"])
        self.assertFalse(transport.requests[0].follow_redirects)
        self.assertEqual(result.page.text, body.decode())

    async def test_private_dns_answer_is_rejected_before_http(self):
        transport = FakePinnedHttpTransport()
        reader = _website_reader(transport, FakeResolver("127.0.0.1"))
        with self.assertRaises(AdapterResponseError):
            await reader.read(
                WebsiteReadRequest(url="https://example.org/", official_domain="example.org"),
                budget=RequestBudget(),
                collected_at=NOW,
            )
        self.assertEqual(transport.requests, [])

    async def test_redirect_is_denied_and_cross_origin_never_fetched(self):
        redirect = HttpResponse(
            status_code=302,
            headers=(HttpHeader(name="location", value="https://evil.example.net/"),),
            body=b"",
        )
        transport = FakePinnedHttpTransport(redirect)
        reader = _website_reader(
            transport,
            FakeResolver("8.8.8.8"),
            config=WebsiteReaderConfig(
                redirect_policy=WebsiteRedirectPolicy.SAME_ORIGIN,
                max_redirects=1,
            ),
        )
        with self.assertRaises(AdapterRedirectError):
            await reader.read(
                WebsiteReadRequest(url="https://example.org/", official_domain="example.org"),
                budget=RequestBudget(max_requests=2),
                collected_at=NOW,
            )
        self.assertEqual(len(transport.requests), 1)

    async def test_same_host_cross_port_redirect_is_rejected(self):
        redirect = HttpResponse(
            status_code=302,
            headers=(
                HttpHeader(name="location", value="https://example.org:8443/final"),
            ),
            body=b"",
        )
        transport = FakePinnedHttpTransport(
            redirect,
            HttpResponse(
                status_code=200,
                headers=(HttpHeader(name="content-type", value="text/html"),),
                body=b"must not be reached",
            ),
        )
        reader = _website_reader(
            transport,
            FakeResolver("8.8.8.8"),
            config=WebsiteReaderConfig(
                redirect_policy=WebsiteRedirectPolicy.SAME_ORIGIN,
                max_redirects=1,
            ),
        )
        with self.assertRaises(AdapterRedirectError):
            await reader.read(
                WebsiteReadRequest(
                    url="https://example.org/",
                    official_domain="example.org",
                ),
                budget=RequestBudget(max_requests=2),
                collected_at=NOW,
            )
        self.assertEqual(len(transport.requests), 1)

    async def test_special_and_transition_ipv6_addresses_are_rejected_before_http(self):
        addresses = (
            "64:ff9b::7f00:1",
            "::ffff:8.8.8.8",
            "2002:0808:0808::1",
            "2001:0000:4136:e378:8000:63bf:3fff:fdd2",
            "2001:db8::1",
        )
        for address in addresses:
            with self.subTest(address=address):
                transport = FakePinnedHttpTransport()
                reader = _website_reader(transport, FakeResolver(address))
                with self.assertRaises(AdapterResponseError):
                    await reader.read(
                        WebsiteReadRequest(
                            url="https://example.org/",
                            official_domain="example.org",
                        ),
                        budget=RequestBudget(),
                        collected_at=NOW,
                    )
                self.assertEqual(transport.requests, [])
                self.assertEqual(transport.connect_ips, [])

    async def test_website_reader_fails_closed_without_pinning_transport(self):
        transport = FakeHttpTransport(
            HttpResponse(
                status_code=200,
                headers=(HttpHeader(name="content-type", value="text/html"),),
                body=b"must not be reached",
            )
        )
        reader = _website_reader(transport, FakeResolver("8.8.8.8"))
        with self.assertRaises(AdapterConfigurationError):
            await reader.read(
                WebsiteReadRequest(
                    url="https://example.org/",
                    official_domain="example.org",
                ),
                budget=RequestBudget(),
                collected_at=NOW,
            )
        self.assertEqual(transport.requests, [])

    async def test_content_type_and_body_size_are_bounded(self):
        wrong_type = FakePinnedHttpTransport(
            HttpResponse(
                status_code=200,
                headers=(HttpHeader(name="content-type", value="application/pdf"),),
                body=b"pdf",
            )
        )
        with self.assertRaises(AdapterResponseError):
            await _website_reader(wrong_type, FakeResolver("8.8.8.8")).read(
                WebsiteReadRequest(url="https://example.org/", official_domain="example.org"),
                budget=RequestBudget(),
                collected_at=NOW,
            )

        oversized = FakePinnedHttpTransport(
            HttpResponse(
                status_code=200,
                headers=(
                    HttpHeader(name="content-type", value="text/html"),
                    HttpHeader(name="content-length", value="11"),
                ),
                body=b"small",
            )
        )
        with self.assertRaises(AdapterResponseError):
            await _website_reader(
                oversized,
                FakeResolver("8.8.8.8"),
                config=WebsiteReaderConfig(max_response_bytes=10),
            ).read(
                WebsiteReadRequest(url="https://example.org/", official_domain="example.org"),
                budget=RequestBudget(),
                collected_at=NOW,
            )

    def test_local_url_and_domain_mismatch_are_rejected_at_model_boundary(self):
        for values in (
            {"url": "https://127.0.0.1/", "official_domain": "127.0.0.1"},
            {"url": "https://example.org/", "official_domain": "other.example.org"},
            {"url": "http://example.org/", "official_domain": "example.org"},
        ):
            with self.subTest(values=values):
                with self.assertRaises(ValidationError):
                    WebsiteReadRequest(**values)


class ImportIsolationTest(unittest.TestCase):
    def test_existing_runtime_import_does_not_load_new_adapters_or_dry_run(self):
        script = (
            "import sys; import ai_sales_radar.runtime; "
            "bad=sorted(k for k in sys.modules if "
            "k in {'ai_sales_radar.multisource.adapters','ai_sales_radar.multisource.dry_run'}); "
            "raise SystemExit('loaded: '+','.join(bad) if bad else 0)"
        )
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=PROJECT_ROOT,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr or completed.stdout)


def _query(*, limit: int = 1, page_size: int = 1):
    from ai_sales_radar.multisource.adapters import DirectorySearchQuery

    return DirectorySearchQuery(
        text="кофейня",
        city="Санкт-Петербург",
        limit=limit,
        page_size=page_size,
    )


def _website_reader(
    transport: FakeHttpTransport,
    resolver: FakeResolver,
    *,
    config: WebsiteReaderConfig | None = None,
) -> OfficialWebsiteReader:
    return OfficialWebsiteReader(
        transport=transport,
        resolver=resolver,
        entitlement=ProviderEntitlement(
            provider=ProviderName.OFFICIAL_WEBSITE,
            allowed=True,
        ),
        config=config,
    )


if __name__ == "__main__":
    unittest.main()
