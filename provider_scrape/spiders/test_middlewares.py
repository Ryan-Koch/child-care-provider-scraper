"""Tests for shared downloader middlewares (provider_scrape.middlewares)."""

from unittest.mock import patch

import pytest
from scrapy import Spider
from scrapy.http import Request, Response
from scrapy.settings import Settings

from provider_scrape.middlewares import RateLimitBackoffMiddleware


spider = Spider(name="maryland")


class _Slot:
    """Stand-in for scrapy's downloader Slot (only the attrs we touch)."""

    def __init__(self, delay=15.0, randomize_delay=True):
        self.delay = delay
        self.randomize_delay = randomize_delay


class _Downloader:
    def __init__(self, slots, slot_key="www.checkccmd.org"):
        self.slots = slots
        self._slot_key = slot_key

    def get_slot_key(self, request):
        return self._slot_key


class _Engine:
    def __init__(self, downloader):
        self.downloader = downloader


class _Crawler:
    def __init__(self, settings, downloader):
        self.settings = settings
        self.engine = _Engine(downloader)


class _FakeDelayed:
    def __init__(self):
        self._active = True

    def active(self):
        return self._active

    def cancel(self):
        self._active = False


def _make(settings_overrides=None, slot=None, slot_key="www.checkccmd.org"):
    settings = {
        "RATELIMIT_BACKOFF_ENABLED": True,
        "RATELIMIT_BACKOFF_DOMAINS": ["checkccmd.org"],
        "RATELIMIT_BACKOFF_HTTP_CODES": [403],
        "RATELIMIT_BACKOFF_COOLDOWN": 60,
        "RATELIMIT_BACKOFF_MAX_RETRIES": 8,
    }
    settings.update(settings_overrides or {})
    slot = slot if slot is not None else _Slot()
    downloader = _Downloader({slot_key: slot}, slot_key=slot_key)
    crawler = _Crawler(Settings(settings), downloader)
    return RateLimitBackoffMiddleware.from_crawler(crawler), slot


def _resp(status=403, url="https://www.checkccmd.org/FacilityDetail.aspx?fi=1"):
    request = Request(url=url)
    return request, Response(url=url, status=status, request=request)


def test_passthrough_when_disabled():
    mw, _ = _make({"RATELIMIT_BACKOFF_ENABLED": False})
    request, response = _resp(403)
    assert mw.process_response(request, response, spider) is response


def test_passthrough_on_success_status():
    mw, _ = _make()
    request, response = _resp(200)
    assert mw.process_response(request, response, spider) is response


def test_passthrough_on_unlisted_domain():
    mw, _ = _make(slot_key="example.com")
    request, response = _resp(403, url="https://example.com/x")
    assert mw.process_response(request, response, spider) is response


def test_403_returns_retry_and_pauses_slot():
    mw, slot = _make()
    request, response = _resp(403)

    with patch("twisted.internet.reactor.callLater", return_value=_FakeDelayed()) as cl:
        out = mw.process_response(request, response, spider)

    # A retry request is returned (not the 403 response).
    assert isinstance(out, Request)
    assert out is not response
    assert out.meta["ratelimit_retries"] == 1
    assert out.dont_filter is True

    # The host slot is paused for the cooldown, jitter disabled, original saved.
    assert slot.delay == 60
    assert slot.randomize_delay is False
    assert slot._ratelimit_saved == (15.0, True)
    cl.assert_called_once()
    assert cl.call_args.args[0] == 60  # cooldown seconds


def test_retry_counter_increments_across_trips():
    mw, _ = _make()
    request = Request(
        url="https://www.checkccmd.org/FacilityDetail.aspx?fi=1",
        meta={"ratelimit_retries": 3},
    )
    response = Response(url=request.url, status=403, request=request)

    with patch("twisted.internet.reactor.callLater", return_value=_FakeDelayed()):
        out = mw.process_response(request, response, spider)

    assert out.meta["ratelimit_retries"] == 4


def test_gives_up_after_max_retries(caplog):
    mw, _ = _make({"RATELIMIT_BACKOFF_MAX_RETRIES": 8})
    request = Request(
        url="https://www.checkccmd.org/FacilityDetail.aspx?fi=1",
        meta={"ratelimit_retries": 8},
    )
    response = Response(url=request.url, status=403, request=request)

    with caplog.at_level("ERROR"):
        out = mw.process_response(request, response, spider)

    # Exhausted: the 403 response is returned (not another retry) and logged.
    assert out is response
    assert any("gave up after 8" in r.message for r in caplog.records)


def test_overlapping_trip_reissues_timer_and_keeps_original():
    mw, slot = _make()
    request, response = _resp(403)

    first_timer = _FakeDelayed()
    with patch(
        "twisted.internet.reactor.callLater",
        side_effect=[first_timer, _FakeDelayed()],
    ):
        mw.process_response(request, response, spider)
        # Second trip while still paused: original stays the pristine value and
        # the prior restore timer is cancelled (re-armed).
        mw.process_response(request, response, spider)

    assert slot._ratelimit_saved == (15.0, True)
    assert first_timer.active() is False  # cancelled by the re-arm


def test_restore_slot_reverts_delay_and_jitter():
    slot = _Slot(delay=15.0, randomize_delay=True)
    slot.delay = 60
    slot.randomize_delay = False
    slot._ratelimit_saved = (15.0, True)
    slot._ratelimit_restore = _FakeDelayed()

    RateLimitBackoffMiddleware._restore_slot(slot)

    assert slot.delay == 15.0
    assert slot.randomize_delay is True
    assert not hasattr(slot, "_ratelimit_saved")
    assert slot._ratelimit_restore is None


def test_missing_slot_still_returns_retry():
    # No slot registered yet for the host: still retry (best effort), don't crash.
    mw, _ = _make()
    mw.crawler.engine.downloader.slots.clear()
    request, response = _resp(403)

    with patch("twisted.internet.reactor.callLater", return_value=_FakeDelayed()):
        out = mw.process_response(request, response, spider)

    assert isinstance(out, Request)
    assert out.meta["ratelimit_retries"] == 1
