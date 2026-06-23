import sys
from pathlib import Path
from unittest.mock import Mock, call, patch

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import _http


class Response:
    def __init__(self, payload=None, text="", status_code=200, headers=None):
        self.payload = payload if payload is not None else {}
        self.text = text
        self.status_code = status_code
        self.headers = headers if headers is not None else {}

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("http error")


class TestHttpGet:
    def test_success_first_try_returns_response_and_waits(self):
        limiter = Mock()
        response = Response({"ok": True})

        with patch("pipeline._http.requests.get", return_value=response) as get, patch(
            "pipeline._http.time.sleep"
        ) as sleep:
            result = _http.http_get("https://example.test", params={"q": "x"}, limiter=limiter)

        assert result is response
        limiter.wait.assert_called_once_with()
        get.assert_called_once_with(
            "https://example.test", params={"q": "x"}, headers=None, timeout=30
        )
        sleep.assert_not_called()

    def test_429_retry_after_then_success(self):
        limiter = Mock()
        throttled = Response(status_code=429, headers={"Retry-After": "3"})
        response = Response(status_code=200)

        with patch("pipeline._http.requests.get", side_effect=[throttled, response]), patch(
            "pipeline._http.time.sleep"
        ) as sleep:
            result = _http.http_get("https://example.test", limiter=limiter)

        assert result is response
        assert limiter.wait.call_count == 2
        sleep.assert_called_once_with(3)

    def test_5xx_backoff_then_success(self):
        limiter = Mock()
        responses = [Response(status_code=500), Response(status_code=503), Response(status_code=200)]

        with patch("pipeline._http.requests.get", side_effect=responses), patch(
            "pipeline._http.time.sleep"
        ) as sleep:
            result = _http.http_get("https://example.test", limiter=limiter, retries=3)

        assert result is responses[-1]
        assert limiter.wait.call_count == 3
        assert sleep.call_args_list == [call(_http._backoff_delay(0)), call(_http._backoff_delay(1))]

    def test_request_exception_retries_then_reraises_after_exhausting(self):
        limiter = Mock()
        first = requests.RequestException("first")
        last = requests.RequestException("last")

        with patch("pipeline._http.requests.get", side_effect=[first, last]), patch(
            "pipeline._http.time.sleep"
        ) as sleep:
            with pytest.raises(requests.RequestException) as exc:
                _http.http_get("https://example.test", limiter=limiter, retries=2)

        assert exc.value is last
        assert limiter.wait.call_count == 2
        sleep.assert_called_once_with(_http._backoff_delay(0))

    def test_persistent_5xx_after_retries_reraises_raise_for_status_error(self):
        limiter = Mock()
        responses = [Response(status_code=500), Response(status_code=502)]

        with patch("pipeline._http.requests.get", side_effect=responses), patch(
            "pipeline._http.time.sleep"
        ) as sleep:
            with pytest.raises(requests.HTTPError, match="http error"):
                _http.http_get("https://example.test", limiter=limiter, retries=2)

        assert limiter.wait.call_count == 2
        sleep.assert_called_once_with(_http._backoff_delay(0))


class TestHttpPost:
    def test_success_first_try_returns_response_and_waits(self):
        limiter = Mock()
        response = Response({"ok": True})

        with patch("pipeline._http.requests.post", return_value=response) as post, patch(
            "pipeline._http.time.sleep"
        ) as sleep:
            result = _http.http_post(
                "https://example.test", json_body={"x": 1}, params={"q": "x"}, limiter=limiter
            )

        assert result is response
        limiter.wait.assert_called_once_with()
        post.assert_called_once_with(
            "https://example.test", params={"q": "x"}, json={"x": 1}, headers=None, timeout=30
        )
        sleep.assert_not_called()

    def test_429_retry_after_then_success(self):
        limiter = Mock()
        throttled = Response(status_code=429, headers={"Retry-After": "7"})
        response = Response(status_code=200)

        with patch("pipeline._http.requests.post", side_effect=[throttled, response]), patch(
            "pipeline._http.time.sleep"
        ) as sleep:
            result = _http.http_post("https://example.test", limiter=limiter)

        assert result is response
        assert limiter.wait.call_count == 2
        sleep.assert_called_once_with(7)

    def test_5xx_backoff_then_success(self):
        limiter = Mock()
        responses = [Response(status_code=500), Response(status_code=503), Response(status_code=200)]

        with patch("pipeline._http.requests.post", side_effect=responses), patch(
            "pipeline._http.time.sleep"
        ) as sleep:
            result = _http.http_post("https://example.test", limiter=limiter, retries=3)

        assert result is responses[-1]
        assert limiter.wait.call_count == 3
        assert sleep.call_args_list == [call(_http._backoff_delay(0)), call(_http._backoff_delay(1))]

    def test_request_exception_retries_then_reraises_after_exhausting(self):
        limiter = Mock()
        first = requests.RequestException("first")
        last = requests.RequestException("last")

        with patch("pipeline._http.requests.post", side_effect=[first, last]), patch(
            "pipeline._http.time.sleep"
        ) as sleep:
            with pytest.raises(requests.RequestException) as exc:
                _http.http_post("https://example.test", limiter=limiter, retries=2)

        assert exc.value is last
        assert limiter.wait.call_count == 2
        sleep.assert_called_once_with(_http._backoff_delay(0))

    def test_persistent_5xx_after_retries_reraises_raise_for_status_error(self):
        limiter = Mock()
        responses = [Response(status_code=500), Response(status_code=502)]

        with patch("pipeline._http.requests.post", side_effect=responses), patch(
            "pipeline._http.time.sleep"
        ) as sleep:
            with pytest.raises(requests.HTTPError, match="http error"):
                _http.http_post("https://example.test", limiter=limiter, retries=2)

        assert limiter.wait.call_count == 2
        sleep.assert_called_once_with(_http._backoff_delay(0))


class TestHttpHelpers:
    def test_backoff_delay_grows_and_caps(self):
        assert _http._backoff_delay(0) == 0.5
        assert _http._backoff_delay(1) == 1.0
        assert _http._backoff_delay(2) == 2.0
        assert _http._backoff_delay(4) == 8.0
        assert _http._backoff_delay(99) == 8.0

    def test_retry_after_seconds_parsing(self):
        assert _http._retry_after_seconds(Response(headers={"Retry-After": "12"})) == 12
        assert _http._retry_after_seconds(Response()) is None
        assert _http._retry_after_seconds(Response(headers={"Retry-After": "soon"})) is None


class TestRateLimiter:
    def test_two_sequential_waits_enforce_min_interval(self):
        limiter = _http._RateLimiter(0.5)

        with patch("pipeline._http.time.monotonic", side_effect=[10.0, 10.2]), patch(
            "pipeline._http.time.sleep"
        ) as sleep:
            limiter.wait()
            limiter.wait()

        sleep.assert_called_once()
        assert sleep.call_args.args[0] == pytest.approx(0.3)
        assert limiter._next == pytest.approx(11.0)
