"""Signed read-only GET requests to OKX, Bitget, and Binance.

Python port of the request signing in ``sign.js``. Each exchange is one
``Exchange`` entry: how to sign a request and how to tell success from an
exchange-level error. ``python -m clients.exchanges`` probes every endpoint
``sign.js`` calls and prints only status fields, never balances or trades.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

REQUEST_TIMEOUT_S = 10


@dataclass(frozen=True)
class Credentials:
    api_key: str
    secret_key: str
    passphrase: str = ""


@dataclass(frozen=True)
class SignedRequest:
    url: str
    headers: dict[str, str]


def _hmac_sha256(secret: str, message: str) -> bytes:
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).digest()


def sign_okx(creds: Credentials, path_with_query: str, timestamp: str) -> SignedRequest:
    signature = base64.b64encode(_hmac_sha256(creds.secret_key, timestamp + "GET" + path_with_query)).decode()
    return SignedRequest(
        url="https://www.okx.com" + path_with_query,
        headers={
            "OK-ACCESS-KEY": creds.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": creds.passphrase,
            "Content-Type": "application/json",
        },
    )


def sign_bitget(creds: Credentials, path_with_query: str, timestamp: str) -> SignedRequest:
    signature = base64.b64encode(_hmac_sha256(creds.secret_key, timestamp + "GET" + path_with_query)).decode()
    return SignedRequest(
        url="https://api.bitget.com" + path_with_query,
        headers={
            "ACCESS-KEY": creds.api_key,
            "ACCESS-SIGN": signature,
            "ACCESS-PASSPHRASE": creds.passphrase,
            "ACCESS-TIMESTAMP": timestamp,
            "locale": "en-US",
            "Content-Type": "application/json",
        },
    )


def sign_binance(creds: Credentials, path_with_query: str, timestamp: str) -> SignedRequest:
    path, _, query = path_with_query.partition("?")
    query = f"timestamp={timestamp}&{query}" if query else f"timestamp={timestamp}"
    signature = _hmac_sha256(creds.secret_key, query).hex()
    return SignedRequest(
        url=f"https://api.binance.com{path}?{query}&signature={signature}",
        headers={"X-MBX-APIKEY": creds.api_key},
    )


def _okx_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _epoch_ms() -> str:
    return str(int(time.time() * 1000))


def _okx_error(body: Any) -> str | None:
    return None if body.get("code") == "0" else f"code={body.get('code')} msg={body.get('msg')}"


def _bitget_error(body: Any) -> str | None:
    return None if body.get("code") == "00000" else f"code={body.get('code')} msg={body.get('msg')}"


def _binance_error(body: Any) -> str | None:
    return f"code={body.get('code')} msg={body.get('msg')}" if "code" in body and "rows" not in body else None


@dataclass(frozen=True)
class Exchange:
    name: str
    env_prefix: str
    sign: Callable[[Credentials, str, str], SignedRequest]
    timestamp: Callable[[], str]
    error_of: Callable[[Any], str | None]

    def credentials(self) -> Credentials:
        return Credentials(
            api_key=os.getenv(f"{self.env_prefix}_API_KEY", ""),
            secret_key=os.getenv(f"{self.env_prefix}_SECRET_KEY", ""),
            passphrase=os.getenv(f"{self.env_prefix}_PASSPHRASE", ""),
        )


OKX = Exchange("OKX", "OKX", sign_okx, _okx_timestamp, _okx_error)
BITGET = Exchange("Bitget", "BITGET", sign_bitget, _epoch_ms, _bitget_error)
BINANCE = Exchange("Binance", "BINANCE", sign_binance, _epoch_ms, _binance_error)


class ExchangeError(RuntimeError):
    pass


def signed_get(exchange: Exchange, path_with_query: str) -> Any:
    """GET a signed endpoint and return the parsed JSON body, or raise ``ExchangeError``."""
    request = exchange.sign(exchange.credentials(), path_with_query, exchange.timestamp())
    response = requests.get(request.url, headers=request.headers, timeout=REQUEST_TIMEOUT_S)
    try:
        body = response.json()
    except ValueError as exc:
        raise ExchangeError(f"{exchange.name} HTTP {response.status_code}, body is not JSON") from exc
    error = exchange.error_of(body) if isinstance(body, dict) else None
    if not response.ok or error:
        raise ExchangeError(f"{exchange.name} HTTP {response.status_code} {error or ''}".strip())
    return body


SIGN_JS_ENDPOINTS: list[tuple[Exchange, str, Callable[[Any], Any]]] = [
    (OKX, "/api/v5/finance/savings/balance?ccy=USDT", lambda b: b.get("data")),
    (BITGET, "/api/v2/earn/account/assets?coin=USDT", lambda b: b.get("data")),
    (BITGET, "/api/v2/spot/trade/history-orders?" + urlencode({"symbol": "BTCUSDT", "limit": "100"}), lambda b: b.get("data")),
    (BITGET, "/api/v2/spot/trade/history-orders?" + urlencode({"symbol": "ETHUSDT", "limit": "100"}), lambda b: b.get("data")),
    (BINANCE, "/sapi/v1/simple-earn/flexible/position?" + urlencode({"asset": "", "current": "1", "size": "10"}), lambda b: b.get("rows")),
]


def probe() -> bool:
    """Call every endpoint ``sign.js`` uses. Print status and record counts only."""
    load_dotenv()
    all_ok = True
    for exchange, path, records_of in SIGN_JS_ENDPOINTS:
        endpoint = path.split("?")[0]
        try:
            records = records_of(signed_get(exchange, path))
            fields = sorted(records[0].keys()) if records else []
            print(f"OK    {exchange.name:8} {endpoint}  records={len(records or [])}  fields={fields}")
        except (ExchangeError, requests.RequestException) as exc:
            all_ok = False
            print(f"FAIL  {exchange.name:8} {endpoint}  {type(exc).__name__}: {exc}")
    return all_ok


if __name__ == "__main__":
    raise SystemExit(0 if probe() else 1)
