from __future__ import annotations

import concurrent.futures
import html
import json
import math
import os
import smtplib
import ssl
import time
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# ============================================================
# Binance Spot — Multi-Timeframe Trend Following Scanner
# Architecture: 1D context -> 4H confirmation -> 1H trigger
# Calculation/reporting only. NO ORDERS / NO POSITION TRACKING.
# ============================================================

HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "465"))
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASS = os.getenv("EMAIL_PASS", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")
EMAIL_TOP_RESULTS = int(os.getenv("EMAIL_TOP_RESULTS", "50"))

SPOT_BASE_URL = os.getenv(
    "BINANCE_SPOT_BASE_URL",
    os.getenv("BINANCE_BASE_URL", "https://data-api.binance.vision"),
)
BINANCE_WEB_BASE_URL = os.getenv("BINANCE_WEB_BASE_URL", "https://www.binance.com")
BINANCE_PRODUCTS_PATH = "/bapi/asset/v2/public/asset-service/product/get-products"

QUOTE_ASSETS = {
    x.strip().upper()
    for x in os.getenv("QUOTE_ASSETS", "").split(",")
    if x.strip()
}
EXCLUDE_STABLECOINS = os.getenv("EXCLUDE_STABLECOINS", "true").lower() == "true"
EXCLUDE_LEVERAGED_TOKENS = os.getenv("EXCLUDE_LEVERAGED_TOKENS", "true").lower() == "true"
MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "1000000"))
MAX_SPREAD_PERCENT = float(os.getenv("MAX_SPREAD_PERCENT", "0.10"))
MIN_HISTORY_DAYS = int(os.getenv("MIN_HISTORY_DAYS", "30"))
ORDER_BOOK_DEPTH_ENABLED = os.getenv("ORDER_BOOK_DEPTH_ENABLED", "true").lower() == "true"
ORDER_BOOK_DEPTH_LIMIT = int(os.getenv("ORDER_BOOK_DEPTH_LIMIT", "100"))
ORDER_BOOK_DEPTH_PCT = float(os.getenv("ORDER_BOOK_DEPTH_PCT", "0.25"))
MIN_ORDER_BOOK_DEPTH_QUOTE = float(os.getenv("MIN_ORDER_BOOK_DEPTH_QUOTE", "25000"))
ORDER_BOOK_DEPTH_WORKERS = int(os.getenv("ORDER_BOOK_DEPTH_WORKERS", "8"))

# -------------------- MTF strategy --------------------
DAILY_INTERVAL = os.getenv("DAILY_INTERVAL", "1d")
DAILY_EMA_PERIOD = int(os.getenv("DAILY_EMA_PERIOD", "200"))
DAILY_KLINES_LIMIT = int(os.getenv("DAILY_KLINES_LIMIT", "260"))

CONFIRM_INTERVAL = os.getenv("CONFIRM_INTERVAL", "4h")
CONFIRM_SUPERTREND_ATR_PERIOD = int(os.getenv("CONFIRM_SUPERTREND_ATR_PERIOD", "10"))
CONFIRM_SUPERTREND_MULTIPLIER = float(os.getenv("CONFIRM_SUPERTREND_MULTIPLIER", "3.0"))
CONFIRM_KLINES_LIMIT = int(os.getenv("CONFIRM_KLINES_LIMIT", "250"))

TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "1h")
TRIGGER_SUPERTREND_ATR_PERIOD = int(os.getenv("TRIGGER_SUPERTREND_ATR_PERIOD", "10"))
TRIGGER_SUPERTREND_MULTIPLIER = float(os.getenv("TRIGGER_SUPERTREND_MULTIPLIER", "3.0"))
TRIGGER_KLINES_LIMIT = int(os.getenv("TRIGGER_KLINES_LIMIT", "500"))
TREND_SIGNAL_DIRECTIONS = os.getenv("TREND_SIGNAL_DIRECTIONS", "BOTH").upper()

# Optional confirmations on the 1H trigger timeframe.
ADX_PERIOD = int(os.getenv("ADX_PERIOD", "14"))
ADX_THRESHOLD = float(os.getenv("ADX_THRESHOLD", "25"))
MACD_FAST_PERIOD = int(os.getenv("MACD_FAST_PERIOD", "12"))
MACD_SLOW_PERIOD = int(os.getenv("MACD_SLOW_PERIOD", "26"))
MACD_SIGNAL_PERIOD = int(os.getenv("MACD_SIGNAL_PERIOD", "9"))
BB_PERIOD = int(os.getenv("BB_PERIOD", "20"))
BB_STDDEV_MULT = float(os.getenv("BB_STDDEV_MULT", "2.0"))
BB_VOLUME_SMA_PERIOD = int(os.getenv("BB_VOLUME_SMA_PERIOD", "20"))
BB_VOLUME_MULT = float(os.getenv("BB_VOLUME_MULT", "1.5"))
ICHIMOKU_TENKAN_PERIOD = int(os.getenv("ICHIMOKU_TENKAN_PERIOD", "9"))
ICHIMOKU_KIJUN_PERIOD = int(os.getenv("ICHIMOKU_KIJUN_PERIOD", "26"))
ICHIMOKU_SENKOU_B_PERIOD = int(os.getenv("ICHIMOKU_SENKOU_B_PERIOD", "52"))
ICHIMOKU_DISPLACEMENT = int(os.getenv("ICHIMOKU_DISPLACEMENT", "26"))
BONUS_WEIGHT_MACD = float(os.getenv("BONUS_WEIGHT_MACD", "40"))
BONUS_WEIGHT_BOLLINGER = float(os.getenv("BONUS_WEIGHT_BOLLINGER", "30"))
BONUS_WEIGHT_ICHIMOKU = float(os.getenv("BONUS_WEIGHT_ICHIMOKU", "30"))

# Risk is calculated from the 1H ATR of the MTF trigger timeframe.
RISK_ENABLED = os.getenv("RISK_ENABLED", os.getenv("EMA_RISK_ENABLED", "true")).lower() == "true"
RISK_METHOD = os.getenv("RISK_METHOD", os.getenv("EMA_RISK_METHOD", "atr")).strip().lower()
RISK_ATR_PERIOD = int(os.getenv("RISK_ATR_PERIOD", os.getenv("EMA_RISK_ATR_PERIOD", "14")))
RISK_ATR_MULTIPLIER = float(os.getenv("RISK_ATR_MULTIPLIER", os.getenv("EMA_RISK_ATR_MULTIPLIER", "2.0")))
RISK_RR_RATIO = float(os.getenv("RISK_RR_RATIO", os.getenv("EMA_RISK_RR_RATIO", "2.0")))
RISK_SL_PERCENT = float(os.getenv("RISK_SL_PERCENT", os.getenv("EMA_RISK_SL_PERCENT", "3.0")))
RISK_TP_PERCENT = float(os.getenv("RISK_TP_PERCENT", os.getenv("EMA_RISK_TP_PERCENT", "6.0")))
RISK_TRAIL_PERCENT = float(os.getenv("RISK_TRAIL_PERCENT", os.getenv("EMA_RISK_TRAIL_PERCENT", "1.5")))

LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")
LEVERAGED_PATTERNS = ("3L", "3S", "5L", "5S", "2L", "2S")


class BinanceHTTPError(RuntimeError):
    pass


class CriterionAudit:
    def __init__(self) -> None:
        self.rows: List[Dict[str, Any]] = []

    def add(self, name: str, before: int, selected: int) -> None:
        rejected = before - selected
        self.rows.append({
            "criterion": name,
            "before": before,
            "selected": selected,
            "rejected": rejected,
            "retention": selected / before * 100 if before else 0.0,
            "rejection": rejected / before * 100 if before else 0.0,
        })


def http_get_json(base_url: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = base_url.rstrip("/") + path
    if params:
        query = urlencode({k: v for k, v in params.items() if v is not None})
        if query:
            url += "?" + query
    last_error: Optional[Exception] = None
    for attempt in range(HTTP_RETRIES + 1):
        try:
            request = Request(
                url,
                headers={"User-Agent": "BinanceMTFTrendFollowingScanner/3.0"},
                method="GET",
            )
            with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            last_error = exc
            retryable = exc.code in {418, 429, 500, 502, 503, 504}
            if not retryable or attempt >= HTTP_RETRIES:
                try:
                    body = exc.read().decode("utf-8")
                except Exception:
                    body = ""
                raise BinanceHTTPError(f"HTTP {exc.code} {path}: {body[:500]}") from exc
            retry_after = exc.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after else min(15.0, 2.0 ** attempt)
            except ValueError:
                delay = min(15.0, 2.0 ** attempt)
            time.sleep(delay)
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt >= HTTP_RETRIES:
                raise BinanceHTTPError(f"Request failed {path}: {exc}") from exc
            time.sleep(min(10.0, 2.0 ** attempt))
    raise BinanceHTTPError(f"Request failed {path}: {last_error}")


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_percent_spread(bid: float, ask: float) -> Optional[float]:
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = (bid + ask) / 2.0
    return ((ask - bid) / mid) * 100.0 if mid > 0 else None


def is_leveraged_symbol(base_asset: str) -> bool:
    base = base_asset.upper()
    return any(base.endswith(x) for x in LEVERAGED_SUFFIXES + LEVERAGED_PATTERNS)


def apply_criterion(
    assets: List[Dict[str, Any]],
    audit: CriterionAudit,
    name: str,
    predicate: Callable[[Dict[str, Any]], bool],
) -> List[Dict[str, Any]]:
    before = len(assets)
    selected = [asset for asset in assets if predicate(asset)]
    audit.add(name, before, len(selected))
    return selected


# ============================================================
# Binance data / universe
# ============================================================

def spot_exchange_info() -> Dict[str, Any]:
    return http_get_json(SPOT_BASE_URL, "/api/v3/exchangeInfo")


def spot_tickers() -> List[Dict[str, Any]]:
    return http_get_json(SPOT_BASE_URL, "/api/v3/ticker/24hr")


def spot_book_tickers() -> List[Dict[str, Any]]:
    return http_get_json(SPOT_BASE_URL, "/api/v3/ticker/bookTicker")


def spot_order_book(symbol: str) -> Dict[str, Any]:
    return http_get_json(
        SPOT_BASE_URL, "/api/v3/depth",
        {"symbol": symbol, "limit": ORDER_BOOK_DEPTH_LIMIT},
    )


def spot_klines(symbol: str, interval: str, limit: int) -> List[List[Any]]:
    return http_get_json(
        SPOT_BASE_URL, "/api/v3/klines",
        {"symbol": symbol, "interval": interval, "limit": limit},
    )


def _extract_stablecoin_assets(payload: Any, active_assets: set[str]) -> set[str]:
    found: set[str] = set()
    candidate_keys = ("symbol", "asset", "assetCode", "baseAsset", "coin", "ticker", "code", "s", "b")

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            tags = obj.get("tags")
            tag_values: List[str] = []
            if isinstance(tags, list):
                for tag in tags:
                    if isinstance(tag, dict):
                        tag_values.extend(
                            str(v).strip().lower() for v in tag.values()
                            if isinstance(v, (str, int, float))
                        )
                    else:
                        tag_values.append(str(tag).strip().lower())
            elif isinstance(tags, str):
                tag_values = [tags.strip().lower()]
            tag_values.extend(
                str(obj.get(k, "")).strip().lower()
                for k in ("type", "category", "assetType") if obj.get(k) is not None
            )
            if any("stablecoin" in tag for tag in tag_values):
                for key in candidate_keys:
                    value = obj.get(key)
                    if isinstance(value, str):
                        candidate = value.strip().upper()
                        if candidate in active_assets:
                            found.add(candidate)
            for value in obj.values():
                walk(value)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(payload)
    return found


def discover_binance_stablecoins(exchange_info: Dict[str, Any]) -> set[str]:
    active_assets: set[str] = set()
    for symbol in exchange_info.get("symbols", []):
        if not isinstance(symbol, dict):
            continue
        for key in ("baseAsset", "quoteAsset"):
            value = str(symbol.get(key, "")).strip().upper()
            if value:
                active_assets.add(value)
    payload = http_get_json(
        BINANCE_WEB_BASE_URL, BINANCE_PRODUCTS_PATH, {"includeEtf": "true"}
    )
    stablecoins = _extract_stablecoin_assets(payload, active_assets)
    if not stablecoins:
        raise BinanceHTTPError(
            "Le catalogue produit Binance a été interrogé mais aucun actif portant "
            "le tag 'stablecoin' n'a pu être identifié."
        )
    return stablecoins


def resolve_spot_quote_assets(exchange_info: Dict[str, Any], stablecoins: set[str]) -> set[str]:
    active_quotes = {
        str(x.get("quoteAsset", "")).strip().upper()
        for x in exchange_info.get("symbols", []) if isinstance(x, dict)
    }
    if QUOTE_ASSETS:
        return QUOTE_ASSETS & active_quotes
    return stablecoins & active_quotes


def build_spot_universe(exchange_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = []
    for x in exchange_info.get("symbols", []):
        if not isinstance(x, dict):
            continue
        result.append({
            "symbol": x.get("symbol", ""),
            "baseAsset": x.get("baseAsset", ""),
            "quoteAsset": x.get("quoteAsset", ""),
            "status": x.get("status", ""),
            "permissions": x.get("permissions", []),
            "permissionSets": x.get("permissionSets", []),
        })
    return result


def merge_spot_tickers(assets: List[Dict[str, Any]], tickers: List[Dict[str, Any]]) -> None:
    by_symbol = {x.get("symbol"): x for x in tickers if x.get("symbol")}
    for asset in assets:
        ticker = by_symbol.get(asset["symbol"], {})
        asset.update(
            lastPrice=as_float(ticker.get("lastPrice")),
            priceChangePercent=as_float(ticker.get("priceChangePercent")),
            quoteVolume=as_float(ticker.get("quoteVolume")),
            trades=as_int(ticker.get("count")),
        )


def merge_spot_books(assets: List[Dict[str, Any]], books: List[Dict[str, Any]]) -> None:
    by_symbol = {x.get("symbol"): x for x in books if x.get("symbol")}
    for asset in assets:
        book = by_symbol.get(asset["symbol"], {})
        bid = as_float(book.get("bidPrice"))
        ask = as_float(book.get("askPrice"))
        asset.update(bidPrice=bid, askPrice=ask, spreadPercent=safe_percent_spread(bid, ask))


def order_book_depth_metrics(book: Dict[str, Any], mid_price: float) -> Dict[str, float]:
    if mid_price <= 0:
        return {"depthTotalQuote": 0.0}
    band = ORDER_BOOK_DEPTH_PCT / 100.0
    min_bid = mid_price * (1.0 - band)
    max_ask = mid_price * (1.0 + band)
    bid_depth = 0.0
    ask_depth = 0.0
    for level in book.get("bids", []) or []:
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            continue
        price, quantity = as_float(level[0]), as_float(level[1])
        if price >= min_bid and price > 0 and quantity > 0:
            bid_depth += price * quantity
    for level in book.get("asks", []) or []:
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            continue
        price, quantity = as_float(level[0]), as_float(level[1])
        if price <= max_ask and price > 0 and quantity > 0:
            ask_depth += price * quantity
    return {"depthTotalQuote": bid_depth + ask_depth}


def parallel_map(fn: Callable[[Any], Any], items: List[Any], max_workers: int) -> List[Any]:
    if not items:
        return []
    workers = max(1, min(max_workers, len(items)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(fn, items))


def merge_spot_order_book_depth(assets: List[Dict[str, Any]], warnings: List[str]) -> None:
    if not ORDER_BOOK_DEPTH_ENABLED or not assets:
        return

    def fetch(asset: Dict[str, Any]) -> Tuple[float, Optional[str]]:
        try:
            book = spot_order_book(asset["symbol"])
            mid = as_float(asset.get("lastPrice"))
            return order_book_depth_metrics(book, mid)["depthTotalQuote"], None
        except Exception as exc:
            return 0.0, f"Order book {asset.get('symbol')}: {exc}"

    for asset, (depth, error) in zip(
        assets, parallel_map(fetch, assets, ORDER_BOOK_DEPTH_WORKERS)
    ):
        asset["depthTotalQuote"] = depth
        if error:
            warnings.append(error)


def bars_per_day(interval: str) -> float:
    return {
        "1m": 1440, "3m": 480, "5m": 288, "15m": 96,
        "30m": 48, "1h": 24, "2h": 12, "4h": 6,
        "6h": 4, "8h": 3, "12h": 2, "1d": 1, "3d": 1 / 3,
        "1w": 1 / 7,
    }.get(interval, 24)


def closed_klines_only(klines: List[List[Any]], now_ms: Optional[int] = None) -> List[List[Any]]:
    if now_ms is None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return [
        k for k in klines
        if isinstance(k, list) and len(k) >= 7 and as_int(k[6]) <= now_ms
    ]


def _flatten_permissions(asset: Dict[str, Any]) -> set[str]:
    values: set[str] = set()
    for p in asset.get("permissions") or []:
        values.add(str(p).upper())
    for group in asset.get("permissionSets") or []:
        if isinstance(group, (list, tuple, set)):
            values.update(str(p).upper() for p in group)
        else:
            values.add(str(group).upper())
    return values


def screen_spot(
    assets: List[Dict[str, Any]],
    quote_assets: set[str],
    stablecoins: set[str],
    audit: CriterionAudit,
) -> List[Dict[str, Any]]:
    assets = apply_criterion(assets, audit, "1. Statut TRADING", lambda x: str(x.get("status", "")).upper() == "TRADING")
    assets = apply_criterion(assets, audit, "2. Permission SPOT", lambda x: "SPOT" in _flatten_permissions(x))
    assets = apply_criterion(assets, audit, "3. Quote asset autorisé", lambda x: str(x.get("quoteAsset", "")).upper() in quote_assets)
    if EXCLUDE_STABLECOINS:
        assets = apply_criterion(
            assets, audit, "4. Exclusion dynamique des stablecoins",
            lambda x: str(x.get("baseAsset", "")).upper() not in stablecoins,
        )
    else:
        audit.add("4. Exclusion dynamique des stablecoins", len(assets), len(assets))
    if EXCLUDE_LEVERAGED_TOKENS:
        assets = apply_criterion(
            assets, audit, "5. Exclusion leveraged tokens",
            lambda x: not is_leveraged_symbol(str(x.get("baseAsset", ""))),
        )
    else:
        audit.add("5. Exclusion leveraged tokens", len(assets), len(assets))
    assets = apply_criterion(assets, audit, "6. Données 24H / prix valides", lambda x: as_float(x.get("lastPrice")) > 0)
    assets = apply_criterion(assets, audit, "7. Volume quote 24H minimum", lambda x: as_float(x.get("quoteVolume")) >= MIN_24H_QUOTE_VOLUME)
    if ORDER_BOOK_DEPTH_ENABLED:
        assets = apply_criterion(
            assets, audit, "8. Profondeur carnet minimum",
            lambda x: as_float(x.get("depthTotalQuote")) >= MIN_ORDER_BOOK_DEPTH_QUOTE,
        )
    else:
        audit.add("8. Profondeur carnet minimum", len(assets), len(assets))
    assets = apply_criterion(
        assets, audit, "9. Spread maximum",
        lambda x: x.get("spreadPercent") is not None and as_float(x.get("spreadPercent")) <= MAX_SPREAD_PERCENT,
    )
    # A minimum 30-day history remains a liquidity/data-quality filter.
    min_bars = max(10, math.ceil(MIN_HISTORY_DAYS * bars_per_day("1h")))

    def check_history(asset: Dict[str, Any]) -> bool:
        try:
            bars = closed_klines_only(spot_klines(asset["symbol"], "1h", max(min_bars + 5, 100)))
            return len(bars) >= min_bars
        except Exception:
            return False

    results = parallel_map(check_history, assets, max(4, ORDER_BOOK_DEPTH_WORKERS))
    selected = [a for a, ok in zip(assets, results) if ok]
    audit.add("10. Historique minimum 30 jours", len(assets), len(selected))
    return selected


# ============================================================
# Indicator primitives
# ============================================================

def closes_from_klines(klines: List[List[Any]]) -> List[float]:
    return [as_float(k[4]) for k in klines]


def highs_from_klines(klines: List[List[Any]]) -> List[float]:
    return [as_float(k[2]) for k in klines]


def lows_from_klines(klines: List[List[Any]]) -> List[float]:
    return [as_float(k[3]) for k in klines]


def volumes_from_klines(klines: List[List[Any]]) -> List[float]:
    return [as_float(k[5]) for k in klines]


def ema_series(values: List[float], period: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if period <= 0 or len(values) < period:
        return out
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    alpha = 2.0 / (period + 1.0)
    prev = seed
    for i in range(period, len(values)):
        prev = (values[i] - prev) * alpha + prev
        out[i] = prev
    return out


def sma_series(values: List[float], period: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if period <= 0:
        return out
    running = 0.0
    for i, value in enumerate(values):
        running += value
        if i >= period:
            running -= values[i - period]
        if i >= period - 1:
            out[i] = running / period
    return out


def stddev_series(values: List[float], period: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if period <= 0:
        return out
    for i in range(period - 1, len(values)):
        window = values[i - period + 1:i + 1]
        mean = sum(window) / period
        out[i] = math.sqrt(sum((x - mean) ** 2 for x in window) / period)
    return out


def atr_series(highs: List[float], lows: List[float], closes: List[float], period: int) -> List[Optional[float]]:
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    if n == 0 or period <= 0:
        return out
    tr = [0.0] * n
    tr[0] = highs[0] - lows[0]
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
    if n < period:
        return out
    first = sum(tr[:period]) / period
    out[period - 1] = first
    prev = first
    for i in range(period, n):
        prev = ((prev * (period - 1)) + tr[i]) / period
        out[i] = prev
    return out


def adx_series(highs: List[float], lows: List[float], closes: List[float], period: int) -> List[Optional[float]]:
    n = len(closes)
    adx: List[Optional[float]] = [None] * n
    if n <= period * 2:
        return adx
    tr = [0.0] * n
    plus_dm = [0.0] * n
    minus_dm = [0.0] * n
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        plus_dm[i] = up if up > down and up > 0 else 0.0
        minus_dm[i] = down if down > up and down > 0 else 0.0
    atr = sum(tr[1:period + 1])
    plus = sum(plus_dm[1:period + 1])
    minus = sum(minus_dm[1:period + 1])
    dx: List[Optional[float]] = [None] * n
    for i in range(period, n):
        if i > period:
            atr = atr - atr / period + tr[i]
            plus = plus - plus / period + plus_dm[i]
            minus = minus - minus / period + minus_dm[i]
        if atr <= 0:
            continue
        pdi = 100.0 * plus / atr
        mdi = 100.0 * minus / atr
        denom = pdi + mdi
        dx[i] = 100.0 * abs(pdi - mdi) / denom if denom else 0.0
    first_values = [x for x in dx[period:] if x is not None]
    if len(first_values) < period:
        return adx
    seed_index = period + period - 1
    if seed_index >= n:
        return adx
    adx[seed_index] = sum(first_values[:period]) / period
    prev = adx[seed_index]
    for i in range(seed_index + 1, n):
        if dx[i] is not None:
            prev = ((prev * (period - 1)) + dx[i]) / period
            adx[i] = prev
    return adx


def supertrend_series(
    highs: List[float], lows: List[float], closes: List[float],
    atr_period: int, multiplier: float,
) -> Tuple[List[Optional[float]], List[Optional[str]]]:
    n = len(closes)
    line: List[Optional[float]] = [None] * n
    direction: List[Optional[str]] = [None] * n
    atr = atr_series(highs, lows, closes, atr_period)
    final_upper: List[Optional[float]] = [None] * n
    final_lower: List[Optional[float]] = [None] * n
    for i in range(n):
        if atr[i] is None:
            continue
        hl2 = (highs[i] + lows[i]) / 2.0
        basic_upper = hl2 + multiplier * atr[i]
        basic_lower = hl2 - multiplier * atr[i]
        if i == 0 or final_upper[i - 1] is None:
            final_upper[i] = basic_upper
            final_lower[i] = basic_lower
            direction[i] = "UP" if closes[i] >= hl2 else "DOWN"
        else:
            prev_upper = final_upper[i - 1]
            prev_lower = final_lower[i - 1]
            final_upper[i] = basic_upper if basic_upper < prev_upper or closes[i - 1] > prev_upper else prev_upper
            final_lower[i] = basic_lower if basic_lower > prev_lower or closes[i - 1] < prev_lower else prev_lower
            prev_dir = direction[i - 1]
            if prev_dir == "DOWN" and closes[i] > final_upper[i - 1]:
                direction[i] = "UP"
            elif prev_dir == "UP" and closes[i] < final_lower[i - 1]:
                direction[i] = "DOWN"
            else:
                direction[i] = prev_dir
        line[i] = final_lower[i] if direction[i] == "UP" else final_upper[i]
    return line, direction


def macd_series(closes: List[float], fast: int, slow: int, signal: int) -> Tuple[List[Optional[float]], List[Optional[float]]]:
    ef = ema_series(closes, fast)
    es = ema_series(closes, slow)
    macd: List[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        if ef[i] is not None and es[i] is not None:
            macd[i] = ef[i] - es[i]
    usable = [x if x is not None else 0.0 for x in macd]
    sig = ema_series(usable, signal)
    for i in range(len(sig)):
        if macd[i] is None:
            sig[i] = None
    return macd, sig


def ichimoku_values(highs: List[float], lows: List[float]) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
    n = len(highs)
    tenkan: List[Optional[float]] = [None] * n
    kijun: List[Optional[float]] = [None] * n
    cloud_a: List[Optional[float]] = [None] * n
    cloud_b: List[Optional[float]] = [None] * n
    for i in range(n):
        if i + 1 >= ICHIMOKU_TENKAN_PERIOD:
            tenkan[i] = (max(highs[i - ICHIMOKU_TENKAN_PERIOD + 1:i + 1]) + min(lows[i - ICHIMOKU_TENKAN_PERIOD + 1:i + 1])) / 2.0
        if i + 1 >= ICHIMOKU_KIJUN_PERIOD:
            kijun[i] = (max(highs[i - ICHIMOKU_KIJUN_PERIOD + 1:i + 1]) + min(lows[i - ICHIMOKU_KIJUN_PERIOD + 1:i + 1])) / 2.0
        if i + 1 >= ICHIMOKU_SENKOU_B_PERIOD:
            cloud_b[i] = (max(highs[i - ICHIMOKU_SENKOU_B_PERIOD + 1:i + 1]) + min(lows[i - ICHIMOKU_SENKOU_B_PERIOD + 1:i + 1])) / 2.0
        if tenkan[i] is not None and kijun[i] is not None:
            cloud_a[i] = (tenkan[i] + kijun[i]) / 2.0
    return tenkan, kijun, cloud_a, cloud_b


# ============================================================
# MTF signal
# ============================================================

def _close_time(klines: List[List[Any]], index: int) -> str:
    return datetime.fromtimestamp(as_int(klines[index][6]) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def compute_mtf_signal(asset: Dict[str, Any]) -> Dict[str, Any]:
    symbol = asset["symbol"]
    result: Dict[str, Any] = {"symbol": symbol, "signalDirection": None}
    try:
        # Fetch independently: each timeframe uses its own closed candles.
        daily = closed_klines_only(spot_klines(symbol, DAILY_INTERVAL, DAILY_KLINES_LIMIT))
        confirm = closed_klines_only(spot_klines(symbol, CONFIRM_INTERVAL, CONFIRM_KLINES_LIMIT))
        trigger = closed_klines_only(spot_klines(symbol, TRIGGER_INTERVAL, TRIGGER_KLINES_LIMIT))

        if len(daily) < DAILY_EMA_PERIOD + 2:
            result["error"] = f"Historique {DAILY_INTERVAL} insuffisant pour EMA{DAILY_EMA_PERIOD}."
            return result
        if len(confirm) < CONFIRM_SUPERTREND_ATR_PERIOD + 3:
            result["error"] = f"Historique {CONFIRM_INTERVAL} insuffisant pour Supertrend."
            return result
        trigger_min = max(
            TRIGGER_SUPERTREND_ATR_PERIOD + 3,
            ADX_PERIOD * 2 + 2,
            MACD_SLOW_PERIOD + MACD_SIGNAL_PERIOD + 2,
            ICHIMOKU_SENKOU_B_PERIOD + ICHIMOKU_DISPLACEMENT + 2,
            RISK_ATR_PERIOD + 2,
        )
        if len(trigger) < trigger_min:
            result["error"] = f"Historique {TRIGGER_INTERVAL} insuffisant pour les indicateurs 1H."
            return result

        # ---------------- Daily context ----------------
        d_close = closes_from_klines(daily)
        d_ema = ema_series(d_close, DAILY_EMA_PERIOD)
        di = len(d_close) - 1
        if d_ema[di] is None:
            result["error"] = "EMA Daily indisponible."
            return result
        daily_bull = d_close[di] > d_ema[di]
        daily_bear = d_close[di] < d_ema[di]

        # ---------------- 4H confirmation ----------------
        c_high = highs_from_klines(confirm)
        c_low = lows_from_klines(confirm)
        c_close = closes_from_klines(confirm)
        c_st, c_dir = supertrend_series(
            c_high, c_low, c_close,
            CONFIRM_SUPERTREND_ATR_PERIOD, CONFIRM_SUPERTREND_MULTIPLIER,
        )
        ci = len(c_close) - 1
        confirm_bull = c_dir[ci] == "UP"
        confirm_bear = c_dir[ci] == "DOWN"
        if c_dir[ci] is None:
            result["error"] = "Supertrend 4H indisponible."
            return result

        # ---------------- 1H trigger ----------------
        t_high = highs_from_klines(trigger)
        t_low = lows_from_klines(trigger)
        t_close = closes_from_klines(trigger)
        t_vol = volumes_from_klines(trigger)
        t_st, t_dir = supertrend_series(
            t_high, t_low, t_close,
            TRIGGER_SUPERTREND_ATR_PERIOD, TRIGGER_SUPERTREND_MULTIPLIER,
        )
        ti = len(t_close) - 1
        tp = ti - 1
        if t_dir[tp] is None or t_dir[ti] is None:
            result["error"] = "Supertrend 1H indisponible."
            return result

        trigger_long = t_dir[tp] == "DOWN" and t_dir[ti] == "UP"
        trigger_short = t_dir[tp] == "UP" and t_dir[ti] == "DOWN"

        signal_direction: Optional[str] = None
        if TREND_SIGNAL_DIRECTIONS in {"BOTH", "LONG"} and daily_bull and confirm_bull and trigger_long:
            signal_direction = "LONG"
        elif TREND_SIGNAL_DIRECTIONS in {"BOTH", "SHORT"} and daily_bear and confirm_bear and trigger_short:
            signal_direction = "SHORT"

        t_atr = atr_series(t_high, t_low, t_close, RISK_ATR_PERIOD)[ti]
        adx = adx_series(t_high, t_low, t_close, ADX_PERIOD)[ti]
        macd, macd_signal = macd_series(t_close, MACD_FAST_PERIOD, MACD_SLOW_PERIOD, MACD_SIGNAL_PERIOD)
        bb_mid = sma_series(t_close, BB_PERIOD)
        bb_std = stddev_series(t_close, BB_PERIOD)
        vol_sma = sma_series(t_vol, BB_VOLUME_SMA_PERIOD)
        _, _, cloud_a, cloud_b = ichimoku_values(t_high, t_low)

        result.update({
            "price": t_close[ti],
            "dailyClose": d_close[di],
            "dailyEma200": d_ema[di],
            "dailyDirection": "BULL" if daily_bull else "BEAR" if daily_bear else "NEUTRAL",
            "dailyCloseTimeUtc": _close_time(daily, di),
            "confirmSupertrend": c_st[ci],
            "confirmDirection": "BULL" if confirm_bull else "BEAR",
            "confirmCloseTimeUtc": _close_time(confirm, ci),
            "triggerSupertrend": t_st[ti],
            "triggerDirection": "BULL" if t_dir[ti] == "UP" else "BEAR",
            "previousTriggerDirection": "BULL" if t_dir[tp] == "UP" else "BEAR",
            "triggerFlipLong": trigger_long,
            "triggerFlipShort": trigger_short,
            "signalDirection": signal_direction,
            "adx": adx,
            "adxOk": adx is not None and adx >= ADX_THRESHOLD,
            "atr": t_atr,
            "closeTimeMs": as_int(trigger[ti][6]),
            "closeTimeUtc": _close_time(trigger, ti),
        })

        # Bonus indicators are confirmations only; they do not create a signal.
        if not signal_direction:
            result.update({
                "confidenceScore": None,
                "macdOk": None,
                "bollingerVolume": None,
                "ichimoku": None,
            })
            return result

        macd_ok = (
            macd[ti] is not None and macd_signal[ti] is not None and
            (macd[ti] > macd_signal[ti] if signal_direction == "LONG" else macd[ti] < macd_signal[ti])
        )
        volume_ok = vol_sma[ti] is not None and t_vol[ti] >= vol_sma[ti] * BB_VOLUME_MULT
        bb_ok = False
        if bb_mid[ti] is not None and bb_std[ti] is not None:
            upper = bb_mid[ti] + BB_STDDEV_MULT * bb_std[ti]
            lower = bb_mid[ti] - BB_STDDEV_MULT * bb_std[ti]
            bb_ok = (t_close[ti] > bb_mid[ti] and volume_ok) if signal_direction == "LONG" else (t_close[ti] < bb_mid[ti] and volume_ok)
        else:
            upper = lower = None

        cloud_index = ti - ICHIMOKU_DISPLACEMENT
        cloud_top = cloud_bottom = None
        if cloud_index >= 0 and cloud_a[cloud_index] is not None and cloud_b[cloud_index] is not None:
            cloud_top = max(cloud_a[cloud_index], cloud_b[cloud_index])
            cloud_bottom = min(cloud_a[cloud_index], cloud_b[cloud_index])
        ichi_ok = False
        if cloud_top is not None and cloud_bottom is not None:
            ichi_ok = t_close[ti] > cloud_top if signal_direction == "LONG" else t_close[ti] < cloud_bottom

        total_weight = BONUS_WEIGHT_MACD + BONUS_WEIGHT_BOLLINGER + BONUS_WEIGHT_ICHIMOKU
        score = 0.0
        if total_weight > 0:
            score = (
                (BONUS_WEIGHT_MACD if macd_ok else 0) +
                (BONUS_WEIGHT_BOLLINGER if bb_ok else 0) +
                (BONUS_WEIGHT_ICHIMOKU if ichi_ok else 0)
            ) / total_weight * 100.0

        result.update({
            "confidenceScore": max(0.0, min(100.0, score)),
            "macd": macd[ti],
            "macdSignal": macd_signal[ti],
            "macdOk": macd_ok,
            "bollingerVolume": bb_ok,
            "volumeOk": volume_ok,
            "ichimoku": ichi_ok,
            "bbUpper": upper,
            "bbLower": lower,
            "cloudTop": cloud_top,
            "cloudBottom": cloud_bottom,
        })
        return result
    except Exception as exc:
        result["error"] = str(exc)
        return result


def merge_mtf_signals(assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return parallel_map(compute_mtf_signal, assets, 8)


# ============================================================
# Risk calculation — 1H ATR of the MTF trigger
# ============================================================

def risk_levels(entry: float, direction: str, atr: Optional[float]) -> Dict[str, float]:
    if entry <= 0:
        return {}
    use_atr = RISK_METHOD == "atr" and atr is not None and atr > 0
    if use_atr:
        distance = atr * RISK_ATR_MULTIPLIER
        reward = distance * RISK_RR_RATIO
        if direction == "LONG":
            return {
                "entry": entry, "sl": entry - distance, "tp": entry + reward,
                "trailActivation": entry + distance, "trailDistance": distance, "atr": atr,
            }
        return {
            "entry": entry, "sl": entry + distance, "tp": entry - reward,
            "trailActivation": entry - distance, "trailDistance": distance, "atr": atr,
        }
    sl = RISK_SL_PERCENT / 100.0
    tp = RISK_TP_PERCENT / 100.0
    trail = RISK_TRAIL_PERCENT / 100.0
    if direction == "LONG":
        return {
            "entry": entry, "sl": entry * (1.0 - sl), "tp": entry * (1.0 + tp),
            "trailActivation": entry * (1.0 + trail), "trailDistance": entry * trail,
        }
    return {
        "entry": entry, "sl": entry * (1.0 + sl), "tp": entry * (1.0 - tp),
        "trailActivation": entry * (1.0 - trail), "trailDistance": entry * trail,
    }


def build_risk_parameters(signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not RISK_ENABLED:
        return []
    output = []
    for signal in signals:
        direction = signal.get("signalDirection")
        if direction not in {"LONG", "SHORT"}:
            continue
        row = dict(signal)
        row["riskDirection"] = direction
        row["risk"] = risk_levels(as_float(signal.get("price")), direction, signal.get("atr"))
        output.append(row)
    return output


# ============================================================
# HTML report
# ============================================================

def fmt(value: Any, digits: int = 6) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        if not math.isfinite(value):
            return "—"
        return f"{value:.{digits}f}"
    return str(value)


def render_table(headers: List[str], rows: List[List[str]]) -> str:
    head = "".join(f"<th>{h}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows)
    return f"<table><tr>{head}</tr>{body}</table>"


def audit_to_html(audit: CriterionAudit) -> str:
    rows = [
        [html.escape(str(r["criterion"])), str(r["before"]), str(r["selected"]), str(r["rejected"]), f"{r['retention']:.1f}%", f"{r['rejection']:.1f}%"]
        for r in audit.rows
    ]
    return render_table(["Critère", "Avant", "Sélectionnés", "Rejetés", "Rétention", "Rejet"], rows)


def mtf_signals_table_html(signals: List[Dict[str, Any]]) -> str:
    valid = [x for x in signals if x.get("signalDirection") in {"LONG", "SHORT"}]
    valid.sort(key=lambda x: x.get("confidenceScore") if x.get("confidenceScore") is not None else -1, reverse=True)
    if not valid:
        return "<p>Aucun signal MTF 1D → 4H → 1H sur la dernière bougie 1H clôturée.</p>"
    rows = []
    for x in valid[:EMAIL_TOP_RESULTS]:
        rows.append([
            html.escape(str(x.get("symbol"))),
            html.escape(str(x.get("signalDirection"))),
            fmt(x.get("price")),
            html.escape(str(x.get("dailyDirection", "—"))),
            html.escape(str(x.get("confirmDirection", "—"))),
            html.escape(str(x.get("triggerDirection", "—"))),
            fmt(x.get("adx"), 2),
            fmt(x.get("confidenceScore"), 1),
            "✓" if x.get("macdOk") else "—",
            "✓" if x.get("bollingerVolume") else "—",
            "✓" if x.get("ichimoku") else "—",
            html.escape(str(x.get("closeTimeUtc", "—"))),
        ])
    return render_table(
        ["Symbol", "Direction", "Prix", "1D", "4H", "1H Trigger", "ADX 1H", "Score bonus", "MACD", "BB+Vol", "Ichimoku", "Clôture 1H"],
        rows,
    )


def risk_html(risk_rows: List[Dict[str, Any]]) -> str:
    if not risk_rows:
        return "<p>Aucun paramètre SL/TP/trailing à calculer pour cette exécution.</p>"
    rows = []
    for x in risk_rows[:EMAIL_TOP_RESULTS]:
        r = x.get("risk") or {}
        rows.append([
            html.escape(str(x.get("symbol"))), html.escape(str(x.get("riskDirection"))),
            fmt(r.get("entry")), fmt(r.get("atr")), fmt(r.get("sl")), fmt(r.get("tp")),
            fmt(r.get("trailActivation")), fmt(r.get("trailDistance")),
        ])
    method_note = (
        f"<p><b>Méthode :</b> ATR adaptatif (1H, période {RISK_ATR_PERIOD}, x{fmt(RISK_ATR_MULTIPLIER,1)}, R:R x{fmt(RISK_RR_RATIO,1)}).</p>"
        if RISK_METHOD == "atr" else "<p><b>Méthode :</b> Pourcentage fixe.</p>"
    )
    note = method_note + (
        "<p><b>Calcul uniquement.</b> Les niveaux sont calculés au moment du signal MTF. "
        "Le scanner ne suit pas le cours, ne détecte pas les niveaux atteints, ne simule aucune position, "
        "ne calcule aucun P/L et ne passe aucun ordre.</p>"
    )
    return note + render_table(
        ["Symbol", "Direction", "Entrée", "ATR 1H", "SL", "TP", "Activation trailing", "Distance trailing"], rows
    )


def errors_html(errors: List[str]) -> str:
    return "".join(f"<li>{html.escape(str(e))}</li>" for e in errors) or "<li>Aucune.</li>"


def warnings_html(warnings: List[str]) -> str:
    return "".join(f"<li>{html.escape(str(w))}</li>" for w in warnings) or "<li>Aucun.</li>"


def build_report(
    generated_at: datetime,
    universe_count: int,
    screened_count: int,
    quote_assets: set[str],
    stablecoins: set[str],
    audit: CriterionAudit,
    signals: List[Dict[str, Any]],
    risk_rows: List[Dict[str, Any]],
    warnings: List[str],
    errors: List[str],
) -> str:
    signal_count = sum(1 for x in signals if x.get("signalDirection") in {"LONG", "SHORT"})
    return f"""<!doctype html>
<html><head><meta charset="utf-8">
<style>
body{{font-family:Arial,sans-serif;font-size:14px;color:#222}}
table{{border-collapse:collapse;width:100%;margin:12px 0}}
th,td{{border:1px solid #ccc;padding:6px;text-align:left}}
th{{font-weight:bold}}
.note{{padding:10px;border:1px solid #ccc}}
</style></head><body>
<h1>Binance Spot — Multi-Timeframe Trend Following Scanner</h1>
<p><b>Date :</b> {generated_at.strftime("%Y-%m-%d %H:%M:%S UTC")}</p>

<h2>1. Architecture MTF</h2>
<ul>
<li><b>1D — tendance de fond :</b> dernière bougie Daily clôturée, Close 1D vs EMA{DAILY_EMA_PERIOD}.</li>
<li><b>4H — confirmation :</b> dernière bougie 4H clôturée, Supertrend période {CONFIRM_SUPERTREND_ATR_PERIOD}, multiplicateur {CONFIRM_SUPERTREND_MULTIPLIER:g}.</li>
<li><b>1H — déclencheur :</b> dernière bougie 1H clôturée, retournement Supertrend période {TRIGGER_SUPERTREND_ATR_PERIOD}, multiplicateur {TRIGGER_SUPERTREND_MULTIPLIER:g}.</li>
<li><b>LONG :</b> 1D haussier + 4H haussier + nouveau flip Supertrend 1H haussier.</li>
<li><b>SHORT :</b> 1D baissier + 4H baissier + nouveau flip Supertrend 1H baissier.</li>
<li>MACD, Bollinger + Volume et Ichimoku sont des confirmations bonus du signal 1H ; ils ne créent pas de signal seuls.</li>
<li>ADX 1H est affiché comme information/confirmation et n'est pas un prérequis du signal MTF.</li>
<li>Toutes les décisions utilisent uniquement des bougies clôturées : aucune anticipation de bougie en formation.</li>
</ul>

<h2>2. Univers et filtrage</h2>
<p>Univers Spot analysé : <b>{universe_count}</b> actifs.</p>
<p>Après pipeline : <b>{screened_count}</b> actifs.</p>
<p><b>Quote assets :</b> {html.escape(", ".join(sorted(quote_assets)) or "aucun")}</p>
<p><b>Stablecoins dynamiques :</b> {html.escape(", ".join(sorted(stablecoins)) or "aucun")}</p>
{audit_to_html(audit)}

<h2>3. Signaux MTF 1D → 4H → 1H</h2>
<p><b>Signaux MTF détectés :</b> {signal_count}</p>
{mtf_signals_table_html(signals)}

<h2>4. Lecture du signal</h2>
<p>Le Daily ne déclenche pas l'entrée : il définit le sens de fond. Le 4H confirme que la structure intermédiaire va dans le même sens. Le 1H fournit le timing via un nouveau retournement du Supertrend.</p>
<p>Un retournement 1H isolé contre le contexte 1D/4H n'est donc pas présenté comme signal MTF.</p>

<h2>5. SL / TP / Trailing</h2>
{risk_html(risk_rows)}
<div class="note">
<b>Calcul uniquement :</b> SL, TP et trailing sont calculés à partir du signal MTF et de l'ATR 1H configuré. Le programme ne suit pas le cours après le calcul, ne vérifie pas si un niveau est atteint, ne simule aucune position, ne calcule aucun P/L et ne passe aucun ordre Binance.
</div>

<h2>6. Warnings</h2><ul>{warnings_html(warnings)}</ul>
<h2>7. Erreurs</h2><ul>{errors_html(errors)}</ul>
</body></html>"""


def send_email(subject: str, html_body: str) -> None:
    if not EMAIL_USER or not EMAIL_PASS or not EMAIL_TO:
        raise RuntimeError("EMAIL_USER, EMAIL_PASS ou EMAIL_TO est manquant.")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT, context=context) as server:
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, [EMAIL_TO], msg.as_string())


def main() -> None:
    generated_at = datetime.now(timezone.utc)
    warnings: List[str] = []
    errors: List[str] = []
    audit = CriterionAudit()
    universe_count = 0
    screened_count = 0
    quote_assets: set[str] = set()
    stablecoins: set[str] = set()
    signals: List[Dict[str, Any]] = []
    risk_rows: List[Dict[str, Any]] = []

    try:
        exchange_info = spot_exchange_info()
        universe = build_spot_universe(exchange_info)
        universe_count = len(universe)
        stablecoins = discover_binance_stablecoins(exchange_info)
        quote_assets = resolve_spot_quote_assets(exchange_info, stablecoins)
        if not quote_assets:
            raise BinanceHTTPError(
                "Aucun quote asset autorisé n'a été résolu. Vérifier QUOTE_ASSETS ou le catalogue stablecoin Binance."
            )

        merge_spot_tickers(universe, spot_tickers())
        merge_spot_books(universe, spot_book_tickers())
        merge_spot_order_book_depth(universe, warnings)
        screened = screen_spot(universe, quote_assets, stablecoins, audit)
        screened_count = len(screened)

        signals = merge_mtf_signals(screened)
        risk_rows = build_risk_parameters(signals)
    except Exception as exc:
        errors.append(str(exc))

    report = build_report(
        generated_at, universe_count, screened_count, quote_assets, stablecoins,
        audit, signals, risk_rows, warnings, errors,
    )
    subject = "Binance Spot — MTF Trend Following — " + generated_at.strftime("%Y-%m-%d %H:%M UTC")
    try:
        send_email(subject, report)
    except Exception as exc:
        print(f"EMAIL ERROR: {exc}")
        print(report)
        raise

    print(
        f"Scan terminé : univers={universe_count}, filtrés={screened_count}, "
        f"signaux MTF={sum(1 for x in signals if x.get('signalDirection') in {'LONG','SHORT'})}"
    )


if __name__ == "__main__":
    main()
