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
# Adaptive Moving Average = Kaufman Adaptive Moving Average (KAMA).
# The same period is used across the three timeframes so the MTF logic
# remains coherent while the adaptive smoothing reduces range noise.
DAILY_INTERVAL = os.getenv("DAILY_INTERVAL", "1d")
DAILY_AMA_PERIOD = int(os.getenv("DAILY_AMA_PERIOD", "50"))
DAILY_AMA_FAST = int(os.getenv("DAILY_AMA_FAST", "2"))
DAILY_AMA_SLOW = int(os.getenv("DAILY_AMA_SLOW", "30"))
DAILY_KLINES_LIMIT = int(os.getenv("DAILY_KLINES_LIMIT", "300"))
DAILY_SUPERTREND_ATR_PERIOD = int(os.getenv("DAILY_SUPERTREND_ATR_PERIOD", "10"))
DAILY_SUPERTREND_MULTIPLIER = float(os.getenv("DAILY_SUPERTREND_MULTIPLIER", "3.0"))

CONFIRM_INTERVAL = os.getenv("CONFIRM_INTERVAL", "4h")
CONFIRM_AMA_PERIOD = int(os.getenv("CONFIRM_AMA_PERIOD", "50"))
CONFIRM_AMA_FAST = int(os.getenv("CONFIRM_AMA_FAST", "2"))
CONFIRM_AMA_SLOW = int(os.getenv("CONFIRM_AMA_SLOW", "30"))
CONFIRM_SUPERTREND_ATR_PERIOD = int(os.getenv("CONFIRM_SUPERTREND_ATR_PERIOD", "10"))
CONFIRM_SUPERTREND_MULTIPLIER = float(os.getenv("CONFIRM_SUPERTREND_MULTIPLIER", "3.0"))
CONFIRM_KLINES_LIMIT = int(os.getenv("CONFIRM_KLINES_LIMIT", "300"))

TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "1h")
TRIGGER_AMA_PERIOD = int(os.getenv("TRIGGER_AMA_PERIOD", "50"))
TRIGGER_AMA_FAST = int(os.getenv("TRIGGER_AMA_FAST", "2"))
TRIGGER_AMA_SLOW = int(os.getenv("TRIGGER_AMA_SLOW", "30"))
TRIGGER_SUPERTREND_ATR_PERIOD = int(os.getenv("TRIGGER_SUPERTREND_ATR_PERIOD", "12"))
TRIGGER_SUPERTREND_MULTIPLIER = float(os.getenv("TRIGGER_SUPERTREND_MULTIPLIER", "3.0"))
TRIGGER_KLINES_LIMIT = int(os.getenv("TRIGGER_KLINES_LIMIT", "600"))
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

# Risk management: the SuperTrend itself is the dynamic stop/trailing reference.
# No rigid take-profit is calculated. This scanner never monitors the market after the scan.
RISK_ENABLED = os.getenv("RISK_ENABLED", "true").lower() == "true"
RISK_SUPERTREND_SOURCE = os.getenv("RISK_SUPERTREND_SOURCE", "4h").strip().lower()
if RISK_SUPERTREND_SOURCE not in {"1h", "4h"}:
    RISK_SUPERTREND_SOURCE = "4h"

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


def adaptive_ma_series(
    values: List[float], period: int, fast_period: int = 2, slow_period: int = 30,
) -> List[Optional[float]]:
    """Kaufman Adaptive Moving Average (KAMA).

    Efficiency Ratio controls the smoothing constant: the average becomes
    responsive during directional movement and slower during noisy/ranging periods.
    """
    n = len(values)
    out: List[Optional[float]] = [None] * n
    if period <= 0 or n < period or fast_period <= 0 or slow_period <= 0:
        return out
    fast_sc = 2.0 / (fast_period + 1.0)
    slow_sc = 2.0 / (slow_period + 1.0)
    seed_index = period - 1
    seed = sum(values[:period]) / period
    out[seed_index] = seed
    prev = seed
    for i in range(period, n):
        direction = abs(values[i] - values[i - period])
        volatility = sum(abs(values[j] - values[j - 1]) for j in range(i - period + 1, i + 1))
        er = direction / volatility if volatility > 0 else 0.0
        sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2
        prev = prev + sc * (values[i] - prev)
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
    result: Dict[str, Any] = {
        "symbol": symbol,
        "signalDirection": None,
        "dailyBull": False,
        "dailyBear": False,
        "confirmBull": False,
        "confirmBear": False,
        "triggerFlipLong": False,
        "triggerFlipShort": False,
        "triggerAmaReboundLong": False,
        "triggerAmaReboundShort": False,
    }
    try:
        daily = closed_klines_only(spot_klines(symbol, DAILY_INTERVAL, DAILY_KLINES_LIMIT))
        confirm = closed_klines_only(spot_klines(symbol, CONFIRM_INTERVAL, CONFIRM_KLINES_LIMIT))
        trigger = closed_klines_only(spot_klines(symbol, TRIGGER_INTERVAL, TRIGGER_KLINES_LIMIT))

        if len(daily) < max(DAILY_AMA_PERIOD + 2, DAILY_SUPERTREND_ATR_PERIOD + 3):
            result["error"] = f"Historique {DAILY_INTERVAL} insuffisant pour AMA{DAILY_AMA_PERIOD}/Supertrend."
            return result
        if len(confirm) < max(CONFIRM_AMA_PERIOD + 2, CONFIRM_SUPERTREND_ATR_PERIOD + 3):
            result["error"] = f"Historique {CONFIRM_INTERVAL} insuffisant pour AMA{CONFIRM_AMA_PERIOD}/Supertrend."
            return result
        trigger_min = max(
            TRIGGER_AMA_PERIOD + 2,
            TRIGGER_SUPERTREND_ATR_PERIOD + 3,
            ADX_PERIOD * 2 + 2,
            MACD_SLOW_PERIOD + MACD_SIGNAL_PERIOD + 2,
            ICHIMOKU_SENKOU_B_PERIOD + ICHIMOKU_DISPLACEMENT + 2,
        )
        if len(trigger) < trigger_min:
            result["error"] = f"Historique {TRIGGER_INTERVAL} insuffisant pour les indicateurs 1H."
            return result

        # ---------------- Daily context ----------------
        d_high = highs_from_klines(daily)
        d_low = lows_from_klines(daily)
        d_close = closes_from_klines(daily)
        d_ama = adaptive_ma_series(d_close, DAILY_AMA_PERIOD, DAILY_AMA_FAST, DAILY_AMA_SLOW)
        d_st, d_dir = supertrend_series(
            d_high, d_low, d_close,
            DAILY_SUPERTREND_ATR_PERIOD, DAILY_SUPERTREND_MULTIPLIER,
        )
        di = len(d_close) - 1
        dpi = di - 1
        if d_ama[di] is None or d_ama[dpi] is None or d_dir[di] is None:
            result["error"] = "AMA/Supertrend Daily indisponible."
            return result
        daily_ama_up = d_ama[di] > d_ama[dpi]
        daily_ama_down = d_ama[di] < d_ama[dpi]
        daily_bull = d_close[di] > d_ama[di] and daily_ama_up and d_dir[di] == "UP"
        daily_bear = d_close[di] < d_ama[di] and daily_ama_down and d_dir[di] == "DOWN"
        result["dailyBull"] = daily_bull
        result["dailyBear"] = daily_bear

        # ---------------- 4H confirmation ----------------
        c_high = highs_from_klines(confirm)
        c_low = lows_from_klines(confirm)
        c_close = closes_from_klines(confirm)
        c_ama = adaptive_ma_series(c_close, CONFIRM_AMA_PERIOD, CONFIRM_AMA_FAST, CONFIRM_AMA_SLOW)
        c_st, c_dir = supertrend_series(
            c_high, c_low, c_close,
            CONFIRM_SUPERTREND_ATR_PERIOD, CONFIRM_SUPERTREND_MULTIPLIER,
        )
        ci = len(c_close) - 1
        cpi = ci - 1
        if c_ama[ci] is None or c_ama[cpi] is None or c_dir[ci] is None:
            result["error"] = "AMA/Supertrend 4H indisponible."
            return result
        confirm_ama_up = c_ama[ci] > c_ama[cpi]
        confirm_ama_down = c_ama[ci] < c_ama[cpi]
        confirm_bull = c_close[ci] > c_ama[ci] and confirm_ama_up and c_dir[ci] == "UP"
        confirm_bear = c_close[ci] < c_ama[ci] and confirm_ama_down and c_dir[ci] == "DOWN"
        result["confirmBull"] = confirm_bull
        result["confirmBear"] = confirm_bear

        # ---------------- 1H trigger ----------------
        t_high = highs_from_klines(trigger)
        t_low = lows_from_klines(trigger)
        t_close = closes_from_klines(trigger)
        t_vol = volumes_from_klines(trigger)
        t_ama = adaptive_ma_series(t_close, TRIGGER_AMA_PERIOD, TRIGGER_AMA_FAST, TRIGGER_AMA_SLOW)
        t_st, t_dir = supertrend_series(
            t_high, t_low, t_close,
            TRIGGER_SUPERTREND_ATR_PERIOD, TRIGGER_SUPERTREND_MULTIPLIER,
        )
        ti = len(t_close) - 1
        tp = ti - 1
        if t_ama[ti] is None or t_ama[tp] is None or t_dir[tp] is None or t_dir[ti] is None:
            result["error"] = "AMA/Supertrend 1H indisponible."
            return result

        trigger_flip_long = t_dir[tp] == "DOWN" and t_dir[ti] == "UP"
        trigger_flip_short = t_dir[tp] == "UP" and t_dir[ti] == "DOWN"
        # Rebound/reclaim of the adaptive MA: previous close was on the other side,
        # current candle touched the AMA and closed back through it.
        trigger_ama_rebound_long = (
            t_close[tp] <= t_ama[tp] and t_close[ti] > t_ama[ti] and t_low[ti] <= t_ama[ti]
        )
        trigger_ama_rebound_short = (
            t_close[tp] >= t_ama[tp] and t_close[ti] < t_ama[ti] and t_high[ti] >= t_ama[ti]
        )
        trigger_long = trigger_flip_long or trigger_ama_rebound_long
        trigger_short = trigger_flip_short or trigger_ama_rebound_short

        signal_direction: Optional[str] = None
        if TREND_SIGNAL_DIRECTIONS in {"BOTH", "LONG"} and daily_bull and confirm_bull and trigger_long:
            signal_direction = "LONG"
        elif TREND_SIGNAL_DIRECTIONS in {"BOTH", "SHORT"} and daily_bear and confirm_bear and trigger_short:
            signal_direction = "SHORT"

        adx = adx_series(t_high, t_low, t_close, ADX_PERIOD)[ti]
        macd, macd_signal = macd_series(t_close, MACD_FAST_PERIOD, MACD_SLOW_PERIOD, MACD_SIGNAL_PERIOD)
        bb_mid = sma_series(t_close, BB_PERIOD)
        bb_std = stddev_series(t_close, BB_PERIOD)
        vol_sma = sma_series(t_vol, BB_VOLUME_SMA_PERIOD)
        _, _, cloud_a, cloud_b = ichimoku_values(t_high, t_low)

        result.update({
            "price": t_close[ti],
            "dailyClose": d_close[di],
            "dailyAma": d_ama[di],
            "dailyAmaPrevious": d_ama[dpi],
            "dailyAmaSlope": "UP" if daily_ama_up else "DOWN" if daily_ama_down else "FLAT",
            "dailySupertrend": d_st[di],
            "dailyDirection": "BULL" if daily_bull else "BEAR" if daily_bear else "NEUTRAL",
            "dailySupertrendDirection": d_dir[di],
            "dailyCloseTimeUtc": _close_time(daily, di),
            "confirmClose": c_close[ci],
            "confirmAma": c_ama[ci],
            "confirmAmaPrevious": c_ama[cpi],
            "confirmAmaSlope": "UP" if confirm_ama_up else "DOWN" if confirm_ama_down else "FLAT",
            "confirmSupertrend": c_st[ci],
            "confirmDirection": "BULL" if confirm_bull else "BEAR" if confirm_bear else "NEUTRAL",
            "confirmSupertrendDirection": c_dir[ci],
            "confirmCloseTimeUtc": _close_time(confirm, ci),
            "triggerAma": t_ama[ti],
            "triggerAmaPrevious": t_ama[tp],
            "triggerSupertrend": t_st[ti],
            "triggerDirection": "BULL" if t_dir[ti] == "UP" else "BEAR",
            "previousTriggerDirection": "BULL" if t_dir[tp] == "UP" else "BEAR",
            "triggerFlipLong": trigger_flip_long,
            "triggerFlipShort": trigger_flip_short,
            "triggerAmaReboundLong": trigger_ama_rebound_long,
            "triggerAmaReboundShort": trigger_ama_rebound_short,
            "triggerLong": trigger_long,
            "triggerShort": trigger_short,
            "signalDirection": signal_direction,
            "adx": adx,
            "adxOk": adx is not None and adx >= ADX_THRESHOLD,
            "closeTimeMs": as_int(trigger[ti][6]),
            "closeTimeUtc": _close_time(trigger, ti),
        })

        if not signal_direction:
            result.update({"confidenceScore": None, "macdOk": None, "bollingerVolume": None, "ichimoku": None})
            return result

        macd_ok = (
            macd[ti] is not None and macd_signal[ti] is not None and
            (macd[ti] > macd_signal[ti] if signal_direction == "LONG" else macd[ti] < macd_signal[ti])
        )
        volume_ok = vol_sma[ti] is not None and t_vol[ti] >= vol_sma[ti] * BB_VOLUME_MULT
        bb_ok = False
        upper = lower = None
        if bb_mid[ti] is not None and bb_std[ti] is not None:
            upper = bb_mid[ti] + BB_STDDEV_MULT * bb_std[ti]
            lower = bb_mid[ti] - BB_STDDEV_MULT * bb_std[ti]
            bb_ok = (t_close[ti] > bb_mid[ti] and volume_ok) if signal_direction == "LONG" else (t_close[ti] < bb_mid[ti] and volume_ok)

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
            "macd": macd[ti], "macdSignal": macd_signal[ti], "macdOk": macd_ok,
            "bollingerVolume": bb_ok, "volumeOk": volume_ok, "ichimoku": ichi_ok,
            "bbUpper": upper, "bbLower": lower, "cloudTop": cloud_top, "cloudBottom": cloud_bottom,
        })
        return result
    except Exception as exc:
        result["error"] = str(exc)
        return result


def merge_mtf_signals(assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return parallel_map(compute_mtf_signal, assets, 8)


def build_mtf_audit(signals: List[Dict[str, Any]]) -> Dict[str, int]:
    rows = [x for x in signals if not x.get("error")]
    return {
        "analyzed": len(rows),
        "errors": sum(1 for x in signals if x.get("error")),
        "dailyBull": sum(1 for x in rows if x.get("dailyBull")),
        "dailyBear": sum(1 for x in rows if x.get("dailyBear")),
        "dailyBullConfirmBull": sum(1 for x in rows if x.get("dailyBull") and x.get("confirmBull")),
        "dailyBearConfirmBear": sum(1 for x in rows if x.get("dailyBear") and x.get("confirmBear")),
        "triggerFlipLong": sum(1 for x in rows if x.get("triggerFlipLong")),
        "triggerFlipShort": sum(1 for x in rows if x.get("triggerFlipShort")),
        "triggerAmaReboundLong": sum(1 for x in rows if x.get("triggerAmaReboundLong")),
        "triggerAmaReboundShort": sum(1 for x in rows if x.get("triggerAmaReboundShort")),
        "longCandidates": sum(1 for x in rows if x.get("dailyBull") and x.get("confirmBull") and x.get("triggerLong")),
        "shortCandidates": sum(1 for x in rows if x.get("dailyBear") and x.get("confirmBear") and x.get("triggerShort")),
        "longSignals": sum(1 for x in rows if x.get("signalDirection") == "LONG"),
        "shortSignals": sum(1 for x in rows if x.get("signalDirection") == "SHORT"),
    }


# ============================================================
# Risk calculation — SuperTrend dynamic stop / trailing reference

def risk_levels(
    entry: float,
    direction: str,
    st_1h: Optional[float],
    st_4h: Optional[float],
) -> Dict[str, Any]:
    if entry <= 0:
        return {}
    preferred = st_4h if RISK_SUPERTREND_SOURCE == "4h" else st_1h
    preferred_name = "4H" if RISK_SUPERTREND_SOURCE == "4h" else "1H"
    fallback = st_1h if RISK_SUPERTREND_SOURCE == "4h" else st_4h
    fallback_name = "1H" if RISK_SUPERTREND_SOURCE == "4h" else "4H"
    stop = preferred
    source = preferred_name
    if stop is None or (direction == "LONG" and stop >= entry) or (direction == "SHORT" and stop <= entry):
        if fallback is not None and ((direction == "LONG" and fallback < entry) or (direction == "SHORT" and fallback > entry)):
            stop = fallback
            source = fallback_name
    if stop is None:
        return {"entry": entry, "sl": None, "tp": None, "trailingStop": None, "stopSource": "indisponible"}
    return {
        "entry": entry,
        "sl": stop,
        "tp": None,
        "trailingStop": stop,
        "stopSource": source,
        "takeProfitMode": "RUNNER — sortie sur retournement SuperTrend",
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
        row["risk"] = risk_levels(
            as_float(signal.get("price")),
            direction,
            signal.get("triggerSupertrend"),
            signal.get("confirmSupertrend"),
        )
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


def mtf_audit_html(audit: Dict[str, int]) -> str:
    rows = [
        ["Actifs analysés", str(audit.get("analyzed", 0)), ""],
        ["Erreurs MTF", str(audit.get("errors", 0)), ""],
        ["1D haussier (AMA + pente + ST)", str(audit.get("dailyBull", 0)), "LONG"],
        ["1D baissier (AMA + pente + ST)", str(audit.get("dailyBear", 0)), "SHORT"],
        ["1D haussier + 4H haussier", str(audit.get("dailyBullConfirmBull", 0)), "LONG"],
        ["1D baissier + 4H baissier", str(audit.get("dailyBearConfirmBear", 0)), "SHORT"],
        ["Flip SuperTrend 1H DOWN → UP", str(audit.get("triggerFlipLong", 0)), "LONG"],
        ["Flip SuperTrend 1H UP → DOWN", str(audit.get("triggerFlipShort", 0)), "SHORT"],
        ["Rebond / reprise AMA 1H haussier", str(audit.get("triggerAmaReboundLong", 0)), "LONG"],
        ["Rebond / reprise AMA 1H baissier", str(audit.get("triggerAmaReboundShort", 0)), "SHORT"],
        ["Candidats MTF LONG", str(audit.get("longCandidates", 0)), "LONG"],
        ["Candidats MTF SHORT", str(audit.get("shortCandidates", 0)), "SHORT"],
        ["Signaux MTF LONG", str(audit.get("longSignals", 0)), "LONG"],
        ["Signaux MTF SHORT", str(audit.get("shortSignals", 0)), "SHORT"],
    ]
    return render_table(["Étape MTF", "Nombre", "Sens"], rows)


def mtf_signals_table_html(signals: List[Dict[str, Any]]) -> str:
    valid = [x for x in signals if x.get("signalDirection") in {"LONG", "SHORT"}]
    valid.sort(key=lambda x: x.get("confidenceScore") if x.get("confidenceScore") is not None else -1, reverse=True)
    if not valid:
        return "<p>Aucun signal MTF 1D → 4H → 1H sur la dernière bougie 1H clôturée.</p>"
    rows = []
    for x in valid[:EMAIL_TOP_RESULTS]:
        trigger_reason = []
        if x.get("triggerFlipLong") or x.get("triggerFlipShort"):
            trigger_reason.append("Flip ST")
        if x.get("triggerAmaReboundLong") or x.get("triggerAmaReboundShort"):
            trigger_reason.append("Reprise AMA")
        rows.append([
            html.escape(str(x.get("symbol"))), html.escape(str(x.get("signalDirection"))),
            fmt(x.get("price")), html.escape(str(x.get("dailyDirection", "—"))),
            html.escape(str(x.get("confirmDirection", "—"))),
            ", ".join(trigger_reason) or "—", fmt(x.get("adx"), 2),
            fmt(x.get("confidenceScore"), 1),
            "✓" if x.get("macdOk") else "—", "✓" if x.get("bollingerVolume") else "—",
            "✓" if x.get("ichimoku") else "—", html.escape(str(x.get("closeTimeUtc", "—"))),
        ])
    return render_table(
        ["Symbol", "Direction", "Prix", "1D", "4H", "Déclencheur 1H", "ADX 1H", "Score bonus", "MACD", "BB+Vol", "Ichimoku", "Clôture 1H"],
        rows,
    )


def risk_html(risk_rows: List[Dict[str, Any]]) -> str:
    if not risk_rows:
        return "<p>Aucun paramètre SL/trailing à calculer pour cette exécution.</p>"
    rows = []
    for x in risk_rows[:EMAIL_TOP_RESULTS]:
        r = x.get("risk") or {}
        rows.append([
            html.escape(str(x.get("symbol"))), html.escape(str(x.get("riskDirection"))),
            fmt(r.get("entry")), fmt(r.get("sl")), fmt(r.get("tp")),
            fmt(r.get("trailingStop")), html.escape(str(r.get("stopSource", "—"))),
        ])
    note = (
        f"<p><b>Méthode :</b> SuperTrend dynamique, source préférentielle {RISK_SUPERTREND_SOURCE.upper()}. "
        "Le TP rigide est désactivé : la logique de sortie repose sur le retournement du SuperTrend.</p>"
        "<p><b>Calcul uniquement.</b> Le scanner calcule le niveau au moment du signal. Il ne suit pas le cours, "
        "ne détecte pas si le niveau est atteint, ne simule aucune position, ne calcule aucun P/L et ne passe aucun ordre.</p>"
    )
    return note + render_table(
        ["Symbol", "Direction", "Entrée", "SL SuperTrend", "TP", "Trailing SuperTrend", "Source"], rows
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
    mtf_audit = build_mtf_audit(signals)
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
<li><b>1D — tendance majeure :</b> dernière bougie Daily clôturée, prix au-dessus/au-dessous de l'AMA{DAILY_AMA_PERIOD}, AMA orientée dans le même sens, et SuperTrend {DAILY_SUPERTREND_ATR_PERIOD}/{DAILY_SUPERTREND_MULTIPLIER:g} aligné.</li>
<li><b>4H — confirmation :</b> dernière bougie 4H clôturée, prix du bon côté de l'AMA{CONFIRM_AMA_PERIOD}, AMA orientée dans le même sens, et SuperTrend {CONFIRM_SUPERTREND_ATR_PERIOD}/{CONFIRM_SUPERTREND_MULTIPLIER:g} aligné.</li>
<li><b>1H — timing :</b> dernière bougie 1H clôturée, SuperTrend {TRIGGER_SUPERTREND_ATR_PERIOD}/{TRIGGER_SUPERTREND_MULTIPLIER:g} ou reprise/rebond validé sur l'AMA{TRIGGER_AMA_PERIOD}.</li>
<li><b>LONG :</b> 1D haussier + 4H haussier + déclencheur 1H haussier.</li>
<li><b>SHORT :</b> 1D baissier + 4H baissier + déclencheur 1H baissier.</li>
<li><b>AMA :</b> Kaufman Adaptive Moving Average (KAMA), période {DAILY_AMA_PERIOD}/{CONFIRM_AMA_PERIOD}/{TRIGGER_AMA_PERIOD}, fast {TRIGGER_AMA_FAST}, slow {TRIGGER_AMA_SLOW}.</li>
<li>MACD, Bollinger + Volume et Ichimoku restent des confirmations bonus ; ils ne créent pas de signal seuls.</li>
<li>ADX 1H reste informatif et n'est pas un prérequis.</li>
<li>Toutes les décisions utilisent uniquement des bougies clôturées.</li>
</ul>

<h2>2. Univers et filtrage</h2>
<p>Univers Spot analysé : <b>{universe_count}</b> actifs.</p>
<p>Après pipeline : <b>{screened_count}</b> actifs.</p>
<p><b>Quote assets :</b> {html.escape(", ".join(sorted(quote_assets)) or "aucun")}</p>
<p><b>Stablecoins dynamiques :</b> {html.escape(", ".join(sorted(stablecoins)) or "aucun")}</p>
{audit_to_html(audit)}

<h2>3. Audit MTF 1D → 4H → 1H</h2>
{mtf_audit_html(mtf_audit)}

<h2>4. Signaux MTF</h2>
<p><b>Signaux MTF détectés :</b> {signal_count}</p>
{mtf_signals_table_html(signals)}

<h2>5. Lecture du signal</h2>
<p>Le 1D définit le biais majeur. Si l'AMA est plate ou si le SuperTrend contredit l'AMA, aucun biais directionnel n'est validé. Le 4H doit confirmer le même sens avec AMA + SuperTrend. Le 1H fournit ensuite le timing par retournement du SuperTrend ou reprise/rebond validé sur l'AMA.</p>

<h2>6. SL / TP / Trailing</h2>
{risk_html(risk_rows)}
<div class="note">
<b>Important :</b> aucun TP rigide n'est calculé. Le SuperTrend sert de référence dynamique de sortie/trailing. Ce programme effectue uniquement un calcul au moment du signal : il ne suit pas le cours après le calcul, ne vérifie pas si le niveau est atteint, ne simule aucune position, ne calcule aucun P/L et ne passe aucun ordre Binance.
</div>

<h2>7. Warnings</h2><ul>{warnings_html(warnings)}</ul>
<h2>8. Erreurs</h2><ul>{errors_html(errors)}</ul>
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
