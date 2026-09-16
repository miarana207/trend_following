from __future__ import annotations

import concurrent.futures
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
# BINANCE SPOT — TREND FOLLOWING SCANNER
# Signal cœur (bloquant) : EMA200 (direction) + Supertrend (déclencheur
# de flip) + ADX (force de tendance). Long ET Short.
# Bonus (non bloquant, affiché dans l'email pour la confiance du signal) :
# MACD, Bollinger Bands + Volume, Ichimoku (cassure du Kumo).
# Univers et pipeline de filtrage liquidité/spread repris à l'identique
# du screener V3 (screener_v3_chop_adx_atr_table.py), sans restriction de
# quote asset et sans le module de régime CHOP/ADX/ATR (remplacé ici par
# la logique de signal trend following).
# NO ORDERS ARE EVER EXECUTED.
# ============================================================

HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "465"))
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASS = os.getenv("EMAIL_PASS", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")
EMAIL_TOP_RESULTS = int(os.getenv("EMAIL_TOP_RESULTS", "50"))

SPOT_BASE_URL = os.getenv("BINANCE_SPOT_BASE_URL", os.getenv("BINANCE_BASE_URL", "https://data-api.binance.vision"))

# Aucune restriction de quote asset : si QUOTE_ASSETS est vide, le scanner
# détecte dynamiquement les stablecoins actuellement actifs comme quote
# asset Spot chez Binance (identique au screener V3).
QUOTE_ASSETS = {x.strip().upper() for x in os.getenv("QUOTE_ASSETS", "").split(",") if x.strip()}
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

# ----------------------- PARAMÈTRES TREND FOLLOWING -----------------------
# Validés avec l'utilisateur : marché Spot uniquement, timeframe 1h.
# Valeurs par défaut proposées (soumises à validation) : voir le message
# de présentation associé à ce script pour les sources.
TREND_INTERVAL = os.getenv("TREND_INTERVAL", "1h")
TREND_SIGNAL_DIRECTIONS = os.getenv("TREND_SIGNAL_DIRECTIONS", "BOTH")  # LONG / SHORT / BOTH — validé : BOTH

EMA_TREND_PERIOD = int(os.getenv("EMA_TREND_PERIOD", "200"))

SUPERTREND_ATR_PERIOD = int(os.getenv("SUPERTREND_ATR_PERIOD", "10"))
SUPERTREND_MULTIPLIER = float(os.getenv("SUPERTREND_MULTIPLIER", "3.0"))

ADX_PERIOD = int(os.getenv("ADX_PERIOD", "14"))
ADX_THRESHOLD = float(os.getenv("ADX_THRESHOLD", "25"))

# Bonus — non bloquants, affichés pour la confiance du signal
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

# Nombre de bougies récupérées par actif. Doit couvrir large : EMA200 a
# besoin d'un historique important pour être stable, et l'Ichimoku a besoin
# de SENKOU_B_PERIOD + DISPLACEMENT bougies de recul.
TREND_KLINES_LIMIT = int(os.getenv("TREND_KLINES_LIMIT", "500"))
TREND_WORKERS = int(os.getenv("TREND_WORKERS", "8"))

# ---------------- SURVEILLANCE EMA 50/200 — INDÉPENDANTE ----------------
# Cette catégorie ne filtre aucun actif. Elle surveille uniquement les actifs
# ayant franchi les 10 critères du pipeline et alerte sur les nouveaux
# Golden Cross / Death Cross sur bougies 4H clôturées.
EMA_CROSS_INTERVAL = os.getenv("EMA_CROSS_INTERVAL", "4h")
EMA_CROSS_FAST_PERIOD = int(os.getenv("EMA_CROSS_FAST_PERIOD", "50"))
EMA_CROSS_SLOW_PERIOD = int(os.getenv("EMA_CROSS_SLOW_PERIOD", "200"))
EMA_CROSS_KLINES_LIMIT = int(os.getenv("EMA_CROSS_KLINES_LIMIT", "500"))
EMA_CROSS_STATE_FILE = os.getenv("EMA_CROSS_STATE_FILE", "ema_cross_state.json")

# ---------------- PARAMÈTRES DE RISQUE EMA50/EMA200 — INDÉPENDANTS ----------------
# Le module calcule uniquement les niveaux théoriques associés à un nouveau
# Golden Cross / Death Cross. Il ne suit jamais le cours et ne simule aucune sortie.
# IMPORTANT : le scanner reste en mode analyse. Aucun ordre Binance n'est envoyé.
EMA_RISK_ENABLED = os.getenv("EMA_RISK_ENABLED", "true").lower() == "true"
EMA_RISK_SL_PERCENT = float(os.getenv("EMA_RISK_SL_PERCENT", "3.0"))
EMA_RISK_TP_PERCENT = float(os.getenv("EMA_RISK_TP_PERCENT", "6.0"))
EMA_RISK_TRAIL_PERCENT = float(os.getenv("EMA_RISK_TRAIL_PERCENT", "1.5"))

# Pondération du score de confiance bonus (0 à 100), affiché mais jamais
# utilisé pour bloquer un signal.
BONUS_WEIGHT_MACD = float(os.getenv("BONUS_WEIGHT_MACD", "40"))
BONUS_WEIGHT_BOLLINGER = float(os.getenv("BONUS_WEIGHT_BOLLINGER", "30"))
BONUS_WEIGHT_ICHIMOKU = float(os.getenv("BONUS_WEIGHT_ICHIMOKU", "30"))

# ---------------- DIAGNOSTIC EMA200 (EXPÉRIMENTAL) ----------------
# Non bloquant : mesure la distance des flips Supertrend + ADX qui sont
# rejetés uniquement par la confirmation EMA200.
EMA_NEAR_05_PCT = float(os.getenv("EMA_NEAR_05_PCT", "0.5"))
EMA_NEAR_1_PCT = float(os.getenv("EMA_NEAR_1_PCT", "1.0"))
EMA_NEAR_2_PCT = float(os.getenv("EMA_NEAR_2_PCT", "2.0"))
EMA_NEAR_5_PCT = float(os.getenv("EMA_NEAR_5_PCT", "5.0"))
EMA_DIAGNOSTIC_TOP_N = int(os.getenv("EMA_DIAGNOSTIC_TOP_N", "20"))

STABLECOIN_BASES = {
    "USDT", "USDC", "FDUSD", "BUSD", "DAI", "TUSD", "USDP", "USDE",
    "USDD", "FRAX", "PYUSD", "EURC", "USD1", "RLUSD", "XUSD", "EURI",
    "USDG", "USDS", "U", "AEUR", "PAXG", "USD0",
}
STABLECOIN_QUOTE_MARKERS = (
    "USDT", "USDC", "FDUSD", "TUSD", "USDP", "USDE", "USDD",
    "PYUSD", "USD1", "RLUSD", "USDG", "USDS", "USD0", "XUSD",
    "EURC", "EURI", "AEUR",
)
LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")
LEVERAGED_PATTERNS = ("3L", "3S", "5L", "5S", "2L", "2S")


class BinanceHTTPError(RuntimeError):
    pass


def is_geo_restriction_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "http 451" in text or ("451" in text and "unavailable" in text)


def http_get_json(base_url: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = base_url.rstrip("/") + path
    if params:
        query = urlencode({k: v for k, v in params.items() if v is not None})
        if query:
            url += "?" + query
    last_error: Optional[Exception] = None
    for attempt in range(HTTP_RETRIES + 1):
        try:
            request = Request(url, headers={"User-Agent": "BinanceTrendFollowingScanner/1.0"}, method="GET")
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


class CriterionAudit:
    def __init__(self) -> None:
        self.rows: List[Dict[str, Any]] = []

    def add(self, name: str, before: int, selected: int) -> None:
        rejected = before - selected
        retention = selected / before * 100 if before else 0.0
        rejection = rejected / before * 100 if before else 0.0
        self.rows.append({"criterion": name, "before": before, "selected": selected, "rejected": rejected, "retention": retention, "rejection": rejection})


def apply_criterion(assets: List[Dict[str, Any]], audit: CriterionAudit, name: str, predicate: Callable[[Dict[str, Any]], bool]) -> List[Dict[str, Any]]:
    before = len(assets)
    selected = [asset for asset in assets if predicate(asset)]
    audit.add(name, before, len(selected))
    return selected


# ----------------------------- UNIVERS SPOT -----------------------------
def spot_exchange_info() -> Dict[str, Any]:
    return http_get_json(SPOT_BASE_URL, "/api/v3/exchangeInfo")


def spot_tickers() -> List[Dict[str, Any]]:
    return http_get_json(SPOT_BASE_URL, "/api/v3/ticker/24hr")


def spot_book_tickers() -> List[Dict[str, Any]]:
    return http_get_json(SPOT_BASE_URL, "/api/v3/ticker/bookTicker")


def discover_active_stablecoin_quotes(exchange_info: Dict[str, Any]) -> set[str]:
    active_quotes = {
        str(x.get("quoteAsset", "")).upper().strip()
        for x in exchange_info.get("symbols", [])
        if isinstance(x, dict) and str(x.get("status", "")).upper() == "TRADING"
    }
    discovered = {q for q in active_quotes if q in STABLECOIN_BASES}
    for quote in active_quotes - discovered:
        if any(marker in quote for marker in STABLECOIN_QUOTE_MARKERS):
            discovered.add(quote)
    return discovered


def resolve_spot_quote_assets(exchange_info: Dict[str, Any]) -> set[str]:
    active_quotes = {
        str(x.get("quoteAsset", "")).upper().strip()
        for x in exchange_info.get("symbols", [])
        if isinstance(x, dict)
    }
    if QUOTE_ASSETS:
        return QUOTE_ASSETS & active_quotes
    return discover_active_stablecoin_quotes(exchange_info)


def build_spot_universe(exchange_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for x in exchange_info.get("symbols", []):
        if not isinstance(x, dict):
            continue
        result.append({
            "symbol": x.get("symbol", ""),
            "baseAsset": x.get("baseAsset", ""),
            "quoteAsset": x.get("quoteAsset", ""),
            "status": x.get("status", ""),
            "permissions": x.get("permissions", []),
        })
    return result


def merge_spot_tickers(assets: List[Dict[str, Any]], tickers: List[Dict[str, Any]]) -> None:
    by_symbol = {x.get("symbol"): x for x in tickers if x.get("symbol")}
    for a in assets:
        t = by_symbol.get(a["symbol"], {})
        a.update(lastPrice=as_float(t.get("lastPrice")), priceChangePercent=as_float(t.get("priceChangePercent")), quoteVolume=as_float(t.get("quoteVolume")), trades=as_int(t.get("count")))


def merge_spot_books(assets: List[Dict[str, Any]], books: List[Dict[str, Any]]) -> None:
    by_symbol = {x.get("symbol"): x for x in books if x.get("symbol")}
    for a in assets:
        b = by_symbol.get(a["symbol"], {})
        bid, ask = as_float(b.get("bidPrice")), as_float(b.get("askPrice"))
        a.update(bidPrice=bid, askPrice=ask, spreadPercent=safe_percent_spread(bid, ask))


def spot_order_book(symbol: str) -> Dict[str, Any]:
    return http_get_json(SPOT_BASE_URL, "/api/v3/depth", {"symbol": symbol, "limit": ORDER_BOOK_DEPTH_LIMIT})


def order_book_depth_metrics(book: Dict[str, Any], mid_price: float) -> Dict[str, float]:
    if mid_price <= 0:
        return {"depthTotalQuote": 0.0}
    band = ORDER_BOOK_DEPTH_PCT / 100.0
    min_bid, max_ask = mid_price * (1.0 - band), mid_price * (1.0 + band)
    bid_depth = ask_depth = 0.0
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


def merge_spot_order_book_depth(assets: List[Dict[str, Any]], warnings: List[str]) -> None:
    if not ORDER_BOOK_DEPTH_ENABLED or not assets:
        return
    workers = max(1, min(ORDER_BOOK_DEPTH_WORKERS, len(assets)))
    failures = 0

    def fetch(asset: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        return asset["symbol"], spot_order_book(asset["symbol"])

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(fetch, asset): asset for asset in assets}
        for future in concurrent.futures.as_completed(future_map):
            asset = future_map[future]
            try:
                _, book = future.result()
                bid, ask = as_float(asset.get("bidPrice")), as_float(asset.get("askPrice"))
                mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
                asset.update(order_book_depth_metrics(book, mid))
            except Exception as exc:
                failures += 1
                asset.update(depthTotalQuote=0.0, depthError=str(exc))
    if failures:
        warnings.append(f"Order Book Depth : {failures} échecs de récupération sur {len(assets)} paires.")


def bars_per_day(interval: str) -> float:
    unit = interval[-1] if interval else "h"
    try:
        value = int(interval[:-1])
    except (ValueError, IndexError):
        value = 1
    minutes_per_bar = {"m": value, "h": value * 60, "d": value * 1440, "w": value * 10080}.get(unit, 60)
    return 1440.0 / minutes_per_bar if minutes_per_bar > 0 else 24.0


def screen_spot(assets: List[Dict[str, Any]], quote_assets: set[str], warnings: List[str]) -> Tuple[List[Dict[str, Any]], CriterionAudit]:
    """Pipeline de filtrage repris à l'identique du screener V3 (étapes 1 à 9),
    sans le module de régime CHOP/ADX/ATR qui est remplacé ici par la logique
    de signal trend following."""
    audit = CriterionAudit()
    assets = apply_criterion(assets, audit, "1. Status TRADING", lambda x: x["status"] == "TRADING")
    assets = apply_criterion(assets, audit, "2. Permission SPOT", lambda x: not x["permissions"] or "SPOT" in x["permissions"])
    assets = apply_criterion(assets, audit, "3. Quote asset autorisé", lambda x: x["quoteAsset"] in quote_assets)
    if EXCLUDE_STABLECOINS:
        assets = apply_criterion(assets, audit, "4. Exclusion stablecoins", lambda x: x["baseAsset"].upper() not in STABLECOIN_BASES)
    if EXCLUDE_LEVERAGED_TOKENS:
        assets = apply_criterion(assets, audit, "5. Exclusion tokens à levier", lambda x: not is_leveraged_symbol(x["baseAsset"]))
    assets = apply_criterion(assets, audit, "6. Données 24h disponibles", lambda x: x["lastPrice"] > 0)
    assets = apply_criterion(assets, audit, "7. Volume quote 24h minimum", lambda x: x["quoteVolume"] >= MIN_24H_QUOTE_VOLUME)
    if ORDER_BOOK_DEPTH_ENABLED:
        merge_spot_order_book_depth(assets, warnings)
        assets = apply_criterion(assets, audit, "8. Épaisseur carnet d'ordres minimum", lambda x: x.get("depthTotalQuote", 0.0) >= MIN_ORDER_BOOK_DEPTH_QUOTE)
    else:
        audit.add("8. Épaisseur carnet d'ordres minimum", len(assets), len(assets))
    assets = apply_criterion(assets, audit, "9. Spread maximum", lambda x: x["spreadPercent"] is not None and x["spreadPercent"] <= MAX_SPREAD_PERCENT)

    if MIN_HISTORY_DAYS > 0 and assets:
        required_history_bars = max(1, int(math.ceil(MIN_HISTORY_DAYS * bars_per_day(TREND_INTERVAL))))
        history_fetch_limit = max(required_history_bars, TREND_KLINES_LIMIT)

        def fetch_history(asset: Dict[str, Any]) -> None:
            try:
                klines = spot_klines(asset["symbol"], TREND_INTERVAL, history_fetch_limit)
            except Exception as exc:
                asset["historyError"] = str(exc)
                asset["_historyOk"] = False
                return
            asset["_historyKlines"] = klines
            asset["_historyOk"] = len(klines) >= required_history_bars

        workers = max(1, min(TREND_WORKERS, len(assets)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(fetch_history, assets))
        assets = apply_criterion(assets, audit, "10. Ancienneté minimale", lambda x: x.get("_historyOk", False))
    else:
        audit.add("10. Ancienneté minimale", len(assets), len(assets))

    return assets, audit


SPOT_KLINES_CACHE: Dict[Tuple[str, str, int], List[List[Any]]] = {}


def spot_klines(symbol: str, interval: Optional[str] = None, limit: Optional[int] = None) -> List[List[Any]]:
    effective_interval = interval or TREND_INTERVAL
    effective_limit = limit or TREND_KLINES_LIMIT
    cache_key = (symbol, effective_interval, effective_limit)
    if cache_key in SPOT_KLINES_CACHE:
        return SPOT_KLINES_CACHE[cache_key]
    data = http_get_json(SPOT_BASE_URL, "/api/v3/klines", {"symbol": symbol, "interval": effective_interval, "limit": effective_limit})
    if not isinstance(data, list):
        raise BinanceHTTPError(f"Réponse klines invalide pour {symbol}")
    SPOT_KLINES_CACHE[cache_key] = data
    return data


# ============================================================
# INDICATEURS
# ============================================================
def ema_series(values: List[float], period: int) -> List[Optional[float]]:
    if period <= 0 or len(values) < period:
        return [None] * len(values)
    result: List[Optional[float]] = [None] * (period - 1)
    multiplier = 2.0 / (period + 1)
    sma = sum(values[:period]) / period
    result.append(sma)
    prev = sma
    for value in values[period:]:
        prev = (value - prev) * multiplier + prev
        result.append(prev)
    return result


def sma_series(values: List[float], period: int) -> List[Optional[float]]:
    if period <= 0 or len(values) < period:
        return [None] * len(values)
    result: List[Optional[float]] = [None] * (period - 1)
    window_sum = sum(values[:period])
    result.append(window_sum / period)
    for i in range(period, len(values)):
        window_sum += values[i] - values[i - period]
        result.append(window_sum / period)
    return result


def stddev_series(values: List[float], period: int) -> List[Optional[float]]:
    if period <= 0 or len(values) < period:
        return [None] * len(values)
    result: List[Optional[float]] = [None] * (period - 1)
    for i in range(period - 1, len(values)):
        window = values[i - period + 1:i + 1]
        mean = sum(window) / period
        variance = sum((v - mean) ** 2 for v in window) / period
        result.append(math.sqrt(variance))
    return result


def atr_series(highs: List[float], lows: List[float], closes: List[float], period: int) -> List[Optional[float]]:
    n = len(closes)
    if period <= 0 or n <= period:
        return [None] * n
    trs = [None] + [
        max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        for i in range(1, n)
    ]
    result: List[Optional[float]] = [None] * n
    first = sum(t for t in trs[1:period + 1]) / period
    result[period] = first
    prev = first
    for i in range(period + 1, n):
        prev = (prev * (period - 1) + trs[i]) / period
        result[i] = prev
    return result


def adx_series(highs: List[float], lows: List[float], closes: List[float], period: int) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
    n = len(closes)
    if period <= 0 or n < period * 2 + 1:
        return [None] * n, [None] * n, [None] * n

    trs, plus_dm, minus_dm = [0.0], [0.0], [0.0]
    for i in range(1, n):
        up_move = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))

    adx_out: List[Optional[float]] = [None] * n
    plus_di_out: List[Optional[float]] = [None] * n
    minus_di_out: List[Optional[float]] = [None] * n

    sm_tr = sum(trs[1:period + 1])
    sm_plus = sum(plus_dm[1:period + 1])
    sm_minus = sum(minus_dm[1:period + 1])
    dx_values: List[float] = []

    def compute_di(idx: int) -> float:
        pdi = 100.0 * sm_plus / sm_tr if sm_tr > 0 else 0.0
        mdi = 100.0 * sm_minus / sm_tr if sm_tr > 0 else 0.0
        plus_di_out[idx] = pdi
        minus_di_out[idx] = mdi
        denom = pdi + mdi
        return 100.0 * abs(pdi - mdi) / denom if denom > 0 else 0.0

    dx_values.append(compute_di(period))
    for i in range(period + 1, n):
        sm_tr = sm_tr - (sm_tr / period) + trs[i]
        sm_plus = sm_plus - (sm_plus / period) + plus_dm[i]
        sm_minus = sm_minus - (sm_minus / period) + minus_dm[i]
        dx_values.append(compute_di(i))

    if len(dx_values) >= period:
        adx_value = sum(dx_values[:period]) / period
        adx_out[period * 2 - 1] = adx_value
        idx = period * 2
        for value in dx_values[period:]:
            adx_value = (adx_value * (period - 1) + value) / period
            if idx < n:
                adx_out[idx] = adx_value
            idx += 1

    return adx_out, plus_di_out, minus_di_out


def supertrend_series(highs: List[float], lows: List[float], closes: List[float], atr_period: int, multiplier: float) -> Tuple[List[Optional[float]], List[Optional[str]]]:
    """Supertrend standard (bandes ATR + logique de trailing). Retourne la
    valeur de la ligne et la direction ('UP'=haussier / 'DOWN'=baissier) par
    bougie. None tant que l'historique est insuffisant."""
    n = len(closes)
    atr = atr_series(highs, lows, closes, atr_period)
    st_value: List[Optional[float]] = [None] * n
    st_dir: List[Optional[str]] = [None] * n

    final_upper: Optional[float] = None
    final_lower: Optional[float] = None
    direction: Optional[str] = None

    for i in range(n):
        if atr[i] is None:
            continue
        hl2 = (highs[i] + lows[i]) / 2.0
        basic_upper = hl2 + multiplier * atr[i]
        basic_lower = hl2 - multiplier * atr[i]

        if final_upper is None or final_lower is None:
            final_upper, final_lower = basic_upper, basic_lower
            direction = "UP" if closes[i] >= hl2 else "DOWN"
            st_value[i] = final_lower if direction == "UP" else final_upper
            st_dir[i] = direction
            continue

        prev_close = closes[i - 1]
        final_upper = basic_upper if (basic_upper < final_upper or prev_close > final_upper) else final_upper
        final_lower = basic_lower if (basic_lower > final_lower or prev_close < final_lower) else final_lower

        if direction == "UP" and closes[i] < final_lower:
            direction = "DOWN"
        elif direction == "DOWN" and closes[i] > final_upper:
            direction = "UP"

        st_value[i] = final_lower if direction == "UP" else final_upper
        st_dir[i] = direction

    return st_value, st_dir


def macd_series(closes: List[float], fast: int, slow: int, signal: int) -> Tuple[List[Optional[float]], List[Optional[float]]]:
    ema_fast = ema_series(closes, fast)
    ema_slow = ema_series(closes, slow)
    macd_line = [
        (f - s) if (f is not None and s is not None) else None
        for f, s in zip(ema_fast, ema_slow)
    ]
    clean = [v for v in macd_line if v is not None]
    if len(clean) < signal:
        return macd_line, [None] * len(closes)
    signal_clean = ema_series(clean, signal)
    signal_line: List[Optional[float]] = [None] * (len(macd_line) - len(signal_clean)) + signal_clean
    return macd_line, signal_line


def ichimoku_cloud(highs: List[float], lows: List[float], tenkan_p: int, kijun_p: int, senkou_b_p: int, displacement: int) -> Tuple[List[Optional[float]], List[Optional[float]]]:
    """Retourne (senkou_a_raw, senkou_b_raw), non décalés. Le nuage applicable
    à la bougie i est senkou_a_raw[i-displacement] / senkou_b_raw[i-displacement]."""
    n = len(highs)

    def donchian_mid(period: int) -> List[Optional[float]]:
        out: List[Optional[float]] = [None] * n
        for i in range(period - 1, n):
            hh = max(highs[i - period + 1:i + 1])
            ll = min(lows[i - period + 1:i + 1])
            out[i] = (hh + ll) / 2.0
        return out

    tenkan = donchian_mid(tenkan_p)
    kijun = donchian_mid(kijun_p)
    senkou_b_raw = donchian_mid(senkou_b_p)
    senkou_a_raw = [
        (t + k) / 2.0 if (t is not None and k is not None) else None
        for t, k in zip(tenkan, kijun)
    ]
    return senkou_a_raw, senkou_b_raw


# ============================================================
# SURVEILLANCE EMA 50/200 — ALERTE UNIQUEMENT
# ============================================================
def closed_klines_only(klines: List[List[Any]]) -> List[List[Any]]:
    """Ne conserve que les bougies dont l'heure de clôture est passée."""
    now_ms = int(time.time() * 1000)
    return [row for row in klines if len(row) > 6 and as_int(row[6]) <= now_ms]


def load_ema_cross_state() -> Dict[str, Dict[str, Any]]:
    """Charge le dernier événement déjà alerté par symbole et direction."""
    try:
        with open(EMA_CROSS_STATE_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Avertissement EMA50/200 : état illisible ({exc}); état réinitialisé.")
        return {}


def save_ema_cross_state(state: Dict[str, Dict[str, Any]]) -> None:
    """Sauvegarde atomiquement l'état pour éviter les alertes répétées."""
    tmp_path = EMA_CROSS_STATE_FILE + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp_path, EMA_CROSS_STATE_FILE)


def compute_ema_cross(asset: Dict[str, Any], warnings: List[str]) -> None:
    """Analyse EMA50/EMA200 en 4H sans bloquer ni modifier le pipeline."""
    symbol = asset["symbol"]
    try:
        klines = spot_klines(symbol, EMA_CROSS_INTERVAL, EMA_CROSS_KLINES_LIMIT)
        klines = closed_klines_only(klines)
    except Exception as exc:
        asset["emaCrossError"] = str(exc)
        return

    minimum = max(EMA_CROSS_SLOW_PERIOD + 2, EMA_CROSS_FAST_PERIOD + 2)
    if len(klines) < minimum:
        asset["emaCrossError"] = f"Historique EMA50/200 insuffisant ({len(klines)}/{minimum})"
        return

    closes = [as_float(row[4]) for row in klines]
    ema50 = ema_series(closes, EMA_CROSS_FAST_PERIOD)
    ema200 = ema_series(closes, EMA_CROSS_SLOW_PERIOD)

    last = len(closes) - 1
    prev = last - 1
    if ema50[last] is None or ema200[last] is None or ema50[prev] is None or ema200[prev] is None:
        asset["emaCrossError"] = "EMA50/EMA200 non calculables sur les dernières bougies clôturées"
        return

    previous_fast = ema50[prev]
    previous_slow = ema200[prev]
    current_fast = ema50[last]
    current_slow = ema200[last]

    golden_cross = previous_fast <= previous_slow and current_fast > current_slow
    death_cross = previous_fast >= previous_slow and current_fast < current_slow

    close_time_ms = as_int(klines[last][6])
    close_time = datetime.fromtimestamp(close_time_ms / 1000.0, tz=timezone.utc)
    structure = "BULLISH" if current_fast > current_slow else "BEARISH" if current_fast < current_slow else "NEUTRAL"
    spread_pct = ((current_fast - current_slow) / current_slow * 100.0) if current_slow else None

    asset["emaCrossInterval"] = EMA_CROSS_INTERVAL
    asset["ema50"] = current_fast
    asset["ema200Cross"] = current_slow
    asset["ema50Ema200Structure"] = structure
    asset["ema50Ema200SpreadPct"] = spread_pct
    asset["goldenCross"] = golden_cross
    asset["deathCross"] = death_cross
    asset["emaCrossClosePrice"] = closes[last]
    asset["emaCrossCandleCloseMs"] = close_time_ms
    asset["emaCrossCandleCloseUtc"] = close_time.strftime("%Y-%m-%d %H:%M:%S UTC")
    asset["emaCrossAlertDirection"] = "GOLDEN CROSS" if golden_cross else "DEATH CROSS" if death_cross else None


def merge_ema_cross_alerts(assets: List[Dict[str, Any]], warnings: List[str]) -> Dict[str, Any]:
    """Surveille EMA50/200 après les 10 filtres; aucun rejet n'est effectué."""
    if not assets:
        return {"alerts": [], "goldenCrosses": 0, "deathCrosses": 0, "newAlerts": 0}

    workers = max(1, min(TREND_WORKERS, len(assets)))
    failures = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(compute_ema_cross, asset, warnings): asset for asset in assets}
        for future in concurrent.futures.as_completed(futures):
            asset = futures[future]
            try:
                future.result()
                if asset.get("emaCrossError"):
                    failures += 1
            except Exception as exc:
                failures += 1
                asset["emaCrossError"] = str(exc)

    if failures:
        warnings.append(f"Surveillance EMA50/200 : {failures} échecs sur {len(assets)} paires.")

    state = load_ema_cross_state()
    alerts: List[Dict[str, Any]] = []

    for asset in assets:
        direction = asset.get("emaCrossAlertDirection")
        if direction not in {"GOLDEN CROSS", "DEATH CROSS"}:
            continue

        close_time_ms = as_int(asset.get("emaCrossCandleCloseMs"))
        symbol = asset.get("symbol", "-")
        key = f"{symbol}:{direction}"
        previous_alert_ms = as_int(state.get(key, {}).get("candleCloseMs"), -1)

        # Un même croisement reste présent dans les données pendant plusieurs
        # scans horaires. Une clé persistante empêche donc toute répétition.
        is_new = close_time_ms > previous_alert_ms
        asset["emaCrossIsNewAlert"] = is_new

        if is_new:
            alert = {
                "symbol": symbol,
                "direction": direction,
                "price": asset.get("emaCrossClosePrice"),
                "ema50": asset.get("ema50"),
                "ema200": asset.get("ema200Cross"),
                "structure": asset.get("ema50Ema200Structure"),
                "spreadPct": asset.get("ema50Ema200SpreadPct"),
                "candleCloseMs": close_time_ms,
                "candleCloseUtc": asset.get("emaCrossCandleCloseUtc"),
            }
            alerts.append(alert)
            state[key] = {
                "candleCloseMs": close_time_ms,
                "candleCloseUtc": asset.get("emaCrossCandleCloseUtc"),
            }

    try:
        save_ema_cross_state(state)
    except OSError as exc:
        warnings.append(f"Surveillance EMA50/200 : impossible de sauvegarder l'état : {exc}")

    return {
        "alerts": alerts,
        "goldenCrosses": sum(1 for a in assets if a.get("goldenCross")),
        "deathCrosses": sum(1 for a in assets if a.get("deathCross")),
        "newAlerts": len(alerts),
    }


# ============================================================
# PARAMÈTRES DE RISQUE EMA50/EMA200 — CALCUL UNIQUEMENT
# ============================================================
def risk_levels(entry_price: float, direction: str) -> Dict[str, float]:
    """Calcule les niveaux théoriques SL / TP / activation trailing.

    Aucun suivi de marché n'est effectué ici. Le trailing est donc décrit par
    son seuil d'activation et sa distance, sans calcul d'un trailing stop
    évolutif après l'entrée.
    """
    sl = EMA_RISK_SL_PERCENT / 100.0
    tp = EMA_RISK_TP_PERCENT / 100.0
    trail = EMA_RISK_TRAIL_PERCENT / 100.0

    if direction == "LONG":
        return {
            "slPrice": entry_price * (1.0 - sl),
            "tpPrice": entry_price * (1.0 + tp),
            "trailActivationPrice": entry_price * (1.0 + trail),
            "trailDistancePercent": EMA_RISK_TRAIL_PERCENT,
        }

    return {
        "slPrice": entry_price * (1.0 + sl),
        "tpPrice": entry_price * (1.0 - tp),
        "trailActivationPrice": entry_price * (1.0 - trail),
        "trailDistancePercent": EMA_RISK_TRAIL_PERCENT,
    }


def build_ema_risk_parameters(ema_alerts: Dict[str, Any]) -> Dict[str, Any]:
    """Associe des paramètres SL/TP/trailing aux nouveaux croisements uniquement.

    Cette fonction ne crée aucune position virtuelle, ne lit aucun état de
    position et ne vérifie jamais si un niveau a été touché.
    """
    if not EMA_RISK_ENABLED:
        return {
            "enabled": False,
            "signals": [],
            "slPercent": EMA_RISK_SL_PERCENT,
            "tpPercent": EMA_RISK_TP_PERCENT,
            "trailPercent": EMA_RISK_TRAIL_PERCENT,
        }

    signals: List[Dict[str, Any]] = []
    for alert in ema_alerts.get("alerts", []):
        direction = "LONG" if alert.get("direction") == "GOLDEN CROSS" else "SHORT"
        entry_price = as_float(alert.get("price"))
        if entry_price <= 0:
            continue

        levels = risk_levels(entry_price, direction)
        signals.append({
            **alert,
            "tradeDirection": direction,
            **levels,
        })

    return {
        "enabled": True,
        "signals": signals,
        "slPercent": EMA_RISK_SL_PERCENT,
        "tpPercent": EMA_RISK_TP_PERCENT,
        "trailPercent": EMA_RISK_TRAIL_PERCENT,
    }


# ============================================================
# LOGIQUE DE SIGNAL
# ============================================================
def compute_trend_signal(asset: Dict[str, Any], warnings: List[str]) -> None:
    cached = asset.pop("_historyKlines", None)
    try:
        klines = (cached[-TREND_KLINES_LIMIT:] if cached is not None else spot_klines(asset["symbol"]))
    except Exception as exc:
        asset["signalError"] = str(exc)
        return

    min_required = max(EMA_TREND_PERIOD, ICHIMOKU_SENKOU_B_PERIOD + ICHIMOKU_DISPLACEMENT, ADX_PERIOD * 2 + 2) + 5
    if len(klines) < min_required:
        asset["signalError"] = f"Historique insuffisant ({len(klines)}/{min_required})"
        return

    highs = [as_float(row[2]) for row in klines]
    lows = [as_float(row[3]) for row in klines]
    closes = [as_float(row[4]) for row in klines]
    volumes = [as_float(row[5]) for row in klines]

    ema_trend = ema_series(closes, EMA_TREND_PERIOD)
    st_value, st_dir = supertrend_series(highs, lows, closes, SUPERTREND_ATR_PERIOD, SUPERTREND_MULTIPLIER)
    adx_vals, plus_di, minus_di = adx_series(highs, lows, closes, ADX_PERIOD)
    macd_line, macd_signal = macd_series(closes, MACD_FAST_PERIOD, MACD_SLOW_PERIOD, MACD_SIGNAL_PERIOD)
    bb_mid = sma_series(closes, BB_PERIOD)
    bb_std = stddev_series(closes, BB_PERIOD)
    volume_sma = sma_series(volumes, BB_VOLUME_SMA_PERIOD)
    senkou_a_raw, senkou_b_raw = ichimoku_cloud(highs, lows, ICHIMOKU_TENKAN_PERIOD, ICHIMOKU_KIJUN_PERIOD, ICHIMOKU_SENKOU_B_PERIOD, ICHIMOKU_DISPLACEMENT)

    last = len(closes) - 1
    prev = last - 1

    # --- Cœur du signal : flip Supertrend sur la dernière bougie close,
    # avec confirmation EMA200 (direction) et ADX (force) sur cette même
    # bougie. Rien n'est envoyé si le flip n'est pas confirmé.
    signal_direction: Optional[str] = None
    ema_diagnostic: Optional[Dict[str, Any]] = None
    is_flip = st_dir[last] is not None and st_dir[prev] is not None and st_dir[last] != st_dir[prev]

    if is_flip and ema_trend[last] is not None and adx_vals[last] is not None:
        flip_direction = "LONG" if st_dir[last] == "UP" else "SHORT"
        adx_ok = adx_vals[last] >= ADX_THRESHOLD
        direction_allowed = flip_direction in ("LONG", "SHORT") and (
            TREND_SIGNAL_DIRECTIONS == "BOTH" or TREND_SIGNAL_DIRECTIONS == flip_direction
        )
        ema_ok = (
            closes[last] > ema_trend[last] if flip_direction == "LONG"
            else closes[last] < ema_trend[last]
        )
        distance_pct = abs(closes[last] - ema_trend[last]) / ema_trend[last] * 100.0 if ema_trend[last] > 0 else None
        ema_diagnostic = {
            "symbol": asset.get("symbol", "-"),
            "direction": flip_direction,
            "price": closes[last],
            "ema200": ema_trend[last],
            "adx": adx_vals[last],
            "adxOk": adx_ok,
            "emaOk": ema_ok,
            "directionAllowed": direction_allowed,
            "distancePct": distance_pct,
            "emaRejected": bool(adx_ok and direction_allowed and not ema_ok),
        }
        if adx_ok and ema_ok and direction_allowed:
            signal_direction = flip_direction

    asset["signalDirection"] = signal_direction
    asset["emaDiagnostic"] = ema_diagnostic
    asset["emaTrend"] = ema_trend[last]
    asset["supertrendValue"] = st_value[last]
    asset["supertrendDirection"] = st_dir[last]
    asset["adx"] = adx_vals[last]
    asset["plusDI"] = plus_di[last]
    asset["minusDI"] = minus_di[last]
    asset["closePrice"] = closes[last]

    if signal_direction is None:
        return

    # --- Bonus MACD : ligne au-dessus/en-dessous du signal, dans le sens
    # du signal cœur.
    macd_bonus = False
    if macd_line[last] is not None and macd_signal[last] is not None:
        if signal_direction == "LONG" and macd_line[last] > macd_signal[last]:
            macd_bonus = True
        elif signal_direction == "SHORT" and macd_line[last] < macd_signal[last]:
            macd_bonus = True
    asset["macdLine"], asset["macdSignal"], asset["macdBonus"] = macd_line[last], macd_signal[last], macd_bonus

    # --- Bonus Bollinger : cassure de bande + volume, dans le sens du signal.
    bb_bonus = False
    bb_upper = bb_lower = None
    if bb_mid[last] is not None and bb_std[last] is not None:
        bb_upper = bb_mid[last] + BB_STDDEV_MULT * bb_std[last]
        bb_lower = bb_mid[last] - BB_STDDEV_MULT * bb_std[last]
        volume_ok = volume_sma[last] is not None and volumes[last] >= BB_VOLUME_MULT * volume_sma[last]
        if signal_direction == "LONG" and closes[last] > bb_upper and volume_ok:
            bb_bonus = True
        elif signal_direction == "SHORT" and closes[last] < bb_lower and volume_ok:
            bb_bonus = True
    asset["bbUpper"], asset["bbLower"], asset["bbBonus"] = bb_upper, bb_lower, bb_bonus

    # --- Bonus Ichimoku : prix au-dessus/en-dessous du nuage projeté.
    ichimoku_bonus = False
    cloud_top = cloud_bottom = None
    cloud_idx = last - ICHIMOKU_DISPLACEMENT
    if cloud_idx >= 0 and senkou_a_raw[cloud_idx] is not None and senkou_b_raw[cloud_idx] is not None:
        cloud_top = max(senkou_a_raw[cloud_idx], senkou_b_raw[cloud_idx])
        cloud_bottom = min(senkou_a_raw[cloud_idx], senkou_b_raw[cloud_idx])
        if signal_direction == "LONG" and closes[last] > cloud_top:
            ichimoku_bonus = True
        elif signal_direction == "SHORT" and closes[last] < cloud_bottom:
            ichimoku_bonus = True
    asset["cloudTop"], asset["cloudBottom"], asset["ichimokuBonus"] = cloud_top, cloud_bottom, ichimoku_bonus

    score = 0.0
    if macd_bonus:
        score += BONUS_WEIGHT_MACD
    if bb_bonus:
        score += BONUS_WEIGHT_BOLLINGER
    if ichimoku_bonus:
        score += BONUS_WEIGHT_ICHIMOKU
    asset["confidenceScore"] = score


def merge_trend_signals(assets: List[Dict[str, Any]], warnings: List[str]) -> None:
    if not assets:
        return
    workers = max(1, min(TREND_WORKERS, len(assets)))
    failures = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(compute_trend_signal, asset, warnings): asset for asset in assets}
        for future in concurrent.futures.as_completed(futures):
            asset = futures[future]
            try:
                future.result()
                if asset.get("signalError"):
                    failures += 1
            except Exception as exc:
                failures += 1
                asset["signalError"] = str(exc)
    if failures:
        warnings.append(f"Calcul du signal trend following : {failures} échecs sur {len(assets)} paires.")


def build_ema_diagnostic(assets: List[Dict[str, Any]]) -> Dict[str, Any]:
    diagnostics = [
        a["emaDiagnostic"] for a in assets
        if isinstance(a.get("emaDiagnostic"), dict)
    ]
    long_flips = [d for d in diagnostics if d.get("direction") == "LONG"]
    short_flips = [d for d in diagnostics if d.get("direction") == "SHORT"]
    adx_long = [d for d in long_flips if d.get("adxOk")]
    adx_short = [d for d in short_flips if d.get("adxOk")]
    ema_rejected = [d for d in diagnostics if d.get("emaRejected")]

    def buckets(items: List[Dict[str, Any]]) -> Dict[str, int]:
        return {
            "<=0.5%": sum(1 for d in items if d.get("distancePct") is not None and d["distancePct"] <= EMA_NEAR_05_PCT),
            "<=1.0%": sum(1 for d in items if d.get("distancePct") is not None and d["distancePct"] <= EMA_NEAR_1_PCT),
            "<=2.0%": sum(1 for d in items if d.get("distancePct") is not None and d["distancePct"] <= EMA_NEAR_2_PCT),
            "<=5.0%": sum(1 for d in items if d.get("distancePct") is not None and d["distancePct"] <= EMA_NEAR_5_PCT),
            ">5.0%": sum(1 for d in items if d.get("distancePct") is not None and d["distancePct"] > EMA_NEAR_5_PCT),
        }

    rejected_sorted = sorted(
        ema_rejected,
        key=lambda d: (d.get("distancePct") if d.get("distancePct") is not None else float("inf"))
    )
    return {
        "allFlips": len(diagnostics),
        "longFlips": len(long_flips),
        "shortFlips": len(short_flips),
        "longAdx": len(adx_long),
        "shortAdx": len(adx_short),
        "longAdxEma": sum(1 for d in adx_long if d.get("emaOk")),
        "shortAdxEma": sum(1 for d in adx_short if d.get("emaOk")),
        "emaRejected": len(ema_rejected),
        "emaRejectedLong": sum(1 for d in ema_rejected if d.get("direction") == "LONG"),
        "emaRejectedShort": sum(1 for d in ema_rejected if d.get("direction") == "SHORT"),
        "buckets": buckets(ema_rejected),
        "topRejected": rejected_sorted[:max(0, EMA_DIAGNOSTIC_TOP_N)],
    }


# ----------------------------- REPORTING -----------------------------
def format_number(value: Any, decimals: int = 4) -> str:
    try:
        return f"{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return "-"


def html_escape(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def audit_to_html(audit: CriterionAudit) -> str:
    rows = "".join(
        f"<tr><td>{html_escape(r['criterion'])}</td><td>{r['before']:,}</td><td>{r['selected']:,}</td>"
        f"<td>{r['rejected']:,}</td><td>{r['retention']:.2f}%</td><td>{r['rejection']:.2f}%</td></tr>"
        for r in audit.rows
    )
    return (
        "<table border='1' cellpadding='5' cellspacing='0'>"
        "<tr><th>Critère</th><th>Avant</th><th>Retenus</th><th>Rejetés</th><th>Rétention</th><th>Rejet</th></tr>"
        + rows + "</table>"
    )


def signals_table_html(title: str, signals: List[Dict[str, Any]]) -> str:
    if not signals:
        return f"<h3>{html_escape(title)} (0)</h3><p>Aucun signal.</p>"
    signals = sorted(signals, key=lambda a: as_float(a.get("confidenceScore")), reverse=True)
    html = [
        f"<h3>{html_escape(title)} ({len(signals)})</h3>",
        "<table border='1' cellpadding='5' cellspacing='0'>",
        "<tr><th>Symbol</th><th>Prix</th><th>EMA200</th><th>Supertrend</th><th>ADX</th>"
        "<th>Volume 24h</th><th>MACD</th><th>Bollinger</th><th>Ichimoku</th><th>Score confiance</th></tr>",
    ]
    for a in signals:
        html.append(
            "<tr>"
            f"<td><b>{html_escape(a.get('symbol', '-'))}</b></td>"
            f"<td>{format_number(a.get('closePrice'), 6)}</td>"
            f"<td>{format_number(a.get('emaTrend'), 6)}</td>"
            f"<td>{format_number(a.get('supertrendValue'), 6)}</td>"
            f"<td>{format_number(a.get('adx'), 2)}</td>"
            f"<td>${format_number(a.get('quoteVolume'), 0)}</td>"
            f"<td>{'✅' if a.get('macdBonus') else '—'}</td>"
            f"<td>{'✅' if a.get('bbBonus') else '—'}</td>"
            f"<td>{'✅' if a.get('ichimokuBonus') else '—'}</td>"
            f"<td>{a.get('confidenceScore', 0):.0f}/100</td>"
            "</tr>"
        )
    html.append("</table>")
    return "".join(html)


def ema_cross_alerts_table_html(ema_alerts: Dict[str, Any]) -> str:
    alerts = ema_alerts.get("alerts", []) if isinstance(ema_alerts, dict) else []
    if not alerts:
        return (
            "<hr><h2>11. Surveillance EMA 50 / EMA 200 — 4H</h2>"
            "<p>Aucun nouveau Golden Cross ou Death Cross détecté sur la dernière bougie 4H clôturée.</p>"
        )

    rows = []
    for a in alerts:
        marker = "🟢 GOLDEN CROSS" if a.get("direction") == "GOLDEN CROSS" else "🔴 DEATH CROSS"
        rows.append(
            "<tr>"
            f"<td><b>{html_escape(a.get('symbol', '-'))}</b></td>"
            f"<td>{marker}</td>"
            f"<td>{format_number(a.get('price'), 8)}</td>"
            f"<td>{format_number(a.get('ema50'), 8)}</td>"
            f"<td>{format_number(a.get('ema200'), 8)}</td>"
            f"<td>{html_escape(a.get('structure', '-'))}</td>"
            f"<td>{format_number(a.get('spreadPct'), 3)}%</td>"
            f"<td>{html_escape(a.get('candleCloseUtc', '-'))}</td>"
            "</tr>"
        )
    return (
        "<hr><h2>🚨 11. Alertes EMA 50 / EMA 200 — 4H</h2>"
        "<p><b>Ces alertes ne filtrent aucun actif.</b> Elles signalent uniquement un nouveau croisement "
        "sur la dernière bougie 4H clôturée des actifs ayant passé les 10 filtres.</p>"
        "<table border='1' cellpadding='5' cellspacing='0'>"
        "<tr><th>Symbol</th><th>Événement</th><th>Prix</th><th>EMA50</th><th>EMA200</th>"
        "<th>Structure</th><th>Écart EMA50/200</th><th>Clôture 4H</th></tr>"
        + "".join(rows) + "</table>"
    )


def ema_cross_watch_html(assets: List[Dict[str, Any]]) -> str:
    valid = [a for a in assets if a.get("ema50") is not None and a.get("ema200Cross") is not None]
    bullish = sum(1 for a in valid if a.get("ema50Ema200Structure") == "BULLISH")
    bearish = sum(1 for a in valid if a.get("ema50Ema200Structure") == "BEARISH")
    neutral = sum(1 for a in valid if a.get("ema50Ema200Structure") == "NEUTRAL")
    golden = sum(1 for a in valid if a.get("goldenCross"))
    death = sum(1 for a in valid if a.get("deathCross"))
    return (
        "<h3>Surveillance EMA50/EMA200 — état de l'univers retenu</h3>"
        f"<p>Actifs analysés en {html_escape(EMA_CROSS_INTERVAL)} : {len(valid):,} &nbsp;|&nbsp; "
        f"Structure BULLISH : {bullish:,} &nbsp;|&nbsp; BEARISH : {bearish:,} &nbsp;|&nbsp; NEUTRAL : {neutral:,}</p>"
        f"<p>Croisements détectés sur la dernière bougie clôturée : "
        f"Golden Cross {golden:,} &nbsp;|&nbsp; Death Cross {death:,}</p>"
    )


def ema_risk_html(ema_risk: Dict[str, Any]) -> str:
    if not ema_risk.get("enabled"):
        return "<hr><h2>12. Paramètres de risque EMA50/EMA200</h2><p>Module désactivé.</p>"

    signals = ema_risk.get("signals", [])
    rows = []
    for s in signals:
        direction = html_escape(s.get("tradeDirection", "-"))
        rows.append(
            "<tr>"
            f"<td><b>{html_escape(s.get('symbol', '-'))}</b></td>"
            f"<td>{direction}</td>"
            f"<td>{format_number(s.get('price'), 8)}</td>"
            f"<td>{format_number(s.get('slPrice'), 8)}</td>"
            f"<td>{format_number(s.get('tpPrice'), 8)}</td>"
            f"<td>{format_number(s.get('trailActivationPrice'), 8)}</td>"
            f"<td>{ema_risk.get('trailPercent', EMA_RISK_TRAIL_PERCENT):.2f}%</td>"
            f"<td>{html_escape(s.get('candleCloseUtc', '-'))}</td>"
            "</tr>"
        )

    table = (
        "<table border='1' cellpadding='5' cellspacing='0'>"
        "<tr><th>Symbol</th><th>Sens</th><th>Entrée référence</th><th>Stop Loss</th>"
        "<th>Take Profit</th><th>Activation trailing</th><th>Distance trailing</th><th>Clôture 4H</th></tr>"
        + "".join(rows) + "</table>"
        if rows else "<p>Aucun nouveau croisement : aucun niveau SL/TP/trailing à calculer sur ce scan.</p>"
    )

    return (
        "<hr><h2>12. Paramètres de risque EMA50 / EMA200 — 4H</h2>"
        "<p>Pour chaque nouveau Golden Cross / Death Cross, le système calcule uniquement "
        "les niveaux théoriques à partir du prix de référence de la bougie 4H clôturée. "
        "<b>Il ne suit pas le cours et ne détecte aucune sortie.</b></p>"
        f"<ul><li>Stop Loss fixe : {ema_risk.get('slPercent', EMA_RISK_SL_PERCENT):.2f}%</li>"
        f"<li>Take Profit fixe : {ema_risk.get('tpPercent', EMA_RISK_TP_PERCENT):.2f}%</li>"
        f"<li>Trailing : activation après +/− {ema_risk.get('trailPercent', EMA_RISK_TRAIL_PERCENT):.2f}% ; distance {ema_risk.get('trailPercent', EMA_RISK_TRAIL_PERCENT):.2f}%</li>"
        f"<li>Ratio rendement/risque théorique fixe : 1:{(EMA_RISK_TP_PERCENT / EMA_RISK_SL_PERCENT) if EMA_RISK_SL_PERCENT > 0 else 0:.2f}</li></ul>"
        "<h3>Niveaux calculés pour les nouveaux croisements</h3>" + table +
        "<p><b>Important :</b> le trailing stop n'est pas suivi. Le rapport fournit seulement "
        "son seuil d'activation et sa distance théorique.</p>"
    )


def build_report(assets: List[Dict[str, Any]], audit: CriterionAudit, warnings: List[str], errors: List[str], ema_diag: Optional[Dict[str, Any]] = None, ema_alerts: Optional[Dict[str, Any]] = None, ema_risk: Optional[Dict[str, Any]] = None) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    long_signals = [a for a in assets if a.get("signalDirection") == "LONG"]
    short_signals = [a for a in assets if a.get("signalDirection") == "SHORT"]
    ema_diag = ema_diag or build_ema_diagnostic(assets)
    ema_alerts = ema_alerts or {"alerts": [], "goldenCrosses": 0, "deathCrosses": 0, "newAlerts": 0}
    ema_risk = ema_risk or {"signals": [], "enabled": EMA_RISK_ENABLED, "slPercent": EMA_RISK_SL_PERCENT, "tpPercent": EMA_RISK_TP_PERCENT, "trailPercent": EMA_RISK_TRAIL_PERCENT}

    rejected_rows = []
    for d in ema_diag.get("topRejected", []):
        rejected_rows.append(
            "<tr>"
            f"<td><b>{html_escape(d.get('symbol', '-'))}</b></td>"
            f"<td>{html_escape(d.get('direction', '-'))}</td>"
            f"<td>{format_number(d.get('price'), 8)}</td>"
            f"<td>{format_number(d.get('ema200'), 8)}</td>"
            f"<td>{format_number(d.get('adx'), 2)}</td>"
            f"<td>{format_number(d.get('distancePct'), 2)}%</td>"
            "<td>EMA200 confirmation non satisfaite</td>"
            "</tr>"
        )
    rejected_table = (
        "<table border='1' cellpadding='5' cellspacing='0'>"
        "<tr><th>Symbol</th><th>Sens</th><th>Prix</th><th>EMA200</th><th>ADX</th><th>Écart EMA200</th><th>Motif</th></tr>"
        + "".join(rejected_rows)
        + "</table>"
        if rejected_rows else "<p>Aucun flip + ADX ne respecte les critères de cette watchlist EMA200.</p>"
    )

    b = ema_diag.get("buckets", {})
    ema_diagnostic_html = (
        "<hr><h2>Diagnostic expérimental — proximité EMA200</h2>"
        "<p>Diagnostic non bloquant : il mesure les flips Supertrend qui passent l'ADX "
        "mais sont rejetés uniquement par la confirmation EMA200. Il ne crée aucun signal.</p>"
        "<ul>"
        f"<li>Flips LONG : {ema_diag.get('longFlips', 0)} &nbsp;|&nbsp; Flips SHORT : {ema_diag.get('shortFlips', 0)}</li>"
        f"<li>LONG + ADX ≥ {ADX_THRESHOLD:.0f} : {ema_diag.get('longAdx', 0)} &nbsp;|&nbsp; LONG + ADX + EMA200 : {ema_diag.get('longAdxEma', 0)}</li>"
        f"<li>SHORT + ADX ≥ {ADX_THRESHOLD:.0f} : {ema_diag.get('shortAdx', 0)} &nbsp;|&nbsp; SHORT + ADX + EMA200 : {ema_diag.get('shortAdxEma', 0)}</li>"
        f"<li>Rejets EMA200 après ADX : {ema_diag.get('emaRejected', 0)} (LONG {ema_diag.get('emaRejectedLong', 0)} / SHORT {ema_diag.get('emaRejectedShort', 0)})</li>"
        "</ul>"
        "<p><b>Distance des candidats rejetés par EMA200 :</b> "
        f"≤ 0,5% : {b.get('≤0.5%', 0)} &nbsp;|&nbsp; "
        f"≤ 1% : {b.get('≤1.0%', 0)} &nbsp;|&nbsp; "
        f"≤ 2% : {b.get('≤2.0%', 0)} &nbsp;|&nbsp; "
        f"≤ 5% : {b.get('≤5.0%', 0)} &nbsp;|&nbsp; "
        f"> 5% : {b.get('>5.0%', 0)}</p>"
        "<h3>Top candidats rejetés par EMA200</h3>" + rejected_table
    )

    parts = [
        "<html><body>",
        "<h1>Binance Spot — Trend Following Scanner</h1>",
        f"<p><b>Date :</b> {now}</p>",
        f"<p><b>Univers analysé :</b> {len(assets):,} actifs après filtrage liquidité/spread.</p>",
        f"<p><b>Signaux LONG :</b> {len(long_signals)} &nbsp;|&nbsp; <b>Signaux SHORT :</b> {len(short_signals)}</p>",
        "<p>Signal cœur : flip Supertrend confirmé par l'EMA200 (direction) et l'ADX ≥ "
        f"{ADX_THRESHOLD:.0f} (force). MACD / Bollinger+Volume / Ichimoku sont des bonus non "
        "bloquants affichés comme score de confiance (0-100).</p>",
        "<hr>",
        signals_table_html("Signaux LONG", long_signals),
        signals_table_html("Signaux SHORT", short_signals),
        ema_cross_alerts_table_html(ema_alerts),
        ema_cross_watch_html(assets),
        ema_risk_html(ema_risk),
        ema_diagnostic_html,
        "<hr><h2>Pipeline de filtrage (repris du screener V3)</h2>",
        audit_to_html(audit),
        "<h2>Paramètres principaux</h2><ul>",
        f"<li>Timeframe Trend Following existant : {TREND_INTERVAL}</li>",
        f"<li>Surveillance EMA50/EMA200 : {EMA_CROSS_INTERVAL} — EMA{EMA_CROSS_FAST_PERIOD}/EMA{EMA_CROSS_SLOW_PERIOD} — aucune exclusion</li>",
        f"<li>Paramètres de risque EMA : SL {EMA_RISK_SL_PERCENT:.2f}% | TP {EMA_RISK_TP_PERCENT:.2f}% | Trailing activation/distance {EMA_RISK_TRAIL_PERCENT:.2f}%</li>",
        f"<li>Sens des signaux : {TREND_SIGNAL_DIRECTIONS}</li>",
        f"<li>EMA de tendance : {EMA_TREND_PERIOD}</li>",
        f"<li>Supertrend : ATR({SUPERTREND_ATR_PERIOD}) × {SUPERTREND_MULTIPLIER}</li>",
        f"<li>ADX({ADX_PERIOD}) seuil {ADX_THRESHOLD}</li>",
        f"<li>MACD({MACD_FAST_PERIOD},{MACD_SLOW_PERIOD},{MACD_SIGNAL_PERIOD}) — bonus</li>",
        f"<li>Bollinger({BB_PERIOD}, {BB_STDDEV_MULT}σ) + volume ≥ {BB_VOLUME_MULT}× SMA{BB_VOLUME_SMA_PERIOD} — bonus</li>",
        f"<li>Ichimoku({ICHIMOKU_TENKAN_PERIOD},{ICHIMOKU_KIJUN_PERIOD},{ICHIMOKU_SENKOU_B_PERIOD},{ICHIMOKU_DISPLACEMENT}) — bonus</li>",
        "</ul>",
    ]
    if warnings:
        parts.append("<h2>Avertissements</h2><ul>" + "".join(f"<li>{html_escape(w)}</li>" for w in warnings) + "</ul>")
    if errors:
        parts.append("<h2>Erreurs techniques</h2><ul>" + "".join(f"<li>{html_escape(e)}</li>" for e in errors) + "</ul>")
    parts.append("<hr><p><b>Important :</b> ce programme effectue uniquement de la collecte et de l'analyse de données de marché. Aucun ordre Binance n'est exécuté.</p></body></html>")
    return "".join(parts)


def send_email(subject: str, html: str) -> None:
    if not (EMAIL_USER and EMAIL_PASS and EMAIL_TO):
        raise RuntimeError("EMAIL_USER / EMAIL_PASS / EMAIL_TO non configurés")
    message = MIMEMultipart("alternative")
    message["Subject"], message["From"], message["To"] = subject, EMAIL_USER, EMAIL_TO
    message.attach(MIMEText(html, "html", "utf-8"))
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT, context=context) as server:
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, [EMAIL_TO], message.as_string())


# ----------------------------- MAIN -----------------------------
def main() -> int:
    started = time.time()
    warnings: List[str] = []
    errors: List[str] = []
    ema_alerts: Dict[str, Any] = {"alerts": [], "goldenCrosses": 0, "deathCrosses": 0, "newAlerts": 0}
    ema_risk: Dict[str, Any] = {"signals": [], "enabled": EMA_RISK_ENABLED, "slPercent": EMA_RISK_SL_PERCENT, "tpPercent": EMA_RISK_TP_PERCENT, "trailPercent": EMA_RISK_TRAIL_PERCENT}
    print("=" * 70)
    print("BINANCE SPOT — TREND FOLLOWING SCANNER")
    print("=" * 70)

    try:
        exchange = spot_exchange_info()
        assets = build_spot_universe(exchange)
        effective_quotes = resolve_spot_quote_assets(exchange)
        merge_spot_tickers(assets, spot_tickers())
        try:
            merge_spot_books(assets, spot_book_tickers())
        except Exception as exc:
            warnings.append(f"Spot bookTicker indisponible : {exc}")
            for asset in assets:
                asset["spreadPercent"] = None
        warnings.append(
            "Quote assets Spot dynamiques : " + ", ".join(sorted(effective_quotes))
            if effective_quotes else "Aucun stablecoin quote actif détecté."
        )
        assets, audit = screen_spot(assets, effective_quotes, warnings)
        print(f"Univers après filtrage liquidité/spread : {len(assets):,}")

        merge_trend_signals(assets, warnings)
        ema_diag = build_ema_diagnostic(assets)
        ema_alerts = merge_ema_cross_alerts(assets, warnings)
        ema_risk = build_ema_risk_parameters(ema_alerts)
        print(
            "Diagnostic EMA200 : "
            f"LONG flips={ema_diag['longFlips']} | LONG+ADX={ema_diag['longAdx']} | "
            f"LONG+ADX+EMA200={ema_diag['longAdxEma']} | "
            f"SHORT flips={ema_diag['shortFlips']} | SHORT+ADX={ema_diag['shortAdx']} | "
            f"SHORT+ADX+EMA200={ema_diag['shortAdxEma']} | "
            f"EMA rejects={ema_diag['emaRejected']}"
        )
        long_count = sum(1 for a in assets if a.get("signalDirection") == "LONG")
        short_count = sum(1 for a in assets if a.get("signalDirection") == "SHORT")
        print(f"Signaux LONG : {long_count} | Signaux SHORT : {short_count}")
        print(
            "EMA50/200 4H : "
            f"Golden Cross={ema_alerts.get('goldenCrosses', 0)} | "
            f"Death Cross={ema_alerts.get('deathCrosses', 0)} | "
            f"Nouvelles alertes={ema_alerts.get('newAlerts', 0)}"
        )

    except Exception as exc:
        errors.append(f"Erreur fatale : {exc}")
        assets, audit = [], CriterionAudit()
        ema_diag = build_ema_diagnostic(assets)

    if 'ema_diag' not in locals():
        ema_diag = build_ema_diagnostic(assets)
    html = build_report(assets, audit, warnings, errors, ema_diag, ema_alerts, ema_risk)
    elapsed = time.time() - started
    subject = f"Binance Trend Following — {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC"
    try:
        send_email(subject, html)
        print(f"\nEmail envoyé. Durée : {elapsed:.2f}s")
    except Exception as exc:
        print("\nERREUR EMAIL:", exc)
        print(html)
        return 1
    print(f"\nDurée totale : {elapsed:.2f}s")
    if errors:
        print(f"Erreurs techniques réelles : {len(errors)}")
    if warnings:
        print(f"Avertissements : {len(warnings)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())