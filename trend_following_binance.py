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
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


# ============================================================
# Binance Spot — Trend Following Scanner
#
# ARCHITECTURE
#   1D = tendance majeure
#   4H = confirmation + référence SL / trailing
#   1H = timing d'entrée
#
# SUPERTREND
#   Logique alignée sur l'implémentation TradingView :
#   - source HL2
#   - ATR Wilder / RMA
#   - bandes finales persistantes
#   - changement de direction sur cassure de bande
#
# AMA
#   Kaufman Adaptive Moving Average (KAMA)
#   période 50
#
# RISQUE
#   - SL = SuperTrend 4H par défaut
#   - trailing = SuperTrend 4H
#   - aucun TP fixe
#   - sortie conceptuelle = retournement du SuperTrend 4H
#
# IMPORTANT
#   Ce programme calcule et rapporte uniquement.
#   Aucun ordre Binance.
#   Aucun suivi de position.
#   Aucun calcul de P/L.
#   Aucun test ultérieur de déclenchement SL/TP/trailing.
# ============================================================


# ============================================================
# CONFIGURATION GÉNÉRALE
# ============================================================

BINANCE_BASE_URL = os.getenv(
    "BINANCE_BASE_URL",
    "https://data-api.binance.vision",
).rstrip("/")

BINANCE_SPOT_BASE_URL = os.getenv(
    "BINANCE_SPOT_BASE_URL",
    BINANCE_BASE_URL,
).rstrip("/")

BINANCE_WEB_BASE_URL = os.getenv(
    "BINANCE_WEB_BASE_URL",
    "https://www.binance.com",
).rstrip("/")


# ============================================================
# UNIVERS
# ============================================================

QUOTE_ASSETS_ENV = os.getenv(
    "QUOTE_ASSETS",
    "",
).strip()

EXCLUDE_STABLECOINS = (
    os.getenv("EXCLUDE_STABLECOINS", "true").lower() == "true"
)

EXCLUDE_LEVERAGED_TOKENS = (
    os.getenv("EXCLUDE_LEVERAGED_TOKENS", "true").lower() == "true"
)

MAX_SPREAD_PERCENT = float(
    os.getenv("MAX_SPREAD_PERCENT", "0.10")
)

MIN_24H_QUOTE_VOLUME = float(
    os.getenv("MIN_24H_QUOTE_VOLUME", "1000000")
)

MIN_HISTORY_DAYS = int(
    os.getenv("MIN_HISTORY_DAYS", "30")
)


# ============================================================
# ORDER BOOK
# ============================================================

ORDER_BOOK_DEPTH_ENABLED = (
    os.getenv("ORDER_BOOK_DEPTH_ENABLED", "true").lower() == "true"
)

ORDER_BOOK_DEPTH_LIMIT = int(
    os.getenv("ORDER_BOOK_DEPTH_LIMIT", "100")
)

ORDER_BOOK_DEPTH_PCT = float(
    os.getenv("ORDER_BOOK_DEPTH_PCT", "0.25")
)

MIN_ORDER_BOOK_DEPTH_QUOTE = float(
    os.getenv("MIN_ORDER_BOOK_DEPTH_QUOTE", "25000")
)

ORDER_BOOK_DEPTH_WORKERS = int(
    os.getenv("ORDER_BOOK_DEPTH_WORKERS", "8")
)


# ============================================================
# 1D — TENDANCE MAJEURE
# ============================================================

DAILY_INTERVAL = os.getenv(
    "DAILY_INTERVAL",
    "1d",
)

DAILY_AMA_PERIOD = int(
    os.getenv("DAILY_AMA_PERIOD", "50")
)

DAILY_AMA_FAST = int(
    os.getenv("DAILY_AMA_FAST", "2")
)

DAILY_AMA_SLOW = int(
    os.getenv("DAILY_AMA_SLOW", "30")
)

DAILY_KLINES_LIMIT = int(
    os.getenv("DAILY_KLINES_LIMIT", "300")
)

DAILY_SUPERTREND_ATR_PERIOD = int(
    os.getenv("DAILY_SUPERTREND_ATR_PERIOD", "10")
)

DAILY_SUPERTREND_MULTIPLIER = float(
    os.getenv("DAILY_SUPERTREND_MULTIPLIER", "3.0")
)


# ============================================================
# 4H — CONFIRMATION
# ============================================================

CONFIRM_INTERVAL = os.getenv(
    "CONFIRM_INTERVAL",
    "4h",
)

CONFIRM_AMA_PERIOD = int(
    os.getenv("CONFIRM_AMA_PERIOD", "50")
)

CONFIRM_AMA_FAST = int(
    os.getenv("CONFIRM_AMA_FAST", "2")
)

CONFIRM_AMA_SLOW = int(
    os.getenv("CONFIRM_AMA_SLOW", "30")
)

CONFIRM_SUPERTREND_ATR_PERIOD = int(
    os.getenv("CONFIRM_SUPERTREND_ATR_PERIOD", "10")
)

CONFIRM_SUPERTREND_MULTIPLIER = float(
    os.getenv("CONFIRM_SUPERTREND_MULTIPLIER", "3.0")
)

CONFIRM_KLINES_LIMIT = int(
    os.getenv("CONFIRM_KLINES_LIMIT", "300")
)


# ============================================================
# 1H — DÉCLENCHEUR
# ============================================================

TRIGGER_INTERVAL = os.getenv(
    "TRIGGER_INTERVAL",
    "1h",
)

TRIGGER_AMA_PERIOD = int(
    os.getenv("TRIGGER_AMA_PERIOD", "50")
)

TRIGGER_AMA_FAST = int(
    os.getenv("TRIGGER_AMA_FAST", "2")
)

TRIGGER_AMA_SLOW = int(
    os.getenv("TRIGGER_AMA_SLOW", "30")
)

TRIGGER_SUPERTREND_ATR_PERIOD = int(
    os.getenv("TRIGGER_SUPERTREND_ATR_PERIOD", "12")
)

TRIGGER_SUPERTREND_MULTIPLIER = float(
    os.getenv("TRIGGER_SUPERTREND_MULTIPLIER", "3.0")
)

TRIGGER_KLINES_LIMIT = int(
    os.getenv("TRIGGER_KLINES_LIMIT", "600")
)

TREND_SIGNAL_DIRECTIONS = os.getenv(
    "TREND_SIGNAL_DIRECTIONS",
    "BOTH",
).upper()


# ============================================================
# RISQUE
# ============================================================

RISK_ENABLED = (
    os.getenv("RISK_ENABLED", "true").lower() == "true"
)

RISK_SUPERTREND_SOURCE = (
    os.getenv("RISK_SUPERTREND_SOURCE", "4h")
    .strip()
    .lower()
)

if RISK_SUPERTREND_SOURCE not in {"1h", "4h"}:
    RISK_SUPERTREND_SOURCE = "4h"


# ============================================================
# INDICATEURS BONUS / INFORMATIFS
# ============================================================

ADX_PERIOD = int(
    os.getenv("ADX_PERIOD", "14")
)

ADX_THRESHOLD = float(
    os.getenv("ADX_THRESHOLD", "25")
)

MACD_FAST_PERIOD = int(
    os.getenv("MACD_FAST_PERIOD", "12")
)

MACD_SLOW_PERIOD = int(
    os.getenv("MACD_SLOW_PERIOD", "26")
)

MACD_SIGNAL_PERIOD = int(
    os.getenv("MACD_SIGNAL_PERIOD", "9")
)

BB_PERIOD = int(
    os.getenv("BB_PERIOD", "20")
)

BB_STDDEV_MULT = float(
    os.getenv("BB_STDDEV_MULT", "2.0")
)

BB_VOLUME_SMA_PERIOD = int(
    os.getenv("BB_VOLUME_SMA_PERIOD", "20")
)

BB_VOLUME_MULT = float(
    os.getenv("BB_VOLUME_MULT", "1.5")
)

ICHIMOKU_TENKAN_PERIOD = int(
    os.getenv("ICHIMOKU_TENKAN_PERIOD", "9")
)

ICHIMOKU_KIJUN_PERIOD = int(
    os.getenv("ICHIMOKU_KIJUN_PERIOD", "26")
)

ICHIMOKU_SENKOU_B_PERIOD = int(
    os.getenv("ICHIMOKU_SENKOU_B_PERIOD", "52")
)

ICHIMOKU_DISPLACEMENT = int(
    os.getenv("ICHIMOKU_DISPLACEMENT", "26")
)

BONUS_WEIGHT_MACD = float(
    os.getenv("BONUS_WEIGHT_MACD", "40")
)

BONUS_WEIGHT_BOLLINGER = float(
    os.getenv("BONUS_WEIGHT_BOLLINGER", "30")
)

BONUS_WEIGHT_ICHIMOKU = float(
    os.getenv("BONUS_WEIGHT_ICHIMOKU", "30")
)


# ============================================================
# HTTP
# ============================================================

HTTP_TIMEOUT_SECONDS = float(
    os.getenv("HTTP_TIMEOUT_SECONDS", "20")
)

HTTP_RETRIES = int(
    os.getenv("HTTP_RETRIES", "3")
)


# ============================================================
# EMAIL
# ============================================================

EMAIL_HOST = os.getenv(
    "EMAIL_HOST",
    "smtp.gmail.com",
)

EMAIL_PORT = int(
    os.getenv("EMAIL_PORT", "465")
)

EMAIL_USER = os.getenv(
    "EMAIL_USER",
    "",
)

EMAIL_PASS = os.getenv(
    "EMAIL_PASS",
    "",
)

EMAIL_TO = os.getenv(
    "EMAIL_TO",
    "",
)

EMAIL_TOP_RESULTS = int(
    os.getenv("EMAIL_TOP_RESULTS", "50")
)


# ============================================================
# HTTP / BINANCE
# ============================================================

def http_get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
) -> Any:

    if params:
        url = f"{url}?{urlencode(params)}"

    last_error: Optional[Exception] = None

    for attempt in range(1, HTTP_RETRIES + 1):

        try:

            request = Request(
                url,
                headers={
                    "User-Agent":
                        "Mozilla/5.0 "
                        "Binance-Trend-Following-Scanner/1.0",
                    "Accept": "application/json",
                },
                method="GET",
            )

            with urlopen(
                request,
                timeout=HTTP_TIMEOUT_SECONDS,
            ) as response:

                return json.loads(
                    response.read().decode("utf-8")
                )

        except (
            HTTPError,
            URLError,
            TimeoutError,
            json.JSONDecodeError,
        ) as exc:

            last_error = exc

            if attempt < HTTP_RETRIES:
                time.sleep(
                    min(2 ** (attempt - 1), 5)
                )

    raise RuntimeError(
        f"HTTP request failed: {url} | {last_error}"
    )


def get_exchange_info() -> Dict[str, Any]:

    return http_get_json(
        f"{BINANCE_SPOT_BASE_URL}/api/v3/exchangeInfo"
    )


def get_24h_tickers() -> List[Dict[str, Any]]:

    data = http_get_json(
        f"{BINANCE_SPOT_BASE_URL}/api/v3/ticker/24hr"
    )

    return data if isinstance(data, list) else []


def get_order_book(
    symbol: str,
) -> Dict[str, Any]:

    return http_get_json(
        f"{BINANCE_SPOT_BASE_URL}/api/v3/depth",
        {
            "symbol": symbol,
            "limit": ORDER_BOOK_DEPTH_LIMIT,
        },
    )


def get_klines(
    symbol: str,
    interval: str,
    limit: int,
) -> List[List[Any]]:

    return http_get_json(
        f"{BINANCE_SPOT_BASE_URL}/api/v3/klines",
        {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        },
    )


# ============================================================
# STABLECOINS DYNAMIQUES
# ============================================================

def get_dynamic_stablecoins() -> List[str]:

    url = (
        "https://www.binance.com/"
        "bapi/asset/v2/public/"
        "asset-service/product/get-products"
    )

    try:

        data = http_get_json(
            url,
            {"includeEtf": "true"},
        )

    except Exception:

        return []

    found = set()

    def walk(obj: Any) -> None:

        if isinstance(obj, dict):

            for key, value in obj.items():

                if isinstance(value, str):

                    key_lower = str(key).lower()

                    if key_lower in {
                        "assetcode",
                        "asset_code",
                        "baseasset",
                        "base_asset",
                        "symbol",
                        "coin",
                        "currency",
                        "asset",
                    }:

                        candidate = (
                            value.upper().strip()
                        )

                        if (
                            candidate.isalnum()
                            and 2 <= len(candidate) <= 20
                        ):

                            found.add(candidate)

                walk(value)

        elif isinstance(obj, list):

            for item in obj:
                walk(item)

    walk(data)

    common = {
        "USDT",
        "USDC",
        "FDUSD",
        "TUSD",
        "USDP",
        "USDS",
        "DAI",
        "USDE",
        "USD1",
        "RLUSD",
        "EURI",
        "U",
        "XUSD",
        "BFUSD",
        "KGST",
    }

    return sorted(
        found.intersection(common)
    )


def parse_quote_assets() -> List[str]:

    if QUOTE_ASSETS_ENV:

        return sorted({
            item.strip().upper()
            for item in QUOTE_ASSETS_ENV.split(",")
            if item.strip()
        })

    return sorted({
        "USDT",
        "USDC",
        "FDUSD",
        "TUSD",
        "USDP",
        "USDS",
        "USD1",
        "RLUSD",
        "EURI",
        "U",
    })


# ============================================================
# LEVERAGED TOKENS
# ============================================================

LEVERAGED_SUFFIXES = (
    "UP",
    "DOWN",
    "BULL",
    "BEAR",
)

LEVERAGED_REJECT_EXACT = {
    "BTCDOWN",
    "BTCUP",
    "ETHDOWN",
    "ETHUP",
}


def is_leveraged_token(
    base_asset: str,
    symbol: str,
) -> bool:

    base = base_asset.upper()
    sym = symbol.upper()

    if (
        base in LEVERAGED_REJECT_EXACT
        or sym in LEVERAGED_REJECT_EXACT
    ):
        return True

    return any(
        base.endswith(suffix)
        or sym.endswith(suffix)
        for suffix in LEVERAGED_SUFFIXES
    )


# ============================================================
# SPREAD / ORDER BOOK
# ============================================================

def compute_spread_percent(
    order_book: Dict[str, Any],
) -> Optional[float]:

    bids = order_book.get("bids") or []
    asks = order_book.get("asks") or []

    if not bids or not asks:
        return None

    try:

        bid = float(bids[0][0])
        ask = float(asks[0][0])

        mid = (bid + ask) / 2.0

        if mid <= 0:
            return None

        return (
            (ask - bid)
            / mid
            * 100.0
        )

    except (
        TypeError,
        ValueError,
        IndexError,
    ):

        return None


def compute_depth_quote(
    order_book: Dict[str, Any],
    center_price: float,
    pct: float,
) -> float:

    if center_price <= 0:
        return 0.0

    low = (
        center_price
        * (1.0 - pct / 100.0)
    )

    high = (
        center_price
        * (1.0 + pct / 100.0)
    )

    total = 0.0

    for side in (
        "bids",
        "asks",
    ):

        for row in order_book.get(side) or []:

            try:

                price = float(row[0])
                quantity = float(row[1])

            except (
                TypeError,
                ValueError,
                IndexError,
            ):

                continue

            if low <= price <= high:

                total += (
                    price * quantity
                )

    return total


# ============================================================
# CONSTRUCTION DE L'UNIVERS
# ============================================================

def build_universe() -> Tuple[
    List[Dict[str, Any]],
    Dict[str, Any],
]:

    exchange = get_exchange_info()
    tickers = get_24h_tickers()

    ticker_map = {
        str(item.get("symbol", "")).upper(): item
        for item in tickers
        if item.get("symbol")
    }

    symbols = exchange.get("symbols") or []

    quote_assets = set(
        parse_quote_assets()
    )

    dynamic_stablecoins = (
        set(get_dynamic_stablecoins())
        if EXCLUDE_STABLECOINS
        else set()
    )

    stats: Dict[str, Any] = {
        "spotUniverse": len(symbols),
        "quoteAssets": sorted(quote_assets),
        "dynamicStablecoins":
            sorted(dynamic_stablecoins),
        "steps": [],
    }

    def record(
        name: str,
        before: int,
        after: int,
    ) -> None:

        stats["steps"].append({
            "name": name,
            "before": before,
            "after": after,
        })

    # --------------------------------------------------------
    # 1. TRADING
    # --------------------------------------------------------

    current = [
        symbol
        for symbol in symbols
        if symbol.get("status") == "TRADING"
    ]

    record(
        "TRADING",
        len(symbols),
        len(current),
    )

    # --------------------------------------------------------
    # 2. SPOT
    # --------------------------------------------------------

    before = len(current)

    current = [
        symbol
        for symbol in current
        if (
            "SPOT"
            in (symbol.get("permissions") or [])
        )
        or any(
            "SPOT" in permission_set
            for permission_set
            in (symbol.get("permissionSets") or [])
        )
    ]

    record(
        "SPOT",
        before,
        len(current),
    )

    # --------------------------------------------------------
    # 3. QUOTE ASSET
    # --------------------------------------------------------

    before = len(current)

    current = [
        symbol
        for symbol in current
        if str(
            symbol.get("quoteAsset", "")
        ).upper()
        in quote_assets
    ]

    record(
        "Quote asset",
        before,
        len(current),
    )

    # --------------------------------------------------------
    # 4. STABLECOINS
    # --------------------------------------------------------

    before = len(current)

    if EXCLUDE_STABLECOINS:

        current = [
            symbol
            for symbol in current
            if str(
                symbol.get("baseAsset", "")
            ).upper()
            not in dynamic_stablecoins
        ]

    record(
        "Stablecoin exclusion",
        before,
        len(current),
    )

    # --------------------------------------------------------
    # 5. LEVERAGED TOKENS
    # --------------------------------------------------------

    before = len(current)

    if EXCLUDE_LEVERAGED_TOKENS:

        current = [
            symbol
            for symbol in current
            if not is_leveraged_token(
                str(symbol.get("baseAsset", "")),
                str(symbol.get("symbol", "")),
            )
        ]

    record(
        "Leveraged",
        before,
        len(current),
    )

    # --------------------------------------------------------
    # 6. TICKER / PRIX
    # --------------------------------------------------------

    before = len(current)

    valid = []

    for symbol_info in current:

        symbol = str(
            symbol_info.get("symbol", "")
        ).upper()

        ticker = ticker_map.get(symbol)

        if not ticker:
            continue

        try:

            price = float(
                ticker.get("lastPrice", 0)
            )

            quote_volume = float(
                ticker.get("quoteVolume", 0)
            )

        except (
            TypeError,
            ValueError,
        ):

            continue

        if price > 0:

            symbol_info["_ticker"] = ticker
            symbol_info["_price"] = price
            symbol_info["_quote_volume"] = (
                quote_volume
            )

            valid.append(symbol_info)

    current = valid

    record(
        "Valid 24H/price",
        before,
        len(current),
    )

    # --------------------------------------------------------
    # 7. VOLUME 24H
    # --------------------------------------------------------

    before = len(current)

    current = [
        symbol
        for symbol in current
        if float(
            symbol.get("_quote_volume", 0)
        ) >= MIN_24H_QUOTE_VOLUME
    ]

    record(
        "24H quote volume",
        before,
        len(current),
    )

    # --------------------------------------------------------
    # 8. ORDER BOOK
    # --------------------------------------------------------

    if ORDER_BOOK_DEPTH_ENABLED:

        def enrich(
            symbol_info: Dict[str, Any],
        ) -> Optional[Dict[str, Any]]:

            try:

                book = get_order_book(
                    str(
                        symbol_info["symbol"]
                    ).upper()
                )

                symbol_info["_spread"] = (
                    compute_spread_percent(book)
                )

                symbol_info["_depth_quote"] = (
                    compute_depth_quote(
                        book,
                        float(
                            symbol_info["_price"]
                        ),
                        ORDER_BOOK_DEPTH_PCT,
                    )
                )

                return symbol_info

            except Exception:

                return None

        enriched = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max(
                1,
                ORDER_BOOK_DEPTH_WORKERS,
            )
        ) as executor:

            for item in executor.map(
                enrich,
                current,
            ):

                if item is not None:
                    enriched.append(item)

        before = len(current)

        current = [
            symbol
            for symbol in enriched
            if float(
                symbol.get(
                    "_depth_quote",
                    0,
                )
            ) >= MIN_ORDER_BOOK_DEPTH_QUOTE
        ]

        record(
            "Order book depth",
            before,
            len(current),
        )

    else:

        record(
            "Order book depth",
            len(current),
            len(current),
        )

    # --------------------------------------------------------
    # 9. SPREAD
    # --------------------------------------------------------

    before = len(current)

    current = [
        symbol
        for symbol in current
        if (
            symbol.get("_spread") is None
            or float(
                symbol.get("_spread")
            ) <= MAX_SPREAD_PERCENT
        )
    ]

    record(
        "Spread",
        before,
        len(current),
    )

    # --------------------------------------------------------
    # 10. HISTORIQUE 30 JOURS
    # --------------------------------------------------------

    before = len(current)

    final = []

    for symbol_info in current:

        try:

            klines = get_klines(
                str(
                    symbol_info["symbol"]
                ).upper(),
                "1d",
                max(
                    MIN_HISTORY_DAYS + 5,
                    40,
                ),
            )

            closed = closed_klines_only(
                klines
            )

            if len(closed) >= MIN_HISTORY_DAYS:

                symbol_info["_history_days"] = (
                    len(closed)
                )

                final.append(symbol_info)

        except Exception:

            continue

    record(
        "30-day history",
        before,
        len(final),
    )

    stats["finalUniverse"] = len(final)

    return final, stats


# ============================================================
# BOUGIES CLÔTURÉES
# ============================================================

def closed_klines_only(
    klines: List[List[Any]],
) -> List[List[Any]]:

    now_ms = int(
        datetime.now(
            timezone.utc
        ).timestamp()
        * 1000
    )

    return [
        candle
        for candle in klines
        if (
            len(candle) > 6
            and int(candle[6]) <= now_ms
        )
    ]


def series_from_klines(
    klines: List[List[Any]],
) -> Tuple[
    List[float],
    List[float],
    List[float],
    List[float],
    List[float],
    List[int],
]:

    closed = closed_klines_only(
        klines
    )

    return (
        [float(candle[1]) for candle in closed],
        [float(candle[2]) for candle in closed],
        [float(candle[3]) for candle in closed],
        [float(candle[4]) for candle in closed],
        [float(candle[5]) for candle in closed],
        [int(candle[6]) for candle in closed],
    )


# ============================================================
# TRUE RANGE
# ============================================================

def true_range_series(
    highs: List[float],
    lows: List[float],
    closes: List[float],
) -> List[float]:

    n = len(closes)

    tr = [0.0] * n

    for i in range(n):

        if i == 0:

            tr[i] = (
                highs[i]
                - lows[i]
            )

        else:

            tr[i] = max(
                highs[i] - lows[i],
                abs(
                    highs[i]
                    - closes[i - 1]
                ),
                abs(
                    lows[i]
                    - closes[i - 1]
                ),
            )

    return tr


# ============================================================
# RMA / WILDER
# ============================================================

def rma_series(
    values: List[float],
    period: int,
) -> List[Optional[float]]:

    n = len(values)

    output: List[
        Optional[float]
    ] = [None] * n

    if (
        period <= 0
        or n < period
    ):

        return output

    previous = (
        sum(values[:period])
        / period
    )

    output[period - 1] = previous

    for i in range(
        period,
        n,
    ):

        previous = (
            (
                previous
                * (period - 1)
            )
            + values[i]
        ) / period

        output[i] = previous

    return output


# ============================================================
# ATR WILDER
# ============================================================

def atr_series(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int,
) -> List[Optional[float]]:

    return rma_series(
        true_range_series(
            highs,
            lows,
            closes,
        ),
        period,
    )


# ============================================================
# SUPERTREND — LOGIQUE TRADINGVIEW
# ============================================================

def supertrend_series(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    atr_period: int,
    multiplier: float,
) -> Tuple[
    List[Optional[float]],
    List[str],
]:

    """
    SuperTrend aligné sur la logique standard TradingView.

    Source :
        HL2 = (High + Low) / 2

    ATR :
        Wilder RMA

    Basic bands :
        Upper = HL2 + multiplier * ATR
        Lower = HL2 - multiplier * ATR

    Final bands :
        bandes persistantes selon la logique SuperTrend.

    Direction :
        DOWN tant que le cours ne casse pas la bande haute.
        UP tant que le cours ne casse pas la bande basse.

    La direction et le niveau retournés sont ceux de la
    dernière bougie clôturée fournie à cette fonction.
    """

    n = len(closes)

    atr = atr_series(
        highs,
        lows,
        closes,
        atr_period,
    )

    final_upper: List[
        Optional[float]
    ] = [None] * n

    final_lower: List[
        Optional[float]
    ] = [None] * n

    direction = [
        "DOWN"
        for _ in range(n)
    ]

    supertrend: List[
        Optional[float]
    ] = [None] * n

    for i in range(n):

        if atr[i] is None:

            direction[i] = "DOWN"
            continue

        hl2 = (
            highs[i]
            + lows[i]
        ) / 2.0

        basic_upper = (
            hl2
            + multiplier
            * float(atr[i])
        )

        basic_lower = (
            hl2
            - multiplier
            * float(atr[i])
        )

        # ----------------------------------------------------
        # Bande haute finale
        # ----------------------------------------------------

        if (
            i == 0
            or final_upper[i - 1] is None
        ):

            final_upper[i] = basic_upper

        else:

            previous_upper = float(
                final_upper[i - 1]
            )

            if (
                basic_upper < previous_upper
                or closes[i - 1]
                > previous_upper
            ):

                final_upper[i] = (
                    basic_upper
                )

            else:

                final_upper[i] = (
                    previous_upper
                )

        # ----------------------------------------------------
        # Bande basse finale
        # ----------------------------------------------------

        if (
            i == 0
            or final_lower[i - 1] is None
        ):

            final_lower[i] = basic_lower

        else:

            previous_lower = float(
                final_lower[i - 1]
            )

            if (
                basic_lower > previous_lower
                or closes[i - 1]
                < previous_lower
            ):

                final_lower[i] = (
                    basic_lower
                )

            else:

                final_lower[i] = (
                    previous_lower
                )

        # ----------------------------------------------------
        # Direction
        # ----------------------------------------------------

        if i == 0:

            direction[i] = "DOWN"

        elif direction[i - 1] == "DOWN":

            if (
                closes[i]
                > float(final_upper[i])
            ):

                direction[i] = "UP"

            else:

                direction[i] = "DOWN"

        else:

            if (
                closes[i]
                < float(final_lower[i])
            ):

                direction[i] = "DOWN"

            else:

                direction[i] = "UP"

        # ----------------------------------------------------
        # Valeur SuperTrend
        # ----------------------------------------------------

        if direction[i] == "UP":

            supertrend[i] = float(
                final_lower[i]
            )

        else:

            supertrend[i] = float(
                final_upper[i]
            )

    return (
        supertrend,
        direction,
    )


# ============================================================
# KAMA / AMA
# ============================================================

def adaptive_ma_series(
    closes: List[float],
    period: int = 50,
    fast: int = 2,
    slow: int = 30,
) -> List[Optional[float]]:

    """
    Kaufman Adaptive Moving Average.

    Période principale :
        50

    Fast :
        2

    Slow :
        30

    La pente de l'AMA est utilisée comme filtre
    directionnel.
    """

    n = len(closes)

    output: List[
        Optional[float]
    ] = [None] * n

    if (
        n <= period
        or period <= 0
        or fast <= 0
        or slow <= 0
    ):

        return output

    fast_sc = (
        2.0
        / (fast + 1.0)
    )

    slow_sc = (
        2.0
        / (slow + 1.0)
    )

    output[period] = closes[period]

    for i in range(
        period + 1,
        n,
    ):

        change = abs(
            closes[i]
            - closes[i - period]
        )

        volatility = sum(
            abs(
                closes[j]
                - closes[j - 1]
            )
            for j in range(
                i - period + 1,
                i + 1,
            )
        )

        if volatility:

            efficiency_ratio = (
                change
                / volatility
            )

        else:

            efficiency_ratio = 0.0

        smoothing_constant = (
            efficiency_ratio
            * (
                fast_sc
                - slow_sc
            )
            + slow_sc
        ) ** 2

        previous = output[i - 1]

        if previous is None:
            previous = closes[i - 1]

        output[i] = (
            previous
            + smoothing_constant
            * (
                closes[i]
                - previous
            )
        )

    return output


# ============================================================
# EMA
# ============================================================

def ema_series(
    values: List[float],
    period: int,
) -> List[Optional[float]]:

    output: List[
        Optional[float]
    ] = [None] * len(values)

    if (
        period <= 0
        or len(values) < period
    ):

        return output

    previous = (
        sum(values[:period])
        / period
    )

    output[period - 1] = previous

    alpha = (
        2.0
        / (period + 1.0)
    )

    for i in range(
        period,
        len(values),
    ):

        previous = (
            alpha * values[i]
            + (
                1.0 - alpha
            ) * previous
        )

        output[i] = previous

    return output


# ============================================================
# MACD
# ============================================================

def macd(
    closes: List[float],
    fast: int,
    slow: int,
    signal: int,
) -> Tuple[
    List[Optional[float]],
    List[Optional[float]],
    List[Optional[float]],
]:

    fast_ema = ema_series(
        closes,
        fast,
    )

    slow_ema = ema_series(
        closes,
        slow,
    )

    line: List[
        Optional[float]
    ] = [None] * len(closes)

    compact = []
    indices = []

    for i in range(len(closes)):

        if (
            fast_ema[i] is not None
            and slow_ema[i] is not None
        ):

            line[i] = (
                fast_ema[i]
                - slow_ema[i]
            )

            compact.append(line[i])
            indices.append(i)

    signal_compact = ema_series(
        compact,
        signal,
    )

    signal_line: List[
        Optional[float]
    ] = [None] * len(closes)

    for j, index in enumerate(indices):

        signal_line[index] = (
            signal_compact[j]
        )

    histogram: List[
        Optional[float]
    ] = [None] * len(closes)

    for i in range(len(closes)):

        if (
            line[i] is not None
            and signal_line[i] is not None
        ):

            histogram[i] = (
                line[i]
                - signal_line[i]
            )

    return (
        line,
        signal_line,
        histogram,
    )


# ============================================================
# BOLLINGER
# ============================================================

def bollinger(
    closes: List[float],
    period: int,
    std_mult: float,
) -> Tuple[
    List[Optional[float]],
    List[Optional[float]],
    List[Optional[float]],
]:

    n = len(closes)

    middle: List[
        Optional[float]
    ] = [None] * n

    upper: List[
        Optional[float]
    ] = [None] * n

    lower: List[
        Optional[float]
    ] = [None] * n

    for i in range(
        period - 1,
        n,
    ):

        window = closes[
            i - period + 1:
            i + 1
        ]

        mean = (
            sum(window)
            / period
        )

        variance = (
            sum(
                (value - mean) ** 2
                for value in window
            )
            / period
        )

        standard_deviation = math.sqrt(
            max(
                variance,
                0.0,
            )
        )

        middle[i] = mean

        upper[i] = (
            mean
            + std_mult
            * standard_deviation
        )

        lower[i] = (
            mean
            - std_mult
            * standard_deviation
        )

    return (
        middle,
        upper,
        lower,
    )


# ============================================================
# ADX
# ============================================================

def adx_series(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int,
) -> List[Optional[float]]:

    n = len(closes)

    if n < period + 1:

        return [
            None
            for _ in range(n)
        ]

    tr = true_range_series(
        highs,
        lows,
        closes,
    )

    plus_dm = [
        0.0
        for _ in range(n)
    ]

    minus_dm = [
        0.0
        for _ in range(n)
    ]

    for i in range(
        1,
        n,
    ):

        upward_move = (
            highs[i]
            - highs[i - 1]
        )

        downward_move = (
            lows[i - 1]
            - lows[i]
        )

        if (
            upward_move
            > downward_move
            and upward_move > 0
        ):

            plus_dm[i] = (
                upward_move
            )

        if (
            downward_move
            > upward_move
            and downward_move > 0
        ):

            minus_dm[i] = (
                downward_move
            )

    atr = rma_series(
        tr,
        period,
    )

    plus_rma = rma_series(
        plus_dm,
        period,
    )

    minus_rma = rma_series(
        minus_dm,
        period,
    )

    dx = [
        0.0
        for _ in range(n)
    ]

    valid = [
        False
        for _ in range(n)
    ]

    for i in range(n):

        if (
            atr[i] is None
            or plus_rma[i] is None
            or minus_rma[i] is None
        ):

            continue

        atr_value = float(
            atr[i]
        )

        if atr_value:

            plus_di = (
                100.0
                * float(plus_rma[i])
                / atr_value
            )

            minus_di = (
                100.0
                * float(minus_rma[i])
                / atr_value
            )

        else:

            plus_di = 0.0
            minus_di = 0.0

        denominator = (
            plus_di
            + minus_di
        )

        if denominator:

            dx[i] = (
                100.0
                * abs(
                    plus_di
                    - minus_di
                )
                / denominator
            )

        else:

            dx[i] = 0.0

        valid[i] = True

    compact = [
        dx[i]
        for i in range(n)
        if valid[i]
    ]

    smoothed = rma_series(
        compact,
        period,
    )

    output: List[
        Optional[float]
    ] = [None] * n

    compact_index = 0

    for i in range(n):

        if valid[i]:

            output[i] = (
                smoothed[
                    compact_index
                ]
            )

            compact_index += 1

    return output


# ============================================================
# ICHIMOKU
# ============================================================

def ichimoku(
    highs: List[float],
    lows: List[float],
    displacement: int,
    tenkan_period: int,
    kijun_period: int,
    senkou_b_period: int,
) -> Dict[
    str,
    List[Optional[float]],
]:

    n = len(highs)

    def midpoint(
        period: int,
    ) -> List[Optional[float]]:

        output: List[
            Optional[float]
        ] = [None] * n

        for i in range(
            period - 1,
            n,
        ):

            output[i] = (
                max(
                    highs[
                        i - period + 1:
                        i + 1
                    ]
                )
                + min(
                    lows[
                        i - period + 1:
                        i + 1
                    ]
                )
            ) / 2.0

        return output

    tenkan = midpoint(
        tenkan_period
    )

    kijun = midpoint(
        kijun_period
    )

    span_a: List[
        Optional[float]
    ] = [None] * n

    span_b = midpoint(
        senkou_b_period
    )

    for i in range(n):

        if (
            tenkan[i] is not None
            and kijun[i] is not None
        ):

            span_a[i] = (
                tenkan[i]
                + kijun[i]
            ) / 2.0

    return {
        "tenkan": tenkan,
        "kijun": kijun,
        "spanA": span_a,
        "spanB": span_b,
    }


# ============================================================
# DERNIERS POINTS AMA VALIDES
# ============================================================

def last_two_valid(
    values: List[
        Optional[float]
    ],
) -> Optional[
    Tuple[int, int]
]:

    valid_indices = [
        i
        for i, value in enumerate(values)
        if value is not None
    ]

    if len(valid_indices) < 2:

        return None

    return (
        valid_indices[-2],
        valid_indices[-1],
    )


# ============================================================
# SIGNAL MTF
# ============================================================

def compute_mtf_signal(
    symbol: str,
) -> Dict[str, Any]:

    result: Dict[str, Any] = {
        "symbol": symbol,
        "error": None,
        "signalDirection": None,
    }

    try:

        # ----------------------------------------------------
        # DONNÉES
        # ----------------------------------------------------

        daily_klines = get_klines(
            symbol,
            DAILY_INTERVAL,
            DAILY_KLINES_LIMIT,
        )

        confirm_klines = get_klines(
            symbol,
            CONFIRM_INTERVAL,
            CONFIRM_KLINES_LIMIT,
        )

        trigger_klines = get_klines(
            symbol,
            TRIGGER_INTERVAL,
            TRIGGER_KLINES_LIMIT,
        )

        (
            _,
            daily_high,
            daily_low,
            daily_close,
            _,
            daily_close_time,
        ) = series_from_klines(
            daily_klines
        )

        (
            _,
            confirm_high,
            confirm_low,
            confirm_close,
            _,
            confirm_close_time,
        ) = series_from_klines(
            confirm_klines
        )

        (
            _,
            trigger_high,
            trigger_low,
            trigger_close,
            trigger_volume,
            trigger_close_time,
        ) = series_from_klines(
            trigger_klines
        )

        # ----------------------------------------------------
        # HISTORIQUE MINIMUM
        # ----------------------------------------------------

        if len(daily_close) < (
            DAILY_AMA_PERIOD + 2
        ):

            raise RuntimeError(
                "Insufficient 1D history"
            )

        if len(confirm_close) < (
            CONFIRM_AMA_PERIOD + 2
        ):

            raise RuntimeError(
                "Insufficient 4H history"
            )

        if len(trigger_close) < (
            TRIGGER_AMA_PERIOD + 2
        ):

            raise RuntimeError(
                "Insufficient 1H history"
            )

        # ----------------------------------------------------
        # AMA
        # ----------------------------------------------------

        daily_ama = adaptive_ma_series(
            daily_close,
            DAILY_AMA_PERIOD,
            DAILY_AMA_FAST,
            DAILY_AMA_SLOW,
        )

        confirm_ama = adaptive_ma_series(
            confirm_close,
            CONFIRM_AMA_PERIOD,
            CONFIRM_AMA_FAST,
            CONFIRM_AMA_SLOW,
        )

        trigger_ama = adaptive_ma_series(
            trigger_close,
            TRIGGER_AMA_PERIOD,
            TRIGGER_AMA_FAST,
            TRIGGER_AMA_SLOW,
        )

        # ----------------------------------------------------
        # SUPERTREND
        # ----------------------------------------------------

        daily_supertrend, daily_direction = (
            supertrend_series(
                daily_high,
                daily_low,
                daily_close,
                DAILY_SUPERTREND_ATR_PERIOD,
                DAILY_SUPERTREND_MULTIPLIER,
            )
        )

        confirm_supertrend, confirm_direction = (
            supertrend_series(
                confirm_high,
                confirm_low,
                confirm_close,
                CONFIRM_SUPERTREND_ATR_PERIOD,
                CONFIRM_SUPERTREND_MULTIPLIER,
            )
        )

        trigger_supertrend, trigger_direction = (
            supertrend_series(
                trigger_high,
                trigger_low,
                trigger_close,
                TRIGGER_SUPERTREND_ATR_PERIOD,
                TRIGGER_SUPERTREND_MULTIPLIER,
            )
        )

        # ----------------------------------------------------
        # INDICES AMA
        # ----------------------------------------------------

        daily_pair = last_two_valid(
            daily_ama
        )

        confirm_pair = last_two_valid(
            confirm_ama
        )

        trigger_pair = last_two_valid(
            trigger_ama
        )

        if (
            not daily_pair
            or not confirm_pair
            or not trigger_pair
        ):

            raise RuntimeError(
                "Insufficient AMA history"
            )

        daily_previous, daily_index = (
            daily_pair
        )

        confirm_previous, confirm_index = (
            confirm_pair
        )

        trigger_previous, trigger_index = (
            trigger_pair
        )

        # ----------------------------------------------------
        # 1D — PHARE
        # ----------------------------------------------------

        daily_ama_up = (
            daily_ama[daily_index]
            > daily_ama[daily_previous]
        )

        daily_ama_down = (
            daily_ama[daily_index]
            < daily_ama[daily_previous]
        )

        daily_bull = (
            daily_close[daily_index]
            > float(
                daily_ama[daily_index]
            )
            and daily_ama_up
            and daily_direction[
                daily_index
            ] == "UP"
        )

        daily_bear = (
            daily_close[daily_index]
            < float(
                daily_ama[daily_index]
            )
            and daily_ama_down
            and daily_direction[
                daily_index
            ] == "DOWN"
        )

        # ----------------------------------------------------
        # 4H — CONFIRMATION
        # ----------------------------------------------------

        confirm_ama_up = (
            confirm_ama[confirm_index]
            > confirm_ama[confirm_previous]
        )

        confirm_ama_down = (
            confirm_ama[confirm_index]
            < confirm_ama[confirm_previous]
        )

        confirm_bull = (
            confirm_close[confirm_index]
            > float(
                confirm_ama[confirm_index]
            )
            and confirm_ama_up
            and confirm_direction[
                confirm_index
            ] == "UP"
        )

        confirm_bear = (
            confirm_close[confirm_index]
            < float(
                confirm_ama[confirm_index]
            )
            and confirm_ama_down
            and confirm_direction[
                confirm_index
            ] == "DOWN"
        )

        # ----------------------------------------------------
        # 1H — DÉCLENCHEUR
        #
        # Deux déclencheurs autorisés :
        #
        # 1. nouveau flip SuperTrend
        # 2. reclaim/rebond AMA après retracement
        #
        # Les deux sont calculés uniquement sur bougies
        # clôturées.
        # ----------------------------------------------------

        trigger_flip_long = (
            trigger_direction[
                trigger_previous
            ] == "DOWN"
            and trigger_direction[
                trigger_index
            ] == "UP"
        )

        trigger_flip_short = (
            trigger_direction[
                trigger_previous
            ] == "UP"
            and trigger_direction[
                trigger_index
            ] == "DOWN"
        )

        trigger_ama_rebound_long = (
            trigger_close[
                trigger_previous
            ]
            <= float(
                trigger_ama[
                    trigger_previous
                ]
            )
            and trigger_close[
                trigger_index
            ]
            > float(
                trigger_ama[
                    trigger_index
                ]
            )
            and trigger_low[
                trigger_index
            ]
            <= float(
                trigger_ama[
                    trigger_index
                ]
            )
        )

        trigger_ama_rebound_short = (
            trigger_close[
                trigger_previous
            ]
            >= float(
                trigger_ama[
                    trigger_previous
                ]
            )
            and trigger_close[
                trigger_index
            ]
            < float(
                trigger_ama[
                    trigger_index
                ]
            )
            and trigger_high[
                trigger_index
            ]
            >= float(
                trigger_ama[
                    trigger_index
                ]
            )
        )

        trigger_long = (
            trigger_flip_long
            or trigger_ama_rebound_long
        )

        trigger_short = (
            trigger_flip_short
            or trigger_ama_rebound_short
        )

        # ----------------------------------------------------
        # SIGNAL FINAL
        # ----------------------------------------------------

        signal_direction: Optional[str] = None

        if (
            TREND_SIGNAL_DIRECTIONS
            in {"BOTH", "LONG"}
            and daily_bull
            and confirm_bull
            and trigger_long
        ):

            signal_direction = "LONG"

        elif (
            TREND_SIGNAL_DIRECTIONS
            in {"BOTH", "SHORT"}
            and daily_bear
            and confirm_bear
            and trigger_short
        ):

            signal_direction = "SHORT"

        # ----------------------------------------------------
        # BONUS
        # ----------------------------------------------------

        macd_line, macd_signal, _ = macd(
            trigger_close,
            MACD_FAST_PERIOD,
            MACD_SLOW_PERIOD,
            MACD_SIGNAL_PERIOD,
        )

        bb_middle, bb_upper, bb_lower = (
            bollinger(
                trigger_close,
                BB_PERIOD,
                BB_STDDEV_MULT,
            )
        )

        adx = adx_series(
            trigger_high,
            trigger_low,
            trigger_close,
            ADX_PERIOD,
        )

        ichi = ichimoku(
            trigger_high,
            trigger_low,
            ICHIMOKU_DISPLACEMENT,
            ICHIMOKU_TENKAN_PERIOD,
            ICHIMOKU_KIJUN_PERIOD,
            ICHIMOKU_SENKOU_B_PERIOD,
        )

        latest_trigger_index = (
            len(trigger_close) - 1
        )

        volume_sma = None

        if (
            len(trigger_volume)
            >= BB_VOLUME_SMA_PERIOD
        ):

            volume_sma = (
                sum(
                    trigger_volume[
                        -BB_VOLUME_SMA_PERIOD:
                    ]
                )
                / BB_VOLUME_SMA_PERIOD
            )

        bonus_macd = bool(
            signal_direction
            and macd_line[
                latest_trigger_index
            ] is not None
            and macd_signal[
                latest_trigger_index
            ] is not None
            and (
                (
                    signal_direction == "LONG"
                    and macd_line[
                        latest_trigger_index
                    ]
                    >
                    macd_signal[
                        latest_trigger_index
                    ]
                )
                or
                (
                    signal_direction == "SHORT"
                    and macd_line[
                        latest_trigger_index
                    ]
                    <
                    macd_signal[
                        latest_trigger_index
                    ]
                )
            )
        )

        bonus_bollinger = bool(
            signal_direction
            and bb_middle[
                latest_trigger_index
            ] is not None
            and volume_sma is not None
            and trigger_volume[
                latest_trigger_index
            ]
            >= (
                volume_sma
                * BB_VOLUME_MULT
            )
            and (
                (
                    signal_direction == "LONG"
                    and trigger_close[
                        latest_trigger_index
                    ]
                    >
                    bb_middle[
                        latest_trigger_index
                    ]
                )
                or
                (
                    signal_direction == "SHORT"
                    and trigger_close[
                        latest_trigger_index
                    ]
                    <
                    bb_middle[
                        latest_trigger_index
                    ]
                )
            )
        )

        bonus_ichimoku = bool(
            signal_direction
            and ichi["spanA"][
                latest_trigger_index
            ] is not None
            and ichi["spanB"][
                latest_trigger_index
            ] is not None
            and (
                (
                    signal_direction == "LONG"
                    and trigger_close[
                        latest_trigger_index
                    ]
                    >
                    max(
                        ichi["spanA"][
                            latest_trigger_index
                        ],
                        ichi["spanB"][
                            latest_trigger_index
                        ],
                    )
                )
                or
                (
                    signal_direction == "SHORT"
                    and trigger_close[
                        latest_trigger_index
                    ]
                    <
                    min(
                        ichi["spanA"][
                            latest_trigger_index
                        ],
                        ichi["spanB"][
                            latest_trigger_index
                        ],
                    )
                )
            )
        )

        bonus_score = (
            (
                BONUS_WEIGHT_MACD
                if bonus_macd
                else 0.0
            )
            + (
                BONUS_WEIGHT_BOLLINGER
                if bonus_bollinger
                else 0.0
            )
            + (
                BONUS_WEIGHT_ICHIMOKU
                if bonus_ichimoku
                else 0.0
            )
        )

        # ----------------------------------------------------
        # RISQUE
        # ----------------------------------------------------

        risk = risk_levels(
            entry_price=trigger_close[
                trigger_index
            ],
            direction=signal_direction,
            confirm_supertrend=confirm_supertrend[
                confirm_index
            ],
            confirm_direction=confirm_direction[
                confirm_index
            ],
            trigger_supertrend=trigger_supertrend[
                trigger_index
            ],
            trigger_direction=trigger_direction[
                trigger_index
            ],
        )

        # ----------------------------------------------------
        # RÉSULTAT
        # ----------------------------------------------------

        result.update({

            # ------------------------------
            # 1D
            # ------------------------------

            "dailyClose":
                daily_close[daily_index],

            "dailyAma":
                daily_ama[daily_index],

            "dailySupertrend":
                daily_supertrend[daily_index],

            "dailySupertrendDirection":
                daily_direction[daily_index],

            "dailyAmaUp":
                daily_ama_up,

            "dailyAmaDown":
                daily_ama_down,

            "dailyBull":
                daily_bull,

            "dailyBear":
                daily_bear,

            # ------------------------------
            # 4H
            # ------------------------------

            "confirmClose":
                confirm_close[confirm_index],

            "confirmAma":
                confirm_ama[confirm_index],

            "confirmSupertrend":
                confirm_supertrend[
                    confirm_index
                ],

            "confirmSupertrendDirection":
                confirm_direction[
                    confirm_index
                ],

            "confirmAmaUp":
                confirm_ama_up,

            "confirmAmaDown":
                confirm_ama_down,

            "confirmBull":
                confirm_bull,

            "confirmBear":
                confirm_bear,

            # ------------------------------
            # 1H
            # ------------------------------

            "triggerClose":
                trigger_close[
                    trigger_index
                ],

            "triggerAma":
                trigger_ama[
                    trigger_index
                ],

            "triggerSupertrend":
                trigger_supertrend[
                    trigger_index
                ],

            "triggerSupertrendDirection":
                trigger_direction[
                    trigger_index
                ],

            "triggerFlipLong":
                trigger_flip_long,

            "triggerFlipShort":
                trigger_flip_short,

            "triggerAmaReboundLong":
                trigger_ama_rebound_long,

            "triggerAmaReboundShort":
                trigger_ama_rebound_short,

            "triggerLong":
                trigger_long,

            "triggerShort":
                trigger_short,

            # ------------------------------
            # Bonus
            # ------------------------------

            "adx1h":
                adx[
                    latest_trigger_index
                ],

            "adxInformational":
                True,

            "adxThreshold":
                ADX_THRESHOLD,

            "macdBonus":
                bonus_macd,

            "bollingerVolumeBonus":
                bonus_bollinger,

            "ichimokuBonus":
                bonus_ichimoku,

            "bonusScore":
                bonus_score,

            # ------------------------------
            # Dates
            # ------------------------------

            "lastClosed1hTime":
                datetime.fromtimestamp(
                    trigger_close_time[
                        trigger_index
                    ] / 1000,
                    tz=timezone.utc,
                ).isoformat(),

            "lastClosed4hTime":
                datetime.fromtimestamp(
                    confirm_close_time[
                        confirm_index
                    ] / 1000,
                    tz=timezone.utc,
                ).isoformat(),

            "lastClosed1dTime":
                datetime.fromtimestamp(
                    daily_close_time[
                        daily_index
                    ] / 1000,
                    tz=timezone.utc,
                ).isoformat(),

            # ------------------------------
            # Risk
            # ------------------------------

            "risk":
                risk,
        })

        return result

    except Exception as exc:

        result["error"] = str(exc)

        return result


# ============================================================
# GESTION DU RISQUE
# ============================================================

def risk_levels(
    entry_price: float,
    direction: Optional[str],
    confirm_supertrend: Optional[float],
    confirm_direction: str,
    trigger_supertrend: Optional[float],
    trigger_direction: str,
) -> Dict[str, Any]:

    """
    CALCUL UNIQUEMENT.

    LONG :
        SL / trailing = SuperTrend sous le prix d'entrée.

    SHORT :
        SL / trailing = SuperTrend au-dessus du prix d'entrée.

    Aucun TP fixe.

    Sortie conceptuelle :
        LONG  -> SuperTrend 4H passe DOWN
        SHORT -> SuperTrend 4H passe UP

    Le scanner ne suit PAS une position.
    """

    if (
        not RISK_ENABLED
        or direction
        not in {"LONG", "SHORT"}
    ):

        return {
            "enabled": False,
            "stopLoss": None,
            "trailingStop": None,
            "takeProfit": None,
            "takeProfitMode": "NONE",
            "supertrendSource": None,
            "monitoring": False,
        }

    if RISK_SUPERTREND_SOURCE == "4h":

        candidates = [
            (
                "4h",
                confirm_supertrend,
                confirm_direction,
            ),
            (
                "1h",
                trigger_supertrend,
                trigger_direction,
            ),
        ]

    else:

        candidates = [
            (
                "1h",
                trigger_supertrend,
                trigger_direction,
            ),
            (
                "4h",
                confirm_supertrend,
                confirm_direction,
            ),
        ]

    for (
        source,
        level,
        st_direction,
    ) in candidates:

        if level is None:
            continue

        if direction == "LONG":

            valid = (
                st_direction == "UP"
                and level < entry_price
            )

            exit_rule = (
                "Sortie lorsque le "
                "SuperTrend 4H passe DOWN"
            )

        else:

            valid = (
                st_direction == "DOWN"
                and level > entry_price
            )

            exit_rule = (
                "Sortie lorsque le "
                "SuperTrend 4H passe UP"
            )

        if valid:

            return {
                "enabled": True,
                "stopLoss": float(level),
                "trailingStop": float(level),
                "takeProfit": None,
                "takeProfitMode": "NONE",
                "supertrendSource": source,
                "exitRule": exit_rule,
                "monitoring": False,
            }

    return {
        "enabled": True,
        "stopLoss": None,
        "trailingStop": None,
        "takeProfit": None,
        "takeProfitMode": "NONE",
        "supertrendSource": None,
        "exitRule":
            "Aucun SuperTrend valide "
            "disponible pour le calcul du SL",
        "monitoring": False,
    }


# ============================================================
# AUDIT MTF
# ============================================================

def build_mtf_audit(
    signals: List[Dict[str, Any]],
) -> Dict[str, int]:

    rows = [
        item
        for item in signals
        if not item.get("error")
    ]

    return {

        "analyzed":
            len(rows),

        "errors":
            sum(
                1
                for item in signals
                if item.get("error")
            ),

        "dailyBull":
            sum(
                1
                for item in rows
                if item.get("dailyBull")
            ),

        "dailyBear":
            sum(
                1
                for item in rows
                if item.get("dailyBear")
            ),

        "dailyBullConfirmBull":
            sum(
                1
                for item in rows
                if (
                    item.get("dailyBull")
                    and item.get("confirmBull")
                )
            ),

        "dailyBearConfirmBear":
            sum(
                1
                for item in rows
                if (
                    item.get("dailyBear")
                    and item.get("confirmBear")
                )
            ),

        "triggerFlipLong":
            sum(
                1
                for item in rows
                if item.get(
                    "triggerFlipLong"
                )
            ),

        "triggerFlipShort":
            sum(
                1
                for item in rows
                if item.get(
                    "triggerFlipShort"
                )
            ),

        "triggerAmaReboundLong":
            sum(
                1
                for item in rows
                if item.get(
                    "triggerAmaReboundLong"
                )
            ),

        "triggerAmaReboundShort":
            sum(
                1
                for item in rows
                if item.get(
                    "triggerAmaReboundShort"
                )
            ),

        "longCandidates":
            sum(
                1
                for item in rows
                if (
                    item.get("dailyBull")
                    and item.get("confirmBull")
                    and item.get("triggerLong")
                )
            ),

        "shortCandidates":
            sum(
                1
                for item in rows
                if (
                    item.get("dailyBear")
                    and item.get("confirmBear")
                    and item.get("triggerShort")
                )
            ),

        "longSignals":
            sum(
                1
                for item in rows
                if item.get(
                    "signalDirection"
                ) == "LONG"
            ),

        "shortSignals":
            sum(
                1
                for item in rows
                if item.get(
                    "signalDirection"
                ) == "SHORT"
            ),
    }


# ============================================================
# FORMATAGE
# ============================================================

def fmt_num(
    value: Any,
    digits: int = 4,
) -> str:

    if value is None:
        return "—"

    try:

        return (
            f"{float(value):,."
            f"{digits}f}"
        )

    except (
        TypeError,
        ValueError,
    ):

        return html.escape(
            str(value)
        )


# ============================================================
# TABLE HTML
# ============================================================

def render_table(
    rows: List[List[str]],
    headers: List[str],
) -> str:

    output = [
        "<table>",
        "<thead>",
        "<tr>",
    ]

    output.extend(
        f"<th>{html.escape(header)}</th>"
        for header in headers
    )

    output.extend([
        "</tr>",
        "</thead>",
        "<tbody>",
    ])

    for row in rows:

        output.append("<tr>")

        output.extend(
            f"<td>{cell}</td>"
            for cell in row
        )

        output.append("</tr>")

    output.extend([
        "</tbody>",
        "</table>",
    ])

    return "".join(output)


# ============================================================
# RAPPORT HTML
# ============================================================

def build_html_report(
    universe: List[Dict[str, Any]],
    universe_stats: Dict[str, Any],
    signals: List[Dict[str, Any]],
    generated_at: datetime,
) -> str:

    audit = build_mtf_audit(
        signals
    )

    signal_rows = [
        item
        for item in signals
        if (
            not item.get("error")
            and item.get(
                "signalDirection"
            )
            in {"LONG", "SHORT"}
        )
    ]

    signal_rows.sort(
        key=lambda item:
            float(
                item.get(
                    "bonusScore",
                    0.0,
                )
            ),
        reverse=True,
    )

    signal_rows = signal_rows[
        :EMAIL_TOP_RESULTS
    ]

    universe_rows = [
        [
            html.escape(
                str(step["name"])
            ),
            str(step["before"]),
            str(step["after"]),
        ]
        for step
        in universe_stats.get(
            "steps",
            [],
        )
    ]

    signal_table_rows = []

    for item in signal_rows:

        risk = (
            item.get("risk")
            or {}
        )

        signal_table_rows.append([

            html.escape(
                str(
                    item.get(
                        "symbol",
                        "",
                    )
                )
            ),

            html.escape(
                str(
                    item.get(
                        "signalDirection",
                        "",
                    )
                )
            ),

            fmt_num(
                item.get(
                    "triggerClose"
                )
            ),

            html.escape(
                str(
                    item.get(
                        "dailySupertrendDirection",
                        "",
                    )
                )
            ),

            html.escape(
                str(
                    item.get(
                        "confirmSupertrendDirection",
                        "",
                    )
                )
            ),

            html.escape(
                str(
                    item.get(
                        "triggerSupertrendDirection",
                        "",
                    )
                )
            ),

            fmt_num(
                risk.get(
                    "stopLoss"
                )
            ),

            html.escape(
                str(
                    risk.get(
                        "supertrendSource"
                    )
                    or "—"
                )
            ),

            "Aucun",

            fmt_num(
                item.get(
                    "bonusScore"
                ),
                1,
            ),
        ])

    if signal_table_rows:

        signal_html = render_table(
            signal_table_rows,
            [
                "Symbol",
                "Direction",
                "Entrée réf.",
                "ST 1D",
                "ST 4H",
                "ST 1H",
                "SL / Trailing",
                "Source",
                "TP",
                "Bonus",
            ],
        )

    else:

        signal_html = (
            "<p>"
            "<strong>"
            "Aucun signal MTF complet."
            "</strong>"
            "</p>"
        )

    audit_rows = [

        [
            "Actifs analysés",
            str(audit["analyzed"]),
        ],

        [
            "Erreurs",
            str(audit["errors"]),
        ],

        [
            "1D haussier",
            str(audit["dailyBull"]),
        ],

        [
            "1D baissier",
            str(audit["dailyBear"]),
        ],

        [
            "1D haussier → 4H haussier",
            str(
                audit[
                    "dailyBullConfirmBull"
                ]
            ),
        ],

        [
            "1D baissier → 4H baissier",
            str(
                audit[
                    "dailyBearConfirmBear"
                ]
            ),
        ],

        [
            "Flip 1H LONG",
            str(
                audit[
                    "triggerFlipLong"
                ]
            ),
        ],

        [
            "Flip 1H SHORT",
            str(
                audit[
                    "triggerFlipShort"
                ]
            ),
        ],

        [
            "Rebond AMA 1H LONG",
            str(
                audit[
                    "triggerAmaReboundLong"
                ]
            ),
        ],

        [
            "Rebond AMA 1H SHORT",
            str(
                audit[
                    "triggerAmaReboundShort"
                ]
            ),
        ],

        [
            "Candidats LONG",
            str(
                audit[
                    "longCandidates"
                ]
            ),
        ],

        [
            "Candidats SHORT",
            str(
                audit[
                    "shortCandidates"
                ]
            ),
        ],

        [
            "Signaux LONG",
            str(
                audit[
                    "longSignals"
                ]
            ),
        ],

        [
            "Signaux SHORT",
            str(
                audit[
                    "shortSignals"
                ]
            ),
        ],
    ]

    return f"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">

<title>
Binance Spot — Trend Following Scanner
</title>

<style>

body {{
    font-family: Arial, sans-serif;
    margin: 24px;
    color: #222;
    line-height: 1.45;
}}

table {{
    border-collapse: collapse;
    width: 100%;
    margin: 12px 0 24px;
    font-size: 13px;
}}

th,
td {{
    border: 1px solid #ddd;
    padding: 7px;
    text-align: left;
}}

th {{
    font-weight: 700;
}}

.note {{
    border: 1px solid #ddd;
    padding: 12px;
    margin: 12px 0;
}}

.small {{
    font-size: 12px;
}}

</style>

</head>

<body>

<h1>
Binance Spot — Trend Following Scanner
</h1>

<p>
<strong>Date :</strong>
{html.escape(generated_at.isoformat())}
</p>

<div class="note">

<strong>Architecture :</strong>
1D → 4H → 1H.

<br>

<strong>SuperTrend :</strong>
logique TradingView
(HL2 + ATR Wilder/RMA +
bandes persistantes +
direction).

<br>

<strong>AMA :</strong>
KAMA 50.

<br>

<strong>1D :</strong>
tendance majeure.

<br>

<strong>4H :</strong>
confirmation et référence
SL / trailing.

<br>

<strong>1H :</strong>
timing d'entrée.

<br>

<strong>Risque :</strong>
SuperTrend 4H par défaut.

<br>

<strong>TP :</strong>
aucun TP fixe.

<br>

<strong>Monitoring :</strong>
aucun suivi de position.

</div>


<h2>
1. Configuration
</h2>

{render_table(
    [
        [
            "1D",
            "KAMA 50",
            "10 / 3",
            "Tendance majeure",
        ],
        [
            "4H",
            "KAMA 50",
            "10 / 3",
            "Confirmation + SL/trailing",
        ],
        [
            "1H",
            "KAMA 50",
            "12 / 3",
            "Timing d'entrée",
        ],
    ],
    [
        "UT",
        "AMA",
        "SuperTrend",
        "Rôle",
    ],
)}


<h2>
2. Univers Binance
</h2>

<p>

Univers final :
<strong>
{universe_stats.get(
    "finalUniverse",
    len(universe),
)}
</strong>
actifs.

</p>

<p>

Quote assets :
{html.escape(
    ", ".join(
        universe_stats.get(
            "quoteAssets",
            [],
        )
    )
)}

<br>

Stablecoins dynamiques :
{html.escape(
    ", ".join(
        universe_stats.get(
            "dynamicStablecoins",
            [],
        )
    )
)}

<br>

Volume quote 24H minimum :
{fmt_num(
    MIN_24H_QUOTE_VOLUME,
    0,
)}
<br>

Spread maximum :
{fmt_num(
    MAX_SPREAD_PERCENT,
    2,
)} %

<br>

Profondeur minimale :
{fmt_num(
    MIN_ORDER_BOOK_DEPTH_QUOTE,
    0,
)}

</p>

{render_table(
    universe_rows,
    [
        "Filtre",
        "Avant",
        "Après",
    ],
)}


<h2>
3. Audit MTF 1D → 4H → 1H
</h2>

{render_table(
    audit_rows,
    [
        "Test",
        "Nombre",
    ],
)}


<h2>
4. Signaux
</h2>

{signal_html}


<h2>
5. Règles du signal
</h2>

<ul>

<li>
<strong>LONG 1D :</strong>
prix au-dessus de l'AMA,
AMA montante,
SuperTrend UP.
</li>

<li>
<strong>SHORT 1D :</strong>
prix sous l'AMA,
AMA descendante,
SuperTrend DOWN.
</li>

<li>
<strong>4H :</strong>
même direction que le 1D,
prix du bon côté de l'AMA,
AMA orientée dans le même sens
et SuperTrend aligné.
</li>

<li>
<strong>1H :</strong>
flip SuperTrend validé
ou reclaim/rebond AMA validé,
sur bougie clôturée.
</li>

</ul>


<h2>
6. Gestion du risque
</h2>

<ul>

<li>
<strong>SL initial :</strong>
SuperTrend 4H par défaut.
</li>

<li>
<strong>Trailing :</strong>
SuperTrend 4H.
</li>

<li>
<strong>TP fixe :</strong>
aucun.
</li>

<li>
<strong>Sortie conceptuelle :</strong>
retournement du SuperTrend 4H
contre la direction du scénario.
</li>

<li>
<strong>Suivi :</strong>
le scanner ne vérifie pas si le
SL ou le trailing a ensuite été touché.
</li>

</ul>


<h2>
7. Indicateurs bonus
</h2>

<p>

MACD, Bollinger + volume
et Ichimoku sont des confirmations
bonus.

Ils ne créent pas à eux seuls
un signal MTF.

ADX 1H reste informatif
et n'est pas une condition
obligatoire.

</p>


<h2>
8. Données
</h2>

<p class="small">

Les calculs utilisent uniquement
des bougies dont l'heure de clôture
est passée.

La correspondance avec TradingView
suppose que les données OHLC utilisées
sont identiques.

</p>

</body>
</html>
"""


# ============================================================
# EMAIL
# ============================================================

def send_email(
    subject: str,
    html_body: str,
) -> None:

    if (
        not EMAIL_USER
        or not EMAIL_PASS
        or not EMAIL_TO
    ):

        print(
            "Email non envoyé : "
            "secrets EMAIL_USER / "
            "EMAIL_PASS / EMAIL_TO absents."
        )

        return

    message = MIMEMultipart(
        "alternative"
    )

    message["Subject"] = subject
    message["From"] = EMAIL_USER
    message["To"] = EMAIL_TO

    message.attach(
        MIMEText(
            html_body,
            "html",
            "utf-8",
        )
    )

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(
        EMAIL_HOST,
        EMAIL_PORT,
        context=context,
        timeout=30,
    ) as server:

        server.login(
            EMAIL_USER,
            EMAIL_PASS,
        )

        recipients = [
            item.strip()
            for item
            in EMAIL_TO.split(",")
            if item.strip()
        ]

        server.sendmail(
            EMAIL_USER,
            recipients,
            message.as_string(),
        )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    started = datetime.now(
        timezone.utc
    )

    print(
        "Scanner start:",
        started.isoformat(),
    )

    # --------------------------------------------------------
    # UNIVERS
    # --------------------------------------------------------

    universe, universe_stats = (
        build_universe()
    )

    print(
        "Universe final:",
        len(universe),
    )

    # --------------------------------------------------------
    # CALCUL MTF
    # --------------------------------------------------------

    signals: List[
        Dict[str, Any]
    ] = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=8
    ) as executor:

        futures = [
            executor.submit(
                compute_mtf_signal,
                str(
                    item["symbol"]
                ).upper(),
            )
            for item in universe
        ]

        for future in concurrent.futures.as_completed(
            futures
        ):

            try:

                signals.append(
                    future.result()
                )

            except Exception as exc:

                signals.append({
                    "symbol": "?",
                    "error": str(exc),
                })

    # --------------------------------------------------------
    # TRI
    # --------------------------------------------------------

    signals.sort(
        key=lambda item: (
            0
            if item.get(
                "signalDirection"
            )
            in {"LONG", "SHORT"}
            else 1,

            -float(
                item.get(
                    "bonusScore",
                    0.0,
                )
            ),

            str(
                item.get(
                    "symbol",
                    "",
                )
            ),
        )
    )

    # --------------------------------------------------------
    # AUDIT
    # --------------------------------------------------------

    audit = build_mtf_audit(
        signals
    )

    print(
        "MTF audit:"
    )

    print(
        json.dumps(
            audit,
            ensure_ascii=False,
            indent=2,
        )
    )

    # --------------------------------------------------------
    # RAPPORT
    # --------------------------------------------------------

    generated_at = datetime.now(
        timezone.utc
    )

    report = build_html_report(
        universe,
        universe_stats,
        signals,
        generated_at,
    )

    subject = (
        "Binance Trend Following — "
        f"{audit['longSignals']} LONG / "
        f"{audit['shortSignals']} SHORT — "
        f"{generated_at.strftime('%Y-%m-%d %H:%M UTC')}"
    )

    send_email(
        subject,
        report,
    )

    print(
        "Signals:",
        audit["longSignals"],
        "LONG /",
        audit["shortSignals"],
        "SHORT",
    )

    print(
        "Finished:",
        datetime.now(
            timezone.utc
        ).isoformat(),
    )


if __name__ == "__main__":

    main()
