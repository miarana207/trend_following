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
# BINANCE SPOT — TREND FOLLOWING SCANNER — EXPERIMENTAL V2
#
# OBJECTIF DE L'EXPÉRIENCE :
# mesurer la sensibilité du signal cœur à l'EMA200.
#
# STRATÉGIE RÉELLE CONSERVÉE :
#   Supertrend flip
#   + ADX >= seuil
#   + EMA200 stricte
#
# SIMULATION DIAGNOSTIQUE :
#   EMA200 stricte       : 0 %
#   Tolérance EMA200     : 0.5 %
#   Tolérance EMA200     : 1 %
#   Tolérance EMA200     : 2 %
#   Tolérance EMA200     : 5 %
#
# IMPORTANT :
# Les tolérances ne modifient PAS le signal réel.
# Elles servent uniquement à mesurer combien de candidats
# supplémentaires seraient théoriquement acceptés.
#
# FILTRES DE LIQUIDITÉ EXPÉRIMENTAUX :
#   volume 24h = 0
#   profondeur carnet = 0
#   spread maximum = 100 %
#
# NO ORDERS ARE EVER EXECUTED.
# ============================================================


# ============================================================
# CONFIGURATION GÉNÉRALE
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
    os.getenv(
        "BINANCE_BASE_URL",
        "https://data-api.binance.vision",
    ),
)


# ============================================================
# UNIVERS SPOT
# ============================================================

QUOTE_ASSETS = {
    x.strip().upper()
    for x in os.getenv("QUOTE_ASSETS", "").split(",")
    if x.strip()
}

EXCLUDE_STABLECOINS = (
    os.getenv("EXCLUDE_STABLECOINS", "true").lower() == "true"
)

EXCLUDE_LEVERAGED_TOKENS = (
    os.getenv("EXCLUDE_LEVERAGED_TOKENS", "true").lower() == "true"
)

MIN_24H_QUOTE_VOLUME = float(
    os.getenv("MIN_24H_QUOTE_VOLUME", "0")
)

MAX_SPREAD_PERCENT = float(
    os.getenv("MAX_SPREAD_PERCENT", "100")
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
    os.getenv("MIN_ORDER_BOOK_DEPTH_QUOTE", "0")
)

ORDER_BOOK_DEPTH_WORKERS = int(
    os.getenv("ORDER_BOOK_DEPTH_WORKERS", "8")
)


# ============================================================
# TREND FOLLOWING
# ============================================================

TREND_INTERVAL = os.getenv(
    "TREND_INTERVAL",
    "1h",
)

TREND_SIGNAL_DIRECTIONS = os.getenv(
    "TREND_SIGNAL_DIRECTIONS",
    "BOTH",
)


EMA_TREND_PERIOD = int(
    os.getenv("EMA_TREND_PERIOD", "200")
)

SUPERTREND_ATR_PERIOD = int(
    os.getenv("SUPERTREND_ATR_PERIOD", "10")
)

SUPERTREND_MULTIPLIER = float(
    os.getenv("SUPERTREND_MULTIPLIER", "3.0")
)

ADX_PERIOD = int(
    os.getenv("ADX_PERIOD", "14")
)

ADX_THRESHOLD = float(
    os.getenv("ADX_THRESHOLD", "25")
)


# ============================================================
# EXPÉRIENCE EMA200
# ============================================================

EMA_TOLERANCES = (
    0.0,
    0.5,
    1.0,
    2.0,
    5.0,
)


# ============================================================
# BONUS
# ============================================================

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


TREND_KLINES_LIMIT = int(
    os.getenv("TREND_KLINES_LIMIT", "500")
)

TREND_WORKERS = int(
    os.getenv("TREND_WORKERS", "8")
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
# EXCLUSIONS
# ============================================================

STABLECOIN_BASES = {
    "USDT",
    "USDC",
    "FDUSD",
    "BUSD",
    "DAI",
    "TUSD",
    "USDP",
    "USDE",
    "USDD",
    "FRAX",
    "PYUSD",
    "EURC",
    "USD1",
    "RLUSD",
    "XUSD",
    "EURI",
    "USDG",
    "USDS",
    "U",
    "AEUR",
    "PAXG",
    "USD0",
}

STABLECOIN_QUOTE_MARKERS = (
    "USDT",
    "USDC",
    "FDUSD",
    "TUSD",
    "USDP",
    "USDE",
    "USDD",
    "PYUSD",
    "RLUSD",
    "USD1",
    "USDG",
    "USDS",
    "USD0",
    "XUSD",
    "EURC",
    "EURI",
    "AEUR",
)

LEVERAGED_SUFFIXES = (
    "UP",
    "DOWN",
    "BULL",
    "BEAR",
)

LEVERAGED_PATTERNS = (
    "3L",
    "3S",
    "5L",
    "5S",
    "2L",
    "2S",
)


# ============================================================
# HTTP
# ============================================================

class BinanceHTTPError(RuntimeError):
    pass


def is_geo_restriction_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "http 451" in text
        or ("451" in text and "unavailable" in text)
    )


def http_get_json(
    base_url: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
) -> Any:

    url = base_url.rstrip("/") + path

    if params:
        query = urlencode(
            {
                k: v
                for k, v in params.items()
                if v is not None
            }
        )

        if query:
            url += "?" + query

    last_error: Optional[Exception] = None

    for attempt in range(HTTP_RETRIES + 1):

        try:

            request = Request(
                url,
                headers={
                    "User-Agent":
                        "BinanceTrendFollowingExperimentalV2/1.0"
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

        except HTTPError as exc:

            last_error = exc

            retryable = exc.code in {
                418,
                429,
                500,
                502,
                503,
                504,
            }

            if (
                not retryable
                or attempt >= HTTP_RETRIES
            ):

                try:
                    body = exc.read().decode("utf-8")
                except Exception:
                    body = ""

                raise BinanceHTTPError(
                    f"HTTP {exc.code} {path}: "
                    f"{body[:500]}"
                ) from exc

            retry_after = exc.headers.get(
                "Retry-After"
            )

            try:
                delay = (
                    float(retry_after)
                    if retry_after
                    else min(
                        15.0,
                        2.0 ** attempt,
                    )
                )
            except ValueError:
                delay = min(
                    15.0,
                    2.0 ** attempt,
                )

            time.sleep(delay)

        except (
            URLError,
            TimeoutError,
            json.JSONDecodeError,
        ) as exc:

            last_error = exc

            if attempt >= HTTP_RETRIES:

                raise BinanceHTTPError(
                    f"Request failed {path}: {exc}"
                ) from exc

            time.sleep(
                min(
                    10.0,
                    2.0 ** attempt,
                )
            )

    raise BinanceHTTPError(
        f"Request failed {path}: {last_error}"
    )


# ============================================================
# UTILITAIRES
# ============================================================

def as_float(
    value: Any,
    default: float = 0.0,
) -> float:

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(
    value: Any,
    default: int = 0,
) -> int:

    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_percent_spread(
    bid: float,
    ask: float,
) -> Optional[float]:

    if (
        bid <= 0
        or ask <= 0
        or ask < bid
    ):
        return None

    mid = (bid + ask) / 2.0

    return (
        ((ask - bid) / mid) * 100.0
        if mid > 0
        else None
    )


def is_leveraged_symbol(
    base_asset: str,
) -> bool:

    base = base_asset.upper()

    return any(
        base.endswith(x)
        for x in (
            LEVERAGED_SUFFIXES
            + LEVERAGED_PATTERNS
        )
    )


# ============================================================
# AUDIT FILTRAGE
# ============================================================

class CriterionAudit:

    def __init__(self) -> None:
        self.rows: List[Dict[str, Any]] = []

    def add(
        self,
        name: str,
        before: int,
        selected: int,
    ) -> None:

        rejected = before - selected

        retention = (
            selected / before * 100
            if before
            else 0.0
        )

        rejection = (
            rejected / before * 100
            if before
            else 0.0
        )

        self.rows.append(
            {
                "criterion": name,
                "before": before,
                "selected": selected,
                "rejected": rejected,
                "retention": retention,
                "rejection": rejection,
            }
        )


def apply_criterion(
    assets: List[Dict[str, Any]],
    audit: CriterionAudit,
    name: str,
    predicate: Callable[
        [Dict[str, Any]],
        bool,
    ],
) -> List[Dict[str, Any]]:

    before = len(assets)

    selected = [
        asset
        for asset in assets
        if predicate(asset)
    ]

    audit.add(
        name,
        before,
        len(selected),
    )

    return selected


# ============================================================
# AUDIT SIGNAL
# ============================================================

class TrendSignalAudit:

    def __init__(self) -> None:

        self.total = 0

        self.long_flip = 0
        self.short_flip = 0

        self.long_adx = 0
        self.short_adx = 0

        self.long_ema = 0
        self.short_ema = 0

        self.final_long = 0
        self.final_short = 0

        self.errors = 0

    def consume(
        self,
        asset: Dict[str, Any],
    ) -> None:

        self.total += 1

        direction = asset.get(
            "diagnosticFlipDirection"
        )

        if direction == "LONG":

            self.long_flip += 1

            if asset.get("diagnosticAdxOk"):

                self.long_adx += 1

                if asset.get("diagnosticEmaOk"):
                    self.long_ema += 1

        elif direction == "SHORT":

            self.short_flip += 1

            if asset.get("diagnosticAdxOk"):

                self.short_adx += 1

                if asset.get("diagnosticEmaOk"):
                    self.short_ema += 1

        if asset.get(
            "signalDirection"
        ) == "LONG":

            self.final_long += 1

        elif asset.get(
            "signalDirection"
        ) == "SHORT":

            self.final_short += 1

        if asset.get("signalError"):
            self.errors += 1


# ============================================================
# UNIVERS SPOT
# ============================================================

def spot_exchange_info() -> Dict[str, Any]:

    return http_get_json(
        SPOT_BASE_URL,
        "/api/v3/exchangeInfo",
    )


def spot_tickers() -> List[Dict[str, Any]]:

    return http_get_json(
        SPOT_BASE_URL,
        "/api/v3/ticker/24hr",
    )


def spot_book_tickers() -> List[Dict[str, Any]]:

    return http_get_json(
        SPOT_BASE_URL,
        "/api/v3/ticker/bookTicker",
    )


def discover_active_stablecoin_quotes(
    exchange_info: Dict[str, Any],
) -> set[str]:

    active_quotes = {
        str(
            x.get(
                "quoteAsset",
                "",
            )
        ).upper().strip()

        for x in exchange_info.get(
            "symbols",
            [],
        )

        if (
            isinstance(x, dict)
            and str(
                x.get(
                    "status",
                    "",
                )
            ).upper()
            == "TRADING"
        )
    }

    discovered = {
        q
        for q in active_quotes
        if q in STABLECOIN_BASES
    }

    for quote in active_quotes - discovered:

        if any(
            marker in quote
            for marker
            in STABLECOIN_QUOTE_MARKERS
        ):
            discovered.add(quote)

    return discovered


def resolve_spot_quote_assets(
    exchange_info: Dict[str, Any],
) -> set[str]:

    active_quotes = {
        str(
            x.get(
                "quoteAsset",
                "",
            )
        ).upper().strip()

        for x in exchange_info.get(
            "symbols",
            [],
        )
        if isinstance(x, dict)
    }

    if QUOTE_ASSETS:
        return (
            QUOTE_ASSETS
            & active_quotes
        )

    return discover_active_stablecoin_quotes(
        exchange_info
    )


def build_spot_universe(
    exchange_info: Dict[str, Any],
) -> List[Dict[str, Any]]:

    result: List[Dict[str, Any]] = []

    for x in exchange_info.get(
        "symbols",
        [],
    ):

        if not isinstance(x, dict):
            continue

        result.append(
            {
                "symbol": x.get(
                    "symbol",
                    "",
                ),
                "baseAsset": x.get(
                    "baseAsset",
                    "",
                ),
                "quoteAsset": x.get(
                    "quoteAsset",
                    "",
                ),
                "status": x.get(
                    "status",
                    "",
                ),
                "permissions": x.get(
                    "permissions",
                    [],
                ),
            }
        )

    return result


def merge_spot_tickers(
    assets: List[Dict[str, Any]],
    tickers: List[Dict[str, Any]],
) -> None:

    by_symbol = {
        x.get("symbol"): x
        for x in tickers
        if x.get("symbol")
    }

    for asset in assets:

        ticker = by_symbol.get(
            asset["symbol"],
            {},
        )

        asset.update(
            lastPrice=as_float(
                ticker.get("lastPrice")
            ),
            priceChangePercent=as_float(
                ticker.get(
                    "priceChangePercent"
                )
            ),
            quoteVolume=as_float(
                ticker.get("quoteVolume")
            ),
            trades=as_int(
                ticker.get("count")
            ),
        )


def merge_spot_books(
    assets: List[Dict[str, Any]],
    books: List[Dict[str, Any]],
) -> None:

    by_symbol = {
        x.get("symbol"): x
        for x in books
        if x.get("symbol")
    }

    for asset in assets:

        book = by_symbol.get(
            asset["symbol"],
            {},
        )

        bid = as_float(
            book.get("bidPrice")
        )

        ask = as_float(
            book.get("askPrice")
        )

        asset.update(
            bidPrice=bid,
            askPrice=ask,
            spreadPercent=safe_percent_spread(
                bid,
                ask,
            ),
        )


def spot_order_book(
    symbol: str,
) -> Dict[str, Any]:

    return http_get_json(
        SPOT_BASE_URL,
        "/api/v3/depth",
        {
            "symbol": symbol,
            "limit": ORDER_BOOK_DEPTH_LIMIT,
        },
    )


def order_book_depth_metrics(
    book: Dict[str, Any],
    mid_price: float,
) -> Dict[str, float]:

    if mid_price <= 0:
        return {
            "depthTotalQuote": 0.0
        }

    band = (
        ORDER_BOOK_DEPTH_PCT
        / 100.0
    )

    min_bid = (
        mid_price
        * (1.0 - band)
    )

    max_ask = (
        mid_price
        * (1.0 + band)
    )

    bid_depth = 0.0
    ask_depth = 0.0

    for level in (
        book.get("bids", [])
        or []
    ):

        if (
            not isinstance(
                level,
                (list, tuple),
            )
            or len(level) < 2
        ):
            continue

        price = as_float(level[0])
        quantity = as_float(level[1])

        if (
            price >= min_bid
            and price > 0
            and quantity > 0
        ):
            bid_depth += (
                price * quantity
            )

    for level in (
        book.get("asks", [])
        or []
    ):

        if (
            not isinstance(
                level,
                (list, tuple),
            )
            or len(level) < 2
        ):
            continue

        price = as_float(level[0])
        quantity = as_float(level[1])

        if (
            price <= max_ask
            and price > 0
            and quantity > 0
        ):
            ask_depth += (
                price * quantity
            )

    return {
        "depthTotalQuote":
            bid_depth + ask_depth
    }


def merge_spot_order_book_depth(
    assets: List[Dict[str, Any]],
    warnings: List[str],
) -> None:

    if (
        not ORDER_BOOK_DEPTH_ENABLED
        or not assets
    ):
        return

    workers = max(
        1,
        min(
            ORDER_BOOK_DEPTH_WORKERS,
            len(assets),
        ),
    )

    failures = 0

    def fetch(
        asset: Dict[str, Any],
    ) -> Tuple[
        str,
        Dict[str, Any],
    ]:

        return (
            asset["symbol"],
            spot_order_book(
                asset["symbol"]
            ),
        )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=workers
    ) as executor:

        future_map = {
            executor.submit(
                fetch,
                asset,
            ): asset
            for asset in assets
        }

        for future in (
            concurrent.futures.as_completed(
                future_map
            )
        ):

            asset = future_map[future]

            try:

                _, book = future.result()

                bid = as_float(
                    asset.get("bidPrice")
                )

                ask = as_float(
                    asset.get("askPrice")
                )

                mid = (
                    (bid + ask) / 2.0
                    if (
                        bid > 0
                        and ask > 0
                    )
                    else 0.0
                )

                asset.update(
                    order_book_depth_metrics(
                        book,
                        mid,
                    )
                )

            except Exception as exc:

                failures += 1

                asset.update(
                    depthTotalQuote=0.0,
                    depthError=str(exc),
                )

    if failures:

        warnings.append(
            "Order Book Depth : "
            f"{failures} échecs de récupération "
            f"sur {len(assets)} paires."
        )


# ============================================================
# HISTORIQUE
# ============================================================

def bars_per_day(
    interval: str,
) -> float:

    unit = (
        interval[-1]
        if interval
        else "h"
    )

    try:
        value = int(
            interval[:-1]
        )
    except (
        ValueError,
        IndexError,
    ):
        value = 1

    minutes_per_bar = {
        "m": value,
        "h": value * 60,
        "d": value * 1440,
        "w": value * 10080,
    }.get(
        unit,
        60,
    )

    return (
        1440.0
        / minutes_per_bar
        if minutes_per_bar > 0
        else 24.0
    )


SPOT_KLINES_CACHE: Dict[
    Tuple[str, str, int],
    List[List[Any]],
] = {}


def spot_klines(
    symbol: str,
    interval: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[List[Any]]:

    effective_interval = (
        interval
        or TREND_INTERVAL
    )

    effective_limit = (
        limit
        or TREND_KLINES_LIMIT
    )

    cache_key = (
        symbol,
        effective_interval,
        effective_limit,
    )

    if cache_key in SPOT_KLINES_CACHE:
        return SPOT_KLINES_CACHE[
            cache_key
        ]

    data = http_get_json(
        SPOT_BASE_URL,
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval":
                effective_interval,
            "limit":
                effective_limit,
        },
    )

    if not isinstance(
        data,
        list,
    ):
        raise BinanceHTTPError(
            "Réponse klines invalide "
            f"pour {symbol}"
        )

    SPOT_KLINES_CACHE[
        cache_key
    ] = data

    return data


# ============================================================
# FILTRAGE SPOT
# ============================================================

def screen_spot(
    assets: List[Dict[str, Any]],
    quote_assets: set[str],
    warnings: List[str],
) -> Tuple[
    List[Dict[str, Any]],
    CriterionAudit,
]:

    audit = CriterionAudit()

    assets = apply_criterion(
        assets,
        audit,
        "1. Status TRADING",
        lambda x:
            x["status"] == "TRADING",
    )

    assets = apply_criterion(
        assets,
        audit,
        "2. Permission SPOT",
        lambda x:
            (
                not x["permissions"]
                or "SPOT"
                in x["permissions"]
            ),
    )

    assets = apply_criterion(
        assets,
        audit,
        "3. Quote asset autorisé",
        lambda x:
            x["quoteAsset"]
            in quote_assets,
    )

    if EXCLUDE_STABLECOINS:

        assets = apply_criterion(
            assets,
            audit,
            "4. Exclusion stablecoins",
            lambda x:
                x["baseAsset"].upper()
                not in STABLECOIN_BASES,
        )

    if EXCLUDE_LEVERAGED_TOKENS:

        assets = apply_criterion(
            assets,
            audit,
            "5. Exclusion tokens à levier",
            lambda x:
                not is_leveraged_symbol(
                    x["baseAsset"]
                ),
        )

    assets = apply_criterion(
        assets,
        audit,
        "6. Données 24h disponibles",
        lambda x:
            x["lastPrice"] > 0,
    )

    assets = apply_criterion(
        assets,
        audit,
        "7. Volume quote 24h minimum",
        lambda x:
            x["quoteVolume"]
            >= MIN_24H_QUOTE_VOLUME,
    )

    if ORDER_BOOK_DEPTH_ENABLED:

        merge_spot_order_book_depth(
            assets,
            warnings,
        )

        assets = apply_criterion(
            assets,
            audit,
            "8. Épaisseur carnet d'ordres minimum",
            lambda x:
                x.get(
                    "depthTotalQuote",
                    0.0,
                )
                >= MIN_ORDER_BOOK_DEPTH_QUOTE,
        )

   
