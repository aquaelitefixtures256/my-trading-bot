# features/tech_features.py
"""
Robust technical indicators for Notex5.

Exports:
 - add_technical_indicators(df) -> DataFrame
 - technical_signal_score(df) -> float

Behavior:
 - Uses 'ta' library if present (preferred) but falls back to pure-pandas implementations.
 - Defensive: coerces numeric types, fills small NaNs safely, handles short series.
 - Does not mutate the caller's DataFrame.
"""
from __future__ import annotations
import logging
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)

# Try to import 'ta' (bukosabino/ta). If missing, we'll fall back to pandas.
try:
    from ta.trend import SMAIndicator, EMAIndicator, MACD
    from ta.momentum import RSIIndicator
    from ta.volatility import AverageTrueRange, BollingerBands
    from ta.volume import OnBalanceVolumeIndicator
    TA_AVAILABLE = True
    logger.info("ta library detected: using 'ta' for indicators")
except Exception:
    TA_AVAILABLE = False
    logger.info("ta library not available: falling back to pandas implementations")


def _ensure_df(df: pd.DataFrame) -> pd.DataFrame:
    """Return a safe copy of df with required columns as numeric and a datetime index if possible."""
    if df is None:
        return pd.DataFrame()
    df = df.copy()
    # ensure index is datetime if possible (many sources give timestamps)
    try:
        df.index = pd.to_datetime(df.index)
    except Exception:
        # leave index as-is if conversion fails
        pass
    # Ensure OHLCV columns exist and are numeric
    for col in ("open", "high", "low", "close", "volume"):
        if col not in df.columns:
            df[col] = 0.0
        # coerce to numeric; replace non-numeric with NaN then fill with 0 for OHLCV safety
        df[col] = pd.to_numeric(df[col], errors="coerce")
    # If some columns became all-NaN, fill with zeros to avoid downstream errors
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].fillna(method=None).fillna(0.0)
    return df


def add_technical_indicators(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Add indicator columns and return a new DataFrame.
    Columns added: sma5, sma20, ema50, macd, rsi14, bb_upper, bb_lower, atr14, obv
    """
    df = _ensure_df(df)
    if df.empty:
        return df

    try:
        if TA_AVAILABLE:
            # Using ta library implementations (more correct formulas)
            df["sma5"] = SMAIndicator(close=df["close"], window=5).sma_indicator()
            df["sma20"] = SMAIndicator(close=df["close"], window=20).sma_indicator()
            df["ema50"] = EMAIndicator(close=df["close"], window=50).ema_indicator()

            macd = MACD(close=df["close"], window_slow=26, window_fast=12, window_sign=9)
            df["macd"] = macd.macd()

            df["rsi14"] = RSIIndicator(close=df["close"], window=14).rsi()

            bb = BollingerBands(close=df["close"], window=20, window_dev=2)
            df["bb_upper"] = bb.bollinger_hband()
            df["bb_lower"] = bb.bollinger_lband()

            atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=14)
            df["atr14"] = atr.average_true_range()

            # OBV - may fail if volume shape unexpected, catch errors
            try:
                obv = OnBalanceVolumeIndicator(close=df["close"], volume=df["volume"])
                df["obv"] = obv.on_balance_volume()
            except Exception:
                df["obv"] = 0.0
        else:
            # pandas fallback implementations (stable & simple)
            df["sma5"] = df["close"].rolling(window=5, min_periods=1).mean()
            df["sma20"] = df["close"].rolling(window=20, min_periods=1).mean()
            df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()

            # MACD fallback = EMA12 - EMA26
            ema12 = df["close"].ewm(span=12, adjust=False).mean()
            ema26 = df["close"].ewm(span=26, adjust=False).mean()
            df["macd"] = ema12 - ema26

            # RSI fallback (Wilder-style approximation)
            delta = df["close"].diff().fillna(0.0)
            up = delta.clip(lower=0.0)
            down = -delta.clip(upper=0.0)
            avg_gain = up.rolling(window=14, min_periods=1).mean()
            avg_loss = down.rolling(window=14, min_periods=1).mean().replace(0, 1e-9)
            rs = avg_gain / avg_loss
            df["rsi14"] = 100 - (100 / (1 + rs))

            # Bollinger bands fallback
            ma20 = df["close"].rolling(window=20, min_periods=1).mean()
            std20 = df["close"].rolling(window=20, min_periods=1).std().fillna(0.0)
            df["bb_upper"] = ma20 + (std20 * 2.0)
            df["bb_lower"] = ma20 - (std20 * 2.0)

            # ATR fallback
            tr1 = (df["high"] - df["low"]).abs()
            tr2 = (df["high"] - df["close"].shift()).abs()
            tr3 = (df["low"] - df["close"].shift()).abs()
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            df["atr14"] = tr.rolling(window=14, min_periods=1).mean()

            # OBV fallback: cumulative volume with sign of price change
            sign = df["close"].diff().fillna(0.0).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
            df["obv"] = (df["volume"] * sign).cumsum().fillna(0.0)

        # Post-process: fill remaining NaNs safely (bfill then ffill then zeros)
        for col in ("sma5", "sma20", "ema50", "macd", "rsi14", "bb_upper", "bb_lower", "atr14", "obv"):
            if col in df.columns:
                # bfill then ffill then fillna(0.0) — avoids deprecated fillna(method=...)
                df[col] = df[col].bfill().ffill().fillna(0.0)
    except Exception:
        logger.exception("add_technical_indicators: unexpected error - returning best-effort df")
        # Return DataFrame that may contain partial columns — don't raise to caller
        return df

    return df


def technical_signal_score(df: Optional[pd.DataFrame]) -> float:
    """
    Heuristic score in [-1,1] combining:
     - sma5/sma20 crossover (strong weight)
     - macd sign (small weight)
     - rsi extremes (small weight)
    Designed to be conservative and stable on short series.
    """
    try:
        if df is None:
            return 0.0
        # require at least two rows to compute deltas
        if len(df) < 2:
            return 0.0

        latest = df.iloc[-1]
        prev = df.iloc[-2]

        score = 0.0

        # SMA crossover (dominant)
        sma5_prev = float(prev.get("sma5", 0.0) or 0.0)
        sma20_prev = float(prev.get("sma20", 0.0) or 0.0)
        sma5_latest = float(latest.get("sma5", 0.0) or 0.0)
        sma20_latest = float(latest.get("sma20", 0.0) or 0.0)

        if sma5_prev <= sma20_prev and sma5_latest > sma20_latest:
            score += 0.6
        if sma5_prev >= sma20_prev and sma5_latest < sma20_latest:
            score -= 0.6

        # MACD direction (small)
        macd_val = float(latest.get("macd", 0.0) or 0.0)
        if macd_val > 0:
            score += 0.12
        elif macd_val < 0:
            score -= 0.12

        # RSI zones
        r = float(latest.get("rsi14", 50.0) or 50.0)
        if r < 30:
            score += 0.25
        elif r > 70:
            score -= 0.25

        # clamp
        if score > 1.0:
            score = 1.0
        if score < -1.0:
            score = -1.0
        return float(score)
    except Exception:
        logger.exception("technical_signal_score failed")
        return 0.0
