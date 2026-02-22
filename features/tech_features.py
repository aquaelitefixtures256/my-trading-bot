# features/tech_features.py
"""
Technical indicators module using the 'ta' library when available,
with robust pandas fallbacks when 'ta' is not installed.

Exports:
 - add_technical_indicators(df) -> DataFrame (adds sma5,sma20,ema50,macd,rsi14,bb_upper,bb_lower,atr14,obv)
 - technical_signal_score(df) -> float in [-1.0, 1.0]
"""

from __future__ import annotations
import logging
import pandas as pd
from typing import Any

logger = logging.getLogger(__name__)

# Try to import the 'ta' library classes (bukosabino/ta)
try:
    from ta.trend import SMAIndicator, EMAIndicator, MACD
    from ta.momentum import RSIIndicator
    from ta.volatility import AverageTrueRange, BollingerBands
    from ta.volume import OnBalanceVolumeIndicator
    TA_AVAILABLE = True
    logger.info("ta library detected: using ta-based indicators")
except Exception:
    TA_AVAILABLE = False
    logger.info("ta library not available: falling back to pandas implementations")


def _ensure_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Make sure OHLCV columns exist and are numeric."""
    df = df.copy()
    # Ensure required columns exist
    for col in ("open", "high", "low", "close", "volume"):
        if col not in df.columns:
            df[col] = 0.0
    # Coerce to numeric to avoid strange dtype issues
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds indicator columns to a copy of df:
      - sma5, sma20, ema50
      - macd (MACD line)
      - rsi14
      - bb_upper, bb_lower
      - atr14
      - obv
    Defensive: uses ta when available, otherwise pandas fallbacks.
    """
    df = _ensure_numeric_columns(df)

    # Nothing to do for empty frames
    if df.shape[0] == 0:
        return df

    try:
        if TA_AVAILABLE:
            # use ta classes (proper formulas)
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

            # on-balance volume (needs volume)
            try:
                obv = OnBalanceVolumeIndicator(close=df["close"], volume=df["volume"])
                df["obv"] = obv.on_balance_volume()
            except Exception:
                df["obv"] = 0.0

        else:
            # pandas-only fallbacks (stable and simple)
            df["sma5"] = df["close"].rolling(window=5, min_periods=1).mean()
            df["sma20"] = df["close"].rolling(window=20, min_periods=1).mean()
            df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()

            # Simple MACD fallback: ema12 - ema26
            ema12 = df["close"].ewm(span=12, adjust=False).mean()
            ema26 = df["close"].ewm(span=26, adjust=False).mean()
            df["macd"] = ema12 - ema26

            # RSI fallback (Wilder-like via rolling mean of gains/losses)
            delta = df["close"].diff()
            up = delta.clip(lower=0.0)
            down = -delta.clip(upper=0.0)
            avg_gain = up.rolling(window=14, min_periods=1).mean()
            avg_loss = down.rolling(window=14, min_periods=1).mean()
            rs = avg_gain / (avg_loss.replace(0, 1e-8))
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

            # OBV fallback - simple cumulative volume for rising closes minus falling closes
            obv = (df["volume"] * (df["close"].diff().fillna(0).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))))
            df["obv"] = obv.cumsum()

        # Clean early NaNs to make short series stable
        for col in ("sma5", "sma20", "ema50", "macd", "rsi14", "bb_upper", "bb_lower", "atr14", "obv"):
            if col in df.columns:
                # use bfill then ffill as safety
                df[col] = df[col].bfill().ffill().fillna(0.0)

    except Exception:
        logger.exception("add_technical_indicators: unexpected error - returning best-effort df")
        # on error return df (may have partial columns)
        return df

    return df


def technical_signal_score(df: pd.DataFrame) -> float:
    """
    Compute a small heuristic score using SMA crossover, MACD sign, and RSI.
    Output in [-1.0, 1.0].
    """
    try:
        if df is None or len(df) < 2:
            return 0.0

        latest = df.iloc[-1]
        prev = df.iloc[-2]
        score = 0.0

        sma5_prev = float(prev.get("sma5", 0.0) or 0.0)
        sma20_prev = float(prev.get("sma20", 0.0) or 0.0)
        sma5_latest = float(latest.get("sma5", 0.0) or 0.0)
        sma20_latest = float(latest.get("sma20", 0.0) or 0.0)

        # SMA crossover weight
        if sma5_prev <= sma20_prev and sma5_latest > sma20_latest:
            score += 0.6
        if sma5_prev >= sma20_prev and sma5_latest < sma20_latest:
            score -= 0.6

        # MACD direction
        macd_val = float(latest.get("macd", 0.0) or 0.0)
        if macd_val > 0:
            score += 0.15
        elif macd_val < 0:
            score -= 0.15

        # RSI bands
        r = float(latest.get("rsi14", 50.0) or 50.0)
        if r < 30:
            score += 0.25
        elif r > 70:
            score -= 0.25

        # clamp
        return max(-1.0, min(1.0, score))

    except Exception:
        logger.exception("technical_signal_score failed")
        return 0.0
