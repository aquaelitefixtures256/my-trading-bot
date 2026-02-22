# paste START — features/tech_features.py
"""
Robust, pandas-first technical indicators used by the bot.
If the `ta` library is available, it will be used for more
accurate indicator implementations; otherwise a pandas-based
fallback is used.

Provides:
 - add_technical_indicators(df) -> df with sma5, sma20, rsi14, atr14
 - technical_signal_score(df) -> float score in [-1.0, 1.0]
"""

from __future__ import annotations
import logging
import pandas as pd

logger = logging.getLogger(__name__)

# Try to import 'ta' optionally; fall back to pandas-only if missing
try:
    from ta.trend import SMAIndicator
    from ta.momentum import RSIIndicator
    from ta.volatility import AverageTrueRange
    TA_AVAILABLE = True
    logger.info("ta library available: using ta-based indicators")
except Exception:
    TA_AVAILABLE = False
    logger.info("ta library not available: using pandas fallbacks")


def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy of df with sma5, sma20, rsi14, atr14 added.
    Defensive: uses pandas fallback when ta is unavailable and avoids long NaN runs.
    """
    df = df.copy()
    # require basic columns
    if "close" not in df or "high" not in df or "low" not in df:
        return df

    try:
        if TA_AVAILABLE:
            df["sma5"] = SMAIndicator(close=df["close"], window=5).sma_indicator()
            df["sma20"] = SMAIndicator(close=df["close"], window=20).sma_indicator()
            df["rsi14"] = RSIIndicator(close=df["close"], window=14).rsi()
            atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=14)
            df["atr14"] = atr.average_true_range()
        else:
            df["sma5"] = df["close"].rolling(window=5, min_periods=1).mean()
            df["sma20"] = df["close"].rolling(window=20, min_periods=1).mean()

            # RSI-like fallback (more stable on short series)
            up = df["close"].diff().clip(lower=0)
            down = -df["close"].diff().clip(upper=0)
            avg_gain = up.rolling(window=14, min_periods=1).mean()
            avg_loss = down.rolling(window=14, min_periods=1).mean()
            rs = avg_gain / (avg_loss.replace(0, 1e-8))
            df["rsi14"] = 100 - (100 / (1 + rs))

            # ATR fallback: true range -> rolling mean
            tr1 = (df["high"] - df["low"]).abs()
            tr2 = (df["high"] - df["close"].shift()).abs()
            tr3 = (df["low"] - df["close"].shift()).abs()
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            df["atr14"] = tr.rolling(window=14, min_periods=1).mean()

        # Backfill any early NaNs so short series still produce numbers
        df["sma5"] = df["sma5"].bfill()
        df["sma20"] = df["sma20"].bfill()
        df["rsi14"] = df["rsi14"].bfill()
        df["atr14"] = df["atr14"].bfill()

    except Exception:
        logger.exception("add_technical_indicators failed — returning original df copy")
        return df

    return df


def technical_signal_score(df: pd.DataFrame) -> float:
    """
    Defensive scoring function returning a float in [-1.0, 1.0].
    Uses SMA crossover and RSI thresholds.
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

        if sma5_prev <= sma20_prev and sma5_latest > sma20_latest:
            score += 0.6
        if sma5_prev >= sma20_prev and sma5_latest < sma20_latest:
            score -= 0.6

        r = float(latest.get("rsi14", 50.0) or 50.0)
        if r < 30:
            score += 0.2
        elif r > 70:
            score -= 0.2

        return max(-1.0, min(1.0, score))
    except Exception:
        logger.exception("technical_signal_score failed")
        return 0.0
# paste END
