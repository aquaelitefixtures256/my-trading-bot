# features/tech_features.py
"""
Robust technical indicators for Notex5.

Exports:
 - add_technical_indicators(df) -> DataFrame
 - technical_signal_score(df) -> float

This implementation:
 - Uses 'ta' package if available, else pandas fallbacks
 - Is defensive about dtypes and short series
 - Returns floats even when source contains Series/numpy arrays
"""
from __future__ import annotations
import logging
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)

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


def _ensure_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    df = df.copy()
    try:
        df.index = pd.to_datetime(df.index)
    except Exception:
        pass
    for col in ("open","high","low","close","volume"):
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].fillna(0.0)
    return df

def add_technical_indicators(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    df = _ensure_df(df)
    if df.empty:
        return df
    try:
        if TA_AVAILABLE:
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
            try:
                obv = OnBalanceVolumeIndicator(close=df["close"], volume=df["volume"])
                df["obv"] = obv.on_balance_volume()
            except Exception:
                df["obv"] = 0.0
        else:
            df["sma5"] = df["close"].rolling(window=5, min_periods=1).mean()
            df["sma20"] = df["close"].rolling(window=20, min_periods=1).mean()
            df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
            ema12 = df["close"].ewm(span=12, adjust=False).mean()
            ema26 = df["close"].ewm(span=26, adjust=False).mean()
            df["macd"] = ema12 - ema26
            delta = df["close"].diff().fillna(0.0)
            up = delta.clip(lower=0.0)
            down = -delta.clip(upper=0.0)
            avg_gain = up.rolling(window=14, min_periods=1).mean()
            avg_loss = down.rolling(window=14, min_periods=1).mean().replace(0,1e-9)
            rs = avg_gain / avg_loss
            df["rsi14"] = 100 - (100/(1+rs))
            ma20 = df["close"].rolling(window=20, min_periods=1).mean()
            std20 = df["close"].rolling(window=20, min_periods=1).std().fillna(0.0)
            df["bb_upper"] = ma20 + (std20 * 2.0)
            df["bb_lower"] = ma20 - (std20 * 2.0)
            tr1 = (df["high"] - df["low"]).abs()
            tr2 = (df["high"] - df["close"].shift()).abs()
            tr3 = (df["low"] - df["close"].shift()).abs()
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            df["atr14"] = tr.rolling(window=14, min_periods=1).mean()
            sign = df["close"].diff().fillna(0.0).apply(lambda x: 1 if x>0 else (-1 if x<0 else 0))
            df["obv"] = (df["volume"] * sign).cumsum().fillna(0.0)
        for col in ("sma5","sma20","ema50","macd","rsi14","bb_upper","bb_lower","atr14","obv"):
            if col in df.columns:
                df[col] = df[col].bfill().ffill().fillna(0.0)
    except Exception:
        logger.exception("add_technical_indicators: unexpected error - returning best-effort df")
        return df
    return df

def _safe_get_scalar(row, col, default=0.0):
    try:
        val = row.get(col, default)
    except Exception:
        return float(default)
    try:
        if isinstance(val, pd.Series):
            if len(val) == 0:
                return float(default)
            return float(val.iloc[-1])
    except Exception:
        pass
    try:
        if hasattr(val, "__len__") and not isinstance(val, (str, bytes)):
            if len(val) == 0:
                return float(default)
            return float(val[-1])
    except Exception:
        pass
    try:
        return float(val)
    except Exception:
        return float(default)

def technical_signal_score(df: Optional[pd.DataFrame]) -> float:
    try:
        if df is None or len(df) < 2:
            return 0.0
        latest = df.iloc[-1]; prev = df.iloc[-2]
        score = 0.0
        sma5_prev = _safe_get_scalar(prev, "sma5", 0.0)
        sma20_prev = _safe_get_scalar(prev, "sma20", 0.0)
        sma5_latest = _safe_get_scalar(latest, "sma5", 0.0)
        sma20_latest = _safe_get_scalar(latest, "sma20", 0.0)
        if sma5_prev <= sma20_prev and sma5_latest > sma20_latest:
            score += 0.6
        if sma5_prev >= sma20_prev and sma5_latest < sma20_latest:
            score -= 0.6
        macd_val = _safe_get_scalar(latest, "macd", 0.0)
        if macd_val > 0:
            score += 0.12
        elif macd_val < 0:
            score -= 0.12
        r = _safe_get_scalar(latest, "rsi14", 50.0)
        if r < 30:
            score += 0.25
        elif r > 70:
            score -= 0.25
        # clamp
        if score > 1.0: score = 1.0
        if score < -1.0: score = -1.0
        return float(score)
    except Exception:
        logger.exception("technical_signal_score failed")
        return 0.0
