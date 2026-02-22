# features/tech_features.py
"""
Defensive technical indicators module.
Normalizes odd input shapes and produces stable indicator columns.
"""
from __future__ import annotations
import logging
from typing import Optional
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

try:
    from ta.trend import SMAIndicator, EMAIndicator, MACD
    from ta.momentum import RSIIndicator
    from ta.volatility import AverageTrueRange, BollingerBands
    from ta.volume import OnBalanceVolumeIndicator
    TA_AVAILABLE = True
    logger.info("ta library detected")
except Exception:
    TA_AVAILABLE = False
    logger.info("ta library not available — using pandas fallbacks")


def _to_dataframe_safe(obj) -> pd.DataFrame:
    if obj is None:
        return pd.DataFrame()
    if isinstance(obj, pd.DataFrame):
        return obj.copy()
    try:
        if isinstance(obj, pd.Series):
            return obj.to_frame()
        return pd.DataFrame(obj)
    except Exception:
        # last-resort: wrap scalar
        try:
            return pd.DataFrame([obj])
        except Exception:
            return pd.DataFrame()


def _flatten_cell(x):
    try:
        if x is None:
            return np.nan
        if isinstance(x, (str, bytes)):
            return x
        if isinstance(x, pd.Series):
            return x.iloc[-1] if len(x) else np.nan
        if isinstance(x, (list, tuple, np.ndarray)):
            return x[-1] if len(x) else np.nan
        # objects with __len__ (but not strings)
        if hasattr(x, "__len__") and not isinstance(x, (str, bytes)):
            try:
                return x[-1]
            except Exception:
                return x
        return x
    except Exception:
        return np.nan


def _normalize_ohlcv(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if df is None:
        return None
    df = _to_dataframe_safe(df)
    if df.empty:
        return None

    # If MultiIndex columns, flatten them
    try:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join(map(str, c)).strip() for c in df.columns]
    except Exception:
        pass

    # ensure string column names and lowercase for matching
    col_map = {}
    cols = list(df.columns)
    cols_str = [str(c) for c in cols]
    for orig, cs in zip(cols, cols_str):
        col_map[orig] = cs.lower()
    try:
        df.rename(columns={k: v for k, v in col_map.items()}, inplace=True)
    except Exception:
        # fallback: create mapping by position
        pass

    # Map possible names to standard names
    for standard in ("open", "high", "low", "close", "volume"):
        if standard not in df.columns:
            # find first column name that contains the substring
            found = None
            for c in df.columns:
                cstr = str(c).lower()
                if standard in cstr:
                    found = c
                    break
            if found is not None:
                df[standard] = df[found]

    # If a column contains nested values, flatten
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            # if dtype object, check some entries
            if df[col].dtype == object:
                try:
                    # apply flatten safely
                    df[col] = df[col].apply(_flatten_cell)
                except Exception:
                    pass

    # Now coerce to numeric with safe fallback
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            except Exception:
                # try elementwise cast
                try:
                    df[col] = df[col].apply(lambda x: float(x) if (not pd.isna(x) and not isinstance(x, (list, tuple, pd.Series, np.ndarray))) else np.nan)
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                except Exception:
                    df[col] = np.nan
        else:
            df[col] = np.nan

    # fill small missing values to avoid catastrophic failures
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].fillna(method="ffill").fillna(method="bfill").fillna(0.0)

    # ensure datetime index
    try:
        df.index = pd.to_datetime(df.index)
    except Exception:
        pass

    return df


def add_technical_indicators(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    df = _to_dataframe_safe(df)
    df = _normalize_ohlcv(df) or pd.DataFrame()
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
            avg_loss = down.rolling(window=14, min_periods=1).mean().replace(0, 1e-9)
            rs = avg_gain / avg_loss
            df["rsi14"] = 100 - (100 / (1 + rs))
            ma20 = df["close"].rolling(window=20, min_periods=1).mean()
            std20 = df["close"].rolling(window=20, min_periods=1).std().fillna(0.0)
            df["bb_upper"] = ma20 + (std20 * 2.0)
            df["bb_lower"] = ma20 - (std20 * 2.0)
            tr1 = (df["high"] - df["low"]).abs()
            tr2 = (df["high"] - df["close"].shift()).abs()
            tr3 = (df["low"] - df["close"].shift()).abs()
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            df["atr14"] = tr.rolling(window=14, min_periods=1).mean()
            sign = df["close"].diff().fillna(0.0).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
            df["obv"] = (df["volume"] * sign).cumsum().fillna(0.0)

        for col in ("sma5", "sma20", "ema50", "macd", "rsi14", "bb_upper", "bb_lower", "atr14", "obv"):
            if col in df.columns:
                df[col] = df[col].bfill().ffill().fillna(0.0)
    except Exception as e:
        logger.exception("add_technical_indicators: unexpected error %s", e)
        return df

    return df


def _safe_get_scalar(row, col, default=0.0):
    try:
        val = row.get(col, default)
    except Exception:
        return float(default)
    try:
        if isinstance(val, pd.Series):
            return float(val.iloc[-1]) if len(val) else float(default)
    except Exception:
        pass
    try:
        if isinstance(val, (list, tuple, np.ndarray)):
            return float(val[-1]) if len(val) else float(default)
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
        latest = df.iloc[-1]
        prev = df.iloc[-2]
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
        if score > 1.0:
            score = 1.0
        if score < -1.0:
            score = -1.0
        return float(score)
    except Exception:
        logger.exception("technical_signal_score failed")
        return 0.0                
