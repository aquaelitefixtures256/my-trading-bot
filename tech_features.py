# features/tech_features.py
"""
Very defensive technical indicators module.
Normalizes weird input shapes (lists, arrays, nested Series) into clean numeric OHLCV columns
before computing indicators. Uses 'ta' when available, otherwise pandas fallbacks.

Exports:
 - add_technical_indicators(df) -> DataFrame
 - technical_signal_score(df) -> float
"""
from __future__ import annotations
import logging
from typing import Optional
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# try ta (optional)
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


def _to_dataframe_safe(obj) -> pd.DataFrame:
    """
    Ensure we return a pandas DataFrame.
    If obj is already a DataFrame, return a copy.
    If obj is list/ndarray/dict/Series-like, attempt conversion.
    If conversion fails, return empty DataFrame.
    """
    if obj is None:
        return pd.DataFrame()
    if isinstance(obj, pd.DataFrame):
        return obj.copy()
    try:
        # If it's a pandas Series, convert to DataFrame
        if isinstance(obj, pd.Series):
            return obj.to_frame()
        # For list / ndarray / dict -> DataFrame
        return pd.DataFrame(obj)
    except Exception:
        try:
            # last resort: wrap scalar into single-row DataFrame
            return pd.DataFrame([obj])
        except Exception:
            return pd.DataFrame()


def _flatten_cell(x):
    """
    If x is array-like (but not a string/bytes), return last element; else return x.
    This is used to normalize rows where a cell contains an array/Series.
    """
    try:
        if x is None:
            return np.nan
        # strings are not array-like for our purpose
        if isinstance(x, (str, bytes)):
            return x
        # pandas Series
        if isinstance(x, pd.Series):
            if len(x) == 0:
                return np.nan
            return x.iloc[-1]
        # numpy array or list or tuple
        if isinstance(x, (list, tuple, np.ndarray)):
            if len(x) == 0:
                return np.nan
            return x[-1]
        # any object with __len__ but not str/bytes - try to get last element
        if hasattr(x, "__len__") and not isinstance(x, (str, bytes)):
            try:
                ln = len(x)
                if ln == 0:
                    return np.nan
                return x[-1]
            except Exception:
                pass
        return x
    except Exception:
        return np.nan


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure df has numeric 'open','high','low','close','volume' columns where each cell is scalar numeric.
    - Handles MultiIndex columns by simplifying to last level
    - Flattens array-like cells (picks last element)
    - Coerces to numeric safely
    """
    if df is None:
        return pd.DataFrame()

    df = df.copy()

    # If columns are a MultiIndex, simplify them to strings (take last level)
    try:
        if isinstance(df.columns, pd.MultiIndex):
            new_cols = []
            for c in df.columns:
                if isinstance(c, tuple):
                    new_cols.append(c[-1])
                else:
                    new_cols.append(c)
            df.columns = [str(c) for c in new_cols]
    except Exception:
        pass

    # Ensure required columns exist (create if missing)
    for col in ("open", "high", "low", "close", "volume"):
        if col not in df.columns:
            df[col] = np.nan

    # Flatten array-like cells per problematic column
    for col in ("open", "high", "low", "close", "volume"):
        try:
            series = df[col]
            # If first non-null element is array-like, we need to flatten the whole column
            first_valid_index = None
            try:
                first_valid_index = series.first_valid_index()
            except Exception:
                first_valid_index = None
            need_flatten = False
            if first_valid_index is not None:
                sample = series.loc[first_valid_index]
                # decide if sample is array-like (but not string/bytes)
                if not isinstance(sample, (str, bytes)) and hasattr(sample, "__len__") and not isinstance(sample, (pd.Series, np.ndarray)) and isinstance(sample, (list, tuple, np.ndarray)):
                    need_flatten = True
                # also check for pandas Series
                if isinstance(sample, pd.Series):
                    need_flatten = True
            # if no valid sample, attempt to detect via dtype object or python objects
            if not need_flatten:
                # object dtype often indicates mixed types — check a few rows
                if series.dtype == object:
                    # sample up to 5 values
                    for v in series.iloc[:5].values:
                        if v is None:
                            continue
                        if isinstance(v, pd.Series) or isinstance(v, (list, tuple, np.ndarray)):
                            need_flatten = True
                            break
            if need_flatten:
                # apply flatten to all cells in column
                df[col] = series.apply(_flatten_cell)
        except Exception:
            # if anything goes wrong, leave column as-is and continue
            try:
                df[col] = df[col]
            except Exception:
                df[col] = np.nan

    # Now coerce to numeric where appropriate; if coercion fails for a column, set NaNs
    for col in ("open", "high", "low", "close", "volume"):
        try:
            # safe conversion: if the column is a pandas Series, convert elementwise after flatten attempt
            if col in df.columns:
                # If column values are not list/tuple/Series (should be flattened by now), do to_numeric
                try:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                except TypeError:
                    # last-resort: convert via applying float on each element
                    df[col] = df[col].apply(lambda x: float(x) if (not pd.isna(x) and not isinstance(x, (list, tuple, np.ndarray, pd.Series))) else np.nan)
                    df[col] = pd.to_numeric(df[col], errors="coerce")
        except Exception:
            df[col] = np.nan

    # Fill remaining NaNs in OHLC with 0.0 to avoid downstream errors (indicators will handle short windows)
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].fillna(0.0)

    # Ensure datetime index if possible
    try:
        df.index = pd.to_datetime(df.index)
    except Exception:
        pass

    return df


def add_technical_indicators(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Add stable indicator columns and return a new DataFrame.
    Defensive: will not raise on odd input shapes.
    """
    df = _to_dataframe_safe(df)
    df = _normalize_columns(df)

    if df.shape[0] == 0:
        return df

    try:
        if TA_AVAILABLE:
            # use ta library where possible
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
            # pandas fallbacks
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

        # safe fill for newly created indicator columns
        for col in ("sma5", "sma20", "ema50", "macd", "rsi14", "bb_upper", "bb_lower", "atr14", "obv"):
            if col in df.columns:
                df[col] = df[col].bfill().ffill().fillna(0.0)

    except Exception as e:
        logger.exception("add_technical_indicators: unexpected error (%s) - returning best-effort df", e)
        return df

    return df


def _safe_get_scalar(row, col, default=0.0):
    """Return a numeric scalar even if the stored value is an array-like or Series."""
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
        if isinstance(val, (list, tuple, np.ndarray)):
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
    """
    Conservative, robust heuristic score in [-1,1].
    """
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

        # clamp
        if score > 1.0: score = 1.0
        if score < -1.0: score = -1.0
        return float(score)
    except Exception:
        logger.exception("technical_signal_score failed")
        return 0.0
