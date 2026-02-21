# features/tech_features.py
import pandas as pd
def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "close" in df:
        df["sma5"] = df["close"].rolling(5).mean()
        df["sma20"] = df["close"].rolling(20).mean()
        df["rsi14"] = (df["close"].diff().fillna(0) > 0).rolling(14).mean() * 100
        df["atr14"] = (df["high"] - df["low"]).rolling(14).mean().fillna(method="bfill")
    return df

def technical_signal_score(df: pd.DataFrame) -> float:
    try:
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        score = 0.0
        if prev.get("sma5", 0) <= prev.get("sma20", 0) and latest.get("sma5", 0) > latest.get("sma20", 0):
            score += 0.6
        r = latest.get("rsi14", 50)
        if r < 30:
            score += 0.2
        elif r > 70:
            score -= 0.2
        return max(-1.0, min(1.0, float(score)))
    except Exception:
        return 0.0
