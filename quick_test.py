# quick_test.py
import pandas as pd
from features.tech_features import add_technical_indicators, technical_signal_score

# create a tiny OHLCV sample (60 rows)
rng = pd.date_range(end=pd.Timestamp.utcnow(), periods=60, freq="H")
import numpy as np
price = 100 + np.cumsum(np.random.randn(len(rng))*0.5)
df = pd.DataFrame({
    "open": price,
    "high": price + np.abs(np.random.rand(len(rng))*0.5),
    "low": price - np.abs(np.random.rand(len(rng))*0.5),
    "close": price,
    "volume": np.random.randint(1,1000,size=len(rng))
}, index=rng)

df2 = add_technical_indicators(df)
print(df2[["sma5","sma20","rsi14","atr14"]].tail())
print("score=", technical_signal_score(df2))
