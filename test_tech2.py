# paste into test_tech.py
from features.tech_features import add_technical_indicators, technical_signal_score
import pandas as pd

n = 60
df = pd.DataFrame({
    "open": [1.0 + i for i in range(n)],
    "high": [1.5 + i for i in range(n)],
    "low": [0.5 + i for i in range(n)],
    "close": [1.0 + i for i in range(n)],
    "volume": [100 for _ in range(n)]
})
df2 = add_technical_indicators(df)
print(df2[['sma5','sma20','rsi14','atr14']].tail(5).to_string())
print("SCORE:", technical_signal_score(df2))
