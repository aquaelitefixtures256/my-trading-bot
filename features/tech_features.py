import pandas as pd
import pandas_ta as ta

def add_technical_indicators(df):
    df = df.copy()
    # ensure required columns
    for c in ['open','high','low','close','volume']:
        if c not in df.columns:
            df[c] = df['close'] if 'close' in df.columns else 0
    # strong set of indicators
    try:
        df['sma5'] = ta.sma(df['close'], length=5)
        df['sma20'] = ta.sma(df['close'], length=20)
        df['ema50'] = ta.ema(df['close'], length=50)
        macd = ta.macd(df['close'])
        df['macd'] = macd.get('MACD_12_26_9', 0)
        df['rsi14'] = ta.rsi(df['close'], length=14)
        bb = ta.bbands(df['close'], length=20, std=2)
        df['bb_upper'] = bb.get('BBU_20_2.0', df['close'])
        df['bb_lower'] = bb.get('BBL_20_2.0', df['close'])
        df['atr14'] = ta.atr(df['high'], df['low'], df['close'], length=14)
        df['obv'] = ta.obv(df['close'], df.get('volume', None))
    except Exception:
        # graceful fallback: basic moving averages only
        df['sma5'] = df['close'].rolling(5).mean()
        df['sma20'] = df['close'].rolling(20).mean()
        df['rsi14'] = pd.Series([50]*len(df), index=df.index)
        df['macd'] = 0
        df['atr14'] = (df['high'] - df['low']).rolling(14).mean()
        df['obv'] = 0
    return df

def technical_signal_score(df):
    if df is None or len(df) < 3:
        return 0.0
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    score = 0.0
    try:
        if prev.get('sma5',0) <= prev.get('sma20',0) and latest.get('sma5',0) > latest.get('sma20',0):
            score += 0.6
        if prev.get('sma5',0) >= prev.get('sma20',0) and latest.get('sma5',0) < latest.get('sma20',0):
            score -= 0.6
        if latest.get('macd', 0) > 0:
            score += 0.2
        else:
            score -= 0.2
        r = latest.get('rsi14', 50)
        if r < 30:
            score += 0.2
        elif r > 70:
            score -= 0.2
    except Exception:
        pass
    return max(-1.0, min(1.0, score))
