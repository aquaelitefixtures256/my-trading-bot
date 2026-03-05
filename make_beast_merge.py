#!/usr/bin/env python3
# make_beast_merge.py
# Run: python make_beast_merge.py
import os, traceback
from pathlib import Path

SRC = Path("void2.0.py")
DST = Path("alter_instinct_final_beast.py")   # output file

if not SRC.exists():
    print("ERROR: source file not found:", SRC)
    raise SystemExit(2)

enhanced_block = r'''
# ===== BEGIN BEAST UPGRADE BLOCK (activated by key ALT_BEAST_FUNDAMENTALS_LOCKED_05MAR2026_X9K) =====
import os, math, time, requests, json
from datetime import datetime, timedelta, timezone

# Logger fallback
try:
    logger
except NameError:
    class _SimpleLogger:
        def info(self, *a, **k): print("INFO", *a)
        def warning(self, *a, **k): print("WARN", *a)
        def error(self, *a, **k): print("ERR", *a)
        def debug(self, *a, **k): print("DBG", *a)
    logger = _SimpleLogger()

# Preserve risk env or defaults
BASE_RISK_PER_TRADE_PCT = float(os.getenv("BASE_RISK_PER_TRADE_PCT", "0.003"))
MIN_RISK_PER_TRADE_PCT = float(os.getenv("MIN_RISK_PER_TRADE_PCT", "0.002"))
MAX_RISK_PER_TRADE_PCT = float(os.getenv("MAX_RISK_PER_TRADE_PCT", "0.01"))
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", str(BASE_RISK_PER_TRADE_PCT)))

# Thresholds preserved
BUY_THRESHOLD = float(os.getenv("BUY_THRESHOLD", "0.18"))
SELL_THRESHOLD = float(os.getenv("SELL_THRESHOLD", "-0.18"))

# Keys
NEWSDATA_KEY = os.getenv("NEWSDATA_KEY", "")
MARKETAUX_KEY = os.getenv("MARKETAUX_KEY", "")
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")

# Smoothed sentiment state
_SENT_EMA = None
_SENT_EMA_ALPHA = 0.4

# Attempt to import VADER
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _VADER = SentimentIntensityAnalyzer()
except Exception:
    _VADER = None

# Keywords
_FUND_KEYWORDS = {
    "gold": ["gold","xau","xauusd"],
    "silver": ["silver","xag","xagusd"],
    "oil": ["oil","brent","wti","crude","usoil"],
    "iran": ["iran","tehran","missile","strike","attack","war","sanction"],
    "inflation": ["cpi","inflation","fed","rate","interest"]
}
_SYMBOL_KEYWORD_MAP = {"XAUUSD":["gold","xau"], "XAGUSD":["silver","xag"], "USOIL":["oil","wti","brent"], "BTCUSD":["bitcoin","btc"]}

# Weights
_TECH_WEIGHT = 0.60
_FUND_WEIGHT = 0.25
_SENT_WEIGHT = 0.15

_KEYWORD_HIT_PENALTY = 0.18

def _clamp(x, lo=-1.0, hi=1.0):
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return lo

def fetch_newsdata(q: str, pagesize: int = 30, max_pages: int = 2, recent_hours: int = 72):
    try:
        if 'FUNDAMENTAL_AVAILABLE' in globals() and not FUNDAMENTAL_AVAILABLE:
            return {"count":0,"articles":[]}
    except Exception:
        pass
    key = NEWSDATA_KEY or ""
    if not key:
        return {"count":0,"articles":[]}
    base = "https://newsdata.io/api/1/news"
    finance_boost = " OR ".join(["gold","silver","oil","brent","wti","bitcoin","cpi","inflation","fed"])
    q = (q or "").strip()
    query = f"({q}) OR ({finance_boost})" if q else finance_boost
    out = []
    for page in range(1, max_pages+1):
        params = {"q":query, "language":"en", "page":page, "apikey":key}
        try:
            r = requests.get(base, params=params, timeout=10)
        except Exception as e:
            logger.warning("fetch_newsdata request failed: %s", e)
            break
        if r.status_code != 200:
            logger.warning("fetch_newsdata non-200 %s", r.status_code)
            break
        try:
            j = r.json()
        except Exception:
            break
        items = j.get("results") or j.get("articles") or j.get("data") or []
        if isinstance(items, dict):
            for k in ("results","articles","data"):
                if isinstance(items.get(k), list):
                    items = items.get(k); break
        if not isinstance(items, list) or len(items)==0:
            break
        for a in items:
            try:
                pub = a.get("pubDate") or a.get("publishedAt") or a.get("published_at") or ""
                pd = None
                try:
                    if pub:
                        pd = datetime.fromisoformat(pub.replace("Z","+00:00")).astimezone(timezone.utc)
                except Exception:
                    pd = None
                if pd is not None:
                    delta_h = (datetime.now(timezone.utc)-pd).total_seconds()/3600.0
                    if delta_h > recent_hours:
                        continue
                out.append({"title":a.get("title"), "description":a.get("description") or a.get("summary") or "", "source": a.get("source_id") or (a.get("source") and (a.get("source").get("name") if isinstance(a.get("source"), dict) else a.get("source")) ) or "", "publishedAt":pub, "raw":a})
            except Exception:
                continue
        if len(items)<1:
            break
    if out:
        return {"count":len(out),"articles":out}
    # MarketAux fallback
    if MARKETAUX_KEY:
        try:
            url = "https://api.marketaux.com/v1/news/all"
            params = {"api_token":MARKETAUX_KEY, "q": q or "", "language":"en", "limit":pagesize}
            r = requests.get(url, params=params, timeout=8)
            if r.status_code==200:
                j = r.json()
                items = j.get("data") or j.get("results") or j.get("articles") or []
                processed = []
                for a in items[:pagesize]:
                    processed.append({"title":a.get("title"), "description":a.get("description"), "source": a.get("source_name") or a.get("source"), "publishedAt": a.get("published_at") or a.get("publishedAt"), "raw":a})
                if processed:
                    return {"count":len(processed),"articles":processed}
        except Exception:
            logger.exception("marketaux fallback failed")
    return {"count":0,"articles":[]}

def _simple_keyword_sentiment(text: str):
    txt = (text or "").lower()
    positive = ("gain","rise","surge","up","positive","beat","better","strong","rally","outperform")
    negative = ("drop","fall","down","loss","negative","miss","weaker","selloff","crash","attack","strike","war","sanction")
    p = sum(txt.count(w) for w in positive)
    n = sum(txt.count(w) for w in negative)
    denom = max(1.0, len(txt.split()))
    return max(-1.0, min(1.0, (p-n)/denom))

def _update_sentiment_ema(raw_sent):
    global _SENT_EMA, _SENT_EMA_ALPHA
    try:
        if _SENT_EMA is None:
            _SENT_EMA = float(raw_sent)
        else:
            _SENT_EMA = (_SENT_EMA_ALPHA * float(raw_sent)) + ((1.0 - _SENT_EMA_ALPHA) * _SENT_EMA)
    except Exception:
        _SENT_EMA = float(raw_sent or 0.0)
    return float(_SENT_EMA or 0.0)

def fetch_fundamental_score(symbol: str, lookback_days: int=2, recent_hours: int=72):
    s = (symbol or "").upper()
    details = {"news_count":0, "news_hits":0, "matched_keywords":{}, "articles_sample": []}
    news_sent = 0.0
    cal_signal = 0.0
    query_parts = []
    if s.startswith("XAU") or "GOLD" in s:
        query_parts += _FUND_KEYWORDS.get("gold", [])
    elif s.startswith("XAG") or "SILVER" in s:
        query_parts += _FUND_KEYWORDS.get("silver", [])
    elif s.startswith("BTC"):
        query_parts += _FUND_KEYWORDS.get("bitcoin", [])
    elif s in ("USOIL","OIL","WTI","BRENT"):
        query_parts += _FUND_KEYWORDS.get("oil", [])
    else:
        query_parts.append(s)
    query_parts += ["inflation","cpi","fed","interest rate","oil","gold","stock","earnings"]
    q = " OR ".join(set([p for p in query_parts if p]))
    try:
        news = fetch_newsdata(q, pagesize=30, max_pages=2, recent_hours=recent_hours)
        articles = news.get("articles", []) if isinstance(news, dict) else []
        details["news_count"] = len(articles)
        if articles:
            scores=[]
            matched={}
            for a in articles:
                title = (a.get("title") or "") or ""
                desc = (a.get("description") or "") or ""
                txt = (title+" "+desc).strip()
                hits=0
                for kw_group, kw_list in _FUND_KEYWORDS.items():
                    for kw in kw_list:
                        if kw in txt.lower():
                            hits+=1
                            matched[kw_group]=matched.get(kw_group,0)+1
                try:
                    if _VADER is not None:
                        sscore = _VADER.polarity_scores(txt).get("compound",0.0)
                    else:
                        sscore = _simple_keyword_sentiment(txt)
                except Exception:
                    sscore = _simple_keyword_sentiment(txt)
                scores.append(float(sscore))
                if len(details["articles_sample"])<4:
                    details["articles_sample"].append({"title":title,"source":a.get("source"),"publishedAt":a.get("publishedAt"),"score":sscore})
                details["news_hits"] = details.get("news_hits",0)+hits
            avg_sent = float(sum(scores)/max(1,len(scores)))
            if details.get("news_hits",0) >=2:
                avg_sent = avg_sent - (_KEYWORD_HIT_PENALTY * min(3, details["news_hits"]))
            news_sent = max(-1.0, min(1.0, avg_sent))
            details["matched_keywords"] = matched
        else:
            news_sent = 0.0
    except Exception:
        logger.exception("fetch_fundamental_score news step failed")
        news_sent = 0.0
    try:
        if 'should_pause_for_events' in globals():
            pause, ev = should_pause_for_events(symbol, 60)
            if pause:
                cal_signal = -1.0
                details["calendar_event"] = ev
            else:
                cal_signal = 0.0
    except Exception:
        cal_signal = 0.0
    symbol_boost = 0.0
    try:
        for sym, keys in _SYMBOL_KEYWORD_MAP.items():
            if sym == s:
                for k in keys:
                    if k in (details.get("matched_keywords") or {}):
                        symbol_boost += 0.08
    except Exception:
        symbol_boost = 0.0
    smoothed = _update_sentiment_ema(news_sent)
    fund_component = (0.7 * news_sent) + (0.3 * cal_signal) + symbol_boost
    fund_component = max(-1.0, min(1.0, fund_component))
    details["news_sentiment"]=news_sent
    details["smoothed_sentiment"]=smoothed
    details["symbol_boost"]=symbol_boost
    details["fund_component"]=fund_component
    return {"combined":float(fund_component), "news_sentiment":float(news_sent), "calendar_signal":float(cal_signal), "details":details}

def compute_combined_score(tech_score, model_score, fundamental_score, sentiment_score):
    try:
        tech = float(tech_score or 0.0)
        mod = float(model_score or 0.0)
        fund = float(fundamental_score or 0.0)
        sent = float(sentiment_score or 0.0)
    except Exception:
        tech, mod, fund, sent = 0.0,0.0,0.0,0.0
    combined = (_TECH_WEIGHT * tech) + (0.25 * mod) + (_FUND_WEIGHT * fund) + (_SENT_WEIGHT * sent)
    return max(-1.0, min(1.0, combined))

def compute_position_risk(base_risk_pct, tech_score, fund_score, sent_score):
    try:
        base = float(base_risk_pct)
    except Exception:
        base = BASE_RISK_PER_TRADE_PCT
    s_tech = math.copysign(1, tech_score) if abs(tech_score) >= 0.01 else 0
    s_fund = math.copysign(1, fund_score) if abs(fund_score) >= 0.01 else 0
    s_sent = math.copysign(1, sent_score) if abs(sent_score) >= 0.01 else 0
    multiplier = 1.0
    if s_tech != 0 and s_tech == s_fund == s_sent:
        multiplier = 1.2
    elif s_tech !=0 and s_tech == s_fund:
        multiplier = 1.1
    elif s_tech !=0 and s_tech == s_sent:
        multiplier = 1.05
    elif s_fund !=0 and s_tech !=0 and s_tech != s_fund:
        multiplier = 0.5
    else:
        multiplier = 1.0
    risk = base * multiplier
    risk = max(MIN_RISK_PER_TRADE_PCT, min(MAX_RISK_PER_TRADE_PCT, risk))
    return float(risk), multiplier

def make_decision_for_symbol(symbol, simulate_only=False):
    try:
        if 'reconcile_closed_deals' in globals():
            try:
                reconcile_closed_deals(lookback_seconds=3600)
            except Exception:
                logger.debug("reconcile_closed_deals failed")
    except Exception:
        pass
    debug_info = {"symbol":symbol,"timestamp":str(datetime.utcnow()), "reason":None}
    try:
        tech_score = 0.0
        model_score = 0.0
        fund_score = 0.0
        sent_score = 0.0
        if 'compute_tech_score' in globals():
            try:
                tech_score = float(compute_tech_score(symbol))
            except Exception:
                tech_score = 0.0
        if 'compute_model_score' in globals():
            try:
                model_score = float(compute_model_score(symbol))
            except Exception:
                model_score = 0.0
        try:
            fund_res = fetch_fundamental_score(symbol)
            fund_score = float(fund_res.get("combined",0.0))
            sent_score = float(fund_res.get("news_sentiment",0.0))
            smoothed_sent = float(fund_res.get("details", {}).get("smoothed_sentiment",0.0))
        except Exception:
            fund_score=0.0; sent_score=0.0; smoothed_sent=0.0
        combined = compute_combined_score(tech_score, model_score, fund_score, smoothed_sent)
        combined = max(-1.0, min(1.0, combined))
        debug_info.update({"tech":tech_score,"model":model_score,"fund":fund_score,"smoothed_sent":smoothed_sent,"combined":combined})
        spread_ok = True
        if 'check_spread_ok' in globals():
            try:
                spread_ok = bool(check_spread_ok(symbol))
            except Exception:
                spread_ok = True
        if not spread_ok:
            debug_info["reason"]="spread"
            logger.info("TRADE BLOCKED %s reason=%s details=%s", symbol, debug_info["reason"], debug_info)
            return {"placed":False, "reason":debug_info["reason"], "debug":debug_info}
        max_ok = True
        try:
            if 'count_open_positions_for_symbol' in globals():
                open_count = int(count_open_positions_for_symbol(symbol))
                max_per_symbol = int(os.getenv("MAX_OPEN_PER_SYMBOL", "3"))
                if open_count >= max_per_symbol:
                    max_ok = False
        except Exception:
            max_ok = True
        if not max_ok:
            debug_info["reason"]="max_open"
            logger.info("TRADE BLOCKED %s reason=%s details=%s", symbol, debug_info["reason"], debug_info)
            return {"placed":False, "reason":debug_info["reason"], "debug":debug_info}
        try:
            if 'should_pause_for_events' in globals():
                pause, ev = should_pause_for_events(symbol, lookahead_minutes=60)
                if pause:
                    debug_info["reason"]="calendar_pause"; debug_info["calendar_event"]=ev
                    logger.info("TRADE BLOCKED %s reason=%s event=%s", symbol, debug_info["reason"], ev)
                    return {"placed":False, "reason":debug_info["reason"], "debug":debug_info}
        except Exception:
            pass
        if combined >= BUY_THRESHOLD:
            direction="BUY"
        elif combined <= SELL_THRESHOLD:
            direction="SELL"
        else:
            debug_info["reason"]="threshold_not_met"
            logger.debug("NO TRADE %s combined=%.4f tech=%.4f fund=%.4f sent=%.4f", symbol, combined, tech_score, fund_score, smoothed_sent)
            return {"placed":False, "reason":debug_info["reason"], "debug":debug_info}
        risk_pct, multiplier = compute_position_risk(RISK_PER_TRADE_PCT, tech_score, fund_score, smoothed_sent)
        debug_info.update({"direction":direction,"risk_pct":risk_pct,"multiplier":multiplier})
        placed_result = {"status":"simulated","symbol":symbol,"direction":direction,"risk_pct":risk_pct}
        try:
            if not simulate_only:
                if 'place_order' in globals():
                    placed_result = place_order(symbol, direction, risk_pct)
                elif 'send_order' in globals():
                    placed_result = send_order(symbol, direction, risk_pct)
                else:
                    placed_result = {"status":"simulated","symbol":symbol,"direction":direction,"risk_pct":risk_pct}
        except Exception as e:
            debug_info["reason"]="execution_error"; debug_info["execution_error"]=str(e)
            logger.exception("Order placement failed for %s: %s", symbol, e)
            return {"placed":False, "reason":debug_info["reason"], "debug":debug_info}
        logger.info("ORDER_PLACED %s dir=%s combined=%.4f risk=%.4f details=%s", symbol, direction, combined, risk_pct, debug_info)
        return {"placed":True, "status":placed_result, "debug":debug_info}
    except Exception as e:
        logger.exception("make_decision_for_symbol wrapper failed: %s", e)
        return {"placed":False, "reason":"internal_error", "debug":{"exc":str(e)}}
# ===== END BEAST BLOCK =====
'''
# create backup and merged file
bak = SRC.with_name(SRC.stem + "_backup_before_beast.py")
if not bak.exists():
    bak.write_text(SRC.read_text(encoding="utf-8"), encoding="utf-8")
DST.write_text(SRC.read_text(encoding="utf-8") + "\n\n" + enhanced_block, encoding="utf-8")
print("Created", DST)
print("Run a syntax check: python -m py_compile", DST.name)
