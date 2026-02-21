# features/ensemble.py
def combined_score(tech, fund, sent, w_tech=0.5, w_fund=0.25, w_sent=0.25):
    try:
        return float(w_tech*tech + w_fund*fund + w_sent*sent)
    except Exception:
        return 0.0

def map_score_to_signal(score, buy_thresh=0.35, sell_thresh=-0.35):
    try:
        s = float(score)
        if s >= buy_thresh:
            return "BUY"
        if s <= sell_thresh:
            return "SELL"
    except Exception:
        pass
    return None
