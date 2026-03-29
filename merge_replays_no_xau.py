# merge_replays_no_xau.py
import csv, os

OUT = "KYOTO_V16_BACKTEST_REPORT.csv"
REPLAY_DIR = "debug_backtest_output"
# list only the replay files to include (no XAUUSDm)
FILES = [
    "replay_trades_BTCUSDm.csv",
    "replay_trades_EURUSDm.csv",
    "replay_trades_USDJPYm.csv",
    "replay_trades_USOILm.csv",
    "replay_trades_DXYm.csv",
]

# header expected
HEADER = ["time", "type", "entry", "exit", "pnl", "exit_time", "atr_at_entry"]

# write fresh file
with open(OUT, "w", newline="", encoding="utf-8") as of:
    writer = csv.writer(of)
    writer.writerow(HEADER)

    total = 0
    for fn in FILES:
        path = os.path.join(REPLAY_DIR, fn)
        if not os.path.exists(path):
            print("Missing:", path)
            continue
        with open(path, "r", encoding="utf-8") as rf:
            rdr = csv.reader(rf)
            hdr = next(rdr, None)
            # accept either header or data starting immediately
            for row in rdr:
                if not row:
                    continue
                # pad/truncate to 7 columns
                row = (row + [""]*7)[:7]
                writer.writerow(row)
                total += 1
    print("Appended", total, "rows to", OUT)
