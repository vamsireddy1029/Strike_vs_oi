import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time

def round_to_3min(dt):
    minute = dt.minute - (dt.minute % 3)
    return dt.replace(minute=minute, second=0, microsecond=0)

def fetch_and_snapshot():
    now = datetime.now()
    snapshot_start = round_to_3min(now)
    snapshot_end = snapshot_start + timedelta(minutes=3) - timedelta(seconds=1)

    start_str = snapshot_start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = snapshot_end.strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n⏱ Snapshot range: {start_str} to {end_str}")

    # Connect to source DB
    src_conn = sqlite3.connect("market_data.db")
    query = f"""
        SELECT timestamp, trading_symbol, oi, oi_day_high
        FROM market_data
        WHERE timestamp >= '{start_str}' AND timestamp <= '{end_str}'
    """
    df = pd.read_sql_query(query, src_conn)
    src_conn.close()

    if df.empty:
        print("⚠️  No data found in this window.")
        return

    print(f"✅ Found {len(df)} records. Saving snapshot...")

    # Connect to destination DB
    dst_conn = sqlite3.connect("snapshot_data.db")
    dst_conn.execute("""
        CREATE TABLE IF NOT EXISTS oi_snapshot (
            snapshot_time TEXT,
            trading_symbol TEXT,
            oi INTEGER,
            oi_day_high INTEGER
        )
    """)
    dst_conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_unique
        ON oi_snapshot (snapshot_time, trading_symbol)
    """)

    # Insert rows (skip duplicates)
    for row in df.itertuples():
        dst_conn.execute(
            "INSERT OR IGNORE INTO oi_snapshot (snapshot_time, trading_symbol, oi, oi_day_high) VALUES (?, ?, ?, ?)",
            (snapshot_start.strftime("%Y-%m-%d %H:%M:%S"), row.trading_symbol, row.oi, row.oi_day_high)
        )

    dst_conn.commit()
    dst_conn.close()
    print("🚀 Snapshot saved successfully!")

print("📡 Running snapshot fetch every 3 minutes using time.time()...")

interval = 3 * 60
next_run = time.time()

while True:
    now = time.time()
    if now >= next_run:
        fetch_and_snapshot()
        next_run += interval
