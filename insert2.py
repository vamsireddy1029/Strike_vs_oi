import sqlite3
import zmq
import time
import os
import sys
from datetime import datetime
import json

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Move up one level to reach 'C:\Users\stk\Desktop\RMS29'
base_dir = os.path.abspath(os.path.join(current_dir, ".."))

# Add the directory containing 'paths' to sys.path
sys.path.append(base_dir)

# File to track the last run date
DATE_TRACKER_FILE = os.path.join(base_dir, "last_run_date.txt") 

# Database name with folder path
db_name = os.path.join(current_dir, "market_data.db")

def initialize_database(reset=False):
    """Initialize the database. Drops table if reset=True."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    if reset:
        cursor.execute("DROP TABLE IF EXISTS market_data")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            timestamp TEXT,
            exc_timestamp TEXT,
            token INTEGER PRIMARY KEY,
            instrument_token INTEGER,
            trading_symbol TEXT,
            ltp REAL,
            bidprice REAL,
            bidqty INTEGER,
            askprice REAL,
            askqty INTEGER,
            volume INTEGER,
            oi INTEGER,
            oi_day_high INTEGER,
            oi_day_low INTEGER
        )
    """)

    conn.commit()
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_token ON market_data(token);")
    conn.close()

# Check last run date and decide if a reset is needed
current_date = datetime.today().strftime('%Y_%m_%d')

reset_db = False  # Default: Do not reset the DB

if os.path.exists(DATE_TRACKER_FILE):
    with open(DATE_TRACKER_FILE, 'r') as file:
        last_run_date = file.read().strip()
    if last_run_date != current_date:
        reset_db = True  # Reset the DB since the date changed
else:
    reset_db = True  # No file exists, assume first run

# Initialize the database (drop table if reset_db=True)
initialize_database(reset=reset_db)

def to_float_or_none(value):
    try:
        return float(value) if value is not None else None
    except:
        return None

def to_int_or_none(value):
    try:
        return int(value) if value is not None else None
    except:
        return None

# Batch upsert function to efficiently insert or update database records
def upsert_data_batch(conn, chunks):
    cursor = conn.cursor()
    query = """
    INSERT INTO market_data (
        timestamp, exc_timestamp, token, instrument_token, trading_symbol,
        ltp, bidprice, bidqty, askprice, askqty,
        volume, oi, oi_day_high, oi_day_low
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(token) DO UPDATE SET
        timestamp=excluded.timestamp,
        exc_timestamp=excluded.exc_timestamp,
        token=excluded.token,
        instrument_token=excluded.instrument_token,
        trading_symbol=excluded.trading_symbol,
        ltp=excluded.ltp,
        bidprice=excluded.bidprice,
        bidqty=excluded.bidqty,
        askprice=excluded.askprice,
        askqty=excluded.askqty,
        volume=excluded.volume,
        oi=excluded.oi,
        oi_day_high=excluded.oi_day_high,
        oi_day_low=excluded.oi_day_low
    """

    cursor.executemany(query, chunks)
    conn.commit()
    

# Wrapper for ZeroMQ TCP communication
class TCP_pipe:
    def __init__(self, address="tcp://192.168.1.40:4801", mode="sub"):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB if mode == "sub" else zmq.PUB)
        if mode == "sub":
            self.socket.connect(address)
            self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
        elif mode == "pub":
            self.socket.bind(address)

    def send(self, message):
        self.socket.send_string(message)

    def poll(self, timeout=1000):
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
        socks = dict(poller.poll(timeout))
        return socks.get(self.socket) == zmq.POLLIN

    def recv(self):
        message = self.socket.recv_string()
        return message.strip().split("\n")

    def close(self):
        self.socket.close()
        self.context.term()

# Extract data from the stream and update the database
def extract_ltp_from_stream_to_db():
    datastream = TCP_pipe()
    conn = sqlite3.connect(db_name)
    batch_data = []
    batch_size = 100

    try:
        while True:
            if datastream.poll():
                tick = datastream.recv()
                parsed_batch = []
                for line in tick:
                    try:
                        data = json.loads(line)
                        print(data)
                        #exit(0)
                        parsed_batch.append((
                            data.get("last_trade_time") or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            data.get("exchange_timestamp") or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            int(data["exchange_token"]),
                            int(data["instrument_token"]),
                            data.get("trading_symbol"),
                            to_float_or_none(data.get("last_price")),
                            to_float_or_none(data.get("bid_depth", [{}])[0].get("price")),
                            to_int_or_none(data.get("bid_depth", [{}])[0].get("quantity")),
                            to_float_or_none(data.get("ask_depth", [{}])[0].get("price")),
                            to_int_or_none(data.get("ask_depth", [{}])[0].get("quantity")),
                            to_float_or_none(data.get("volume")),
                            to_int_or_none(data.get("oi")),
                            to_int_or_none(data.get("oi_day_high")),
                            to_int_or_none(data.get("oi_day_low"))
                        ))
                    except Exception as e:
                        print(f"Error parsing tick: {line}, error: {e}")

                if parsed_batch:
                    upsert_data_batch(conn, parsed_batch)

    except KeyboardInterrupt:
        print("Terminating the stream...")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if batch_data:
            upsert_data_batch(conn, batch_data)
            print(f"Final batch of {len(batch_data)} rows processed.")
        datastream.close()
        conn.close()

if __name__ == "__main__":
    extract_ltp_from_stream_to_db()
