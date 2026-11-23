import os
import signal
import sqlite3
import threading
import pandas as pd
from dotenv import load_dotenv
from trading_ig import IGService
from trading_ig.stream import IGStreamService, Subscription

load_dotenv()

stop_event = threading.Event()


def handle_sigint(sig, frame):
    print("\nShutting down...")
    stop_event.set()


signal.signal(signal.SIGINT, handle_sigint)


# ---------------------------------------
# DB STORAGE
# ---------------------------------------
class CandleStore:
    def __init__(self, db_path="btbot.db"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        with self.lock:
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS price_data (
                    epic TEXT,
                    date TEXT,
                    scale TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    PRIMARY KEY (epic, date, scale)
                )
                """
            )
            self.conn.commit()

    def write(self, ohlc):

        with self.lock:
            try:
                self.conn.execute(
                    """
                    INSERT OR REPLACE INTO price_data
                    (epic, date, scale, open, high, low, close)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ohlc["epic"],
                        ohlc["date"],
                        ohlc["scale"],
                        ohlc["open"],
                        ohlc["high"],
                        ohlc["low"],
                        ohlc["close"],
                    ),
                )
                self.conn.commit()
            except Exception as e:
                print("DB error:", e)

    def close(self):
        with self.lock:
            self.conn.close()


# ---------------------------------------
# TICK -> CANDLE AGGREGATOR
# ---------------------------------------
class TickAggregator:
    """Builds 1-minute and 5-minute OHLC candles from mid-price ticks."""

    def __init__(self, store_func):
        self.store = store_func
        self.lock = threading.Lock()
        self.current_1m = {}
        self.current_5m = {}
        self.last_5m_bucket = {}

    @staticmethod
    def _new_candle(epic, bucket_ts, scale, mid):
        return {
            "epic": epic,
            "date": bucket_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "scale": scale,
            "open": mid,
            "high": mid,
            "low": mid,
            "close": mid,
        }

    def process_tick(self, epic, bid, offer, ts):
        mid = (bid + offer) / 2.0
        ts_1m = ts.replace(second=0, microsecond=0)
        ts_5m = ts.replace(minute=ts.minute - (ts.minute % 5), second=0, microsecond=0)

        with self.lock:
            # 1-minute candle
            c1 = self.current_1m.get(epic)
            label_1m = ts_1m.strftime("%Y-%m-%d %H:%M:%S")
            if c1 is None:
                self.current_1m[epic] = self._new_candle(epic, ts_1m, "1MINUTE", mid)
            elif c1["date"] != label_1m:
                self.store(c1)
                self.current_1m[epic] = self._new_candle(epic, ts_1m, "1MINUTE", mid)
            else:
                c1["high"] = max(c1["high"], mid)
                c1["low"] = min(c1["low"], mid)
                c1["close"] = mid

            # 5-minute candle
            c5 = self.current_5m.get(epic)
            if c5 is None:
                self.current_5m[epic] = self._new_candle(epic, ts_5m, "5MINUTE", mid)
                self.last_5m_bucket[epic] = ts_5m
            elif ts_5m != self.last_5m_bucket[epic]:
                self.store(c5)
                self.current_5m[epic] = self._new_candle(epic, ts_5m, "5MINUTE", mid)
                self.last_5m_bucket[epic] = ts_5m
            else:
                c5["high"] = max(c5["high"], mid)
                c5["low"] = min(c5["low"], mid)
                c5["close"] = mid

    def flush(self):
        with self.lock:
            for c in list(self.current_1m.values()):
                self.store(c)
            for c in list(self.current_5m.values()):
                self.store(c)
            self.current_1m.clear()
            self.current_5m.clear()
            self.last_5m_bucket.clear()


# ---------------------------------------
# PRICE STREAM LISTENER
# ---------------------------------------
class MarketListener:
    def __init__(self, aggregator):
        self.agg = aggregator

    def onSubscription(self):
        print("📡 PRICE subscription active")

    def onSubscriptionError(self, code, message):
        print(f"❌ Subscription error {code}: {message}")

    def onItemUpdate(self, update):
        try:
            item_name = update.getItemName()
            _, epic = item_name.rsplit(":", 1)

            bid_raw = update.getValue("BIDPRICE1") or update.getValue("BID")
            offer_raw = update.getValue("ASKPRICE1") or update.getValue("OFFER")
            ts_raw = update.getValue("TIMESTAMP") or update.getValue("UPDATE_TIME")

            if bid_raw is None or offer_raw is None:
                return

            bid = float(bid_raw)
            offer = float(offer_raw)

            if ts_raw is not None and str(ts_raw).strip().isdigit():
                ts = pd.to_datetime(int(ts_raw), unit="ms", utc=True)
            elif ts_raw:
                ts = pd.to_datetime(ts_raw, utc=True)
            else:
                ts = pd.Timestamp.now(tz="UTC")

            self.agg.process_tick(epic, bid, offer, ts)
        except Exception as e:
            print(f"❌ Error in onItemUpdate: {e}")

    def onItemLostUpdates(self, item, lost, key):
        print(f"⚠️ Lost {lost} updates for {item}")


# ---------------------------------------
# MAIN
# ---------------------------------------
def main():
    store = CandleStore("btbot.db")
    aggregator = TickAggregator(store.write)

    ig_service = IGService(
        username=os.getenv("IDENTIFIER"),
        password=os.getenv("PASSWORD"),
        api_key=os.getenv("API_KEY"),
        acc_type=os.getenv("ACC_TYPE"),
    )

    session_info = ig_service.create_session() or {}
    acc_number = (
        session_info.get("currentAccountId")
        or getattr(ig_service, "acc_number", None)
        or os.getenv("ACC_NUMBER")
    )
    if not acc_number:
        raise RuntimeError(
            "Could not determine account ID from create_session(); set ACC_NUMBER in .env if needed."
        )

    stream_svc = IGStreamService(ig_service)
    stream_svc.create_session()

    epics = ["IX.D.FTSE.DAILY.IP", "CS.D.USCGC.TODAY.IP", "IX.D.DOW.DAILY.IP"]
    fields = ["BIDPRICE1", "ASKPRICE1", "TIMESTAMP"]

    for epic in epics:
        item = f"PRICE:{acc_number}:{epic}"
        sub = Subscription(mode="MERGE", items=[item], fields=fields)
        sub.setDataAdapter("Pricing")
        sub.addListener(MarketListener(aggregator))
        stream_svc.subscribe(sub)

    try:
        while not stop_event.is_set():
            stop_event.wait(timeout=0.5)
    except KeyboardInterrupt:
        print("\nInterrupted via KeyboardInterrupt...")
        stop_event.set()
    finally:
        aggregator.flush()
        stream_svc.disconnect()
        store.close()
        print("Disconnected cleanly")


if __name__ == "__main__":
    main()