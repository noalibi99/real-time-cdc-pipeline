import uuid
import time
import random
import os
from datetime import datetime

import numpy as np
import psycopg2


# =========================================================
# DATABASE
# =========================================================

def reset_table(conn):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE trades RESTART IDENTITY CASCADE;")
    conn.commit()
    print("🧹 trades table truncated")

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="trading",
    user="pgres",
    password="pgres"
)

reset_table(conn)

conn.autocommit = True


# =========================================================
# MARKET DATA
# =========================================================

TICKERS = {
    "AAPL": 210,
    "MSFT": 470,
    "NVDA": 1200,
    "AMZN": 190,
    "META": 620,
    "GOOGL": 180,
    "TSLA": 260,
    "AMD": 170,
    "NFLX": 950,
    "PLTR": 42,
}


CURRENT_PRICES = TICKERS.copy()


PORTFOLIOS = [
    {
        "portfolio_id": str(uuid.uuid4()),
        "strategy": "Momentum",
        "tickers": ["NVDA", "TSLA", "AMD"]
    },
    {
        "portfolio_id": str(uuid.uuid4()),
        "strategy": "Tech Growth",
        "tickers": ["AAPL", "MSFT", "META"]
    },
    {
        "portfolio_id": str(uuid.uuid4()),
        "strategy": "Retail",
        "tickers": list(TICKERS.keys())
    },
    {
        "portfolio_id": str(uuid.uuid4()),
        "strategy": "Value",
        "tickers": ["GOOGL", "AMZN"]
    },
    {
        "portfolio_id": str(uuid.uuid4()),
        "strategy": "Dividend",
        "tickers": ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
    }
]


# =========================================================
# HELPERS
# =========================================================

def update_price(ticker):

    CURRENT_PRICES[ticker] *= random.uniform(0.998, 1.002)

    return round(CURRENT_PRICES[ticker], 2)


def generate_trade():

    portfolio = random.choice(PORTFOLIOS)

    ticker = random.choice(portfolio["tickers"])

    price = update_price(ticker)

    trade_value = max(
        100,
        np.random.lognormal(mean=8, sigma=1)
    )

    quantity = int(np.round(trade_value / price, 2))
    quantity = max(1, quantity)


    side = np.random.choice(
        ["BUY", "SELL"],
        p=[0.55, 0.45]
    )

    return {
        "trade_id": str(uuid.uuid4()),
        "portfolio_id": portfolio["portfolio_id"],
        "ticker": ticker,
        "side": side,
        "quantity": quantity,
        "price": price,
        "trade_value": round(quantity * price, 2),
        "fees": round(trade_value * 0.0005, 2),
        "trade_timestamp": datetime.utcnow()
    }


# =========================================================
# INSERT
# =========================================================

INSERT_QUERY = """
INSERT INTO trades (
    trade_id,
    portfolio_id,
    ticker,
    side,
    quantity,
    price,
    trade_value,
    fees,
    trade_timestamp
)
VALUES (
    %(trade_id)s,
    %(portfolio_id)s,
    %(ticker)s,
    %(side)s,
    %(quantity)s,
    %(price)s,
    %(trade_value)s,
    %(fees)s,
    %(trade_timestamp)s
)
"""


# =========================================================
# MAIN LOOP
# =========================================================

cursor = conn.cursor()

print("Starting live trade generation...\n")


trades_per_second = int(os.getenv("TRADES_PER_SECOND", "10"))


while True:

    start = time.time()

    for _ in range(trades_per_second):

        trade = generate_trade()

        cursor.execute(INSERT_QUERY, trade)

        print(
            f"[{trade['trade_timestamp']}] "
            f"{trade['side']:4} "
            f"{trade['ticker']:5} "
            f"qty={trade['quantity']:8} "
            f"price=${trade['price']:8} "
            f"value=${trade['trade_value']:10}"
        )

    elapsed = time.time() - start

    sleep_time = max(0, 1 - elapsed)

    time.sleep(sleep_time)