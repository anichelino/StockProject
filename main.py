import os
import yfinance as yf 
import time
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

# Supabase configuration
#SUPABASE_URL = "https://lpywbpqmxwcrnodkeaau.supabase.co"
#SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxweXdicHFteHdjcm5vZGtlYWF1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDQ0NzE4MzcsImV4cCI6MjA2MDA0NzgzN30.kqCVUVjkzZUYHsRkJbyy6ug1YG9h1gvGWHMPb-x53Xo"
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# List of stock tickers to track
# Fetch the top 100 most relevant stocks from a predefined list or an external source
STOCKS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "BRK-B", "JNJ", "V",
    "UNH", "WMT", "PG", "JPM", "MA", "XOM", "LLY", "HD", "CVX", "ABBV",
    "KO", "PEP", "MRK", "BAC", "PFE", "COST", "TMO", "AVGO", "DIS", "CSCO",
    "MCD", "ADBE", "CRM", "NFLX", "ACN", "DHR", "TXN", "LIN", "NEE", "PM",
    "NKE", "WFC", "BMY", "AMD", "HON", "UNP", "AMGN", "INTC", "LOW", "RTX",
    "MS", "ELV", "SCHW", "SPGI", "GS", "PLD", "IBM", "BLK", "T", "MDT",
    "CAT", "CVS", "DE", "AMT", "C", "NOW", "LMT", "INTU", "SYK", "MO",
    "BKNG", "ISRG", "ADI", "ZTS", "GE", "EQIX", "REGN", "ADP", "MDLZ", "MU",
    "GILD", "AXP", "TGT", "BSX", "CI", "CB", "MMC", "EW", "CSX", "DUK",
    "SO", "PNC", "BDX", "ITW", "SHW", "APD", "ICE", "HUM", "NSC", "PGR"
]



def fetch_stock_data():
    data = {}
    for ticker in STOCKS:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1d", interval="1m")
        if not hist.empty:
            current_price = hist.iloc[-1]['Close']
            print(f"Current price of {ticker}: {current_price}")  # Print the current stock price
            data[ticker] = current_price
    return data

def store_data_in_supabase(data):
    # Ensure the table exists before inserting data
    for ticker, price in data.items():
        # Check if the "dropdown" column exists in the table
        table_info = supabase.table("stock_prices").select("*").limit(1).execute()
        if table_info.data and "dropdown" not in table_info.data[0]:
            print("The 'dropdown' column does not exist in the 'stock_prices' table. Please add it manually to the database schema.")
            return

        supabase.table("stock_prices").insert({
            "ticker": ticker,
            "price": price,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "dropdown": None  # Initialize dropdown as None or 0 if needed
        }).execute()

def check_dropdowns():
    one_hour_ago = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    for ticker in STOCKS:
        response = supabase.table("stock_prices").select("*").eq("ticker", ticker).gte("timestamp", (datetime.now(tz=timezone.utc) - timedelta(hours=3)).isoformat()).lt("timestamp", one_hour_ago.isoformat()).execute()
        records = response.data
        if records:
            print(f"Records found for {ticker}: {len(records)}")
            prices = [record["price"] for record in records]
            timestamps = [record["timestamp"] for record in records]
            max_price = max(prices)
            min_price = min(prices)
            initial_price = prices[0]
            final_price = prices[-1]
            dropdown = (max_price - final_price) / max_price * 100

            # Ensure the "dropdowns" table exists before inserting data
            table_info = supabase.table("dropdowns").select("*").limit(1).execute()
            if not table_info.data:
                print("The 'dropdowns' table does not exist in the database. Creating it now.")
                supabase.table("dropdowns").create({
                    "ticker": "TEXT",
                    "initial_price": "FLOAT",
                    "final_price": "FLOAT",
                    "max_price": "FLOAT",
                    "min_price": "FLOAT",
                    "dropdown": "FLOAT",
                    "start_timestamp": "TIMESTAMP",
                    "end_timestamp": "TIMESTAMP",
                    "calculated_at": "TIMESTAMP"
                }).execute()

            # Insert dropdown data into the "dropdowns" table
            supabase.table("dropdowns").insert({
            "ticker": ticker,
            "initial_price": initial_price,
            "final_price": final_price,
            "max_price": max_price,
            "min_price": min_price,
            "dropdown": dropdown,
            "start_timestamp": timestamps[0],
            "end_timestamp": timestamps[-1],
            "calculated_at": datetime.now(timezone.utc).isoformat()
            }).execute()

            print(f"{ticker}: Dropdown table updated with initial price {initial_price}, final price {final_price}, max price {max_price}, min price {min_price}, and dropdown {dropdown:.2f}%")
        else:
            print(f"No records found for {ticker} in the last hour.")
            

def clean_old_records():
    # Calculate the timestamp for one day ago
    two_days_ago = datetime.now(tz=timezone.utc) - timedelta(days=2)
    # Delete all records older than one day
    response = supabase.table("stock_prices").delete().lt("timestamp", two_days_ago.isoformat()).execute()
    print(f"Deleted records older than one day: {response}")


def main():
    #while True:
        clean_old_records()  # Clean up old records
        stock_data = fetch_stock_data()
        store_data_in_supabase(stock_data)
        check_dropdowns()
        #time.sleep(80)  # Wait for 1 minute

if __name__ == "__main__":
    main()