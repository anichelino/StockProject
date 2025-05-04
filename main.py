import os
import requests
import yfinance as yf 
import time
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

# Supabase configuration
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
    "SO", "PNC", "BDX", "ITW", "SHW", "APD", "ICE", "HUM", "NSC", "PGR", 
    "RY", "BHP", "RIO", "TM", "SHEL", "BP", "UL", "VZ", "FDX", "UPS", 
    "NEM", "ORCL", "PAYX", "GLD", "SLV", "USO", "UNG", "DBC",  # Materie prime
    "NOVO-B.CO",  # Novo Nordisk (Danimarca, salute)
    "ASML.AS",  # ASML (Paesi Bassi, semiconduttori)
    "SAP",  # SAP (Germania, software enterprise)
    "LVMH.PA",  # LVMH (Francia, lusso)
    "HSBC",  # HSBC (UK, banca)
    "UNILEVER",  # Unilever (Regno Unito/Paesi Bassi, beni di consumo)
    "SIEMENS",  # Siemens (Germania, ingegneria e tecnologia)
    "BMW",  # BMW (Germania, automotive)
    "STELLANTIS",  # Stellantis (Olanda, automotive)
    "TOTALENERGIES",  # TotalEnergies (Francia, energia)
    "BASF",  # BASF (Germania, chimica)
    "DANONE",  # Danone (Francia, alimenti e bevande)
    "AIRBUS",  # Airbus (Francia, aerospaziale)
    "BP",  # BP (Regno Unito, energia)
    "ROCHE",  # Roche (Svizzera, salute)
    "NOVARTIS",  # Novartis (Svizzera, farmaceutica)
    "VODAFONE",  # Vodafone (UK, telecomunicazioni)
    "DEUTSCHE BANK",  # Deutsche Bank (Germania, banca)
    "KERING",  # Kering (Francia, moda e lusso)
    "SHELL",  # Shell (Paesi Bassi, energia)
    "ALLIANZ",  # Allianz (Germania, assicurazioni)
    "RENAULT",  # Renault (Francia, automotive)
]


def fetch_stock_data():
    data = {}
    for ticker in STOCKS:
        stock = yf.Ticker(ticker)
        try:
            hist = stock.history(period="1d", interval="1m")
            if not hist.empty:
                current_price = hist.iloc[-1]['Close']
                print(f"Current price of {ticker}: {current_price}")  # Print the current stock price
                data[ticker] = current_price
        except yf.exceptions.YFRateLimitError:
            print(f"Rate limit hit for {ticker}. Retrying after a delay...")
            time.sleep(5)  # Wait for 5 seconds before retrying
        time.sleep(1)  # Add a delay between requests to avoid hitting the rate limit
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
        response = supabase.table("stock_prices").select("*").eq("ticker", ticker).gte("timestamp", (datetime.now(tz=timezone.utc) - timedelta(hours=3)).isoformat()).execute()
        records = response.data
        if records:
            print(f"Records found for {ticker}: {len(records)}")
            prices = [record["price"] for record in records]
            timestamps = [record["timestamp"] for record in records]
            max_price = max(prices)
            min_price = min(prices)
            initial_price = prices[0]

            # Fetch the current price for the ticker
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d", interval="1m")
            if not hist.empty:
                final_price = hist.iloc[-1]['Close']
            else:
                print(f"Failed to fetch current price for {ticker}. Skipping...")
                continue
            dropdown = (max_price - final_price) / max_price * 100

            # Ensure the "dropdowns" table exists before inserting or updating data
            table_info = supabase.table("dropdowns").select("*").limit(1).execute()
            if not table_info.data:
                print("The 'dropdowns' table does not exist in the database. Please create it manually using the Supabase dashboard or SQL migration.")
                continue

            # Check if a record for the current ticker already exists
            existing_record_response = supabase.table("dropdowns").select("*").eq("ticker", ticker).order("calculated_at", desc=True).limit(1).execute()
            existing_record = existing_record_response.data[0] if existing_record_response.data else None

            if existing_record:
                # Compare the new dropdown with the existing one
                if dropdown > existing_record["dropdown"] or dropdown>=5:
                    #Update the existing record
                    supabase.table("dropdowns").update({
                        "initial_price": initial_price,
                        "final_price": final_price,
                        "max_price": max_price,
                        "min_price": min_price,
                        "dropdown": dropdown,
                        "start_timestamp": timestamps[0],
                        "end_timestamp": timestamps[-1],
                        "calculated_at": datetime.now(timezone.utc).isoformat()
                    }).eq("id", existing_record["id"]).execute()
                    print(f"{ticker}: Updated existing dropdown record with new dropdown {dropdown:.2f}%")
                    # Send a message to a Telegram account with the dropdown information

                    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
                    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

                    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                        message = (
                            f"Ticker: {ticker}\n"
                            f"Initial Price: {initial_price}\n"
                            f"Final Price: {final_price}\n"
                            f"Max Price: {max_price}\n"
                            f"Min Price: {min_price}\n"
                            f"Dropdown: {dropdown:.2f}%\n"
                            f"Start Timestamp: {timestamps[0]}\n"
                            f"End Timestamp: {timestamps[-1]}"
                        )
                        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                        payload = {
                            "chat_id": TELEGRAM_CHAT_ID,
                            "text": message
                        }
                        response = requests.post(url, json=payload)
                        if response.status_code == 200:
                            print(f"Message sent to Telegram for {ticker}")
                        else:
                            print(f"Failed to send message to Telegram for {ticker}: {response.text}")
                    else:
                        print("Telegram bot token or chat ID is not set. Unable to send message.")
                else:
                    print(f"{ticker}: Existing dropdown {existing_record['dropdown']:.2f}% is greater than or equal to the new dropdown {dropdown:.2f}%. No update made.")
            else:
                # Insert a new record if none exists
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
                print(f"{ticker}: Inserted new dropdown record with dropdown {dropdown:.2f}%")
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
