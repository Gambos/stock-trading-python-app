import requests
import os
import csv
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
LIMIT = 1000

def run_stock_job():
    url = f'https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    response = requests.get(url)
    tickers = []
    data = response.json()
    
    if data.get("status") == "ERROR":
            print("API error:", data.get("error"))
            exit()

    if "results" in data:
        for ticker in data['results']:
            tickers.append(ticker)

    while data.get("next_url"):
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()

        if data.get("status") == "ERROR":
            print("Stopped due to:", data.get("error"))
            break

        tickers.extend(data.get("results", []))

    example_ticker = {'ticker': 'HCSG', 'name': 'Healthcare Services Group', 'market': 'stocks', 'locale': 'us', 'primary_exchange': 'XNAS', 'type': 'CS', 'active': True, 'currency_name': 'usd', 'cik': '0000731012', 'composite_figi': 'BBG000BKYVF0', 'share_class_figi': 'BBG001S5RTV8', 'last_updated_utc': '2026-02-13T07:06:17.893536523Z'}

    # Write tickers to CSV with the same schema as example_ticker
    csv_filename = 'tickers.csv'
    if tickers:
        fieldnames = list(example_ticker.keys())
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(tickers)
        print(f"Successfully wrote {len(tickers)} tickers to {csv_filename}")

if __name__ == "__main__":
    run_stock_job()