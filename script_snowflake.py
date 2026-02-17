# Script to extract data from Massive API and load to Snowflake

import requests
import os
import snowflake.connector
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
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

    # Write tickers to Snowflake table
    if tickers:
        try:
            # Connect to Snowflake
            conn = snowflake.connector.connect(
                account=SNOWFLAKE_ACCOUNT,
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA
            )
            
            cursor = conn.cursor()
            
            # Create table if it doesn't exist
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS tickers (
                ticker VARCHAR(255),
                name VARCHAR(255),
                market VARCHAR(50),
                locale VARCHAR(10),
                primary_exchange VARCHAR(50),
                type VARCHAR(20),
                active BOOLEAN,
                currency_name VARCHAR(50),
                cik VARCHAR(50),
                composite_figi VARCHAR(50),
                share_class_figi VARCHAR(50),
                last_updated_utc TIMESTAMP_NTZ
            )
            """
            cursor.execute(create_table_sql)
            
            # Truncate table before inserting new data
            cursor.execute("TRUNCATE TABLE tickers")
            
            # Insert tickers into Snowflake table
            for ticker in tickers:
                insert_sql = """
                INSERT INTO tickers (
                    ticker, name, market, locale, primary_exchange, type, 
                    active, currency_name, cik, composite_figi, share_class_figi, 
                    last_updated_utc
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_sql, (
                    ticker.get('ticker'),
                    ticker.get('name'),
                    ticker.get('market'),
                    ticker.get('locale'),
                    ticker.get('primary_exchange'),
                    ticker.get('type'),
                    ticker.get('active'),
                    ticker.get('currency_name'),
                    ticker.get('cik'),
                    ticker.get('composite_figi'),
                    ticker.get('share_class_figi'),
                    ticker.get('last_updated_utc')
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Successfully wrote {len(tickers)} tickers to Snowflake table 'tickers'")
            
        except Exception as e:
            print(f"Error writing to Snowflake: {e}")
            raise


if __name__ == "__main__":
    run_stock_job()