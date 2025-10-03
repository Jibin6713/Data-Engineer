
import requests
import os
from dotenv import load_dotenv
load_dotenv()
import snowflake.connector
from datetime import datetime



POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

LIMIT = 100
DS = '2025-10-03'

def run_stock_job():
    DS = datetime.now().strftime('%Y-%m-%d')
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    tickers = []
    data = response.json()
    if 'results' in data:
        for ticker in data['results']:
            ticker['ds'] = DS
            tickers.append(ticker)
    else:
        print("Error in initial response:", data.get('error', data))

    while 'next_url' in data:
        print('requesting next page', data['next_url'])
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        print(data)
        if 'results' in data:
            for ticker in data['results']:
                ticker['ds'] = DS
                tickers.append(ticker)
        else:
            print("Error in paginated response:", data.get('error', data))
            break

    print(f"Total tickers fetched: {len(tickers)}")

    # Connect to Snowflake using provided account and username
    ctx = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),  # Derived from your URL
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
    )
    cs = ctx.cursor()

    columns = [
        'ticker', 'name', 'market', 'locale', 'primary_exchange', 'type', 'active',
        'currency_name', 'cik', 'last_updated_utc', 'ds'
    ]

    # Insert each ticker into Snowflake
    SNOWFLAKE_TABLE = "STOCK_TICKERS"
    insert_sql = f"""
        INSERT INTO {SNOWFLAKE_TABLE} ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
    """

    for ticker in tickers:
        row = [ticker.get(col, '') for col in columns]
        try:
            cs.execute(insert_sql, row)
        except Exception as e:
            print(f"Error inserting row: {row}\n{e}")

    cs.close()
    ctx.close()
    print(f"Tickers written to Snowflake table {SNOWFLAKE_TABLE}")

if __name__=='__main__':
    run_stock_job()    
