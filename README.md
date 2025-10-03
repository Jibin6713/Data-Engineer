
# stock-trading-python-app

This project uses the Polygon.io API to extract stock data and loads it into a Snowflake warehouse.

## Features
- Fetches stock tickers and metadata from Polygon.io
- Loads data directly into a Snowflake table (`STOCK_TICKERS`)
- Configurable via environment variables

## Setup
1. Clone the repository:
	```bash
	git clone https://github.com/Jibin6713/stock-trading-python-app.git
	```
2. Create and activate a Python virtual environment:
	```bash
	python -m venv venv
	source venv/bin/activate  # On Windows: venv\Scripts\activate
	```
3. Install dependencies:
	```bash
	pip install -r requirements.txt
	```
4. Set up your `.env` file with the following variables:
	```env
	POLYGON_API_KEY=your_polygon_api_key
	SNOWFLAKE_PASSWORD=your_snowflake_password
	SNOWFLAKE_DATABASE=your_database
	SNOWFLAKE_SCHEMA=your_schema
	SNOWFLAKE_WAREHOUSE=your_warehouse
	```

## Usage
Run the main script:
```bash
python script.py
```

