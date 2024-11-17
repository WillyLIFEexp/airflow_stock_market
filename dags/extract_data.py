from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

import yfinance as yf
import pandas as pd

default_args = {
    "owner": "William Chen",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 11, 1),
}

@dag(
    dag_id="stock_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",  # Run daily
    catchup=False,
)
def stock_data_pipeline():
    @task
    def create_raw_table(ticker: str, postgres_conn_id: str):
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS raw_stock_data_{ticker.lower()} (
            date TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            dividends FLOAT,
            stock_splits FLOAT
        );
        """
        pg_hook.run(create_table_query)
        print(f"Table raw_stock_data_{ticker.lower()} created successfully.")


    @task
    def fetch_raw_data(ticker: str, postgres_conn_id: str):
        # Fetch data from Yahoo Finance
        stock = yf.Ticker(ticker)
        data = stock.history(period="1mo")
        data.reset_index(inplace=True)

        # Convert data types to Python-native types
        data = data.astype({
            "Open": float,
            "High": float,
            "Low": float,
            "Close": float,
            "Volume": int,
            "Dividends": float,
            "Stock Splits": float
        })

        # Insert data into PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        insert_query = f"""
        INSERT INTO raw_stock_data_{ticker.lower()} (date, open, high, low, close, volume, dividends, stock_splits)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        rows = data.to_records(index=False).tolist()
        pg_hook.insert_rows(table=f"raw_stock_data_{ticker.lower()}", rows=rows)
        print(f"Successfully saved {len(rows)} raw data for {ticker} to PostgreSQL.")

    @task
    def select_raw_table(ticker: str, postgres_conn_id: str):
        # Connect to PostgreSQL using PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Query the table
        query = f"SELECT * FROM raw_stock_data_{ticker.lower()} LIMIT 10;"  # Use LIMIT to avoid fetching too much data
        cursor.execute(query)
        
        # Fetch and print results
        rows = cursor.fetchall()
        print(f"Data from table raw_stock_data_{ticker.lower()}:")
        for row in rows:
            print(row)

    # List of stock tickers
    tickers = ["AAPL", "GOOGL", "MSFT"]

    # Create tasks for each ticker
    for ticker in tickers:
        create_table_task = create_raw_table(ticker, postgres_conn_id="postgresql_raw")
        fetch_task = fetch_raw_data(ticker, postgres_conn_id="postgresql_raw")
        select_table = select_raw_table(ticker, postgres_conn_id="postgresql_raw")

        # Define task dependencies
        create_table_task >> fetch_task >> select_table

# Instantiate the DAG
dag_instance = stock_data_pipeline() 
