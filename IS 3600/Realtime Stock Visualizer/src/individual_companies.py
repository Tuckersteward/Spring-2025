import yfinance as yf
import os
from datetime import datetime
import pandas as pd
import psycopg2 as psql

def gather_company_data(db_config, tickers=None):
    path = os.path.join('..', 'data') 
    file_name = f"gather_company_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    file = os.path.join(path, f"{file_name}.csv")

    conn = psql.connect(**db_config) 
    cur = conn.cursor()

    query = "select ticker from tickers"
    df = pd.read_sql(query, conn)
    tickers = df['ticker'].tolist()

    query = "SELECT MAX(ingestion_time) AS max_time FROM data"
    latest_ingestion_time = pd.read_sql(query, conn).iloc[0, 0]

    if not latest_ingestion_time:
        start_date = datetime.today().date().replace(year=datetime.today().year - 1)
        print("Table is empty, fetching data from one year ago.")
    else:
        start_date = latest_ingestion_time
        print(f"Fetching data starting from: {start_date}")
    
    data = yf.download(tickers, start=start_date, interval='1h', group_by='ticker')

    if data.empty:
        print("No new data available to fetch.")
    else:
        data = data.stack(level=0, future_stack=True).reset_index()
        data.columns = ['trans_date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']

        data['Volume'] = data['Volume'].fillna(0).astype(int)
        
        data.to_csv(file, index=False)

        try:
            cur.execute("""
                CREATE TEMP TABLE temp_data (
                    trans_date TIMESTAMP,
                    Ticker TEXT,
                    Open FLOAT,
                    High FLOAT,
                    Low FLOAT,
                    Close FLOAT,
                    Adj_Close FLOAT,
                    Volume BIGINT
                );
            """)
            
            with open(file, 'r') as f:
                next(f)
                cur.copy_expert("""
                    COPY temp_data (trans_date, Ticker, Open, High, Low, Close, Adj_Close, Volume)
                    FROM STDIN WITH CSV HEADER DELIMITER ',';
                """, f)
            
            cur.execute("""
                INSERT INTO data (trans_date, Ticker, Open, High, Low, Close, Adj_Close, Volume)
                SELECT *
                FROM temp_data
                ON CONFLICT (trans_date, ticker) DO NOTHING;
            """)

            conn.commit()
            print("Data inserted successfully, duplicates skipped!")
        except Exception as e:
            print(f"Error during data insertion: {e}")
        finally:
            conn.close()
            os.remove(file)
