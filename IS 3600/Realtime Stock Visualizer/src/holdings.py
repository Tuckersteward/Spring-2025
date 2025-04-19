from yahooquery import Ticker
import psycopg2

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

def insert_summary_details(symbol):
    etf = Ticker(symbol)
    summary = etf.summary_detail.get(symbol, {})

    if isinstance(summary, dict):
        cursor.execute(
            """
            INSERT INTO etf_summary_details (
                etf_symbol, regular_market_price, currency, market_cap, 
                yield, expense_ratio, fifty_two_week_low, fifty_two_week_high, 
                fifty_day_average, two_hundred_day_average, 
                trailing_annual_dividend_rate, trailing_annual_dividend_yield, 
                nav_price, previous_close, open_price, day_low, day_high, volume
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (etf_symbol) DO NOTHING
            """,
            (
                symbol,
                summary.get('regularMarketPreviousClose'),
                summary.get('currency'),
                summary.get('totalAssets'),
                summary.get('yield'),
                summary.get('annualReportExpenseRatio'),
                summary.get('fiftyTwoWeekLow'),
                summary.get('fiftyTwoWeekHigh'),
                summary.get('fiftyDayAverage'),
                summary.get('twoHundredDayAverage'),
                summary.get('trailingAnnualDividendRate'),
                summary.get('trailingAnnualDividendYield'),
                summary.get('navPrice'),
                summary.get('previousClose'),
                summary.get('open'),
                summary.get('dayLow'),
                summary.get('dayHigh'),
                summary.get('volume'),
            )
        )
        conn.commit()
        print(f"Summary details for {symbol} inserted successfully.")
    else:
        print(f"Unexpected data format for {symbol}: {summary}")

def insert_holdings(symbol):
    etf = Ticker(symbol)
    holdings_data = etf.fund_holding_info.get(symbol, {})

    holdings = holdings_data.get('holdings', [])

    if isinstance(holdings, list) and holdings:
        for holding in holdings:
            try:
                cursor.execute(
                    """
                    INSERT INTO etf_holdings (
                        etf_symbol, holding_name, holding_symbol, 
                        holding_percent, shares_held, holding_type, market_value
                    )
                    VALUES (%s, %s, %s, %s, NULL, NULL, NULL)
                    ON CONFLICT (etf_symbol, holding_symbol) DO NOTHING
                    """,
                    (
                        symbol,
                        holding.get('holdingName'),
                        holding.get('symbol'),
                        holding.get('holdingPercent')
                    )
                )
            except Exception as e:
                print(f"Error inserting holding: {e}")
        
        conn.commit()
        print(f"Holdings for {symbol} inserted successfully.")
    else:
        print(f"No holdings data found for {symbol} or unexpected data format.")

etf_symbol = 'SPY' 
insert_summary_details(etf_symbol)
insert_holdings(etf_symbol)

# Close the connection
cursor.close()
conn.close()
