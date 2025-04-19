import psycopg2 as psql
import pandas as pd
from yahooquery import Ticker


def get_financials(db_config):
    conn = psql.connect(**db_config)
    cursor = conn.cursor()

    query = "SELECT ticker FROM tickers where type <> 'Mutual Fund'"
    tickers_df = pd.read_sql_query(query, conn)
    tickers_list = tickers_df['ticker'].tolist()

    fields_to_insert = [
        'ticker', 'company_name', 'sector', 'industry', 'market_cap',
        'dividend_yield', 'trailing_pe', 'forward_pe', 'beta', 
        'earnings_growth', 'revenue_growth', 'return_on_assets', 
        'return_on_equity', 'current_price', 'target_high_price', 
        'target_low_price', 'target_mean_price', 'recommendation_key', 
        'number_of_analyst_opinions', 'total_cash', 'total_debt', 
        'revenue_per_share', 'gross_margins', 'operating_margins', 
        'net_income_to_common', 'total_revenue'
    ]

    tickers = Ticker(tickers_list)
    financial_data = tickers.get_modules('summaryDetail,financialData,defaultKeyStatistics,quoteType,assetProfile')

    for ticker in tickers_list:
        data = financial_data.get(ticker, {})

        if not data or 'summaryDetail' not in data or 'marketCap' not in data.get('summaryDetail', {}):
            print(f"Data missing or incomplete for {ticker}. Skipping.")
            continue

        insert_values = (
            data.get('quoteType', {}).get('symbol', ticker),
            data.get('quoteType', {}).get('longName', ''),
            data.get('assetProfile', {}).get('sector', 'N/A'),
            data.get('assetProfile', {}).get('industry', 'N/A'),
            data.get('summaryDetail', {}).get('marketCap', 0),
            data.get('summaryDetail', {}).get('dividendYield', 0),
            data.get('summaryDetail', {}).get('trailingPE', 0),
            data.get('summaryDetail', {}).get('forwardPE', 0),
            data.get('summaryDetail', {}).get('beta', 0),
            data.get('financialData', {}).get('earningsGrowth', 0),
            data.get('financialData', {}).get('revenueGrowth', 0),
            data.get('financialData', {}).get('returnOnAssets', 0),
            data.get('financialData', {}).get('returnOnEquity', 0),
            data.get('financialData', {}).get('currentPrice', 0),
            data.get('financialData', {}).get('targetHighPrice', 0),
            data.get('financialData', {}).get('targetLowPrice', 0),
            data.get('financialData', {}).get('targetMeanPrice', 0),
            data.get('financialData', {}).get('recommendationKey', 'N/A'),
            data.get('financialData', {}).get('numberOfAnalystOpinions', 0),
            data.get('financialData', {}).get('totalCash', 0),
            data.get('financialData', {}).get('totalDebt', 0),
            data.get('financialData', {}).get('revenuePerShare', 0),
            data.get('financialData', {}).get('grossMargins', 0),
            data.get('financialData', {}).get('operatingMargins', 0),
            data.get('financialData', {}).get('netIncomeToCommon', 0),
            data.get('financialData', {}).get('totalRevenue', 0)
        )

        insert_query = f"""
            INSERT INTO financials ({', '.join(fields_to_insert)})
            VALUES ({', '.join(['%s'] * len(fields_to_insert))})
            ON CONFLICT (ticker) DO UPDATE SET 
            {', '.join([f"{field} = EXCLUDED.{field}" for field in fields_to_insert[1:]])}
        """

        cursor.execute(insert_query, insert_values)
        print(f"Inserted or updated data for {ticker}")

        conn.commit()

    cursor.close()
    conn.close()
