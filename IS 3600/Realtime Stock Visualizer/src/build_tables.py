import psycopg2 as psql


def build_tables(DB_CONFIG):
    conn = psql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS data (
    	data_id serial4 NOT NULL,
    	trans_date timestamptz NULL,
    	ticker text NULL,
    	"open" float8 NULL,
    	high float8 NULL,
    	low float8 NULL,
    	"close" float8 NULL,
    	adj_close float8 NULL,
    	volume float8 NULL,
    	ingestion_time timestamptz DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'::text) NULL,
        PRIMARY KEY (trans_date, Ticker)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS etf_holdings (
        etf_symbol TEXT,
        symbol TEXT,
        description TEXT,
        portfolio_weight TEXT,
        shares_held TEXT,
        market_value TEXT,
        PRIMARY KEY (etf_symbol, symbol),  -- Ensures uniqueness for etf_symbol and symbol
        CONSTRAINT unique_etf_symbol_symbol UNIQUE (etf_symbol, symbol)  -- Explicit UNIQUE constraint
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS etf_summary_details (
    	etf_symbol text NOT NULL,
    	regular_market_price numeric NULL,
    	currency text NULL,
    	market_cap int8 NULL,
    	yield numeric NULL,
    	expense_ratio numeric NULL,
    	fifty_two_week_low numeric NULL,
    	fifty_two_week_high numeric NULL,
    	fifty_day_average numeric NULL,
    	two_hundred_day_average numeric NULL,
    	trailing_annual_dividend_rate numeric NULL,
    	trailing_annual_dividend_yield numeric NULL,
    	nav_price numeric NULL,
    	previous_close numeric NULL,
    	open_price numeric NULL,
    	day_low numeric NULL,
    	day_high numeric NULL,
    	volume int8 NULL,
    	CONSTRAINT etf_summary_details_pkey PRIMARY KEY (etf_symbol)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tickers (
    	ticker text NOT NULL,
    	companyname text NULL,
    	sector text NULL,
    	subsector text NULL,
    	headquarterslocation text NULL,
    	"type" text NULL,
    	CONSTRAINT tickers_pkey PRIMARY KEY (ticker)
    );"""
    )
    cursor.execute("""
    CREATE TABLE if not exists financials (
    	ticker VARCHAR(10) PRIMARY KEY,
    	company_name VARCHAR(100),
    	sector VARCHAR(50),
    	industry VARCHAR(100),
    	market_cap BIGINT,
    	dividend_yield FLOAT,
    	pe_ratio FLOAT,
    	forward_pe FLOAT,
    	beta FLOAT,
    	earnings_growth FLOAT,
    	revenue_growth FLOAT,
    	return_on_assets FLOAT,
    	return_on_equity FLOAT,
    	current_price FLOAT,
    	target_high_price FLOAT,
    	target_low_price FLOAT,
    	target_mean_price FLOAT,
    	recommendation_key VARCHAR(20),
    	number_of_analyst_opinions INT,
    	total_cash BIGINT,
    	total_debt BIGINT,
    	revenue_per_share FLOAT,
    	gross_margins FLOAT,
    	operating_margins FLOAT,
    	net_income_to_common BIGINT,
    	total_revenue BIGINT);"""
		)


    conn.commit()
    conn.close()

    print("All tables created successfully!")
