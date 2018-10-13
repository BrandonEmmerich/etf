QUERY_INSERT = '''INSERT INTO etf.etf_prices (ticker, run_id, date_close, price_close, ticker_timestamp_id) VALUES (%(ticker)s, %(run_id)s,%(date_close)s, %(price_close)s, %(ticker_timestamp_id)s)'''
QUERY_GET_DATA = '''SELECT DISTINCT ticker_timestamp_id FROM etf.etf_prices'''
QUERY_GET_TICKER = '''SELECT DISTINCT ticker FROM et.etf_prices'''

XPATH_ETF_TICKER = "//td[@class='eft_or_etn']/span/a/text()"
