import private
import psycopg2
import settings
import time
import web

run_id = int(time.time())
conn = psycopg2.connect(private.DATABASE_CONNECTION_STRING)
tickers = web.get_tickers_query(conn)

list_of_ids = web.get_existing_data(conn)

for ticker in tickers:
    try:
        web.get_data_and_write_to_db(ticker, list_of_ids, run_id, conn)

    except Exception as e:
        conn.rollback()
        print e
