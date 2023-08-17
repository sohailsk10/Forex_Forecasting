import MetaTrader5 as mt5
from datetime import datetime, timedelta, time
from live_data import get_db_conn, get_currency

# currency = "LTCUSD"
currency = get_currency()
# def get_currency():
#     currency = currency_
#     return currency


def truncate_database():
    try:
        # conn = psycopg2.connect(database="postgres", user='postgres', password='admin', host='127.0.0.1', port='5432')
        conn = get_db_conn()
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE live_data_forex_tbl")
        conn.commit()
        print("Database truncated successfully at", datetime.now())
    except Exception as e:
        print("Error:", str(e))
    finally:
        cursor.close()
        conn.close()

def live_forex_data_tbl(currency, bid, ask, get_current_date,):
    values = [(bid, ), (ask, ), (get_current_date, ),(currency, )]
    try:
        # conn = psycopg2.connect(database="postgres", user='postgres', password='admin', host='127.0.0.1', port='5432')
        conn = get_db_conn()
        cursor = conn.cursor()
        postgres_insert_query = f"INSERT INTO live_data_forex_tbl (current_datetime, bid_value, ask_value, currency) VALUES (%s, %s, %s, %s)"
        cursor.execute(postgres_insert_query, values)
        conn.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully for", {bid}, "and", {ask}, "at time", {get_current_date})
    except Exception as e:
        print("Error:", str(e))
    finally:
        cursor.close()
        conn.close()



def get_live_data(currency):
    try:
        if not mt5.initialize():
            print("initialize() failed, error code =", mt5.last_error())
            return None
        else:
            bid = round(mt5.symbol_info_tick(currency).bid, 5)
            ask = round(mt5.symbol_info_tick(currency).ask, 5)
            get_current_date = datetime.now()
            live_forex_data_tbl(currency, get_current_date, bid, ask)

            return get_current_date, bid, ask
    except:
        pass


while True:
    current_time = datetime.now().time()
    # print("current_time", current_time)
    # if current_time >= time(0, 0) and current_time < time(0, 1):
    #     truncate_database()
    get_live_data(currency)