import psycopg2
from datetime import datetime, timedelta
import threading
import schedule
import subprocess
import time
import pandas as pd
from prophet import Prophet
from datetime import datetime, timedelta
import os
import sys
import numpy as np
import MetaTrader5 as mt5
import pytz
from multiprocessing import Pool
from live_data import get_live_data, get_currency, get_db_conn
import logging


# mt5_path = r"C:\Program Files\Octa Markets MetaTrader 5\terminal64.exe"
# subprocess.Popen(mt5_path)

logging.basicConfig(filename='predictions.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

time_frame_parameters = {  # periods time.sleep mt5_data
    "1_min": [1, 30, 120, 60, 300, 5],
    "5_min": [5, 150, 36000, 300, 90000, 25],
    "15_min": [15, 450, 1800, 900, 4500, 75],
    "30_min": [30, 900, 3600, 1800, 9000, 150],
    "60_min": [60, 1800, 7200, 3600, 18000, 300],
    "240_min": [240, 7200, 28800, 14400, 72000, 1200]
}

currency = get_currency()


def wait_until_monday():
    today = datetime.now(tz=pytz.utc).date()
    if today.weekday() >= 5:  # 5 and 6 represent Saturday and Sunday
        days_until_monday = (7 - today.weekday()) % 7
        next_monday = datetime.now(tz=pytz.utc) + \
                      timedelta(days=days_until_monday)
        time_until_monday = (
                next_monday - datetime.now(tz=pytz.utc)).total_seconds()
        print("It is currently the weekend. Program will resume on Monday.")
        time.sleep(time_until_monday)
    else:
        print("It is not the weekend. Program will continue.")


def get_bid_data():
    live_bid, live_ask, get_time_ = get_live_data(currency)
    return live_bid, get_time_


def get_data(currency, len_mt5_data):
    try:
        conn = get_db_conn()
        cursor = conn.cursor()

        current_datetime = datetime.now()
        five_minutes_ago = current_datetime - timedelta(seconds=len_mt5_data)

        current_minute = current_datetime.minute

        # Adjust the current_datetime to exclude the current minute
        if current_minute > 0:
            current_datetime = current_datetime.replace(
                minute=current_minute - 1, second=59, microsecond=999999)
        else:
            current_datetime = current_datetime.replace(hour=current_datetime.hour - 1, minute=59, second=59,
                                                        microsecond=999999)

        postgres_select_query = "SELECT current_datetime, ask_value, bid_value FROM live_data_forex_tbl WHERE currency=%s AND current_datetime >= %s AND current_datetime <= %s ORDER BY current_datetime ASC"
        cursor.execute(postgres_select_query, (currency,
                                               five_minutes_ago, current_datetime))
        rows = cursor.fetchall()

        datetime_values = [row[0] for row in rows]
        ask_values = [row[1] for row in rows]
        bid_values = [row[2] for row in rows]

        return datetime_values, ask_values, bid_values
    except Exception as e:
        print("Error:", str(e))


def insert_or_update_forex_pred_tbl(values, column_name):
    # conn = psycopg2.connect(database="postgres", user='postgres', password='admin', host='127.0.0.1', port='5432')
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM forex_pred_ask_tbl")
        row_count = cursor.fetchone()[0]

        if row_count == 0:
            postgres_insert_query = f"INSERT INTO forex_pred_bid_tbl ({column_name}) VALUES (%s)"
            cursor.execute(postgres_insert_query, values)
            conn.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully for", column_name)
        else:
            # Update the existing value in the table
            postgres_update_query = f"UPDATE forex_pred_bid_tbl SET {column_name} = %s"
            cursor.execute(postgres_update_query, values)
            conn.commit()
            count = cursor.rowcount
            # print(count, "Record updated successfully for", column_name)
    except Exception as e:
        print("Error:", str(e))
    finally:
        cursor.close()
        conn.close()


def forex_pred_tbl(currency, interval, yhat_values, actuaL_high_value, prediction_hit, prediction_time,
                   last_ask_value_1_min, check_sell, check_strong_sell, check_bullish_sell, current_time_):
    values = [(currency), (interval), (yhat_values), (actuaL_high_value), (prediction_hit), (prediction_time),
              (last_ask_value_1_min), (check_sell), (check_strong_sell), (check_bullish_sell), (current_time_)]
    # conn = psycopg2.connect(database="postgres", user='postgres', password='admin', host='127.0.0.1', port='5432')
    conn = get_db_conn()
    try:
        cursor = conn.cursor()
        postgres_insert_query = f"INSERT INTO forex_pred_bid_logs_tbl (currency, interval, buy_prediction, actual_min_max_bid, actual_low_high_dt, prediction_dt, actual_bid, check_buy, check_strong_buy, check_bullish_buy, current_dt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(postgres_insert_query, values)
        conn.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully for", {
            interval}, "and its prediction value is", {yhat_values})
    except Exception as e:
        print("Error:", str(e))
    finally:
        cursor.close()
        conn.close()


def get_previous_prediction(interval):
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        postgres_insert_query = f"SELECT buy_prediction FROM forex_pred_bid_logs_tbl WHERE interval = '" + \
                                interval + "' ORDER BY prediction_dt DESC LIMIT 3"
        cursor.execute(postgres_insert_query)
        conn.commit()
        results = cursor.fetchall()
        previous_pred = results[0][0]
        print("results", results)
        is_increasing = all(results[i][0] <= results[i + 1][0] for i in range(len(results) - 1))
        
        return previous_pred, is_increasing

    except Exception as e:
        previous_pred = 0
        is_increasing = False
        print("Error in getting last 3 predictions:", str(e))
        
        return previous_pred, is_increasing


conn = get_db_conn()


def get_pedictions(currency, len_mt5_data, periods_, interval):
    # live_bid, get_datetime = get_bid_data()
    # print("live_bid", live_bid, get_datetime)
    curr_time = datetime.now()
    curr_time = curr_time.replace(microsecond=0)
    print("curr_time", curr_time)
    logging.info("curr_time %s", curr_time)

    datetime_, ask, bid = get_data(currency, len_mt5_data)
    df = pd.DataFrame({'datetime': datetime_, 'ask': ask, 'bid': bid})
    df['datetime'] = pd.to_datetime(df['datetime'])
    # df.to_csv("Check_data.csv")
    df = df.set_index('datetime')
    df = df.reset_index()
    df = df.rename(columns={'datetime': 'ds', 'bid': 'y'})
    print("Dataset for Training", df.shape)
    logging.info("Dataset for Training %s", df.shape)

    model = Prophet()
    start_time = datetime.now()
    model.fit(df)
    end_time = datetime.now()
    time_took_for_training = end_time - start_time
    time_diff_seconds = (end_time - start_time).total_seconds()
    print("time_diff_seconds for training", round(time_diff_seconds))
    logging.info("time_diff_seconds for training %s", round(time_diff_seconds))

    #TODO SAVE THE LIVE BID VALUE HERE BEFORE THE STARTIG THE PREDICTION  (PROGRAM START 10:56)
    live_bid, get_datetime = get_bid_data()
    print("live_bid", live_bid, get_datetime)
    logging.info("live_bid %s %s", live_bid, get_datetime)

    future = model.make_future_dataframe(periods=periods_, freq='S', include_history=False)
    predictions = model.predict(future)
    predictions['ds'] = pd.to_datetime(predictions['ds'])
    predictions.to_csv("Get_Max_min.csv")
    logging.info("predictions %s", predictions)
    max_value, max_value_date = predictions['yhat'].max(), predictions.loc[predictions['yhat']==predictions['yhat'].max(), 'ds'].values[0]
    min_value, min_value_date = predictions['yhat'].min(), predictions.loc[predictions['yhat']==predictions['yhat'].min(), 'ds'].values[0]
    print("GET MAXIMUM VALUE", max_value, max_value_date)
    print("GET MINIMUM VALUE", min_value, min_value_date)


    #TODO 11:05 Prediction Value 1.30924

    #TODO Determin buy or sell prediction buysell == 0 (BUY Prediction)
    # buysell == 1 (SELL Prediction), 
    # if live_bid_program_start (10:56) value > yhat_or_predicted_value (1.30924)
#                                       buysell = 1 
    # else:
    #    buysell = 0
    

    predictions = predictions.set_index('ds')
    predictions.index = pd.DatetimeIndex(predictions.index)

    current_time = datetime.now()
    current_time = current_time.replace(microsecond=0)
    print("Current Time", current_time, "Live Bid Value", live_bid)
    logging.info("Current Time %s Live Bid Value %s", current_time, live_bid)

    print("INTERVAL", interval)
    logging.info("INTERVAL %s", interval)

    if interval == '5_min':
        # print("GET MAXIMUM VALAUE", predictions['yhat'].max())
        # print("GET MINIMUM VALAUE", predictions['yhat'].min())

        minute_5_max_all = predictions.resample('5T').max()
        print(minute_5_max_all)
        current_time = datetime.now().replace(second=0, microsecond=0)
        print("current_time", current_time)

        minute_5_max = minute_5_max_all[minute_5_max_all.index > current_time]
        print("minute_5_max", minute_5_max)
        yhat_values_5Max = round(minute_5_max.loc[:, 'yhat'], 5)
        minute_5_max = minute_5_max_all[minute_5_max_all.index > current_time].iloc[[1]]
        print('yhat_values for 5 Min', yhat_values_5Max)
        logging.info("yhat_values for 5 Min %s", yhat_values_5Max)

        column_name = 'minutes_5'
        insert_or_update_forex_pred_tbl(yhat_values_5Max, column_name)

        minute_5_max = minute_5_max.reset_index()
        prediction_time = minute_5_max.loc[0, 'ds']
        yhat_values = round(minute_5_max.loc[:, 'yhat'].values[0], 5)

        prev_pred, is_increasing_ = get_previous_prediction(interval)
        print("5 minute", "yhat_values: ", yhat_values, "live_bid:", live_bid, "previous_pred:", prev_pred)


        buysell = 0    # BUY  PREDICTION
        # buysell = 1    # SELL PREDICTION,

        if live_bid > yhat_values:
            print("In line 235")
            buysell = 1
        else:
            print("In line 238")
            buysell = 0

        # if live_bid > yhat_values: #TODO and buysell = 0 
        #     print("Buy Prediction Successfull")

        # if yhat_values > live_bid: #TODO and buysell = 1 
        #     print("Sell Prediction Successfull")

        print("BUYSELL", buysell)
        logging.info("BUYSELL %s", buysell)

        prediction_hit = None
        
        # if live_bid > yhat_values and buysell == 0:
        #     print("Buy Prediction")
        #     prediction_hit = datetime.now()
        #     prediction_hit = prediction_hit.strftime("%Y-%m-%d %H:%M:%S")
        #     print("Prediction HIT on line 252")

        # if yhat_values > live_bid and buysell == 1:
        #     print("Sell Prediction")
        #     prediction_hit = datetime.now()
        #     prediction_hit = prediction_hit.strftime("%Y-%m-%d %H:%M:%S")
        #     print("Prediction HIT on line 258")


        last_bid_value_5_max = None
        last_bid_value_5_min = None

        datetime_, ask, bid = get_data(currency, len_mt5_data)
        df = pd.DataFrame({'datetime': datetime_, 'ask': ask, 'bid': bid})
        df['datetime'] = pd.to_datetime(df['datetime'])
        # df.to_csv("Check_data_5min.csv")

        if buysell == 0:
            last_bid_value_5_max = df.set_index('datetime')['bid'].resample('5T').max().tail(1).values[0]
            max_bid_values_per_5minute = df.set_index('datetime')['bid'].resample('5T').max().tail(1).index[0]
            print("last_bid_value_5_max", last_bid_value_5_max, max_bid_values_per_5minute)
            logging.info("last_bid_value_5_max %s %s", last_bid_value_5_max, max_bid_values_per_5minute)
            if last_bid_value_5_max >= yhat_values:
                prediction_hit = datetime.now()
                prediction_hit = prediction_hit.strftime("%Y-%m-%d %H:%M:%S")
                print("Prediction HIT for BUY")
                logging.info("Prediction HIT for BUY")
            else:
                print("Prediction Not Hit for BUY")
        
        if buysell == 1:
            last_bid_value_5_min = df.set_index('datetime')['bid'].resample('5T').min().tail(1).values[0]
            min_bid_values_per_5minute = df.set_index('datetime')['bid'].resample('5T').min().tail(1).index[0]
            print("last_bid_value_5_min", last_bid_value_5_min, min_bid_values_per_5minute)
            logging.info("last_bid_value_5_min %s %s", last_bid_value_5_min, min_bid_values_per_5minute)
            if last_bid_value_5_min <= yhat_values:
                prediction_hit = datetime.now()
                prediction_hit = prediction_hit.strftime("%Y-%m-%d %H:%M:%S")
                print("Prediction HIT for Sell")
                logging.info("Prediction HIT for Sell")
            else:
                print("Prediction Not Hit for Sell")
        else:
            prediction_hit = None
            print("Prediction Not HIT")
            logging.info("Prediction Not HIT")

        if live_bid > yhat_values and yhat_values > prev_pred:
            print("4th and 5th Condition", is_increasing_)
            logging.info("4th and 5th Condition %s", is_increasing_)
            check_buy = "Buy"
            check_strong_buy = None
            check_bullish_buy = None
            if yhat_values > prev_pred and is_increasing_ is True:
                check_strong_buy = "Strong Buy"
                print("In 5th and 6th Condiotion")
                logging.info("In 5th and 6th Condition")

            if live_bid > yhat_values and yhat_values > prev_pred and is_increasing_ is True:
                check_bullish_buy = "Bullish Buy"
                print("4th, 5th and 6th Condition")
                logging.info("4th, 5th and 6th Condition")

        else:
            check_buy = None
            check_strong_buy = None
            check_bullish_buy = None

        if buysell == 1:
            forex_pred_tbl(currency, interval, yhat_values, last_bid_value_5_min, prediction_hit, prediction_time,live_bid,
                           check_buy, check_strong_buy, check_bullish_buy, curr_time)
        elif buysell == 0:
            forex_pred_tbl(currency, interval, yhat_values, last_bid_value_5_max, prediction_hit, prediction_time,
                           live_bid,
                           check_buy, check_strong_buy, check_bullish_buy, curr_time)
        else:
            forex_pred_tbl(currency, interval, yhat_values, None, prediction_hit, prediction_time, live_bid,
                           check_buy, check_strong_buy, check_bullish_buy, curr_time)


min_1_param = time_frame_parameters["1_min"]
min_5_param = time_frame_parameters["5_min"]


def run_predictions_1_min():
    get_pedictions(currency, min_1_param[4], min_1_param[2], '1_min')


def run_predictions_5_min():
    get_pedictions(currency, min_5_param[4], min_5_param[2], '5_min')


# while True:
run_predictions_5_min()
#


# if __name__ == "__main__":
#     # wait_until_monday()
#
#     pool = Pool()
#     schedule.every(1).seconds.do(pool.apply_async, get_bid_data)
#     # schedule.every(1).minutes.at(":00").do(pool.apply_async, run_predictions_1_min)
#     schedule.every(5).minutes.do(pool.apply_async, run_predictions_5_min)
#     # schedule.every(30).seconds.do(pool.apply_async, run_predictions_1_min)
#
#     while True:
#         schedule.run_pending()
#         time.sleep(1)
