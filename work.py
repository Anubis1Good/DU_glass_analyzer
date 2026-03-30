import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime, timedelta
from libs.QuikPy import QuikPy

def parse_datetime(dt_dict):
    # dt_dict = eval(dt_str)
    return datetime(
        year=dt_dict['year'],
        month=dt_dict['month'],
        day=dt_dict['day'],
        hour=dt_dict['hour'],
        minute=dt_dict['min'],
        second=dt_dict['sec'],
        microsecond=dt_dict['ms'] * 1000
    )

qp_provider = QuikPy()
df = pd.DataFrame(columns=['flags','datetime','sec_code','qty','tick'])

# ticks = qp_provider.get_all_trade()
# df = pd.DataFrame(ticks['data'])
# df = df.drop(['class_code','exec_market','repoterm','tradenum','yield','trade_num','value','period','benchmark','reporate','exchange_code','open_interest','accruedint','repo2value','repovalue','settlecode','price','seccode'],axis=1)
# df['datetime'] = df['datetime'].apply(parse_datetime)
# df['tick'] = np.where(df['flags'] == 1025, -df['qty'], df['qty'])
# last_time = df['datetime'].max()
# time_threshold = last_time - timedelta(hours=1)
# df = df[df['datetime'] >= time_threshold].copy()

def on_tick(tick):
    global df
    tick = tick['data']
    new_row = {
        'datetime': parse_datetime(tick['datetime']),
        'flags': tick['flags'],
        'qty': tick['qty'],
        'sec_code': tick['sec_code'],
        'tick': np.where(tick['flags'] == 1025, -tick['qty'], tick['qty'])
    }
    new_df_row = pd.DataFrame([new_row])
    df = pd.concat([df, new_df_row], ignore_index=True)
    df['flags'] = df['flags'].astype('int64')
    df['qty'] = df['qty'].astype('float64')
    df['tick'] = df['tick'].astype('float64')
    df['datetime'] = pd.to_datetime(df['datetime'])

qp_provider.on_all_trade = on_tick

try:
    while True:
        df.info()
        print(df.tail())
        sleep(5)
except:
    qp_provider.close_connection_and_thread() 