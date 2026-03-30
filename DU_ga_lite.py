import os
import pandas as pd
import numpy as np
import traceback
from time import sleep
from datetime import datetime, timedelta
from libs.QuikPy import QuikPy
from utils.table_funcs import get_table_ticks
from tabulate import tabulate

# Функция для преобразования датафрейма в строку таблицы
def df_to_table_str(df, name):
    if 'rank' in df.columns:
        df = df.drop('rank', axis=1)
    return f"\n{name}:\n{tabulate(df, headers='keys', tablefmt='grid', showindex=False)}"

def drop_rank_if_exists(df):
    if 'rank' in df.columns:
        return df.drop('rank', axis=1)
    return df

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
    
    # Безопасное преобразование типов
    df['flags'] = pd.to_numeric(df['flags'], errors='coerce').fillna(0).astype('int64')
    df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0).astype('float64')
    df['tick'] = pd.to_numeric(df['tick'], errors='coerce').fillna(0).astype('float64')
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    last_time = df['datetime'].max()
    time_threshold = last_time - timedelta(minutes=5)
    df = df[df['datetime'] >= time_threshold].copy()

qp_provider.on_all_trade = on_tick

tickers_stock = ('GAZP','SBER','SBERP','LKOH','YDEX','OZON','PHOR','AFLT','MTSS','PIKK')
tickers_stock2 = ('T','VTBR','NVTK','TRNFP','ROSN','TATN','SIBN','SNGSP','IRAO','AFKS')
tickers_stock3 = ('CHMF','NLMK','MAGN','MGNT','X5','GMKN','RUAL','MTLR','PLZL','ALRS')
tickers_currency = ('CNYRUBF','CNYRUB_TOM','CNYRUB_TOD','SiM6','CRM6','USDRUBF')

TOP_N = 2
COUNT_THRESHOLD = 5

try:
    while True:
        df1 = df[df['sec_code'].isin(tickers_stock)].copy()
        df2 = df[df['sec_code'].isin(tickers_stock2)].copy()
        df3 = df[df['sec_code'].isin(tickers_stock3)].copy()
        df5 = df[df['sec_code'].isin(tickers_currency)].copy()

        tt1 = get_table_ticks(df1, COUNT_THRESHOLD, TOP_N)
        tt2 = get_table_ticks(df2, COUNT_THRESHOLD, TOP_N)
        tt3 = get_table_ticks(df3, COUNT_THRESHOLD, TOP_N)
        tt5 = get_table_ticks(df5, COUNT_THRESHOLD, 4)
        
        tables = []
        table_names = ['tt1', 'tt2', 'tt3', 'tt5']
        dfs = [tt1, tt2, tt3, tt5]

        for name, df_ in zip(table_names, dfs):
            df_clean = drop_rank_if_exists(df_)
            # Проверяем, что датафрейм не пустой
            if df_clean.empty:
                # Создаем пустую таблицу с заголовками
                table_str = tabulate([], headers=df_clean.columns, tablefmt='grid', showindex=False)
            else:
                table_str = tabulate(df_clean, headers='keys', tablefmt='grid', showindex=False)
            tables.append((name, table_str.split('\n')))
        
        # Находим максимальную высоту - ИСПРАВЛЕНО: берем длину table_lines, а не всего кортежа
        max_height = max(len(table_lines) for _, table_lines in tables)
        
        # Очищаем консоль (раскомментируйте если нужно)
        os.system('cls')
        
        # Выводим время
        print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 120)
        
        # Выводим заголовки таблиц
        header_parts = [f"{name:^30}" for name in table_names]
        print(' | '.join(header_parts))
        print("-" * 120)
        
        # Выводим таблицы построчно
        for i in range(max_height):
            row_parts = []
            for _, table_lines in tables:
                if i < len(table_lines):
                    row_parts.append(table_lines[i])
                else:
                    # Добавляем пустую строку нужной длины
                    row_parts.append(' ' * (len(table_lines[0]) if table_lines else 0))
            print(' | '.join(row_parts))
        
        print("=" * 120)
        sleep(10)
        
except Exception:
    traceback.print_exc()
    qp_provider.close_connection_and_thread()