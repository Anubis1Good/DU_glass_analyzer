import os
import pandas as pd
import numpy as np
import traceback
from time import sleep
from datetime import datetime, timedelta
from libs.QuikPy import QuikPy
from utils.table_funcs import get_table_ticks
from tabulate import tabulate

# ANSI цветовые коды
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def color_tick(value):
    """Окрашивает положительные значения тиков в желтый цвет"""
    try:
        if isinstance(value, (int, float)) and value > 0:
            return f"{Colors.YELLOW}{value}{Colors.END}"
        return str(value)
    except:
        return str(value)

def drop_rank_if_exists(df):
    if df is not None and not df.empty and 'rank' in df.columns:
        return df.drop('rank', axis=1)
    return df

def parse_datetime(dt_dict):
    return datetime(
        year=dt_dict['year'],
        month=dt_dict['month'],
        day=dt_dict['day'],
        hour=dt_dict['hour'],
        minute=dt_dict['min'],
        second=dt_dict['sec'],
        microsecond=dt_dict['ms'] * 1000
    )

def format_table_with_colors(df, tablefmt='grid'):
    """Форматирует таблицу с цветным выделением положительных тиков"""
    if df is None or df.empty:
        return tabulate([], headers=[], tablefmt=tablefmt, showindex=False)
    
    # Создаем копию с цветными значениями
    colored_df = df.copy()
    
    # Применяем цвет к столбцу tick (если он существует)
    if 'tick' in colored_df.columns:
        colored_df['tick'] = colored_df['tick'].apply(color_tick)
    
    return tabulate(colored_df, headers='keys', tablefmt=tablefmt, showindex=False)

def safe_convert_dataframe(df):
    """Безопасно приводит типы данных в датафрейме"""
    if df is None or df.empty:
        return df
    
    try:
        # Создаем копию
        df = df.copy()
        
        # Приводим sec_code к строковому типу
        if 'sec_code' in df.columns:
            df['sec_code'] = df['sec_code'].astype(str)
        
        # Приводим числовые колонки
        if 'flags' in df.columns:
            df['flags'] = pd.to_numeric(df['flags'], errors='coerce').fillna(0).astype('int64')
        
        if 'qty' in df.columns:
            df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0).astype('float64')
        
        if 'tick' in df.columns:
            df['tick'] = pd.to_numeric(df['tick'], errors='coerce').fillna(0).astype('float64')
        
        return df
    except Exception as e:
        print(f"Ошибка при преобразовании типов: {e}")
        return pd.DataFrame()  # Возвращаем пустой датафрейм в случае ошибки

qp_provider = QuikPy()
df = pd.DataFrame(columns=['flags', 'datetime', 'sec_code', 'qty', 'tick'])

def on_tick(tick):
    global df
    tick = tick['data']
    
    try:
        # Вычисляем tick как скалярное значение
        tick_value = -tick['qty'] if tick['flags'] == 1025 else tick['qty']
        
        new_row = {
            'datetime': parse_datetime(tick['datetime']),
            'flags': tick['flags'],
            'qty': tick['qty'],
            'sec_code': tick['sec_code'],
            'tick': tick_value
        }
        
        new_df_row = pd.DataFrame([new_row])
        df = pd.concat([df, new_df_row], ignore_index=True)
        
        # Приводим типы данных
        if not df.empty:
            if 'flags' in df.columns:
                df['flags'] = df['flags'].astype('int64')
            if 'qty' in df.columns:
                df['qty'] = df['qty'].astype('float64')
            if 'tick' in df.columns:
                df['tick'] = df['tick'].astype('float64')
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Оставляем только последние 5 минут
        if not df.empty and 'datetime' in df.columns:
            last_time = df['datetime'].max()
            time_threshold = last_time - timedelta(minutes=5)
            df = df[df['datetime'] >= time_threshold].copy()
    
    except Exception as e:
        print(f"Ошибка в on_tick: {e}")
        traceback.print_exc()

qp_provider.on_all_trade = on_tick

tickers_stock = ('GAZP','SBER','SBERP','LKOH','YDEX','OZON','PHOR','AFLT','MTSS','PIKK')
tickers_stock2 = ('T','VTBR','NVTK','TRNFP','ROSN','TATN','SIBN','SNGSP','IRAO','AFKS')
tickers_stock3 = ('CHMF','NLMK','MAGN','MGNT','X5','GMKN','RUAL','MTLR','PLZL','ALRS')
tickers_currency = ('CNYRUBF','CNYRUB_TOM','CNY000000TOD','SiM6','CRM6','USDRUBF')

TOP_N = 2
COUNT_THRESHOLD = 5

try:
    while True:
        try:
            # Создаем копии с проверкой наличия данных
            df1 = df[df['sec_code'].isin(tickers_stock)].copy() if not df.empty else pd.DataFrame()
            df2 = df[df['sec_code'].isin(tickers_stock2)].copy() if not df.empty else pd.DataFrame()
            df3 = df[df['sec_code'].isin(tickers_stock3)].copy() if not df.empty else pd.DataFrame()
            df5 = df[df['sec_code'].isin(tickers_currency)].copy() if not df.empty else pd.DataFrame()
            
            # Безопасно приводим типы для всех датафреймов
            df1 = safe_convert_dataframe(df1)
            df2 = safe_convert_dataframe(df2)
            df3 = safe_convert_dataframe(df3)
            df5 = safe_convert_dataframe(df5)
            
            tt1 = get_table_ticks(df1, COUNT_THRESHOLD, TOP_N) if not df1.empty else pd.DataFrame()
            tt2 = get_table_ticks(df2, COUNT_THRESHOLD, TOP_N) if not df2.empty else pd.DataFrame()
            tt3 = get_table_ticks(df3, COUNT_THRESHOLD, TOP_N) if not df3.empty else pd.DataFrame()
            tt5 = get_table_ticks(df5, COUNT_THRESHOLD, 4) if not df5.empty else pd.DataFrame()
            
            tables = []
            table_names = ['STOCKS', 'STOCKS2', 'STOCKS3', 'CURRENCY']
            dfs_result = [tt1, tt2, tt3, tt5]
            
            for name, df_ in zip(table_names, dfs_result):
                df_clean = drop_rank_if_exists(df_)
                # Форматируем таблицу с цветами
                if df_clean is None or df_clean.empty:
                    # Проверяем, есть ли колонки в исходном датафрейме
                    if df_ is not None and not df_.empty:
                        headers = [col for col in df_.columns if col != 'rank']
                        table_str = tabulate([], headers=headers, tablefmt='grid', showindex=False)
                    else:
                        table_str = tabulate([], headers=[], tablefmt='grid', showindex=False)
                else:
                    table_str = format_table_with_colors(df_clean, 'grid')
                
                tables.append((name, table_str.split('\n')))
            
            # Находим максимальную высоту
            if tables:
                max_height = max(len(table_lines) for _, table_lines in tables)
                
                # Очищаем консоль
                os.system('cls' if os.name == 'nt' else 'clear')
                
                # Выводим время с цветом
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(f"\n{Colors.BOLD}{Colors.CYAN}Last update: {current_time}{Colors.END}")
                print(f"{Colors.BOLD}{'='*120}{Colors.END}")
                
                # Выводим заголовки таблиц с цветом
                header_parts = [f"{Colors.BOLD}{Colors.GREEN}{name:^30}{Colors.END}" for name in table_names]
                print(' | '.join(header_parts))
                print(f"{Colors.BOLD}{'-'*120}{Colors.END}")
                
                # Выводим таблицы построчно
                for i in range(max_height):
                    row_parts = []
                    for _, table_lines in tables:
                        if i < len(table_lines):
                            row_parts.append(table_lines[i])
                        else:
                            row_parts.append(' ' * (len(table_lines[0]) if table_lines else 0))
                    print(' | '.join(row_parts))
                
                print(f"{Colors.BOLD}{'='*120}{Colors.END}")
                print(f"{Colors.YELLOW}Note: {Colors.END}Yellow values = Positive ticks")
            
            sleep(30)
            
        except Exception as e:
            print(f"Ошибка в основном цикле: {e}")
            traceback.print_exc()
            sleep(5)  # Пауза перед следующей итерацией
            
except Exception:
    traceback.print_exc()
    qp_provider.close_connection_and_thread()