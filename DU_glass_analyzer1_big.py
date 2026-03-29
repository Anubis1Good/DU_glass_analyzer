import os
from time import sleep
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tabulate import tabulate
from utils.quik_funcs import get_all_ticks




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

def get_table_for_period(df, minutes, count_threshold=2, top_n=5):
    last_time = df['datetime'].max()
    time_threshold = last_time - timedelta(minutes=minutes)
    df_filtered = df[df['datetime'] >= time_threshold].copy()
    
    algos = (
        df_filtered.groupby(['sec_code', 'tick'])
        .size()
        .reset_index(name='count')
    )
    algos['volume'] = abs(algos['tick'] * algos['count'])
    algos_filtered = algos[algos['count'] > count_threshold].copy()
    
    result = (
        algos_filtered
        .sort_values(['sec_code', 'volume'], ascending=[True, False])
        .groupby('sec_code')
        .head(top_n)
        .reset_index(drop=True)
    )
    
    # Добавляем ранг внутри каждого sec_code
    if not result.empty:
        result['rank'] = result.groupby('sec_code').cumcount() + 1
    
    return result

def align_tables_by_sec_code(table1, table2, top_n=5):
    """
    Выравнивает две таблицы по sec_code, создавая ровно top_n строк для каждого sec_code
    """
    # Получаем все уникальные sec_code из обеих таблиц
    all_codes = set()
    if not table1.empty:
        all_codes.update(table1['sec_code'].unique())
    if not table2.empty:
        all_codes.update(table2['sec_code'].unique())
    
    all_codes = sorted(list(all_codes))
    
    # Функция для расширения таблицы до top_n строк на каждый sec_code
    def expand_table_to_full_ranks(table, all_codes, top_n):
        if table.empty:
            # Создаем пустую таблицу с нужными колонками и рангами
            expanded_rows = []
            for code in all_codes:
                for rank in range(1, top_n + 1):
                    expanded_rows.append({
                        'sec_code': code,
                        'rank': rank,
                        'tick': np.nan,
                        'count': np.nan,
                        'volume': np.nan
                    })
            return pd.DataFrame(expanded_rows)
        
        expanded_rows = []
        for code in all_codes:
            code_data = table[table['sec_code'] == code].copy()
            
            # Добавляем все существующие строки для этого sec_code
            for _, row in code_data.iterrows():
                expanded_rows.append(row.to_dict())
            
            # Добавляем недостающие строки до top_n
            existing_ranks = set(code_data['rank']) if 'rank' in code_data.columns else set()
            for rank in range(1, top_n + 1):
                if rank not in existing_ranks:
                    expanded_rows.append({
                        'sec_code': code,
                        'rank': rank,
                        'tick': np.nan,
                        'count': np.nan,
                        'volume': np.nan
                    })
        
        result_df = pd.DataFrame(expanded_rows)
        if 'rank' in result_df.columns:
            result_df = result_df.sort_values(['sec_code', 'rank']).reset_index(drop=True)
        return result_df
    
    # Расширяем обе таблицы
    table1_expanded = expand_table_to_full_ranks(table1, all_codes, top_n)
    table2_expanded = expand_table_to_full_ranks(table2, all_codes, top_n)
    
    return table1_expanded, table2_expanded

def align_multiple_tables(tables, top_n=5):
    """
    Выравнивает несколько таблиц по sec_code и рангам, добавляя пустые строки для недостающих рангов
    
    Args:
        tables: Список таблиц (DataFrame)
        top_n: Количество строк на каждый sec_code
    
    Returns:
        Список выровненных таблиц
    """
    # Собираем все уникальные sec_code из всех таблиц
    all_codes = set()
    for table in tables:
        if not table.empty:
            all_codes.update(table['sec_code'].unique())
    all_codes = sorted(list(all_codes))
    
    # Функция для расширения одной таблицы до полной структуры
    def expand_table_to_full(table, all_codes, top_n):
        if table.empty:
            # Создаем полностью пустую таблицу
            expanded_rows = []
            for code in all_codes:
                for rank in range(1, top_n + 1):
                    expanded_rows.append({
                        'sec_code': code,
                        'rank': rank,
                        'tick': np.nan,
                        'count': np.nan,
                        'volume': np.nan
                    })
            return pd.DataFrame(expanded_rows)
        
        expanded_rows = []
        for code in all_codes:
            code_data = table[table['sec_code'] == code].copy()
            
            # Убеждаемся, что есть колонка rank
            if 'rank' not in code_data.columns:
                # Если нет rank, создаем его
                code_data = code_data.sort_values('volume', ascending=False).reset_index(drop=True)
                code_data['rank'] = code_data.index + 1
            
            # Получаем существующие ранги
            existing_ranks = set(code_data['rank'])
            
            # Добавляем существующие строки
            for _, row in code_data.iterrows():
                expanded_rows.append(row.to_dict())
            
            # Добавляем недостающие ранги
            for rank in range(1, top_n + 1):
                if rank not in existing_ranks:
                    expanded_rows.append({
                        'sec_code': code,
                        'rank': rank,
                        'tick': np.nan,
                        'count': np.nan,
                        'volume': np.nan
                    })
        
        result_df = pd.DataFrame(expanded_rows)
        result_df = result_df.sort_values(['sec_code', 'rank']).reset_index(drop=True)
        return result_df
    
    # Расширяем все таблицы
    aligned_tables = []
    for table in tables:
        aligned_tables.append(expand_table_to_full(table, all_codes, top_n))
    
    return aligned_tables

def print_aligned_tables(*tables, titles=None, top_n=5, groups=None):
    """
    Печатает несколько таблиц рядом с выравниванием по sec_code и рангам
    
    Args:
        *tables: Переменное количество таблиц (DataFrame)
        titles: Список заголовков для каждой таблицы (если None, будут использованы названия по умолчанию)
        top_n: Количество строк на каждый sec_code
        groups: Список групп, указывающий, какие таблицы выравнивать вместе
                Например: groups=[[0,1,2], [3,4]] - первые 3 таблицы в одной группе, следующие 2 в другой
    """
    if not tables:
        print("No tables to display")
        return
    
    num_tables = len(tables)
    
    # Создаем заголовки по умолчанию, если не указаны
    if titles is None:
        titles = [f"TABLE {i+1}" for i in range(num_tables)]
    elif len(titles) != num_tables:
        raise ValueError(f"Number of titles ({len(titles)}) must match number of tables ({num_tables})")
    
    # Если группы не указаны, считаем все таблицы одной группой
    if groups is None:
        groups = [list(range(num_tables))]
    
    # Функция для форматирования таблицы
    def format_table(table):
        if table.empty:
            return ["No data available"]
        
        table_display = table.copy()
        
        # Убираем колонку rank из отображения
        if 'rank' in table_display.columns:
            table_display = table_display.drop(columns=['rank'])
        
        # Заменяем NaN на пустые строки
        table_display = table_display.fillna('')
        
        # Форматируем числовые значения
        for col in ['tick', 'count', 'volume']:
            if col in table_display.columns:
                table_display[col] = table_display[col].apply(
                    lambda x: f"{int(x):,}" if isinstance(x, (int, float)) and not pd.isna(x) and x == int(x) 
                    else (f"{x:,.1f}" if isinstance(x, (int, float)) and not pd.isna(x) else '')
                )
        
        return tabulate(
            table_display,
            headers='keys',
            tablefmt='grid',
            showindex=False,
            numalign='right',
            stralign='right'
        ).split('\n')
    
    # Выравниваем таблицы по группам
    aligned_tables = [None] * num_tables
    
    for group in groups:
        group_tables = [tables[i] for i in group]
        group_aligned = align_multiple_tables(group_tables, top_n)
        for idx, table in zip(group, group_aligned):
            aligned_tables[idx] = table
    
    # Получаем строковое представление всех таблиц
    table_lines = []
    max_widths = []
    
    for i, table in enumerate(aligned_tables):
        lines = format_table(table)
        table_lines.append(lines)
        max_widths.append(max(len(line) for line in lines) if lines else 0)
    
    # Определяем максимальную ширину для всех таблиц
    target_width = max(max_widths) if max_widths else 0
    
    # Выводим заголовки
    total_width = target_width * num_tables + 3 * (num_tables - 1) + 2
    print("\n" + "=" * total_width)
    
    # Формируем строку заголовков
    header_parts = []
    for i, title in enumerate(titles):
        header_parts.append(title.center(target_width))
    print(" | ".join(header_parts))
    print("-" * total_width)
    
    # Выводим строки
    max_lines = max(len(lines) for lines in table_lines) if table_lines else 0
    
    for line_idx in range(max_lines):
        row_parts = []
        for table_idx, lines in enumerate(table_lines):
            if line_idx < len(lines):
                line = lines[line_idx]
                # Выравниваем строку до целевой ширины
                if len(line) < target_width:
                    row_parts.append(line.ljust(target_width))
                else:
                    row_parts.append(line[:target_width])
            else:
                # Пустая строка
                row_parts.append(" " * target_width)
        
        # Проверяем, является ли строка разделителем
        is_separator = any(part.strip().startswith('+') or part.strip().startswith('|-') for part in row_parts)
        
        if is_separator:
            # Для разделителей создаем единую линию
            separator_parts = []
            for part in row_parts:
                if part.strip().startswith('+'):
                    separator_parts.append('+' + '-' * (target_width - 2) + '+')
                elif part.strip().startswith('|-'):
                    separator_parts.append('|' + '-' * (target_width - 2) + '|')
                else:
                    separator_parts.append(' ' * target_width)
            print(" | ".join(separator_parts))
        else:
            # Обычные строки
            print(" | ".join(row_parts))
    
    print("=" * total_width)

# Также нужно обновить align_multiple_tables, чтобы она работала с отдельными группами
def align_multiple_tables(tables, top_n=5):
    """
    Выравнивает несколько таблиц по sec_code и рангам, добавляя пустые строки для недостающих рангов
    
    Args:
        tables: Список таблиц (DataFrame)
        top_n: Количество строк на каждый sec_code
    
    Returns:
        Список выровненных таблиц
    """
    # Собираем все уникальные sec_code из всех таблиц
    all_codes = set()
    for table in tables:
        if not table.empty:
            all_codes.update(table['sec_code'].unique())
    all_codes = sorted(list(all_codes))
    
    # Функция для расширения одной таблицы до полной структуры
    def expand_table_to_full(table, all_codes, top_n):
        if table.empty:
            # Создаем полностью пустую таблицу
            expanded_rows = []
            for code in all_codes:
                for rank in range(1, top_n + 1):
                    expanded_rows.append({
                        'sec_code': code,
                        'rank': rank,
                        'tick': np.nan,
                        'count': np.nan,
                        'volume': np.nan
                    })
            return pd.DataFrame(expanded_rows)
        
        expanded_rows = []
        for code in all_codes:
            code_data = table[table['sec_code'] == code].copy()
            
            # Убеждаемся, что есть колонка rank
            if 'rank' not in code_data.columns:
                # Если нет rank, создаем его
                code_data = code_data.sort_values('volume', ascending=False).reset_index(drop=True)
                code_data['rank'] = code_data.index + 1
            
            # Получаем существующие ранги
            existing_ranks = set(code_data['rank'])
            
            # Добавляем существующие строки
            for _, row in code_data.iterrows():
                expanded_rows.append(row.to_dict())
            
            # Добавляем недостающие ранги
            for rank in range(1, top_n + 1):
                if rank not in existing_ranks:
                    expanded_rows.append({
                        'sec_code': code,
                        'rank': rank,
                        'tick': np.nan,
                        'count': np.nan,
                        'volume': np.nan
                    })
        
        result_df = pd.DataFrame(expanded_rows)
        result_df = result_df.sort_values(['sec_code', 'rank']).reset_index(drop=True)
        return result_df
    
    # Расширяем все таблицы
    aligned_tables = []
    for table in tables:
        aligned_tables.append(expand_table_to_full(table, all_codes, top_n))
    
    return aligned_tables

def get_table_for_period(df, minutes, count_threshold=2, top_n=5):
    """
    Возвращает DataFrame с топ N тиков по объему за последние X минут
    """
    last_time = df['datetime'].max()
    time_threshold = last_time - timedelta(minutes=minutes)
    df_filtered = df[df['datetime'] >= time_threshold].copy()
    
    algos = (
        df_filtered.groupby(['sec_code', 'tick'])
        .size()
        .reset_index(name='count')
    )
    algos['volume'] = abs(algos['tick'] * algos['count'])
    algos_filtered = algos[algos['count'] > count_threshold].copy()
    
    result = (
        algos_filtered
        .sort_values(['sec_code', 'volume'], ascending=[True, False])
        .groupby('sec_code')
        .head(top_n)
        .reset_index(drop=True)
    )
    
    # Добавляем ранг внутри каждого sec_code
    if not result.empty:
        result['rank'] = result.groupby('sec_code').cumcount() + 1
    
    return result



# Загрузка и подготовка данных
TOP_N = 3
COUNT_THRESHOLD = 5
tickers_stock = ('GAZP','SBER','SBERP','LKOH','YDEX','OZON','PHOR','AFLT','MTSS','PIKK')
tickers_stock2 = ('T','VTBR','NVTK','TRNFP','ROSN','TATN','SIBN','SNGSP','IRAO','AFKS')
tickers_stock3 = ('CHMF','NLMK','MAGN','MGNT','X5','GMKN','RUAL','MTLR','PLZL','ALRS')
tickers_fut = ('IMOEXF','MMM6','BMK6','S1M6','GLDRUBF')
tickers_currency = ('CNYRUBF','CNYRUB_TOM','CNYRUB_TOD','SiM6','CRM6')
# tickers_stock = ('GAZP','SBER','LKOH','YDEX')
# tickers_currency = ('IMOEXF','MMM6')


while True:
    ticks = get_all_ticks()
    df = pd.DataFrame(ticks['data'])
    df['datetime'] = df['datetime'].apply(parse_datetime)
    df['tick'] = np.where(df['flags'] == 1025, -df['qty'], df['qty'])
    df1 = df[df['sec_code'].isin(tickers_stock)].copy()
    df2 = df[df['sec_code'].isin(tickers_stock2)].copy()
    df3 = df[df['sec_code'].isin(tickers_stock3)].copy()
    df4 = df[df['sec_code'].isin(tickers_fut)].copy()
    df5 = df[df['sec_code'].isin(tickers_currency)].copy()
    # df.info()

    # Параметры

    # Получение таблиц
    # print(f"\nGenerating tables (top {TOP_N} ticks per sec_code, count > {COUNT_THRESHOLD})...")
    table_10min1 = get_table_for_period(df1, minutes=5, count_threshold=COUNT_THRESHOLD, top_n=TOP_N)
    table_10min2 = get_table_for_period(df2, minutes=5, count_threshold=COUNT_THRESHOLD, top_n=TOP_N)
    table_10min3 = get_table_for_period(df3, minutes=5, count_threshold=COUNT_THRESHOLD, top_n=TOP_N)

    table_10min4 = get_table_for_period(df4, minutes=5, count_threshold=COUNT_THRESHOLD, top_n=TOP_N)
    table_10min5 = get_table_for_period(df5, minutes=5, count_threshold=COUNT_THRESHOLD, top_n=TOP_N)
    
    # Для Windows
    os.system('cls')
    # Выводим выровненные таблицы
    print(datetime.now())
    print_aligned_tables(table_10min1, table_10min2,table_10min3,table_10min4,table_10min5, titles=["LAST 5 MINUTES", "LAST 5 MINUTES","LAST 5 MINUTES","LAST 5 MINUTES", "LAST 5 MINUTES"], top_n=TOP_N,groups=[[0],[1],[2],[3],[4]])

    # # Выводим статистику
    # # print_detailed_stats(table_10min, table_1hour)
    # # Сохраняем выровненные версии для анализа
    # aligned_10min, aligned_1hour = align_tables_by_sec_code(table_10min, table_1hour, TOP_N)
    sleep(60)

