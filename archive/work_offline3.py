import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tabulate import tabulate

def parse_datetime(dt_str):
    dt_dict = eval(dt_str)
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

def print_aligned_tables(table1, table2, title1="LAST 10 MINUTES", title2="LAST HOUR", top_n=5):
    """
    Печатает две таблицы рядом с выравниванием по sec_code и рангам
    """
    # Выравниваем таблицы
    table1_aligned, table2_aligned = align_tables_by_sec_code(table1, table2, top_n)
    
    # Функция для форматирования таблицы
    def format_table(table):
        if table.empty:
            return "No data"
        
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
        )
    
    # Получаем строковое представление
    str1 = format_table(table1_aligned)
    str2 = format_table(table2_aligned)
    
    lines1 = str1.split('\n')
    lines2 = str2.split('\n')
    
    # Вычисляем максимальную ширину первой таблицы
    max_width1 = max(len(line) for line in lines1) if lines1 else 0
    
    # Выводим заголовки
    print("\n" + "=" * (max_width1 + 60))
    title1_padded = title1.center(max_width1)
    title2_padded = title2.center(50)
    print(f"{title1_padded}    {title2_padded}")
    print("-" * (max_width1 + 60))
    
    # Выводим строки
    max_lines = max(len(lines1), len(lines2))
    
    for i in range(max_lines):
        line1 = lines1[i] if i < len(lines1) else " " * max_width1
        line2 = lines2[i] if i < len(lines2) else ""
        
        # Если строка из первой таблицы это разделитель, добавляем отступ
        if line1.strip().startswith('+') or line1.strip().startswith('|-'):
            if not (line2.strip().startswith('+') or line2.strip().startswith('|-')):
                line2 = " " * 50
        
        print(f"{line1}    {line2}")
    
    print("=" * (max_width1 + 60))

def print_detailed_stats(table1, table2):
    """
    Выводит детальную статистику по таблицам
    """
    print("\n" + "="*100)
    print("STATISTICS")
    print("="*100)
    
    for name, table in [("Last 10 Minutes", table1), ("Last Hour", table2)]:
        if not table.empty:
            print(f"\n{name}:")
            print(f"  Total records: {len(table)}")
            print(f"  Unique sec_codes: {table['sec_code'].nunique()}")
            print(f"  Total volume: {table['volume'].sum():,.0f}")
            
            # Статистика по sec_code
            volume_by_code = table.groupby('sec_code')['volume'].sum().sort_values(ascending=False)
            for code, vol in volume_by_code.items():
                top_ticks = table[table['sec_code'] == code].nlargest(3, 'volume')
                tick_info = ", ".join([f"tick={int(t['tick'])} (vol={t['volume']:,.0f})" 
                                       for _, t in top_ticks.iterrows()])
                print(f"    {code}: {vol:,.0f} volume [{tick_info}]")
        else:
            print(f"\n{name}: No data available")

# Загрузка и подготовка данных

df = pd.read_csv('test_old6.csv')
df['datetime'] = df['datetime'].apply(parse_datetime)
df['tick'] = np.where(df['flags'] == 1025, -df['qty'], df['qty'])

# Параметры
TOP_N = 3
COUNT_THRESHOLD = 2

# Получение таблиц
# print(f"\nGenerating tables (top {TOP_N} ticks per sec_code, count > {COUNT_THRESHOLD})...")
table_10min = get_table_for_period(df, minutes=10, count_threshold=COUNT_THRESHOLD, top_n=TOP_N)
table_1hour = get_table_for_period(df, minutes=60, count_threshold=COUNT_THRESHOLD, top_n=TOP_N)

# Выводим выровненные таблицы
print_aligned_tables(table_10min, table_1hour, "LAST 10 MINUTES", "LAST HOUR", TOP_N)

# Выводим статистику
print_detailed_stats(table_10min, table_1hour)

# Сохранение результатов
table_10min.to_csv('algos_10min.csv', index=False)
table_1hour.to_csv('algos_1hour.csv', index=False)

# Сохраняем выровненные версии для анализа
aligned_10min, aligned_1hour = align_tables_by_sec_code(table_10min, table_1hour, TOP_N)
aligned_10min.to_csv('algos_10min_aligned.csv', index=False)
aligned_1hour.to_csv('algos_1hour_aligned.csv', index=False)

print("\n" + "="*100)
print("FILES SAVED")
print("="*100)
print(f"  - algos_10min.csv ({len(table_10min)} records)")
print(f"  - algos_1hour.csv ({len(table_1hour)} records)")
print(f"  - algos_10min_aligned.csv ({len(aligned_10min)} records - {TOP_N} rows per sec_code)")
print(f"  - algos_1hour_aligned.csv ({len(aligned_1hour)} records - {TOP_N} rows per sec_code)")

# Дополнительно: показываем структуру выровненных таблиц
print("\n" + "="*100)
print("ALIGNED TABLES STRUCTURE")
print("="*100)
print(f"For each sec_code, there are exactly {TOP_N} rows (ranks 1-{TOP_N})")
print("Empty cells indicate no data for that specific rank")