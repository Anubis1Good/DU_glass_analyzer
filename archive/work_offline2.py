import pandas as pd
import numpy as np
from datetime import datetime,timedelta

# Функция для преобразования строки со словарём в datetime
def parse_datetime(dt_str):
    # Преобразуем строку в словарь
    dt_dict = eval(dt_str)
    # Создаём datetime объект
    return datetime(
        year=dt_dict['year'],
        month=dt_dict['month'],
        day=dt_dict['day'],
        hour=dt_dict['hour'],
        minute=dt_dict['min'],
        second=dt_dict['sec'],
        microsecond=dt_dict['ms'] * 1000  # ms в микросекунды
    )

# Применяем функцию к столбцу datetime
df = pd.read_csv('test_old6.csv')
df['datetime'] = df['datetime'].apply(parse_datetime)
df['tick'] = np.where(df['flags'] == 1025,-df['qty'],df['qty'])
df.info()
print(df.tail())
# print(df['flags'].value_counts())
# df = df[df['datetime'].dt.hour == 17]
print(df.head())
df['datetime'] = pd.to_datetime(df['datetime'])

# 2. Фильтруем записи за последние 20 минут
last_time = df['datetime'].max()
time_threshold = last_time - timedelta(minutes=50)
df_filtered = df[df['datetime'] >= time_threshold].copy()

# 3. Группируем по sec_code и tick, считаем count и volume
algos = (
    df_filtered.groupby(['sec_code', 'tick'])
    .size()
    .reset_index(name='count')
)

# 4. Считаем volume по формуле algos['volume'] = abs(algos['tick'] * algos['count'])
algos['volume'] = abs(algos['tick'] * algos['count'])

# 5. Оставляем только те тики, у которых count > 10
algos_filtered = algos[algos['count'] > 2].copy()

# 6. Для каждого sec_code отбираем первые 5 тиков по объёму (по убыванию)
result = (
    algos_filtered
    .sort_values(['sec_code', 'volume'], ascending=[True, False])
    .groupby('sec_code')
    .head(5)
    .reset_index(drop=True)
)

# Результат
print(result)
result.to_csv('algos2.csv')