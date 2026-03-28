import pandas as pd
import numpy as np
from datetime import datetime

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
df.to_csv('test2.csv')
# algos = df[df['qty'] > 10]['tick'].value_counts()
algos = df['tick'].value_counts()
algos = pd.DataFrame(algos)
algos = algos.reset_index()
algos['volume'] = abs(algos['tick']*algos['count'])
algos = algos.sort_values('volume',ascending=False)
algos.info()
# print(algos.sample(5))
algos = algos.reset_index(drop=True)
algos.to_csv('algos.csv')
print('Покупки:',algos[algos['tick'] > 0]['volume'].sum())
print('Продажи:',algos[algos['tick'] < 0]['volume'].sum())