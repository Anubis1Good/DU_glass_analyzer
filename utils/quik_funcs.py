# import itertools
from time import time
from typing import Optional
import pandas as pd
from libs.QuikPy import QuikPy

# trans_id = itertools.count(1)
# class_code = 'SPBFUT'  # Фьючерсы
# security_codes = ('SiM5', 'RIM5')  

#декоратор подключения
def provider(func):
    def wrapper(*args, **kwargs):
        qp_provider = None
        try:
            qp_provider = QuikPy()
            res = func(qp_provider,*args, **kwargs)
            qp_provider.close_connection_and_thread() 
            return res
        except:
            if qp_provider:
                qp_provider.close_connection_and_thread()
    return wrapper


@provider
def get_bars(qp_provider:QuikPy,sec_code,tf='M5',count=60,class_code='SPBFUT') -> pd.DataFrame:
    '''получение датафрейма баров'''
    time_frame, _ = qp_provider.timeframe_to_quik_timeframe(tf) 
    history = qp_provider.get_candles_from_data_source(class_code, sec_code, time_frame,count=count)  # Получаем все бары из QUIK

    pd_bars = pd.DataFrame()
    if 'data' in history:
        new_bars = history['data'] 
        pd_bars = pd.json_normalize(new_bars)
        pd_bars = pd_bars.rename(columns={'datetime.year': 'year', 'datetime.month': 'month', 'datetime.day': 'day',
                            'datetime.hour': 'hour', 'datetime.min': 'minute', 'datetime.sec': 'second'})
        pd_bars['ms'] = pd.to_datetime(pd_bars[['year', 'month', 'day', 'hour', 'minute', 'second']])
        pd_bars = pd_bars[['ms', 'open', 'high', 'low', 'close', 'volume']]  
        # pd_bars.index = pd_bars['ms']
        pd_bars.volume = pd.to_numeric(pd_bars.volume, downcast='integer')  
        pd_bars.drop_duplicates(keep='last', inplace=True)
        pd_bars.reset_index(drop=True)
        pd_bars['direction'] = pd_bars.apply(lambda row: 1 if row['open'] < row['close'] else -1, axis=1)
        pd_bars['middle'] = pd_bars.apply(lambda row: (row['high']+row['low'])/2,axis=1)
        pd_bars['x'] = pd_bars.index
    return pd_bars


@provider
def send_transaction(qp_provider:QuikPy,sec_code,price,direction='B',quantity = 1,class_code='SPBFUT'):
    '''отправка заявки'''
    account = next((account for account in qp_provider.accounts if class_code in account['class_codes']), None)
    # cur_trans_id = str(next(trans_id))
    cur_trans_id = str(int(time()*100))[3:]
    if account:
        client_code = account['client_code'] if account['client_code'] else ''
        trade_account_id = account['trade_account_id']
        transaction = {  # Все значения должны передаваться в виде строк
            'TRANS_ID': cur_trans_id,  # Следующий номер транзакции
            'CLIENT_CODE': client_code,  # Код клиента
            'ACCOUNT': trade_account_id,  # Счет
            'ACTION': 'NEW_ORDER',  # Тип заявки: Новая лимитная/рыночная заявка
            'CLASSCODE': class_code,  # Код режима торгов
            'SECCODE': sec_code,  # Код тикера
            'OPERATION': direction,  # B = покупка, S = продажа
            'PRICE': str(price),  # Цена исполнения
            'QUANTITY': str(quantity),  # Кол-во в лотах
            'TYPE': 'L'
        }
        qp_provider.send_transaction(transaction)
        return cur_trans_id


@provider
def get_active_order(qp_provider:QuikPy,sec_code):
    '''получение активных ордеров'''
    orders = qp_provider.get_all_orders()['data'] 
    active_orders = []
    for order in orders:
        if order['sec_code'] == sec_code and order['flags'] % 2 == 1:
            active_orders.append(order)
    return active_orders

@provider
def get_code_orders(qp_provider:QuikPy,sec_code):
    '''получение ордеров по тикеру'''
    orders = qp_provider.get_all_orders()['data'] 
    code_orders = []
    for order in orders:
        if order['sec_code'] == sec_code:
            code_orders.append(order)
    return code_orders

@provider
def help_close_active_order(qp_provider:QuikPy,active_orders,sec_code):
    '''вспомогательная фунция для закрытия активных ордеров'''
    cur_trans_ids = []
    if active_orders:
        for ao in active_orders:
            # print(ao['price'])
            cur_trans_id = str(int(time()*100))[3:]
            transaction = {  # Все значения должны передаваться в виде строк
                'TRANS_ID': cur_trans_id,  # Следующий номер транзакции
                'ACTION': 'KILL_ORDER',  # Тип заявки: Удаление существующей заявки
                'CLASSCODE': ao['class_code'],  # Код режима торгов
                'SECCODE': sec_code,  # Код тикера
                'ORDER_KEY': str(ao['order_num'])
            }
            qp_provider.send_transaction(transaction)
            cur_trans_ids.append(cur_trans_id)
    return cur_trans_ids


@provider
def help_smart_close_active_order(qp_provider:QuikPy,active_orders,sec_code,price):
    '''вспомогательная функция для закрытия активных ордеров не совпадающих по цене'''
    cur_trans_ids = []
    skip_close = 0
    if active_orders:
        for ao in active_orders:
            # print(ao['price'],price) #delete
            # print(type(ao['price']),type(price)) #delete
            if ao['price'] == price:
                skip_close += 1
                continue
            cur_trans_id = str(int(time()*100))[3:]
            transaction = {  # Все значения должны передаваться в виде строк
                'TRANS_ID': cur_trans_id,  # Следующий номер транзакции
                'ACTION': 'KILL_ORDER',  # Тип заявки: Удаление существующей заявки
                'CLASSCODE': ao['class_code'],  # Код режима торгов
                'SECCODE': sec_code,  # Код тикера
                'ORDER_KEY': str(ao['order_num'])
            }
            qp_provider.send_transaction(transaction)
            cur_trans_ids.append(cur_trans_id)
    return cur_trans_ids,skip_close


def close_active_order(sec_code):
    '''закрытие активных'''
    active_orders = get_active_order(sec_code)
    trans_ids = help_close_active_order(active_orders,sec_code)
    return trans_ids

def smart_close_active_order(sec_code,price:float):
    '''закрытие активных ордеров не совпадающих по цене'''
    active_orders = get_active_order(sec_code)
    trans_ids,skip_close = help_smart_close_active_order(active_orders,sec_code,price)
    return trans_ids,skip_close

@provider
def get_pos_futures(qp_provider:QuikPy,sec_code):
    '''получение позиции по фьючерсам из таблицы'''
    active_futures_holdings = [futuresHolding for futuresHolding in qp_provider.get_futures_holdings()['data'] if futuresHolding['totalnet'] != 0]
    for afh in active_futures_holdings:
        if afh['sec_code'] == sec_code:
            return afh['totalnet']
    return 0

@provider
def get_current_price(qp_provider: QuikPy, sec_code: str, class_code='SPBFUT'):
    '''Получение текущей цены инструмента'''
    param_data = qp_provider.get_param_ex(class_code, sec_code, "LAST")['data']
    current_price = float(param_data['param_value'])
    print(param_data)
    print(current_price)
    
@provider
def debug_futures_holdings(qp_provider: QuikPy, sec_code: str):
    '''Отладочная функция для просмотра всех полей позиции'''
    futures_holdings = qp_provider.get_futures_holdings()['data']
    
    for position in futures_holdings:
        if position['sec_code'] == sec_code:
            print("=== ВСЕ ПОЛЯ ПОЗИЦИИ ===")
            for key, value in position.items():
                print(f"{key}: {value}")
            print("========================")
            
            # Основные поля для расчета
            total_net = position['totalnet']
            avg_price = position['avrposnprice']
            open_equity = position['open_equity']  # Возможно, это то что нужно
            realized_pl = position['realized_pl']
            
            print(f"Позиция: {total_net} лотов")
            print(f"Средняя цена: {avg_price}")
            print(f"Открытая вариационная маржа: {open_equity}")
            print(f"Реализованный P&L: {realized_pl}")
            
            return position
    
    print(f"Позиция по {sec_code} не найдена")
    return None

@provider
def get_glass(qp_provider:QuikPy,sec_code,class_code='SPBFUT'):
    '''получение стакана'''
    glass = qp_provider.get_quote_level2(class_code, sec_code)["data"]
    return glass

def get_best_glass(sec_code,class_code='SPBFUT'):
    '''return bbid,bask'''
    glass = get_glass(sec_code,class_code)
    bbid = glass['bid'][-1]['price']
    bask = glass['offer'][0]['price']
    return bbid,bask

@provider
def get_ticks(qp_provider:QuikPy,sec_code,class_code='SPBFUT'):
    """тики за 2 дня"""
    ticks = qp_provider.get_trade(class_code,sec_code)
    return ticks

@provider
def get_all_ticks(qp_provider:QuikPy):
    """тики за 2 дня"""
    ticks = qp_provider.get_all_trade()
    return ticks

@provider
def get_order_by_trans_id(qp_provider: QuikPy, trans_id: int) -> Optional[str]:
    """
    Ищет заявку по trans_id
    Возвращает order если находит
    """
    orders = qp_provider.get_all_orders()['data']
    for order in orders:
        if str(order['trans_id']) == str(trans_id):
            return order

    return None

@provider
def get_result_futures(qp_provider:QuikPy,sec_code):
    '''получение маржи по фьючерсам из таблицы
    первое значение текущая маржа, второе общая маржа
    '''
    active_futures_holdings = [futuresHolding for futuresHolding in qp_provider.get_futures_holdings()['data']]
    for afh in active_futures_holdings:
        if afh['sec_code'] == sec_code:
            return afh['varmargin'], afh['total_varmargin']
    return 0, 0