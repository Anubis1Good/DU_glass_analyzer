import pandas as pd
from utils.quik_funcs import get_glass

# glass = get_glass('IMOEXF')
glass = get_glass('SBER','TQBR')
bids = pd.DataFrame(glass['bid'])
offers = pd.DataFrame(glass['offer'])
bids['quantity'] = pd.to_numeric(bids['quantity'])
offers['quantity'] = pd.to_numeric(offers['quantity'])
print('Покупают:',bids['quantity'].sum())
print('Продают:',offers['quantity'].sum())