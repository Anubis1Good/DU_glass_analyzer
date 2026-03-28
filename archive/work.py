import pandas as pd
from utils.quik_funcs import get_ticks,get_all_ticks

# ticks = get_ticks('EDM6')
ticks = get_all_ticks()
# ticks = get_ticks('CNYRUB_TOM','CETS')
# ticks = get_ticks('SBER','TQBR')
for t in ticks:
    print(t)
df = pd.DataFrame(ticks['data'])
print(df.tail())
df.info()
df.to_csv('./test.csv')