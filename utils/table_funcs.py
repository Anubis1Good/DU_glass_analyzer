
def get_table_ticks(df,  count_threshold=2, top_n=5):
    algos = (
        df.groupby(['sec_code', 'tick'])
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