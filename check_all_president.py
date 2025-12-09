import pandas as pd

for year in [2016, 2020, 2024]:
    df = pd.read_csv(f'各縣市候選人分類/花蓮縣/{year}_總統.csv')
    print(f'\n=== {year} 總統資料 ===')
    print(f'欄位數: {len(df.columns)}')
    print('前8個欄位:')
    for i, col in enumerate(df.columns[:8], 1):
        print(f'  {i}. {col}')
