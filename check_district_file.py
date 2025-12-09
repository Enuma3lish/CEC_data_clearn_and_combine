import pandas as pd

df = pd.read_csv('各縣市候選人分類/花蓮縣/2020_立法委員_第01選區.csv', encoding='utf-8-sig')
stats_cols = [c for c in df.columns if '選舉人數' in c]
print('Columns with 選舉人數:', stats_cols)
if stats_cols:
    print(f'First 5 values: {df[stats_cols[0]].head().tolist()}')
    print(f'Unique values: {df[stats_cols[0]].unique()[:10]}')
else:
    print("NO COLUMN FOUND!")
