import pandas as pd

df = pd.read_csv('各縣市候選人分類/花蓮縣/2020_立法委員.csv', encoding='utf-8-sig')
print(f'Total rows: {len(df)}')
print(f'選舉人數G column exists: {"選舉人數G" in df.columns}')
print(f'選舉人數G unique values: {df["選舉人數G"].unique()[:10]}')
print(f'選舉人數G min: {df["選舉人數G"].min()}')
print(f'選舉人數G max: {df["選舉人數G"].max()}')
print(f'選舉人數G mean: {df["選舉人數G"].mean()}')
print(f'\nFirst 5 rows with stats:')
print(df[['行政區別', '鄰里', '投票數C', '選舉人數G']].head())
