import pandas as pd
import sys
sys.path.append('.')
from main import repair_president_data

# Generate 2020 president data for 花蓮縣
repair_president_data('花蓮縣', 2020, '2020_總統.csv', 10, 15)

# Check the file before conversion
df_before = pd.read_csv('voteData/2020總統立委/總統/elctks.csv', encoding='utf-8', dtype=str)
print(f'原始資料欄位數: {len(df_before.columns)}')
print(f'原始資料欄位: {df_before.columns.tolist()[:20]}')

# Check after repair
df_after = pd.read_csv('各縣市候選人分類/花蓮縣/2020_總統.csv')
print(f'\n修復後欄位數: {len(df_after.columns)}')
print(f'修復後欄位: {df_after.columns.tolist()}')

print(f'\n第1筆資料的統計欄位:')
for col in ['有效票數A', '無效票數B', '投票數C', '選舉人數G']:
    print(f'{col}: {df_after[col].iloc[0]}')
