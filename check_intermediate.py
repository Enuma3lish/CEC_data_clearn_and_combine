import pandas as pd

df = pd.read_csv('各縣市候選人分類/花蓮縣/2024_立法委員.csv')

print('=== 中間檔案: 2024_立法委員.csv ===\n')
print(f'總筆數: {len(df)}')
print(f'總欄位數: {len(df.columns)}')

cand_cols = [c for c in df.columns if '候選人' in c and '名稱' in c]
print(f'\n候選人名稱欄位數: {len(cand_cols)}')

print('\n前10個候選人相關欄位:')
cols = [c for c in df.columns if '候選人' in c][:10]
for col in cols:
    print(f'  - {col}')

print('\n\n第1筆資料的前3位候選人名稱:')
for i in range(1, 4):
    col = f'候選人{i}＿候選人名稱'
    if col in df.columns:
        val = df[col].iloc[0]
        print(f'  {col}: {val}')
