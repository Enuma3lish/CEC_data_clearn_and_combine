import pandas as pd

# 檢查 2020 中間檔案
df_2020 = pd.read_csv('各縣市候選人分類/花蓮縣/2020_立法委員.csv')
print('=== 2020 中間檔案 (各縣市候選人分類) ===')
print(f'筆數: {len(df_2020)}, 欄位數: {len(df_2020.columns)}')

print('\n前15個欄位名稱:')
for i, col in enumerate(df_2020.columns[:15], 1):
    print(f'{i:2d}. {col}')

print('\n\n第1筆資料 - 前6位候選人:')
for i in range(1, 7):
    name_col = f'候選人{i}＿候選人名稱'
    party_col = f'候選人{i}＿黨籍'
    vote_col = f'候選人{i}_得票數'
    
    if name_col in df_2020.columns:
        name = df_2020[name_col].iloc[0]
        party = df_2020[party_col].iloc[0] if party_col in df_2020.columns else 'N/A'
        votes = df_2020[vote_col].iloc[0] if vote_col in df_2020.columns else 'N/A'
        print(f'{i}. {name} ({party}) - {votes} 票')

# 檢查 2020 總統資料
df_pres = pd.read_csv('各縣市候選人分類/花蓮縣/2020_總統.csv')
print('\n\n=== 2020 總統資料 ===')
print(f'筆數: {len(df_pres)}, 欄位數: {len(df_pres.columns)}')

print('\n前10個欄位:')
for i, col in enumerate(df_pres.columns[:10], 1):
    print(f'{i:2d}. {col}')

print('\n第1筆資料 - 總統候選人:')
for i in range(1, 4):
    name_col = f'候選人{i}＿候選人名稱'
    if name_col in df_pres.columns:
        name = df_pres[name_col].iloc[0]
        print(f'{i}. {name}')
