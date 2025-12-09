import pandas as pd

df = pd.read_csv('各縣市候選人分類/花蓮縣/2020_總統.csv')

print('=== 2020 總統資料 (轉換後) ===')
print(f'欄位數: {len(df.columns)}')

print('\n前12個欄位:')
for i, col in enumerate(df.columns[:12], 1):
    print(f'{i:2d}. {col}')

print('\n\n第1筆資料 - 總統候選人:')
for i in range(1, 4):
    name_col = f'候選人{i}＿候選人名稱'
    party_col = f'候選人{i}＿黨籍'
    vote_col = f'候選人{i}_得票數'
    
    if name_col in df.columns:
        name = df[name_col].iloc[0]
        party = df[party_col].iloc[0] if party_col in df.columns else 'N/A'
        votes = df[vote_col].iloc[0] if vote_col in df.columns else 'N/A'
        print(f'{i}. {name} ({party}) - {votes} 票')
