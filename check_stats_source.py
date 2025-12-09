import pandas as pd

df = pd.read_csv('各縣市候選人分類/花蓮縣/2020_總統.csv')

print('總統中間檔案欄位:')
print(df.columns.tolist())

print('\n第1筆資料:')
for col in df.columns:
    print(f'{col}: {df[col].iloc[0]}')
