import pandas as pd

df = pd.read_csv('鄰里整理輸出/花蓮縣/2024_選舉資料_花蓮縣.csv')

print('=== 2024 花蓮縣最終輸出檢查 ===\n')
print(f'總筆數: {len(df)}')
print(f'總欄位數: {len(df.columns)}')

# 檢查區域立委
print('\n前4位區域立委候選人:')
for i in range(1, 5):
    name_col = f'區域立委候選人{i}＿候選人名稱'
    if name_col in df.columns:
        val = df[name_col].iloc[0]
        print(f'  {i}. {val}')

# 檢查統計數據
if '區域立委選舉人數G' in df.columns:
    print(f'\n區域立委選舉人數G (第1筆): {df["區域立委選舉人數G"].iloc[0]}')

if '總統候選人投票數C' in df.columns:
    print(f'總統候選人投票數C (第1筆): {df["總統候選人投票數C"].iloc[0]}')

print('\n\n=== 2020 花蓮縣 ===')
df2020 = pd.read_csv('鄰里整理輸出/花蓮縣/2020_選舉資料_花蓮縣.csv')
print(f'總筆數: {len(df2020)}')
print('\n前6位區域立委候選人:')
for i in range(1, 7):
    name_col = f'區域立委候選人{i}＿候選人名稱'
    if name_col in df2020.columns:
        val = df2020[name_col].iloc[0]
        print(f'  {i}. {val}')

print('\n\n=== 2016 花蓮縣 ===')
df2016 = pd.read_csv('鄰里整理輸出/花蓮縣/2016_選舉資料_花蓮縣.csv')
print(f'總筆數: {len(df2016)}')
print('\n前4位區域立委候選人 (應包含楊悟空):')
for i in range(1, 5):
    name_col = f'區域立委候選人{i}＿候選人名稱'
    if name_col in df2016.columns:
        val = df2016[name_col].iloc[0]
        print(f'  {i}. {val}')
