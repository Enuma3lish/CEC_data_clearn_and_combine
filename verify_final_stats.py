import pandas as pd

df = pd.read_csv('鄰里整理輸出/花蓮縣/2020_選舉資料_花蓮縣.csv', encoding='utf-8-sig')

print('2020 Final Output Statistics (First Row):')
stats = ['總統候選人投票數C', '總統候選人選舉人數G', '區域立委投票數C', '區域立委選舉人數G',
         '總統候選人有效票數A', '總統候選人無效票數B']
for c in stats:
    print(f'{c}: {df[c].iloc[0]}')

print('\n計算驗證:')
print(f'總統: 有效票數A({df["總統候選人有效票數A"].iloc[0]}) + 無效票數B({df["總統候選人無效票數B"].iloc[0]}) = {df["總統候選人有效票數A"].iloc[0] + df["總統候選人無效票數B"].iloc[0]}')
print(f'總統: 投票數C = {df["總統候選人投票數C"].iloc[0]}')
print(f'Match: {(df["總統候選人有效票數A"].iloc[0] + df["總統候選人無效票數B"].iloc[0]) == df["總統候選人投票數C"].iloc[0]}')

print(f'\n區域立委選舉人數G > 0: {df["區域立委選舉人數G"].iloc[0] > 0}')
print(f'區域立委選舉人數G value: {df["區域立委選舉人數G"].iloc[0]}')
