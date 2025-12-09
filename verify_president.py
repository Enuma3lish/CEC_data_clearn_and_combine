import pandas as pd

df = pd.read_csv('鄰里整理輸出/花蓮縣/2020_選舉資料_花蓮縣.csv', encoding='utf-8-sig')

print('2020總統候選人資料:\n')
print(f'候選人1: {df["總統候選人1＿候選人名稱"].iloc[0]}')
print(f'  黨籍: {df["總統候選人1＿黨籍"].iloc[0]}')
print(f'  得票數: {df["總統候選人1_得票數"].iloc[0]}')

print(f'\n候選人2: {df["總統候選人2＿候選人名稱"].iloc[0]}')
print(f'  黨籍: {df["總統候選人2＿黨籍"].iloc[0]}')
print(f'  得票數: {df["總統候選人2_得票數"].iloc[0]}')

print(f'\n候選人3: {df["總統候選人3＿候選人名稱"].iloc[0]}')
print(f'  黨籍: {df["總統候選人3＿黨籍"].iloc[0]}')
print(f'  得票數: {df["總統候選人3_得票數"].iloc[0]}')
