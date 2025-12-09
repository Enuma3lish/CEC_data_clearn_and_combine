import pandas as pd

df_leg = pd.read_csv('各縣市候選人分類/花蓮縣/2020_立法委員.csv')
df_pres = pd.read_csv('各縣市候選人分類/花蓮縣/2020_總統.csv')

print('=== 立委資料統計 (第1筆) ===')
print(f'選舉人數G: {df_leg["選舉人數G"].iloc[0]}')
print(f'投票數C: {df_leg["投票數C"].iloc[0]}')
print(f'有效票數A: {df_leg["有效票數A"].iloc[0]}')
print(f'無效票數B: {df_leg["無效票數B"].iloc[0]}')

print('\n=== 總統資料統計 (第1筆) ===')
print(f'選舉人數G: {df_pres["選舉人數G"].iloc[0]}')
print(f'投票數C: {df_pres["投票數C"].iloc[0]}')
print(f'有效票數A: {df_pres["有效票數A"].iloc[0]}')
print(f'無效票數B: {df_pres["無效票數B"].iloc[0]}')

print('\n=== 鄰里資訊 ===')
print(f'立委第1筆: {df_leg["行政區別"].iloc[0]} - {df_leg["村里別"].iloc[0]}')
print(f'總統第1筆: {df_pres["行政區別"].iloc[0]} - {df_pres["村里別"].iloc[0]}')
