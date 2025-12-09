import pandas as pd

# Check legislator data in final output
df = pd.read_csv('鄰里整理輸出/花蓮縣/2024_選舉資料_花蓮縣.csv')

# Get legislator columns
leg_cols = [c for c in df.columns if '區域立委' in c][:15]

print('區域立委相關欄位 (前15個):')
for i, col in enumerate(leg_cols, 1):
    print(f'{i}. {col}')

print('\n第1筆資料:')
for col in leg_cols[:6]:
    print(f'{col}: [{df[col].iloc[0]}]')
