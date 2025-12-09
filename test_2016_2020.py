from main import repair_legislator_data, RAW_DIR
import pandas as pd

print('=== 2016 立委 ===')
repair_legislator_data('花蓮縣', 2016, '2016_立法委員.csv', 10, 15)
df = pd.read_csv(RAW_DIR / '花蓮縣' / '2016_立法委員.csv')
cands = [c for c in df.columns if '候選人' in c and '名稱' in c]
print(f'\n候選人總數: {len(cands)}')
print(f'總筆數: {len(df)}')

print('\n前6位候選人 (應包含楊悟空):')
for i in range(1, 7):
    col = f'候選人{i}＿候選人名稱'
    if col in df.columns:
        names = df[col].dropna().unique()
        if len(names) > 0 and str(names[0]) != '' and str(names[0]) != 'nan':
            print(f'  {i}. {names[0]}')

print('\n\n=== 2020 立委 ===')
repair_legislator_data('花蓮縣', 2020, '2020_立法委員.csv', 10, 15)
df2 = pd.read_csv(RAW_DIR / '花蓮縣' / '2020_立法委員.csv')
cands2 = [c for c in df2.columns if '候選人' in c and '名稱' in c]
print(f'\n候選人總數: {len(cands2)} (應為27位)')
print(f'總筆數: {len(df2)}')

print('\n前6位候選人 (區域立委):')
for i in range(1, 7):
    col = f'候選人{i}＿候選人名稱'
    if col in df2.columns:
        names = df2[col].dropna().unique()
        if len(names) > 0 and str(names[0]) != '' and str(names[0]) != 'nan':
            print(f'  {i}. {names[0]}')
