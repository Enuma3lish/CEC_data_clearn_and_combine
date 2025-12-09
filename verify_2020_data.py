import pandas as pd

df = pd.read_csv('鄰里整理輸出/花蓮縣/2020_選舉資料_花蓮縣.csv')

print('=== 2020 花蓮縣最終輸出檔案檢查 ===\n')
print(f'總筆數: {len(df)}')
print(f'總欄位數: {len(df.columns)}')

print('\n前30個欄位名稱:')
for i, col in enumerate(df.columns[:30], 1):
    print(f'{i:2d}. {col}')

print('\n\n=== 區域立委候選人檢查 ===')
regional_cands = []
for i in range(1, 20):
    col = f'區域立委候選人{i}＿候選人名稱'
    if col in df.columns:
        val = df[col].iloc[0]
        if pd.notna(val) and str(val) != '' and '候選人' not in str(val):
            regional_cands.append((i, val))

print(f'找到 {len(regional_cands)} 位區域立委候選人:')
for idx, name in regional_cands:
    print(f'  {idx}. {name}')

print('\n\n=== 統計數據檢查 ===')
if '區域立委選舉人數G' in df.columns:
    val = df['區域立委選舉人數G'].iloc[0]
    print(f'區域立委選舉人數G (第1筆): {val}')

if '總統候選人投票數C' in df.columns:
    val = df['總統候選人投票數C'].iloc[0]
    print(f'總統候選人投票數C (第1筆): {val}')

if '總統候選人有效票數A' in df.columns:
    val_a = df['總統候選人有效票數A'].iloc[0]
    val_b = df['總統候選人無效票數B'].iloc[0] if '總統候選人無效票數B' in df.columns else 0
    val_c = df['總統候選人投票數C'].iloc[0] if '總統候選人投票數C' in df.columns else 0
    print(f'\n有效票數A: {val_a}')
    print(f'無效票數B: {val_b}')
    print(f'投票數C: {val_c}')
    print(f'A + B = {val_a + val_b} (應該 = C = {val_c})')
    if val_a + val_b == val_c:
        print('✅ 統計公式正確!')
    else:
        print('❌ 統計公式錯誤!')

print('\n\n=== 第1筆資料詳細內容 (前10個欄位) ===')
for col in df.columns[:10]:
    val = df[col].iloc[0]
    print(f'{col}: [{val}]')
