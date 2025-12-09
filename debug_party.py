import pandas as pd

# Read intermediate CSV
df = pd.read_csv('各縣市候選人分類/花蓮縣/2022_縣市長.csv', encoding='utf-8-sig')

print("=== 2022縣市長中間檔欄位檢查 ===")
print(f"總欄位數: {len(df.columns)}")
print(f"前20個欄位: {df.columns.tolist()[:20]}")
print()

# Check party columns
party_cols = [c for c in df.columns if '黨籍' in c]
print(f"黨籍欄位數量: {len(party_cols)}")
if party_cols:
    print(f"前5個黨籍欄位: {party_cols[:5]}")
    print()
    print("黨籍欄位的值 (第一筆):")
    for col in party_cols[:5]:
        print(f"  {col} = '{df[col].iloc[0]}'")
else:
    print("沒有找到黨籍欄位！")

print()
print("=== 候選人欄位檢查 ===")
cand_cols = [c for c in df.columns if c not in ['行政區別', '村里別'] and not any(kw in c for kw in ['票數', '投票', '選舉', '黨籍'])]
print(f"候選人欄位 ({len(cand_cols)}): {cand_cols}")
