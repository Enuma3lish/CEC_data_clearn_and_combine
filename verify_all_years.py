import pandas as pd

print("檢查花蓮縣所有年份的黨籍資料：\n")
for year in [2014, 2016, 2018, 2020, 2022, 2024]:
    try:
        df = pd.read_csv(f'鄰里整理輸出/花蓮縣/{year}_選舉資料_花蓮縣.csv', encoding='utf-8-sig')
        
        # 找黨籍欄位
        party_cols = [c for c in df.columns if '黨籍' in c]
        
        if len(party_cols) == 0:
            print(f"{year}: ❌ 無黨籍欄位")
            continue
        
        # 檢查非空黨籍值
        non_empty = []
        for col in party_cols[:5]:  # 檢查前5個
            val = str(df[col].iloc[0])
            if val and val != 'nan' and len(val) > 0:
                non_empty.append(val)
        
        if non_empty:
            print(f"{year}: ✅ 黨籍={len(party_cols)}欄 範例: {non_empty[0]}")
        else:
            print(f"{year}: ⚠️ 黨籍={len(party_cols)}欄 但值為空")
    except Exception as e:
        print(f"{year}: ❌ Error - {e}")
