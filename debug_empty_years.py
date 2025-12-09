import pandas as pd

years = [2014, 2018, 2024]
for year in years:
    try:
        # 檢查中間檔
        df = pd.read_csv(f'各縣市候選人分類/花蓮縣/{year}_縣市長.csv', encoding='utf-8-sig')
        pcols = [c for c in df.columns if '黨籍' in c]
        print(f"\n{year} 中間檔:")
        print(f"  黨籍欄位數: {len(pcols)}")
        if pcols:
            print(f"  第一個黨籍欄: {pcols[0]}")
            val = df[pcols[0]].iloc[0]
            print(f"  值: '{val}' (type={type(val)}, len={len(str(val))})")
        
        # 檢查最終輸出
        df2 = pd.read_csv(f'鄰里整理輸出/花蓮縣/{year}_選舉資料_花蓮縣.csv', encoding='utf-8-sig')
        pcols2 = [c for c in df2.columns if '黨籍' in c]
        print(f"  最終輸出黨籍欄位數: {len(pcols2)}")
        if pcols2:
            val2 = df2[pcols2[0]].iloc[0]
            print(f"  最終輸出值: '{val2}' (type={type(val2)}, len={len(str(val2))})")
    except Exception as e:
        print(f"\n{year}: Error - {e}")
