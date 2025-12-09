import pandas as pd
from pathlib import Path

for year in [2016, 2020, 2024]:
    p = Path(f'鄰里整理輸出/花蓮縣/{year}_選舉資料_花蓮縣.csv')
    if p.exists():
        df = pd.read_csv(p)
        print(f'\n{year} 年花蓮縣:')
        print(f'  總筆數: {len(df)}')
        
        legislator_cands = [c for c in df.columns if '立委候選人' in c and '名稱' in c]
        president_cands = [c for c in df.columns if '總統候選人' in c and '名稱' in c]
        
        print(f'  立委候選人欄位數: {len(legislator_cands)}')
        print(f'  總統候選人欄位數: {len(president_cands)}')
        
        if '區域立委選舉人數G' in df.columns:
            print(f'  區域立委選舉人數G: {df["區域立委選舉人數G"].iloc[0]}')
        
        if '總統候選人投票數C' in df.columns:
            print(f'  總統投票數C: {df["總統候選人投票數C"].iloc[0]}')
        
        # 檢查前幾位立委候選人名稱
        print(f'\n  前5位立委候選人:')
        for i in range(1, min(6, len(legislator_cands)+1)):
            col = f'立委候選人{i}＿候選人名稱'
            if col in df.columns:
                names = df[col].dropna().unique()
                if len(names) > 0 and str(names[0]) != '':
                    print(f'    {i}. {names[0]}')
