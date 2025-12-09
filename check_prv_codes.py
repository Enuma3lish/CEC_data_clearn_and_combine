import pandas as pd
from pathlib import Path

years = [2016, 2020, 2024]
for year in years:
    p = Path(f'voteData/{year}總統立委/區域立委/elctks.csv')
    if p.exists():
        df = pd.read_csv(p, encoding='utf-8', header=None)
        prv_codes = sorted(df[0].unique())
        print(f'{year}: prv_code 唯一值 = {prv_codes}')
        
        # 檢查 prv_code=10 或 63 的 city_code
        for prv in [10, 63]:
            if prv in prv_codes:
                city_codes = sorted(df[df[0]==prv][2].unique())
                print(f'  prv_code={prv}: city_code = {city_codes[:20]}')
