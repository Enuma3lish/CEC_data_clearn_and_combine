import pandas as pd
from pathlib import Path

# Check 2024 regional legislator source data
elctks = Path('voteData/2024總統立委/區域立委/elctks.csv')
elcand = Path('voteData/2024總統立委/區域立委/elcand.csv')

if elctks.exists():
    df_ctks = pd.read_csv(elctks, header=None, encoding='utf-8-sig', dtype=str)
    # Filter for Hualien (縣市代碼=10 for new system, or check 鄉鎮代碼=015)
    df_hl = df_ctks[df_ctks[0] == '10']
    if len(df_hl) == 0:
        # Try checking 鄉鎮代碼
        df_hl = df_ctks[df_ctks[1] == '015']
    
    print(f'elctks.csv 花蓮縣資料筆數: {len(df_hl)}')
    if len(df_hl) > 0:
        print('縣市代碼:', df_hl[0].unique())
        print('鄉鎮代碼:', df_hl[1].unique())
        print('候選人號次:', df_hl[6].unique())
        print('\n前3筆資料:')
        print(df_hl.head(3))
else:
    print('elctks.csv 不存在')

print('\n' + '='*60)

if elcand.exists():
    df_cand = pd.read_csv(elcand, header=None, encoding='utf-8-sig', dtype=str)
    df_hl_cand = df_cand[df_cand[0] == '10']
    if len(df_hl_cand) == 0:
        df_hl_cand = df_cand[df_cand[1] == '015']
    
    print(f'elcand.csv 花蓮縣候選人數: {len(df_hl_cand)}')
    if len(df_hl_cand) > 0:
        print('候選人資料:')
        for _, row in df_hl_cand.iterrows():
            print(f"  號次{row[5]}: {row[6]} ({row[7]})")
else:
    print('elcand.csv 不存在')
