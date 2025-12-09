import pandas as pd
from pathlib import Path

folder = Path('voteData/2016總統立委/區域立委')
elprof = folder / 'elprof.csv'

print(f'2016 區域立委 elprof 存在: {elprof.exists()}')

if elprof.exists():
    df = pd.read_csv(elprof, header=None, encoding='utf-8-sig', dtype=str, nrows=10)
    print(f'\n前10行（縣市代碼, 有效票數A, 投票數C, 選舉人數G）:')
    for i, row in df.iterrows():
        print(f'  Row {i}: 縣市={row[0]}, A={row[6]}, C={row[8]}, G={row[9]}')
