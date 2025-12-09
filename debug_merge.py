import pandas as pd
from pathlib import Path

county_folder = Path('各縣市候選人分類/花蓮縣')
year = 2020

# Find all district files
district_files = sorted(county_folder.glob(f'{year}_立法委員_第*.csv'))
print(f'Found {len(district_files)} district files:')
for f in district_files:
    print(f'  - {f.name}')

# Read each and check 選舉人數G
dfs = []
for f in district_files:
    df = pd.read_csv(f, encoding='utf-8-sig')
    print(f'\n{f.name}:')
    print(f'  Rows: {len(df)}')
    print(f'  選舉人數G exists: {"選舉人數G" in df.columns}')
    if '選舉人數G' in df.columns:
        print(f'  選舉人數G unique: {df["選舉人數G"].unique()[:5]}')
    dfs.append(df)

# Merge
if len(dfs) > 1:
    merged = pd.concat(dfs, ignore_index=True)
else:
    merged = dfs[0]

print(f'\nMerged result:')
print(f'  Rows: {len(merged)}')
print(f'  選舉人數G exists: {"選舉人數G" in merged.columns}')
if '選舉人數G' in merged.columns:
    print(f'  選舉人數G unique: {merged["選舉人數G"].unique()[:10]}')
    print(f'  選舉人數G min: {merged["選舉人數G"].min()}')
    print(f'  選舉人數G max: {merged["選舉人數G"].max()}')
