import pandas as pd
import sys
sys.path.append('.')
from pathlib import Path

# Manually replicate the processing for 2020 總統

# Load data
from processors.process_election_unified import read_csv_auto_detect

folder = Path('voteData/2020總統立委/總統')

# Read files
df_base = pd.read_csv(folder / 'elbase.csv', header=None, encoding='utf-8-sig', dtype=str)
df_cand = pd.read_csv(folder / 'elcand.csv', header=None, encoding='utf-8-sig', dtype=str)
df_ctks = pd.read_csv(folder / 'elctks.csv', header=None, encoding='utf-8-sig', dtype=str)
df_prof = pd.read_csv(folder / 'elprof.csv', header=None, encoding='utf-8-sig', dtype=str)

# Rename columns for df_prof
df_prof.columns = ['縣市代碼', '鄉鎮代碼', '選區代碼', '鄉鎮市區代碼', '村里投開票所代碼',
                  '區域', '有效票數A', '無效票數B', '投票數C', '選舉人數G',
                  '已領未投票數D', '發出票數E', 'col12', 'col13', 'col14', 'col15',
                  '用餘票數F', '投票率H_raw', 'col18', 'col19']

# Filter non-summary rows
df_prof = df_prof[df_prof['村里投開票所代碼'] != '0000'].copy()

# Convert to numeric
for col in ['有效票數A', '無效票數B', '投票數C', '已領未投票數D', '發出票數E', '用餘票數F', '選舉人數G']:
    df_prof[col] = pd.to_numeric(df_prof[col], errors='coerce').fillna(0).astype(int)

print("Before aggregation (df_prof):")
print(df_prof[['縣市代碼', '鄉鎮代碼', '鄉鎮市區代碼', '村里投開票所代碼', '有效票數A', '無效票數B', '投票數C', '選舉人數G']].head(10))
print(f"\nTotal rows: {len(df_prof)}")

# Check for duplicates
index_cols = ['縣市代碼', '鄉鎮代碼', '鄉鎮市區代碼', '村里投開票所代碼']
duplicates = df_prof[df_prof.duplicated(subset=index_cols, keep=False)]
print(f"\nDuplicate rows by index_cols: {len(duplicates)}")
if len(duplicates) > 0:
    print("\nFirst 10 duplicates:")
    print(duplicates[index_cols + ['有效票數A', '無效票數B', '投票數C', '選舉人數G']].head(10))

# Aggregate (village level)
df_stats = df_prof.groupby(index_cols).agg({
    '有效票數A': 'sum',
    '無效票數B': 'sum',
    '投票數C': 'sum',
    '已領未投票數D': 'sum',
    '發出票數E': 'sum',
    '用餘票數F': 'sum',
    '選舉人數G': 'sum'
}).reset_index()

print("\nAfter aggregation (df_stats):")
print(df_stats[index_cols + ['有效票數A', '無效票數B', '投票數C', '選舉人數G']].head(10))
print(f"\nTotal rows: {len(df_stats)}")
