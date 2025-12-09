import pandas as pd

# Read raw elprof.csv
df = pd.read_csv('voteData/2020總統立委/總統/elprof.csv', header=None, encoding='utf-8-sig', dtype=str)

# Apply column names
df.columns = ['縣市代碼', '鄉鎮代碼', '選區代碼', '鄉鎮市區代碼', '村里投開票所代碼',
              '區域', '有效票數A', '無效票數B', '投票數C', '選舉人數G',
              '已領未投票數D', '發出票數E', 'col12', 'col13', 'col14', 'col15',
              '用餘票數F', '投票率H_raw', 'col18', 'col19']

# Filter to a specific village (e.g., 花蓮縣鳳林鎮北富村)
# 縣市=15, 鄉鎮=041, 鄉鎮市區=041, 村里投開票所=0002 (北富村第2投開票所)
df_filtered = df[(df['縣市代碼'] == '63') & (df['村里投開票所代碼'] != '0000')].copy()

# Convert to numeric
for col in ['有效票數A', '無效票數B', '投票數C', '已領未投票數D', '發出票數E', '用餘票數F', '選舉人數G']:
    df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce').fillna(0).astype(int)

print("First 5 rows (台北市):")
print(df_filtered[['縣市代碼', '鄉鎮代碼', '村里投開票所代碼', '有效票數A', '無效票數B', '投票數C', '選舉人數G', '發出票數E']].head())

# Check if the issue is from the raw data
print("\n投票數C should = 有效票數A + 無效票數B:")
df_filtered['計算投票數'] = df_filtered['有效票數A'] + df_filtered['無效票數B']
df_filtered['差異'] = df_filtered['投票數C'] - df_filtered['計算投票數']
print(df_filtered[['有效票數A', '無效票數B', '投票數C', '計算投票數', '差異']].head())
