import pandas as pd

df = pd.read_csv('voteData/2020總統立委/總統/elprof.csv', header=None, dtype=str, encoding='utf-8')
df_h = df[(df[0] == '10') & (df[1] == '015')].copy()

# Build mapping
df_base = pd.read_csv('voteData/2020總統立委/總統/elbase.csv', header=None, dtype=str, encoding='utf-8')
df_base = df_base[(df_base[0] == '10') & (df_base[1] == '015')]

dist_map = {}
village_map = {}
for _, row in df_base.iterrows():
    dept_code = str(row[3])
    li_code = str(row[4])
    name = str(row[5]).strip('"')
    
    if li_code == '0000':
        dist_map[dept_code] = name
    else:
        village_key = f"{dept_code}_{li_code}"
        village_map[village_key] = name

# Map village names
df_h['dept_code'] = df_h[3].astype(str)
df_h['li_code'] = df_h[4].astype(str)
df_h['village_key'] = df_h['dept_code'] + "_" + df_h['li_code']
df_h['行政區別'] = df_h['dept_code'].map(dist_map)
df_h['村里別'] = df_h['village_key'].map(village_map)

# Show raw data for北富村before aggregation
beifu = df_h[(df_h['行政區別'] == '光復鄉') & (df_h['村里別'] == '北富村')]
print(f"北富村 polling stations: {len(beifu)}")
print(f"\nColumns 5-12 for all polling stations:")
print(beifu[[5, 6, 7, 8, 9, 10, 11, 12]].to_string())

# Try to identify what these columns actually mean
print(f"\nSum of each column:")
for col in [5, 6, 7, 8, 9, 10, 11, 12]:
    val_sum = pd.to_numeric(beifu[col], errors='coerce').sum()
    print(f"  Column[{col}]: {val_sum}")
