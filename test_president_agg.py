import pandas as pd

# Read and aggregate president elprof
df = pd.read_csv('voteData/2020總統立委/總統/elprof.csv', header=None, dtype=str, encoding='utf-8')
df_h = df[(df[0] == '10') & (df[1] == '015')].copy()

print(f"Total polling stations: {len(df_h)}")

# Build mapping from elbase.csv
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

print(f"Districts: {len(dist_map)}, Villages: {len(village_map)}")

# Map village names
df_h['dept_code'] = df_h[3].astype(str)
df_h['li_code'] = df_h[4].astype(str)
df_h['village_key'] = df_h['dept_code'] + "_" + df_h['li_code']
df_h['行政區別'] = df_h['dept_code'].map(dist_map)
df_h['村里別'] = df_h['village_key'].map(village_map)

# Convert numeric
for col_idx, col_name in [(5, 'A'), (6, 'B'), (7, 'C'), (8, 'D'), (9, 'E'), (10, 'F'), (11, 'G'), (12, 'H')]:
    df_h[col_name] = pd.to_numeric(df_h[col_idx], errors='coerce').fillna(0)

# Aggregate
agg = df_h.groupby(['行政區別', '村里別'], as_index=False).agg({
    'A': 'sum', 'B': 'sum', 'C': 'sum', 'D': 'sum',
    'E': 'sum', 'F': 'sum', 'G': 'sum', 'H': 'max'
})

print(f"\nAggregated villages: {len(agg)}")
print(f"\nFirst village (北富村):")
first_village = agg[(agg['行政區別'] == '光復鄉') & (agg['村里別'] == '北富村')]
if len(first_village) > 0:
    print(first_village[['行政區別', '村里別', 'A', 'B', 'C', 'G']].to_dict('records')[0])
else:
    print("NOT FOUND!")
    print(f"\nAvailable villages in 光復鄉:")
    print(agg[agg['行政區別'] == '光復鄉'][['村里別', 'A', 'B', 'C', 'G']])
