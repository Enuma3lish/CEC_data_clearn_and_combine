import pandas as pd

df = pd.read_csv('voteData/2020總統立委/總統/elprof.csv', header=None, dtype=str, encoding='utf-8')
df_h = df[(df[0] == '10') & (df[1] == '015')]

print(f"Total rows: {len(df_h)}")
print(f"\nColumns 5-12 for first row:")
for i in range(5, 13):
    print(f"  Column[{i}] = {df_h.iloc[0,i]}")

print(f"\nFirst 5 rows, all columns:")
print(df_h.head())

# Check if we need to sum  
print(f"\nSum of column 5 (should be total valid votes): {pd.to_numeric(df_h[5], errors='coerce').sum()}")
print(f"Sum of column 6: {pd.to_numeric(df_h[6], errors='coerce').sum()}")
print(f"Sum of column 7: {pd.to_numeric(df_h[7], errors='coerce').sum()}")
