import pandas as pd

# Compare president vs legislator elprof structure
print("=== 總統 elprof.csv ===")
df_pres = pd.read_csv('voteData/2020總統立委/總統/elprof.csv', header=None, dtype=str, encoding='utf-8')
df_ph = df_pres[(df_pres[0] == '10') & (df_pres[1] == '015')]
print(f"Columns: {len(df_ph.columns)}")
print(f"First row:")
print(df_ph.iloc[0].to_dict())

print("\n=== 區域立委 elprof.csv ===")
df_leg = pd.read_csv('voteData/2020總統立委/區域立委/elprof.csv', header=None, dtype=str, encoding='utf-8')
df_lh = df_leg[(df_leg[0] == '10') & (df_leg[1] == '015')]
print(f"Columns: {len(df_lh.columns)}")
print(f"First row:")
print(df_lh.iloc[0].to_dict())

# Based on successful legislator repair, columns should be:
# 5: valid votes (A), 6: invalid votes (B), 7: total votes (C),  
# 8: received but not cast (D), 9: issued (E), 10: remaining (F),
# 11: eligible voters (G), 12: turnout rate (H)
