import pandas as pd

# Read elbase to get party code to name mapping
df_elbase = pd.read_csv('voteData/2020總統立委/總統/elbase.csv', header=None, encoding='utf-8', quotechar='"')
print("elbase前10筆 (政黨代碼和名稱):")
print(df_elbase.head(10)[[1,2]])
print()

# Column 1 should be party code, column 2 should be party name
party_map = df_elbase.set_index(1)[2].to_dict()
print(f"政黨對應 (共{len(party_map)}個):")
for code, name in list(party_map.items())[:10]:
    print(f"  {code} → {name}")
