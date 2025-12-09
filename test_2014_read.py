import pandas as pd

# 模擬main.py讀取2014 elcand的過程
elcand = 'voteData/2014-103年地方公職人員選舉/縣市市長/elcand.csv'

df_cand = pd.read_csv(elcand, encoding='utf-8', header=None, quotechar='"', quoting=1, 
                      dtype={'2': str, '3': str, '4': str, '5': str})

# Assign column names
df_cand.columns = ['prv_code','city_code','area_code','dept_code','li_code','cand_no','name','party','gender','birth','age','address','education','elected','incumbent'] + [f'extra{i}' for i in range(len(df_cand.columns) - 15)]

# Clean
for col in df_cand.columns:
    if df_cand[col].dtype == object:
        df_cand[col] = df_cand[col].astype(str).str.replace("'", "").str.replace('"', "").str.strip()

# 花蓮縣 prv_code=10, city_code=15
df_hualien = df_cand[(pd.to_numeric(df_cand['prv_code'], errors='coerce')==10) & 
                     (pd.to_numeric(df_cand['city_code'], errors='coerce')==15)]

print(f"花蓮縣2014縣市長候選人數: {len(df_hualien)}")
if len(df_hualien) > 0:
    print("\n前3筆:")
    print(df_hualien[['name', 'party', 'cand_no']].head(3))
else:
    print("沒有找到花蓮縣候選人！")
    print(f"\ndf_cand總筆數: {len(df_cand)}")
    print(f"prv_code範圍: {df_cand['prv_code'].unique()[:10]}")
    print(f"city_code範圍: {df_cand['city_code'].unique()[:10]}")
