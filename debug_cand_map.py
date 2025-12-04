# -*- coding: utf-8 -*-
"""Debug候選人映射問題"""

import pandas as pd
import os

folder = "voteData/voteData/2024總統立委/山地立委"

# 讀取候選人資料
def read_cec_csv(file_path):
    encodings = ['utf-8-sig', 'big5', 'cp950', 'utf-8']
    for enc in encodings:
        try:
            return pd.read_csv(file_path, header=None, encoding=enc)
        except:
            continue
    return None

# 讀取候選人檔案
cand_file = os.path.join(folder, "elcand.csv")
df_cand = read_cec_csv(cand_file)

if df_cand is not None:
    print("✓ 成功讀取候選人檔案")
    print(f"欄位數: {len(df_cand.columns)}")
    
    if len(df_cand.columns) >= 16:
        df_cand.columns = ['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'ConstCode', 'CandNo',
                           'CandName', 'PartyCode', 'Gender', 'BirthDate', 'Age', 'BirthPlace',
                           'Edu', 'Incumbent', 'Won', 'Vice']
        
        print("\n候選人資料:")
        print(df_cand[['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'ConstCode', 'CandNo', 'CandName']].head(10))
        
        # 測試候選人映射（修正版）
        print("\n\n=== 測試候選人映射（修正版） ===")
        cand_map = {}
        for _, row in df_cand.iterrows():
            cand_name = f"{row['CandNo']}_{row['CandName']}"
            
            # 候選人檔案的PrvCode可能是0或實際省碼
            prv_codes = [row['PrvCode']]
            if row['PrvCode'] == 0:
                prv_codes.extend([63, 64])  # 臺灣省、福建省
            
            for prv in prv_codes:
                keys = [
                    (prv, row['CityCode'], row['DeptCode'], row['GroupCode'], row['CandNo']),
                    (prv, row['CityCode'], row['DeptCode'], '000', row['CandNo']),
                    (prv, row['CityCode'], '000', row['GroupCode'], row['CandNo']),
                    (prv, row['CityCode'], '000', '000', row['CandNo']),
                    (prv, '000', row['DeptCode'], row['GroupCode'], row['CandNo']),
                    (prv, '000', '000', row['GroupCode'], row['CandNo']),
                ]
                
                for key in keys:
                    cand_map[key] = cand_name
        
        print(f"cand_map 總共有 {len(cand_map)} 個key")
        print("\n前20個cand_map項目（修正版）:")
        for i, (k, v) in enumerate(list(cand_map.items())[:20]):
            print(f"  {k} => {v}")
        
        # 讀取得票數檔案並測試查找
        print("\n\n=== 測試從得票數檔案查找候選人 ===")
        tks_file = os.path.join(folder, "elctks.csv")
        df_tks = read_cec_csv(tks_file)
        
        if df_tks is not None and len(df_tks.columns) >= 10:
            df_tks.columns = ['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'LiCode',
                              'SiteId', 'CandNo', 'Votes', 'Rate', 'Won']
            
            print(f"得票數檔案有 {len(df_tks)} 筆")
            print("\n前5筆:")
            print(df_tks[['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'CandNo']].head())
            
            # 測試查找
            test_row = df_tks.iloc[0]
            test_key = (test_row['PrvCode'], test_row['CityCode'], test_row['DeptCode'], test_row['GroupCode'], test_row['CandNo'])
            print(f"\n測試查找 key: {test_key}")
            print(f"查找結果: {cand_map.get(test_key, '未找到')}")
            
            # 嘗試所有可能的key組合
            def get_cand_name(prv, city, dept, group, cand_no):
                possible_keys = [
                    (prv, city, dept, group, cand_no),
                    (prv, city, dept, '000', cand_no),
                    (prv, city, '000', group, cand_no),
                    (prv, city, '000', '000', cand_no),
                    (prv, '000', dept, group, cand_no),
                    (prv, '000', '000', group, cand_no),
                    (prv, city, dept, '00', cand_no),
                    (prv, city, '00', group, cand_no),
                    (prv, city, '00', '00', cand_no),
                    (prv, '000', '000', '000', cand_no),
                ]
                
                for key in possible_keys:
                    if key in cand_map:
                        return cand_map[key]
                        
                return f"{cand_no}_候選人"
            
            test_name = get_cand_name(test_row['PrvCode'], test_row['CityCode'], test_row['DeptCode'], test_row['GroupCode'], test_row['CandNo'])
            print(f"使用get_cand_name查找: {test_name}")
            
    else:
        print(f"候選人檔案欄位數不符: {len(df_cand.columns)}")
else:
    print("✗ 無法讀取候選人檔案")
