#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path

# Test filtering 2024 regional legislator data
print("=== 測試 2024 區域立委資料過濾 ===\n")

source_file = Path("voteData/2024總統立委/區域立委/elctks.csv")
if source_file.exists():
    # Read data
    col_names = ['prv_code','city_code','area_code','dept_code','li_code','tks','cand_no','ticket_num','ratio','elected']
    df_tks = pd.read_csv(source_file, names=col_names, quotechar='"', quoting=0, header=None,
                        dtype={'area_code': str, 'dept_code': str, 'li_code': str, 'cand_no': str})
    
    # Clean quotes
    for col in df_tks.columns:
        if df_tks[col].dtype == object:
            df_tks[col] = df_tks[col].astype(str).str.replace("'", "").str.replace('"', "")
    
    print(f"總筆數: {len(df_tks)}")
    print(f"prv_code unique values: {df_tks['prv_code'].unique()[:20]}")
    print(f"prv_code data type: {df_tks['prv_code'].dtype}")
    
    # Test filtering with prv_code = 10
    prv_code = 10
    df_test = df_tks[
        (pd.to_numeric(df_tks['prv_code'], errors='coerce') == int(prv_code))
    ].copy()
    
    print(f"\n過濾後 (prv_code == 10):")
    print(f"  筆數: {len(df_test)}")
    if len(df_test) > 0:
        print(f"  前10筆:")
        print(df_test[['prv_code', 'city_code', 'dept_code', 'li_code', 'cand_no', 'ticket_num']].head(10))
    else:
        print("  ❌ 沒有資料！檢查 prv_code 值:")
        print(f"  轉換後的 prv_code: {pd.to_numeric(df_tks['prv_code'], errors='coerce').unique()[:20]}")
else:
    print(f"❌ 找不到檔案: {source_file}")
