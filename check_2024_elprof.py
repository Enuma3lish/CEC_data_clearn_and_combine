#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path

# Check elprof and elbase structure for 2024 regional
year_folder = Path("voteData/2024總統立委")
sub = "區域立委"

elprof_path = year_folder / sub / "elprof.csv"
elbase_path = year_folder / sub / "elbese.csv"  # Note: typo in filename

print("=== 檢查 elprof.csv ===")
if elprof_path.exists():
    df_prof = pd.read_csv(elprof_path, header=None, encoding='utf-8-sig', dtype=str, nrows=10)
    print(f"Shape: {df_prof.shape}")
    print(f"Columns: {list(df_prof.columns)}")
    print(f"\n前5筆:")
    print(df_prof.head())
    
    # Check for prv_code=10
    df_prof_full = pd.read_csv(elprof_path, header=None, encoding='utf-8-sig', dtype=str)
    print(f"\n縣市代碼為10的筆數: {(df_prof_full.iloc[:, 0] == '10').sum()}")
else:
    print("❌ 檔案不存在")

print("\n\n=== 檢查 elbese.csv (elbase) ===")
if elbase_path.exists():
    df_base = pd.read_csv(elbase_path, header=None, encoding='utf-8-sig', dtype=str, nrows=10)
    print(f"Shape: {df_base.shape}")
    print(f"Columns: {list(df_base.columns)}")
    print(f"\n前5筆:")
    print(df_base.head())
    
    # Check for prv_code=10
    df_base_full = pd.read_csv(elbase_path, header=None, encoding='utf-8-sig', dtype=str)
    print(f"\n縣市代碼為10的筆數: {(df_base_full.iloc[:, 0] == '10').sum()}")
else:
    print("❌ 檔案不存在")
