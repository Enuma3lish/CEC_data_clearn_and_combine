#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import pandas as pd
import sys

# Import functions from main.py
sys.path.insert(0, str(Path(__file__).parent))
from main import repair_legislator_data, RAW_DIR

# Regenerate 2024 Legislator data
print("=== 重新生成花蓮縣 2024 立委資料 ===\n")
repair_legislator_data("花蓮縣", 2024, "2024_立法委員.csv", 10, 15)

# Check result
csv_path = RAW_DIR / "花蓮縣" / "2024_立法委員.csv"
if csv_path.exists():
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    
    print(f"\n=== 檢查結果 ===")
    print(f"總筆數: {len(df)}")
    print(f"總欄位數: {len(df.columns)}")
    
    # Count candidates
    cand_cols = [c for c in df.columns if '候選人' in c and '名稱' in c]
    print(f"候選人數: {len(cand_cols)}")
    
    if cand_cols:
        print(f"\n前15位候選人:")
        for i, col in enumerate(cand_cols[:15], 1):
            name = df[col].dropna().unique()[0] if not df[col].dropna().empty else 'N/A'
            print(f"  {i}. {name}")
    else:
        print("❌ 沒有候選人資料！")
else:
    print(f"❌ 找不到檔案: {csv_path}")
