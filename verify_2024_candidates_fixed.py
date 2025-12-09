#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path

# Check 2024 final output
print("=== 檢查 2024 年花蓮縣最終輸出 ===\n")

output_file = Path("鄰里整理輸出/花蓮縣/2024_選舉資料_花蓮縣.csv")
if output_file.exists():
    df = pd.read_csv(output_file, encoding='utf-8')
    
    # Count regional legislator candidates
    regional_cols = [c for c in df.columns if '區域立委候選人' in c and '得票數' in c and c != '區域立委候選人投票數C']
    
    print(f"總欄位數: {len(df.columns)}")
    print(f"區域立委候選人得票數欄位數: {len(regional_cols)}")
    
    if regional_cols:
        print(f"\n區域立委候選人欄位:")
        for col in regional_cols:
            name_col = col.replace('_得票數', '＿候選人名稱')
            party_col = col.replace('_得票數', '＿黨籍')
            
            if name_col in df.columns:
                name = df[name_col].dropna().unique()[0] if not df[name_col].dropna().empty else 'N/A'
                party = df[party_col].dropna().unique()[0] if party_col in df.columns and not df[party_col].dropna().empty else 'N/A'
                votes = df[col].sum()
                print(f"  {col}: {name} ({party}) - 總票數: {votes:,}")
    else:
        print("❌ 完全沒有區域立委候選人資料！")
    
    # Check intermediate file too
    print("\n=== 檢查中間檔 ===")
    intermediate = Path("各縣市候選人分類/花蓮縣/2024_立法委員.csv")
    if intermediate.exists():
        df_int = pd.read_csv(intermediate, encoding='utf-8')
        print(f"\n中間檔總欄位數: {len(df_int.columns)}")
        
        # Count all candidate columns
        all_cand_cols = [c for c in df_int.columns if '候選人' in c and '名稱' in c]
        print(f"候選人名稱欄位總數: {len(all_cand_cols)}")
        
        # Sample candidate names
        if all_cand_cols:
            print(f"\n前10位候選人:")
            for i, col in enumerate(all_cand_cols[:10], 1):
                name = df_int[col].dropna().unique()[0] if not df_int[col].dropna().empty else 'N/A'
                print(f"  {i}. {name}")

else:
    print(f"❌ 找不到檔案: {output_file}")
