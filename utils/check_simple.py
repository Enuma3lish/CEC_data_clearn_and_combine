import pandas as pd
import os
from pathlib import Path
import sys

# Force UTF-8 for Windows console
sys.stdout.reconfigure(encoding='utf-8')

# Use Y drive
repo_2022 = Path(r'Y:')

subdirs = ['C1', 'D1', 'D2', 'R1', 'R2', 'R3', 'T1', 'T2', 'T3', 'V1']

print("Checking 2022 subfolders explicitly...")

for name in subdirs:
    item = repo_2022 / name
    if item.exists():
        candidates = [
            item / 'elctks.csv',
            item / 'prv' / 'elctks.csv',
            item / 'city' / 'elctks.csv'
        ]
        csv = None
        for c in candidates:
            if c.exists():
                csv = c
                break
        
        status = "Source CSV Found" if csv else "No CSV"
        print(f"Folder {name}: {status}")
        
        if csv:
             try:
                # Read headerless
                df = pd.read_csv(csv, header=None, nrows=10, quotechar='"', quoting=0)
                # Clean
                for c in df.columns:
                    if df[c].dtype == object:
                         df[c] = df[c].astype(str).str.replace("'", "").str.replace('"', "")
                
                # Check for 63 (Taipei) or 10 (Hualien) in col 0 (Prv)
                has_63 = (pd.to_numeric(df[0], errors='coerce') == 63).any()
                has_10 = (pd.to_numeric(df[0], errors='coerce') == 10).any()
                
                desc = []
                if has_63: desc.append("Taipei(63)")
                if has_10: desc.append("Hualien(10)")
                if desc:
                    print(f"  -> Contains: {', '.join(desc)}")
             except Exception as e:
                 print(f"  -> Error reading CSV: {e}")
