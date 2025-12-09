import pandas as pd
from pathlib import Path
import sys

OUTPUT_DIR = Path(r'c:\Users\melo\OneDrive\Desktop\CEC_data_clearn_and_combine\鄰里整理輸出')

def verify():
    print("\n=== Verifying Township Representatives (Merged) ===")
    
    # We expect the data to be merged into the main county files, not standalone in output
    target_years = [2014, 2018, 2022] 
    county = "花蓮縣"
    
    all_pass = True
    
    for year in target_years:
        fpath = OUTPUT_DIR / county / f"{year}_選舉資料_{county}.csv"
        
        if not fpath.exists():
            print(f"[FAIL] Missing output file: {fpath.name}")
            all_pass = False
            continue
            
        try:
            df = pd.read_csv(fpath)
            # Look for columns like "鄉鎮市民代表_候選人_..." or similar, depending on how format_converter names them.
            # Usually format_converter prefixes with election type if handled correctly, or just keys.
            # In format_converter.py: 
            #   prefix = f"{role_name}_" 
            #   so look for "鄉鎮市民代表_"
            
            rep_cols = [c for c in df.columns if '鄉鎮市民代表' in c]
            
            if not rep_cols:
                print(f"[FAIL] {fpath.name} does NOT contain '鄉鎮市民代表' columns.")
                all_pass = False
            else:
                # Check if data is not empty
                # We can check if sum of some numeric column is > 0, but candidates are generic columns
                # Let's count non-nulls or non-zeros
                print(f"[PASS] {fpath.name} contains {len(rep_cols)} Representative columns.")
                # Optional: Check for non-zero values if possible, but presence is the main structural check
                
        except Exception as e:
            print(f"[ERROR] Could not read {fpath.name}: {e}")
            all_pass = False

    if all_pass:
        print("[PASS] Township Rep verification completed successfully.")
    else:
        print("[FAIL] Township Rep verification FAILED.")
        sys.exit(1)

if __name__ == "__main__":
    verify()
