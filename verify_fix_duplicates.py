import pandas as pd
from pathlib import Path
import sys

OUTPUT_DIR = Path(__file__).parent / "鄰里整理輸出"

def verify():
    all_pass = True
    print("\n=== Verifying Duplicates ===")
    
    files = list(OUTPUT_DIR.rglob("*.csv"))
    if not files:
        print("[ERROR] No output files found!")
        sys.exit(1)
        
    for path in files:
        try:
            df = pd.read_csv(path)
            if '鄰里' not in df.columns or '行政區別' not in df.columns:
                continue
                
            dups = df[df.duplicated(subset=['行政區別', '鄰里'], keep=False)]
            if not dups.empty:
                print(f"[FAIL] Duplicates found in {path.name} ({len(dups)} rows)")
                all_pass = False
            else:
                pass 
                # print(f"[PASS] {path.name}")
        except Exception as e:
            print(f"[ERROR] Could not read {path.name}: {e}")
            all_pass = False

    if all_pass:
        print("[PASS] ALL FILES PASSED: No duplicate neighborhoods found.")
    else:
        print("[FAIL] VERIFICATION FAILED: Duplicates still exist.")
        sys.exit(1)

if __name__ == "__main__":
    verify()
