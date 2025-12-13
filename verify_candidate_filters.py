import pandas as pd
from pathlib import Path
import sys

OUTPUT_DIR = Path(__file__).parent / "鄰里整理輸出"

def verify():
    print("\n=== Verifying Candidate Filtering ===")
    input_files = list(OUTPUT_DIR.rglob("*立法委員*.csv"))
    
    all_pass = True
    for path in input_files:
        df = pd.read_csv(path)
        
        # Check column count
        cand_cols = [c for c in df.columns if '候選人名稱' in c]
        count = len(cand_cols)
        
        # Heuristic: Hualien Regional + Indigenous should be around 10-20 max, definitely not 100
        if count > 50:
             print(f"[FAIL] {path.name} has {count} candidates! Too many.")
             all_pass = False
        else:
             print(f"[PASS] {path.name} has {count} candidates (Reasonable).")

    if all_pass:
         print("[PASS] Candidate count verification PASSED.")
    else:
         print("[FAIL] Candidate count verification FAILED.")
         sys.exit(1)

if __name__ == "__main__":
    verify()
