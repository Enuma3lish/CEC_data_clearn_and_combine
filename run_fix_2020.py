import sys
from pathlib import Path
# Ensure we can import main
sys.path.append(r'c:\Users\melo\OneDrive\Desktop\CEC_data_clearn_and_combine')
try:
    from main import repair_legislator_data, process_year_data, RAW_DIR, OUTPUT_DIR
except ImportError:
    # Fallback if PWD is not set
    sys.path.append(str(Path(__file__).parent))
    from main import repair_legislator_data, process_year_data, RAW_DIR, OUTPUT_DIR

def run():
    print("Starting 2020/2024 Fix...")
    
    # 2020
    print("Repairing 2020 Legislator Data...")
    repair_legislator_data("花蓮縣", 2020, "2020_立法委員.csv", 10, 15)
    print("Processing 2020 Data...")
    process_year_data(2020, "花蓮縣", RAW_DIR, OUTPUT_DIR)

    # 2024 (Also ensure this is clean)
    print("Repairing 2024 Legislator Data...")
    repair_legislator_data("花蓮縣", 2024, "2024_立法委員.csv", 10, 15)
    print("Processing 2024 Data...")
    process_year_data(2024, "花蓮縣", RAW_DIR, OUTPUT_DIR)
    
    print("Done.")

if __name__ == "__main__":
    run()
