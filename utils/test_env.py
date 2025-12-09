
import sys
import os
# Add parent dir to path so we can import local modules if needed (not needed for pandas/numpy check)
# sys.path.append(os.path.dirname(os.getcwd()))

with open("env_status_utils.txt", "w", encoding="utf-8") as f:
    f.write(f"Executable: {sys.executable}\n")
    f.write(f"Cwd: {os.getcwd()}\n")
    try:
        import pandas
        f.write(f"Pandas version: {pandas.__version__}\n")
    except ImportError as e:
        f.write(f"ImportError: {e}\n")
    except Exception as e:
        f.write(f"Error: {e}\n")
