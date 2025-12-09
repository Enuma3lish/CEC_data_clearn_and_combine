import pandas as pd
from pathlib import Path
import shutil

target_dir = Path('各縣市候選人分類')
years = [2016, 2020, 2024]

for year in years:
    for county_folder in target_dir.iterdir():
        if not county_folder.is_dir():
            continue
        
        county = county_folder.name
        
        # Find all district-specific legislator files for this year
        district_files = list(county_folder.glob(f'{year}_立法委員_第*.csv'))
        
        if not district_files:
            continue
        
        # If only one district, just rename it to the generic name
        if len(district_files) == 1:
            generic_name = county_folder / f'{year}_立法委員.csv'
            shutil.copy2(district_files[0], generic_name)
            print(f"Copied: {county}/{year}_立法委員.csv (from {district_files[0].name})")
        else:
            # Multiple districts - merge them
            dfs = []
            for f in sorted(district_files):
                df = pd.read_csv(f, encoding='utf-8-sig')
                dfs.append(df)
            
            merged = pd.concat(dfs, ignore_index=True)
            output_file = county_folder / f'{year}_立法委員.csv'
            merged.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"Merged {len(dfs)} files: {county}/{year}_立法委員.csv")

print("\nCompleted!")
