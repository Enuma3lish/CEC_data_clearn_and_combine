import shutil
from pathlib import Path

source_dir = Path('processed_data_unified')
target_dir = Path('各縣市候選人分類')
years = [2016, 2020, 2024]
count = 0

for year in years:
    for county_folder in source_dir.iterdir():
        if county_folder.is_dir():
            county = county_folder.name
            target_county = target_dir / county
            target_county.mkdir(exist_ok=True)
            
            for src_file in county_folder.glob(f'{year}_*.csv'):
                dst_file = target_county / src_file.name.replace(f'_{county}', '')
                shutil.copy2(src_file, dst_file)
                count += 1

print(f'Copied {count} files successfully')
