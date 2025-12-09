#!/usr/bin/env python3
"""Regenerate all president and legislator intermediate CSVs for 2016, 2020, 2024"""

import sys
import shutil
from pathlib import Path
sys.path.append('.')

from processors.process_election_unified import UnifiedElectionProcessor

# Initialize processor
base_path = Path('voteData')
processor = UnifiedElectionProcessor(base_path)

# Process years with president and legislator elections
years = [2016, 2020, 2024]

for year in years:
    year_folder = Path(f'voteData/{year}總統立委')
    if not year_folder.exists():
        print(f"Warning: {year_folder} not found, skipping")
        continue
    
    print(f"\n{'='*60}")
    print(f"Processing {year} data...")
    print(f"{'='*60}")
    
    # Process president
    president_folder = year_folder / '總統'
    if president_folder.exists():
        print(f"\n處理: {year} 總統")
        processor.process_election_folder(
            president_folder,
            year=year,
            election_type='總統',
            aggregate_level='village',
            is_president=True
        )
    
    # Process regional legislators
    legislator_folder = year_folder / '區域立委'
    if legislator_folder.exists():
        print(f"\n處理: {year} 區域立委")
        processor.process_election_folder(
            legislator_folder,
            year=year,
            election_type='立法委員',
            aggregate_level='polling',
            is_president=False
        )

print("\n" + "="*60)
print("Regeneration complete! Copying files to 各縣市候選人分類...")
print("="*60)

# Copy files from processed_data_unified to 各縣市候選人分類
source_dir = Path('processed_data_unified')
target_dir = Path('各縣市候選人分類')

for year in years:
    for county_folder in source_dir.iterdir():
        if county_folder.is_dir():
            county = county_folder.name
            
            # Copy president file
            president_src = county_folder / f'{year}_總統_{county}.csv'
            president_dst = target_dir / county / f'{year}_總統.csv'
            if president_src.exists():
                president_dst.parent.mkdir(exist_ok=True)
                shutil.copy2(president_src, president_dst)
                print(f"  OK: {county}/{year}_總統.csv")
            
            # Copy legislator files (may have multiple files for different districts)
            for legislator_file in county_folder.glob(f'{year}_立法委員*.csv'):
                # Map from "2020_立法委員_第01選區_臺北市.csv" to "2020_立法委員.csv"
                # or keep district-specific naming if needed
                legislator_dst = target_dir / county / legislator_file.name.replace(f'_{county}', '')
                legislator_dst.parent.mkdir(exist_ok=True)
                shutil.copy2(legislator_file, legislator_dst)
                print(f"  OK: {county}/{legislator_file.name}")

print("\n完成！All files regenerated and copied.")
