#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Regenerate 2020 總統 intermediate CSV to fix statistics column bug
"""

import sys
from pathlib import Path
sys.path.append('.')

from processors.process_election_unified import UnifiedElectionProcessor

# Initialize processor
base_path = Path('voteData')
processor = UnifiedElectionProcessor(base_path)

# Process only 2020 總統
year_folder = Path('voteData/2020總統立委')
print(f"Processing: {year_folder}")

# Process each election type folder
for election_folder in year_folder.iterdir():
    if election_folder.is_dir() and election_folder.name == '總統':
        print(f"\n處理: {election_folder.name}")
        processor.process_election_folder(
            election_folder,
            year=2020,
            election_type='總統',
            aggregate_level='village',
            is_president=True
        )

print("\n完成！")
