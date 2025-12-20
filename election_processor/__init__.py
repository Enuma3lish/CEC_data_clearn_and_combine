# -*- coding: utf-8 -*-
"""
選舉資料處理模組
Taiwan CEC Election Data Processor

模組架構：
- config.py: 設定檔（路徑、縣市代碼）
- election_types.py: 選舉類型配置（便於新增選舉類型）
- utils.py: 工具函數
- base.py: 基礎處理函數（可重用）
- processor.py: 資料處理函數
- output.py: 輸出函數

功能：
- 處理 2014 縣市議員、縣市長、鄉鎮市長選舉資料
- 處理 2020 總統、區域立委、山地立委、平地立委、政黨票選舉資料
- 輸出 Excel 格式檔案
"""

# Config
from .config import (
    DATA_DIR,
    OUTPUT_DIR,
    YEAR_FOLDERS,
    MUNICIPALITIES,
    COUNTIES,
    ALL_CITIES,
    is_municipality,
    get_city_info,
)

# Election Types Configuration
from .election_types import (
    ElectionType,
    ELECTION_TYPES_2014,
    ELECTION_TYPES_2020,
    ALL_ELECTION_TYPES,
    ELECTION_TYPES_BY_YEAR,
    get_election_types,
    get_election_config,
    MERGE_CONFIGS,
    FILE_PATTERNS,
    MAX_CANDIDATES,
    STAT_FIELDS,
)

# Utils
from .utils import (
    clean_val,
    clean_number,
    read_csv_clean,
    load_party_map,
    get_party_name,
)

# Base Processing Functions
from .base import (
    load_election_data,
    filter_by_city,
    build_name_maps,
    build_candidate_list,
    build_stats_map,
    build_votes_map,
    calculate_totals,
    generate_rows,
    process_single_area_election,
    process_multi_area_election,
)

# Processor Functions (legacy compatibility)
from .processor import (
    process_council_municipality,
    process_council_county,
    process_mayor_municipality,
    process_mayor_county,
    process_legislator,
    process_president,
    process_township_mayor,
    process_indigenous_legislator,
    process_party_vote,
)

# Output Functions
from .output import (
    save_council_excel,
    save_mayor_excel,
    save_legislator_excel,
    save_president_excel,
    save_township_mayor_excel,
    save_indigenous_legislator_excel,
    save_party_vote_excel,
    create_city_combined_file,
    create_national_election_file,
)

__all__ = [
    # Config
    'DATA_DIR',
    'OUTPUT_DIR',
    'YEAR_FOLDERS',
    'MUNICIPALITIES',
    'COUNTIES',
    'ALL_CITIES',
    'is_municipality',
    'get_city_info',
    # Election Types
    'ElectionType',
    'ELECTION_TYPES_2014',
    'ELECTION_TYPES_2020',
    'ALL_ELECTION_TYPES',
    'ELECTION_TYPES_BY_YEAR',
    'get_election_types',
    'get_election_config',
    'MERGE_CONFIGS',
    'FILE_PATTERNS',
    'MAX_CANDIDATES',
    'STAT_FIELDS',
    # Utils
    'clean_val',
    'clean_number',
    'read_csv_clean',
    'load_party_map',
    'get_party_name',
    # Base Processing
    'load_election_data',
    'filter_by_city',
    'build_name_maps',
    'build_candidate_list',
    'build_stats_map',
    'build_votes_map',
    'calculate_totals',
    'generate_rows',
    'process_single_area_election',
    'process_multi_area_election',
    # Processor (legacy)
    'process_council_municipality',
    'process_council_county',
    'process_mayor_municipality',
    'process_mayor_county',
    'process_legislator',
    'process_president',
    'process_township_mayor',
    'process_indigenous_legislator',
    'process_party_vote',
    # Output
    'save_council_excel',
    'save_mayor_excel',
    'save_legislator_excel',
    'save_president_excel',
    'save_township_mayor_excel',
    'save_indigenous_legislator_excel',
    'save_party_vote_excel',
    'create_city_combined_file',
    'create_national_election_file',
]

__version__ = '2.1.0'
