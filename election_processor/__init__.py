# -*- coding: utf-8 -*-
"""
選舉資料處理模組
Taiwan CEC Election Data Processor

模組架構：
- config.py: 設定檔（路徑、縣市代碼）
- election_types.py: 選舉類型配置（便於新增選舉類型）
- utils.py: 工具函數
- base.py: 基礎處理函數（統一處理入口）
- processor.py: 資料處理函數（向後相容包裝器）
- output.py: 輸出函數（統一輸出入口）

功能：
- 處理 2014 縣市議員、縣市長、鄉鎮市長選舉資料
- 處理 2020 總統、區域立委、山地立委、平地立委、政黨票選舉資料
- 輸出 Excel 格式檔案

使用方式：
    # 推薦：使用統一入口
    from election_processor import (
        process_election,
        save_election_excel,
        get_election_config,
    )

    election_type = get_election_config('president')
    result = process_election(election_type, data_dir, prv_code, city_code, city_name)
    save_election_excel(result, output_path, election_type, city_name)

    # 向後相容：使用舊版函數
    from election_processor import process_president, save_president_excel
    result = process_president(data_dir, prv_code, city_code, city_name)
    save_president_excel(result, output_path, city_name, year)
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
    ELECTION_TYPES_2016,
    ELECTION_TYPES_2018,
    ELECTION_TYPES_2020,
    ELECTION_TYPES_2022,
    ELECTION_TYPES_2024,
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

# Base Processing Functions (Unified API)
from .base import (
    # Unified entry point (recommended)
    process_election,
    # Low-level functions
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

# Processor Functions (legacy compatibility wrappers)
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
    # Unified entry point (recommended)
    save_election_excel,
    # Legacy functions
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
    # Unified API (recommended)
    'process_election',
    'save_election_excel',
    # Base Processing (low-level)
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
    # Processor (legacy wrappers)
    'process_council_municipality',
    'process_council_county',
    'process_mayor_municipality',
    'process_mayor_county',
    'process_legislator',
    'process_president',
    'process_township_mayor',
    'process_indigenous_legislator',
    'process_party_vote',
    # Output (legacy functions)
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

__version__ = '3.0.0'
