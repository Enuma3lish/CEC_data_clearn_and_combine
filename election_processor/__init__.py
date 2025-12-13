"""
選舉資料處理模組
Taiwan CEC Election Data Processor

支援年份：2014-2024
輸出格式：符合「鄰里整理範例」格式
"""

from .config import COUNTY_CONFIG, PARTY_CODE_MAP, YEAR_FOLDER_MAP, LOCAL_YEARS, PRESIDENTIAL_YEARS
from .processor import ElectionProcessor
from .utils import find_file, load_csv_safe, normalize_district
from .merge import merge_election_results, process_local_election, process_presidential_election

__all__ = [
    'COUNTY_CONFIG',
    'PARTY_CODE_MAP',
    'YEAR_FOLDER_MAP',
    'LOCAL_YEARS',
    'PRESIDENTIAL_YEARS',
    'ElectionProcessor',
    'find_file',
    'load_csv_safe',
    'normalize_district',
    'merge_election_results',
    'process_local_election',
    'process_presidential_election',
]

__version__ = '1.0.0'
