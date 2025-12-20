# -*- coding: utf-8 -*-
"""
選舉資料處理系統 - 資料處理函數
Data processing functions for election data processor

本模組提供向後相容的處理函數介面。
所有函數都是 base.process_election() 的薄包裝器。

新程式碼建議直接使用：
    from election_processor import process_election, get_election_config

    election_type = get_election_config('president')
    result = process_election(election_type, data_dir, prv_code, city_code, city_name)
"""

from .base import process_election
from .election_types import get_election_config


# =============================================================================
# 2014 選舉處理函數
# =============================================================================

def process_council_municipality(data_dir, prv_code, city_name):
    """處理直轄市區域議員選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省市代碼
        city_name: 縣市名稱

    Returns:
        dict: 各選區的 DataFrame 和候選人資訊
    """
    election_type = get_election_config('council_municipality')
    return process_election(election_type, data_dir, prv_code, '000', city_name)


def process_council_county(data_dir, prv_code, city_code, city_name):
    """處理縣市區域議員選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省代碼
        city_code: 縣市代碼
        city_name: 縣市名稱

    Returns:
        dict: 各選區的 DataFrame 和候選人資訊
    """
    election_type = get_election_config('council_county')
    return process_election(election_type, data_dir, prv_code, city_code, city_name)


def process_mayor_municipality(data_dir, prv_code, city_name):
    """處理直轄市市長選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省市代碼
        city_name: 縣市名稱

    Returns:
        dict: DataFrame 和候選人資訊
    """
    election_type = get_election_config('mayor_municipality')
    return process_election(election_type, data_dir, prv_code, '000', city_name)


def process_mayor_county(data_dir, prv_code, city_code, city_name):
    """處理縣市市長選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省代碼
        city_code: 縣市代碼
        city_name: 縣市名稱

    Returns:
        dict: DataFrame 和候選人資訊
    """
    election_type = get_election_config('mayor_county')
    return process_election(election_type, data_dir, prv_code, city_code, city_name)


def process_township_mayor(data_dir, prv_code, city_code, city_name):
    """處理鄉鎮市長選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省代碼
        city_code: 縣市代碼
        city_name: 縣市名稱

    Returns:
        dict: 各選區的 DataFrame 和候選人資訊
    """
    election_type = get_election_config('township_mayor')
    return process_election(election_type, data_dir, prv_code, city_code, city_name)


# =============================================================================
# 2020 選舉處理函數
# =============================================================================

def process_president(data_dir, prv_code, city_code, city_name):
    """處理總統選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省市代碼
        city_code: 縣市代碼（直轄市為 '000'）
        city_name: 縣市名稱

    Returns:
        dict: DataFrame 和候選人資訊
    """
    election_type = get_election_config('president')
    return process_election(election_type, data_dir, prv_code, city_code, city_name)


def process_legislator(data_dir, prv_code, city_code, city_name):
    """處理區域立法委員選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省市代碼
        city_code: 縣市代碼（直轄市為 '000'）
        city_name: 縣市名稱

    Returns:
        dict: 各選區的 DataFrame 和候選人資訊
    """
    election_type = get_election_config('legislator')
    return process_election(election_type, data_dir, prv_code, city_code, city_name)


def process_indigenous_legislator(data_dir, prv_code, city_code, city_name, legislator_type='mountain'):
    """處理原住民立法委員選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省市代碼
        city_code: 縣市代碼（直轄市為 '000'）
        city_name: 縣市名稱
        legislator_type: 'mountain' 或 'plain'

    Returns:
        dict: DataFrame 和候選人資訊
    """
    if legislator_type == 'mountain':
        election_type = get_election_config('mountain_legislator')
    else:
        election_type = get_election_config('plain_legislator')
    return process_election(election_type, data_dir, prv_code, city_code, city_name)


def process_party_vote(data_dir, prv_code, city_code, city_name):
    """處理政黨票選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省市代碼
        city_code: 縣市代碼（直轄市為 '000'）
        city_name: 縣市名稱

    Returns:
        dict: DataFrame 和政黨資訊
    """
    election_type = get_election_config('party_vote')
    return process_election(election_type, data_dir, prv_code, city_code, city_name)
