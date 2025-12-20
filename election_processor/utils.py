# -*- coding: utf-8 -*-
"""
選舉資料處理系統 - 工具函數
Utility functions for election data processor
"""

import os
import pandas as pd

from .config import PARTY_CODE_MAP


def clean_val(x):
    """清理值（移除引號等）

    Args:
        x: 輸入值

    Returns:
        清理後的字串
    """
    if pd.isna(x):
        return ''
    return str(x).replace("'", '').strip()


def clean_number(x):
    """清理並轉換數字

    Args:
        x: 輸入值

    Returns:
        整數值，無效時返回 0
    """
    if pd.isna(x):
        return 0
    val = clean_val(x).replace(',', '')
    try:
        return int(float(val))
    except:
        return 0


def read_csv_clean(filepath):
    """讀取並清理 CSV 檔案

    Args:
        filepath: CSV 檔案路徑

    Returns:
        清理後的 DataFrame
    """
    df = pd.read_csv(filepath, header=None, dtype=str)
    for col in df.columns:
        df[col] = df[col].apply(clean_val)
    return df


def load_party_map(elpaty_file):
    """載入政黨對照表

    Args:
        elpaty_file: elpaty.csv 檔案路徑
    """
    global PARTY_CODE_MAP
    if os.path.exists(elpaty_file):
        df = pd.read_csv(elpaty_file, header=None, dtype=str)
        for _, row in df.iterrows():
            code = clean_val(row[0])
            name = clean_val(row[1])
            PARTY_CODE_MAP[code] = name


def get_party_name(code):
    """取得政黨名稱

    Args:
        code: 政黨代碼

    Returns:
        政黨名稱
    """
    if code in PARTY_CODE_MAP:
        return PARTY_CODE_MAP[code]
    if code in ('', '0', '00'):
        return '無黨籍'
    return '無黨籍'
