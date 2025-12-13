"""
選舉資料處理工具函數
Utility functions for election data processing
"""

import re
import pandas as pd
from pathlib import Path


def find_file(directory: Path, candidates: list) -> Path:
    """在目錄中尋找存在的檔案

    Args:
        directory: 搜尋目錄
        candidates: 候選檔名列表

    Returns:
        找到的檔案路徑，或 None
    """
    for name in candidates:
        path = directory / name
        if path.exists():
            return path
    return None


def load_csv_safe(filepath: Path, **kwargs) -> pd.DataFrame:
    """安全讀取 CSV，自動嘗試多種編碼

    Args:
        filepath: CSV 檔案路徑
        **kwargs: 傳遞給 pd.read_csv 的額外參數

    Returns:
        DataFrame 或 None（讀取失敗時）
    """
    encodings = ['utf-8', 'utf-8-sig', 'cp950', 'big5']
    for enc in encodings:
        try:
            return pd.read_csv(filepath, encoding=enc, **kwargs)
        except:
            continue
    return None


def normalize_district(name: str) -> str:
    """正規化行政區名稱

    移除選舉區字樣和縣市前綴

    Args:
        name: 原始行政區名稱

    Returns:
        正規化後的名稱
    """
    if not isinstance(name, str):
        return name

    # 移除選舉區字樣
    name = re.sub(r'第\d+選舉?區', '', name)

    # 移除縣市前綴
    prefixes = (
        r'^(臺北市|新北市|桃園市|臺中市|臺南市|高雄市|基隆市|新竹市|嘉義市|'
        r'宜蘭縣|新竹縣|苗栗縣|彰化縣|南投縣|雲林縣|嘉義縣|屏東縣|'
        r'臺東縣|花蓮縣|澎湖縣|金門縣|連江縣)'
    )
    return re.sub(prefixes, '', name)


def clean_string(s) -> str:
    """清理字串，移除引號和空白

    Args:
        s: 輸入值

    Returns:
        清理後的字串
    """
    if pd.isna(s):
        return ''
    return str(s).replace("'", "").replace('"', '').strip()
