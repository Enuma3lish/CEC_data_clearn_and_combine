"""
Regional Code Management Module
處理台灣行政區域代碼的標準化與驗證
"""

import re
from typing import Dict, Optional, Tuple
from loguru import logger


class RegionalCodeManager:
    """
    管理台灣行政區域代碼

    代碼層級:
    - 省市代碼 (prv_code): 2位數字
    - 縣市代碼 (city_code): 3位數字
    - 鄉鎮市區代碼 (dept_code): 2位數字
    - 村里代碼 (li_code): 3位數字

    標準格式: prv_code,city_code,dept_code,li_code
    範例: 63,000,00,000 (台北市全市)
    """

    # 縣市代碼對照表 (根據CEC資料)
    CITY_CODES = {
        # 直轄市
        '63000': '臺北市',
        '65000': '新北市',
        '68000': '桃園市',
        '66000': '臺中市',
        '67000': '臺南市',
        '64000': '高雄市',

        # 省轄市
        '10017': '基隆市',
        '10018': '新竹市',
        '10020': '嘉義市',

        # 縣
        '10002': '宜蘭縣',
        '10004': '新竹縣',
        '10005': '苗栗縣',
        '10007': '彰化縣',
        '10008': '南投縣',
        '10009': '雲林縣',
        '10010': '嘉義縣',
        '10013': '屏東縣',
        '10014': '臺東縣',
        '10015': '花蓮縣',
        '10016': '澎湖縣',

        # 福建省
        '09007': '連江縣',
        '09020': '金門縣',
    }

    # 反向對照表
    CITY_NAMES = {v: k for k, v in CITY_CODES.items()}

    @staticmethod
    def standardize_code(prv_code: str, city_code: str, dept_code: str,
                         li_code: str) -> Dict[str, str]:
        """
        標準化區域代碼

        Args:
            prv_code: 省市代碼
            city_code: 縣市代碼
            dept_code: 鄉鎮市區代碼
            li_code: 村里代碼

        Returns:
            標準化後的代碼字典
        """
        # 確保代碼格式正確
        prv_code = str(prv_code).zfill(2) if prv_code else '00'
        city_code = str(city_code).zfill(3) if city_code else '000'
        dept_code = str(dept_code).zfill(2) if dept_code else '00'
        li_code = str(li_code).zfill(3) if li_code else '000'

        return {
            '省市代碼': prv_code,
            '縣市代碼': city_code,
            '鄉鎮市區代碼': dept_code,
            '村里代碼': li_code,
            '完整代碼': f"{prv_code},{city_code},{dept_code},{li_code}"
        }

    @staticmethod
    def parse_code(unified_code: str) -> Dict[str, str]:
        """
        解析統一代碼格式

        Args:
            unified_code: 格式如 "63,000,00,000"

        Returns:
            解析後的代碼字典
        """
        parts = unified_code.split(',')
        if len(parts) != 4:
            logger.warning(f"Invalid code format: {unified_code}")
            return {
                '省市代碼': '00',
                '縣市代碼': '000',
                '鄉鎮市區代碼': '00',
                '村里代碼': '000'
            }

        return {
            '省市代碼': parts[0],
            '縣市代碼': parts[1],
            '鄉鎮市區代碼': parts[2],
            '村里代碼': parts[3]
        }

    @classmethod
    def validate_code(cls, prv_code: str, city_code: str) -> bool:
        """
        驗證區域代碼是否有效

        Args:
            prv_code: 省市代碼
            city_code: 縣市代碼

        Returns:
            是否為有效代碼
        """
        full_code = f"{prv_code}{city_code}"
        return full_code in cls.CITY_CODES

    @classmethod
    def get_region_name(cls, prv_code: str, city_code: str) -> Optional[str]:
        """
        根據代碼獲取區域名稱

        Args:
            prv_code: 省市代碼
            city_code: 縣市代碼

        Returns:
            區域名稱,如果代碼無效則返回 None
        """
        full_code = f"{prv_code}{city_code}"
        return cls.CITY_CODES.get(full_code)

    @classmethod
    def get_code_by_name(cls, region_name: str) -> Optional[str]:
        """
        根據區域名稱獲取代碼

        Args:
            region_name: 區域名稱

        Returns:
            區域代碼,如果名稱無效則返回 None
        """
        return cls.CITY_NAMES.get(region_name)

    @staticmethod
    def get_hierarchy(prv_code: str, city_code: str, dept_code: str,
                      li_code: str) -> Dict[str, str]:
        """
        獲取區域層級資訊

        Args:
            prv_code: 省市代碼
            city_code: 縣市代碼
            dept_code: 鄉鎮市區代碼
            li_code: 村里代碼

        Returns:
            層級資訊字典
        """
        level = "未知"

        if li_code and li_code != '000' and li_code != '0000':
            level = "村里"
        elif dept_code and dept_code != '00' and dept_code != '000':
            level = "鄉鎮市區"
        elif city_code and city_code != '000' and city_code != '0000':
            level = "縣市"
        elif prv_code and prv_code != '00':
            level = "省市"
        else:
            level = "全國"

        return {
            '層級': level,
            '省市代碼': prv_code,
            '縣市代碼': city_code,
            '鄉鎮市區代碼': dept_code,
            '村里代碼': li_code
        }

    @classmethod
    def get_all_cities(cls) -> Dict[str, str]:
        """
        獲取所有縣市代碼對照表

        Returns:
            縣市代碼與名稱對照字典
        """
        return cls.CITY_CODES.copy()
