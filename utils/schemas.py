"""
Unified Schema Definition Module
定義統一的資料結構與欄位對照
"""

from typing import Dict, List, Any, Optional
import pandas as pd
from loguru import logger


class UnifiedSchema:
    """
    統一資料結構定義
    所有選舉類型都會被轉換成這個統一格式
    """

    # 統一欄位定義
    UNIFIED_FIELDS = {
        # 選舉基本資訊
        '選舉年份': int,
        '選舉類型': str,  # 總統、立委、縣市長等
        '選舉層級': str,  # 全國、縣市、鄉鎮市區、村里

        # 區域代碼 (主鍵)
        '省市代碼': str,
        '縣市代碼': str,
        '鄉鎮市區代碼': str,
        '村里代碼': str,
        '區域名稱': str,

        # 候選人資訊
        '候選人編號': str,
        '候選人姓名': str,
        '候選人號次': str,
        '政黨代碼': str,
        '政黨名稱': str,
        '性別': str,
        '出生年': str,
        '學歷': str,
        '現任': str,

        # 投票結果
        '得票數': int,
        '得票率': float,
        '當選': str,

        # 元資料
        '資料來源': str,
        '爬取時間': str
    }

    # 欄位順序 (用於輸出CSV時的欄位排序)
    FIELD_ORDER = [
        # 選舉資訊
        '選舉年份', '選舉類型', '選舉層級',

        # 區域代碼
        '省市代碼', '縣市代碼', '鄉鎮市區代碼', '村里代碼', '區域名稱',

        # 候選人資訊
        '候選人號次', '候選人姓名', '候選人編號',
        '政黨代碼', '政黨名稱', '性別', '出生年', '學歷', '現任',

        # 投票結果
        '得票數', '得票率', '當選',

        # 元資料
        '資料來源', '爬取時間'
    ]

    @classmethod
    def get_empty_dataframe(cls) -> pd.DataFrame:
        """
        建立空的統一格式 DataFrame

        Returns:
            空的 DataFrame,包含所有統一欄位
        """
        return pd.DataFrame(columns=cls.FIELD_ORDER)

    @classmethod
    def validate_dataframe(cls, df: pd.DataFrame) -> bool:
        """
        驗證 DataFrame 是否符合統一格式

        Args:
            df: 要驗證的 DataFrame

        Returns:
            是否符合統一格式
        """
        required_fields = [
            '選舉年份', '選舉類型', '區域名稱',
            '候選人姓名', '得票數', '得票率'
        ]

        missing_fields = [f for f in required_fields if f not in df.columns]

        if missing_fields:
            logger.warning(f"Missing required fields: {missing_fields}")
            return False

        return True


class FieldMapper:
    """
    欄位對照器
    將不同選舉類型的原始欄位對照到統一格式
    """

    # 總統選舉欄位對照
    PRESIDENTIAL_MAPPING = {
        'prv_code': '省市代碼',
        'city_code': '縣市代碼',
        'dept_code': '鄉鎮市區代碼',
        'li_code': '村里代碼',
        'area_name': '區域名稱',
        'cand_id': '候選人編號',
        'cand_name': '候選人姓名',
        'cand_no': '候選人號次',
        'party_code': '政黨代碼',
        'party_name': '政黨名稱',
        'cand_sex': '性別',
        'cand_birthyear': '出生年',
        'cand_edu': '學歷',
        'is_current': '現任',
        'ticket_num': '得票數',
        'ticket_percent': '得票率',
        'is_victor': '當選註記',
        'election_year': '選舉年份',
        'data_level': '資料層級'
    }

    # 縣市長選舉欄位對照
    MAYOR_MAPPING = {
        'prv_code': '省市代碼',
        'city_code': '縣市代碼',
        'dept_code': '鄉鎮市區代碼',
        'li_code': '村里代碼',
        'area_name': '區域名稱',
        'cand_id': '候選人編號',
        'cand_name': '候選人姓名',
        'cand_no': '候選人號次',
        'party_code': '政黨代碼',
        'party_name': '政黨名稱',
        'cand_sex': '性別',
        'cand_birthyear': '出生年',
        'cand_edu': '學歷',
        'is_current': '現任',
        'ticket_num': '得票數',
        'ticket_percent': '得票率',
        'is_victor': '當選註記',
        'election_year': '選舉年份',
        'data_level': '資料層級'
    }

    # 縣市議員選舉欄位對照
    COUNCILOR_MAPPING = {
        'prv_code': '省市代碼',
        'city_code': '縣市代碼',
        'dept_code': '鄉鎮市區代碼',
        'li_code': '村里代碼',
        'area_name': '區域名稱',
        'cand_id': '候選人編號',
        'cand_name': '候選人姓名',
        'cand_no': '候選人號次',
        'party_code': '政黨代碼',
        'party_name': '政黨名稱',
        'cand_sex': '性別',
        'cand_birthyear': '出生年',
        'cand_edu': '學歷',
        'is_current': '現任',
        'ticket_num': '得票數',
        'ticket_percent': '得票率',
        'is_victor': '當選註記',
        'election_year': '選舉年份',
        'data_level': '資料層級'
    }

    # 區域立委選舉欄位對照
    LEGISLATIVE_REGIONAL_MAPPING = {
        'prv_code': '省市代碼',
        'city_code': '縣市代碼',
        'dept_code': '鄉鎮市區代碼',
        'li_code': '村里代碼',
        'area_name': '區域名稱',
        'cand_id': '候選人編號',
        'cand_name': '候選人姓名',
        'cand_no': '候選人號次',
        'party_code': '政黨代碼',
        'party_name': '政黨名稱',
        'cand_sex': '性別',
        'cand_birthyear': '出生年',
        'cand_edu': '學歷',
        'is_current': '現任',
        'ticket_num': '得票數',
        'ticket_percent': '得票率',
        'is_victor': '當選註記',
        'election_year': '選舉年份',
        'data_level': '資料層級'
    }

    # 不分區立委選舉欄位對照
    LEGISLATIVE_AT_LARGE_MAPPING = {
        'prv_code': '省市代碼',
        'city_code': '縣市代碼',
        'dept_code': '鄉鎮市區代碼',
        'li_code': '村里代碼',
        'area_name': '區域名稱',
        'cand_id': '候選人編號',
        'cand_name': '候選人姓名',  # 可能是政黨名稱
        'cand_no': '候選人號次',   # 政黨號次
        'party_code': '政黨代碼',
        'party_name': '政黨名稱',
        'ticket_num': '得票數',
        'ticket_percent': '得票率',
        'is_victor': '當選註記',
        'election_year': '選舉年份',
        'data_level': '資料層級'
    }

    @classmethod
    def map_fields(cls, df: pd.DataFrame, election_type: str) -> pd.DataFrame:
        """
        將原始資料欄位對照到統一格式

        Args:
            df: 原始資料 DataFrame
            election_type: 選舉類型

        Returns:
            對照後的 DataFrame
        """
        if df is None or df.empty:
            return UnifiedSchema.get_empty_dataframe()

        # 選擇對應的欄位對照表
        mapping = cls._get_mapping(election_type)

        # 複製資料避免修改原始資料
        df = df.copy()

        # 重新命名欄位
        df = df.rename(
            columns={k: v for k, v in mapping.items() if k in df.columns})

        # 處理特殊欄位
        df = cls._process_special_fields(df, election_type)

        # 確保所有統一欄位都存在
        for field in UnifiedSchema.FIELD_ORDER:
            if field not in df.columns:
                df[field] = ''

        # 按照統一順序排列欄位
        df = df[UnifiedSchema.FIELD_ORDER]

        return df

    @classmethod
    def _get_mapping(cls, election_type: str) -> Dict[str, str]:
        """
        根據選舉類型獲取欄位對照表
        """
        mappings = {
            'presidential': cls.PRESIDENTIAL_MAPPING,
            'mayor': cls.MAYOR_MAPPING,
            'councilor': cls.COUNCILOR_MAPPING,
            'legislative_regional': cls.LEGISLATIVE_REGIONAL_MAPPING,
            'legislative_at_large': cls.LEGISLATIVE_AT_LARGE_MAPPING
        }

        return mappings.get(election_type, {})

    @classmethod
    def _process_special_fields(cls, df: pd.DataFrame, election_type: str) -> pd.DataFrame:
        """
        處理特殊欄位的轉換
        """
        # 處理當選註記
        if '當選註記' in df.columns:
            df['當選'] = df['當選註記'].apply(lambda x: '是' if x == '*' else '否')

        # 處理性別
        if '性別' in df.columns:
            df['性別'] = df['性別'].apply(
                lambda x: '男' if str(x) == '1' else (
                    '女' if str(x) == '2' else str(x))
            )

        # 處理現任
        if '現任' in df.columns:
            df['現任'] = df['現任'].apply(lambda x: '是' if x == 'Y' else '否')

        # 標準化區域代碼格式
        for code_field in ['省市代碼', '縣市代碼', '鄉鎮市區代碼', '村里代碼']:
            if code_field in df.columns:
                df[code_field] = df[code_field].astype(str).fillna('0')

        # 處理數值欄位
        if '得票數' in df.columns:
            df['得票數'] = pd.to_numeric(
                df['得票數'], errors='coerce').fillna(0).astype(int)

        if '得票率' in df.columns:
            df['得票率'] = pd.to_numeric(df['得票率'], errors='coerce').fillna(0.0)

        # 添加選舉類型
        if election_type == 'presidential':
            df['選舉類型'] = '總統'
        elif election_type == 'mayor':
            df['選舉類型'] = '縣市長'
        elif election_type == 'councilor':
            df['選舉類型'] = '縣市議員'
        elif election_type == 'legislative_regional':
            df['選舉類型'] = '區域立委'
        elif election_type == 'legislative_at_large':
            df['選舉類型'] = '不分區立委'

        # 處理資料層級
        if '資料層級' in df.columns:
            level_mapping = {
                'N': '全國',
                'C': '縣市',
                'D': '鄉鎮市區',
                'L': '村里'
            }
            df['選舉層級'] = df['資料層級'].map(level_mapping).fillna(df['資料層級'])

        return df
