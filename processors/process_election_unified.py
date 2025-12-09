#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
中選會選舉資料統一處理系統（2010-2024）
處理所有年份選舉資料並輸出為標準CSV格式

輸出檔名格式：{年份}_{選舉類型}_{縣市}.csv
"""

import pandas as pd
from pathlib import Path
import re


class ElectionYearConfig:
    """選舉年份配置類別"""

    # 資料夾映射配置
    FOLDER_MAPPINGS = {
        # 舊格式（無引號）
        '3屆立委': {'year': 1998, 'structure': 'old', 'name': '立法委員'},
        '5屆立委': {'year': 2004, 'structure': 'old', 'name': '立法委員'},
        '9任總統': {'year': 1996, 'structure': 'old', 'name': '總統'},
        '2010鄉鎮市民代表': {'year': 2010, 'structure': 'old', 'name': '鄉鎮市民代表'},

        # 引號格式（帶'前綴）
        '20101127-五都市長議員及里長': {'year': 2010, 'structure': 'chinese_folder_quote_prefix'},
        '20120114-總統及立委': {'year': 2012, 'structure': 'chinese_folder_quote_prefix'},
        '2014-103年地方公職人員選舉': {'year': 2014, 'structure': 'chinese_folder_quote_prefix'},
        '2018-107年地方公職人員選舉': {'year': 2018, 'structure': 'chinese_folder_quote_prefix'},

        # 引號格式（無'前綴）
        '2020總統立委': {'year': 2020, 'structure': 'chinese_folder_quote'},
        '2024總統立委': {'year': 2024, 'structure': 'chinese_folder_quote'},

        # 特殊處理
        '2016總統立委': {'year': 2016, 'structure': 'special_2016'},
        '2022-111年地方公職人員選舉': {'year': 2022, 'structure': 'code_system_2022'},
    }

    # 2022代碼映射
    CODE_2022_MAPPING = {
        'C1': {'name': '縣市長', 'aggregate': 'village'},
        'D1': {'name': '鄉鎮市長', 'aggregate': 'village'},
        'D2': {'name': '區長', 'aggregate': 'village'},
        'T1': {'name': '縣市議員_區域', 'aggregate': 'polling'},
        'T2': {'name': '縣市議員_平地原住民', 'aggregate': 'polling'},
        'T3': {'name': '縣市議員_山地原住民', 'aggregate': 'polling'},
        'R1': {'name': '鄉鎮市民代表_區域', 'aggregate': 'polling'},
        'R2': {'name': '鄉鎮市民代表_平地原住民', 'aggregate': 'polling'},
        'R3': {'name': '鄉鎮市民代表_山地原住民', 'aggregate': 'polling'},
        'V1': {'aggregate': 'exclude'},  # 排除村里長
    }

    # 中文資料夾映射（2014/2018/2020/2024）
    CHINESE_FOLDER_MAPPING = {
        # 直轄市層級 (2014/2018格式)
        '直轄市市長': {'name': '直轄市長', 'aggregate': 'village'},
        '直轄市區域議員': {'name': '直轄市議員_區域', 'aggregate': 'polling'},
        '直轄市山原議員': {'name': '直轄市議員_山地原住民', 'aggregate': 'polling'},
        '直轄市平原議員': {'name': '直轄市議員_平地原住民', 'aggregate': 'polling'},
        '直轄市區長': {'name': '直轄市區長', 'aggregate': 'village'},
        '直轄市區民代表': {'name': '直轄市區民代表', 'aggregate': 'polling'},

        # 直轄市層級 (2020/2024格式)
        '直轄市議員': {'name': '直轄市議員', 'aggregate': 'polling'},

        # 縣市層級 (2014/2018格式)
        '縣市市長': {'name': '縣市長', 'aggregate': 'village'},
        '縣市區域議員': {'name': '縣市議員_區域', 'aggregate': 'polling'},
        '縣市山原議員': {'name': '縣市議員_山地原住民', 'aggregate': 'polling'},
        '縣市平原議員': {'name': '縣市議員_平地原住民', 'aggregate': 'polling'},
        '縣市鄉鎮市長': {'name': '鄉鎮市長', 'aggregate': 'village'},
        '縣市鄉鎮市民代表': {'name': '鄉鎮市民代表', 'aggregate': 'polling'},
        '縣市鄉鎮市民平原代表': {'name': '鄉鎮市民代表_平地原住民', 'aggregate': 'polling'},

        # 縣市層級 (2020/2024格式)
        '縣市長': {'name': '縣市長', 'aggregate': 'village'},
        '縣市議員': {'name': '縣市議員', 'aggregate': 'polling'},

        # 鄉鎮層級
        '鄉鎮市長': {'name': '鄉鎮市長', 'aggregate': 'village'},
        '鄉鎮市民代表': {'name': '鄉鎮市民代表', 'aggregate': 'polling'},

        # 區長
        '區長': {'name': '區長', 'aggregate': 'village'},

        # 立委和總統
        '立法委員': {'name': '立法委員', 'aggregate': 'polling'},
        '區域立委': {'name': '立法委員', 'aggregate': 'polling'},
        '山地立委': {'name': '立法委員_山地原住民', 'aggregate': 'polling'},
        '平地立委': {'name': '立法委員_平地原住民', 'aggregate': 'polling'},
        '總統': {'name': '總統', 'aggregate': 'village'},

        # 市長 (20101127五都)
        '市長': {'name': '直轄市長', 'aggregate': 'village'},
        '市議員': {'name': '直轄市議員', 'aggregate': 'polling'},

        # 排除項目
        '村里長': {'aggregate': 'exclude'},
        '直轄市村里長': {'aggregate': 'exclude'},
        '縣市村里長': {'aggregate': 'exclude'},
        '鄰長': {'aggregate': 'exclude'},
        '不分區政黨': {'aggregate': 'exclude'},
        '里長': {'aggregate': 'exclude'},
    }

    # 2016縣市代碼轉換表（old → new）
    COUNTY_CODE_2016_MAPPING = {
        '01': '63',  # 臺北市
        '02': '64',  # 高雄市
        '03': '10',  # 新北市
        '04': '65',  # 臺中市
        '05': '67',  # 臺南市
        '06': '66',  # 桃園市
        # 其他縣市根據需要添加
    }

    # 鄉鎮代碼對應表（用於縣市代碼='10'的情況，2014年後使用）
    # 格式：鄉鎮代碼 -> 縣市名稱
    TOWNSHIP_CODE_MAPPING = {
        '002': '宜蘭縣',
        '004': '新竹縣',
        '005': '苗栗縣',
        '007': '彰化縣',
        '008': '南投縣',
        '009': '雲林縣',
        '010': '嘉義縣',
        '013': '屏東縣',
        '014': '臺東縣',
        '015': '花蓮縣',
        '016': '澎湖縣',
        '017': '基隆市',  # 省轄市
        '018': '新竹市',  # 省轄市
        '020': '嘉義市',  # 省轄市
        '025': '金門縣',
    }


def read_csv_auto_detect(file_path):
    """
    自動檢測並處理不同CSV格式

    處理三種格式：
    1. 舊格式（無引號）：00,000,00,000,0000,全國
    2. 引號格式（帶'前綴）："'00","'000","'00","'000","'0000","全國"
    3. 引號格式（無'前綴）："63","000","00","000","0000","臺北市"

    Returns:
        DataFrame: 處理後的資料
    """
    encodings = ['utf-8', 'big5', 'cp950', 'gb18030']

    for encoding in encodings:
        try:
            # 嘗試讀取檔案
            df = pd.read_csv(file_path, header=None, encoding=encoding, dtype=str)

            # 檢測是否有單引號前綴
            # 檢查第一列第一個值是否以單引號開頭
            first_value = str(df.iloc[0, 0])

            if first_value.startswith("'"):
                # 格式2：引號格式（帶'前綴）
                # 清除所有欄位的單引號前綴
                for col in df.columns:
                    df[col] = df[col].astype(str).str.replace("^'", "", regex=True)

            return df

        except (UnicodeDecodeError, UnicodeError):
            continue

    raise ValueError(f"無法讀取檔案: {file_path}")


class UnifiedElectionProcessor:
    """統一選舉資料處理器（2010-2024）"""

    def __init__(self, base_path, output_base=None):
        self.base_path = Path(base_path)
        if output_base is None:
            self.output_base = Path('processed_data_unified')
        else:
            self.output_base = Path(output_base)
        self.output_base.mkdir(exist_ok=True)
        self.config = ElectionYearConfig()

    def load_election_data(self, folder_path):
        """載入選舉資料（elbase/elbese, elcand, elctks, elprof）"""
        folder = Path(folder_path)

        # 讀取各個檔案（elbase 或 elbese）
        base_file = folder / 'elbase.csv'
        if not base_file.exists():
            base_file = folder / 'elbese.csv'
        df_base = read_csv_auto_detect(base_file)
        df_cand = read_csv_auto_detect(folder / 'elcand.csv')
        df_ctks = read_csv_auto_detect(folder / 'elctks.csv')
        df_prof = read_csv_auto_detect(folder / 'elprof.csv')

        # 重新命名欄位
        base_cols = ['縣市代碼', '鄉鎮代碼', '選區代碼', '鄉鎮市區代碼', '村里投開票所代碼', '名稱']
        if len(df_base.columns) > 6:
            base_cols.extend([f'col{i}' for i in range(6, len(df_base.columns))])
        df_base.columns = base_cols[:len(df_base.columns)]

        # 標準化代碼欄位格式（補齊前導零）
        df_base['縣市代碼'] = df_base['縣市代碼'].astype(str).str.strip().str.strip("'").str.zfill(2)
        df_base['鄉鎮代碼'] = df_base['鄉鎮代碼'].astype(str).str.strip().str.strip("'").str.zfill(3)
        df_base['選區代碼'] = df_base['選區代碼'].astype(str).str.strip().str.strip("'").str.zfill(2)
        df_base['鄉鎮市區代碼'] = df_base['鄉鎮市區代碼'].astype(str).str.strip().str.strip("'").str.zfill(3)
        df_base['村里投開票所代碼'] = df_base['村里投開票所代碼'].astype(str).str.strip().str.strip("'").str.zfill(4)

        df_cand.columns = ['縣市代碼', '鄉鎮代碼', '選區代碼', '鄉鎮市區代碼', '村里投開票所代碼',
                          '號次', '姓名', '政黨代碼', '性別', '年齡', '學歷',
                          'col11', 'col12', '當選註記', 'col14', 'col15']

        # 標準化df_cand代碼欄位
        df_cand['縣市代碼'] = df_cand['縣市代碼'].astype(str).str.strip().str.strip("'").str.zfill(2)
        df_cand['鄉鎮代碼'] = df_cand['鄉鎮代碼'].astype(str).str.strip().str.strip("'").str.zfill(3)
        df_cand['選區代碼'] = df_cand['選區代碼'].astype(str).str.strip().str.strip("'").str.zfill(2)
        df_cand['鄉鎮市區代碼'] = df_cand['鄉鎮市區代碼'].astype(str).str.strip().str.strip("'").str.zfill(3)
        df_cand['村里投開票所代碼'] = df_cand['村里投開票所代碼'].astype(str).str.strip().str.strip("'").str.zfill(4)

        df_ctks.columns = ['縣市代碼', '鄉鎮代碼', '選區代碼', '鄉鎮市區代碼', '村里投開票所代碼',
                          '區域', '候選人號次', '得票數', '得票率', '當選註記']

        # 標準化df_ctks代碼欄位
        df_ctks['縣市代碼'] = df_ctks['縣市代碼'].astype(str).str.strip().str.strip("'").str.zfill(2)
        df_ctks['鄉鎮代碼'] = df_ctks['鄉鎮代碼'].astype(str).str.strip().str.strip("'").str.zfill(3)
        df_ctks['選區代碼'] = df_ctks['選區代碼'].astype(str).str.strip().str.strip("'").str.zfill(2)
        df_ctks['鄉鎮市區代碼'] = df_ctks['鄉鎮市區代碼'].astype(str).str.strip().str.strip("'").str.zfill(3)
        df_ctks['村里投開票所代碼'] = df_ctks['村里投開票所代碼'].astype(str).str.strip().str.strip("'").str.zfill(4)

        # 注意：elprof的欄位順序經過驗證
        # Col 6=有效票數A, Col 7=無效票數B, Col 8=投票數C
        # Col 9=選舉人數G (非已領未投票數！)
        # Col 10=已領未投票數D, Col 18=投票率H
        df_prof.columns = ['縣市代碼', '鄉鎮代碼', '選區代碼', '鄉鎮市區代碼', '村里投開票所代碼',
                          '區域', '有效票數A', '無效票數B', '投票數C', '選舉人數G',
                          '已領未投票數D', '發出票數E', 'col12', 'col13', 'col14', 'col15',
                          '用餘票數F', '投票率H_raw', 'col18', 'col19']

        # 標準化df_prof代碼欄位
        df_prof['縣市代碼'] = df_prof['縣市代碼'].astype(str).str.strip().str.strip("'").str.zfill(2)
        df_prof['鄉鎮代碼'] = df_prof['鄉鎮代碼'].astype(str).str.strip().str.strip("'").str.zfill(3)
        df_prof['選區代碼'] = df_prof['選區代碼'].astype(str).str.strip().str.strip("'").str.zfill(2)
        df_prof['鄉鎮市區代碼'] = df_prof['鄉鎮市區代碼'].astype(str).str.strip().str.strip("'").str.zfill(3)
        df_prof['村里投開票所代碼'] = df_prof['村里投開票所代碼'].astype(str).str.strip().str.strip("'").str.zfill(4)

        return df_base, df_cand, df_ctks, df_prof

    def get_location_info(self, df_base, county_code, town_code, district_code, village_code):
        """根據代碼取得地區資訊"""
        # 查詢縣市名稱
        mask = (df_base['縣市代碼'] == county_code) & \
               (df_base['鄉鎮代碼'] == '000') & \
               (df_base['選區代碼'] == '00') & \
               (df_base['鄉鎮市區代碼'] == '000') & \
               (df_base['村里投開票所代碼'] == '0000')
        county_name = df_base[mask]['名稱'].iloc[0] if len(df_base[mask]) > 0 else ''

        # 查詢鄉鎮市區名稱
        town_name = ''
        if district_code != '000':
            mask = (df_base['縣市代碼'] == county_code) & \
                   (df_base['鄉鎮代碼'] == town_code) & \
                   (df_base['選區代碼'] == '00') & \
                   (df_base['鄉鎮市區代碼'] == district_code) & \
                   (df_base['村里投開票所代碼'] == '0000')
            town_name = df_base[mask]['名稱'].iloc[0] if len(df_base[mask]) > 0 else ''

        # 查詢村里名稱
        village_name = ''
        if village_code != '0000':
            mask = (df_base['縣市代碼'] == county_code) & \
                   (df_base['鄉鎮代碼'] == town_code) & \
                   (df_base['選區代碼'] == '00') & \
                   (df_base['鄉鎮市區代碼'] == district_code) & \
                   (df_base['村里投開票所代碼'] == village_code)
            village_name = df_base[mask]['名稱'].iloc[0] if len(df_base[mask]) > 0 else ''

        # 組合行政區別
        if town_name:
            admin_area = f"{county_name}{town_name}"
        else:
            admin_area = county_name

        return county_name, admin_area, village_name

    def generate_output_filename(self, year, election_type, county_name, district=None, indigenous_type=None):
        """
        生成輸出檔名

        格式：
        - 總統/市長：{年份}_{選舉類型}_{縣市}.csv
        - 議員區域：{年份}_{選舉類型}_第{XX}選區_{縣市}.csv
        - 議員原住民：{年份}_{選舉類型}_{原住民類型}_{縣市}.csv
        """
        base_name = f"{year}_{election_type}"

        if district:
            base_name += f"_第{district:02d}選區"
        elif indigenous_type:
            base_name += f"_{indigenous_type}"

        base_name += f"_{county_name}.csv"
        return base_name

    def process_election_folder(self, data_folder, year, election_type, aggregate_level='polling', is_president=False):
        """
        處理單一選舉資料夾

        Args:
            data_folder: 資料夾路徑
            year: 年份
            election_type: 選舉類型（總統、立法委員、縣市長等）
            aggregate_level: 彙總層級 ('village'村里層級 或 'polling'投開票所層級)
            is_president: 是否為總統選舉（需要組合正副候選人）
        """
        print(f"    處理: {election_type} ({aggregate_level}層級)")

        try:
            df_base, df_cand, df_ctks, df_prof = self.load_election_data(data_folder)
        except Exception as e:
            print(f"      錯誤: 無法載入資料 - {e}")
            return

        # 過濾掉彙總資料
        df_ctks = df_ctks[df_ctks['村里投開票所代碼'] != '0000'].copy()
        df_prof = df_prof[df_prof['村里投開票所代碼'] != '0000'].copy()

        if len(df_ctks) == 0:
            print(f"      警告: 沒有找到詳細資料")
            return

        # 清理候選人號次
        df_ctks['候選人號次'] = df_ctks['候選人號次'].str.strip()

        # 轉換數值
        df_ctks['得票數'] = pd.to_numeric(df_ctks['得票數'], errors='coerce').fillna(0).astype(int)
        for col in ['有效票數A', '無效票數B', '投票數C', '已領未投票數D', '發出票數E', '用餘票數F', '選舉人數G']:
            df_prof[col] = pd.to_numeric(df_prof[col], errors='coerce').fillna(0).astype(int)

        # 計算缺失的統計欄位
        # 注意：2024年資料格式中，D/E/F欄位不可靠，需重新計算
        # 公式: A=sum(candidates), C=A+B, D=E-C, E≈C (假設已領未投票很少), F=G-E, G=E+F, H=C/G*100

        # 重新計算投票數C = A + B
        df_prof['投票數C'] = df_prof['有效票數A'] + df_prof['無效票數B']

        # 用簡化假設計算缺失欄位：
        # 假設已領未投票數D很少，發出票數E ≈ 投票數C
        # 用餘票數F = 選舉人數G - 發出票數E
        df_prof['發出票數E'] = df_prof['投票數C']  # 簡化：E ≈ C
        df_prof['已領未投票數D'] = 0  # 簡化：假設D很小

        # 計算用餘票數F = G - E
        df_prof['用餘票數F'] = 0
        mask_g_positive = df_prof['選舉人數G'] > 0
        df_prof.loc[mask_g_positive, '用餘票數F'] = (df_prof.loc[mask_g_positive, '選舉人數G'] - df_prof.loc[mask_g_positive, '發出票數E']).clip(lower=0)

        # 計算投票率H = C / G * 100
        df_prof['投票率H'] = 0.0
        df_prof.loc[mask_g_positive, '投票率H'] = (df_prof.loc[mask_g_positive, '投票數C'] / df_prof.loc[mask_g_positive, '選舉人數G'] * 100).round(2)

        # 根據彙總層級處理
        if aggregate_level == 'village':
            # 村里層級：彙總到村里
            df_votes = df_ctks.groupby(['縣市代碼', '鄉鎮代碼', '鄉鎮市區代碼', '村里投開票所代碼', '候選人號次'])['得票數'].sum().reset_index()
            df_stats = df_prof.groupby(['縣市代碼', '鄉鎮代碼', '鄉鎮市區代碼', '村里投開票所代碼']).agg({
                '有效票數A': 'sum',
                '無效票數B': 'sum',
                '投票數C': 'sum',
                '已領未投票數D': 'sum',
                '發出票數E': 'sum',
                '用餘票數F': 'sum',
                '選舉人數G': 'sum'
            }).reset_index()
            # 重新計算投票率
            df_stats['投票率H'] = 0.0
            mask_g_nonzero = df_stats['選舉人數G'] > 0
            df_stats.loc[mask_g_nonzero, '投票率H'] = (df_stats.loc[mask_g_nonzero, '投票數C'] / df_stats.loc[mask_g_nonzero, '選舉人數G'] * 100).round(2)
            index_cols = ['縣市代碼', '鄉鎮代碼', '鄉鎮市區代碼', '村里投開票所代碼']
        else:
            # 投開票所層級：聚合以移除重複
            df_votes = df_ctks.groupby(['縣市代碼', '鄉鎮代碼', '選區代碼', '鄉鎮市區代碼', '村里投開票所代碼', '候選人號次'])['得票數'].sum().reset_index()
            # 聚合統計資料以移除重複
            df_stats = df_prof.groupby(['縣市代碼', '鄉鎮代碼', '選區代碼', '鄉鎮市區代碼', '村里投開票所代碼']).agg({
                '有效票數A': 'sum',
                '無效票數B': 'sum',
                '投票數C': 'sum',
                '已領未投票數D': 'sum',
                '發出票數E': 'sum',
                '用餘票數F': 'sum',
                '選舉人數G': 'sum'
            }).reset_index()
            # 重新計算投票率
            df_stats['投票率H'] = 0.0
            mask_g_nonzero = df_stats['選舉人數G'] > 0
            df_stats.loc[mask_g_nonzero, '投票率H'] = (df_stats.loc[mask_g_nonzero, '投票數C'] / df_stats.loc[mask_g_nonzero, '選舉人數G'] * 100).round(2)
            index_cols = ['縣市代碼', '鄉鎮代碼', '選區代碼', '鄉鎮市區代碼', '村里投開票所代碼']

        # 取得候選人資訊（包含選區代碼或縣市代碼以便後續過濾）
        # Also include party code for conversion
        if '選區代碼' in df_cand.columns and aggregate_level == 'polling':
            df_cand_info = df_cand[['選區代碼', '號次', '姓名', '政黨代碼']].drop_duplicates()
        elif '縣市代碼' in df_cand.columns and aggregate_level == 'village':
            # 村里層級：包含縣市代碼和鄉鎮代碼以區分不同縣市的候選人
            if '鄉鎮代碼' in df_cand.columns:
                df_cand_info = df_cand[['縣市代碼', '鄉鎮代碼', '號次', '姓名', '政黨代碼']].drop_duplicates()
            else:
                df_cand_info = df_cand[['縣市代碼', '號次', '姓名', '政黨代碼']].drop_duplicates()
        else:
            df_cand_info = df_cand[['號次', '姓名', '政黨代碼']].drop_duplicates()
        
        # Convert party code to party name
        party_code_map = {
            '1': '中國國民黨', '2': '民主進步黨', '3': '親民黨', '4': '台灣團結聯盟',
            '5': '無黨團結聯盟', '6': '綠黨', '7': '新黨', '8': '台灣基進', '9': '台灣民眾黨',
            '10': '時代力量', '11': '一邊一國行動黨', '12': '勞動黨', '13': '中華統一促進黨',
            '14': '國會政黨聯盟', '15': '台澎黨', '16': '民主進步黨', '17': '社會民主黨',
            '18': '和平鴿聯盟黨', '19': '喜樂島聯盟', '20': '安定力量', '21': '合一行動聯盟',
            '90': '親民黨', '99': '無黨籍及未經政黨推薦', '999': '無黨籍及未經政黨推薦',
            '348': '喜樂島聯盟',
        }
        df_cand_info['政黨'] = df_cand_info['政黨代碼'].astype(str).map(party_code_map).fillna('無黨籍及未經政黨推薦')

        # 建立選區候選人映射（用於後續過濾）
        district_candidates = {}
        county_candidates = {}

        if '選區代碼' in df_cand.columns and aggregate_level == 'polling':
            # 投開票所層級：建立選區候選人映射
            for _, row in df_cand_info.iterrows():
                district = str(row['選區代碼']).strip()
                number = str(row['號次']).strip()
                if district not in district_candidates:
                    district_candidates[district] = set()
                district_candidates[district].add(number)
        elif '縣市代碼' in df_cand_info.columns and aggregate_level == 'village':
            # 村里層級：建立縣市候選人映射（存儲"號次_姓名"組合）
            for _, row in df_cand_info.iterrows():
                county = str(row['縣市代碼']).strip().strip("'")
                number = str(row['號次']).strip()
                name = str(row['姓名']).strip()
                cand_col_name = f"{number}_{name}"

                # 當縣市代碼='10'時，需要使用鄉鎮代碼組合作為key
                if county == '10' and '鄉鎮代碼' in row.index:
                    township = str(row['鄉鎮代碼']).strip().strip("'")
                    composite_key = f"{county}-{township}"
                else:
                    composite_key = county

                if composite_key not in county_candidates:
                    county_candidates[composite_key] = set()
                county_candidates[composite_key].add(cand_col_name)

        if is_president:
            # 組合正副候選人
            temp_dict = {}
            temp_party_dict = {}  # 儲存政黨資訊
            for idx, row in df_cand_info.iterrows():
                number = str(row['號次']).strip()
                name = str(row['姓名']).strip()
                party = str(row['政黨']).strip() if '政黨' in row.index else ''
                if number not in temp_dict:
                    temp_dict[number] = []
                    temp_party_dict[number] = party  # 儲存該號次的政黨
                temp_dict[number].append(name)

            # 建立候選人字典和政黨字典
            cand_dict = {}
            party_dict = {}
            for number in temp_dict:
                if len(temp_dict[number]) >= 2:
                    combined_name = f"{temp_dict[number][0]}/{temp_dict[number][1]}"
                else:
                    combined_name = temp_dict[number][0]
                
                party_value = temp_party_dict.get(number, '')
                
                # 對於村里層級的總統選舉，需要為所有可能的縣市建立映射
                if aggregate_level == 'village':
                    # 總統選舉候選人對全國都相同，從投票數據中獲取所有縣市代碼
                    # 此時df_votes已經建立（在前面的aggregate_level判斷中）
                    if '縣市代碼' in df_votes.columns:
                        # 清理縣市代碼
                        counties_clean = df_votes['縣市代碼'].astype(str).str.strip().str.strip("'")
                        unique_counties = counties_clean.unique()
                        
                        for county_str in unique_counties:
                            # 對於新北市（代碼10），需要處理鄉鎮代碼
                            if county_str == '10' and '鄉鎮代碼' in df_votes.columns:
                                # 獲取此縣市的所有鄉鎮代碼
                                mask = (df_votes['縣市代碼'].astype(str).str.strip().str.strip("'") == '10')
                                townships_clean = df_votes.loc[mask, '鄉鎮代碼'].astype(str).str.strip().str.strip("'")
                                unique_townships = townships_clean.unique()
                                
                                for township_str in unique_townships:
                                    key = f"{county_str}-{township_str}_{number}"
                                    cand_dict[key] = combined_name
                                    party_dict[key] = party_value
                            else:
                                key = f"{county_str}_{number}"
                                cand_dict[key] = combined_name
                                party_dict[key] = party_value
                
                # 同時保留簡單的號次映射（用於投開票所層級）
                cand_dict[number] = combined_name
                party_dict[number] = party_value
        else:
            # 一般候選人
            # 根據資料層級決定如何建立候選人字典和政黨字典
            if '選區代碼' in df_cand_info.columns and aggregate_level == 'polling':
                # 投開票所層級且有選區：使用選區-號次作為key
                cand_dict = {}
                party_dict = {}
                for _, row in df_cand_info.iterrows():
                    district = str(row['選區代碼']).strip()
                    number = str(row['號次']).strip()
                    name = str(row['姓名']).strip()
                    party = str(row['政黨']).strip() if '政黨' in row.index else ''
                    key = f"{district}_{number}"
                    cand_dict[key] = name
                    party_dict[key] = party
            elif '縣市代碼' in df_cand_info.columns:
                # 有縣市代碼（村里層級，如市長、總統）：使用縣市代碼-號次作為key
                cand_dict = {}
                party_dict = {}
                for _, row in df_cand_info.iterrows():
                    county = str(row['縣市代碼']).strip().strip("'")
                    number = str(row['號次']).strip()
                    name = str(row['姓名']).strip()
                    party = str(row['政黨']).strip() if '政黨' in row.index else ''

                    # 當縣市代碼='10'時，需要使用鄉鎮代碼組合作為key
                    if county == '10' and '鄉鎮代碼' in row.index:
                        township = str(row['鄉鎮代碼']).strip().strip("'")
                        key = f"{county}-{township}_{number}"
                    else:
                        key = f"{county}_{number}"

                    cand_dict[key] = name
                    party_dict[key] = party
            else:
                # 無選區或縣市代碼時，使用簡單映射（號次->姓名）
                cand_dict = {}
                party_dict = {}
                for _, row in df_cand_info.iterrows():
                    number = str(row['號次']).strip()
                    name = str(row['姓名']).strip()
                    party = str(row['政黨']).strip() if '政黨' in row.index else ''
                    cand_dict[number] = name
                    party_dict[number] = party

        # 判斷是否需要使用複合鍵（選區或縣市+號次）
        use_district_key = '選區代碼' in df_votes.columns and aggregate_level == 'polling'
        use_county_key = '縣市代碼' in df_votes.columns and aggregate_level == 'village' and '縣市代碼' in df_cand_info.columns

        if use_district_key:
            # 投開票所層級：建立選區-號次組合作為候選人識別
            df_votes['候選人識別'] = df_votes['選區代碼'].astype(str).str.strip() + '_' + df_votes['候選人號次'].astype(str).str.strip()
            pivot_column = '候選人識別'
        elif use_county_key:
            # 村里層級：建立縣市-號次組合作為候選人識別
            # 當縣市代碼='10'時，需要使用鄉鎮代碼組合作為key
            df_votes['縣市代碼_clean'] = df_votes['縣市代碼'].astype(str).str.strip().str.strip("'")

            # 創建複合鍵
            def create_composite_key(row):
                county = str(row['縣市代碼_clean'])
                number = str(row['候選人號次']).strip()
                if county == '10' and '鄉鎮代碼' in df_votes.columns:
                    township = str(row['鄉鎮代碼']).strip().strip("'")
                    return f"{county}-{township}_{number}"
                else:
                    return f"{county}_{number}"

            df_votes['候選人識別'] = df_votes.apply(create_composite_key, axis=1)
            pivot_column = '候選人識別'
        else:
            pivot_column = '候選人號次'

        # 轉換為寬格式（使用sum聚合，確保是整數）
        df_votes_wide = df_votes.pivot_table(
            index=index_cols,
            columns=pivot_column,
            values='得票數',
            aggfunc='sum',  # 使用加總而非平均
            fill_value=0
        ).reset_index()

        # 重新命名候選人欄位並轉換為整數
        # 對於總統村里層級，保留原始列名用於後續縣市過濾，建立映射字典用於最終輸出重命名
        new_cols = {}
        col_to_county = {}  # 映射：原始列名 -> 縣市代碼（用於村里層級過濾）
        col_to_display = {}  # 映射：原始列名 -> 顯示名稱（用於最終輸出）
        
        for col in df_votes_wide.columns:
            if col not in index_cols:
                # 轉換票數為整數
                df_votes_wide[col] = df_votes_wide[col].fillna(0).astype(int)

                # 根據使用的鍵類型來查找候選人姓名
                if use_district_key or use_county_key:
                    # col is in "district_number" or "county_number" format
                    cand_name = cand_dict.get(str(col), '未知')
                    # Extract just the number part for display
                    number_part = str(col).split('_')[-1] if '_' in str(col) else str(col)
                    display_name = f"{number_part}_{cand_name}"
                    
                    # 對於總統村里層級，不立即重命名，保留原始列名用於過濾
                    if use_county_key and is_president:
                        # 從原始列名提取縣市代碼（格式：縣市代碼_號次 或 縣市代碼-鄉鎮代碼_號次）
                        county_part = str(col).split('_')[0] if '_' in str(col) else str(col)
                        col_to_county[col] = county_part  # 記錄原始列名到縣市代碼
                        col_to_display[col] = display_name  # 記錄原始列名到顯示名稱
                        # 不添加到new_cols，保持原始列名
                    else:
                        # 其他情況，直接重命名
                        new_cols[col] = display_name
                else:
                    if is_president:
                        new_col_name = f"({col})\n{cand_dict.get(str(col), '未知')}"
                    else:
                        new_col_name = f"{col}_{cand_dict.get(str(col), '未知')}"
                    new_cols[col] = new_col_name
                    
        # 只重命名需要重命名的列（總統村里層級保留原始列名）
        if new_cols:
            df_votes_wide.rename(columns=new_cols, inplace=True)

        # 為每個候選人欄位加入對應的黨籍欄位
        for col in list(df_votes_wide.columns):
            if col not in index_cols:
                # 找出原始的pivot_column值（候選人識別或號次）
                original_key = None
                if col in new_cols.values():
                    # 找到對應的原始key
                    for orig_key, new_name in new_cols.items():
                        if new_name == col:
                            original_key = orig_key
                            break
                else:
                    # 沒有被重命名的欄位（總統村里層級）
                    original_key = col
                
                if original_key and original_key in party_dict:
                    party_value = party_dict[original_key]
                    # 在候選人得票數欄位後面加入黨籍欄位
                    party_col_name = f"{col}_黨籍"
                    # 找到當前欄位的位置
                    col_idx = df_votes_wide.columns.get_loc(col)
                    # 在該位置後插入黨籍欄位
                    df_votes_wide.insert(col_idx + 1, party_col_name, party_value)

        # 合併統計資料
        df_result = df_votes_wide.merge(df_stats, on=index_cols, how='left')

        # 注意：用餘票數F、選舉人數G、投票率H已經在前面正確計算過了，不需要再重新計算

        # 加入地區名稱 - 優化版本：建立查詢字典
        if aggregate_level == 'polling':
            df_result['投開票所別'] = df_result['村里投開票所代碼']

        # 建立地區名稱查詢字典（包含選區代碼以處理跨選區的行政區）
        location_dict = {}
        has_district_in_data = '選區代碼' in df_result.columns

        # 建立雙重字典：有選區代碼和沒有選區代碼的兩種查詢方式
        for _, row in df_base.iterrows():
            if '選區代碼' in df_base.columns:
                # 包含選區代碼的鍵
                key_with_dist = (row['縣市代碼'], row['鄉鎮代碼'], row['選區代碼'], row['鄉鎮市區代碼'], row['村里投開票所代碼'])
                location_dict[key_with_dist] = row['名稱']
            # 不包含選區代碼的鍵（用於村里層級）
            key_no_dist = (row['縣市代碼'], row['鄉鎮代碼'], row['鄉鎮市區代碼'], row['村里投開票所代碼'])
            location_dict[key_no_dist] = row['名稱']

        # 使用向量化操作添加地區名稱
        def get_admin_area(row):
            if has_district_in_data and '選區代碼' in row.index:
                # 對於有選區的資料，直接使用選區層級的縣市名稱
                county_key = (row['縣市代碼'], '000', row['選區代碼'], '000', '0000')
                base_name = location_dict.get(county_key, '')

                if row['鄉鎮市區代碼'] != '000':
                    town_key = (row['縣市代碼'], row['鄉鎮代碼'], row['選區代碼'], row['鄉鎮市區代碼'], '0000')
                    town_name = location_dict.get(town_key, '')
                    return f"{base_name}{town_name}" if town_name else base_name
                return base_name
            else:
                county_key = (row['縣市代碼'], '000', '000', '0000')
                county_name = location_dict.get(county_key, '')

                if row['鄉鎮市區代碼'] != '000':
                    town_key = (row['縣市代碼'], row['鄉鎮代碼'], row['鄉鎮市區代碼'], '0000')
                    town_name = location_dict.get(town_key, '')
                    return f"{county_name}{town_name}" if town_name else county_name
                return county_name

        def get_village_name(row):
            # Always use 4-tuple key (without 選區代碼) for village lookup
            # because elbase doesn't have district-specific village data
            if row['村里投開票所代碼'] != '0000':
                village_key = (row['縣市代碼'], row['鄉鎮代碼'], row['鄉鎮市區代碼'], row['村里投開票所代碼'])
                return location_dict.get(village_key, '')
            return ''

        df_result['行政區別'] = df_result.apply(get_admin_area, axis=1)
        df_result['村里別'] = df_result.apply(get_village_name, axis=1)

        # 整理欄位順序
        if aggregate_level == 'village':
            base_cols = ['行政區別', '村里別']
        else:
            base_cols = ['行政區別', '村里別', '投開票所別']

        cand_cols = [col for col in df_result.columns if ('_' in col or col.startswith('(')) and
                    col not in ['有效票數A', '無效票數B', '投票數C', '已領未投票數D', '發出票數E', '用餘票數F', '選舉人數G', '投票率H']]
        
        # 對候選人列按號次數字排序
        def extract_candidate_number(col_name):
            """從欄位名稱提取候選人號次用於排序"""
            if col_name.startswith('('):
                # 總統格式: (1)\n候選人
                try:
                    return int(col_name.split(')')[0].replace('(', ''))
                except:
                    return 9999
            elif '_' in col_name:
                # 一般格式: 1_候選人
                try:
                    return int(col_name.split('_')[0])
                except:
                    return 9999
            return 9999
        
        cand_cols = sorted(cand_cols, key=extract_candidate_number)
        stat_cols = ['有效票數A', '無效票數B', '投票數C', '已領未投票數D', '發出票數E', '用餘票數F', '選舉人數G', '投票率H']

        # 輸出分檔
        # 根據彙總層級傳遞相應的候選人映射
        candidates_map = None
        if aggregate_level == 'polling':
            candidates_map = district_candidates
        elif aggregate_level == 'village':
            candidates_map = county_candidates

        self.output_by_county_district(df_result, year, election_type, base_cols, cand_cols, stat_cols, aggregate_level, candidates_map, cand_dict)

    def get_county_name_from_code(self, county_code, township_code=None):
        """
        從縣市代碼取得縣市名稱

        Args:
            county_code: 縣市代碼
            township_code: 鄉鎮代碼（當縣市代碼為'10'時必須提供）

        Returns:
            str: 縣市名稱
        """
        # 將代碼轉為字串並移除引號
        code_str = str(county_code).strip().strip("'")
        town_str = str(township_code).strip().strip("'") if township_code else None

        # 新代碼系統（2014年後）：縣市代碼='10'時，使用鄉鎮代碼判斷縣市
        if code_str == '10' and town_str and town_str != '000':
            county_name = self.config.TOWNSHIP_CODE_MAPPING.get(town_str)
            if county_name:
                return county_name
            # 如果找不到，返回未知
            return f'未知縣市(10-{town_str})'

        # 縣市代碼對照表
        # 新代碼系統（2014年後）：63-68為直轄市，09為連江縣
        # 舊代碼系統（2010年前）：01-26為各縣市
        county_mapping = {
            # 全國
            '00': '全國',
            '0': '全國',

            # 新代碼系統（2014年後使用）
            '63': '臺北市',
            '64': '高雄市',
            '65': '新北市',
            '66': '臺中市',
            '67': '臺南市',
            '68': '桃園市',
            '09': '連江縣',
            '9': '連江縣',

            # 舊代碼系統（2010年前）
            '01': '臺北市',
            '02': '高雄市',
            '03': '基隆市',
            '04': '臺中市',
            '05': '臺南市',
            '06': '新竹市',
            '07': '嘉義市',
            '08': '臺北縣',  # 現為新北市
            '10': '宜蘭縣',  # 舊系統中'10'=宜蘭縣
            '11': '桃園縣',  # 現為桃園市
            '12': '新竹縣',
            '13': '苗栗縣',
            '14': '臺中縣',
            '15': '彰化縣',
            '16': '南投縣',
            '17': '雲林縣',
            '18': '嘉義縣',
            '19': '臺南縣',  # 舊代碼，已併入台南市
            '20': '高雄縣',  # 舊代碼，已併入高雄市
            '21': '屏東縣',
            '22': '臺東縣',
            '23': '花蓮縣',
            '24': '澎湖縣',
            '25': '金門縣',
            '26': '連江縣',
        }

        return county_mapping.get(code_str, f'未知縣市({code_str})')

    def clean_location_name(self, name):
        """清理地區名稱，移除省名稱和多餘的文字"""
        if not name:
            return name
        # 移除省名稱
        name = name.replace('臺灣省', '').replace('福建省', '')
        # 移除多餘空白
        name = name.strip()
        return name

    def reformat_to_standard_columns(self, df, max_candidates=30):
        """
        將資料重新格式化為標準欄位格式（配對候選人欄位）

        Args:
            df: 原始資料框
            max_candidates: 最多候選人數量（預設30）

        Returns:
            重新格式化後的資料框
        """
        # 定義已知的非候選人欄位
        known_cols = {
            '時間', '選舉名稱', '縣市', '行政區別', '村里別', '投開票所別', '選區', '區域代碼',
            '有效票數A', '有效票數', '無效票數B', '無效票數', '投票數C', '投票數',
            '已領未投票數D', '已領未投票數', '發出票數E', '發出票數',
            '用餘票數F', '用餘票數', '選舉人數G', '選舉人數',
            '投票率H', '投票率', '立委選區'
        }

        # 識別候選人欄位（不在known_cols中的數值欄位）
        candidate_cols = []
        for col in df.columns:
            if col not in known_cols and df[col].dtype in ['int64', 'Int64', 'float64']:
                candidate_cols.append(col)

        # 如果沒有候選人欄位，直接返回
        if not candidate_cols:
            return df

        # 建立新的資料框架
        result_df = pd.DataFrame()

        # 1. 複製基礎欄位
        base_cols = ['時間', '選舉名稱', '縣市', '行政區別', '村里別']
        for col in base_cols:
            if col in df.columns:
                result_df[col] = df[col]

        # 2. 添加區域代碼（如果沒有的話，留空）
        if '區域代碼' in df.columns:
            result_df['區域代碼'] = df['區域代碼']
        else:
            result_df['區域代碼'] = ''

        # 3. 添加選區
        if '選區' in df.columns:
            result_df['選區'] = df['選區']
        else:
            result_df['選區'] = ''

        # 4. 添加投開票所別（如果有的話）
        if '投開票所別' in df.columns:
            result_df['投開票所別'] = df['投開票所別']

        # 5. 為每個候選人建立配對欄位（最多max_candidates個）
        for i in range(min(len(candidate_cols), max_candidates)):
            cand_col = candidate_cols[i]
            num = i + 1

            # 候選人名稱（從原始欄位名取得，移除號次前綴）
            # 格式："1_候選人名" → "候選人名"
            cand_name = cand_col
            if '_' in cand_col:
                parts = cand_col.split('_', 1)
                # 檢查第一部分是否為數字（號次）
                if parts[0].isdigit() or (parts[0].startswith('(') and parts[0].rstrip(')').isdigit()):
                    cand_name = parts[1]  # 移除號次，只保留名字
            result_df[f'選舉候選人{num}'] = cand_name

            # 政黨（暫時留空，因為原始資料沒有）
            result_df[f'選舉候選人政黨{num}'] = ''

            # 得票數
            result_df[f'選舉候選人得票數{num}'] = df[cand_col]

            # 得票率（得票數 / 有效票數 * 100）
            valid_votes_col = None
            for col_name in ['有效票數A', '有效票數']:
                if col_name in df.columns:
                    valid_votes_col = col_name
                    break

            if valid_votes_col is not None:
                # 避免除以0
                result_df[f'選舉候選人得票率{num}'] = 0.0
                mask = df[valid_votes_col] > 0
                result_df.loc[mask, f'選舉候選人得票率{num}'] = (
                    df.loc[mask, cand_col] / df.loc[mask, valid_votes_col]
                ).round(6)
            else:
                result_df[f'選舉候選人得票率{num}'] = 0.0

        # 填充剩餘的候選人欄位（如果候選人少於max_candidates）
        for num in range(len(candidate_cols) + 1, max_candidates + 1):
            result_df[f'選舉候選人{num}'] = ''
            result_df[f'選舉候選人政黨{num}'] = ''
            result_df[f'選舉候選人得票數{num}'] = ''
            result_df[f'選舉候選人得票率{num}'] = ''

        # 6. 添加統計欄位
        stat_col_mapping = [
            ('有效票數', ['有效票數A', '有效票數']),
            ('無效票數', ['無效票數B', '無效票數']),
            ('投票數', ['投票數C', '投票數']),
            ('已領未投票數', ['已領未投票數D', '已領未投票數']),
            ('發出票數', ['發出票數E', '發出票數']),
            ('用餘票數', ['用餘票數F', '用餘票數']),
            ('選舉人數', ['選舉人數G', '選舉人數']),
            ('投票率', ['投票率H', '投票率']),
        ]

        for target_col, source_cols in stat_col_mapping:
            found = False
            for source_col in source_cols:
                if source_col in df.columns:
                    result_df[target_col] = df[source_col]
                    found = True
                    break
            if not found:
                result_df[target_col] = 0

        # 7. 添加立委選區（如果有的話）
        if '立委選區' in df.columns:
            result_df['立委選區'] = df['立委選區']
        else:
            result_df['立委選區'] = ''

        return result_df

    def merge_county_files(self, county_name):
        """
        合併單一縣市的所有選舉資料成一個檔案

        Args:
            county_name: 縣市名稱（如：宜蘭縣、臺北市）
        """
        county_folder = self.output_base / county_name
        if not county_folder.exists():
            print(f"  警告: 找不到縣市資料夾 {county_name}")
            return

        # 讀取該縣市所有CSV檔案（排除完成版檔案）
        csv_files = [f for f in county_folder.glob('*.csv') if '完成版' not in f.name]
        if not csv_files:
            print(f"  警告: {county_name} 沒有CSV檔案")
            return

        print(f"  合併 {county_name}: 找到 {len(csv_files)} 個檔案")

        all_data = []
        for csv_file in sorted(csv_files):
            # 從檔名解析年份和選舉類型
            # 格式: {年份}_{選舉類型}[_第XX選區][_原住民類型]_{縣市}.csv
            filename = csv_file.stem  # 去除.csv
            parts = filename.split('_')

            if len(parts) < 2:
                continue

            year = parts[0]
            election_type = parts[1]
            district = ''

            # 檢查是否有選區資訊
            for i, part in enumerate(parts[2:], start=2):
                if part.startswith('第') and '選區' in part:
                    district = part
                elif part in ['山地原住民', '平地原住民']:
                    district = part

            # 讀取資料
            try:
                df = pd.read_csv(csv_file, encoding='utf-8-sig')

                # 新增時間和選舉名稱欄位
                df.insert(0, '時間', year)
                df.insert(1, '選舉名稱', election_type)
                df.insert(2, '縣市', county_name)
                
                # 確保有行政區別欄位（如果沒有，用縣市名稱填充）
                if '行政區別' not in df.columns:
                    df.insert(3, '行政區別', county_name)
                    
                if district:
                    # 如果有選區，插入在行政區別之後
                    col_idx = df.columns.get_loc('行政區別') + 1
                    df.insert(col_idx, '選區', district)

                # 在合併前先格式化每個檔案，確保候選人欄位統一
                # 這樣不同年份的總統選舉（候選人不同）才能正確合併
                df = self.reformat_to_standard_columns(df)

                all_data.append(df)
            except Exception as e:
                print(f"    警告: 無法讀取 {csv_file.name} - {e}")
                continue

        if not all_data:
            print(f"  警告: {county_name} 沒有可合併的資料")
            return

        # 合併所有資料
        df_merged = pd.concat(all_data, ignore_index=True)

        # 分離有投開票所別和沒有投開票所別的資料
        if '投開票所別' in df_merged.columns:
            # 分離：有投開票所別的資料需要聚合，沒有的資料只需去重
            has_polling = df_merged[df_merged['投開票所別'].notna()].copy()
            no_polling = df_merged[df_merged['投開票所別'].isna()].copy()
            
            # 處理有投開票所別的資料（聚合到村里層級）
            if len(has_polling) > 0:
                print(f"    聚合到村里層級（投開票所層級）...")
                
                # 設定聚合鍵（不包含投開票所別）
                group_cols = ['時間', '選舉名稱', '縣市', '行政區別', '村里別']
                if '選區' in has_polling.columns:
                    group_cols.append('選區')
                if '區域代碼' in has_polling.columns:
                    group_cols.append('區域代碼')
                
                # 確保所有分組欄位都存在
                group_cols = [col for col in group_cols if col in has_polling.columns]
                
                # 識別需要聚合的數值欄位
                excluded_cols = set(group_cols + ['投開票所別', '投票率', '投票率H'])
                agg_dict = {}
                
                for col in has_polling.columns:
                    if col in excluded_cols:
                        continue
                    # 候選人名字和政黨欄位（字串）：取第一個值
                    if col.startswith('選舉候選人') and (col.endswith('政黨') or not any(col.endswith(suffix) for suffix in ['得票數', '得票率'])):
                        agg_dict[col] = 'first'
                    # 數值欄位（得票數、統計數據）：加總
                    elif has_polling[col].dtype in ['int64', 'Int64', 'float64', 'Float64']:
                        agg_dict[col] = 'sum'
                
                # 執行聚合
                before_count = len(has_polling)
                has_polling = has_polling.groupby(group_cols, as_index=False).agg(agg_dict)
                after_count = len(has_polling)
                
                # 重新計算投票率
                if '投票數' in has_polling.columns and '選舉人數' in has_polling.columns:
                    has_polling['投票率'] = 0.0
                    mask = has_polling['選舉人數'] > 0
                    has_polling.loc[mask, '投票率'] = (
                        has_polling.loc[mask, '投票數'] / has_polling.loc[mask, '選舉人數'] * 100
                    ).round(2)
                elif '投票數C' in has_polling.columns and '選舉人數G' in has_polling.columns:
                    has_polling['投票率H'] = 0.0
                    mask = has_polling['選舉人數G'] > 0
                    has_polling.loc[mask, '投票率H'] = (
                        has_polling.loc[mask, '投票數C'] / has_polling.loc[mask, '選舉人數G'] * 100
                    ).round(2)
                
                print(f"    投開票所層級 → 村里層級: {before_count} → {after_count} 筆")
                
                # 移除投開票所別欄位
                if '投開票所別' in has_polling.columns:
                    has_polling = has_polling.drop(columns=['投開票所別'])
            
            # 處理沒有投開票所別的資料（只去重）
            if len(no_polling) > 0:
                print(f"    處理村里層級資料（無需聚合）...")
                
                # 移除投開票所別欄位（都是NaN）
                if '投開票所別' in no_polling.columns:
                    no_polling = no_polling.drop(columns=['投開票所別'])
                
                # 去重
                dedup_cols = ['時間', '選舉名稱', '行政區別', '村里別']
                if '選區' in no_polling.columns:
                    dedup_cols.append('選區')
                dedup_cols = [col for col in dedup_cols if col in no_polling.columns]
                
                before_count = len(no_polling)
                no_polling = no_polling.drop_duplicates(subset=dedup_cols, keep='first')
                after_count = len(no_polling)
                
                if before_count > after_count:
                    print(f"    去除重複: {before_count} → {after_count} 筆")
            
            # 合併兩部分
            if len(has_polling) > 0 and len(no_polling) > 0:
                df_merged = pd.concat([has_polling, no_polling], ignore_index=True)
                print(f"    合併: {len(has_polling)} + {len(no_polling)} = {len(df_merged)} 筆")
            elif len(has_polling) > 0:
                df_merged = has_polling
            elif len(no_polling) > 0:
                df_merged = no_polling
        else:
            # 沒有投開票所別欄位，只需去重
            dedup_cols = ['時間', '選舉名稱', '行政區別', '村里別']
            if '選區' in df_merged.columns:
                dedup_cols.append('選區')
            dedup_cols = [col for col in dedup_cols if col in df_merged.columns]
            
            before_count = len(df_merged)
            df_merged = df_merged.drop_duplicates(subset=dedup_cols, keep='first')
            after_count = len(df_merged)
            
            if before_count > after_count:
                print(f"    去除重複: {before_count} → {after_count} 筆 (移除 {before_count - after_count} 筆重複)")

        # 輸出合併檔案（已在讀取時完成格式化，不需要再次調用 reformat_to_standard_columns）
        output_file = county_folder / f"{county_name}_選舉整理_完成版.csv"
        df_merged.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"    ✓ 已輸出: {output_file.name} ({len(df_merged)} 筆)")

    def import_preprocessed_counties(self):
        """
        匯入各縣市候選人分類資料夾中已處理好的CSV檔案

        這些資料夾包含：嘉義縣、南投縣、雲林縣、彰化縣、金門縣、台東縣、花蓮縣等
        僅匯入2010年以後的資料
        """
        print("\n匯入各縣市已處理資料...")

        # 各縣市候選人分類資料夾路徑
        # base_path = .../各縣市候選人分類/votedata/voteData
        # 所以需要 parent.parent 才能回到 各縣市候選人分類
        county_source_base = self.base_path.parent.parent

        if not county_source_base.exists():
            print("  警告: 找不到各縣市候選人分類資料夾")
            return

        # 遍歷所有縣市資料夾
        for county_folder in county_source_base.iterdir():
            if not county_folder.is_dir():
                continue

            county_name = county_folder.name

            # 跳過votedata資料夾（已經被process_all_years()處理）
            if county_name == 'votedata':
                continue

            # 尋找CSV檔案
            csv_files = list(county_folder.glob('*.csv'))
            if not csv_files:
                continue

            # 過濾掉2010年以前的檔案
            csv_files_filtered = []
            for csv_file in csv_files:
                filename = csv_file.stem
                parts = filename.split('_')
                if len(parts) >= 1:
                    try:
                        year = int(parts[0])
                        if year >= 2010:
                            csv_files_filtered.append(csv_file)
                    except ValueError:
                        pass  # 無法解析年份，跳過

            if not csv_files_filtered:
                print(f"  跳過 {county_name}: 沒有2010年以後的檔案")
                continue

            print(f"  匯入 {county_name}: 找到 {len(csv_files_filtered)} 個檔案（2010年以後）")

            # 建立目標縣市資料夾
            target_folder = self.output_base / county_name
            target_folder.mkdir(exist_ok=True, parents=True)

            # 複製檔案並重新命名為標準格式
            for csv_file in csv_files_filtered:
                try:
                    # 讀取檔案
                    df = pd.read_csv(csv_file, encoding='utf-8-sig')

                    # 移除「總計」行
                    if '行政區別' in df.columns:
                        df = df[df['行政區別'] != '總計'].copy()

                    # 移除空村里別的行（通常是總計行）
                    if '村里別' in df.columns:
                        df = df[df['村里別'].notna() & (df['村里別'] != '')].copy()

                    # 填充行政區別（很多CSV沒有填寫行政區別，用縣市名稱填充）
                    if '行政區別' in df.columns:
                        # 先嘗試ffill（如果有值的話）
                        df['行政區別'] = df['行政區別'].ffill()
                        # 如果仍然是空的，用縣市名稱填充
                        if df['行政區別'].isna().all() or (df['行政區別'] == '').all():
                            df['行政區別'] = county_name

                    # 過濾掉行政區別不符合當前縣市的資料（資料錯置問題）
                    if '行政區別' in df.columns:
                        # 只保留行政區別為當前縣市的資料
                        before_filter = len(df)
                        df = df[df['行政區別'] == county_name].copy()
                        after_filter = len(df)
                        if before_filter > after_filter:
                            print(f"      過濾錯置資料: {before_filter} → {after_filter} 筆 (移除 {before_filter - after_filter} 筆錯置)")

                    # 檢查是否有重複的村里別（需要加總）
                    if '行政區別' in df.columns and '村里別' in df.columns:
                        # 檢查是否有重複
                        group_cols = ['行政區別', '村里別']
                        if '投開票所別' in df.columns:
                            # 投開票所層級不需要聚合
                            pass
                        else:
                            # 村里層級：檢查重複並聚合
                            duplicates = df.duplicated(subset=group_cols, keep=False)
                            if duplicates.any():
                                print(f"      發現重複村里，進行加總...")

                                # 識別數值欄位（候選人和統計欄位）
                                numeric_cols = []
                                non_numeric_cols = []

                                for col in df.columns:
                                    if col in group_cols:
                                        continue  # 跳過groupby欄位
                                    if df[col].dtype in ['int64', 'Int64', 'float64']:
                                        numeric_cols.append(col)
                                    else:
                                        non_numeric_cols.append(col)

                                # 聚合：group_cols作為鍵，數值欄位求和，非數值欄位取第一個值
                                agg_dict = {}
                                for col in numeric_cols:
                                    agg_dict[col] = 'sum'
                                for col in non_numeric_cols:
                                    agg_dict[col] = 'first'

                                before_count = len(df)
                                df = df.groupby(group_cols, as_index=False).agg(agg_dict)
                                after_count = len(df)

                                # 確保數值欄位為整數（票數不應有小數）
                                for col in numeric_cols:
                                    if '率' not in col:  # 投票率可以是小數
                                        df[col] = df[col].round().astype('Int64')

                                print(f"      聚合完成: {before_count} → {after_count} 筆")

                    # 解析檔名：{年份}_{選舉類型}.csv
                    filename = csv_file.stem
                    parts = filename.split('_')

                    if len(parts) < 2:
                        print(f"    警告: 無法解析檔名 {filename}")
                        continue

                    year = parts[0]
                    election_type = '_'.join(parts[1:])  # 剩下的部分都是選舉類型

                    # 對於立委選舉，需要按選區分割檔案
                    if '立法委員' in election_type or '立委' in election_type:
                        # 檢查是否有選區資訊（在候選人名稱中）
                        # 預處理檔案的格式：候選人_1, 候選人_2... 代表不同選區的候選人
                        cand_cols = [col for col in df.columns 
                                    if col not in ['行政區別', '村里別', '投開票所別', '有效票數A', '無效票數B', 
                                                   '投票數C', '已領未投票數D', '發出票數E', '用餘票數F', 
                                                   '選舉人數G', '投票率H']]
                        
                        # 分析候選人欄位，找出選區
                        # 格式可能是：候選人_1（選區1）, 候選人_10（選區2）
                        district_candidates = {}
                        for col in cand_cols:
                            if '_' in col and col.startswith('候選人_'):
                                # 提取選區號（假設候選人_N中N是選區號）
                                try:
                                    district_num = int(col.split('_')[1])
                                    if district_num not in district_candidates:
                                        district_candidates[district_num] = []
                                    district_candidates[district_num].append(col)
                                except (ValueError, IndexError):
                                    pass
                            elif not col.startswith('候選人_'):
                                # 真實候選人名稱（沒有候選人_前綴）
                                # 這些是選區1的候選人（或無法判斷選區）
                                if 1 not in district_candidates:
                                    district_candidates[1] = []
                                district_candidates[1].append(col)
                        
                        # 如果有多個選區，分別輸出
                        if len(district_candidates) > 0:
                            for district_num in sorted(district_candidates.keys()):
                                cols = district_candidates[district_num]
                                base_cols = ['行政區別', '村里別']
                                if '投開票所別' in df.columns:
                                    base_cols.append('投開票所別')
                                
                                # 選擇該選區的候選人欄位和統計欄位
                                stat_cols = ['有效票數A', '無效票數B', '投票數C', '已領未投票數D', 
                                           '發出票數E', '用餘票數F', '選舉人數G', '投票率H']
                                output_cols = base_cols + cols + [c for c in stat_cols if c in df.columns]
                                df_district = df[output_cols].copy()
                                
                                # 過濾掉該選區沒有票數的行
                                # （避免輸出大量零票數的資料）
                                df_district = df_district[df_district[cols].sum(axis=1) > 0].copy()
                                
                                if len(df_district) == 0:
                                    continue
                                
                                # 移除候選人欄位的號次前綴（候選人_N → 真實名稱）
                                # 但這裡的候選人名稱已經是真實名稱了，不需要移除
                                
                                # 原住民立委的處理
                                if '山地原住民' in election_type:
                                    district_type = '山地原住民'
                                elif '平地原住民' in election_type:
                                    district_type = '平地原住民'
                                else:
                                    district_type = '區域'
                                
                                # 生成檔名
                                if '原住民' in district_type:
                                    new_filename = f"{year}_立法委員_{district_type}_第{district_num:02d}選區_{county_name}.csv"
                                else:
                                    new_filename = f"{year}_立法委員_第{district_num:02d}選區_{county_name}.csv"
                                
                                target_file = target_folder / new_filename
                                df_district.to_csv(target_file, index=False, encoding='utf-8-sig')
                                print(f"      ✓ {new_filename} ({len(df_district)} 筆, {len(cols)} 候選人)")
                        else:
                            # 無法分選區，直接輸出
                            new_filename = f"{year}_{election_type}_{county_name}.csv"
                            target_file = target_folder / new_filename
                            df.to_csv(target_file, index=False, encoding='utf-8-sig')
                    else:
                        # 非立委選舉，直接輸出
                        new_filename = f"{year}_{election_type}_{county_name}.csv"
                        target_file = target_folder / new_filename
                        df.to_csv(target_file, index=False, encoding='utf-8-sig')

                except Exception as e:
                    print(f"    警告: 無法處理 {csv_file.name} - {e}")
                    continue

            print(f"    ✓ 已匯入 {len(csv_files_filtered)} 個檔案到 {county_name}/")

        print("\n匯入完成！")

    def merge_all_counties(self):
        """合併所有縣市的資料"""
        print("\n開始合併各縣市資料...")

        # 遍歷所有縣市資料夾
        county_folders = [f for f in self.output_base.iterdir() if f.is_dir()]

        for county_folder in sorted(county_folders):
            county_name = county_folder.name
            self.merge_county_files(county_name)

        print("\n合併完成！")

    def output_by_county_district(self, df_result, year, election_type, base_cols, cand_cols, stat_cols, aggregate_level, candidates_map=None, cand_dict=None):
        """按縣市和選區分檔輸出（candidates_map可以是district_candidates或county_candidates）"""

        # 按縣市分組 - 對於縣市代碼='10'的情況，需要同時按鄉鎮代碼分組
        if '鄉鎮代碼' in df_result.columns:
            # 建立複合鍵 (縣市代碼, 鄉鎮代碼)
            county_groups = df_result.groupby(['縣市代碼', '鄉鎮代碼'])
        else:
            # 只按縣市代碼分組
            county_groups = df_result.groupby(['縣市代碼'])

        for group_key, df_county in county_groups:
            if len(df_county) == 0:
                continue

            # 解析分組鍵
            if isinstance(group_key, tuple):
                county_code, township_code = group_key
            else:
                county_code = group_key
                township_code = None

            # 從縣市代碼取得正確的縣市名稱
            county_name = self.get_county_name_from_code(county_code, township_code)

            # 如果是全國，跳過
            if county_name == '全國':
                continue

            # 建立縣市資料夾
            county_folder = self.output_base / county_name
            county_folder.mkdir(exist_ok=True)

            # 檢查是否有選區代碼
            if '選區代碼' in df_county.columns and aggregate_level == 'polling':
                # 按選區分檔
                for district_code in df_county['選區代碼'].unique():
                    if district_code in ['00', '000'] or not district_code:
                        continue

                    try:
                        district_num = int(district_code)
                        if district_num == 0:
                            continue
                    except ValueError:
                        continue

                    df_district = df_county[df_county['選區代碼'] == district_code].copy()

                    if len(df_district) == 0:
                        continue

                    # 轉換選區代碼為數字
                    if len(district_code) == 3:
                        district_num = int(district_code) // 10
                    else:
                        district_num = int(district_code)

                    # 過濾候選人欄位：只保留該選區的候選人
                    # 對於縣市議員等選舉，使用票數過濾更準確（避免不同選區有相同號次但不同候選人的問題）
                    if '縣市議員' in election_type or '直轄市議員' in election_type or '鄉鎮市民代表' in election_type:
                        # 只保留在該選區有票數的候選人
                        filtered_cand_cols = [col for col in cand_cols 
                                            if df_district[col].sum() > 0]
                    elif candidates_map and district_code in candidates_map:
                        valid_candidates = candidates_map[district_code]
                        # 過濾候選人欄位：候選人號次在該選區的候選人集合中
                        filtered_cand_cols = [col for col in cand_cols
                                            if any(col.startswith(f"{num}_") or col.startswith(f"({num})_")
                                                   for num in valid_candidates)]
                    else:
                        # 如果沒有候選人映射，則過濾掉所有票數為0的候選人欄位
                        # 這適用於縣市議員等需要按選區分檔的選舉
                        if '縣市議員' in election_type or '直轄市議員' in election_type or '鄉鎮市民代表' in election_type:
                            filtered_cand_cols = [col for col in cand_cols 
                                                if df_district[col].sum() > 0]
                        else:
                            filtered_cand_cols = cand_cols

                    # 輸出檔案
                    output_cols = base_cols + filtered_cand_cols + stat_cols
                    df_output = df_district[output_cols].copy()

                    # 移除行政區別欄位（投開票所層級沒有意義）
                    if '行政區別' in df_output.columns and aggregate_level == 'polling':
                        df_output = df_output.drop(columns=['行政區別'])
                    # 清理行政區別欄位，移除省名稱（村里層級保留）
                    elif '行政區別' in df_output.columns:
                        df_output['行政區別'] = df_output['行政區別'].apply(self.clean_location_name)

                    # 移除候選人欄位名稱的號次前綴（格式：1_候選人名 → 候選人名）
                    rename_map = {}
                    for col in filtered_cand_cols:
                        if '_' in col:
                            parts = col.split('_', 1)
                            # 檢查第一部分是否為數字（號次）
                            if parts[0].isdigit() or (parts[0].startswith('(') and parts[0].rstrip(')').isdigit()):
                                rename_map[col] = parts[1]  # 移除號次，只保留名字
                    
                    if rename_map:
                        df_output = df_output.rename(columns=rename_map)

                    output_file = county_folder / self.generate_output_filename(year, election_type, county_name, district=district_num)
                    df_output.to_csv(output_file, index=False, encoding='utf-8-sig')
                    print(f"      已輸出: {output_file.name} ({len(df_output)} 筆, {len(filtered_cand_cols)} 候選人)")
            else:
                # 不分選區，整個縣市一個檔案
                # 過濾候選人欄位：只保留該縣市的候選人（針對村里層級如市長、總統）
                county_code_str = str(county_code).strip().strip("'")

                # 建立查詢鍵：當縣市代碼='10'時，需要使用複合鍵
                if county_code_str == '10' and township_code:
                    township_str = str(township_code).strip().strip("'")
                    lookup_key = f"{county_code_str}-{township_str}"
                else:
                    lookup_key = county_code_str

                if candidates_map and lookup_key in candidates_map:
                    valid_candidates = candidates_map[lookup_key]
                    # 過濾候選人欄位：檢查完整的欄位名稱是否在候選人集合中
                    filtered_cand_cols = [col for col in cand_cols
                                        if col in valid_candidates or col.lstrip('(').rstrip(')') in valid_candidates]
                else:
                    # 對於總統選舉（村里層級），過濾掉不屬於當前縣市的候選人列
                    # 村里層級的候選人列名格式為："縣市代碼_號次" 或 "縣市代碼-鄉鎮代碼_號次"
                    if '總統' in election_type:
                        filtered_cand_cols = []
                        
                        for col in cand_cols:
                            # 從列名提取縣市代碼（格式：縣市代碼_號次 或 縣市代碼-鄉鎮代碼_號次）
                            if '_' in col:
                                county_part = col.split('_')[0]  # "64" or "10-020"
                                # 檢查是否匹配當前lookup_key
                                if county_part == lookup_key:
                                    filtered_cand_cols.append(col)
                            else:
                                # 無法解析的列名，保留
                                filtered_cand_cols.append(col)
                    else:
                        filtered_cand_cols = cand_cols

                output_cols = base_cols + filtered_cand_cols + stat_cols
                df_output = df_county[output_cols].copy()

                # 對於村里層級總統選舉，需要將原始列名（如"64_1"）重命名為顯示名稱（如"1_柯文哲/吳欣盈"）
                if '總統' in election_type and cand_dict:
                    rename_map = {}
                    for col in filtered_cand_cols:
                        if col in cand_dict:
                            # 從"64_1"提取號次"1"
                            number_part = col.split('_')[-1] if '_' in col else col
                            display_name = f"{number_part}_{cand_dict[col]}"
                            rename_map[col] = display_name
                    if rename_map:
                        df_output.rename(columns=rename_map, inplace=True)

                # 移除行政區別欄位（投開票所層級沒有意義，因為村里別已經包含地理資訊）
                if '行政區別' in df_output.columns and aggregate_level == 'polling':
                    df_output = df_output.drop(columns=['行政區別'])
                # 清理行政區別欄位，移除省名稱（村里層級保留）
                elif '行政區別' in df_output.columns:
                    df_output['行政區別'] = df_output['行政區別'].apply(self.clean_location_name)

                # 移除候選人欄位名稱的號次前綴（格式：1_候選人名 → 候選人名）
                # 注意：總統選舉已經在上面處理過了，這裡主要處理其他不分選區的選舉
                if '總統' not in election_type:
                    rename_map = {}
                    for col in df_output.columns:
                        if '_' in col and col not in base_cols and col not in stat_cols:
                            parts = col.split('_', 1)
                            # 檢查第一部分是否為數字（號次）
                            if parts[0].isdigit() or (parts[0].startswith('(') and parts[0].rstrip(')').isdigit()):
                                rename_map[col] = parts[1]  # 移除號次，只保留名字
                    
                    if rename_map:
                        df_output = df_output.rename(columns=rename_map)

                output_file = county_folder / self.generate_output_filename(year, election_type, county_name)
                df_output.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"      已輸出: {output_file.name} ({len(df_output)} 筆, {len(filtered_cand_cols)} 候選人)")

    def process_all_years(self):
        """遍歷並處理所有年份資料夾"""
        print("開始處理選舉資料（2010-2024）...")
        print("="*80)

        # 遍歷所有資料夾
        for folder_name, config in self.config.FOLDER_MAPPINGS.items():
            folder_path = self.base_path / folder_name

            if not folder_path.exists():
                print(f"\n跳過不存在的資料夾: {folder_name}")
                continue

            print(f"\n處理資料夾: {folder_name} ({config['year']}年)")

            # 根據structure類型分發處理
            structure = config['structure']

            if structure == 'old':
                self.process_old_format(folder_path, config)
            elif structure == 'chinese_folder_quote_prefix':
                self.process_chinese_folder_quote_prefix(folder_path, config)
            elif structure == 'chinese_folder_quote':
                self.process_chinese_folder_quote(folder_path, config)
            elif structure == 'special_2016':
                self.process_2016_election(folder_path, config)
            elif structure == 'code_system_2022':
                self.process_2022_election(folder_path, config)

        print("\n" + "="*80)
        print("所有資料處理完成！")
        print(f"輸出目錄: {self.output_base}")

    def process_old_format(self, folder_path, config):
        """處理舊格式資料（無引號）- 3/5屆立委, 9任總統, 2010鄉鎮市民代表"""
        print(f"  使用舊格式處理器")

        year = config['year']
        election_name = config['name']

        # 檢查是否有子資料夾
        subfolders = [f for f in folder_path.iterdir() if f.is_dir()]

        if len(subfolders) > 0:
            # 有子資料夾，遍歷處理
            for subfolder in subfolders:
                subfolder_name = subfolder.name

                # 判斷選舉類型
                if subfolder_name in ['區域', '山原', '平原']:
                    # 立委資料
                    if subfolder_name == '區域':
                        election_type = '立法委員'
                        aggregate_level = 'polling'
                        is_president = False
                    elif subfolder_name == '山原':
                        election_type = '立法委員_山地原住民'
                        aggregate_level = 'polling'
                        is_president = False
                    elif subfolder_name == '平原':
                        election_type = '立法委員_平地原住民'
                        aggregate_level = 'polling'
                        is_president = False

                    try:
                        self.process_election_folder(
                            subfolder,
                            year,
                            election_type,
                            aggregate_level=aggregate_level,
                            is_president=is_president
                        )
                    except Exception as e:
                        print(f"      錯誤: {e}")
                        import traceback
                        traceback.print_exc()
                else:
                    print(f"  跳過未知子資料夾: {subfolder_name}")
        else:
            # 沒有子資料夾，直接處理（如9任總統）
            if election_name == '總統':
                aggregate_level = 'village'
                is_president = True
            else:
                aggregate_level = 'polling'
                is_president = False

            try:
                self.process_election_folder(
                    folder_path,
                    year,
                    election_name,
                    aggregate_level=aggregate_level,
                    is_president=is_president
                )
            except Exception as e:
                print(f"      錯誤: {e}")
                import traceback
                traceback.print_exc()

    def process_chinese_folder_quote_prefix(self, folder_path, config):
        """處理引號格式（帶'前綴）- 20101127, 20120114, 2014, 2018"""
        print(f"  使用引號前綴格式處理器")

        year = config['year']

        # 遍歷中文資料夾（與quote格式相同，因為read_csv_auto_detect已處理引號前綴）
        for subfolder in folder_path.iterdir():
            if not subfolder.is_dir():
                continue

            folder_name = subfolder.name

            # 檢查是否在映射表中
            if folder_name not in self.config.CHINESE_FOLDER_MAPPING:
                print(f"  跳過未知資料夾: {folder_name}")
                continue

            folder_config = self.config.CHINESE_FOLDER_MAPPING[folder_name]

            # 檢查是否排除
            if folder_config.get('aggregate') == 'exclude':
                print(f"  排除: {folder_name}")
                continue

            # 取得選舉類型和彙總層級
            election_type = folder_config['name']
            aggregate_level = folder_config['aggregate']
            is_president = (election_type == '總統')

            # 處理資料
            try:
                self.process_election_folder(
                    subfolder,
                    year,
                    election_type,
                    aggregate_level=aggregate_level,
                    is_president=is_president
                )
            except Exception as e:
                print(f"      錯誤: {e}")
                import traceback
                traceback.print_exc()

    def process_chinese_folder_quote(self, folder_path, config):
        """處理引號格式（無'前綴）- 2020/2024"""
        print(f"  使用引號格式處理器")

        year = config['year']

        # 遍歷中文資料夾
        for subfolder in folder_path.iterdir():
            if not subfolder.is_dir():
                continue

            folder_name = subfolder.name

            # 檢查是否在映射表中
            if folder_name not in self.config.CHINESE_FOLDER_MAPPING:
                print(f"  跳過未知資料夾: {folder_name}")
                continue

            folder_config = self.config.CHINESE_FOLDER_MAPPING[folder_name]

            # 檢查是否排除
            if folder_config.get('aggregate') == 'exclude':
                print(f"  排除: {folder_name}")
                continue

            # 取得選舉類型和彙總層級
            election_type = folder_config['name']
            aggregate_level = folder_config['aggregate']
            is_president = (election_type == '總統')

            # 處理資料
            try:
                self.process_election_folder(
                    subfolder,
                    year,
                    election_type,
                    aggregate_level=aggregate_level,
                    is_president=is_president
                )
            except Exception as e:
                print(f"      錯誤: {e}")
                import traceback
                traceback.print_exc()

    def process_2016_election(self, folder_path, config):
        """特殊處理：合併2016 old和新資料夾"""
        print(f"  使用2016特殊處理器（合併old+新）")
        print(f"  TODO: 2016合併邏輯尚未實作")
        # TODO: 實作2016合併邏輯

    def process_2022_election(self, folder_path, config):
        """特殊處理：2022代碼系統轉換"""
        print(f"  使用2022代碼系統處理器")
        print(f"  TODO: 2022代碼系統處理邏輯尚未實作")
        # TODO: 實作2022代碼系統處理邏輯


def test_csv_reading():
    """測試不同格式CSV讀取"""
    print("測試CSV自動檢測讀取...")
    print("="*80)

    base_path = Path('/Users/melowu/Desktop/CEC_data_clearn_and_combine/各縣市候選人分類/votedata/voteData')

    # 測試舊格式（無引號）
    print("\n1. 測試舊格式（無引號）- 3屆立委")
    old_format = base_path / '3屆立委/區域/elbase.csv'
    if old_format.exists():
        df = read_csv_auto_detect(old_format)
        print(f"   讀取成功: {len(df)} 筆資料")
        print(f"   第一列: {df.iloc[0].tolist()}")

    # 測試引號格式（帶'前綴）
    print("\n2. 測試引號格式（帶'前綴）- 20101127五都")
    quote_prefix = base_path / '20101127-五都市長議員及里長/市長/elbase.csv'
    if quote_prefix.exists():
        df = read_csv_auto_detect(quote_prefix)
        print(f"   讀取成功: {len(df)} 筆資料")
        print(f"   第一列: {df.iloc[0].tolist()}")

    # 測試引號格式（無'前綴）
    print("\n3. 測試引號格式（無'前綴）- 2020總統立委")
    quote_no_prefix = base_path / '2020總統立委/總統/elbase.csv'
    if quote_no_prefix.exists():
        df = read_csv_auto_detect(quote_no_prefix)
        print(f"   讀取成功: {len(df)} 筆資料")
        print(f"   第一列: {df.iloc[0].tolist()}")

    print("\n" + "="*80)
    print("CSV讀取測試完成！")


def main():
    """主程式"""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        # 測試模式
        test_csv_reading()
    else:
        # 正式處理
        base_path = Path('/Users/melowu/Desktop/CEC_data_clearn_and_combine/各縣市候選人分類/votedata/voteData')
        processor = UnifiedElectionProcessor(base_path)
        processor.process_all_years()


if __name__ == '__main__':
    main()
