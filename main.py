#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
選舉資料處理系統 - 鄰里整理輸出
支援年份：2014-2024
輸出格式：符合「鄰里整理範例」格式
"""
import pandas as pd
import numpy as np
import os
import re
from pathlib import Path

# ============================================================================
# 基本設定
# ============================================================================
BASE_DIR = Path(__file__).parent
VOTE_DATA_DIR = BASE_DIR / "voteData"
OUTPUT_DIR = BASE_DIR / "鄰里整理輸出"

# 縣市配置
COUNTY_CONFIG = {
    '花蓮縣': {'prv_code': 10, 'city_code': 15, 'is_municipality': False},
    '臺北市': {'prv_code': 63, 'city_code': 0, 'is_municipality': True},
}

# 選舉年份類型
LOCAL_YEARS = [2014, 2018, 2022]  # 地方公職選舉
PRESIDENTIAL_YEARS = [2016, 2020, 2024]  # 總統立委選舉

# 資料夾映射
YEAR_FOLDER_MAP = {
    2014: '2014-103年地方公職人員選舉',
    2016: '2016總統立委',
    2018: '2018-107年地方公職人員選舉',
    2020: '2020總統立委',
    2022: '2022-111年地方公職人員選舉',
    2024: '2024總統立委',
}

# 政黨代碼映射
PARTY_CODE_MAP = {
    '1': '中國國民黨', '2': '民主進步黨', '3': '親民黨', '4': '台灣團結聯盟',
    '5': '無黨團結聯盟', '6': '綠黨', '7': '新黨', '8': '台灣基進',
    '9': '台灣民眾黨', '10': '時代力量', '11': '一邊一國行動黨',
    '12': '勞動黨', '13': '中華統一促進黨', '14': '國會政黨聯盟',
    '15': '台澎黨', '16': '民主進步黨', '17': '社會民主黨',
    '18': '和平鴿聯盟黨', '19': '喜樂島聯盟', '20': '安定力量',
    '21': '合一行動聯盟', '90': '親民黨', '99': '無黨籍及未經政黨推薦',
    '999': '無黨籍及未經政黨推薦', '348': '喜樂島聯盟',
}


# ============================================================================
# 工具函數
# ============================================================================
def find_file(directory, candidates):
    """在目錄中尋找存在的檔案"""
    for name in candidates:
        path = directory / name
        if path.exists():
            return path
    return None


def load_csv_with_encoding(filepath, **kwargs):
    """嘗試多種編碼讀取 CSV"""
    encodings = ['utf-8', 'utf-8-sig', 'cp950', 'big5']
    for enc in encodings:
        try:
            return pd.read_csv(filepath, encoding=enc, **kwargs)
        except:
            continue
    return None


def normalize_district_name(name):
    """正規化行政區名稱 - 移除縣市前綴和選區標記"""
    if not isinstance(name, str):
        return name
    # 移除選舉區/選區標記 (如 "第02選區", "第1選舉區")
    name = re.sub(r'第\d+選舉?區', '', name)
    # 移除縣市前綴
    prefixes = r'^(臺北市|新北市|桃園市|臺中市|臺南市|高雄市|基隆市|新竹市|嘉義市|' \
               r'宜蘭縣|新竹縣|苗栗縣|彰化縣|南投縣|雲林縣|嘉義縣|屏東縣|' \
               r'臺東縣|花蓮縣|澎湖縣|金門縣|連江縣)'
    return re.sub(prefixes, '', name)


# ============================================================================
# 資料讀取與處理
# ============================================================================
class ElectionDataProcessor:
    """選舉資料處理器"""

    def __init__(self, county, year):
        self.county = county
        self.year = year
        self.config = COUNTY_CONFIG[county]
        self.prv_code = self.config['prv_code']
        self.city_code = self.config['city_code']
        self.is_municipality = self.config['is_municipality']
        self.year_folder = VOTE_DATA_DIR / YEAR_FOLDER_MAP.get(year, '')

    def _read_elctks(self, folder_path):
        """讀取選舉票數資料"""
        elctks = find_file(folder_path, ['elctks.csv', 'elctks_T1.csv', 'elctks_P1.csv', 'elctks_T4.csv'])
        if not elctks:
            return None

        col_names = ['prv_code', 'city_code', 'area_code', 'dept_code', 'li_code',
                     'tks', 'cand_no', 'ticket_num', 'ratio', 'elected']
        df = pd.read_csv(elctks, names=col_names, header=None, dtype=str, quotechar='"')

        # 清理資料
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].str.replace("'", "").str.replace('"', "").str.strip()

        return df

    def _read_elbase(self, folder_path):
        """讀取選區基本資料"""
        elbase = find_file(folder_path, ['elbase.csv', 'elbase_T1.csv', 'elbase_P1.csv', 'elbase_T4.csv'])
        if not elbase:
            return None

        col_names = ['prv_code', 'city_code', 'area_code', 'dept_code', 'li_code', 'name']
        df = load_csv_with_encoding(elbase, names=col_names, header=None, dtype=str, quotechar='"')

        if df is not None:
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].str.replace("'", "").str.replace('"', "").str.strip()

        return df

    def _read_elcand(self, folder_path):
        """讀取候選人資料"""
        elcand = find_file(folder_path, ['elcand.csv', 'elcand_T1.csv', 'elcand_P1.csv', 'elcand_T4.csv'])
        if not elcand:
            return None

        df = load_csv_with_encoding(elcand, header=None, dtype=str, quotechar='"')
        if df is None:
            return None

        # 設定欄位名稱
        expected_cols = ['prv_code', 'city_code', 'area_code', 'dept_code', 'li_code',
                        'cand_no', 'name', 'party', 'gender', 'birth', 'age',
                        'address', 'education', 'elected', 'incumbent']
        actual_cols = expected_cols[:len(df.columns)]
        if len(df.columns) > len(expected_cols):
            actual_cols += [f'extra{i}' for i in range(len(df.columns) - len(expected_cols))]
        df.columns = actual_cols

        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].str.replace("'", "").str.replace('"', "").str.strip()

        return df

    def _read_elprof(self, folder_path):
        """讀取統計資料"""
        elprof = folder_path / "elprof.csv"
        if not elprof.exists():
            return None

        df = pd.read_csv(elprof, header=None, dtype=str, quotechar='"')
        return df

    def _filter_by_county(self, df, is_president=False, is_indigenous=False):
        """根據縣市過濾資料"""
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()
        df['prv_code'] = pd.to_numeric(df['prv_code'], errors='coerce')
        df['city_code'] = pd.to_numeric(df['city_code'], errors='coerce')

        if is_president or is_indigenous:
            # 總統/原住民立委: 候選人在全國層級 (prv=0, city=0)
            return df[(df['prv_code'] == 0) & (df['city_code'] == 0)]
        elif self.city_code == 0:
            # 直轄市
            return df[df['prv_code'] == self.prv_code]
        else:
            # 非直轄市
            return df[(df['prv_code'] == self.prv_code) & (df['city_code'] == self.city_code)]

    def _build_district_village_maps(self, df_base):
        """建立行政區和村里映射"""
        if df_base is None or df_base.empty:
            return {}, {}

        df_base = df_base.copy()

        # 行政區映射 (li_code == '0000' 表示鄉鎮市區層級)
        dist_df = df_base[df_base['li_code'].astype(str).str.strip() == '0000']
        dist_map = {}
        for _, row in dist_df.iterrows():
            dept_code = str(row['dept_code']).strip()
            name = normalize_district_name(str(row['name']).strip())
            dist_map[dept_code] = name

        # 村里映射
        village_df = df_base[df_base['li_code'].astype(str).str.strip() != '0000']
        village_map = {}
        for _, row in village_df.iterrows():
            dept_code = str(row['dept_code']).strip()
            li_code = str(row['li_code']).strip()
            name = str(row['name']).strip()
            village_map[f"{dept_code}_{li_code}"] = name

        return dist_map, village_map

    def _build_candidate_map(self, df_cand, is_president=False):
        """建立候選人映射"""
        if df_cand is None or df_cand.empty:
            return {}, {}

        df_cand = df_cand.copy()

        # 正規化代碼
        df_cand['area_code'] = df_cand['area_code'].astype(str).str.zfill(2)
        df_cand['cand_no'] = df_cand['cand_no'].astype(str).str.zfill(2)
        df_cand['map_key'] = df_cand['area_code'] + "_" + df_cand['cand_no']

        # 政黨映射
        df_cand['party_name'] = df_cand['party'].astype(str).map(PARTY_CODE_MAP).fillna('無黨籍及未經政黨推薦')

        if is_president:
            # 總統選舉: 合併正副總統名稱
            name_map = {}
            for cand_no in df_cand['cand_no'].unique():
                group = df_cand[df_cand['cand_no'] == cand_no]
                if len(group) == 2:
                    names = group['name'].tolist()
                    name_map[group.iloc[0]['map_key']] = f"{names[0]}/{names[1]}"
                elif len(group) == 1:
                    name_map[group.iloc[0]['map_key']] = group.iloc[0]['name']
        else:
            name_map = df_cand.set_index('map_key')['name'].to_dict()

        party_map = df_cand.drop_duplicates('map_key').set_index('map_key')['party_name'].to_dict()

        return name_map, party_map

    def process_election_type(self, folder_name, prefix, is_president=False, is_indigenous=False):
        """處理單一選舉類型"""
        folder_path = self.year_folder / folder_name
        if not folder_path.exists():
            print(f"    [SKIP] 資料夾不存在: {folder_name}")
            return None

        # 讀取資料
        df_tks = self._read_elctks(folder_path)
        df_base = self._read_elbase(folder_path)
        df_cand = self._read_elcand(folder_path)
        df_prof = self._read_elprof(folder_path)

        if df_tks is None or df_base is None:
            print(f"    [ERROR] 缺少必要資料: {folder_name}")
            return None

        # 過濾票數資料
        df_tks['prv_code'] = pd.to_numeric(df_tks['prv_code'].str.replace("'", ""), errors='coerce')
        df_tks['city_code'] = pd.to_numeric(df_tks['city_code'].str.replace("'", ""), errors='coerce')

        if self.city_code == 0:
            df_filtered = df_tks[df_tks['prv_code'] == self.prv_code].copy()
        else:
            df_filtered = df_tks[(df_tks['prv_code'] == self.prv_code) &
                                (df_tks['city_code'] == self.city_code)].copy()

        if df_filtered.empty:
            print(f"    [WARN] 無資料: {folder_name}")
            return None

        # 過濾基本資料
        df_base['prv_code'] = pd.to_numeric(df_base['prv_code'], errors='coerce')
        df_base['city_code'] = pd.to_numeric(df_base['city_code'], errors='coerce')

        if self.city_code == 0:
            df_base_filtered = df_base[df_base['prv_code'] == self.prv_code].copy()
        else:
            df_base_filtered = df_base[(df_base['prv_code'] == self.prv_code) &
                                       (df_base['city_code'] == self.city_code)].copy()

        # 建立映射
        dist_map, village_map = self._build_district_village_maps(df_base_filtered)

        # 過濾候選人資料
        df_cand_filtered = self._filter_by_county(df_cand, is_president, is_indigenous)
        name_map, party_map = self._build_candidate_map(df_cand_filtered, is_president)

        # 轉換資料
        df_filtered['ticket_num'] = pd.to_numeric(df_filtered['ticket_num'], errors='coerce').fillna(0).astype(int)
        df_filtered['dept_code'] = df_filtered['dept_code'].astype(str)
        df_filtered['li_code'] = df_filtered['li_code'].astype(str)
        df_filtered['area_code'] = df_filtered['area_code'].astype(str).str.zfill(2)
        df_filtered['cand_no'] = df_filtered['cand_no'].astype(str).str.zfill(2)

        df_filtered['行政區別'] = df_filtered['dept_code'].map(dist_map)
        df_filtered['village_key'] = df_filtered['dept_code'] + "_" + df_filtered['li_code']
        df_filtered['村里別'] = df_filtered['village_key'].map(village_map)

        # 建立候選人查詢鍵
        df_filtered['cand_key'] = df_filtered['area_code'] + "_" + df_filtered['cand_no']
        df_filtered['候選人名稱'] = df_filtered['cand_key'].map(name_map)
        df_filtered['黨籍'] = df_filtered['cand_key'].map(party_map)

        # 過濾無效資料
        df_filtered = df_filtered.dropna(subset=['行政區別', '村里別'])
        df_filtered = df_filtered[df_filtered['村里別'] != '']
        df_filtered = df_filtered[~df_filtered['行政區別'].isin(['總計', ''])]
        df_filtered = df_filtered[~df_filtered['村里別'].isin(['總計', ''])]

        if df_filtered.empty:
            return None

        # 建立候選人欄位
        candidates = df_filtered[['cand_key', '候選人名稱', '黨籍']].drop_duplicates()
        candidates = candidates.sort_values('cand_key')

        # 建立樞紐表
        pivot = df_filtered.pivot_table(
            index=['行政區別', '村里別'],
            columns='候選人名稱',
            values='ticket_num',
            aggfunc='sum',
            fill_value=0
        ).reset_index()

        # 轉換為目標格式
        result = pd.DataFrame()
        result['行政區別'] = pivot['行政區別']
        result['村里別'] = pivot['村里別']

        # 取得候選人順序
        cand_names = [c for c in candidates['候選人名稱'].tolist() if c in pivot.columns]
        cand_party = candidates.set_index('候選人名稱')['黨籍'].to_dict()

        # 新增候選人欄位
        for i, name in enumerate(cand_names, 1):
            party = cand_party.get(name, '')

            if is_president:
                # 總統格式: 候選人N (name), 候選人N_得票數
                result[f'{prefix}候選人{i}'] = name
                result[f'{prefix}候選人{i}_得票數'] = pivot[name].astype(int)
            else:
                # 其他格式
                if prefix in ['區域立委', '平原立委', '山原立委', '區域縣市議員', '平原縣市議員', '山原縣市議員']:
                    # 立委/議員格式: 候選人1只有黨籍, 候選人2+有名稱
                    if i == 1:
                        result[f'{prefix}候選人{i}＿黨籍'] = party
                        result[f'{prefix}候選人{i}_得票數'] = pivot[name].astype(int)
                    else:
                        result[f'{prefix}候選人{i}＿候選人名稱'] = name
                        result[f'{prefix}候選人{i}＿黨籍'] = party
                        result[f'{prefix}候選人{i}_得票數'] = pivot[name].astype(int)
                else:
                    # 市長/鄉鎮市長格式: 完整格式
                    result[f'{prefix}候選人{i}＿候選人名稱'] = name
                    result[f'{prefix}候選人{i}＿黨籍'] = party
                    result[f'{prefix}候選人{i}_得票數'] = pivot[name].astype(int)

        # 加入統計資料
        if df_prof is not None:
            result = self._add_statistics(result, df_prof, dist_map, village_map, prefix)

        print(f"    [OK] {folder_name}: {len(result)} rows, {len(cand_names)} candidates")
        return result

    def _add_statistics(self, result, df_prof, dist_map, village_map, prefix):
        """加入統計資料"""
        try:
            df_prof = df_prof.copy()
            df_prof[0] = pd.to_numeric(df_prof[0], errors='coerce')
            df_prof[1] = pd.to_numeric(df_prof[1], errors='coerce')

            if self.city_code == 0:
                df_prof_filtered = df_prof[df_prof[0] == self.prv_code].copy()
            else:
                df_prof_filtered = df_prof[(df_prof[0] == self.prv_code) &
                                           (df_prof[1] == self.city_code)].copy()

            if df_prof_filtered.empty:
                return result

            df_prof_filtered['dept_code'] = df_prof_filtered[3].astype(str)
            df_prof_filtered['li_code'] = df_prof_filtered[4].astype(str)
            df_prof_filtered['village_key'] = df_prof_filtered['dept_code'] + "_" + df_prof_filtered['li_code']
            df_prof_filtered['行政區別'] = df_prof_filtered['dept_code'].map(dist_map)
            df_prof_filtered['村里別'] = df_prof_filtered['village_key'].map(village_map)

            # 統計欄位映射
            stat_cols = {
                5: f'{prefix}有效票數A',
                6: f'{prefix}無效票數B',
                7: f'{prefix}投票數C',
                11: f'{prefix}選舉人數G'
            }

            for idx, col_name in stat_cols.items():
                if idx < len(df_prof_filtered.columns):
                    df_prof_filtered[col_name] = pd.to_numeric(df_prof_filtered[idx], errors='coerce').fillna(0).astype(int)

            # 合併
            merge_cols = ['行政區別', '村里別'] + list(stat_cols.values())
            merge_cols = [c for c in merge_cols if c in df_prof_filtered.columns]

            df_stats = df_prof_filtered[merge_cols].drop_duplicates(subset=['行政區別', '村里別'])
            result = result.merge(df_stats, on=['行政區別', '村里別'], how='left')

        except Exception as e:
            print(f"    [WARN] 統計資料處理失敗: {e}")

        return result

    def process_party_list(self):
        """處理不分區政黨資料"""
        folder_path = self.year_folder / "不分區政黨"
        if not folder_path.exists():
            return None

        # 讀取資料
        elctks = find_file(folder_path, ['elctks.csv', 'elctks_T4.csv'])
        elcand = find_file(folder_path, ['elcand.csv', 'elcand_T4.csv'])
        elbase = find_file(folder_path, ['elbase.csv', 'elbase_T4.csv'])

        if not elctks:
            return None

        # 讀取票數
        df_tks = pd.read_csv(elctks, header=None, dtype=str)

        prv_str = str(self.prv_code).zfill(2)
        city_str = str(self.city_code).zfill(3) if self.city_code > 0 else '000'

        df_filtered = df_tks[(df_tks[0] == prv_str) & (df_tks[1] == city_str)].copy()
        df_filtered = df_filtered[df_filtered[4] != '0000']  # 排除總計

        if df_filtered.empty:
            return None

        # 讀取政黨名稱
        party_names = {}
        if elcand:
            df_cand = pd.read_csv(elcand, header=None, dtype=str)
            for _, row in df_cand.iterrows():
                party_no = str(row[5]).strip()
                party_name = str(row[6]).strip()
                if party_no and party_name:
                    party_names[party_no] = party_name

        # 讀取村里名稱
        village_names = {}
        if elbase:
            df_base = pd.read_csv(elbase, header=None, dtype=str)
            df_base_f = df_base[(df_base[0] == prv_str) & (df_base[1] == city_str)]
            for _, row in df_base_f.iterrows():
                dept = str(row[3]).strip()
                li = str(row[4]).strip()
                name = str(row[5]).strip()
                if li != '0000':
                    village_names[(dept, li)] = name

        # 建立資料
        df_filtered['dept'] = df_filtered[3].astype(str)
        df_filtered['li'] = df_filtered[4].astype(str)
        df_filtered['party_no'] = df_filtered[6].astype(str)
        df_filtered['votes'] = pd.to_numeric(df_filtered[7], errors='coerce').fillna(0).astype(int)

        # 樞紐表
        df_grouped = df_filtered.groupby(['dept', 'li', 'party_no'])['votes'].sum().reset_index()
        df_grouped['村里別'] = df_grouped.apply(lambda r: village_names.get((r['dept'], r['li']), ''), axis=1)

        df_pivot = df_grouped.pivot_table(
            index='村里別',
            columns='party_no',
            values='votes',
            fill_value=0,
            aggfunc='sum'
        ).reset_index()

        # 重新命名欄位為政黨名稱格式
        new_cols = ['村里別']
        party_cols = []
        for i, col in enumerate(df_pivot.columns[1:], 1):
            party_name = party_names.get(str(col), f'政黨{col}')
            new_col = f'不分區政黨({i})\n\n{party_name}'
            new_cols.append(new_col)
            party_cols.append(new_col)
        df_pivot.columns = new_cols

        # 計算有效票數
        if party_cols:
            df_pivot['不分區政黨有效票數A\nA=1+2+...+N'] = df_pivot[party_cols].sum(axis=1).astype(int)

        print(f"    [OK] 不分區政黨: {len(df_pivot)} rows, {len(party_names)} parties")
        return df_pivot


# ============================================================================
# 主處理函數
# ============================================================================
def process_local_election(county, year):
    """處理地方公職選舉 (2014/2018/2022)"""
    print(f"\n處理 {county} {year} 地方公職選舉...")

    processor = ElectionDataProcessor(county, year)
    results = []

    # 2022 使用不同的資料夾結構
    if year == 2022:
        sub_folder = 'prv' if processor.is_municipality else 'city'

        # 縣市長/直轄市長
        df = processor.process_election_type(f'C1/{sub_folder}', '縣市長' if not processor.is_municipality else '直轄市長')
        if df is not None:
            results.append(df)

        # 區域議員
        df = processor.process_election_type(f'T1/{sub_folder}', '區域縣市議員' if not processor.is_municipality else '區域直轄市議員')
        if df is not None:
            results.append(df)

        # 平原議員
        df = processor.process_election_type(f'T2/{sub_folder}', '平原縣市議員' if not processor.is_municipality else '平原直轄市議員')
        if df is not None:
            results.append(df)

        # 山原議員
        df = processor.process_election_type(f'T3/{sub_folder}', '山原縣市議員' if not processor.is_municipality else '山原直轄市議員')
        if df is not None:
            results.append(df)

        # 鄉鎮市民代表 (非直轄市)
        if not processor.is_municipality:
            df = processor.process_election_type('R1', '鄉鎮市民代表')
            if df is not None:
                results.append(df)
    else:
        # 2014/2018 使用原本的資料夾結構
        # 縣市長
        if processor.is_municipality:
            df = processor.process_election_type('直轄市市長', '直轄市長')
        else:
            df = processor.process_election_type('縣市市長', '縣市長')
        if df is not None:
            results.append(df)

        # 鄉鎮市長 (非直轄市)
        if not processor.is_municipality:
            df = processor.process_election_type('縣市鄉鎮市長', '鄉鎮市長')
            if df is not None:
                results.append(df)

        # 議員
        if processor.is_municipality:
            df = processor.process_election_type('直轄市區域議員', '區域直轄市議員')
        else:
            df = processor.process_election_type('縣市區域議員', '區域縣市議員')
        if df is not None:
            results.append(df)

        # 平原議員
        if processor.is_municipality:
            df = processor.process_election_type('直轄市平原議員', '平原直轄市議員')
        else:
            df = processor.process_election_type('縣市平原議員', '平原縣市議員')
        if df is not None:
            results.append(df)

        # 山原議員
        if processor.is_municipality:
            df = processor.process_election_type('直轄市山原議員', '山原直轄市議員')
        else:
            df = processor.process_election_type('縣市山原議員', '山原縣市議員')
        if df is not None:
            results.append(df)

        # 鄉鎮市民代表 (非直轄市)
        if not processor.is_municipality:
            df = processor.process_election_type('縣市鄉鎮市民代表', '鄉鎮市民代表')
            if df is not None:
                results.append(df)

    return merge_results(results, county)


def process_presidential_election(county, year):
    """處理總統立委選舉 (2016/2020/2024)"""
    print(f"\n處理 {county} {year} 總統立委選舉...")

    processor = ElectionDataProcessor(county, year)
    results = []

    # 總統
    df = processor.process_election_type('總統', '總統候選人', is_president=True)
    if df is not None:
        results.append(df)

    # 區域立委
    df = processor.process_election_type('區域立委', '區域立委')
    if df is not None:
        results.append(df)

    # 平地立委
    df = processor.process_election_type('平地立委', '平原立委', is_indigenous=True)
    if df is not None:
        results.append(df)

    # 山地立委
    df = processor.process_election_type('山地立委', '山原立委', is_indigenous=True)
    if df is not None:
        results.append(df)

    # 不分區政黨
    df = processor.process_party_list()
    if df is not None:
        results.append(df)

    return merge_results(results, county)


def merge_results(results, county):
    """合併多個選舉類型結果"""
    if not results:
        return None

    # 正規化行政區別（移除選區標記）
    for df in results:
        if df is not None and '行政區別' in df.columns:
            df['行政區別'] = df['行政區別'].apply(normalize_district_name)

    # 找出所有共同的合併鍵
    merge_keys = ['行政區別', '村里別']

    merged = results[0]
    for df in results[1:]:
        if df is None or df.empty:
            continue

        # 檢查合併鍵
        common_keys = [k for k in merge_keys if k in merged.columns and k in df.columns]
        if not common_keys:
            continue

        merged = merged.merge(df, on=common_keys, how='outer', suffixes=('', '_dup'))
        # 移除重複欄位
        merged = merged.loc[:, ~merged.columns.str.endswith('_dup')]

    # 加入縣市欄位
    merged.insert(0, '縣市', county)

    # 重新命名 村里別 為 鄰里
    if '村里別' in merged.columns:
        merged = merged.rename(columns={'村里別': '鄰里'})

    # 檢查重複
    if merged.duplicated(subset=['行政區別', '鄰里']).any():
        print(f"  [WARN] 發現重複鄰里，進行合併...")
        numeric_cols = merged.select_dtypes(include=[np.number]).columns.tolist()
        non_numeric_cols = [c for c in merged.columns if c not in numeric_cols and c not in ['縣市', '行政區別', '鄰里']]

        agg_dict = {c: 'sum' for c in numeric_cols}
        for c in non_numeric_cols:
            agg_dict[c] = 'first'

        merged = merged.groupby(['縣市', '行政區別', '鄰里'], as_index=False).agg(agg_dict)

    # 新增區域別代碼
    merged['區域別代碼'] = ''

    # 重新排序欄位
    base_cols = ['縣市', '行政區別', '鄰里', '區域別代碼']
    other_cols = [c for c in merged.columns if c not in base_cols]
    merged = merged[base_cols + other_cols]

    return merged


def save_output(df, county, year):
    """儲存輸出"""
    if df is None or df.empty:
        return

    output_dir = OUTPUT_DIR / county
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{year}_選舉資料_{county}.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  [SAVED] {output_file.name}: {len(df)} rows, {len(df.columns)} columns")


def verify_no_duplicate_neighborhoods(county):
    """驗證無重複鄰里"""
    output_dir = OUTPUT_DIR / county
    if not output_dir.exists():
        return True

    all_pass = True
    for f in output_dir.glob("*.csv"):
        df = pd.read_csv(f)
        if '鄰里' in df.columns and '行政區別' in df.columns:
            dups = df[df.duplicated(subset=['行政區別', '鄰里'], keep=False)]
            if not dups.empty:
                print(f"  [FAIL] {f.name}: 有 {len(dups)} 筆重複鄰里")
                all_pass = False
            else:
                print(f"  [PASS] {f.name}: 無重複鄰里")

    return all_pass


# ============================================================================
# 主程式
# ============================================================================
def main():
    print("=" * 60)
    print("選舉資料處理系統")
    print("支援年份：2014-2024")
    print("=" * 60)

    counties = ['花蓮縣', '臺北市']
    years = [2014, 2016, 2018, 2020, 2022, 2024]

    for county in counties:
        print(f"\n{'='*60}")
        print(f"處理 {county}")
        print(f"{'='*60}")

        for year in years:
            if year in LOCAL_YEARS:
                df = process_local_election(county, year)
            else:
                df = process_presidential_election(county, year)

            if df is not None:
                save_output(df, county, year)

        # 驗證
        print(f"\n驗證 {county} 資料...")
        verify_no_duplicate_neighborhoods(county)

    print("\n" + "=" * 60)
    print("處理完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
