#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
選舉資料處理系統 - 鄰里整理輸出
支援年份：2014-2024
輸出格式：符合「鄰里整理範例」格式

重要說明：
- 每個選區有不同的候選人，候選人名稱和黨籍是 cell values
- 需要先找出每種選舉類型的最大候選人數量
- 統計欄位（有效票數、選舉人數等）每種選舉類型都要有
"""
import pandas as pd
import numpy as np
import re
from pathlib import Path
from collections import defaultdict

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
LOCAL_YEARS = [2014, 2018, 2022]
PRESIDENTIAL_YEARS = [2016, 2020, 2024]

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
    '21': '合一行動聯盟', '90': '親民黨', '99': '無黨籍',
    '999': '無黨籍', '348': '喜樂島聯盟',
}


def find_file(directory, candidates):
    """在目錄中尋找存在的檔案"""
    for name in candidates:
        path = directory / name
        if path.exists():
            return path
    return None


def load_csv_safe(filepath, **kwargs):
    """安全讀取 CSV"""
    encodings = ['utf-8', 'utf-8-sig', 'cp950', 'big5']
    for enc in encodings:
        try:
            return pd.read_csv(filepath, encoding=enc, **kwargs)
        except:
            continue
    return None


def normalize_district(name):
    """正規化行政區名稱"""
    if not isinstance(name, str):
        return name
    name = re.sub(r'第\d+選舉?區', '', name)
    prefixes = r'^(臺北市|新北市|桃園市|臺中市|臺南市|高雄市|基隆市|新竹市|嘉義市|' \
               r'宜蘭縣|新竹縣|苗栗縣|彰化縣|南投縣|雲林縣|嘉義縣|屏東縣|' \
               r'臺東縣|花蓮縣|澎湖縣|金門縣|連江縣)'
    return re.sub(prefixes, '', name)


class ElectionProcessor:
    """選舉資料處理器"""

    def __init__(self, county, year):
        self.county = county
        self.year = year
        self.config = COUNTY_CONFIG[county]
        self.prv_code = self.config['prv_code']
        self.city_code = self.config['city_code']
        self.is_municipality = self.config['is_municipality']
        self.year_folder = VOTE_DATA_DIR / YEAR_FOLDER_MAP.get(year, '')

    def _clean_str(self, s):
        """清理字串"""
        if pd.isna(s):
            return ''
        return str(s).replace("'", "").replace('"', '').strip()

    def _read_raw_data(self, folder_path):
        """讀取原始資料"""
        # 讀取票數
        elctks_file = find_file(folder_path, ['elctks.csv', 'elctks_T1.csv', 'elctks_P1.csv', 'elctks_T4.csv'])
        if not elctks_file:
            return None, None, None, None

        df_tks = pd.read_csv(elctks_file, header=None, dtype=str, quotechar='"')
        for col in df_tks.columns:
            df_tks[col] = df_tks[col].apply(self._clean_str)

        # 讀取基本資料
        elbase_file = find_file(folder_path, ['elbase.csv', 'elbase_T1.csv', 'elbase_P1.csv', 'elbase_T4.csv'])
        df_base = None
        if elbase_file:
            df_base = load_csv_safe(elbase_file, header=None, dtype=str, quotechar='"')
            if df_base is not None:
                for col in df_base.columns:
                    df_base[col] = df_base[col].apply(self._clean_str)

        # 讀取候選人
        elcand_file = find_file(folder_path, ['elcand.csv', 'elcand_T1.csv', 'elcand_P1.csv', 'elcand_T4.csv'])
        df_cand = None
        if elcand_file:
            df_cand = load_csv_safe(elcand_file, header=None, dtype=str, quotechar='"')
            if df_cand is not None:
                for col in df_cand.columns:
                    df_cand[col] = df_cand[col].apply(self._clean_str)

        # 讀取統計
        elprof_file = folder_path / "elprof.csv"
        df_prof = None
        if elprof_file.exists():
            df_prof = pd.read_csv(elprof_file, header=None, dtype=str, quotechar='"')

        return df_tks, df_base, df_cand, df_prof

    def _filter_county(self, df):
        """過濾縣市資料"""
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()
        df[0] = pd.to_numeric(df[0], errors='coerce')
        df[1] = pd.to_numeric(df[1], errors='coerce')

        if self.city_code == 0:
            return df[df[0] == self.prv_code]
        else:
            return df[(df[0] == self.prv_code) & (df[1] == self.city_code)]

    def _build_maps(self, df_base, df_cand, is_president=False, key_by_dept=False):
        """建立映射表

        Args:
            key_by_dept: 若為True，候選人以dept_code為key（用於鄉鎮市長/鄉鎮市民代表）
                         若為False，以area_code為key（用於縣市長/議員/立委）
        """
        # 行政區映射 (dept_code -> name)
        dist_map = {}
        village_map = {}

        if df_base is not None and not df_base.empty:
            df_base_f = self._filter_county(df_base)
            for _, row in df_base_f.iterrows():
                dept = self._clean_str(row[3])
                li = self._clean_str(row[4])
                name = normalize_district(self._clean_str(row[5]))
                if li == '0000':
                    dist_map[dept] = name
                else:
                    village_map[f"{dept}_{li}"] = name

        # 候選人映射 (key -> list of {cand_no, name, party})
        # key可以是area_code或dept_code，取決於選舉類型
        cand_by_key = defaultdict(list)

        if df_cand is not None and not df_cand.empty:
            if is_president:
                # 總統候選人在全國層級
                df_cand_f = df_cand[(df_cand[0].astype(str) == '0') | (df_cand[0].astype(str) == '00')]
            else:
                df_cand_f = self._filter_county(df_cand)

            # Group by cand_no for president (combine president + VP)
            if is_president:
                by_cand_no = defaultdict(list)
                for _, row in df_cand_f.iterrows():
                    cand_no = self._clean_str(row[5])
                    name = self._clean_str(row[6])
                    party_code = self._clean_str(row[7])
                    party = PARTY_CODE_MAP.get(party_code, '無黨籍')
                    by_cand_no[cand_no].append({'name': name, 'party': party})

                for cand_no, items in by_cand_no.items():
                    if len(items) == 2:
                        combined_name = f"{items[0]['name']}/{items[1]['name']}"
                    else:
                        combined_name = items[0]['name'] if items else ''
                    party = items[0]['party'] if items else ''
                    cand_by_key['00'].append({
                        'cand_no': cand_no,
                        'name': combined_name,
                        'party': party
                    })
            else:
                for _, row in df_cand_f.iterrows():
                    # 鄉鎮市長/鄉鎮市民代表用dept_code (col 3)，其他用area_code (col 2)
                    if key_by_dept:
                        key = self._clean_str(row[3])  # dept_code = township
                    else:
                        key = self._clean_str(row[2]).zfill(2)  # area_code = election district
                    cand_no = self._clean_str(row[5])
                    name = self._clean_str(row[6])
                    party_code = self._clean_str(row[7])
                    party = PARTY_CODE_MAP.get(party_code, '無黨籍')
                    cand_by_key[key].append({
                        'cand_no': cand_no,
                        'name': name,
                        'party': party
                    })

        # Sort candidates by cand_no
        for key in cand_by_key:
            cand_by_key[key] = sorted(cand_by_key[key], key=lambda x: x['cand_no'])

        return dist_map, village_map, cand_by_key

    def _get_stats(self, df_prof, dept_code, li_code):
        """取得統計資料"""
        if df_prof is None or df_prof.empty:
            return {}

        df_prof_f = self._filter_county(df_prof)
        if df_prof_f.empty:
            return {}

        # Find matching row
        for _, row in df_prof_f.iterrows():
            if self._clean_str(row[3]) == dept_code and self._clean_str(row[4]) == li_code:
                return {
                    '有效票數A': int(float(row[5])) if pd.notna(row[5]) and row[5] != '' else 0,
                    '無效票數B': int(float(row[6])) if pd.notna(row[6]) and row[6] != '' else 0,
                    '投票數C': int(float(row[7])) if pd.notna(row[7]) and row[7] != '' else 0,
                    '選舉人數G': int(float(row[11])) if len(row) > 11 and pd.notna(row[11]) and row[11] != '' else 0,
                }
        return {}

    def process_election(self, folder_name, prefix, is_president=False, key_by_dept=False, max_candidates=10):
        """處理單一選舉類型，回傳原始資料供合併

        Args:
            key_by_dept: 若為True，候選人以dept_code為key（用於鄉鎮市長/鄉鎮市民代表）
        """
        folder_path = self.year_folder / folder_name
        if not folder_path.exists():
            print(f"    [SKIP] {folder_name}")
            return None

        df_tks, df_base, df_cand, df_prof = self._read_raw_data(folder_path)
        if df_tks is None:
            print(f"    [ERROR] {folder_name} - no data")
            return None

        # Filter and build maps
        df_tks_f = self._filter_county(df_tks)
        if df_tks_f.empty:
            print(f"    [WARN] {folder_name} - empty after filter")
            return None

        dist_map, village_map, cand_by_key = self._build_maps(df_base, df_cand, is_president, key_by_dept)

        # Find max candidates
        if cand_by_key:
            actual_max = max(len(v) for v in cand_by_key.values())
            max_candidates = max(max_candidates, actual_max)

        # Build result data
        # Group by (dept, li) and aggregate votes per candidate
        votes_by_village = defaultdict(lambda: defaultdict(int))
        area_by_village = {}
        dept_by_village = {}

        for _, row in df_tks_f.iterrows():
            dept = self._clean_str(row[3])
            li = self._clean_str(row[4])
            tbox_str = self._clean_str(row[5])  # voting booth number (as string)
            tbox = int(float(tbox_str)) if tbox_str != '' else 0
            area = self._clean_str(row[2]).zfill(2)
            cand_no = self._clean_str(row[6])
            votes = int(float(row[7])) if row[7] != '' else 0

            if li == '0000':
                continue

            # Only use summary rows (tbox=0) which contain pre-aggregated totals
            # This avoids double-counting from individual voting booth rows
            if tbox != 0:
                continue

            key = f"{dept}_{li}"
            votes_by_village[key][cand_no] = votes
            area_by_village[key] = area
            dept_by_village[key] = dept

        if not votes_by_village:
            return None

        # Build rows
        rows = []
        for key, vote_dict in votes_by_village.items():
            dept, li = key.split('_')
            area = area_by_village.get(key, '00')
            dist_name = dist_map.get(dept, '')
            village_name = village_map.get(key, '')

            if not dist_name or not village_name:
                continue

            row_data = {
                '行政區別': dist_name,
                '鄰里': village_name,
                '_dept': dept,
                '_li': li,
                '_area': area,
            }

            # Get candidates for this row based on key type
            if is_president:
                candidates = cand_by_key.get('00', [])
            elif key_by_dept:
                # 鄉鎮市長/鄉鎮市民代表: 用dept查候選人
                candidates = cand_by_key.get(dept, [])
            else:
                # 縣市長/議員: 用area查候選人
                candidates = cand_by_key.get(area, [])

            # Add candidate columns
            for i in range(max_candidates):
                if i < len(candidates):
                    cand = candidates[i]
                    cand_no = cand['cand_no']
                    if is_president:
                        row_data[f'{prefix}{i+1}'] = cand['name']
                        row_data[f'{prefix}{i+1}_得票數'] = vote_dict.get(cand_no, 0)
                    else:
                        row_data[f'{prefix}候選人{i+1}＿候選人名稱'] = cand['name']
                        row_data[f'{prefix}候選人{i+1}＿黨籍'] = cand['party']
                        row_data[f'{prefix}候選人{i+1}_得票數'] = vote_dict.get(cand_no, 0)
                else:
                    if is_president:
                        row_data[f'{prefix}{i+1}'] = ''
                        row_data[f'{prefix}{i+1}_得票數'] = 0
                    else:
                        row_data[f'{prefix}候選人{i+1}＿候選人名稱'] = ''
                        row_data[f'{prefix}候選人{i+1}＿黨籍'] = ''
                        row_data[f'{prefix}候選人{i+1}_得票數'] = 0

            # Add statistics
            stats = self._get_stats(df_prof, dept, li)
            row_data[f'{prefix}有效票數A'] = stats.get('有效票數A', 0)
            row_data[f'{prefix}無效票數B'] = stats.get('無效票數B', 0)
            row_data[f'{prefix}投票數C'] = stats.get('投票數C', 0)
            row_data[f'{prefix}選舉人數G'] = stats.get('選舉人數G', 0)

            rows.append(row_data)

        result = pd.DataFrame(rows)
        print(f"    [OK] {folder_name}: {len(result)} rows, max {max_candidates} candidates")
        return result

    def process_party_list(self, max_parties=20):
        """處理不分區政黨"""
        folder_path = self.year_folder / "不分區政黨"
        if not folder_path.exists():
            return None

        elctks = find_file(folder_path, ['elctks.csv', 'elctks_T4.csv'])
        elcand = find_file(folder_path, ['elcand.csv', 'elcand_T4.csv'])
        elbase = find_file(folder_path, ['elbase.csv', 'elbase_T4.csv'])
        elprof = folder_path / "elprof.csv"

        if not elctks:
            return None

        df_tks = pd.read_csv(elctks, header=None, dtype=str)

        prv_str = str(self.prv_code).zfill(2)
        city_str = str(self.city_code).zfill(3) if self.city_code > 0 else '000'

        df_tks_f = df_tks[(df_tks[0] == prv_str) & (df_tks[1] == city_str)].copy()
        df_tks_f = df_tks_f[df_tks_f[4] != '0000']

        if df_tks_f.empty:
            return None

        # Get party names
        party_names = {}
        party_order = []
        if elcand:
            df_cand = pd.read_csv(elcand, header=None, dtype=str)
            for _, row in df_cand.iterrows():
                party_no = str(row[5]).strip()
                party_name = str(row[6]).strip()
                if party_no and party_name and party_no not in party_names:
                    party_names[party_no] = party_name
                    party_order.append(party_no)

        # Get village names
        village_map = {}
        dist_map = {}
        if elbase:
            df_base = pd.read_csv(elbase, header=None, dtype=str)
            df_base_f = df_base[(df_base[0] == prv_str) & (df_base[1] == city_str)]
            for _, row in df_base_f.iterrows():
                dept = str(row[3]).strip()
                li = str(row[4]).strip()
                name = str(row[5]).strip()
                if li == '0000':
                    dist_map[dept] = normalize_district(name)
                else:
                    village_map[f"{dept}_{li}"] = name

        # Get statistics
        stats_map = {}
        if elprof.exists():
            df_prof = pd.read_csv(elprof, header=None, dtype=str)
            df_prof_f = df_prof[(df_prof[0].astype(str) == prv_str) & (df_prof[1].astype(str) == city_str)]
            for _, row in df_prof_f.iterrows():
                dept = str(row[3]).strip()
                li = str(row[4]).strip()
                if li != '0000':
                    stats_map[f"{dept}_{li}"] = {
                        '有效票數A': int(float(row[5])) if pd.notna(row[5]) and row[5] != '' else 0,
                        '無效票數B': int(float(row[6])) if pd.notna(row[6]) and row[6] != '' else 0,
                        '投票數C': int(float(row[7])) if pd.notna(row[7]) and row[7] != '' else 0,
                        '選舉人數G': int(float(row[11])) if len(row) > 11 and pd.notna(row[11]) and row[11] != '' else 0,
                    }

        # Build votes by village
        votes_by_village = defaultdict(lambda: defaultdict(int))
        for _, row in df_tks_f.iterrows():
            dept = str(row[3]).strip()
            li = str(row[4]).strip()
            party_no = str(row[6]).strip()
            votes = int(float(row[7])) if row[7] != '' else 0
            key = f"{dept}_{li}"
            votes_by_village[key][party_no] = votes

        # Build rows
        rows = []
        for key, vote_dict in votes_by_village.items():
            dept, li = key.split('_')
            dist_name = dist_map.get(dept, '')
            village_name = village_map.get(key, '')

            if not village_name:
                continue

            row_data = {
                '行政區別': dist_name,
                '鄰里': village_name,
                '_dept': dept,
                '_li': li,
            }

            # Add party columns
            for i, party_no in enumerate(party_order[:max_parties], 1):
                party_name = party_names.get(party_no, f'政黨{party_no}')
                col_name = f'不分區政黨({i})\n\n{party_name}'
                row_data[col_name] = vote_dict.get(party_no, 0)

            # Add statistics
            stats = stats_map.get(key, {})
            row_data['不分區政黨有效票數A'] = stats.get('有效票數A', 0)
            row_data['不分區政黨無效票數B'] = stats.get('無效票數B', 0)
            row_data['不分區政黨投票數C'] = stats.get('投票數C', 0)
            row_data['不分區政黨選舉人數G'] = stats.get('選舉人數G', 0)

            rows.append(row_data)

        result = pd.DataFrame(rows)
        print(f"    [OK] 不分區政黨: {len(result)} rows, {len(party_order)} parties")
        return result


def merge_election_results(results, county):
    """合併選舉結果"""
    if not results:
        return None

    valid_results = [r for r in results if r is not None and not r.empty]
    if not valid_results:
        return None

    config = COUNTY_CONFIG[county]
    prv_code = config['prv_code']
    city_code = config['city_code']

    # Merge on 行政區別 + 鄰里
    merged = valid_results[0]
    for df in valid_results[1:]:
        # Get merge columns
        merge_cols = ['行政區別', '鄰里']
        if '_dept' in df.columns:
            merge_cols.extend(['_dept', '_li'])

        common_cols = [c for c in merge_cols if c in merged.columns and c in df.columns]

        # Drop _area if exists (can differ between election types)
        if '_area' in df.columns:
            df = df.drop(columns=['_area'])
        if '_area' in merged.columns:
            merged = merged.drop(columns=['_area'])

        merged = merged.merge(df, on=common_cols, how='outer', suffixes=('', '_dup'))
        merged = merged.loc[:, ~merged.columns.str.endswith('_dup')]

    # Add county column
    merged.insert(0, '縣市', county)

    # Add 區域別代碼 - format: {prv:02d}{city:03d}{dept:02d}{li:04d} = 11 digits
    # Raw dept values are 10, 20, 30... so divide by 10 to get 01, 02, 03...
    if '_dept' in merged.columns and '_li' in merged.columns:
        def build_region_code(row):
            dept_raw = int(row['_dept']) if pd.notna(row['_dept']) else 0
            dept = str(dept_raw // 10).zfill(2)  # 10→01, 20→02, etc.
            li = str(row['_li']).zfill(4)
            return f"{prv_code:02d}{city_code:03d}{dept}{li}"
        merged['區域別代碼'] = merged.apply(build_region_code, axis=1)
        merged = merged.drop(columns=['_dept', '_li'], errors='ignore')
    else:
        merged['區域別代碼'] = ''

    # Reorder columns
    base_cols = ['縣市', '行政區別', '鄰里', '區域別代碼']
    other_cols = [c for c in merged.columns if c not in base_cols and not c.startswith('_')]
    merged = merged[base_cols + other_cols]

    return merged


def process_local_election(county, year):
    """處理地方公職選舉"""
    print(f"\n處理 {county} {year} 地方公職選舉...")

    processor = ElectionProcessor(county, year)
    results = []

    if year == 2022:
        sub = 'prv' if processor.is_municipality else 'city'
        results.append(processor.process_election(f'C1/{sub}', '縣市長' if not processor.is_municipality else '直轄市長'))
        results.append(processor.process_election(f'T1/{sub}', '區域縣市議員' if not processor.is_municipality else '區域直轄市議員'))
        results.append(processor.process_election(f'T2/{sub}', '平原縣市議員' if not processor.is_municipality else '平原直轄市議員'))
        results.append(processor.process_election(f'T3/{sub}', '山原縣市議員' if not processor.is_municipality else '山原直轄市議員'))
        if not processor.is_municipality:
            # 鄉鎮市民代表用 key_by_dept=True (候選人以鄉鎮為單位)
            results.append(processor.process_election('R1', '鄉鎮市民代表', key_by_dept=True))
    else:
        if processor.is_municipality:
            results.append(processor.process_election('直轄市市長', '直轄市長'))
            results.append(processor.process_election('直轄市區域議員', '區域直轄市議員'))
            results.append(processor.process_election('直轄市平原議員', '平原直轄市議員'))
            results.append(processor.process_election('直轄市山原議員', '山原直轄市議員'))
        else:
            results.append(processor.process_election('縣市市長', '縣市長'))
            # 鄉鎮市長用 key_by_dept=True (候選人以鄉鎮為單位)
            results.append(processor.process_election('縣市鄉鎮市長', '鄉鎮市長', key_by_dept=True))
            results.append(processor.process_election('縣市區域議員', '區域縣市議員'))
            results.append(processor.process_election('縣市平原議員', '平原縣市議員'))
            results.append(processor.process_election('縣市山原議員', '山原縣市議員'))
            # 鄉鎮市民代表用 key_by_dept=True (候選人以鄉鎮為單位)
            results.append(processor.process_election('縣市鄉鎮市民代表', '鄉鎮市民代表', key_by_dept=True))

    return merge_election_results(results, county)


def process_presidential_election(county, year):
    """處理總統立委選舉"""
    print(f"\n處理 {county} {year} 總統立委選舉...")

    processor = ElectionProcessor(county, year)
    results = []

    results.append(processor.process_election('總統', '總統候選人', is_president=True))
    results.append(processor.process_election('區域立委', '區域立委'))
    results.append(processor.process_election('平地立委', '平原立委'))
    results.append(processor.process_election('山地立委', '山原立委'))
    results.append(processor.process_party_list())

    return merge_election_results(results, county)


def save_and_verify(df, county, year):
    """儲存並驗證"""
    if df is None or df.empty:
        return

    output_dir = OUTPUT_DIR / county
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{year}_選舉資料_{county}.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

    # Verify no duplicates
    dups = df[df.duplicated(subset=['行政區別', '鄰里'], keep=False)]
    status = "PASS" if dups.empty else f"FAIL ({len(dups)} dups)"
    print(f"  [SAVED] {output_file.name}: {len(df)} rows, {len(df.columns)} cols - {status}")


def combine_counties(counties, output_name):
    """合併多個縣市的資料"""
    print(f"\n合併縣市資料...")

    all_dfs = []
    for county in counties:
        county_dir = OUTPUT_DIR / county
        if not county_dir.exists():
            continue

        for f in sorted(county_dir.glob("*.csv")):
            df = pd.read_csv(f)
            year = f.stem.split('_')[0]
            df.insert(0, '年份', year)
            all_dfs.append(df)
            print(f"  載入: {f.name} ({len(df)} rows)")

    if not all_dfs:
        print("  [ERROR] 無資料可合併")
        return

    # Combine all
    combined = pd.concat(all_dfs, ignore_index=True, sort=False)

    # Fill NaN with empty string for text, 0 for numbers
    for col in combined.columns:
        if combined[col].dtype == 'object':
            combined[col] = combined[col].fillna('')
        else:
            combined[col] = combined[col].fillna(0)

    output_file = OUTPUT_DIR / output_name
    combined.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n  [COMBINED] {output_file}: {len(combined)} rows, {len(combined.columns)} cols")


def main():
    print("=" * 60)
    print("選舉資料處理系統 (2014-2024)")
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
                save_and_verify(df, county, year)

    # Combine all data
    combine_counties(counties, "花蓮縣_臺北市_合併.csv")

    print("\n" + "=" * 60)
    print("處理完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
