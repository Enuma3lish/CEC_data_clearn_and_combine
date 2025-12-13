"""
選舉資料處理器
Election data processor class
"""

import pandas as pd
from collections import defaultdict
from pathlib import Path

from .config import COUNTY_CONFIG, PARTY_CODE_MAP, YEAR_FOLDER_MAP, VOTE_DATA_DIR
from .utils import find_file, load_csv_safe, normalize_district, clean_string


class ElectionProcessor:
    """選舉資料處理器

    處理單一縣市、單一年份的選舉資料

    Attributes:
        county: 縣市名稱
        year: 選舉年份
        config: 縣市配置
    """

    def __init__(self, county: str, year: int):
        """初始化處理器

        Args:
            county: 縣市名稱（如 '花蓮縣', '臺北市'）
            year: 選舉年份（2014-2024）
        """
        self.county = county
        self.year = year
        self.config = COUNTY_CONFIG[county]
        self.prv_code = self.config['prv_code']
        self.city_code = self.config['city_code']
        self.is_municipality = self.config['is_municipality']
        self.year_folder = VOTE_DATA_DIR / YEAR_FOLDER_MAP.get(year, '')

    def _read_raw_data(self, folder_path: Path):
        """讀取原始資料

        Args:
            folder_path: 選舉類型資料夾路徑

        Returns:
            (df_tks, df_base, df_cand, df_prof) 四個 DataFrame
        """
        # 讀取票數
        elctks_file = find_file(folder_path, ['elctks.csv', 'elctks_T1.csv', 'elctks_P1.csv', 'elctks_T4.csv'])
        if not elctks_file:
            return None, None, None, None

        df_tks = pd.read_csv(elctks_file, header=None, dtype=str, quotechar='"')
        for col in df_tks.columns:
            df_tks[col] = df_tks[col].apply(clean_string)

        # 讀取基本資料
        elbase_file = find_file(folder_path, ['elbase.csv', 'elbase_T1.csv', 'elbase_P1.csv', 'elbase_T4.csv'])
        df_base = None
        if elbase_file:
            df_base = load_csv_safe(elbase_file, header=None, dtype=str, quotechar='"')
            if df_base is not None:
                for col in df_base.columns:
                    df_base[col] = df_base[col].apply(clean_string)

        # 讀取候選人
        elcand_file = find_file(folder_path, ['elcand.csv', 'elcand_T1.csv', 'elcand_P1.csv', 'elcand_T4.csv'])
        df_cand = None
        if elcand_file:
            df_cand = load_csv_safe(elcand_file, header=None, dtype=str, quotechar='"')
            if df_cand is not None:
                for col in df_cand.columns:
                    df_cand[col] = df_cand[col].apply(clean_string)

        # 讀取統計
        elprof_file = folder_path / "elprof.csv"
        df_prof = None
        if elprof_file.exists():
            df_prof = pd.read_csv(elprof_file, header=None, dtype=str, quotechar='"')

        return df_tks, df_base, df_cand, df_prof

    def _filter_county(self, df: pd.DataFrame) -> pd.DataFrame:
        """過濾縣市資料

        Args:
            df: 原始 DataFrame

        Returns:
            過濾後的 DataFrame
        """
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
            df_base: 基本資料 DataFrame
            df_cand: 候選人資料 DataFrame
            is_president: 是否為總統選舉
            key_by_dept: 是否以鄉鎮為 key（用於鄉鎮市長/鄉鎮市民代表）

        Returns:
            (dist_map, village_map, cand_by_key) 三個映射
        """
        # 行政區映射 (dept_code -> name)
        dist_map = {}
        village_map = {}

        if df_base is not None and not df_base.empty:
            df_base_f = self._filter_county(df_base)
            for _, row in df_base_f.iterrows():
                dept = clean_string(row[3])
                li = clean_string(row[4])
                name = normalize_district(clean_string(row[5]))
                if li == '0000':
                    dist_map[dept] = name
                else:
                    village_map[f"{dept}_{li}"] = name

        # 候選人映射 (key -> list of {cand_no, name, party})
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
                    cand_no = clean_string(row[5])
                    name = clean_string(row[6])
                    party_code = clean_string(row[7])
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
                    if key_by_dept:
                        key = clean_string(row[3])  # dept_code = township
                    else:
                        key = clean_string(row[2]).zfill(2)  # area_code = election district
                    cand_no = clean_string(row[5])
                    name = clean_string(row[6])
                    party_code = clean_string(row[7])
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

    def _get_stats(self, df_prof, dept_code: str, li_code: str) -> dict:
        """取得統計資料

        Args:
            df_prof: 統計資料 DataFrame
            dept_code: 鄉鎮代碼
            li_code: 村里代碼

        Returns:
            統計資料字典
        """
        if df_prof is None or df_prof.empty:
            return {}

        df_prof_f = self._filter_county(df_prof)
        if df_prof_f.empty:
            return {}

        for _, row in df_prof_f.iterrows():
            if clean_string(row[3]) == dept_code and clean_string(row[4]) == li_code:
                return {
                    '有效票數A': int(float(row[5])) if pd.notna(row[5]) and row[5] != '' else 0,
                    '無效票數B': int(float(row[6])) if pd.notna(row[6]) and row[6] != '' else 0,
                    '投票數C': int(float(row[7])) if pd.notna(row[7]) and row[7] != '' else 0,
                    '選舉人數G': int(float(row[11])) if len(row) > 11 and pd.notna(row[11]) and row[11] != '' else 0,
                }
        return {}

    def process_election(self, folder_name: str, prefix: str, is_president=False,
                         key_by_dept=False, max_candidates=10) -> pd.DataFrame:
        """處理單一選舉類型

        Args:
            folder_name: 選舉類型資料夾名稱
            prefix: 欄位前綴
            is_president: 是否為總統選舉
            key_by_dept: 是否以鄉鎮為 key
            max_candidates: 最大候選人數

        Returns:
            處理結果 DataFrame
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
        votes_by_village = defaultdict(lambda: defaultdict(int))
        area_by_village = {}
        dept_by_village = {}

        for _, row in df_tks_f.iterrows():
            dept = clean_string(row[3])
            li = clean_string(row[4])
            tbox_str = clean_string(row[5])
            tbox = int(float(tbox_str)) if tbox_str != '' else 0
            area = clean_string(row[2]).zfill(2)
            cand_no = clean_string(row[6])
            votes = int(float(row[7])) if row[7] != '' else 0

            if li == '0000':
                continue

            # Only use summary rows (tbox=0)
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

            # Get candidates
            if is_president:
                candidates = cand_by_key.get('00', [])
            elif key_by_dept:
                candidates = cand_by_key.get(dept, [])
            else:
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

    def process_party_list(self, max_parties=20) -> pd.DataFrame:
        """處理不分區政黨

        Args:
            max_parties: 最大政黨數

        Returns:
            處理結果 DataFrame
        """
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
