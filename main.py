#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
選舉資料處理系統 - 支援多職位、多年份處理
時間範圍：2014-2024
目前處理：花蓮縣
"""
import pandas as pd
import os
import sys
from pathlib import Path
from processors.format_converter import process_year_data

# ============================================================================
# 基本設定
# ============================================================================
BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "各縣市候選人分類"
OUTPUT_DIR = BASE_DIR / "鄰里整理輸出"
REPO_DIR = BASE_DIR / "voteData"

# 2022 年使用特殊路徑（可能是 Y: drive）
if os.path.exists(r"Y:\elctks.csv") or os.path.exists(r"Y:\T1"):
    REPO_2022_PATH = Path(r"Y:")
    print("Using Y: drive for 2022 data")
else:
    REPO_2022_PATH = REPO_DIR / "2022-111年地方公職人員選舉"

# ============================================================================
# 縣市配置 Map
# ============================================================================
COUNTY_CONFIG = {
    '花蓮縣': {
        'prv_code': 10,
        'city_code': 15,
        'is_municipality': False,  # 非直轄市
    },
    '臺北市': {
        'prv_code': 63,
        'city_code': 0,
        'is_municipality': True,  # 直轄市
    },
    # 其他縣市可在此擴充
}

# ============================================================================
# 選舉年份 Map - 定義每一年有哪些選舉類型
# ============================================================================
# 地方公職年份：2014, 2018, 2022
# 總統立委年份：2016, 2020, 2024

ELECTION_YEAR_MAP = {
    # === 2014 地方公職 ===
    2014: {
        'type': 'local',  # 地方公職選舉
        'folder_pattern': '2014-103年地方公職人員選舉',
        'elections': {
            # 非直轄市
            'county_councilor_regional': {
                'display_name': '非直轄市議員',
                'folder': '縣市區域議員',
                'output_file': '{year}_非直轄市議員.csv',
                'municipality_only': False,
            },
            'county_councilor_mountain': {
                'display_name': '山原議員',
                'folder': '縣市山原議員',
                'output_file': '{year}_山原議員.csv',
                'municipality_only': False,
            },
            'county_councilor_plain': {
                'display_name': '平原議員',
                'folder': '縣市平原議員',
                'output_file': '{year}_平原議員.csv',
                'municipality_only': False,
            },
            'county_mayor': {
                'display_name': '縣市長',
                'folder': '縣市市長',
                'output_file': '{year}_縣市長.csv',
                'municipality_only': False,
            },
            'township_representative': {
                'display_name': '鄉鎮市民代表',
                'folder': '縣市鄉鎮市民代表',
                'output_file': '{year}_鄉鎮市民代表.csv',
                'municipality_only': False,
            },
            # 直轄市
            'city_councilor_regional': {
                'display_name': '直轄市議員',
                'folder': '直轄市區域議員',
                'output_file': '{year}_直轄市議員.csv',
                'municipality_only': True,
            },
            'city_mayor': {
                'display_name': '直轄市長',
                'folder': '直轄市市長',
                'output_file': '{year}_直轄市長.csv',
                'municipality_only': True,
            },
        }
    },

    # === 2016 總統立委 ===
    2016: {
        'type': 'presidential',  # 總統立委選舉
        'folder_pattern': '2016總統立委',
        'elections': {
            'president': {
                'display_name': '總統',
                'folder': '總統',
                'output_file': '{year}_總統.csv',
                'municipality_only': None,  # 全國性選舉
            },
            'legislator': {
                'display_name': '立法委員',
                'folders': ['區域立委', '平地立委', '山地立委'],  # 合併三種立委
                'output_file': '{year}_立法委員.csv',
                'municipality_only': None,
            },
            'party_list': {
                'display_name': '不分區政黨',
                'folder': '不分區政黨',
                'output_file': '{year}_不分區政黨.csv',
                'municipality_only': None,
            },
        }
    },

    # === 2018 地方公職 ===
    2018: {
        'type': 'local',
        'folder_pattern': '2018-107年地方公職人員選舉',
        'elections': {
            'county_councilor_regional': {
                'display_name': '非直轄市議員',
                'folder': '縣市區域議員',
                'output_file': '{year}_非直轄市議員.csv',
                'municipality_only': False,
            },
            'county_councilor_mountain': {
                'display_name': '山原議員',
                'folder': '縣市山原議員',
                'output_file': '{year}_山原議員.csv',
                'municipality_only': False,
            },
            'county_councilor_plain': {
                'display_name': '平原議員',
                'folder': '縣市平原議員',
                'output_file': '{year}_平原議員.csv',
                'municipality_only': False,
            },
            'county_mayor': {
                'display_name': '縣市長',
                'folder': '縣市市長',
                'output_file': '{year}_縣市長.csv',
                'municipality_only': False,
            },
            'township_representative': {
                'display_name': '鄉鎮市民代表',
                'folder': '縣市鄉鎮市民代表',
                'output_file': '{year}_鄉鎮市民代表.csv',
                'municipality_only': False,
            },
            'city_councilor_regional': {
                'display_name': '直轄市議員',
                'folder': '直轄市區域議員',
                'output_file': '{year}_直轄市議員.csv',
                'municipality_only': True,
            },
            'city_mayor': {
                'display_name': '直轄市長',
                'folder': '直轄市市長',
                'output_file': '{year}_直轄市長.csv',
                'municipality_only': True,
            },
        }
    },

    # === 2020 總統立委 ===
    2020: {
        'type': 'presidential',
        'folder_pattern': '2020總統立委',
        'elections': {
            'president': {
                'display_name': '總統',
                'folder': '總統',
                'output_file': '{year}_總統.csv',
                'municipality_only': None,
            },
            'legislator': {
                'display_name': '立法委員',
                'folders': ['區域立委', '平地立委', '山地立委'],
                'output_file': '{year}_立法委員.csv',
                'municipality_only': None,
            },
            'party_list': {
                'display_name': '不分區政黨',
                'folder': '不分區政黨',
                'output_file': '{year}_不分區政黨.csv',
                'municipality_only': None,
            },
        }
    },

    # === 2022 地方公職 (使用新的代碼系統) ===
    2022: {
        'type': 'local_2022',  # 特殊處理 2022 年
        'folder_pattern': '2022-111年地方公職人員選舉',
        'elections': {
            'county_councilor_regional': {
                'display_name': '非直轄市議員',
                'folder': 'T1',  # 2022 使用代碼
                'sub_folder': 'city',  # 非直轄市使用 city 子資料夾
                'output_file': '{year}_非直轄市議員.csv',
                'municipality_only': False,
            },
            'county_councilor_mountain': {
                'display_name': '山原議員',
                'folder': 'T3',
                'sub_folder': 'city',
                'output_file': '{year}_山原議員.csv',
                'municipality_only': False,
            },
            'county_councilor_plain': {
                'display_name': '平原議員',
                'folder': 'T2',
                'sub_folder': 'city',
                'output_file': '{year}_平原議員.csv',
                'municipality_only': False,
            },
            'county_mayor': {
                'display_name': '縣市長',
                'folder': 'C1',
                'sub_folder': 'city',
                'output_file': '{year}_縣市長.csv',
                'municipality_only': False,
            },
            'township_representative': {
                'display_name': '鄉鎮市民代表',
                'folder': 'R1',
                'sub_folder': None,  # R1 沒有子資料夾
                'output_file': '{year}_鄉鎮市民代表.csv',
                'municipality_only': False,
            },
            'city_councilor_regional': {
                'display_name': '直轄市議員',
                'folder': 'T1',
                'sub_folder': 'prv',  # 直轄市使用 prv 子資料夾
                'output_file': '{year}_直轄市議員.csv',
                'municipality_only': True,
            },
            'city_mayor': {
                'display_name': '直轄市長',
                'folder': 'C1',
                'sub_folder': 'prv',
                'output_file': '{year}_直轄市長.csv',
                'municipality_only': True,
            },
        }
    },

    # === 2024 總統立委 ===
    2024: {
        'type': 'presidential',
        'folder_pattern': '2024總統立委',
        'elections': {
            'president': {
                'display_name': '總統',
                'folder': '總統',
                'output_file': '{year}_總統.csv',
                'municipality_only': None,
            },
            'legislator': {
                'display_name': '立法委員',
                'folders': ['區域立委', '平地立委', '山地立委'],
                'output_file': '{year}_立法委員.csv',
                'municipality_only': None,
            },
            'party_list': {
                'display_name': '不分區政黨',
                'folder': '不分區政黨',
                'output_file': '{year}_不分區政黨.csv',
                'municipality_only': None,
            },
        }
    },
}


# ============================================================================
# 資料修復函數
# ============================================================================

def _process_repair(source_csv, target_path, prv_code, city_code, county, year):
    """
    通用資料修復函數 - 從原始 CSV 處理並輸出到目標路徑
    """
    try:
        col_names = ['prv_code', 'city_code', 'area_code', 'dept_code', 'li_code',
                     'tks', 'cand_no', 'ticket_num', 'ratio', 'elected']
        df_tks = pd.read_csv(source_csv, names=col_names, quotechar='"', quoting=0,
                             header=None, dtype={'area_code': str, 'dept_code': str,
                                                  'li_code': str, 'cand_no': str})

        for col in df_tks.columns:
            if df_tks[col].dtype == object:
                df_tks[col] = df_tks[col].astype(str).str.replace("'", "").str.replace('"', "")

        # 根據 city_code 過濾資料
        if city_code == -1:
            df_target = df_tks[
                pd.to_numeric(df_tks['prv_code'], errors='coerce') == int(prv_code)
            ].copy()
        else:
            df_target = df_tks[
                (pd.to_numeric(df_tks['prv_code'], errors='coerce') == int(prv_code)) &
                (pd.to_numeric(df_tks['city_code'], errors='coerce') == int(city_code))
            ].copy()

        print(f"     [DEBUG] Filtered {len(df_target)} records for prv={prv_code}, city={city_code}")

        if df_target.empty:
            print("     [WARN] No records found.")
            return

        # 讀取元資料
        meta_dir = source_csv.parent
        elbase = _find_file(meta_dir, ['elbase.csv', 'elbase_T1.csv', 'elbase_P1.csv', 'elbese.csv'])
        elcand = _find_file(meta_dir, ['elcand.csv', 'elcand_T1.csv', 'elcand_P1.csv'])

        if not elbase or not elcand:
            print("     [ERROR] Metadata files missing.")
            return

        # 嘗試多種編碼
        df_base, df_cand = _load_metadata(elbase, elcand, prv_code, city_code, source_csv)
        if df_base is None or df_cand is None:
            print("     [WARN] Failed to decode metadata.")
            return

        print(f"     Found {len(df_cand)} candidates")

        # 建立映射
        dist_map, village_map, cand_name_map, cand_party_map = _build_maps(
            df_base, df_cand, source_csv.parent.name
        )

        # 轉換資料
        df_target['ticket_num'] = pd.to_numeric(df_target['ticket_num'], errors='coerce').fillna(0).astype(int)

        for col in ['dept_code', 'li_code', 'area_code', 'cand_no']:
            if col in df_target.columns:
                df_target[col] = df_target[col].astype(str)

        df_target['行政區別'] = df_target['dept_code'].map(dist_map)
        df_target['village_key'] = df_target['dept_code'] + "_" + df_target['li_code']
        df_target['村里別'] = df_target['village_key'].map(village_map)

        # 過濾無效資料
        df_target = df_target.dropna(subset=['行政區別', '村里別'])
        df_target = df_target[df_target['村里別'] != '']
        df_target = df_target[df_target['行政區別'] != '總計']
        df_target = df_target[df_target['村里別'] != '總計']

        # 建立候選人查詢鍵
        df_target['area_code_norm'] = df_target['area_code'].str.zfill(2)
        df_target['cand_no_norm'] = df_target['cand_no'].str.zfill(2)
        df_target['cand_lookup_key'] = df_target['area_code_norm'] + "_" + df_target['cand_no_norm']
        df_target['cand_name'] = df_target['cand_lookup_key'].map(cand_name_map)
        df_target['cand_party'] = df_target['cand_lookup_key'].map(cand_party_map)

        df_target['cand_name'] = df_target['cand_name'].fillna(df_target['cand_no'])
        df_target['cand_party'] = df_target['cand_party'].fillna('')

        # 建立樞紐表
        pivot = df_target.pivot_table(index=['行政區別', '村里別'], columns='cand_name',
                                       values='ticket_num', aggfunc='sum')
        pivot = pivot.reset_index()

        # 加入政黨欄位
        name_party_map = df_target.groupby('cand_name')['cand_party'].first().to_dict()
        for cand_name in pivot.columns:
            if cand_name not in ['行政區別', '村里別']:
                party_name = name_party_map.get(cand_name, '')
                pivot[f"{cand_name}_黨籍"] = party_name

        # 合併 elprof 統計資料
        pivot = _merge_elprof_stats(pivot, meta_dir, prv_code, city_code, dist_map, village_map)

        # 確保數值欄位為整數
        stat_keywords = ['有效票數', '無效票數', '投票數', '選舉人數', '已領未投', '發出票數', '用餘票數', '投票率']
        for col in pivot.columns:
            if col not in ['行政區別', '村里別']:
                if any(kw in col for kw in stat_keywords):
                    pivot[col] = pd.to_numeric(pivot[col], errors='coerce').fillna(0).astype(int)

        # 儲存結果
        target_path.parent.mkdir(parents=True, exist_ok=True)
        pivot.to_csv(target_path, index=False, encoding='utf-8-sig')
        print(f"     [OK] Saved: {target_path.name}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"     [ERROR] Error: {e}")


def _find_file(directory, candidates):
    """尋找存在的檔案"""
    for name in candidates:
        path = directory / name
        if path.exists():
            return path
    return None


def _load_metadata(elbase_path, elcand_path, prv_code, city_code, source_csv):
    """載入元資料檔案"""
    encodings = ['utf-8', 'cp950', 'big5', 'gb2312', 'gbk']
    df_base = None
    df_cand = None

    for encoding in encodings:
        try:
            df_base = pd.read_csv(elbase_path,
                                  names=['prv_code', 'city_code', 'area_code', 'dept_code', 'li_code', 'name'],
                                  quotechar='"', quoting=1, header=None, encoding=encoding,
                                  dtype={'li_code': str, 'dept_code': str})
            df_cand = pd.read_csv(elcand_path, encoding=encoding, header=None,
                                  quotechar='"', quoting=1,
                                  dtype={'li_code': str, 'area_code': str, 'dept_code': str, 'cand_no': str})

            # 設定欄位名稱
            expected_cols = ['prv_code', 'city_code', 'area_code', 'dept_code', 'li_code',
                            'cand_no', 'name', 'party', 'gender', 'birth', 'age',
                            'address', 'education', 'elected', 'incumbent']
            actual_cols = expected_cols + [f'extra{i}' for i in range(len(df_cand.columns) - len(expected_cols))]
            df_cand.columns = actual_cols[:len(df_cand.columns)]

            # 清理字串
            for df in [df_base, df_cand]:
                for col in df.columns:
                    if df[col].dtype == object:
                        df[col] = df[col].astype(str).str.replace("'", "").str.replace('"', "").str.strip()

            # 檢查是否有有效名稱
            if len(df_cand) > 0 and not df_cand['name'].str.contains('\ufffd', na=False).any():
                print(f"     Using encoding: {encoding}")
                break
        except Exception:
            continue

    if df_base is None or df_cand is None:
        return None, None

    # 根據選舉類型過濾
    is_president = source_csv.parent.name == "總統"
    is_indigenous_leg = "立委" in source_csv.parent.name and source_csv.parent.name != "區域立委"

    if is_president:
        df_base = df_base[(pd.to_numeric(df_base['prv_code'], errors='coerce') == int(prv_code)) &
                          (pd.to_numeric(df_base['city_code'], errors='coerce') == int(city_code))]
        df_cand = df_cand[(pd.to_numeric(df_cand['prv_code'], errors='coerce') == 0) &
                          (pd.to_numeric(df_cand['city_code'], errors='coerce') == 0)]
    elif is_indigenous_leg:
        df_base = df_base[(pd.to_numeric(df_base['prv_code'], errors='coerce') == int(prv_code)) &
                          (pd.to_numeric(df_base['city_code'], errors='coerce') == int(city_code))]
        df_cand = df_cand[(pd.to_numeric(df_cand['prv_code'], errors='coerce') == 0) &
                          (pd.to_numeric(df_cand['city_code'], errors='coerce') == 0)]
    elif city_code == -1:
        df_base = df_base[pd.to_numeric(df_base['prv_code'], errors='coerce') == int(prv_code)]
        df_cand = df_cand[pd.to_numeric(df_cand['prv_code'], errors='coerce') == int(prv_code)]
    else:
        df_base = df_base[(pd.to_numeric(df_base['prv_code'], errors='coerce') == int(prv_code)) &
                          (pd.to_numeric(df_base['city_code'], errors='coerce') == int(city_code))]
        df_cand = df_cand[(pd.to_numeric(df_cand['prv_code'], errors='coerce') == int(prv_code)) &
                          (pd.to_numeric(df_cand['city_code'], errors='coerce') == int(city_code))]

    # 轉換代碼欄位為字串 (使用 .copy() 避免 SettingWithCopyWarning)
    df_base = df_base.copy()
    df_cand = df_cand.copy()

    for col in ['prv_code', 'city_code', 'area_code', 'dept_code', 'li_code']:
        if col in df_base.columns:
            df_base[col] = df_base[col].astype(str)
        if col in df_cand.columns:
            df_cand[col] = df_cand[col].astype(str)

    if 'cand_no' in df_cand.columns:
        df_cand['cand_no'] = df_cand['cand_no'].astype(str)

    return df_base, df_cand


def _build_maps(df_base, df_cand, folder_name):
    """建立名稱映射"""
    import re

    # 行政區映射
    dist_map = df_base[df_base['li_code'] == '0000'].set_index('dept_code')['name'].to_dict()
    # 移除選舉區標記
    dist_map = {k: re.sub(r'第\d+選舉區', '', v) for k, v in dist_map.items()}
    # 移除縣市前綴 (如 "臺北市松山區" -> "松山區")
    dist_map = {k: re.sub(r'^(臺北市|新北市|桃園市|臺中市|臺南市|高雄市|基隆市|新竹市|嘉義市|'
                          r'宜蘭縣|新竹縣|苗栗縣|彰化縣|南投縣|雲林縣|嘉義縣|屏東縣|'
                          r'臺東縣|花蓮縣|澎湖縣|金門縣|連江縣)', '', v)
                for k, v in dist_map.items()}

    # 村里映射
    df_base['key'] = df_base['dept_code'] + "_" + df_base['li_code']
    village_map = df_base[df_base['li_code'] != '0000'].set_index('key')['name'].to_dict()

    # 政黨代碼映射
    party_code_map = {
        '1': '中國國民黨', '2': '民主進步黨', '3': '親民黨', '4': '台灣團結聯盟',
        '5': '無黨團結聯盟', '6': '綠黨', '7': '新黨', '8': '台灣基進',
        '9': '台灣民眾黨', '10': '時代力量', '11': '一邊一國行動黨',
        '12': '勞動黨', '13': '中華統一促進黨', '14': '國會政黨聯盟',
        '15': '台澎黨', '16': '民主進步黨', '17': '社會民主黨',
        '18': '和平鴿聯盟黨', '19': '喜樂島聯盟', '20': '安定力量',
        '21': '合一行動聯盟', '90': '親民黨', '99': '無黨籍及未經政黨推薦',
        '999': '無黨籍及未經政黨推薦', '348': '喜樂島聯盟',
    }

    # 候選人映射
    df_cand['area_code_norm'] = df_cand['area_code'].str.zfill(2)
    df_cand['cand_no_norm'] = df_cand['cand_no'].str.zfill(2)
    df_cand['map_key'] = df_cand['area_code_norm'] + "_" + df_cand['cand_no_norm']

    # 特殊處理總統選舉（合併正副總統名稱）
    is_president = folder_name == "總統"
    if is_president:
        combined_names = {}
        for cand_no in df_cand['cand_no'].unique():
            cand_group = df_cand[df_cand['cand_no'] == cand_no]
            if len(cand_group) == 2:
                names = cand_group['name'].tolist()
                combined_name = f"{names[0]}/{names[1]}"
                map_key = cand_group.iloc[0]['map_key']
                combined_names[map_key] = combined_name
            elif len(cand_group) == 1:
                map_key = cand_group.iloc[0]['map_key']
                combined_names[map_key] = cand_group.iloc[0]['name']
        cand_name_map = combined_names
    else:
        cand_name_map = df_cand.set_index('map_key')['name'].to_dict()

    # 政黨映射
    df_cand['party_name'] = df_cand['party'].astype(str).map(party_code_map).fillna('無黨籍及未經政黨推薦')
    cand_party_map = df_cand.set_index('map_key')['party_name'].to_dict()

    return dist_map, village_map, cand_name_map, cand_party_map


def _merge_elprof_stats(pivot, meta_dir, prv_code, city_code, dist_map, village_map):
    """合併 elprof 統計資料"""
    elprof = meta_dir / "elprof.csv"
    if not elprof.exists():
        return pivot

    try:
        df_prof = pd.read_csv(elprof, header=None, quotechar='"', quoting=0,
                              encoding='utf-8', dtype=str)
        df_prof = df_prof[(pd.to_numeric(df_prof[0], errors='coerce') == int(prv_code)) &
                          (pd.to_numeric(df_prof[1], errors='coerce') == int(city_code))]

        if df_prof.empty:
            return pivot

        df_prof['dept_code'] = df_prof[3].astype(str)
        df_prof['li_code'] = df_prof[4].astype(str)
        df_prof['village_key'] = df_prof['dept_code'] + "_" + df_prof['li_code']

        df_prof['行政區別'] = df_prof['dept_code'].map(dist_map)
        df_prof['村里別'] = df_prof['village_key'].map(village_map)

        stat_map = {5: '有效票數A', 6: '無效票數B', 7: '投票數C', 8: '已領未投票數D',
                    9: '發出票數E', 10: '用餘票數F', 11: '選舉人數G', 12: '投票率H'}
        keep_cols = ['行政區別', '村里別']

        for idx, name in stat_map.items():
            if idx < len(df_prof.columns):
                df_prof[name] = pd.to_numeric(df_prof[idx], errors='coerce').fillna(0).astype(int)
                keep_cols.append(name)

        df_stats = df_prof[keep_cols].copy()
        df_stats = df_stats.drop_duplicates(subset=['行政區別', '村里別'])

        pivot = pd.merge(pivot, df_stats, on=['行政區別', '村里別'], how='left')

    except Exception as e:
        print(f"     [WARN] Failed to merge elprof: {e}")

    return pivot


# ============================================================================
# 主要處理函數
# ============================================================================

def repair_data_generic(county, year, target_filename, repo_subfolder, prv_code, city_code):
    """通用資料修復函數（適用於 2014/2018 年）"""
    target_path = RAW_DIR / county / target_filename
    if target_path.exists() and os.path.getsize(target_path) > 1000:
        print(f"  [OK] {county} {year} {target_filename} valid. Skip.")
        return

    # 找到來源資料夾
    match_str = str(year) + "-"
    found = [x for x in REPO_DIR.iterdir() if x.name.startswith(match_str) and "地方" in x.name]
    if not found:
        print(f"  [ERROR] Year folder {year} not found.")
        return

    year_folder = found[0]
    source_csv = year_folder / repo_subfolder / "elctks.csv"

    if not source_csv.exists():
        print(f"  [ERROR] Source {source_csv} not found.")
        return

    print(f"  Processing {county} {year} {target_filename}...")
    _process_repair(source_csv, target_path, prv_code, city_code, county, year)


def repair_2022_scan(county, target_filename, prv_code, city_code):
    """2022 年資料修復（掃描式）"""
    target_path = RAW_DIR / county / target_filename
    if target_path.exists() and os.path.getsize(target_path) > 1000:
        print(f"  [OK] {county} 2022 {target_filename} valid. Skip.")
        return

    if not REPO_2022_PATH.exists():
        print("  [ERROR] 2022 Repo Path not found!")
        return

    found_source = None
    is_councilor = "議員" in target_filename
    is_rep = "代表" in target_filename

    # 判斷議員類型
    councilor_type_folder = None
    if is_councilor:
        if "山原議員" in target_filename:
            councilor_type_folder = "T3"
        elif "平原議員" in target_filename:
            councilor_type_folder = "T2"
        elif "區域議員" in target_filename or "直轄市議員" in target_filename or "非直轄市議員" in target_filename:
            councilor_type_folder = "T1"

    print(f"  [SCAN] Scanning 2022 folder for {county} (Prv={prv_code})...")

    for item in REPO_2022_PATH.iterdir():
        if item.is_dir():
            fname = item.name.upper()

            if is_rep:
                if not fname.startswith("R"):
                    continue
            elif is_councilor:
                if not fname.startswith("T"):
                    continue
                if councilor_type_folder and fname != councilor_type_folder:
                    continue
            else:
                if not (fname.startswith("C") or fname.startswith("D")):
                    continue

            candidates = [
                item / "elctks.csv",
                item / "prv" / "elctks.csv",
                item / "city" / "elctks.csv"
            ]

            for csv_path in candidates:
                if not csv_path.exists():
                    continue

                try:
                    df_chk = pd.read_csv(csv_path, names=['prv', 'city', 'a', 'd', 'l', 't', 'c', 'n', 'r', 'e'],
                                        nrows=3000, header=None, quotechar='"', quoting=0)

                    if df_chk.empty:
                        continue

                    if df_chk['prv'].dtype == object:
                        df_chk['prv'] = df_chk['prv'].astype(str).str.replace("'", "").str.replace('"', '')

                    if (pd.to_numeric(df_chk['prv'], errors='coerce') == int(prv_code)).any():
                        found_source = csv_path
                        break
                except Exception:
                    continue

            if found_source:
                break

    if not found_source:
        print(f"  [ERROR] No source for {county} 2022 {target_filename} found.")
        return

    print(f"  Found source: {found_source.parent.name}")
    _process_repair(found_source, target_path, prv_code, city_code, county, 2022)


def repair_president_data(county, year, target_filename, prv_code, city_code):
    """總統資料修復"""
    target_path = RAW_DIR / county / target_filename
    if target_path.exists() and os.path.getsize(target_path) > 1000:
        print(f"  [OK] {county} {year} President valid. Skip.")
        return

    match_str = str(year)
    found = [x for x in REPO_DIR.iterdir() if x.name.startswith(match_str) and "總統" in x.name]
    if not found:
        print(f"  [ERROR] Year folder {year} not found.")
        return

    year_folder = found[0]
    source_csv = _find_file(year_folder / "總統", ['elctks.csv', 'elctks_P1.csv', 'elctks_T1.csv'])

    if not source_csv:
        print(f"  [ERROR] Source elctks not found in {year_folder / '總統'}.")
        return

    print(f"  Processing President {year} for {county}...")
    _process_repair(source_csv, target_path, prv_code, city_code, county, year)


def repair_legislator_data(county, year, target_filename, prv_code, city_code):
    """立委資料修復（合併區域/平地/山地立委）"""
    target_path = RAW_DIR / county / target_filename

    print(f"  Processing Legislator {year} for {county}...")

    match_str = str(year)
    found = [x for x in REPO_DIR.iterdir() if x.name.startswith(match_str) and "總統" in x.name]
    if not found:
        print(f"  [ERROR] Year folder {year} not found.")
        return

    year_folder = found[0]
    subfolders = ['區域立委', '平地立委', '山地立委']
    dfs = []

    for sub in subfolders:
        source_csv = _find_file(year_folder / sub, ['elctks.csv', 'elctks_T1.csv'])
        if not source_csv:
            continue

        temp_path = RAW_DIR / county / f"temp_{year}_{sub}.csv"
        _process_repair(source_csv, temp_path, prv_code, city_code, county, year)

        if temp_path.exists():
            try:
                df = pd.read_csv(temp_path, encoding='utf-8-sig')
                keys = ['行政區別', '村里別']
                stat_keywords = ['有效票數', '無效票數', '投票數', '選舉人數', '已領未投', '發出票數', '用餘票數', '投票率']
                stat_cols = [c for c in df.columns if any(kw in c for kw in stat_keywords)]
                candidate_cols = [c for c in df.columns if c not in keys and c not in stat_cols]

                df_clean = df[keys + candidate_cols].copy()
                print(f"     Loaded {temp_path.name}: {len(df_clean)} rows")
                dfs.append(df_clean)
                os.remove(temp_path)
            except Exception as e:
                print(f"  [WARN] Failed to load {temp_path}: {e}")

    if not dfs:
        print("  [WARN] No legislator data found.")
        return

    # 合併資料
    keys = ['行政區別', '村里別']
    merged = dfs[0].copy()

    for i in range(1, len(dfs)):
        df_next = dfs[i]
        merged = pd.merge(merged, df_next, on=keys, how='outer', suffixes=('', '_dup'))
        dup_cols = [c for c in merged.columns if c.endswith('_dup')]
        merged = merged.drop(columns=dup_cols)

    for col in merged.columns:
        if col not in keys and '黨籍' not in col:
            merged[col] = pd.to_numeric(merged[col], errors='coerce').fillna(0).astype(int)

    # 轉換為標準格式
    stat_cols = ['有效票數A', '無效票數B', '投票數C', '選舉人數G', '已領未投票數D', '發出票數E', '用餘票數F', '投票率H']
    candidate_name_cols = [c for c in merged.columns if c not in keys and c not in stat_cols and not c.endswith('_黨籍')]

    result = merged[keys].copy()

    for i, cand_name in enumerate(candidate_name_cols, 1):
        party_col = f"{cand_name}_黨籍"
        result[f'候選人{i}＿候選人名稱'] = cand_name
        result[f'候選人{i}＿黨籍'] = merged[party_col] if party_col in merged.columns else ''
        result[f'候選人{i}_得票數'] = merged[cand_name]

    for stat_col in stat_cols:
        if stat_col in merged.columns:
            result[stat_col] = merged[stat_col]

    target_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(target_path, index=False, encoding='utf-8-sig')
    print(f"  [OK] Legislator Combined Saved: {target_path.name}")


def repair_party_list_data(county, year, target_filename, prv_code, city_code):
    """不分區政黨資料修復"""
    print(f"  [INFO] Processing Party-List {year} for {county}...")

    match_str = str(year)
    found = [x for x in REPO_DIR.iterdir() if x.name.startswith(match_str) and "總統" in x.name]
    if not found:
        print(f"  [WARN] Year folder {year} not found.")
        return

    year_folder = found[0] / "不分區政黨"
    if not year_folder.exists():
        print(f"  [WARN] Party-list folder not found: {year_folder}")
        return

    # 嘗試不同的檔案名稱格式 (2016 使用 _T4 後綴)
    elctks_path = _find_file(year_folder, ['elctks.csv', 'elctks_T4.csv'])
    if not elctks_path:
        print(f"  [WARN] Party-list elctks not found in {year_folder}")
        return

    target_path = RAW_DIR / county / target_filename

    legislator_file = RAW_DIR / county / f"{year}_立法委員.csv"
    if not legislator_file.exists():
        print(f"  [WARN] Legislator file not found: {legislator_file}")
        return

    try:
        df_leg = pd.read_csv(legislator_file, encoding='utf-8-sig')
        result = df_leg[['行政區別', '村里別']].drop_duplicates().copy()
        result.insert(0, '縣市', county)

        df_ctks = pd.read_csv(elctks_path, encoding='utf-8', dtype=str, header=None, keep_default_na=False)

        prv_str = str(prv_code).zfill(2)
        city_str = '000' if city_code == 0 else str(city_code).zfill(3)
        df_filtered = df_ctks[(df_ctks.iloc[:, 0] == prv_str) & (df_ctks.iloc[:, 1] == city_str)].copy()
        df_filtered = df_filtered[df_filtered.iloc[:, 4] != '0000'].copy()

        # 讀取政黨名稱 (嘗試不同的檔案名稱格式)
        elcand_path = _find_file(year_folder, ['elcand.csv', 'elcand_T4.csv'])
        party_map = {}
        if elcand_path and elcand_path.exists():
            df_cand = pd.read_csv(elcand_path, encoding='utf-8', dtype=str, header=None, keep_default_na=False)
            for _, row in df_cand.iterrows():
                party_no = row.iloc[5].strip()
                party_name = row.iloc[6].strip()
                if party_no and party_name:
                    party_map[party_no] = party_name
            print(f"     Found {len(party_map)} parties")

        # 讀取村里名稱 (嘗試不同的檔案名稱格式)
        elbase_path = _find_file(year_folder, ['elbase.csv', 'elbase_T4.csv'])
        village_map = {}
        if elbase_path and elbase_path.exists():
            df_base = pd.read_csv(elbase_path, encoding='utf-8', dtype=str, keep_default_na=False)
            df_base_f = df_base[(df_base.iloc[:, 0] == prv_str) & (df_base.iloc[:, 1] == city_str)].copy()
            for _, row in df_base_f.iterrows():
                dept = row.iloc[3].strip()
                li = row.iloc[4].strip()
                name = row.iloc[5].strip()
                if li != '0000':
                    village_map[(dept, li)] = name

        df_filtered['dept'] = df_filtered.iloc[:, 3]
        df_filtered['li'] = df_filtered.iloc[:, 4]
        df_filtered['party_no'] = df_filtered.iloc[:, 6]
        df_filtered['votes'] = pd.to_numeric(df_filtered.iloc[:, 7], errors='coerce').fillna(0).astype(int)

        df_grouped = df_filtered.groupby(['dept', 'li', 'party_no'])['votes'].sum().reset_index()
        df_grouped['村里別'] = df_grouped.apply(lambda r: village_map.get((r['dept'], r['li']), ''), axis=1)

        df_pivot = df_grouped.pivot_table(
            index='村里別', columns='party_no', values='votes', fill_value=0, aggfunc='sum'
        ).reset_index()

        df_pivot.columns = [
            party_map.get(str(col), f'政黨{col}') if col != '村里別' else col
            for col in df_pivot.columns
        ]

        result = result.merge(df_pivot, on='村里別', how='left')

        party_cols = [c for c in result.columns if c not in ['縣市', '行政區別', '村里別']]
        for col in party_cols:
            result[col] = result[col].fillna(0).astype(int)

        if party_cols:
            result['不分區政黨有效票數A'] = result[party_cols].sum(axis=1).astype(int)

        target_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(target_path, index=False, encoding='utf-8-sig')
        print(f"  [OK] Party-List Saved: {target_path.name} ({len(result)} rows)")

    except Exception as e:
        import traceback
        print(f"  [ERROR] Failed to process party-list: {e}")
        traceback.print_exc()


# ============================================================================
# 批次處理函數
# ============================================================================

def process_county_year(county: str, year: int):
    """
    處理指定縣市和年份的所有選舉資料
    """
    if county not in COUNTY_CONFIG:
        print(f"[ERROR] County {county} not configured.")
        return

    if year not in ELECTION_YEAR_MAP:
        print(f"[ERROR] Year {year} not configured.")
        return

    config = COUNTY_CONFIG[county]
    year_config = ELECTION_YEAR_MAP[year]

    prv_code = config['prv_code']
    city_code = config['city_code']
    is_municipality = config['is_municipality']

    print(f"\n{'='*60}")
    print(f"Processing {county} {year} ({year_config['type']})")
    print(f"{'='*60}")

    election_type = year_config['type']
    elections = year_config['elections']

    for election_key, election_config in elections.items():
        # 檢查是否適用此縣市
        municipality_only = election_config.get('municipality_only')
        if municipality_only is True and not is_municipality:
            continue
        if municipality_only is False and is_municipality:
            continue

        output_file = election_config['output_file'].format(year=year)
        display_name = election_config['display_name']

        print(f"\n  [{display_name}]")

        if election_type == 'local':
            # 地方公職選舉 (2014, 2018)
            folder = election_config['folder']
            repair_data_generic(county, year, output_file, folder, prv_code, city_code)

        elif election_type == 'local_2022':
            # 2022 地方公職選舉
            repair_2022_scan(county, output_file, prv_code, city_code)

        elif election_type == 'presidential':
            # 總統立委選舉
            if election_key == 'president':
                repair_president_data(county, year, output_file, prv_code, city_code)
            elif election_key == 'legislator':
                repair_legislator_data(county, year, output_file, prv_code, city_code)
            elif election_key == 'party_list':
                repair_party_list_data(county, year, output_file, prv_code, city_code)


def process_all_years_for_county(county: str, years: list = None):
    """
    處理指定縣市的所有年份
    """
    if years is None:
        years = sorted(ELECTION_YEAR_MAP.keys())

    for year in years:
        process_county_year(county, year)


# ============================================================================
# 主程式
# ============================================================================

def main():
    print("=" * 60)
    print("選舉資料處理系統 (2014-2024)")
    print("=" * 60)

    # 處理花蓮縣和臺北市
    counties_to_process = ['花蓮縣', '臺北市']
    years_to_process = [2014, 2016, 2018, 2020, 2022, 2024]

    # 第一階段：修復/產生原始資料
    print("\n[Phase 1] 修復原始資料...")
    for county in counties_to_process:
        process_all_years_for_county(county, years_to_process)

    # 第二階段：格式轉換
    print("\n[Phase 2] 格式轉換...")
    for county in counties_to_process:
        for year in years_to_process:
            try:
                process_year_data(year, county, RAW_DIR, OUTPUT_DIR)
            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"Error {county} {year}: {e}")

    # 第三階段：驗證
    print("\n[Phase 3] 驗證...")
    try:
        import verify_fix_duplicates
        import verify_candidate_filters
        import verify_township_reps

        verify_fix_duplicates.verify()
        verify_candidate_filters.verify()
        verify_township_reps.verify()
        print("\n=== [SUCCESS] All Verification Checks Passed! ===")
    except ImportError:
        print("[WARN] Verification modules not found, skipping verification.")
    except SystemExit as e:
        if e.code != 0:
            print("\n[FAIL] Verification Failed!")
        sys.exit(e.code)
    except Exception as e:
        print(f"\n[ERROR] Verification Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
