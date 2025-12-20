# -*- coding: utf-8 -*-
"""
選舉資料處理系統 - 基礎處理函數
Base processing functions for election data processor

提供可重用的資料處理函數，減少程式碼重複。
"""

import os
import pandas as pd
from collections import defaultdict

from .utils import read_csv_clean, clean_number, load_party_map, get_party_name
from .election_types import STAT_FIELDS


def load_election_data(data_dir):
    """載入選舉原始 CSV 資料

    Args:
        data_dir: 選舉資料目錄路徑

    Returns:
        tuple: (df_base, df_cand, df_tks, df_prof) 或 None
    """
    if not os.path.exists(data_dir):
        print(f"  [SKIP] 資料夾不存在: {data_dir}")
        return None

    # 載入政黨對照表
    load_party_map(os.path.join(data_dir, 'elpaty.csv'))

    # 讀取 CSV 檔案
    df_base = read_csv_clean(os.path.join(data_dir, 'elbase.csv'))
    df_cand = read_csv_clean(os.path.join(data_dir, 'elcand.csv'))
    df_tks = read_csv_clean(os.path.join(data_dir, 'elctks.csv'))
    df_prof = read_csv_clean(os.path.join(data_dir, 'elprof.csv'))

    return df_base, df_cand, df_tks, df_prof


def filter_by_city(dfs, prv_code, city_code=None):
    """依縣市過濾資料

    Args:
        dfs: tuple (df_base, df_cand, df_tks, df_prof)
        prv_code: 省市代碼
        city_code: 縣市代碼（None 或 '000' 表示直轄市）

    Returns:
        tuple: 過濾後的 (df_base, df_cand, df_tks, df_prof)
    """
    df_base, df_cand, df_tks, df_prof = dfs

    if city_code is None or city_code == '000':
        # 直轄市：只用 prv_code
        df_base = df_base[df_base[0] == prv_code]
        df_cand = df_cand[df_cand[0] == prv_code]
        df_tks = df_tks[df_tks[0] == prv_code]
        df_prof = df_prof[df_prof[0] == prv_code]
    else:
        # 縣市：用 prv_code + city_code
        df_base = df_base[(df_base[0] == prv_code) & (df_base[1] == city_code)]
        df_cand = df_cand[(df_cand[0] == prv_code) & (df_cand[1] == city_code)]
        df_tks = df_tks[(df_tks[0] == prv_code) & (df_tks[1] == city_code)]
        df_prof = df_prof[(df_prof[0] == prv_code) & (df_prof[1] == city_code)]

    return df_base, df_cand, df_tks, df_prof


def build_name_maps(df_base, include_area=False):
    """建立區域名稱對照表

    Args:
        df_base: 基本資料 DataFrame
        include_area: 是否包含選區代碼

    Returns:
        tuple: (dist_map, village_map)
    """
    dist_map = {}
    village_map = {}

    for _, row in df_base.iterrows():
        if include_area:
            area = row[2]
            dept = row[3]
            li = row[4]
            name = row[5]
            if li == '0000' or li == '0':
                dist_map[f"{area}_{dept}"] = name
            else:
                village_map[f"{area}_{dept}_{li}"] = name
        else:
            dept, li, name = row[3], row[4], row[5]
            if li == '0000' or li == '0':
                dist_map[dept] = name
            else:
                village_map[f"{dept}_{li}"] = name

    return dist_map, village_map


def build_candidate_list(df_cand, by_area=False, is_national=False, has_combined_name=False):
    """建立候選人列表

    Args:
        df_cand: 候選人資料 DataFrame
        by_area: 是否按選區分組
        is_national: 是否為全國層級候選人（如總統）
        has_combined_name: 是否合併名稱（如總統副總統）

    Returns:
        dict or list: 候選人資料
    """
    if is_national:
        # 全國層級：過濾全國候選人
        df_cand = df_cand[(df_cand[0] == '0') | (df_cand[0] == '00') |
                         (df_cand[2] == '0') | (df_cand[2] == '00') |
                         (df_cand[2] == '01') | (df_cand[2] == '1')]

    if by_area:
        cand_by_area = defaultdict(list)
        for _, row in df_cand.iterrows():
            cand_by_area[row[2]].append({
                'no': row[5],
                'name': row[6],
                'party': get_party_name(row[7])
            })

        # 排序
        for area in cand_by_area:
            cand_by_area[area] = sorted(
                cand_by_area[area],
                key=lambda x: int(x['no']) if str(x['no']).isdigit() else 0
            )
        return cand_by_area

    # 單一候選人列表
    if has_combined_name:
        # 總統副總統組合
        cand_by_no = defaultdict(list)
        for _, row in df_cand.iterrows():
            cand_by_no[row[5]].append({
                'name': row[6],
                'party': get_party_name(row[7])
            })

        candidates = []
        for cand_no in sorted(cand_by_no.keys(), key=lambda x: int(x) if str(x).isdigit() else 0):
            items = cand_by_no[cand_no]
            if len(items) >= 2:
                combined_name = f"{items[0]['name']}\n{items[1]['name']}"
            else:
                combined_name = items[0]['name'] if items else ''
            candidates.append({
                'no': cand_no,
                'name': combined_name,
                'party': items[0]['party'] if items else ''
            })
        return candidates

    candidates = []
    seen_nos = set()
    for _, row in df_cand.iterrows():
        no = row[5]
        if no in seen_nos:
            continue
        seen_nos.add(no)
        candidates.append({
            'no': no,
            'name': row[6],
            'party': get_party_name(row[7])
        })

    return sorted(candidates, key=lambda x: int(x['no']) if str(x['no']).isdigit() else 0)


def build_stats_map(df_prof, use_village_summary=True, include_area=False):
    """建立統計資料對照表

    Args:
        df_prof: 投票統計 DataFrame
        use_village_summary: 是否使用村里彙總列（tbox=0）
        include_area: 是否包含選區代碼

    Returns:
        dict: 統計資料對照
    """
    stats_map = {}

    for _, row in df_prof.iterrows():
        dept = row[3]
        li = row[4]
        tbox = row[5]

        # 跳過區域彙總列
        if li == '0000' or li == '0':
            continue

        # 根據彙總層級過濾
        if use_village_summary:
            if tbox != '0' and tbox != '0000':
                continue
        else:
            if tbox == '0' or tbox == '0000':
                continue

        # 建立 key
        if include_area:
            key = f"{row[2]}_{dept}_{li}" if use_village_summary else f"{dept}_{li}_{tbox}"
        else:
            key = f"{dept}_{li}" if use_village_summary else f"{dept}_{li}_{tbox}"

        valid_votes = clean_number(row[6])
        invalid_votes = clean_number(row[7])
        total_votes = clean_number(row[8])
        eligible_voters = clean_number(row[9]) if len(row) > 9 else 0

        # 嘗試讀取投票率
        turnout = 0
        if len(row) > 18 and row[18]:
            try:
                turnout = float(row[18])
            except (ValueError, TypeError):
                turnout = 0

        stats_map[key] = {
            '有效票數': valid_votes,
            '無效票數': invalid_votes,
            '投票數': total_votes,
            '選舉人數': eligible_voters,
            '已領未投票數': 0,
            '發出票數': total_votes,
            '用餘票數': eligible_voters - total_votes if eligible_voters > total_votes else 0,
            '投票率': turnout if turnout else (round(total_votes / eligible_voters * 100, 2) if eligible_voters > 0 else 0)
        }

    return stats_map


def build_votes_map(df_tks, use_village_summary=True, by_area=False):
    """建立票數資料對照表

    Args:
        df_tks: 得票數 DataFrame
        use_village_summary: 是否使用村里彙總列（tbox=0）
        by_area: 是否按選區分組

    Returns:
        dict: 票數資料對照
    """
    if by_area:
        vote_data = defaultdict(lambda: defaultdict(dict))
    else:
        vote_data = defaultdict(lambda: defaultdict(int))

    for _, row in df_tks.iterrows():
        li = row[4]
        tbox = row[5]

        # 跳過區域彙總列
        if li == '0000' or li == '0':
            continue

        # 根據彙總層級過濾
        if use_village_summary:
            if tbox != '0' and tbox != '0000':
                continue
            key = f"{row[3]}_{li}"
        else:
            if tbox == '0' or tbox == '0000':
                continue
            key = f"{row[3]}_{li}_{tbox}"

        votes = clean_number(row[7])
        cand_no = row[6]

        if by_area:
            vote_data[row[2]][key][cand_no] = votes
        else:
            vote_data[key][cand_no] = votes

    return vote_data


def calculate_totals(votes_by_village, stats_by_village, candidates):
    """計算區級和總計彙總

    Args:
        votes_by_village: 村里票數對照
        stats_by_village: 村里統計對照
        candidates: 候選人列表

    Returns:
        tuple: (dept_totals, grand_total)
    """
    dept_totals = defaultdict(lambda: {'votes': defaultdict(int), 'stats': defaultdict(int)})
    grand_total = {'votes': defaultdict(int), 'stats': defaultdict(int)}

    for key in votes_by_village.keys():
        parts = key.split('_')
        dept = parts[0]

        votes_dict = votes_by_village[key]
        stats = stats_by_village.get(key, {})

        # 累加候選人得票
        for cand_no, votes in votes_dict.items():
            dept_totals[dept]['votes'][cand_no] += votes
            grand_total['votes'][cand_no] += votes

        # 累加統計欄位
        for stat_key in ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數']:
            dept_totals[dept]['stats'][stat_key] += stats.get(stat_key, 0)
            grand_total['stats'][stat_key] += stats.get(stat_key, 0)

    # 計算投票率
    if grand_total['stats']['選舉人數'] > 0:
        grand_total['stats']['投票率'] = round(
            grand_total['stats']['投票數'] / grand_total['stats']['選舉人數'] * 100, 2
        )

    for dept in dept_totals:
        if dept_totals[dept]['stats']['選舉人數'] > 0:
            dept_totals[dept]['stats']['投票率'] = round(
                dept_totals[dept]['stats']['投票數'] / dept_totals[dept]['stats']['選舉人數'] * 100, 2
            )

    return dept_totals, grand_total


def generate_rows(votes_by_village, stats_by_village, candidates, dist_map, village_map,
                  include_polling_station=False, area_prefix=None):
    """生成輸出資料列

    Args:
        votes_by_village: 村里票數對照
        stats_by_village: 村里統計對照
        candidates: 候選人列表
        dist_map: 行政區名稱對照
        village_map: 村里名稱對照
        include_polling_station: 是否包含投開票所
        area_prefix: 選區前綴（用於 key 查找）

    Returns:
        list: 資料列
    """
    rows = []
    current_dept = None

    # 排序 keys
    sorted_keys = sorted(
        votes_by_village.keys(),
        key=lambda x: tuple(x.split('_'))
    )

    for key in sorted_keys:
        parts = key.split('_')

        if include_polling_station:
            dept, li, tbox = parts[0], parts[1], parts[2]
        else:
            dept, li = parts[0], parts[1]
            tbox = None

        # 取得名稱
        if area_prefix:
            dist_name = dist_map.get(f"{area_prefix}_{dept}", dept)
            village_name = village_map.get(f"{area_prefix}_{dept}_{li}", li)
        else:
            dist_name = dist_map.get(dept, dept)
            village_name = village_map.get(f"{dept}_{li}", li)

        row_data = {
            '行政區別': dist_name if dept != current_dept else '',
            '村里別': village_name,
        }

        if include_polling_station:
            row_data['投開票所別'] = tbox

        # 候選人得票
        votes_dict = votes_by_village[key]
        for i, cand in enumerate(candidates):
            row_data[f'候選人{i+1}'] = votes_dict.get(cand['no'], 0)

        # 統計欄位
        if area_prefix:
            stats_key = f"{area_prefix}_{dept}_{li}"
        else:
            stats_key = key

        stats = stats_by_village.get(stats_key, stats_by_village.get(key, {}))
        row_data['有效票數'] = stats.get('有效票數', 0)
        row_data['無效票數'] = stats.get('無效票數', 0)
        row_data['投票數'] = stats.get('投票數', 0)
        row_data['已領未投票數'] = stats.get('已領未投票數', 0)
        row_data['發出票數'] = stats.get('發出票數', 0)
        row_data['用餘票數'] = stats.get('用餘票數', 0)
        row_data['選舉人數'] = stats.get('選舉人數', 0)
        row_data['投票率'] = stats.get('投票率', 0)

        rows.append(row_data)
        current_dept = dept

    return rows


def process_single_area_election(data_dir, prv_code, city_code, city_name,
                                  use_village_summary=True, is_national=False,
                                  has_combined_name=False):
    """處理單選區選舉（如總統、市長）

    Args:
        data_dir: 資料目錄
        prv_code: 省市代碼
        city_code: 縣市代碼
        city_name: 縣市名稱
        use_village_summary: 是否使用村里彙總
        is_national: 是否全國候選人
        has_combined_name: 是否合併名稱

    Returns:
        dict: 處理結果
    """
    # 載入資料
    data = load_election_data(data_dir)
    if not data:
        return None

    # 過濾縣市
    df_base, df_cand, df_tks, df_prof = filter_by_city(data, prv_code, city_code)

    if df_base.empty:
        print(f"  [SKIP] 無 {city_name} 資料")
        return None

    # 建立名稱對照
    dist_map, village_map = build_name_maps(df_base)

    # 建立候選人列表
    if is_national:
        candidates = build_candidate_list(data[1], by_area=False, is_national=True, has_combined_name=has_combined_name)
    else:
        candidates = build_candidate_list(df_cand, by_area=False)

    # 建立統計和票數對照
    stats_by_village = build_stats_map(df_prof, use_village_summary=use_village_summary)
    votes_by_village = build_votes_map(df_tks, use_village_summary=use_village_summary)

    if not votes_by_village:
        return None

    # 計算彙總
    dept_totals, grand_total = calculate_totals(votes_by_village, stats_by_village, candidates)

    # 生成資料列
    rows = generate_rows(votes_by_village, stats_by_village, candidates, dist_map, village_map)

    if rows:
        return {
            'data': pd.DataFrame(rows),
            'candidates': candidates,
            'dept_totals': dept_totals,
            'grand_total': grand_total,
            'dist_map': dist_map,
        }

    return None


def process_multi_area_election(data_dir, prv_code, city_code, city_name,
                                 use_village_summary=True, use_polling_station=False):
    """處理多選區選舉（如議員、立委）

    Args:
        data_dir: 資料目錄
        prv_code: 省市代碼
        city_code: 縣市代碼
        city_name: 縣市名稱
        use_village_summary: 是否使用村里彙總
        use_polling_station: 是否包含投開票所

    Returns:
        dict: 各選區處理結果
    """
    # 載入資料
    data = load_election_data(data_dir)
    if not data:
        return None

    # 過濾縣市
    df_base, df_cand, df_tks, df_prof = filter_by_city(data, prv_code, city_code)

    if df_base.empty:
        print(f"  [SKIP] 無 {city_name} 資料")
        return None

    # 建立名稱對照
    dist_map, village_map = build_name_maps(df_base)

    # 建立候選人對照（按選區）
    cand_by_area = build_candidate_list(df_cand, by_area=True)

    # 建立統計和票數對照
    stats_map = build_stats_map(df_prof, use_village_summary=use_village_summary)
    vote_data = build_votes_map(df_tks, use_village_summary=use_village_summary, by_area=True)

    # 生成各選區結果
    results = {}
    for area in sorted(cand_by_area.keys()):
        if area == '0' or area == '00':
            continue

        candidates = cand_by_area[area]
        area_votes = vote_data.get(area, {})

        if not area_votes:
            continue

        # 計算彙總
        dept_totals, grand_total = calculate_totals(area_votes, stats_map, candidates)

        # 生成資料列
        rows = generate_rows(
            area_votes, stats_map, candidates, dist_map, village_map,
            include_polling_station=use_polling_station
        )

        if rows:
            results[area] = {
                'data': pd.DataFrame(rows),
                'candidates': candidates,
                'dept_totals': dept_totals,
                'grand_total': grand_total,
                'dist_map': dist_map,
            }

    return results if results else None
