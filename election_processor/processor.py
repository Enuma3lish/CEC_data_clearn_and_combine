# -*- coding: utf-8 -*-
"""
選舉資料處理系統 - 資料處理函數
Data processing functions for election data processor
"""

import os
import pandas as pd
from collections import defaultdict

from .utils import read_csv_clean, clean_number, load_party_map, get_party_name


def process_council_municipality(data_dir, prv_code, city_name):
    """處理直轄市區域議員選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省市代碼
        city_name: 縣市名稱

    Returns:
        dict: 各選區的 DataFrame 和候選人資訊
    """
    if not os.path.exists(data_dir):
        print(f"  [SKIP] 資料夾不存在: {data_dir}")
        return None

    load_party_map(os.path.join(data_dir, 'elpaty.csv'))

    df_base = read_csv_clean(os.path.join(data_dir, 'elbase.csv'))
    df_cand = read_csv_clean(os.path.join(data_dir, 'elcand.csv'))
    df_tks = read_csv_clean(os.path.join(data_dir, 'elctks.csv'))
    df_prof = read_csv_clean(os.path.join(data_dir, 'elprof.csv'))

    # 過濾指定縣市
    df_base = df_base[df_base[0] == prv_code]
    df_cand = df_cand[df_cand[0] == prv_code]
    df_tks = df_tks[df_tks[0] == prv_code]
    df_prof = df_prof[df_prof[0] == prv_code]

    if df_base.empty:
        print(f"  [SKIP] 無 {city_name} 資料")
        return None

    # 建立區域名稱對照
    dist_map = {}
    village_map = {}
    for _, row in df_base.iterrows():
        dept, li, name = row[3], row[4], row[5]
        if li == '0000':
            dist_map[dept] = name
        else:
            village_map[f"{dept}_{li}"] = name

    # 建立候選人對照
    cand_by_area = defaultdict(list)
    for _, row in df_cand.iterrows():
        cand_by_area[row[2]].append({
            'no': row[5],
            'name': row[6],
            'party': get_party_name(row[7])
        })

    for area in cand_by_area:
        cand_by_area[area] = sorted(
            cand_by_area[area],
            key=lambda x: int(x['no']) if x['no'].isdigit() else 0
        )

    # 建立統計資料對照
    stats_map = {}
    for _, row in df_prof.iterrows():
        key = f"{row[3]}_{row[4]}_{row[5]}"
        valid_votes = clean_number(row[6])
        invalid_votes = clean_number(row[7])
        total_votes = clean_number(row[8])
        eligible_voters = clean_number(row[9]) if len(row) > 9 else 0
        turnout = float(row[18]) if len(row) > 18 and row[18] else 0

        # 計算其他欄位
        issued_ballots = total_votes  # E = C (假設 D = 0)
        unused_ballots = eligible_voters - issued_ballots  # F = G - E

        stats_map[key] = {
            '有效票數': valid_votes,
            '無效票數': invalid_votes,
            '投票數': total_votes,
            '已領未投票數': 0,  # D
            '發出票數': issued_ballots,  # E
            '用餘票數': unused_ballots,  # F
            '選舉人數': eligible_voters,
            '投票率': turnout if turnout else (total_votes / eligible_voters * 100 if eligible_voters > 0 else 0)
        }

    # 處理票數資料
    vote_data = defaultdict(lambda: defaultdict(dict))
    for _, row in df_tks.iterrows():
        key = f"{row[3]}_{row[4]}_{row[5]}"
        vote_data[row[2]][key][row[6]] = clean_number(row[7])

    # 生成各選區結果
    results = {}
    for area in sorted(cand_by_area.keys()):
        if area == '00':
            continue

        candidates = cand_by_area[area]
        area_votes = vote_data[area]
        rows = []
        current_dept = None

        sorted_keys = sorted(
            area_votes.keys(),
            key=lambda x: (x.split('_')[0], x.split('_')[1], int(x.split('_')[2]) if x.split('_')[2].isdigit() else 0)
        )

        for key in sorted_keys:
            parts = key.split('_')
            dept, li, tbox = parts[0], parts[1], parts[2]

            if li == '0000' or tbox == '0' or tbox == '0000':
                continue

            row_data = {
                '行政區別': dist_map.get(dept, dept) if dept != current_dept else '',
                '村里別': village_map.get(f"{dept}_{li}", li),
                '投開票所別': tbox,
            }

            votes_dict = area_votes[key]
            for i, cand in enumerate(candidates):
                row_data[f'候選人{i+1}'] = votes_dict.get(cand['no'], 0)

            stats = stats_map.get(key, {})
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

        if rows:
            results[area] = {
                'data': pd.DataFrame(rows),
                'candidates': candidates
            }

    return results


def process_council_county(data_dir, prv_code, city_code, city_name):
    """處理縣市區域議員選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省代碼
        city_code: 縣市代碼
        city_name: 縣市名稱

    Returns:
        dict: 各選區的 DataFrame 和候選人資訊
    """
    if not os.path.exists(data_dir):
        print(f"  [SKIP] 資料夾不存在: {data_dir}")
        return None

    load_party_map(os.path.join(data_dir, 'elpaty.csv'))

    df_base = read_csv_clean(os.path.join(data_dir, 'elbase.csv'))
    df_cand = read_csv_clean(os.path.join(data_dir, 'elcand.csv'))
    df_tks = read_csv_clean(os.path.join(data_dir, 'elctks.csv'))
    df_prof = read_csv_clean(os.path.join(data_dir, 'elprof.csv'))

    # 過濾指定縣市
    df_base = df_base[(df_base[0] == prv_code) & (df_base[1] == city_code)]
    df_cand = df_cand[(df_cand[0] == prv_code) & (df_cand[1] == city_code)]
    df_tks = df_tks[(df_tks[0] == prv_code) & (df_tks[1] == city_code)]
    df_prof = df_prof[(df_prof[0] == prv_code) & (df_prof[1] == city_code)]

    if df_base.empty:
        print(f"  [SKIP] 無 {city_name} 資料")
        return None

    # 建立區域名稱對照
    dist_map = {}
    village_map = {}
    for _, row in df_base.iterrows():
        dept, li, name = row[3], row[4], row[5]
        if li == '0000' or li == '0':
            dist_map[dept] = name
        else:
            village_map[f"{dept}_{li}"] = name

    # 建立候選人對照
    cand_by_area = defaultdict(list)
    for _, row in df_cand.iterrows():
        cand_by_area[row[2]].append({
            'no': row[5],
            'name': row[6],
            'party': get_party_name(row[7])
        })

    for area in cand_by_area:
        cand_by_area[area] = sorted(
            cand_by_area[area],
            key=lambda x: int(x['no']) if x['no'].isdigit() else 0
        )

    # 建立統計資料對照
    stats_map = {}
    for _, row in df_prof.iterrows():
        key = f"{row[3]}_{row[4]}_{row[5]}"
        valid_votes = clean_number(row[6])
        invalid_votes = clean_number(row[7])
        total_votes = clean_number(row[8])
        eligible_voters = clean_number(row[9]) if len(row) > 9 else 0
        turnout = float(row[18]) if len(row) > 18 and row[18] else 0

        issued_ballots = total_votes
        unused_ballots = eligible_voters - issued_ballots

        stats_map[key] = {
            '有效票數': valid_votes,
            '無效票數': invalid_votes,
            '投票數': total_votes,
            '已領未投票數': 0,
            '發出票數': issued_ballots,
            '用餘票數': unused_ballots,
            '選舉人數': eligible_voters,
            '投票率': turnout if turnout else (total_votes / eligible_voters * 100 if eligible_voters > 0 else 0)
        }

    # 處理票數資料
    vote_data = defaultdict(lambda: defaultdict(dict))
    for _, row in df_tks.iterrows():
        key = f"{row[3]}_{row[4]}_{row[5]}"
        vote_data[row[2]][key][row[6]] = clean_number(row[7])

    # 生成結果
    results = {}
    for area in sorted(cand_by_area.keys()):
        if area == '00':
            continue

        candidates = cand_by_area[area]
        area_votes = vote_data[area]
        rows = []
        current_dept = None

        sorted_keys = sorted(
            area_votes.keys(),
            key=lambda x: (x.split('_')[0], x.split('_')[1], int(x.split('_')[2]) if x.split('_')[2].isdigit() else 0)
        )

        for key in sorted_keys:
            parts = key.split('_')
            dept, li, tbox = parts[0], parts[1], parts[2]

            if li == '0000' or li == '0' or tbox == '0' or tbox == '0000':
                continue

            row_data = {
                '行政區別': dist_map.get(dept, dept) if dept != current_dept else '',
                '村里別': village_map.get(f"{dept}_{li}", li),
                '投開票所別': tbox,
            }

            votes_dict = area_votes[key]
            for i, cand in enumerate(candidates):
                row_data[f'候選人{i+1}'] = votes_dict.get(cand['no'], 0)

            stats = stats_map.get(key, {})
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

        if rows:
            results[area] = {
                'data': pd.DataFrame(rows),
                'candidates': candidates
            }

    return results


def process_mayor_municipality(data_dir, prv_code, city_name):
    """處理直轄市市長選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省市代碼
        city_name: 縣市名稱

    Returns:
        dict: data DataFrame 和 candidates list
    """
    if not os.path.exists(data_dir):
        print(f"  [SKIP] 資料夾不存在: {data_dir}")
        return None

    load_party_map(os.path.join(data_dir, 'elpaty.csv'))

    df_base = read_csv_clean(os.path.join(data_dir, 'elbase.csv'))
    df_cand = read_csv_clean(os.path.join(data_dir, 'elcand.csv'))
    df_tks = read_csv_clean(os.path.join(data_dir, 'elctks.csv'))
    df_prof = read_csv_clean(os.path.join(data_dir, 'elprof.csv'))

    # 過濾指定縣市
    df_base = df_base[df_base[0] == prv_code]
    df_cand = df_cand[df_cand[0] == prv_code]
    df_tks = df_tks[df_tks[0] == prv_code]
    df_prof = df_prof[df_prof[0] == prv_code]

    if df_base.empty:
        print(f"  [SKIP] 無 {city_name} 資料")
        return None

    # 建立區域名稱對照
    dist_map = {}
    village_map = {}
    for _, row in df_base.iterrows():
        dept, li, name = row[3], row[4], row[5]
        if li == '0000' or li == '0':
            dist_map[dept] = name
        else:
            village_map[f"{dept}_{li}"] = name

    # 建立候選人列表
    candidates = []
    for _, row in df_cand.iterrows():
        candidates.append({
            'no': row[5],
            'name': row[6],
            'party': get_party_name(row[7])
        })

    candidates = sorted(candidates, key=lambda x: int(x['no']) if str(x['no']).isdigit() else 0)

    # 建立統計資料對照 (使用村里彙總列 tbox=0)
    stats_by_village = {}
    for _, row in df_prof.iterrows():
        dept = row[3]
        li = row[4]
        tbox = row[5]

        if li == '0000' or li == '0':
            continue
        if tbox != '0' and tbox != '0000':
            continue

        key = f"{dept}_{li}"
        stats_by_village[key] = {
            '有效票數': clean_number(row[6]),
            '無效票數': clean_number(row[7]),
            '投票數': clean_number(row[8]),
            '選舉人數': clean_number(row[9]) if len(row) > 9 else 0,
        }

    # 計算衍生欄位
    for key in stats_by_village:
        stats = stats_by_village[key]
        total_votes = stats['投票數']
        eligible_voters = stats['選舉人數']
        stats['已領未投票數'] = 0
        stats['發出票數'] = total_votes
        stats['用餘票數'] = eligible_voters - total_votes if eligible_voters > total_votes else 0
        stats['投票率'] = round(total_votes / eligible_voters * 100, 2) if eligible_voters > 0 else 0

    # 使用村里彙總列的票數
    votes_by_village = defaultdict(lambda: defaultdict(int))
    for _, row in df_tks.iterrows():
        li = row[4]
        tbox = row[5]

        if li == '0000' or li == '0':
            continue
        if tbox != '0' and tbox != '0000':
            continue

        key = f"{row[3]}_{li}"
        votes_by_village[key][row[6]] = clean_number(row[7])

    # 計算區級和總計彙總
    dept_totals = defaultdict(lambda: {'votes': defaultdict(int), 'stats': defaultdict(int)})
    grand_total = {'votes': defaultdict(int), 'stats': defaultdict(int)}

    for key in votes_by_village.keys():
        parts = key.split('_')
        dept = parts[0]

        votes_dict = votes_by_village[key]
        stats = stats_by_village.get(key, {})

        for cand_no, votes in votes_dict.items():
            dept_totals[dept]['votes'][cand_no] += votes
            grand_total['votes'][cand_no] += votes

        for stat_key in ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數']:
            dept_totals[dept]['stats'][stat_key] += stats.get(stat_key, 0)
            grand_total['stats'][stat_key] += stats.get(stat_key, 0)

    # 計算投票率
    if grand_total['stats']['選舉人數'] > 0:
        grand_total['stats']['投票率'] = round(grand_total['stats']['投票數'] / grand_total['stats']['選舉人數'] * 100, 2)

    for dept in dept_totals:
        if dept_totals[dept]['stats']['選舉人數'] > 0:
            dept_totals[dept]['stats']['投票率'] = round(dept_totals[dept]['stats']['投票數'] / dept_totals[dept]['stats']['選舉人數'] * 100, 2)

    # 生成資料列
    rows = []
    current_dept = None

    for key in sorted(votes_by_village.keys(), key=lambda x: (x.split('_')[0], x.split('_')[1])):
        parts = key.split('_')
        dept, li = parts[0], parts[1]

        dist_name = dist_map.get(dept, dept)
        village_name = village_map.get(key, li)

        row_data = {
            '行政區別': dist_name if dept != current_dept else '',
            '村里別': village_name,
        }

        votes_dict = votes_by_village[key]
        for i, cand in enumerate(candidates):
            row_data[f'候選人{i+1}'] = votes_dict.get(cand['no'], 0)

        stats = stats_by_village.get(key, {})
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

    if rows:
        return {
            'data': pd.DataFrame(rows),
            'candidates': candidates,
            'dept_totals': dept_totals,
            'grand_total': grand_total,
            'dist_map': dist_map,
        }

    return None


def process_mayor_county(data_dir, prv_code, city_code, city_name):
    """處理縣市市長選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省代碼
        city_code: 縣市代碼
        city_name: 縣市名稱

    Returns:
        dict: data DataFrame 和 candidates list
    """
    if not os.path.exists(data_dir):
        print(f"  [SKIP] 資料夾不存在: {data_dir}")
        return None

    load_party_map(os.path.join(data_dir, 'elpaty.csv'))

    df_base = read_csv_clean(os.path.join(data_dir, 'elbase.csv'))
    df_cand = read_csv_clean(os.path.join(data_dir, 'elcand.csv'))
    df_tks = read_csv_clean(os.path.join(data_dir, 'elctks.csv'))
    df_prof = read_csv_clean(os.path.join(data_dir, 'elprof.csv'))

    # 過濾指定縣市
    df_base = df_base[(df_base[0] == prv_code) & (df_base[1] == city_code)]
    df_cand = df_cand[(df_cand[0] == prv_code) & (df_cand[1] == city_code)]
    df_tks = df_tks[(df_tks[0] == prv_code) & (df_tks[1] == city_code)]
    df_prof = df_prof[(df_prof[0] == prv_code) & (df_prof[1] == city_code)]

    if df_base.empty:
        print(f"  [SKIP] 無 {city_name} 資料")
        return None

    # 建立區域名稱對照
    dist_map = {}
    village_map = {}
    for _, row in df_base.iterrows():
        dept, li, name = row[3], row[4], row[5]
        if li == '0000' or li == '0':
            dist_map[dept] = name
        else:
            village_map[f"{dept}_{li}"] = name

    # 建立候選人列表
    candidates = []
    for _, row in df_cand.iterrows():
        candidates.append({
            'no': row[5],
            'name': row[6],
            'party': get_party_name(row[7])
        })

    candidates = sorted(candidates, key=lambda x: int(x['no']) if str(x['no']).isdigit() else 0)

    # 建立統計資料對照 (使用村里彙總列 tbox=0)
    stats_by_village = {}
    for _, row in df_prof.iterrows():
        dept = row[3]
        li = row[4]
        tbox = row[5]

        if li == '0000' or li == '0':
            continue
        if tbox != '0' and tbox != '0000':
            continue

        key = f"{dept}_{li}"
        stats_by_village[key] = {
            '有效票數': clean_number(row[6]),
            '無效票數': clean_number(row[7]),
            '投票數': clean_number(row[8]),
            '選舉人數': clean_number(row[9]) if len(row) > 9 else 0,
        }

    # 計算衍生欄位
    for key in stats_by_village:
        stats = stats_by_village[key]
        total_votes = stats['投票數']
        eligible_voters = stats['選舉人數']
        stats['已領未投票數'] = 0
        stats['發出票數'] = total_votes
        stats['用餘票數'] = eligible_voters - total_votes if eligible_voters > total_votes else 0
        stats['投票率'] = round(total_votes / eligible_voters * 100, 2) if eligible_voters > 0 else 0

    # 使用村里彙總列的票數
    votes_by_village = defaultdict(lambda: defaultdict(int))
    for _, row in df_tks.iterrows():
        li = row[4]
        tbox = row[5]

        if li == '0000' or li == '0':
            continue
        if tbox != '0' and tbox != '0000':
            continue

        key = f"{row[3]}_{li}"
        votes_by_village[key][row[6]] = clean_number(row[7])

    # 計算區級和總計彙總
    dept_totals = defaultdict(lambda: {'votes': defaultdict(int), 'stats': defaultdict(int)})
    grand_total = {'votes': defaultdict(int), 'stats': defaultdict(int)}

    for key in votes_by_village.keys():
        parts = key.split('_')
        dept = parts[0]

        votes_dict = votes_by_village[key]
        stats = stats_by_village.get(key, {})

        for cand_no, votes in votes_dict.items():
            dept_totals[dept]['votes'][cand_no] += votes
            grand_total['votes'][cand_no] += votes

        for stat_key in ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數']:
            dept_totals[dept]['stats'][stat_key] += stats.get(stat_key, 0)
            grand_total['stats'][stat_key] += stats.get(stat_key, 0)

    # 計算投票率
    if grand_total['stats']['選舉人數'] > 0:
        grand_total['stats']['投票率'] = round(grand_total['stats']['投票數'] / grand_total['stats']['選舉人數'] * 100, 2)

    for dept in dept_totals:
        if dept_totals[dept]['stats']['選舉人數'] > 0:
            dept_totals[dept]['stats']['投票率'] = round(dept_totals[dept]['stats']['投票數'] / dept_totals[dept]['stats']['選舉人數'] * 100, 2)

    # 生成資料列
    rows = []
    current_dept = None

    for key in sorted(votes_by_village.keys(), key=lambda x: (x.split('_')[0], x.split('_')[1])):
        parts = key.split('_')
        dept, li = parts[0], parts[1]

        dist_name = dist_map.get(dept, dept)
        village_name = village_map.get(key, li)

        row_data = {
            '行政區別': dist_name if dept != current_dept else '',
            '村里別': village_name,
        }

        votes_dict = votes_by_village[key]
        for i, cand in enumerate(candidates):
            row_data[f'候選人{i+1}'] = votes_dict.get(cand['no'], 0)

        stats = stats_by_village.get(key, {})
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

    if rows:
        return {
            'data': pd.DataFrame(rows),
            'candidates': candidates,
            'dept_totals': dept_totals,
            'grand_total': grand_total,
            'dist_map': dist_map,
        }

    return None


def process_legislator(data_dir, prv_code, city_code, city_name):
    """處理區域立委選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省代碼
        city_code: 縣市代碼
        city_name: 縣市名稱

    Returns:
        dict: 各選區的 DataFrame 和候選人資訊
    """
    if not os.path.exists(data_dir):
        print(f"  [SKIP] 資料夾不存在: {data_dir}")
        return None

    load_party_map(os.path.join(data_dir, 'elpaty.csv'))

    df_base = read_csv_clean(os.path.join(data_dir, 'elbase.csv'))
    df_cand = read_csv_clean(os.path.join(data_dir, 'elcand.csv'))
    df_tks = read_csv_clean(os.path.join(data_dir, 'elctks.csv'))
    df_prof = read_csv_clean(os.path.join(data_dir, 'elprof.csv'))

    # 過濾指定縣市
    if city_code == '000':
        # 直轄市
        df_base = df_base[df_base[0] == prv_code]
        df_cand = df_cand[df_cand[0] == prv_code]
        df_tks = df_tks[df_tks[0] == prv_code]
        df_prof = df_prof[df_prof[0] == prv_code]
    else:
        # 縣市
        df_base = df_base[(df_base[0] == prv_code) & (df_base[1] == city_code)]
        df_cand = df_cand[(df_cand[0] == prv_code) & (df_cand[1] == city_code)]
        df_tks = df_tks[(df_tks[0] == prv_code) & (df_tks[1] == city_code)]
        df_prof = df_prof[(df_prof[0] == prv_code) & (df_prof[1] == city_code)]

    if df_base.empty:
        print(f"  [SKIP] 無 {city_name} 資料")
        return None

    # 建立區域名稱對照
    dist_map = {}
    village_map = {}
    for _, row in df_base.iterrows():
        dept, li, name = row[3], row[4], row[5]
        if li == '0000' or li == '0':
            dist_map[dept] = name
        else:
            village_map[f"{dept}_{li}"] = name

    # 建立候選人對照 (按選區)
    cand_by_area = defaultdict(list)
    for _, row in df_cand.iterrows():
        cand_by_area[row[2]].append({
            'no': row[5],
            'name': row[6],
            'party': get_party_name(row[7])
        })

    for area in cand_by_area:
        cand_by_area[area] = sorted(
            cand_by_area[area],
            key=lambda x: int(x['no']) if str(x['no']).isdigit() else 0
        )

    # 建立統計資料對照 (使用村里彙總列 tbox=0)
    stats_map = {}
    for _, row in df_prof.iterrows():
        dept = row[3]
        li = row[4]
        tbox = row[5]

        if li == '0000' or li == '0':
            continue
        if tbox != '0' and tbox != '0000':
            continue

        key = f"{row[2]}_{dept}_{li}"
        valid_votes = clean_number(row[6])
        invalid_votes = clean_number(row[7])
        total_votes = clean_number(row[8])
        eligible_voters = clean_number(row[9]) if len(row) > 9 else 0

        issued_ballots = total_votes
        unused_ballots = eligible_voters - issued_ballots if eligible_voters > issued_ballots else 0

        stats_map[key] = {
            '有效票數': valid_votes,
            '無效票數': invalid_votes,
            '投票數': total_votes,
            '已領未投票數': 0,
            '發出票數': issued_ballots,
            '用餘票數': unused_ballots,
            '選舉人數': eligible_voters,
            '投票率': round(total_votes / eligible_voters * 100, 2) if eligible_voters > 0 else 0
        }

    # 處理票數資料 (使用村里彙總列)
    vote_data = defaultdict(lambda: defaultdict(dict))
    for _, row in df_tks.iterrows():
        li = row[4]
        tbox = row[5]

        if li == '0000' or li == '0':
            continue
        if tbox != '0' and tbox != '0000':
            continue

        key = f"{row[3]}_{li}"
        vote_data[row[2]][key][row[6]] = clean_number(row[7])

    # 生成各選區結果
    results = {}
    for area in sorted(cand_by_area.keys()):
        if area == '0' or area == '00':
            continue

        candidates = cand_by_area[area]
        area_votes = vote_data.get(area, {})
        rows = []
        current_dept = None

        # 計算區級和總計彙總
        dept_totals = defaultdict(lambda: {'votes': defaultdict(int), 'stats': defaultdict(int)})
        grand_total = {'votes': defaultdict(int), 'stats': defaultdict(int)}

        for key in sorted(area_votes.keys(), key=lambda x: (x.split('_')[0], x.split('_')[1])):
            parts = key.split('_')
            dept, li = parts[0], parts[1]

            votes_dict = area_votes[key]
            stats_key = f"{area}_{dept}_{li}"
            stats = stats_map.get(stats_key, {})

            for cand_no, votes in votes_dict.items():
                dept_totals[dept]['votes'][cand_no] += votes
                grand_total['votes'][cand_no] += votes

            for stat_key in ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數']:
                dept_totals[dept]['stats'][stat_key] += stats.get(stat_key, 0)
                grand_total['stats'][stat_key] += stats.get(stat_key, 0)

        # 計算投票率
        if grand_total['stats']['選舉人數'] > 0:
            grand_total['stats']['投票率'] = round(grand_total['stats']['投票數'] / grand_total['stats']['選舉人數'] * 100, 2)

        for dept in dept_totals:
            if dept_totals[dept]['stats']['選舉人數'] > 0:
                dept_totals[dept]['stats']['投票率'] = round(dept_totals[dept]['stats']['投票數'] / dept_totals[dept]['stats']['選舉人數'] * 100, 2)

        # 生成資料列
        for key in sorted(area_votes.keys(), key=lambda x: (x.split('_')[0], x.split('_')[1])):
            parts = key.split('_')
            dept, li = parts[0], parts[1]

            dist_name = dist_map.get(dept, dept)
            village_name = village_map.get(key, li)

            row_data = {
                '行政區別': dist_name if dept != current_dept else '',
                '村里別': village_name,
            }

            votes_dict = area_votes[key]
            for i, cand in enumerate(candidates):
                row_data[f'候選人{i+1}'] = votes_dict.get(cand['no'], 0)

            stats_key = f"{area}_{dept}_{li}"
            stats = stats_map.get(stats_key, {})
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

        if rows:
            results[area] = {
                'data': pd.DataFrame(rows),
                'candidates': candidates,
                'dept_totals': dept_totals,
                'grand_total': grand_total,
                'dist_map': dist_map,
            }

    return results if results else None


def process_president(data_dir, prv_code, city_code, city_name):
    """處理總統選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省代碼
        city_code: 縣市代碼
        city_name: 縣市名稱

    Returns:
        dict: data DataFrame 和 candidates list (總統選舉)
    """
    if not os.path.exists(data_dir):
        print(f"  [SKIP] 總統 - 資料夾不存在")
        return None

    load_party_map(os.path.join(data_dir, 'elpaty.csv'))

    df_base = read_csv_clean(os.path.join(data_dir, 'elbase.csv'))
    df_cand = read_csv_clean(os.path.join(data_dir, 'elcand.csv'))
    df_tks = read_csv_clean(os.path.join(data_dir, 'elctks.csv'))
    df_prof = read_csv_clean(os.path.join(data_dir, 'elprof.csv'))

    # 過濾指定縣市
    if city_code == '000':
        df_base_city = df_base[df_base[0] == prv_code]
        df_tks_city = df_tks[df_tks[0] == prv_code]
        df_prof_city = df_prof[df_prof[0] == prv_code]
    else:
        df_base_city = df_base[(df_base[0] == prv_code) & (df_base[1] == city_code)]
        df_tks_city = df_tks[(df_tks[0] == prv_code) & (df_tks[1] == city_code)]
        df_prof_city = df_prof[(df_prof[0] == prv_code) & (df_prof[1] == city_code)]

    # 候選人是全國層級
    df_cand_nat = df_cand[(df_cand[0] == '0') | (df_cand[0] == '00')]

    if df_base_city.empty:
        print(f"  [SKIP] 總統 - 無 {city_name} 資料")
        return None

    # 建立區域名稱對照
    dist_map = {}
    village_map = {}
    for _, row in df_base_city.iterrows():
        dept, li, name = row[3], row[4], row[5]
        if li == '0000' or li == '0':
            dist_map[dept] = name
        else:
            village_map[f"{dept}_{li}"] = name

    # 建立候選人列表
    cand_by_no = defaultdict(list)
    for _, row in df_cand_nat.iterrows():
        cand_by_no[row[5]].append({
            'name': row[6],
            'party': get_party_name(row[7])
        })

    candidates = []
    for cand_no in sorted(cand_by_no.keys(), key=lambda x: int(x) if x.isdigit() else 0):
        items = cand_by_no[cand_no]
        # 總統副總統組合名稱
        if len(items) >= 2:
            combined_name = f"{items[0]['name']}\n{items[1]['name']}"
        else:
            combined_name = items[0]['name'] if items else ''
        candidates.append({
            'no': cand_no,
            'name': combined_name,
            'party': items[0]['party'] if items else ''
        })

    # 建立統計資料對照 (使用村里彙總列 tbox=0000)
    stats_by_village = {}
    for _, row in df_prof_city.iterrows():
        dept = row[3]
        li = row[4]
        tbox = row[5]

        # 只使用村里層級彙總列 (tbox=0000, li != 0000)
        if li == '0000' or li == '0':
            continue
        if tbox != '0000':
            continue

        key = f"{dept}_{li}"
        stats_by_village[key] = {
            '有效票數': clean_number(row[6]),
            '無效票數': clean_number(row[7]),
            '投票數': clean_number(row[8]),
            '選舉人數': clean_number(row[9]) if len(row) > 9 else 0,
        }

    # 計算衍生欄位
    for key in stats_by_village:
        stats = stats_by_village[key]
        total_votes = stats['投票數']
        eligible_voters = stats['選舉人數']
        stats['已領未投票數'] = 0
        stats['發出票數'] = total_votes
        stats['用餘票數'] = eligible_voters - total_votes if eligible_voters > total_votes else 0
        stats['投票率'] = round(total_votes / eligible_voters * 100, 2) if eligible_voters > 0 else 0

    # 使用村里彙總列的票數 (tbox=0000)
    votes_by_village = defaultdict(lambda: defaultdict(int))
    for _, row in df_tks_city.iterrows():
        li = row[4]
        tbox = row[5]

        # 只使用村里層級彙總列 (tbox=0000, li != 0000)
        if li == '0000' or li == '0':
            continue
        if tbox != '0000':
            continue

        key = f"{row[3]}_{li}"
        votes_by_village[key][row[6]] = clean_number(row[7])

    # 生成結果 - 按行政區分組
    rows = []
    current_dept = None

    # 計算區級和總計彙總
    dept_totals = defaultdict(lambda: {'votes': defaultdict(int), 'stats': defaultdict(int)})
    grand_total = {'votes': defaultdict(int), 'stats': defaultdict(int)}

    for key in sorted(votes_by_village.keys(), key=lambda x: (x.split('_')[0], x.split('_')[1])):
        parts = key.split('_')
        dept, li = parts[0], parts[1]

        votes_dict = votes_by_village[key]
        stats = stats_by_village.get(key, {})

        # 累加到區級統計
        for cand_no, votes in votes_dict.items():
            dept_totals[dept]['votes'][cand_no] += votes
            grand_total['votes'][cand_no] += votes

        for stat_key in ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數']:
            dept_totals[dept]['stats'][stat_key] += stats.get(stat_key, 0)
            grand_total['stats'][stat_key] += stats.get(stat_key, 0)

    # 計算投票率
    if grand_total['stats']['選舉人數'] > 0:
        grand_total['stats']['投票率'] = round(grand_total['stats']['投票數'] / grand_total['stats']['選舉人數'] * 100, 2)

    for dept in dept_totals:
        if dept_totals[dept]['stats']['選舉人數'] > 0:
            dept_totals[dept]['stats']['投票率'] = round(dept_totals[dept]['stats']['投票數'] / dept_totals[dept]['stats']['選舉人數'] * 100, 2)

    # 生成資料列
    for key in sorted(votes_by_village.keys(), key=lambda x: (x.split('_')[0], x.split('_')[1])):
        parts = key.split('_')
        dept, li = parts[0], parts[1]

        dist_name = dist_map.get(dept, dept)
        village_name = village_map.get(key, li)

        row_data = {
            '行政區別': dist_name if dept != current_dept else '',
            '村里別': village_name,
        }

        votes_dict = votes_by_village[key]
        for i, cand in enumerate(candidates):
            row_data[f'候選人{i+1}'] = votes_dict.get(cand['no'], 0)

        stats = stats_by_village.get(key, {})
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

    if rows:
        return {
            'data': pd.DataFrame(rows),
            'candidates': candidates,
            'dept_totals': dept_totals,
            'grand_total': grand_total,
            'dist_map': dist_map,
        }

    return None


def process_township_mayor(data_dir, prv_code, city_code, city_name):
    """處理縣市鄉鎮市長選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省代碼
        city_code: 縣市代碼
        city_name: 縣市名稱

    Returns:
        dict: 各鄉鎮市的 DataFrame 和候選人資訊
    """
    if not os.path.exists(data_dir):
        print(f"  [SKIP] 資料夾不存在: {data_dir}")
        return None

    load_party_map(os.path.join(data_dir, 'elpaty.csv'))

    df_base = read_csv_clean(os.path.join(data_dir, 'elbase.csv'))
    df_cand = read_csv_clean(os.path.join(data_dir, 'elcand.csv'))
    df_tks = read_csv_clean(os.path.join(data_dir, 'elctks.csv'))
    df_prof = read_csv_clean(os.path.join(data_dir, 'elprof.csv'))

    # 過濾指定縣市
    df_base = df_base[(df_base[0] == prv_code) & (df_base[1] == city_code)]
    df_cand = df_cand[(df_cand[0] == prv_code) & (df_cand[1] == city_code)]
    df_tks = df_tks[(df_tks[0] == prv_code) & (df_tks[1] == city_code)]
    df_prof = df_prof[(df_prof[0] == prv_code) & (df_prof[1] == city_code)]

    if df_base.empty:
        print(f"  [SKIP] 無 {city_name} 鄉鎮市長資料")
        return None

    # 建立區域名稱對照（鄉鎮市區）
    dist_map = {}
    village_map = {}
    for _, row in df_base.iterrows():
        area = row[2]  # 選區代碼（鄉鎮市區）
        dept = row[3]
        li = row[4]
        name = row[5]
        if li == '0000' or li == '0':
            dist_map[f"{area}_{dept}"] = name
        else:
            village_map[f"{area}_{dept}_{li}"] = name

    # 建立候選人對照（按鄉鎮市區分組）
    cand_by_area = defaultdict(list)
    for _, row in df_cand.iterrows():
        cand_by_area[row[2]].append({
            'no': row[5],
            'name': row[6],
            'party': get_party_name(row[7])
        })

    for area in cand_by_area:
        cand_by_area[area] = sorted(
            cand_by_area[area],
            key=lambda x: int(x['no']) if str(x['no']).isdigit() else 0
        )

    # 建立統計資料對照 (使用村里彙總列 tbox=0)
    stats_map = {}
    for _, row in df_prof.iterrows():
        area = row[2]
        dept = row[3]
        li = row[4]
        tbox = row[5]

        if li == '0000' or li == '0':
            continue
        if tbox != '0' and tbox != '0000':
            continue

        key = f"{area}_{dept}_{li}"
        stats_map[key] = {
            '有效票數': clean_number(row[6]),
            '無效票數': clean_number(row[7]),
            '投票數': clean_number(row[8]),
            '選舉人數': clean_number(row[9]) if len(row) > 9 else 0,
        }

    # 計算衍生欄位
    for key in stats_map:
        stats = stats_map[key]
        total_votes = stats['投票數']
        eligible_voters = stats['選舉人數']
        stats['已領未投票數'] = 0
        stats['發出票數'] = total_votes
        stats['用餘票數'] = eligible_voters - total_votes if eligible_voters > total_votes else 0
        stats['投票率'] = round(total_votes / eligible_voters * 100, 2) if eligible_voters > 0 else 0

    # 使用村里彙總列的票數
    vote_data = defaultdict(lambda: defaultdict(dict))
    for _, row in df_tks.iterrows():
        li = row[4]
        tbox = row[5]

        if li == '0000' or li == '0':
            continue
        if tbox != '0' and tbox != '0000':
            continue

        key = f"{row[3]}_{li}"
        vote_data[row[2]][key][row[6]] = clean_number(row[7])

    # 生成各鄉鎮市區結果
    results = {}
    for area in sorted(cand_by_area.keys()):
        if area == '0' or area == '00':
            continue

        candidates = cand_by_area[area]
        area_votes = vote_data.get(area, {})
        area_rows = []
        current_dept = None

        # 取得鄉鎮市區名稱
        area_name = None
        for key, name in dist_map.items():
            if key.startswith(f"{area}_"):
                area_name = name
                break

        # 計算區級和總計彙總
        dept_totals = defaultdict(lambda: {'votes': defaultdict(int), 'stats': defaultdict(int)})
        grand_total = {'votes': defaultdict(int), 'stats': defaultdict(int)}

        for key in sorted(area_votes.keys(), key=lambda x: (x.split('_')[0], x.split('_')[1])):
            parts = key.split('_')
            dept, li = parts[0], parts[1]

            votes_dict = area_votes[key]
            stats_key = f"{area}_{dept}_{li}"
            stats = stats_map.get(stats_key, {})

            for cand_no, votes in votes_dict.items():
                dept_totals[dept]['votes'][cand_no] += votes
                grand_total['votes'][cand_no] += votes

            for stat_key in ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數']:
                dept_totals[dept]['stats'][stat_key] += stats.get(stat_key, 0)
                grand_total['stats'][stat_key] += stats.get(stat_key, 0)

        # 計算投票率
        if grand_total['stats']['選舉人數'] > 0:
            grand_total['stats']['投票率'] = round(grand_total['stats']['投票數'] / grand_total['stats']['選舉人數'] * 100, 2)

        for dept in dept_totals:
            if dept_totals[dept]['stats']['選舉人數'] > 0:
                dept_totals[dept]['stats']['投票率'] = round(dept_totals[dept]['stats']['投票數'] / dept_totals[dept]['stats']['選舉人數'] * 100, 2)

        # 生成資料列
        for key in sorted(area_votes.keys(), key=lambda x: (x.split('_')[0], x.split('_')[1])):
            parts = key.split('_')
            dept, li = parts[0], parts[1]

            dist_name_val = dist_map.get(f"{area}_{dept}", dept)
            village_name = village_map.get(f"{area}_{dept}_{li}", li)

            row_data = {
                '行政區別': dist_name_val if dept != current_dept else '',
                '村里別': village_name,
            }

            votes_dict = area_votes[key]
            for i, cand in enumerate(candidates):
                row_data[f'候選人{i+1}'] = votes_dict.get(cand['no'], 0)

            stats_key = f"{area}_{dept}_{li}"
            stats = stats_map.get(stats_key, {})
            row_data['有效票數'] = stats.get('有效票數', 0)
            row_data['無效票數'] = stats.get('無效票數', 0)
            row_data['投票數'] = stats.get('投票數', 0)
            row_data['已領未投票數'] = stats.get('已領未投票數', 0)
            row_data['發出票數'] = stats.get('發出票數', 0)
            row_data['用餘票數'] = stats.get('用餘票數', 0)
            row_data['選舉人數'] = stats.get('選舉人數', 0)
            row_data['投票率'] = stats.get('投票率', 0)

            area_rows.append(row_data)
            current_dept = dept

        if area_rows:
            results[area] = {
                'data': pd.DataFrame(area_rows),
                'candidates': candidates,
                'dept_totals': dept_totals,
                'grand_total': grand_total,
                'dist_map': {k: v for k, v in dist_map.items() if k.startswith(f"{area}_")},
                'area_name': area_name,
            }

    return results if results else None


def process_indigenous_legislator(data_dir, prv_code, city_code, city_name, legislator_type='mountain'):
    """處理原住民立委選舉資料（山地/平地）

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省代碼
        city_code: 縣市代碼
        city_name: 縣市名稱
        legislator_type: 'mountain' 或 'plain'

    Returns:
        dict: data DataFrame 和 candidates list
    """
    if not os.path.exists(data_dir):
        print(f"  [SKIP] 資料夾不存在: {data_dir}")
        return None

    load_party_map(os.path.join(data_dir, 'elpaty.csv'))

    df_base = read_csv_clean(os.path.join(data_dir, 'elbase.csv'))
    df_cand = read_csv_clean(os.path.join(data_dir, 'elcand.csv'))
    df_tks = read_csv_clean(os.path.join(data_dir, 'elctks.csv'))
    df_prof = read_csv_clean(os.path.join(data_dir, 'elprof.csv'))

    # 過濾指定縣市
    if city_code == '000':
        df_base = df_base[df_base[0] == prv_code]
        df_tks = df_tks[df_tks[0] == prv_code]
        df_prof = df_prof[df_prof[0] == prv_code]
    else:
        df_base = df_base[(df_base[0] == prv_code) & (df_base[1] == city_code)]
        df_tks = df_tks[(df_tks[0] == prv_code) & (df_tks[1] == city_code)]
        df_prof = df_prof[(df_prof[0] == prv_code) & (df_prof[1] == city_code)]

    # 候選人是全國層級
    df_cand_nat = df_cand[(df_cand[2] == '01') | (df_cand[2] == '1')]

    if df_base.empty:
        type_name = '山地' if legislator_type == 'mountain' else '平地'
        print(f"  [SKIP] {type_name}原住民立委 - 無 {city_name} 資料")
        return None

    # 建立區域名稱對照
    indigenous_dist_map = {}
    indigenous_village_map = {}
    for _, row in df_base.iterrows():
        dept, li, name = row[3], row[4], row[5]
        if li == '0000' or li == '0':
            indigenous_dist_map[dept] = name
        else:
            indigenous_village_map[f"{dept}_{li}"] = name

    # 建立候選人列表
    indigenous_candidates = []
    for _, row in df_cand_nat.iterrows():
        indigenous_candidates.append({
            'no': row[5],
            'name': row[6],
            'party': get_party_name(row[7])
        })

    indigenous_candidates = sorted(indigenous_candidates, key=lambda x: int(x['no']) if str(x['no']).isdigit() else 0)

    if not indigenous_candidates:
        return None

    # 建立統計資料對照 (使用村里彙總列 tbox=0)
    indigenous_stats_by_village = {}
    for _, row in df_prof.iterrows():
        dept = row[3]
        li = row[4]
        tbox = row[5]

        if li == '0000' or li == '0':
            continue
        if tbox != '0' and tbox != '0000':
            continue

        key = f"{dept}_{li}"
        indigenous_stats_by_village[key] = {
            '有效票數': clean_number(row[6]),
            '無效票數': clean_number(row[7]),
            '投票數': clean_number(row[8]),
            '選舉人數': clean_number(row[9]) if len(row) > 9 else 0,
        }

    # 計算衍生欄位
    for key in indigenous_stats_by_village:
        stats = indigenous_stats_by_village[key]
        total_votes = stats['投票數']
        eligible_voters = stats['選舉人數']
        stats['已領未投票數'] = 0
        stats['發出票數'] = total_votes
        stats['用餘票數'] = eligible_voters - total_votes if eligible_voters > total_votes else 0
        stats['投票率'] = round(total_votes / eligible_voters * 100, 2) if eligible_voters > 0 else 0

    # 使用村里彙總列的票數
    indigenous_votes_by_village = defaultdict(lambda: defaultdict(int))
    for _, row in df_tks.iterrows():
        li = row[4]
        tbox = row[5]

        if li == '0000' or li == '0':
            continue
        if tbox != '0' and tbox != '0000':
            continue

        key = f"{row[3]}_{li}"
        indigenous_votes_by_village[key][row[6]] = clean_number(row[7])

    if not indigenous_votes_by_village:
        return None

    # 計算區級和總計彙總
    indigenous_dept_totals = defaultdict(lambda: {'votes': defaultdict(int), 'stats': defaultdict(int)})
    indigenous_grand_total = {'votes': defaultdict(int), 'stats': defaultdict(int)}

    for key in indigenous_votes_by_village.keys():
        parts = key.split('_')
        dept = parts[0]

        votes_dict = indigenous_votes_by_village[key]
        stats = indigenous_stats_by_village.get(key, {})

        for cand_no, votes in votes_dict.items():
            indigenous_dept_totals[dept]['votes'][cand_no] += votes
            indigenous_grand_total['votes'][cand_no] += votes

        for stat_key in ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數']:
            indigenous_dept_totals[dept]['stats'][stat_key] += stats.get(stat_key, 0)
            indigenous_grand_total['stats'][stat_key] += stats.get(stat_key, 0)

    # 計算投票率
    if indigenous_grand_total['stats']['選舉人數'] > 0:
        indigenous_grand_total['stats']['投票率'] = round(indigenous_grand_total['stats']['投票數'] / indigenous_grand_total['stats']['選舉人數'] * 100, 2)

    for dept in indigenous_dept_totals:
        if indigenous_dept_totals[dept]['stats']['選舉人數'] > 0:
            indigenous_dept_totals[dept]['stats']['投票率'] = round(indigenous_dept_totals[dept]['stats']['投票數'] / indigenous_dept_totals[dept]['stats']['選舉人數'] * 100, 2)

    # 生成資料列
    indigenous_rows = []
    current_dept = None

    for key in sorted(indigenous_votes_by_village.keys(), key=lambda x: (x.split('_')[0], x.split('_')[1])):
        parts = key.split('_')
        dept, li = parts[0], parts[1]

        dist_name_val = indigenous_dist_map.get(dept, dept)
        village_name = indigenous_village_map.get(key, li)

        row_data = {
            '行政區別': dist_name_val if dept != current_dept else '',
            '村里別': village_name,
        }

        votes_dict = indigenous_votes_by_village[key]
        for i, cand in enumerate(indigenous_candidates):
            row_data[f'候選人{i+1}'] = votes_dict.get(cand['no'], 0)

        stats = indigenous_stats_by_village.get(key, {})
        row_data['有效票數'] = stats.get('有效票數', 0)
        row_data['無效票數'] = stats.get('無效票數', 0)
        row_data['投票數'] = stats.get('投票數', 0)
        row_data['已領未投票數'] = stats.get('已領未投票數', 0)
        row_data['發出票數'] = stats.get('發出票數', 0)
        row_data['用餘票數'] = stats.get('用餘票數', 0)
        row_data['選舉人數'] = stats.get('選舉人數', 0)
        row_data['投票率'] = stats.get('投票率', 0)

        indigenous_rows.append(row_data)
        current_dept = dept

    if indigenous_rows:
        return {
            'data': pd.DataFrame(indigenous_rows),
            'candidates': indigenous_candidates,
            'dept_totals': indigenous_dept_totals,
            'grand_total': indigenous_grand_total,
            'dist_map': indigenous_dist_map,
        }

    return None


def process_party_vote(data_dir, prv_code, city_code, city_name):
    """處理不分區政黨票選舉資料

    Args:
        data_dir: 選舉資料目錄路徑
        prv_code: 省代碼
        city_code: 縣市代碼
        city_name: 縣市名稱

    Returns:
        dict: data DataFrame 和 parties list
    """
    if not os.path.exists(data_dir):
        print(f"  [SKIP] 資料夾不存在: {data_dir}")
        return None

    load_party_map(os.path.join(data_dir, 'elpaty.csv'))

    df_base = read_csv_clean(os.path.join(data_dir, 'elbase.csv'))
    df_cand = read_csv_clean(os.path.join(data_dir, 'elcand.csv'))
    df_tks = read_csv_clean(os.path.join(data_dir, 'elctks.csv'))
    df_prof = read_csv_clean(os.path.join(data_dir, 'elprof.csv'))

    # 過濾指定縣市
    if city_code == '000':
        df_base = df_base[df_base[0] == prv_code]
        df_tks = df_tks[df_tks[0] == prv_code]
        df_prof = df_prof[df_prof[0] == prv_code]
    else:
        df_base = df_base[(df_base[0] == prv_code) & (df_base[1] == city_code)]
        df_tks = df_tks[(df_tks[0] == prv_code) & (df_tks[1] == city_code)]
        df_prof = df_prof[(df_prof[0] == prv_code) & (df_prof[1] == city_code)]

    # 政黨（候選人）是全國層級，不過濾
    df_cand_nat = df_cand

    if df_base.empty:
        print(f"  [SKIP] 政黨票 - 無 {city_name} 資料")
        return None

    # 建立區域名稱對照
    party_dist_map = {}
    party_village_map = {}
    for _, row in df_base.iterrows():
        dept, li, name = row[3], row[4], row[5]
        if li == '0000' or li == '0':
            party_dist_map[dept] = name
        else:
            party_village_map[f"{dept}_{li}"] = name

    # 建立政黨列表（政黨票的候選人其實是政黨）
    parties = []
    seen_nos = set()
    for _, row in df_cand_nat.iterrows():
        no = row[5]
        if no in seen_nos:
            continue
        seen_nos.add(no)
        # 政黨票的「候選人」其實是政黨名稱
        party_name = row[6] if pd.notna(row[6]) else get_party_name(row[7])
        parties.append({
            'no': no,
            'name': party_name,
            'party': party_name
        })

    parties = sorted(parties, key=lambda x: int(x['no']) if str(x['no']).isdigit() else 0)

    if not parties:
        return None

    # 建立統計資料對照 (使用村里彙總列 tbox=0)
    party_stats_by_village = {}
    for _, row in df_prof.iterrows():
        dept = row[3]
        li = row[4]
        tbox = row[5]

        if li == '0000' or li == '0':
            continue
        if tbox != '0' and tbox != '0000':
            continue

        key = f"{dept}_{li}"
        party_stats_by_village[key] = {
            '有效票數': clean_number(row[6]),
            '無效票數': clean_number(row[7]),
            '投票數': clean_number(row[8]),
            '選舉人數': clean_number(row[9]) if len(row) > 9 else 0,
        }

    # 計算衍生欄位
    for key in party_stats_by_village:
        stats = party_stats_by_village[key]
        total_votes = stats['投票數']
        eligible_voters = stats['選舉人數']
        stats['已領未投票數'] = 0
        stats['發出票數'] = total_votes
        stats['用餘票數'] = eligible_voters - total_votes if eligible_voters > total_votes else 0
        stats['投票率'] = round(total_votes / eligible_voters * 100, 2) if eligible_voters > 0 else 0

    # 使用村里彙總列的票數
    party_votes_by_village = defaultdict(lambda: defaultdict(int))
    for _, row in df_tks.iterrows():
        li = row[4]
        tbox = row[5]

        if li == '0000' or li == '0':
            continue
        if tbox != '0' and tbox != '0000':
            continue

        key = f"{row[3]}_{li}"
        party_votes_by_village[key][row[6]] = clean_number(row[7])

    if not party_votes_by_village:
        return None

    # 計算區級和總計彙總
    party_dept_totals = defaultdict(lambda: {'votes': defaultdict(int), 'stats': defaultdict(int)})
    party_grand_total = {'votes': defaultdict(int), 'stats': defaultdict(int)}

    for key in party_votes_by_village.keys():
        parts = key.split('_')
        dept = parts[0]

        votes_dict = party_votes_by_village[key]
        stats = party_stats_by_village.get(key, {})

        for party_no, votes in votes_dict.items():
            party_dept_totals[dept]['votes'][party_no] += votes
            party_grand_total['votes'][party_no] += votes

        for stat_key in ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數']:
            party_dept_totals[dept]['stats'][stat_key] += stats.get(stat_key, 0)
            party_grand_total['stats'][stat_key] += stats.get(stat_key, 0)

    # 計算投票率
    if party_grand_total['stats']['選舉人數'] > 0:
        party_grand_total['stats']['投票率'] = round(party_grand_total['stats']['投票數'] / party_grand_total['stats']['選舉人數'] * 100, 2)

    for dept in party_dept_totals:
        if party_dept_totals[dept]['stats']['選舉人數'] > 0:
            party_dept_totals[dept]['stats']['投票率'] = round(party_dept_totals[dept]['stats']['投票數'] / party_dept_totals[dept]['stats']['選舉人數'] * 100, 2)

    # 生成資料列
    party_rows = []
    current_dept = None

    for key in sorted(party_votes_by_village.keys(), key=lambda x: (x.split('_')[0], x.split('_')[1])):
        parts = key.split('_')
        dept, li = parts[0], parts[1]

        dist_name_val = party_dist_map.get(dept, dept)
        village_name = party_village_map.get(key, li)

        row_data = {
            '行政區別': dist_name_val if dept != current_dept else '',
            '村里別': village_name,
        }

        votes_dict = party_votes_by_village[key]
        for i, party in enumerate(parties):
            row_data[f'候選人{i+1}'] = votes_dict.get(party['no'], 0)

        stats = party_stats_by_village.get(key, {})
        row_data['有效票數'] = stats.get('有效票數', 0)
        row_data['無效票數'] = stats.get('無效票數', 0)
        row_data['投票數'] = stats.get('投票數', 0)
        row_data['已領未投票數'] = stats.get('已領未投票數', 0)
        row_data['發出票數'] = stats.get('發出票數', 0)
        row_data['用餘票數'] = stats.get('用餘票數', 0)
        row_data['選舉人數'] = stats.get('選舉人數', 0)
        row_data['投票率'] = stats.get('投票率', 0)

        party_rows.append(row_data)
        current_dept = dept

    if party_rows:
        return {
            'data': pd.DataFrame(party_rows),
            'candidates': parties,  # 使用 candidates 保持一致性
            'dept_totals': party_dept_totals,
            'grand_total': party_grand_total,
            'dist_map': party_dist_map,
        }

    return None
