# -*- coding: utf-8 -*-
"""
選舉資料處理系統 - 輸出函數
Output functions for election data processor

本模組提供選舉資料的 Excel 輸出功能。

新程式碼建議使用統一介面：
    from election_processor import save_election_excel, get_election_config

    election_type = get_election_config('president')
    save_election_excel(result, output_path, election_type, city_name)
"""

import os
import pandas as pd

from .config import ALL_CITIES, DATA_DIR, YEAR_FOLDERS
from .utils import clean_val
from .election_types import MAX_CANDIDATES, MERGE_CONFIGS, get_election_config


def build_area_code_map(city_name, years=None):
    """建立區域代碼映射表

    從 elbase.csv 讀取資料，建立 鄰里 -> 區域別代碼 的映射
    鄰里格式：行政區_里名（如：花蓮市_民立里）
    區域別代碼格式：11位數（如：10015010001）

    Args:
        city_name: 縣市名稱
        years: 年份列表，預設為 [2014, 2020]

    Returns:
        dict: {鄰里: 區域別代碼}
    """
    if years is None:
        years = [2014, 2020]

    # 取得縣市代碼
    prv_code = None
    city_code = None
    for prv, city, name in ALL_CITIES:
        if name == city_name:
            prv_code = prv
            city_code = city
            break

    if not prv_code:
        return {}

    area_code_map = {}

    for year in years:
        year_folder = YEAR_FOLDERS.get(year)
        if not year_folder:
            continue

        # 根據年份選擇資料夾
        if year == 2014:
            if city_code == '000':
                data_dirs = ['直轄市市長', '直轄市區域議員']
            else:
                data_dirs = ['縣市市長', '縣市區域議員']
        elif year == 2020:
            data_dirs = ['總統', '區域立委']
        else:
            continue

        for data_folder in data_dirs:
            elbase_path = os.path.join(DATA_DIR, year_folder, data_folder, 'elbase.csv')
            if not os.path.exists(elbase_path):
                continue

            try:
                df = pd.read_csv(elbase_path, header=None, dtype=str)

                # 先建立 dept -> dept_name 映射
                dept_name_map = {}
                for idx in range(len(df)):
                    row = df.iloc[idx]
                    row_prv = clean_val(row[0])
                    row_city = clean_val(row[1])

                    # 過濾指定縣市
                    if city_code == '000':
                        if row_prv != prv_code:
                            continue
                    else:
                        if row_prv != prv_code or row_city != city_code:
                            continue

                    dept = clean_val(row[3])
                    li = clean_val(row[4])
                    name_val = clean_val(row[5])

                    # 找彙總列建立 dept -> name 映射
                    if li == '0000' or li == '0':
                        dept_name_map[dept] = name_val

                # 再建立村里 -> 區域代碼映射
                for idx in range(len(df)):
                    row = df.iloc[idx]
                    row_prv = clean_val(row[0])
                    row_city = clean_val(row[1])

                    # 過濾指定縣市
                    if city_code == '000':
                        if row_prv != prv_code:
                            continue
                    else:
                        if row_prv != prv_code or row_city != city_code:
                            continue

                    dept = clean_val(row[3])
                    li = clean_val(row[4])
                    name_val = clean_val(row[5])

                    # 跳過彙總列
                    if li == '0000' or li == '0':
                        continue

                    # 建立區域別代碼（11位數）
                    # 注意：dept 可能是 3 位數（如 '010'），需截取前 2 位（如 '01'）
                    dept_2digit = dept[:2].zfill(2) if len(dept) >= 2 else dept.zfill(2)

                    if city_code == '000':
                        # 直轄市：省市代碼(2) + 鄉鎮區代碼(3) + 村里代碼(4) = 9位數，補至11位
                        area_code = f"{row_prv.zfill(2)}{dept.zfill(3)}{li.zfill(4)}00"
                    else:
                        # 縣市：省代碼(2) + 縣市代碼(3) + 鄉鎮區代碼(2) + 村里代碼(4) = 11位數
                        area_code = f"{row_prv.zfill(2)}{row_city.zfill(3)}{dept_2digit}{li.zfill(4)}"

                    # 取得行政區名稱，建立 鄰里 -> 區域別代碼 映射
                    # 鄰里格式：行政區_里名（如：花蓮市_民立里）
                    dept_name = dept_name_map.get(dept)
                    if dept_name:
                        linli = f"{dept_name}_{name_val}"
                        area_code_map[linli] = area_code

            except Exception as e:
                print(f"  [WARN] 無法讀取 {elbase_path}: {e}")

    return area_code_map


def save_election_excel(result, output_path, election_type, city_name):
    """統一選舉結果輸出入口

    根據 ElectionType 配置自動選擇輸出格式。

    Args:
        result: 處理結果（來自 process_election）
        output_path: 輸出檔案路徑
        election_type: ElectionType 配置物件
        city_name: 縣市名稱

    Returns:
        bool: 是否成功儲存

    Example:
        >>> from election_processor import save_election_excel, get_election_config
        >>> election_type = get_election_config('president')
        >>> save_election_excel(result, 'output/president.xlsx', election_type, '臺北市')
    """
    if not result:
        return False

    year = election_type.year
    election_name = election_type.name

    # 根據選舉類型選擇對應的儲存函數
    if election_type.key in ['council_municipality', 'council_county']:
        return save_council_excel(result, output_path, city_name, year, election_name)

    elif election_type.key in ['mayor_municipality', 'mayor_county']:
        return save_mayor_excel(result, output_path, city_name, year, election_name)

    elif election_type.key == 'township_mayor':
        return save_township_mayor_excel(result, output_path, city_name, year)

    elif election_type.key == 'president':
        return save_president_excel(result, output_path, city_name, year)

    elif election_type.key == 'legislator':
        return save_legislator_excel(result, output_path, city_name, year)

    elif election_type.key == 'mountain_legislator':
        return save_indigenous_legislator_excel(result, output_path, city_name, year, 'mountain')

    elif election_type.key == 'plain_legislator':
        return save_indigenous_legislator_excel(result, output_path, city_name, year, 'plain')

    elif election_type.key == 'party_vote':
        return save_party_vote_excel(result, output_path, city_name, year)

    else:
        print(f"  [WARN] 未知選舉類型: {election_type.key}")
        return False


def save_council_excel(results, output_path, city_name, year, election_name):
    """儲存縣市議員選舉結果為 Excel

    Args:
        results: 各選區的 DataFrame 和候選人資訊
        output_path: 輸出檔案路徑
        city_name: 縣市名稱
        year: 年份
        election_name: 選舉名稱
    """
    if not results:
        return

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for area, result in results.items():
            df = result['data']
            candidates = result['candidates']
            sheet_name = f'第{area}選舉區'

            # 定義欄位順序
            base_cols = ['行政區別', '村里別', '投開票所別']
            cand_cols = [f'候選人{i+1}' for i in range(len(candidates))]
            stat_cols = ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數', '投票率']

            output_rows = []

            # Row 0: Title
            title = f'{year}年{city_name}{election_name}第{area}選舉區候選人在各投開票所得票數一覽表'
            title_row = [title] + [''] * (len(base_cols) + len(cand_cols) + len(stat_cols) - 1)
            output_rows.append(title_row)

            # Row 1: Headers
            header_row = ['行政區別', '村里別', '投開票所別']
            header_row += ['各組候選人得票情形'] + [''] * (len(candidates) - 1)
            header_row += ['有效票數A\nA=1+2+...+N', '無效票數B', '投票數C\nC=A+B',
                          '已領未投票數\nD\nD=E-C', '發出票數E\nE=C+D', '用餘票數F',
                          '選舉人數G\nG=E+F', '投票率H\nH=C÷G']
            output_rows.append(header_row)

            # Row 2: Candidate info (包含政黨)
            cand_row = ['', '', '']
            for i, cand in enumerate(candidates):
                party = cand.get('party', '無黨籍') or '無黨籍'
                cand_row.append(f"({cand['no']})\n{cand['name']}\n{party}")
            cand_row += [''] * len(stat_cols)
            output_rows.append(cand_row)

            # Row 3-4: Empty
            empty_row = [''] * len(header_row)
            output_rows.append(empty_row)
            output_rows.append(empty_row)

            # Data rows
            for _, row in df.iterrows():
                data_row = [row['行政區別'], row['村里別'], row['投開票所別']]
                for i in range(len(candidates)):
                    data_row.append(row.get(f'候選人{i+1}', 0))
                data_row += [
                    row.get('有效票數', 0),
                    row.get('無效票數', 0),
                    row.get('投票數', 0),
                    row.get('已領未投票數', 0),
                    row.get('發出票數', 0),
                    row.get('用餘票數', 0),
                    row.get('選舉人數', 0),
                    row.get('投票率', 0),
                ]
                output_rows.append(data_row)

            output_df = pd.DataFrame(output_rows)
            output_df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)

    print(f"  已儲存: {output_path}")


def save_president_excel(result, output_path, city_name, year):
    """儲存總統選舉結果為 Excel (符合目標格式)

    Args:
        result: data DataFrame 和 candidates list
        output_path: 輸出檔案路徑
        city_name: 縣市名稱
        year: 年份

    Returns:
        output DataFrame or None
    """
    if not result:
        return None

    df = result['data']
    candidates = result['candidates']
    dept_totals = result.get('dept_totals', {})
    grand_total = result.get('grand_total', {})
    dist_map = result.get('dist_map', {})

    # 計算欄位數
    num_candidates = len(candidates)
    stat_cols = ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數', '投票率']

    output_rows = []

    # Row 0: Title
    title = f'第{(year - 1996) // 4 + 9}任總統副總統選舉候選人在{city_name}各村(里)得票數一覽表'
    title_row = [title] + [''] * (1 + num_candidates + len(stat_cols))
    output_rows.append(title_row)

    # Row 1: Headers
    header_row = ['行政區別', '村里別']
    header_row += ['各組候選人得票情形'] + [''] * (num_candidates - 1)
    header_row += ['有效票數A\nA=1+2+...+N', '無效票數B', '投票數C\nC=A+B',
                  '已領未投票數\nD\nD=E-C', '發出票數E\nE=C+D', '用餘票數F',
                  '選舉人數G\nG=E+F', '投票率H\nH=C÷G']
    output_rows.append(header_row)

    # Row 2: Candidate info (包含政黨)
    cand_row = ['', '']
    for i, cand in enumerate(candidates):
        party = cand.get('party', '無黨籍') or '無黨籍'
        cand_row.append(f"({cand['no']})\n{cand['name']}\n{party}")
    cand_row += [''] * len(stat_cols)
    output_rows.append(cand_row)

    # Row 3-4: Empty
    empty_row = [''] * len(header_row)
    output_rows.append(empty_row)
    output_rows.append(empty_row)

    # Row 5: Grand total (總計)
    total_row = ['總　計', '']
    for cand in candidates:
        total_row.append(grand_total['votes'].get(cand['no'], 0))
    total_row += [
        grand_total['stats'].get('有效票數', 0),
        grand_total['stats'].get('無效票數', 0),
        grand_total['stats'].get('投票數', 0),
        grand_total['stats'].get('已領未投票數', 0),
        grand_total['stats'].get('發出票數', 0),
        grand_total['stats'].get('用餘票數', 0),
        grand_total['stats'].get('選舉人數', 0),
        grand_total['stats'].get('投票率', 0),
    ]
    output_rows.append(total_row)

    # Data rows with district subtotals
    current_dept = None
    for _, row in df.iterrows():
        dept = None
        for d, name in dist_map.items():
            if row['行政區別'] == name or (row['行政區別'] == '' and current_dept):
                dept = d if row['行政區別'] != '' else current_dept
                break

        # Add district subtotal before first village of new district
        if row['行政區別'] != '' and row['行政區別'] != current_dept:
            if dept and dept in dept_totals:
                dist_name = dist_map.get(dept, dept)
                # 區級小計行
                dept_row = [f'　{dist_name}', '']
                for cand in candidates:
                    dept_row.append(dept_totals[dept]['votes'].get(cand['no'], 0))
                dept_row += [
                    dept_totals[dept]['stats'].get('有效票數', 0),
                    dept_totals[dept]['stats'].get('無效票數', 0),
                    dept_totals[dept]['stats'].get('投票數', 0),
                    dept_totals[dept]['stats'].get('已領未投票數', 0),
                    dept_totals[dept]['stats'].get('發出票數', 0),
                    dept_totals[dept]['stats'].get('用餘票數', 0),
                    dept_totals[dept]['stats'].get('選舉人數', 0),
                    dept_totals[dept]['stats'].get('投票率', 0),
                ]
                output_rows.append(dept_row)
            current_dept = row['行政區別']

        # Village data row
        data_row = ['', row['村里別']]
        for i in range(num_candidates):
            data_row.append(row.get(f'候選人{i+1}', 0))
        data_row += [
            row.get('有效票數', 0),
            row.get('無效票數', 0),
            row.get('投票數', 0),
            row.get('已領未投票數', 0),
            row.get('發出票數', 0),
            row.get('用餘票數', 0),
            row.get('選舉人數', 0),
            row.get('投票率', 0),
        ]
        output_rows.append(data_row)

    output_df = pd.DataFrame(output_rows)
    output_df.to_excel(output_path, index=False, header=False, engine='openpyxl', sheet_name=city_name)
    print(f"  已儲存: {output_path}")

    return output_df


def save_mayor_excel(result, output_path, city_name, year, election_name):
    """儲存縣市首長選舉結果為 Excel

    Args:
        result: data DataFrame 和 candidates list
        output_path: 輸出檔案路徑
        city_name: 縣市名稱
        year: 年份
        election_name: 選舉名稱
    """
    if not result:
        return None

    df = result['data']
    candidates = result['candidates']
    dept_totals = result.get('dept_totals', {})
    grand_total = result.get('grand_total', {})
    dist_map = result.get('dist_map', {})

    num_candidates = len(candidates)
    stat_cols = ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數', '投票率']

    output_rows = []

    # Row 0: Title
    title = f'{year}年{city_name}{election_name}候選人在各村(里)得票數一覽表'
    title_row = [title] + [''] * (1 + num_candidates + len(stat_cols))
    output_rows.append(title_row)

    # Row 1: Headers
    header_row = ['行政區別', '村里別']
    header_row += ['各組候選人得票情形'] + [''] * (num_candidates - 1)
    header_row += ['有效票數A\nA=1+2+...+N', '無效票數B', '投票數C\nC=A+B',
                  '已領未投票數\nD\nD=E-C', '發出票數E\nE=C+D', '用餘票數F',
                  '選舉人數G\nG=E+F', '投票率H\nH=C÷G']
    output_rows.append(header_row)

    # Row 2: Candidate info (包含政黨)
    cand_row = ['', '']
    for i, cand in enumerate(candidates):
        party = cand.get('party', '無黨籍') or '無黨籍'
        cand_row.append(f"({cand['no']})\n{cand['name']}\n{party}")
    cand_row += [''] * len(stat_cols)
    output_rows.append(cand_row)

    # Row 3-4: Empty
    empty_row = [''] * len(header_row)
    output_rows.append(empty_row)
    output_rows.append(empty_row)

    # Row 5: Grand total (總計)
    total_row = ['總　計', '']
    for cand in candidates:
        total_row.append(grand_total['votes'].get(cand['no'], 0))
    total_row += [
        grand_total['stats'].get('有效票數', 0),
        grand_total['stats'].get('無效票數', 0),
        grand_total['stats'].get('投票數', 0),
        grand_total['stats'].get('已領未投票數', 0),
        grand_total['stats'].get('發出票數', 0),
        grand_total['stats'].get('用餘票數', 0),
        grand_total['stats'].get('選舉人數', 0),
        grand_total['stats'].get('投票率', 0),
    ]
    output_rows.append(total_row)

    # Data rows with district subtotals
    current_dept = None
    for _, row in df.iterrows():
        dept = None
        for d, name in dist_map.items():
            if row['行政區別'] == name or (row['行政區別'] == '' and current_dept):
                dept = d if row['行政區別'] != '' else current_dept
                break

        # Add district subtotal before first village of new district
        if row['行政區別'] != '' and row['行政區別'] != current_dept:
            if dept and dept in dept_totals:
                dist_name = dist_map.get(dept, dept)
                dept_row = [f'　{dist_name}', '']
                for cand in candidates:
                    dept_row.append(dept_totals[dept]['votes'].get(cand['no'], 0))
                dept_row += [
                    dept_totals[dept]['stats'].get('有效票數', 0),
                    dept_totals[dept]['stats'].get('無效票數', 0),
                    dept_totals[dept]['stats'].get('投票數', 0),
                    dept_totals[dept]['stats'].get('已領未投票數', 0),
                    dept_totals[dept]['stats'].get('發出票數', 0),
                    dept_totals[dept]['stats'].get('用餘票數', 0),
                    dept_totals[dept]['stats'].get('選舉人數', 0),
                    dept_totals[dept]['stats'].get('投票率', 0),
                ]
                output_rows.append(dept_row)
            current_dept = row['行政區別']

        # Village data row
        data_row = ['', row['村里別']]
        for i in range(num_candidates):
            data_row.append(row.get(f'候選人{i+1}', 0))
        data_row += [
            row.get('有效票數', 0),
            row.get('無效票數', 0),
            row.get('投票數', 0),
            row.get('已領未投票數', 0),
            row.get('發出票數', 0),
            row.get('用餘票數', 0),
            row.get('選舉人數', 0),
            row.get('投票率', 0),
        ]
        output_rows.append(data_row)

    output_df = pd.DataFrame(output_rows)
    output_df.to_excel(output_path, index=False, header=False, engine='openpyxl', sheet_name=city_name)
    print(f"  已儲存: {output_path}")

    return output_df


def save_legislator_excel(results, output_path, city_name, year):
    """儲存區域立委選舉結果為 Excel (多工作表)

    Args:
        results: 各選區的 DataFrame 和候選人資訊
        output_path: 輸出檔案路徑
        city_name: 縣市名稱
        year: 年份
    """
    if not results:
        return

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for area, result in results.items():
            df = result['data']
            candidates = result['candidates']
            dept_totals = result.get('dept_totals', {})
            grand_total = result.get('grand_total', {})
            dist_map = result.get('dist_map', {})

            sheet_name = f'第{area}選舉區'
            num_candidates = len(candidates)
            stat_cols = ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數', '投票率']

            output_rows = []

            # Row 0: Title
            title = f'{year}年{city_name}第{area}選舉區區域立法委員候選人在各村(里)得票數一覽表'
            title_row = [title] + [''] * (1 + num_candidates + len(stat_cols))
            output_rows.append(title_row)

            # Row 1: Headers
            header_row = ['行政區別', '村里別']
            header_row += ['各組候選人得票情形'] + [''] * (num_candidates - 1)
            header_row += ['有效票數A\nA=1+2+...+N', '無效票數B', '投票數C\nC=A+B',
                          '已領未投票數\nD\nD=E-C', '發出票數E\nE=C+D', '用餘票數F',
                          '選舉人數G\nG=E+F', '投票率H\nH=C÷G']
            output_rows.append(header_row)

            # Row 2: Candidate info (包含政黨)
            cand_row = ['', '']
            for i, cand in enumerate(candidates):
                party = cand.get('party', '無黨籍') or '無黨籍'
                cand_row.append(f"({cand['no']})\n{cand['name']}\n{party}")
            cand_row += [''] * len(stat_cols)
            output_rows.append(cand_row)

            # Row 3-4: Empty
            empty_row = [''] * len(header_row)
            output_rows.append(empty_row)
            output_rows.append(empty_row)

            # Row 5: Grand total (總計)
            total_row = ['總　計', '']
            for cand in candidates:
                total_row.append(grand_total['votes'].get(cand['no'], 0))
            total_row += [
                grand_total['stats'].get('有效票數', 0),
                grand_total['stats'].get('無效票數', 0),
                grand_total['stats'].get('投票數', 0),
                grand_total['stats'].get('已領未投票數', 0),
                grand_total['stats'].get('發出票數', 0),
                grand_total['stats'].get('用餘票數', 0),
                grand_total['stats'].get('選舉人數', 0),
                grand_total['stats'].get('投票率', 0),
            ]
            output_rows.append(total_row)

            # Data rows with district subtotals
            current_dept = None
            for _, row in df.iterrows():
                dept = None
                for d, name in dist_map.items():
                    if row['行政區別'] == name or (row['行政區別'] == '' and current_dept):
                        dept = d if row['行政區別'] != '' else current_dept
                        break

                # Add district subtotal before first village of new district
                if row['行政區別'] != '' and row['行政區別'] != current_dept:
                    if dept and dept in dept_totals:
                        dist_name = dist_map.get(dept, dept)
                        dept_row = [f'　{dist_name}', '']
                        for cand in candidates:
                            dept_row.append(dept_totals[dept]['votes'].get(cand['no'], 0))
                        dept_row += [
                            dept_totals[dept]['stats'].get('有效票數', 0),
                            dept_totals[dept]['stats'].get('無效票數', 0),
                            dept_totals[dept]['stats'].get('投票數', 0),
                            dept_totals[dept]['stats'].get('已領未投票數', 0),
                            dept_totals[dept]['stats'].get('發出票數', 0),
                            dept_totals[dept]['stats'].get('用餘票數', 0),
                            dept_totals[dept]['stats'].get('選舉人數', 0),
                            dept_totals[dept]['stats'].get('投票率', 0),
                        ]
                        output_rows.append(dept_row)
                    current_dept = row['行政區別']

                # Village data row
                data_row = ['', row['村里別']]
                for i in range(num_candidates):
                    data_row.append(row.get(f'候選人{i+1}', 0))
                data_row += [
                    row.get('有效票數', 0),
                    row.get('無效票數', 0),
                    row.get('投票數', 0),
                    row.get('已領未投票數', 0),
                    row.get('發出票數', 0),
                    row.get('用餘票數', 0),
                    row.get('選舉人數', 0),
                    row.get('投票率', 0),
                ]
                output_rows.append(data_row)

            output_df = pd.DataFrame(output_rows)
            output_df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)

    print(f"  已儲存: {output_path}")


def save_township_mayor_excel(results, output_path, city_name, year):
    """儲存鄉鎮市長選舉結果為 Excel (多工作表)

    Args:
        results: 各鄉鎮市的 DataFrame 和候選人資訊
        output_path: 輸出檔案路徑
        city_name: 縣市名稱
        year: 年份
    """
    if not results:
        return

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for area, result in results.items():
            df = result['data']
            candidates = result['candidates']
            dept_totals = result.get('dept_totals', {})
            grand_total = result.get('grand_total', {})
            dist_map = result.get('dist_map', {})
            area_name = result.get('area_name', f'第{area}鄉鎮市')

            sheet_name = area_name[:31] if area_name else f'區域{area}'
            num_candidates = len(candidates)
            stat_cols = ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數', '投票率']

            output_rows = []

            # Row 0: Title
            title = f'{year}年{city_name}{area_name}鄉鎮市長候選人在各村(里)得票數一覽表'
            title_row = [title] + [''] * (1 + num_candidates + len(stat_cols))
            output_rows.append(title_row)

            # Row 1: Headers
            header_row = ['行政區別', '村里別']
            header_row += ['各組候選人得票情形'] + [''] * (num_candidates - 1) if num_candidates > 0 else ['各組候選人得票情形']
            header_row += ['有效票數A\nA=1+2+...+N', '無效票數B', '投票數C\nC=A+B',
                          '已領未投票數\nD\nD=E-C', '發出票數E\nE=C+D', '用餘票數F',
                          '選舉人數G\nG=E+F', '投票率H\nH=C÷G']
            output_rows.append(header_row)

            # Row 2: Candidate info
            cand_row = ['', '']
            for i, cand in enumerate(candidates):
                party = cand.get('party', '無黨籍') or '無黨籍'
                cand_row.append(f"({cand['no']})\n{cand['name']}\n{party}")
            cand_row += [''] * len(stat_cols)
            output_rows.append(cand_row)

            # Row 3-4: Empty
            empty_row = [''] * len(header_row)
            output_rows.append(empty_row)
            output_rows.append(empty_row)

            # Row 5: Grand total
            total_row = ['總　計', '']
            for cand in candidates:
                total_row.append(grand_total['votes'].get(cand['no'], 0))
            total_row += [
                grand_total['stats'].get('有效票數', 0),
                grand_total['stats'].get('無效票數', 0),
                grand_total['stats'].get('投票數', 0),
                grand_total['stats'].get('已領未投票數', 0),
                grand_total['stats'].get('發出票數', 0),
                grand_total['stats'].get('用餘票數', 0),
                grand_total['stats'].get('選舉人數', 0),
                grand_total['stats'].get('投票率', 0),
            ]
            output_rows.append(total_row)

            # Data rows
            current_dept = None
            for _, row in df.iterrows():
                data_row = ['', row['村里別']]
                for i in range(num_candidates):
                    data_row.append(row.get(f'候選人{i+1}', 0))
                data_row += [
                    row.get('有效票數', 0),
                    row.get('無效票數', 0),
                    row.get('投票數', 0),
                    row.get('已領未投票數', 0),
                    row.get('發出票數', 0),
                    row.get('用餘票數', 0),
                    row.get('選舉人數', 0),
                    row.get('投票率', 0),
                ]
                output_rows.append(data_row)

            output_df = pd.DataFrame(output_rows)
            output_df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)

    print(f"  已儲存: {output_path}")


def save_indigenous_legislator_excel(result, output_path, city_name, year, legislator_type='mountain'):
    """儲存原住民立委選舉結果為 Excel

    Args:
        result: data DataFrame 和 candidates list
        output_path: 輸出檔案路徑
        city_name: 縣市名稱
        year: 年份
        legislator_type: 'mountain' 或 'plain'
    """
    if not result:
        return None

    df = result['data']
    candidates = result['candidates']
    dept_totals = result.get('dept_totals', {})
    grand_total = result.get('grand_total', {})
    dist_map = result.get('dist_map', {})

    type_name = '山地' if legislator_type == 'mountain' else '平地'
    num_candidates = len(candidates)
    stat_cols = ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數', '投票率']

    output_rows = []

    # Row 0: Title
    title = f'{year}年{type_name}原住民立法委員候選人在{city_name}各村(里)得票數一覽表'
    title_row = [title] + [''] * (1 + num_candidates + len(stat_cols))
    output_rows.append(title_row)

    # Row 1: Headers
    header_row = ['行政區別', '村里別']
    header_row += ['各組候選人得票情形'] + [''] * (num_candidates - 1) if num_candidates > 0 else ['各組候選人得票情形']
    header_row += ['有效票數A\nA=1+2+...+N', '無效票數B', '投票數C\nC=A+B',
                  '已領未投票數\nD\nD=E-C', '發出票數E\nE=C+D', '用餘票數F',
                  '選舉人數G\nG=E+F', '投票率H\nH=C÷G']
    output_rows.append(header_row)

    # Row 2: Candidate info
    cand_row = ['', '']
    for i, cand in enumerate(candidates):
        party = cand.get('party', '無黨籍') or '無黨籍'
        cand_row.append(f"({cand['no']})\n{cand['name']}\n{party}")
    cand_row += [''] * len(stat_cols)
    output_rows.append(cand_row)

    # Row 3-4: Empty
    empty_row = [''] * len(header_row)
    output_rows.append(empty_row)
    output_rows.append(empty_row)

    # Row 5: Grand total
    total_row = ['總　計', '']
    for cand in candidates:
        total_row.append(grand_total['votes'].get(cand['no'], 0))
    total_row += [
        grand_total['stats'].get('有效票數', 0),
        grand_total['stats'].get('無效票數', 0),
        grand_total['stats'].get('投票數', 0),
        grand_total['stats'].get('已領未投票數', 0),
        grand_total['stats'].get('發出票數', 0),
        grand_total['stats'].get('用餘票數', 0),
        grand_total['stats'].get('選舉人數', 0),
        grand_total['stats'].get('投票率', 0),
    ]
    output_rows.append(total_row)

    # Data rows with district subtotals
    current_dept = None
    for _, row in df.iterrows():
        dept = None
        for d, name in dist_map.items():
            if row['行政區別'] == name or (row['行政區別'] == '' and current_dept):
                dept = d if row['行政區別'] != '' else current_dept
                break

        if row['行政區別'] != '' and row['行政區別'] != current_dept:
            if dept and dept in dept_totals:
                dist_name = dist_map.get(dept, dept)
                dept_row = [f'　{dist_name}', '']
                for cand in candidates:
                    dept_row.append(dept_totals[dept]['votes'].get(cand['no'], 0))
                dept_row += [
                    dept_totals[dept]['stats'].get('有效票數', 0),
                    dept_totals[dept]['stats'].get('無效票數', 0),
                    dept_totals[dept]['stats'].get('投票數', 0),
                    dept_totals[dept]['stats'].get('已領未投票數', 0),
                    dept_totals[dept]['stats'].get('發出票數', 0),
                    dept_totals[dept]['stats'].get('用餘票數', 0),
                    dept_totals[dept]['stats'].get('選舉人數', 0),
                    dept_totals[dept]['stats'].get('投票率', 0),
                ]
                output_rows.append(dept_row)
            current_dept = row['行政區別']

        data_row = ['', row['村里別']]
        for i in range(num_candidates):
            data_row.append(row.get(f'候選人{i+1}', 0))
        data_row += [
            row.get('有效票數', 0),
            row.get('無效票數', 0),
            row.get('投票數', 0),
            row.get('已領未投票數', 0),
            row.get('發出票數', 0),
            row.get('用餘票數', 0),
            row.get('選舉人數', 0),
            row.get('投票率', 0),
        ]
        output_rows.append(data_row)

    output_df = pd.DataFrame(output_rows)
    output_df.to_excel(output_path, index=False, header=False, engine='openpyxl', sheet_name=city_name)
    print(f"  已儲存: {output_path}")

    return output_df


def save_party_vote_excel(result, output_path, city_name, year):
    """儲存政黨票選舉結果為 Excel

    Args:
        result: data DataFrame 和 parties list
        output_path: 輸出檔案路徑
        city_name: 縣市名稱
        year: 年份
    """
    if not result:
        return None

    df = result['data']
    parties = result['candidates']  # 這裡是政黨列表
    dept_totals = result.get('dept_totals', {})
    grand_total = result.get('grand_total', {})
    dist_map = result.get('dist_map', {})

    num_parties = len(parties)
    stat_cols = ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數', '投票率']

    output_rows = []

    # Row 0: Title
    title = f'{year}年不分區政黨票在{city_name}各村(里)得票數一覽表'
    title_row = [title] + [''] * (1 + num_parties + len(stat_cols))
    output_rows.append(title_row)

    # Row 1: Headers
    header_row = ['行政區別', '村里別']
    header_row += ['各政黨得票情形'] + [''] * (num_parties - 1) if num_parties > 0 else ['各政黨得票情形']
    header_row += ['有效票數A\nA=1+2+...+N', '無效票數B', '投票數C\nC=A+B',
                  '已領未投票數\nD\nD=E-C', '發出票數E\nE=C+D', '用餘票數F',
                  '選舉人數G\nG=E+F', '投票率H\nH=C÷G']
    output_rows.append(header_row)

    # Row 2: Party info
    party_row = ['', '']
    for i, party in enumerate(parties):
        party_row.append(f"({party['no']})\n{party['name']}")
    party_row += [''] * len(stat_cols)
    output_rows.append(party_row)

    # Row 3-4: Empty
    empty_row = [''] * len(header_row)
    output_rows.append(empty_row)
    output_rows.append(empty_row)

    # Row 5: Grand total
    total_row = ['總　計', '']
    for party in parties:
        total_row.append(grand_total['votes'].get(party['no'], 0))
    total_row += [
        grand_total['stats'].get('有效票數', 0),
        grand_total['stats'].get('無效票數', 0),
        grand_total['stats'].get('投票數', 0),
        grand_total['stats'].get('已領未投票數', 0),
        grand_total['stats'].get('發出票數', 0),
        grand_total['stats'].get('用餘票數', 0),
        grand_total['stats'].get('選舉人數', 0),
        grand_total['stats'].get('投票率', 0),
    ]
    output_rows.append(total_row)

    # Data rows with district subtotals
    current_dept = None
    for _, row in df.iterrows():
        dept = None
        for d, name in dist_map.items():
            if row['行政區別'] == name or (row['行政區別'] == '' and current_dept):
                dept = d if row['行政區別'] != '' else current_dept
                break

        if row['行政區別'] != '' and row['行政區別'] != current_dept:
            if dept and dept in dept_totals:
                dist_name = dist_map.get(dept, dept)
                dept_row = [f'　{dist_name}', '']
                for party in parties:
                    dept_row.append(dept_totals[dept]['votes'].get(party['no'], 0))
                dept_row += [
                    dept_totals[dept]['stats'].get('有效票數', 0),
                    dept_totals[dept]['stats'].get('無效票數', 0),
                    dept_totals[dept]['stats'].get('投票數', 0),
                    dept_totals[dept]['stats'].get('已領未投票數', 0),
                    dept_totals[dept]['stats'].get('發出票數', 0),
                    dept_totals[dept]['stats'].get('用餘票數', 0),
                    dept_totals[dept]['stats'].get('選舉人數', 0),
                    dept_totals[dept]['stats'].get('投票率', 0),
                ]
                output_rows.append(dept_row)
            current_dept = row['行政區別']

        data_row = ['', row['村里別']]
        for i in range(num_parties):
            data_row.append(row.get(f'候選人{i+1}', 0))
        data_row += [
            row.get('有效票數', 0),
            row.get('無效票數', 0),
            row.get('投票數', 0),
            row.get('已領未投票數', 0),
            row.get('發出票數', 0),
            row.get('用餘票數', 0),
            row.get('選舉人數', 0),
            row.get('投票率', 0),
        ]
        output_rows.append(data_row)

    output_df = pd.DataFrame(output_rows)
    output_df.to_excel(output_path, index=False, header=False, engine='openpyxl', sheet_name=city_name)
    print(f"  已儲存: {output_path}")

    return output_df


def create_national_election_file(output_dir, year, cities=None):
    """建立全國單一年份的選舉合併檔案（每個鄰里一列，不同選舉類型水平展開）

    Args:
        output_dir: 輸出目錄
        year: 年份（2014 或 2020）
        cities: 縣市列表，預設使用 ALL_CITIES

    Returns:
        DataFrame or None
    """
    if cities is None:
        cities = ALL_CITIES

    output_path = os.path.join(output_dir, f'全國{year}選舉.xlsx')

    # 如果檔案已存在，先刪除
    if os.path.exists(output_path):
        os.remove(output_path)
        print(f"  已刪除舊檔案: {output_path}")

    # 取得該年份的選舉類型配置
    election_configs = MERGE_CONFIGS.get(year)
    if not election_configs:
        print(f"  [WARN] 不支援的年份: {year}")
        return None

    # 定義每種選舉類型的最大候選人數
    if year == 2014:
        ELECTION_MAX_CANDIDATES = {
            'council': 30,      # 議員選舉
            'mayor': 10,        # 市長選舉
            'township_mayor': 10,  # 鄉鎮市長選舉
        }
    else:  # 2020
        ELECTION_MAX_CANDIDATES = {
            'president': 5,           # 總統選舉
            'legislator': 15,         # 區域立委選舉
            'mountain_legislator': 10,  # 山地原住民立委
            'plain_legislator': 10,   # 平地原住民立委
            'party_vote': 20,         # 政黨票
        }

    # 用字典收集資料，key = (縣市, 鄰里)
    village_data = {}

    # 遍歷所有縣市
    for prv_code, city_code, city_name in cities:
        print(f"  讀取 {city_name}...")
        city_output_dir = os.path.join(output_dir, city_name)

        for election_type, election_name in election_configs:
            max_cand = ELECTION_MAX_CANDIDATES.get(election_type, 10)

            if election_type == 'council':
                if city_code == '000':
                    file_path = os.path.join(city_output_dir, f'{year}_直轄市區域議員_各投開票所得票數_{city_name}.xlsx')
                else:
                    file_path = os.path.join(city_output_dir, f'{year}_縣市區域議員_各投開票所得票數_{city_name}.xlsx')

                if os.path.exists(file_path):
                    xl = pd.ExcelFile(file_path)
                    for sheet_name in xl.sheet_names:
                        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                        rows = _extract_election_data(df, year, election_name, city_name, sheet_name, max_cand)
                        for row in rows:
                            key = (row[2], row[4])  # (縣市, 鄰里)
                            if key not in village_data:
                                village_data[key] = {'base': row[:5]}  # 時間, 選舉名稱, 縣市, 行政區別, 鄰里
                            village_data[key][election_type] = row

            elif election_type == 'mayor':
                if city_code == '000':
                    file_path = os.path.join(city_output_dir, f'{year}_直轄市市長_各村里得票數_{city_name}.xlsx')
                else:
                    file_path = os.path.join(city_output_dir, f'{year}_縣市市長_各村里得票數_{city_name}.xlsx')

                if os.path.exists(file_path):
                    df = pd.read_excel(file_path, header=None)
                    rows = _extract_election_data(df, year, election_name, city_name, None, max_cand)
                    for row in rows:
                        key = (row[2], row[4])
                        if key not in village_data:
                            village_data[key] = {'base': row[:5]}
                        village_data[key][election_type] = row

            elif election_type == 'president':
                file_path = os.path.join(city_output_dir, f'{year}_總統候選人得票數一覽表_各村里_{city_name}.xlsx')

                if os.path.exists(file_path):
                    df = pd.read_excel(file_path, header=None)
                    rows = _extract_election_data(df, year, election_name, city_name, None, max_cand)
                    for row in rows:
                        key = (row[2], row[4])
                        if key not in village_data:
                            village_data[key] = {'base': row[:5]}
                        village_data[key][election_type] = row

            elif election_type == 'legislator':
                file_path = os.path.join(city_output_dir, f'{year}_區域立委_各村里得票數_{city_name}.xlsx')

                if os.path.exists(file_path):
                    xl = pd.ExcelFile(file_path)
                    for sheet_name in xl.sheet_names:
                        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                        rows = _extract_election_data(df, year, election_name, city_name, sheet_name, max_cand, is_legislator=True)
                        for row in rows:
                            key = (row[2], row[4])
                            if key not in village_data:
                                village_data[key] = {'base': row[:5]}
                            village_data[key][election_type] = row

            elif election_type == 'township_mayor':
                file_path = os.path.join(city_output_dir, f'{year}_鄉鎮市長_各村里得票數_{city_name}.xlsx')

                if os.path.exists(file_path):
                    xl = pd.ExcelFile(file_path)
                    for sheet_name in xl.sheet_names:
                        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                        rows = _extract_election_data(df, year, election_name, city_name, sheet_name, max_cand, is_township_mayor=True)
                        for row in rows:
                            key = (row[2], row[4])
                            if key not in village_data:
                                village_data[key] = {'base': row[:5]}
                            village_data[key][election_type] = row

            elif election_type == 'mountain_legislator':
                file_path = os.path.join(city_output_dir, f'{year}_山地原住民立委_各村里得票數_{city_name}.xlsx')

                if os.path.exists(file_path):
                    df = pd.read_excel(file_path, header=None)
                    rows = _extract_election_data(df, year, election_name, city_name, None, max_cand)
                    for row in rows:
                        key = (row[2], row[4])
                        if key not in village_data:
                            village_data[key] = {'base': row[:5]}
                        village_data[key][election_type] = row

            elif election_type == 'plain_legislator':
                file_path = os.path.join(city_output_dir, f'{year}_平地原住民立委_各村里得票數_{city_name}.xlsx')

                if os.path.exists(file_path):
                    df = pd.read_excel(file_path, header=None)
                    rows = _extract_election_data(df, year, election_name, city_name, None, max_cand)
                    for row in rows:
                        key = (row[2], row[4])
                        if key not in village_data:
                            village_data[key] = {'base': row[:5]}
                        village_data[key][election_type] = row

            elif election_type == 'party_vote':
                file_path = os.path.join(city_output_dir, f'{year}_政黨票_各村里得票數_{city_name}.xlsx')

                if os.path.exists(file_path):
                    df = pd.read_excel(file_path, header=None)
                    rows = _extract_election_data(df, year, election_name, city_name, None, max_cand)
                    for row in rows:
                        key = (row[2], row[4])
                        if key not in village_data:
                            village_data[key] = {'base': row[:5]}
                        village_data[key][election_type] = row

    if village_data:
        print(f"  共收集 {len(village_data)} 個鄰里資料")

        # 建立欄位名稱
        columns = ['時間', '縣市', '行政區別', '鄰里', '區域別代碼']

        # 為每個選舉類型添加欄位
        for election_type, election_name in election_configs:
            max_cand = ELECTION_MAX_CANDIDATES.get(election_type, 10)
            prefix = election_name.replace('選舉', '')

            # 選區欄位（議員、鄉鎮市長、區域立委）
            if election_type in ['council', 'township_mayor', 'legislator']:
                columns.append(f'{prefix}_選區')

            # 候選人欄位
            for i in range(1, max_cand + 1):
                columns.extend([
                    f'{prefix}_候選人{i}',
                    f'{prefix}_政黨{i}',
                    f'{prefix}_得票數{i}',
                    f'{prefix}_得票率{i}'
                ])

            # 統計欄位
            columns.extend([
                f'{prefix}_有效票數',
                f'{prefix}_無效票數',
                f'{prefix}_投票數',
                f'{prefix}_已領未投票數',
                f'{prefix}_發出票數',
                f'{prefix}_用餘票數',
                f'{prefix}_選舉人數',
                f'{prefix}_投票率'
            ])

        # 建立輸出資料
        all_rows = []
        for (city_name, linli), data in sorted(village_data.items()):
            base = data.get('base', [year, '', city_name, '', linli])
            # 跳過鄰里為空的資料
            if not linli or linli == '':
                continue

            # 基本欄位：時間, 縣市, 行政區別, 鄰里, 區域別代碼
            row = [base[0], base[2], base[3], base[4], '']  # 區域別代碼稍後填入

            # 為每個選舉類型添加資料
            for election_type, election_name in election_configs:
                max_cand = ELECTION_MAX_CANDIDATES.get(election_type, 10)
                election_row = data.get(election_type)

                if election_row:
                    # 原始資料格式：時間, 選舉名稱, 縣市, 行政區別, 鄰里, 區域別代碼, 選區, [候選人*4]*N, 統計欄位*8
                    # 選區
                    if election_type in ['council', 'township_mayor', 'legislator']:
                        row.append(election_row[6] if len(election_row) > 6 else '')

                    # 候選人資料（從 index 7 開始，每4個一組）
                    cand_start = 7
                    for i in range(max_cand):
                        idx = cand_start + i * 4
                        if idx + 3 < len(election_row):
                            row.extend([
                                election_row[idx],      # 候選人姓名
                                election_row[idx + 1],  # 政黨
                                election_row[idx + 2],  # 得票數
                                election_row[idx + 3]   # 得票率
                            ])
                        else:
                            row.extend([None, None, None, None])

                    # 統計欄位（在候選人之後）
                    stat_start = cand_start + max_cand * 4
                    for i in range(8):
                        idx = stat_start + i
                        row.append(election_row[idx] if idx < len(election_row) else 0)
                else:
                    # 沒有這個選舉類型的資料
                    if election_type in ['council', 'township_mayor', 'legislator']:
                        row.append('')  # 選區

                    # 空的候選人資料
                    for i in range(max_cand):
                        row.extend([None, None, None, None])

                    # 空的統計欄位
                    for i in range(8):
                        row.append(None)

            all_rows.append(row)

        result_df = pd.DataFrame(all_rows, columns=columns)
        print(f"  共 {len(result_df)} 筆資料")

        # 刪除空的候選人欄位（整欄都是空的）
        cols_to_drop = []
        for col in result_df.columns:
            if '_候選人' in col:
                if result_df[col].isna().all() or (result_df[col] == '').all():
                    # 找到對應的政黨、得票數、得票率欄位
                    base_col = col.replace('_候選人', '')
                    suffix = col.split('_候選人')[1]
                    cols_to_drop.extend([
                        col,
                        f'{base_col}_政黨{suffix}',
                        f'{base_col}_得票數{suffix}',
                        f'{base_col}_得票率{suffix}'
                    ])
        if cols_to_drop:
            result_df = result_df.drop(columns=[c for c in cols_to_drop if c in result_df.columns])
            print(f"  刪除空的候選人欄位: {len(cols_to_drop) // 4} 組")

        # 建立並填入區域別代碼
        print("  建立區域別代碼映射...")
        all_area_code_map = {}
        for prv_code, city_code, city_name in cities:
            area_code_map = build_area_code_map(city_name, [year])
            all_area_code_map.update(area_code_map)

        if all_area_code_map:
            def get_area_code(row):
                linli = row['鄰里']
                return all_area_code_map.get(linli, '')
            result_df['區域別代碼'] = result_df.apply(get_area_code, axis=1)
            filled_count = (result_df['區域別代碼'] != '').sum()
            print(f"  填入區域別代碼: {filled_count}/{len(result_df)} 筆")

        result_df.to_excel(output_path, index=False, engine='openpyxl', sheet_name=f'全國{year}選舉資料')
        print(f"  已儲存: {output_path}")

        # 同時輸出 CSV 檔案（標準 UTF-8 編碼）
        csv_path = os.path.join(output_dir, f'全國{year}選舉.csv')
        if os.path.exists(csv_path):
            os.remove(csv_path)
            print(f"  已刪除舊 CSV 檔案: {csv_path}")
        result_df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"  已儲存: {csv_path}")

        print(f"  總筆數: {len(result_df)}")
        return result_df

    return None


def create_city_combined_file(output_dir, city_name, city_code, years=None, cities=None):
    """建立單一縣市的合併檔案（多年度、多選舉類型合併，格式參考苗栗縣選舉整理_完成版.xlsx）

    Args:
        output_dir: 輸出目錄
        city_name: 縣市名稱
        city_code: 縣市代碼（'000' 為直轄市）
        years: 年份列表，預設為 [2014, 2020]
        cities: 縣市列表（用於取得區域代碼），預設使用 ALL_CITIES

    Returns:
        DataFrame or None
    """
    if years is None:
        years = [2014, 2020]
    if cities is None:
        cities = ALL_CITIES

    city_output_dir = os.path.join(output_dir, city_name)
    output_path = os.path.join(city_output_dir, f'{city_name}選舉整理_完成版.xlsx')

    # 判斷是否需要「立委選區」欄位（僅當包含 2020 年資料時）
    include_legislator_col = 2020 in years

    all_data = []

    for year in years:
        # 取得該年份的選舉類型配置
        election_configs = MERGE_CONFIGS.get(year)
        if not election_configs:
            continue

        for election_type, election_name in election_configs:
            if election_type == 'council':
                if city_code == '000':
                    file_path = os.path.join(city_output_dir, f'{year}_直轄市區域議員_各投開票所得票數_{city_name}.xlsx')
                else:
                    file_path = os.path.join(city_output_dir, f'{year}_縣市區域議員_各投開票所得票數_{city_name}.xlsx')

                if os.path.exists(file_path):
                    xl = pd.ExcelFile(file_path)
                    for sheet_name in xl.sheet_names:
                        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                        rows = _extract_election_data(df, year, election_name, city_name, sheet_name, MAX_CANDIDATES, include_legislator_col=include_legislator_col)
                        all_data.extend(rows)

            elif election_type == 'mayor':
                if city_code == '000':
                    file_path = os.path.join(city_output_dir, f'{year}_直轄市市長_各村里得票數_{city_name}.xlsx')
                else:
                    file_path = os.path.join(city_output_dir, f'{year}_縣市市長_各村里得票數_{city_name}.xlsx')

                if os.path.exists(file_path):
                    df = pd.read_excel(file_path, header=None)
                    rows = _extract_election_data(df, year, election_name, city_name, None, MAX_CANDIDATES, include_legislator_col=include_legislator_col)
                    all_data.extend(rows)

            elif election_type == 'president':
                file_path = os.path.join(city_output_dir, f'{year}_總統候選人得票數一覽表_各村里_{city_name}.xlsx')

                if os.path.exists(file_path):
                    df = pd.read_excel(file_path, header=None)
                    rows = _extract_election_data(df, year, election_name, city_name, None, MAX_CANDIDATES, include_legislator_col=include_legislator_col)
                    all_data.extend(rows)

            elif election_type == 'legislator':
                file_path = os.path.join(city_output_dir, f'{year}_區域立委_各村里得票數_{city_name}.xlsx')

                if os.path.exists(file_path):
                    xl = pd.ExcelFile(file_path)
                    for sheet_name in xl.sheet_names:
                        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                        rows = _extract_election_data(df, year, election_name, city_name, sheet_name, MAX_CANDIDATES, is_legislator=True, include_legislator_col=include_legislator_col)
                        all_data.extend(rows)

            elif election_type == 'township_mayor':
                file_path = os.path.join(city_output_dir, f'{year}_鄉鎮市長_各村里得票數_{city_name}.xlsx')

                if os.path.exists(file_path):
                    xl = pd.ExcelFile(file_path)
                    for sheet_name in xl.sheet_names:
                        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                        rows = _extract_election_data(df, year, election_name, city_name, sheet_name, MAX_CANDIDATES, include_legislator_col=include_legislator_col, is_township_mayor=True)
                        all_data.extend(rows)

            elif election_type == 'mountain_legislator':
                file_path = os.path.join(city_output_dir, f'{year}_山地原住民立委_各村里得票數_{city_name}.xlsx')

                if os.path.exists(file_path):
                    df = pd.read_excel(file_path, header=None)
                    rows = _extract_election_data(df, year, election_name, city_name, None, MAX_CANDIDATES, include_legislator_col=include_legislator_col)
                    all_data.extend(rows)

            elif election_type == 'plain_legislator':
                file_path = os.path.join(city_output_dir, f'{year}_平地原住民立委_各村里得票數_{city_name}.xlsx')

                if os.path.exists(file_path):
                    df = pd.read_excel(file_path, header=None)
                    rows = _extract_election_data(df, year, election_name, city_name, None, MAX_CANDIDATES, include_legislator_col=include_legislator_col)
                    all_data.extend(rows)

            elif election_type == 'party_vote':
                file_path = os.path.join(city_output_dir, f'{year}_政黨票_各村里得票數_{city_name}.xlsx')

                if os.path.exists(file_path):
                    df = pd.read_excel(file_path, header=None)
                    rows = _extract_election_data(df, year, election_name, city_name, None, MAX_CANDIDATES, include_legislator_col=include_legislator_col)
                    all_data.extend(rows)

    if all_data:
        # 建立欄位名稱（格式參考範例檔案：2014_選舉資料_花蓮縣.xlsx）
        columns = ['時間', '選舉名稱', '縣市', '行政區別', '鄰里', '區域別代碼', '選區']
        for i in range(1, MAX_CANDIDATES + 1):
            columns.extend([f'選舉候選人{i}', f'選舉候選人政黨{i}', f'選舉候選人得票數{i}', f'選舉候選人得票率{i}'])
        columns.extend(['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數', '投票率'])
        if include_legislator_col:
            columns.append('立委選區')

        result_df = pd.DataFrame(all_data, columns=columns)

        # 刪除鄰里為空的行
        before_count = len(result_df)
        result_df = result_df[result_df['鄰里'].notna() & (result_df['鄰里'] != '')]
        after_count = len(result_df)
        if before_count != after_count:
            print(f"  刪除鄰里為空的行: {before_count - after_count} 筆")

        # 刪除空的候選人欄位（沒有任何資料的候選人）
        cols_to_drop = []
        for i in range(1, MAX_CANDIDATES + 1):
            cand_col = f'選舉候選人{i}'
            if cand_col in result_df.columns:
                # 檢查該候選人欄位是否全部為空
                if result_df[cand_col].isna().all() or (result_df[cand_col] == '').all():
                    cols_to_drop.extend([
                        f'選舉候選人{i}',
                        f'選舉候選人政黨{i}',
                        f'選舉候選人得票數{i}',
                        f'選舉候選人得票率{i}'
                    ])
        if cols_to_drop:
            result_df = result_df.drop(columns=[c for c in cols_to_drop if c in result_df.columns])
            print(f"  刪除空的候選人欄位: {len(cols_to_drop) // 4} 組")

        # 建立並填入區域別代碼
        area_code_map = build_area_code_map(city_name, years)
        if area_code_map:
            def get_area_code(row):
                linli = row['鄰里']
                return area_code_map.get(linli, '')
            result_df['區域別代碼'] = result_df.apply(get_area_code, axis=1)
            filled_count = (result_df['區域別代碼'] != '').sum()
            print(f"  填入區域別代碼: {filled_count}/{len(result_df)} 筆")

        result_df.to_excel(output_path, index=False, engine='openpyxl', sheet_name=city_name)
        print(f"  已儲存: {output_path}")
        print(f"  總筆數: {len(result_df)}")
        return result_df

    return None


def _extract_election_data(df, year, election_name, city_name, area_name, max_candidates, is_legislator=False, include_legislator_col=None, is_township_mayor=False):
    """從 Excel 資料框架中提取選舉資料

    Args:
        df: pandas DataFrame（原始 Excel 資料）
        year: 年份
        election_name: 選舉名稱
        city_name: 縣市名稱
        area_name: 選舉區名稱（如 '第1選舉區'），若為 None 則無選區
        max_candidates: 最大候選人數
        is_legislator: 是否為立委選舉
        include_legislator_col: 是否包含立委選區欄位
        is_township_mayor: 是否為鄉鎮市長選舉（若為 True，使用 area_name 作為行政區別）

    Returns:
        list of rows
        欄位順序：時間, 選舉名稱, 縣市, 行政區別, 鄰里(行政區_里名格式), 區域別代碼, 選區, 候選人資料..., 統計欄位, 立委選區
    """
    rows = []

    # 取得候選人資訊（第3行，index=2）
    cand_row = df.iloc[2].tolist() if len(df) > 2 else []

    # 解析候選人（格式：(1)\n姓名\n政黨）
    candidates = []
    for val in cand_row:
        if pd.notna(val) and str(val).strip():
            val_str = str(val).strip()
            if val_str.startswith('('):
                # 分離號碼、姓名和政黨
                lines = val_str.split('\n')
                no = ''
                name = ''
                party = '無黨籍'

                if len(lines) >= 1:
                    # 第一行格式: (1) 或 (1)姓名
                    first_line = lines[0]
                    if ')' in first_line:
                        parts = first_line.split(')')
                        no = parts[0].replace('(', '').strip()
                        if len(parts) > 1 and parts[1].strip():
                            name = parts[1].strip()

                if len(lines) >= 4:
                    # 總統選舉格式：(1)\n蔡英文\n賴清德\n民主進步黨
                    # 第二行是正總統、第三行是副總統、第四行是政黨
                    president = lines[1].strip()
                    vice_president = lines[2].strip()
                    name = f"{president}/{vice_president}" if vice_president else president
                    party = lines[3].strip() or '無黨籍'
                elif len(lines) >= 3:
                    # 一般格式：(1)\n姓名\n政黨
                    if not name:
                        name = lines[1].strip()
                    party = lines[2].strip() or '無黨籍'
                elif len(lines) >= 2 and not name:
                    # 只有兩行：(1)\n姓名
                    # 對於政黨票，「候選人」就是政黨本身，所以 party = name
                    name = lines[1].strip()
                    party = name
                elif len(lines) >= 2 and name:
                    # 如果姓名已經有了，第二行是政黨
                    party = lines[1].strip() or '無黨籍'

                if no and name:
                    candidates.append({'no': no, 'name': name, 'party': party})

    # 資料從第6行開始（index=5）
    data_start = 5

    # 判斷是否有投開票所欄位
    has_polling_station = '投開票所別' in str(df.iloc[1, 2]) if len(df) > 1 and len(df.columns) > 2 else False
    data_col_start = 3 if has_polling_station else 2

    # 如果有投開票所，需要按村里彙總資料
    if has_polling_station:
        # 先收集所有村里資料並彙總
        village_data = {}  # key: (dept, village), value: {'votes': [...], 'stats': [...]}
        current_dept = ''

        for idx in range(data_start, len(df)):
            row = df.iloc[idx]

            dept = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
            village = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ''

            # 跳過空行和總計行
            if dept in ['總　計', '總計', ''] and village == '':
                continue
            if dept.startswith('　') or dept.startswith(' '):
                continue

            # 更新當前行政區
            if dept and dept not in ['總　計', '總計']:
                current_dept = dept

            if not village:
                continue

            key = (current_dept, village)

            # 收集候選人得票數
            votes_list = []
            for i in range(len(candidates)):
                col_idx = data_col_start + i
                votes = 0
                if col_idx < len(row):
                    v = row.iloc[col_idx]
                    if pd.notna(v):
                        try:
                            votes = int(float(v))
                        except (ValueError, TypeError):
                            votes = 0
                votes_list.append(votes)

            # 收集統計欄位 (不含投票率，投票率需要重新計算)
            stat_start = data_col_start + len(candidates)
            stats_list = []
            for i in range(7):  # 7 個統計欄位（不含投票率）
                col_idx = stat_start + i
                val = 0
                if col_idx < len(row):
                    v = row.iloc[col_idx]
                    if pd.notna(v):
                        try:
                            val = int(float(v))
                        except (ValueError, TypeError):
                            val = 0
                stats_list.append(val)

            # 彙總資料
            if key not in village_data:
                village_data[key] = {'votes': votes_list, 'stats': stats_list}
            else:
                # 累加得票數
                for i in range(len(votes_list)):
                    village_data[key]['votes'][i] += votes_list[i]
                # 累加統計欄位
                for i in range(len(stats_list)):
                    village_data[key]['stats'][i] += stats_list[i]

        # 生成輸出資料
        for (dept, village), data in sorted(village_data.items()):
            votes_list = data['votes']
            stats_list = data['stats']

            # 計算總有效票
            total_valid_votes = sum(votes_list)

            # 鄉鎮市長選舉：使用 area_name（鄉鎮市名稱）作為行政區別
            if is_township_mayor and area_name:
                actual_dept = area_name
                linli = f"{area_name}_{village}" if village else area_name
            else:
                actual_dept = dept
                # 建立鄰里欄位：行政區_里名 格式（如：花蓮市_民立里）
                linli = f"{dept}_{village}" if dept and village else village

            output_row = [
                year,
                election_name,
                city_name,
                actual_dept,  # 行政區別
                linli,  # 鄰里（行政區_里名格式）
                '',  # 區域別代碼
                area_name if area_name else '',
            ]

            # 填入候選人資料
            for i in range(max_candidates):
                if i < len(candidates):
                    votes = votes_list[i] if i < len(votes_list) else 0
                    vote_rate = votes / total_valid_votes if total_valid_votes > 0 else 0
                    output_row.extend([
                        candidates[i]['name'],
                        candidates[i].get('party', ''),
                        votes,
                        vote_rate
                    ])
                else:
                    output_row.extend([None, None, None, None])

            # 統計欄位
            output_row.extend(stats_list)
            # 計算投票率
            turnout = round(stats_list[2] / stats_list[6] * 100, 2) if len(stats_list) > 6 and stats_list[6] > 0 else 0
            output_row.append(turnout)

            # 立委選區（根據 include_legislator_col 參數決定是否包含）
            # 預設：僅 2020 年需要此欄位
            should_include = include_legislator_col if include_legislator_col is not None else (year == 2020)
            if should_include:
                output_row.append(area_name if is_legislator and area_name else '')

            rows.append(output_row)

        return rows

    # 沒有投開票所的情況，逐行處理
    current_dept = ''
    for idx in range(data_start, len(df)):
        row = df.iloc[idx]

        # 取得行政區別和村里別
        dept = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
        village = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ''

        # 跳過空行和總計行
        if dept in ['總　計', '總計', ''] and village == '':
            continue
        if dept.startswith('　') or dept.startswith(' '):
            # 這是區級小計行，跳過
            continue

        # 更新當前行政區
        if dept and dept not in ['總　計', '總計']:
            current_dept = dept

        # 鄉鎮市長選舉：使用 area_name（鄉鎮市名稱）作為行政區別
        if is_township_mayor and area_name:
            actual_dept = area_name
            linli = f"{area_name}_{village}" if village else area_name
        else:
            actual_dept = current_dept
            # 建立鄰里欄位：行政區_里名 格式（如：花蓮市_民立里）
            linli = f"{current_dept}_{village}" if current_dept and village else village

        # 準備輸出資料
        output_row = [
            year,
            election_name,
            city_name,
            actual_dept,  # 行政區別
            linli,  # 鄰里（行政區_里名格式）
            '',  # 區域別代碼
            area_name if area_name else '',
        ]

        # 計算總有效票
        total_valid_votes = 0
        for i in range(len(candidates)):
            col_idx = data_col_start + i
            if col_idx < len(row):
                votes = row.iloc[col_idx]
                if pd.notna(votes):
                    try:
                        total_valid_votes += int(float(votes))
                    except (ValueError, TypeError):
                        pass

        # 填入候選人資料
        for i in range(max_candidates):
            if i < len(candidates):
                col_idx = data_col_start + i
                votes = 0
                if col_idx < len(row):
                    v = row.iloc[col_idx]
                    if pd.notna(v):
                        try:
                            votes = int(float(v))
                        except (ValueError, TypeError):
                            votes = 0

                # 計算得票率
                vote_rate = votes / total_valid_votes if total_valid_votes > 0 else 0

                output_row.extend([
                    candidates[i]['name'],
                    candidates[i].get('party', ''),
                    votes,
                    vote_rate
                ])
            else:
                output_row.extend([None, None, None, None])

        # 統計欄位
        stat_start = data_col_start + len(candidates)
        stats = []
        stat_names = ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數', '投票率']
        for i in range(8):
            col_idx = stat_start + i
            if col_idx < len(row):
                val = row.iloc[col_idx]
                if pd.notna(val):
                    try:
                        stats.append(float(val) if '率' in stat_names[i] else int(float(val)))
                    except (ValueError, TypeError):
                        stats.append(0)
                else:
                    stats.append(0)
            else:
                stats.append(0)

        output_row.extend(stats)

        # 立委選區（根據 include_legislator_col 參數決定是否包含）
        # 預設：僅 2020 年需要此欄位
        should_include = include_legislator_col if include_legislator_col is not None else (year == 2020)
        if should_include:
            output_row.append(area_name if is_legislator and area_name else '')

        rows.append(output_row)

    return rows


