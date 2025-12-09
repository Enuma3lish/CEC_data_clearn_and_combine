#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import csv
import hashlib
import os

COUNTY_CODE_MAP = {
    '臺北市': '63000', '新北市': '65000', '桃園市': '68000', '臺中市': '66000',
    '臺南市': '67000', '高雄市': '64000', '基隆市': '10017', '新竹市': '10018',
    '嘉義市': '10020', '宜蘭縣': '10002', '新竹縣': '10004', '苗栗縣': '10005',
    '彰化縣': '10007', '南投縣': '10008', '雲林縣': '10009', '嘉義縣': '10010',
    '屏東縣': '10013', '臺東縣': '10014', '花蓮縣': '10015', '澎湖縣': '10016',
    '金門縣': '09020', '連江縣': '09007',
}
MUNICIPALITIES = ['臺北市', '新北市', '桃園市', '臺中市', '臺南市', '高雄市']
def is_municipality(county): return county in MUNICIPALITIES
def stable_hash(s):
    if not s or s == 'nan': return 0
    return int(hashlib.md5(s.encode('utf-8')).hexdigest(), 16)
def generate_area_code(county, district, village):
    county_code = COUNTY_CODE_MAP.get(county, '00000')
    district_code = str(abs(stable_hash(district)) % 100).zfill(2)
    village_code = str(abs(stable_hash(village)) % 10000).zfill(4)
    return f"{county_code}{district_code}{village_code}"
    
def clean_df_indices(df: pd.DataFrame) -> pd.DataFrame:
    if '行政區別' in df.columns:
        df['行政區別'] = df['行政區別'].fillna(method='ffill')
        df = df.dropna(subset=['行政區別'])
        df['行政區別'] = df['行政區別'].astype(str).str.strip()
        # Remove "第XX選區" to allow merging of same village across different election types
        df['行政區別'] = df['行政區別'].str.replace(r'第\d+選區', '', regex=True)
    if '鄰里' in df.columns: df['鄰里'] = df['鄰里'].astype(str).str.strip()
    elif '村里別' in df.columns: df['村里別'] = df['村里別'].astype(str).str.strip()
    return df

def normalize_code(c): return c.strip().replace("'", "").replace('"', "").lstrip('0') or '0'
def get_single_col(df, col_name):
    if col_name not in df.columns: return None
    c = df[col_name]
    if isinstance(c, pd.DataFrame): return c.iloc[:, 0]
    return c
def find_vote_folder(data_dir, year):
    target = data_dir / f"{year}總統立委"
    if target.exists(): return target
    target = data_dir / f"{year}_總統立委" 
    if target.exists(): return target
    for item in data_dir.iterdir():
        if item.is_dir() and item.name.startswith(str(year)) and "地方公職" in item.name: return item
    return None
def load_party_names(data_dir, year):
    year_dir = find_vote_folder(data_dir, year)
    search_paths = []
    
    # Special handling for 2022
    if year == 2022:
        year_2022_dir = data_dir / "2022-111年地方公職人員選舉"
        if year_2022_dir.exists():
            # Check common 2022 folders for elpaty.csv
            search_paths += [
                year_2022_dir / "C1" / "prv" / "elpaty.csv",
                year_2022_dir / "C1" / "city" / "elpaty.csv",
                year_2022_dir / "T1" / "prv" / "elpaty.csv",
                year_2022_dir / "T1" / "city" / "elpaty.csv"
            ]
    elif year_dir:
        search_paths += [year_dir / "區域立委" / "elpaty.csv", year_dir / "總統" / "elpaty.csv", year_dir / "直轄市區域議員" / "elpaty.csv", year_dir / "縣市區域議員" / "elpaty.csv", year_dir / "直轄市市長" / "elpaty.csv", year_dir / "縣市市長" / "elpaty.csv"]
    
    party_map = {}
    for path in search_paths:
        if path.exists():
            try:
                # Try multiple encodings
                for encoding in ['utf-8-sig', 'utf-8', 'cp950', 'big5', 'gbk']:
                    try:
                        with open(path, 'r', encoding=encoding, errors='replace') as f:
                            reader = csv.reader(f, quotechar="'", quoting=1)
                            temp_map = {}
                            for row in reader:
                                if len(row) >= 2: 
                                    code = normalize_code(row[0])
                                    name = row[1].strip().replace("'", "").replace('"', '').replace('\ufffd', '')
                                    if name and len(name) >= 2:  # Valid party name
                                        temp_map[code] = name
                            # Check if we got valid names
                            if temp_map and not any('\ufffd' in v for v in temp_map.values()):
                                party_map.update(temp_map)
                                break  # Success
                    except: continue
                if party_map: break 
            except: pass
    return party_map

def load_full_candidate_info(votedata_path, year, county, election_category):
    party_map = load_party_names(votedata_path, year)
    cand_info = {}
    is_muni = is_municipality(county)
    target_prv_code = COUNTY_CODE_MAP.get(county)
    
    # Special handling for 2022 (different folder structure)
    if year == 2022:
        year_dir = votedata_path / "2022-111年地方公職人員選舉"
        if not year_dir.exists():
            return {}
        
        # Determine which subfolder to check based on election category and municipality status
        targets = []
        if election_category == 'Councilor':
            if is_muni:
                targets = [(year_dir / "T1" / "prv", 'regional'), 
                          (year_dir / "T2" / "prv", 'plain'), 
                          (year_dir / "T3" / "prv", 'mountain')]
            else:
                targets = [(year_dir / "T1" / "city", 'regional'),
                          (year_dir / "T2" / "city", 'plain'),
                          (year_dir / "T3" / "city", 'mountain')]
        elif election_category == 'Mayor':
            if is_muni:
                targets = [(year_dir / "C1" / "prv", 'mayor')]
            else:
                targets = [(year_dir / "C1" / "city", 'mayor')]
        elif election_category == 'TownshipRepresentative':
             targets = [(year_dir / "R1" / "city", 'township_rep')]
        
        # Load candidate info from elcand.csv in the determined folders
        for folder_path, type_label in targets:
            elcand_path = folder_path / "elcand.csv"
            if elcand_path.exists():
                try:
                    for encoding in ['utf-8-sig', 'utf-8', 'cp950', 'big5', 'gbk']:
                        try:
                            with open(elcand_path, 'r', encoding=encoding, errors='replace') as f:
                                reader = csv.reader(f, quotechar="'", quoting=1)
                                for row in reader:
                                    if len(row) > 7:
                                        try:
                                            # Data folder structure already ensures correct county/city
                                            # No need for province code filtering (it had bugs with 2-digit vs 5-digit codes)
                                            name = row[6].strip().replace("'", "").replace('"', '').replace('\ufffd', '')
                                            party_code = row[7].strip().replace("'", "").replace('"', '')
                                            if name and len(name) >= 2:
                                                cand_info[name] = {'type': type_label, 'party': party_map.get(normalize_code(party_code), '')}
                                        except: pass
                                break
                        except: continue
                except: pass
        return cand_info
    
    # Original logic for non-2022 years
    targets = {}
    if election_category == 'Legislator': targets = {'區域立委': 'regional', '平地立委': 'plain', '山地立委': 'mountain'}
    elif election_category == 'Councilor':
        targets = {'直轄市區域議員': 'regional', '直轄市平原議員': 'plain', '直轄市山原議員': 'mountain'} if is_muni else {'縣市區域議員': 'regional', '縣市平原議員': 'plain', '縣市山原議員': 'mountain'}
    elif election_category == 'Mayor': targets = {'直轄市市長': 'mayor'} if is_muni else {'縣市市長': 'mayor'}
    elif election_category == 'President': targets = {'總統': 'president'}
    elif election_category == 'TownshipRepresentative': targets = {'鄉鎮市民代表': 'township_rep'}

    year_dir = find_vote_folder(votedata_path, year)
    if not year_dir: return {}
    for subdir, type_label in targets.items():
        path = year_dir / subdir / "elcand.csv"
        # Special check for older repo structure where "鄉鎮市民代表" might be "縣市鄉鎮市民代表"
        if not path.exists() and election_category == 'TownshipRepresentative':
             path = year_dir / "縣市鄉鎮市民代表" / "elcand.csv"

        if path.exists():
            try:
                # Try multiple encodings
                for encoding in ['utf-8-sig', 'utf-8', 'cp950', 'big5', 'gbk']:
                    try:
                        with open(path, 'r', encoding=encoding, errors='replace') as f:
                            reader = csv.reader(f, quotechar="'", quoting=1)
                            for row in reader:
                                if len(row) > 7:  # Need at least 8 columns (0-7)
                                    try:
                                        # Data folder structure already ensures correct county/city
                                        # No need for province code filtering (it had bugs with 2-digit vs 5-digit codes)
                                        name = row[6].strip().replace("'", "").replace('"', '').replace('\ufffd', '')
                                        party_code = row[7].strip().replace("'", "").replace('"', '')
                                        if name and len(name) >= 2:  # Valid name
                                            cand_info[name] = {'type': type_label, 'party': party_map.get(normalize_code(party_code), '')}
                                    except: pass
                        break  # Success, stop trying encodings
                    except: continue
            except: pass
    return cand_info

def format_generic_data(df: pd.DataFrame, county: str, cand_info: Dict[str, Dict], role_prefix: str, election_type: str = None) -> pd.DataFrame:
    df = df.loc[:, ~df.columns.duplicated()]
    df = clean_df_indices(df)
    
    # Filter out merge artifact columns (_x, _y suffixes) before processing
    df = df.loc[:, ~df.columns.str.match(r'.*_[xy]$', na=False)]
    
    if '行政區別' in df.columns: df = df[df['行政區別'] != '總計'].copy()
    if '村里別' in df.columns: 
        df = df.dropna(subset=['村里別'])
        df = df[df['村里別'] != ''].copy()
        df = df[df['村里別'] != 'nan'].copy()  # Filter out string "nan"
        df = df[~df['村里別'].str.lower().isin(['nan', 'none', 'null'])].copy()

    # --- DEDUPLICATION LOGIC START ---
    # Check for duplicates in Neighborhoods. If found, force aggregation.
    # This handles cases where input data has multiple rows per village (e.g. split by polling station but missing station col)
    # but wasn't caught by the '投開票所別' check below.
    keys_to_check = ['行政區別', '村里別'] if '村里別' in df.columns else (['行政區別', '鄰里'] if '鄰里' in df.columns else [])
    
    should_agg = False
    if '投開票所別' in df.columns:
        should_agg = True
    elif keys_to_check and df.duplicated(subset=keys_to_check).any():
        print(f"     [INFO] 發現 {role_prefix} 資料有重複鄰里，將強制進行合併 (Aggregation)...")
        should_agg = True
    
    if should_agg and keys_to_check:
        group_cols = keys_to_check
        possible_stats = [c for c in df.columns if c not in group_cols and c != '投開票所別']
        numeric_cols = [c for c in possible_stats if pd.api.types.is_numeric_dtype(df[c])]
        
        print(f"     [DEBUG agg] Total columns: {len(df.columns)}, group_cols: {len(group_cols)}, possible_stats: {len(possible_stats)}, numeric: {len(numeric_cols)}")
        
        # Build agg dict: sum for numeric, first for party columns (黨籍)
        agg_dict = {col: 'sum' for col in numeric_cols}
        # Add party columns (use 'first' to preserve the value)
        party_cols = [c for c in possible_stats if '黨籍' in c]
        for col in party_cols:
            agg_dict[col] = 'first'
        
        print(f"     [DEBUG agg] Before groupby: {len(df)} rows, unique neighborhoods: {df[keys_to_check].drop_duplicates().shape[0]}")
        df_agg = df.groupby(group_cols, as_index=False).agg(agg_dict)
        print(f"     [DEBUG agg] After groupby: {len(df_agg)} rows, {len(df_agg.columns)} columns")
        print(f"     [DEBUG agg] Columns in df_agg: {list(df_agg.columns)}")
    else: 
        df_agg = df.copy()
        print(f"     [DEBUG agg] No aggregation needed: {len(df_agg)} rows, {len(df_agg.columns)} columns")
    # --- DEDUPLICATION LOGIC END ---

    df_agg = df_agg.loc[:, ~df_agg.columns.duplicated()]
    if '區域別代碼' in df_agg.columns: df_agg = df_agg.drop(columns=['區域別代碼'])

    df_agg.insert(0, '縣市', county)
    try:
        codes = df_agg.apply(lambda row: generate_area_code(county, row['行政區別'], row['村里別'] if '村里別' in row else row['鄰里']), axis=1)
        df_agg['區域別代碼'] = codes
    except Exception:
        df_agg['區域別代碼'] = ["0"] * len(df_agg)

    df_agg = df_agg.rename(columns={'村里別': '鄰里'})

    result = pd.DataFrame()
    result['縣市'] = get_single_col(df_agg, '縣市')
    result['行政區別'] = get_single_col(df_agg, '行政區別')
    result['鄰里'] = get_single_col(df_agg, '鄰里')
    result['區域別代碼'] = get_single_col(df_agg, '區域別代碼')
    
    print(f"     [DEBUG] result shape before add_cols: {result.shape}")

    ignore_keywords = ['有效票數', '無效票數', '投票數', '選舉人數', '得票率', '已領未投', '發出票數', '用餘票數', '投票率', '黨籍']
    ignore_cols = ['行政區別', '鄰里', '投開票所別', '縣市', '區域別代碼']
    
    # === DETECT FORMAT TYPE ===
    # Check if intermediate file uses standard format (候選人X＿候選人名稱) or pivot format (candidate names as columns)
    has_standard_format = any('候選人' in col and '＿候選人名稱' in col for col in df_agg.columns)
    
    if has_standard_format:
        print(f"     [INFO] Detected STANDARD format for {role_prefix} data")
        # Extract candidate info from standard format columns
        # Format: 候選人1＿候選人名稱, 候選人1＿黨籍, 候選人1_得票數, 候選人2＿候選人名稱, ...
        candidate_data = []
        i = 1
        while True:
            name_col = f'候選人{i}＿候選人名稱'
            party_col = f'候選人{i}＿黨籍'
            vote_col = f'候選人{i}_得票數'
            
            # Check if this candidate index exists
            if name_col not in df_agg.columns and vote_col not in df_agg.columns:
                break
            
            # Get candidate name from data values (first row, since name is constant)
            if name_col in df_agg.columns:
                cand_name = df_agg[name_col].iloc[0] if len(df_agg) > 0 and pd.notna(df_agg[name_col].iloc[0]) else None
            else:
                cand_name = None
            
            # Get party
            if party_col in df_agg.columns:
                party = df_agg[party_col].iloc[0] if len(df_agg) > 0 else ''
            else:
                party = ''
            
            # Store candidate info
            if cand_name or vote_col in df_agg.columns:
                candidate_data.append({
                    'name': cand_name,
                    'party': party,
                    'vote_col': vote_col,
                    'index': i
                })
            
            i += 1
        
        print(f"     [DEBUG] Extracted {len(candidate_data)} candidates from standard format")
        
        # Now process using standard format data
        # We'll handle this in add_cols function
        cols_by_type = {'regional': [], 'plain': [], 'mountain': [], 'mayor': [], 'president': [], 'township_rep': []}
        col_party_map = {}
        
        # Organize candidates by type
        for cand in candidate_data:
            cand_name = cand['name']
            if cand_name and cand_name in cand_info:
                info = cand_info[cand_name]
                t = info['type']
                if t in cols_by_type:
                    cols_by_type[t].append(cand)
                else:
                    cols_by_type['regional'].append(cand)
                col_party_map[cand_name] = cand['party'] or info.get('party', '')
            else:
                # Candidate not in cand_info - categorize by role_prefix
                if role_prefix == '市長':
                    cols_by_type['mayor'].append(cand)
                elif role_prefix == '總統':
                    cols_by_type['president'].append(cand)
                elif role_prefix == '鄉鎮市民代表':
                    cols_by_type['township_rep'].append(cand)
                elif role_prefix in ['立委', '議員']:
                    cols_by_type['regional'].append(cand)
                else:
                    cols_by_type['regional'].append(cand)
                
                if cand_name:
                    col_party_map[cand_name] = cand['party']
    
    else:
        print(f"     [INFO] Detected PIVOT format for {role_prefix} data")
        # Original logic for pivot format
        # Also ignore numeric-only column names (like '1', '2', '10') which are merge artifacts
        cand_cols = [c for c in df_agg.columns if not any(x in c for x in ignore_keywords) and c not in ignore_cols and not str(c).isdigit()]
        
        cols_by_type = {'regional': [], 'plain': [], 'mountain': [], 'mayor': [], 'president': [], 'township_rep': []}
        col_party_map = {}
        sorted_names = sorted(cand_info.keys(), key=len, reverse=True)

        # First, extract party information from intermediate CSV columns (候選人名_黨籍)
        for col in df_agg.columns:
            if col.endswith('_黨籍'):
                cand_name = col.replace('_黨籍', '')
                # Get the party value from the first row (party is constant per candidate)
                party_value = df_agg[col].iloc[0] if len(df_agg) > 0 else ''
                col_party_map[cand_name] = party_value

        for col_idx, col in enumerate(cand_cols):
            matched_info = None
            for cand_name in sorted_names:
                if cand_name in col: matched_info = cand_info[cand_name]; break
            
            info = matched_info
            if info:
                t = info['type']
                # Use party from cand_info if available, otherwise use from intermediate CSV
                if col not in col_party_map:
                    col_party_map[col] = info['party']
                if t in cols_by_type: cols_by_type[t].append(col)
                else: cols_by_type['regional'].append(col)
            else:
                if role_prefix == '市長': 
                    cols_by_type['mayor'].append(col)
                elif role_prefix == '總統': 
                    cols_by_type['president'].append(col)
                elif role_prefix == '鄉鎮市民代表': 
                    cols_by_type['township_rep'].append(col)
                elif role_prefix == '立委':
                    # For legislators without cand_info (repaired 2020/2024 data), default to 'regional'
                    # They will be sorted into correct types based on naming later
                    cols_by_type['regional'].append(col)
                elif role_prefix == '議員':
                    # Similarly for councilors
                    cols_by_type['regional'].append(col)
                else: 
                    # For other unmatched candidates, skip to avoid false positives
                    pass
                # Keep party from intermediate CSV if already mapped, otherwise set to empty
                if col not in col_party_map:
                    col_party_map[col] = ''

    def add_cols(type_key, name_prefix):
        cols = cols_by_type.get(type_key, [])
        
        # For PIVOT format: filter out party columns (candidates only)
        # For STANDARD format: cols is already a list of dicts, no filtering needed
        if not has_standard_format:
            cols = [c for c in cols if not c.endswith('_黨籍')]
        
        is_president = (role_prefix == '總統')
        is_legislator_or_councilor = (role_prefix in ['立委', '議員'])
        
        for i, item in enumerate(cols, 1):
            # === STANDARD FORMAT HANDLING ===
            if has_standard_format:
                # item is a dict: {'name': str, 'party': str, 'vote_col': str, 'index': int}
                cand_name = item['name']
                party = item['party']
                vote_col_name = item['vote_col']
                
                # President format: "總統候選人1" (name), "總統候選人1_得票數"
                if is_president:
                    result[f'{name_prefix}候選人{i}'] = cand_name if cand_name else ''
                    vote_col = get_single_col(df_agg, vote_col_name)
                    if vote_col is not None:
                        result[f'{name_prefix}候選人{i}_得票數'] = pd.to_numeric(vote_col, errors='coerce').fillna(0).astype(int)
                    else:
                        result[f'{name_prefix}候選人{i}_得票數'] = 0
                
                # Legislator/Councilor format
                elif is_legislator_or_councilor:
                    if i == 1:
                        # First candidate: only party + votes (no name column)
                        result[f'{name_prefix}候選人{i}＿黨籍'] = party
                        vote_col = get_single_col(df_agg, vote_col_name)
                        if vote_col is not None:
                            result[f'{name_prefix}候選人{i}_得票數'] = pd.to_numeric(vote_col, errors='coerce').fillna(0).astype(int)
                        else:
                            result[f'{name_prefix}候選人{i}_得票數'] = 0
                    else:
                        # Candidates 2+: full format (name + party + votes)
                        result[f'{name_prefix}候選人{i}＿候選人名稱'] = cand_name if cand_name else ''
                        result[f'{name_prefix}候選人{i}＿黨籍'] = party
                        vote_col = get_single_col(df_agg, vote_col_name)
                        if vote_col is not None:
                            result[f'{name_prefix}候選人{i}_得票數'] = pd.to_numeric(vote_col, errors='coerce').fillna(0).astype(int)
                        else:
                            result[f'{name_prefix}候選人{i}_得票數'] = 0
                
                # Other formats (mayor, township_rep, etc.)
                else:
                    result[f'{name_prefix}候選人{i}＿候選人名稱'] = cand_name if cand_name else ''
                    result[f'{name_prefix}候選人{i}＿黨籍'] = party
                    vote_col = get_single_col(df_agg, vote_col_name)
                    if vote_col is not None:
                        result[f'{name_prefix}候選人{i}_得票數'] = pd.to_numeric(vote_col, errors='coerce').fillna(0).astype(int)
                    else:
                        result[f'{name_prefix}候選人{i}_得票數'] = 0
            
            # === PIVOT FORMAT HANDLING (original logic) ===
            else:
                # item is a column name (string) containing candidate name
                name = item
                
                # President format: "總統候選人1" (name as value), "總統候選人1_得票數"
                if is_president:
                    result[f'{name_prefix}候選人{i}'] = name
                    vote_col = get_single_col(df_agg, name)
                    if vote_col is not None:
                        result[f'{name_prefix}候選人{i}_得票數'] = pd.to_numeric(vote_col, errors='coerce').fillna(0).astype(int)
                    else:
                        result[f'{name_prefix}候選人{i}_得票數'] = 0
                
                # Legislator/Councilor format: Candidate 1 missing name column
                elif is_legislator_or_councilor:
                    if i == 1:
                        # First candidate: only party + votes (no name column)
                        party_col_name = f'{name}_黨籍'
                        if party_col_name in df_agg.columns:
                            party_val = get_single_col(df_agg, party_col_name)
                            if party_val is not None:
                                result[f'{name_prefix}候選人{i}＿黨籍'] = party_val
                            else:
                                result[f'{name_prefix}候選人{i}＿黨籍'] = col_party_map.get(name, '')
                        else:
                            result[f'{name_prefix}候選人{i}＿黨籍'] = col_party_map.get(name, '')
                        
                        vote_col = get_single_col(df_agg, name)
                        if vote_col is not None:
                            result[f'{name_prefix}候選人{i}_得票數'] = pd.to_numeric(vote_col, errors='coerce').fillna(0).astype(int)
                        else:
                            result[f'{name_prefix}候選人{i}_得票數'] = 0
                    else:
                        # Candidates 2+: full format (name + party + votes)
                        result[f'{name_prefix}候選人{i}＿候選人名稱'] = name
                        
                        party_col_name = f'{name}_黨籍'
                        if party_col_name in df_agg.columns:
                            party_val = get_single_col(df_agg, party_col_name)
                            if party_val is not None:
                                result[f'{name_prefix}候選人{i}＿黨籍'] = party_val
                            else:
                                result[f'{name_prefix}候選人{i}＿黨籍'] = col_party_map.get(name, '')
                        else:
                            result[f'{name_prefix}候選人{i}＿黨籍'] = col_party_map.get(name, '')
                        
                        vote_col = get_single_col(df_agg, name)
                        if vote_col is not None:
                            result[f'{name_prefix}候選人{i}_得票數'] = pd.to_numeric(vote_col, errors='coerce').fillna(0).astype(int)
                        else:
                            result[f'{name_prefix}候選人{i}_得票數'] = 0
                
                # Mayor/Township Mayor format: all candidates have full format
                else:
                    result[f'{name_prefix}候選人{i}＿候選人名稱'] = name
                    
                    party_col_name = f'{name}_黨籍'
                    if party_col_name in df_agg.columns:
                        party_val = get_single_col(df_agg, party_col_name)
                        if party_val is not None:
                            result[f'{name_prefix}候選人{i}＿黨籍'] = party_val
                        else:
                            result[f'{name_prefix}候選人{i}＿黨籍'] = col_party_map.get(name, '')
                    else:
                        result[f'{name_prefix}候選人{i}＿黨籍'] = col_party_map.get(name, '')
                    
                    vote_col = get_single_col(df_agg, name)
                    if vote_col is not None:
                        result[f'{name_prefix}候選人{i}_得票數'] = pd.to_numeric(vote_col, errors='coerce').fillna(0).astype(int)
                    else:
                        result[f'{name_prefix}候選人{i}_得票數'] = 0


    is_muni = is_municipality(county)
    
    # Handle township representatives
    if role_prefix == '鄉鎮市民代表':
        add_cols('township_rep', '鄉鎮市民代表')
        stat_prefix = '鄉鎮市民代表'
    
    if role_prefix == '總統':
        # print(f"     [DEBUG] 總統候選人欄位: {cols_by_type['president']}")
        # print(f"     [DEBUG] col_party_map keys: {list(col_party_map.keys())}")
        print(f"     [DEBUG] result shape before add_cols: {result.shape}")
        add_cols('president', '總統')
        print(f"     [DEBUG] result shape after add_cols: {result.shape}")
        # print(f"     [DEBUG] 總統候選人1＿候選人名稱 first value: {result['總統候選人1＿候選人名稱'].iloc[0] if len(result) > 0 else 'NO ROWS'}")
        stat_prefix = '總統候選人'
    elif role_prefix == '市長':
        if is_muni:
            add_cols('mayor', '直轄市長')
            stat_prefix = '直轄市長'
        else:
            add_cols('mayor', '縣市長')
            stat_prefix = '縣市長'
    elif role_prefix == '立委':
        add_cols('regional', '區域立委')
        add_cols('plain', '平原立委')
        add_cols('mountain', '山原立委')
        stat_prefix = '區域立委'
    elif role_prefix == '議員':
        if is_muni:
            add_cols('regional', '區域直轄市議員')
            add_cols('plain', '平原直轄市議員')
            add_cols('mountain', '山原直轄市議員')
            stat_prefix = '區域直轄市議員'
        else:
            add_cols('regional', '區域縣市議員')
            add_cols('plain', '平原縣市議員')
            add_cols('mountain', '山原縣市議員')
            stat_prefix = '區域縣市議員'
    elif role_prefix == '鄉鎮市民代表':
        add_cols('township_rep', '鄉鎮市民代表')
        stat_prefix = '鄉鎮市民代表'
    else:
        stat_prefix = role_prefix

    stat_cols = ['有效票數A', '無效票數B', '投票數C', '選舉人數G']
    for col in stat_cols:
        matches = [c for c in df_agg.columns if col in c]
        if matches: 
            stat_val = get_single_col(df_agg, matches[0])
            if stat_val is not None:
                result[f'{stat_prefix}{col}'] = pd.to_numeric(stat_val, errors='coerce').fillna(0).astype(int)

    return result

def merge_dfs(dfs):
    merge_keys = ['縣市', '行政區別', '鄰里', '區域別代碼']
    valid_dfs = [d for d in dfs if d is not None and not d.empty]
    if not valid_dfs: return pd.DataFrame()
    
    result = valid_dfs[0]
    print(f"  [DEBUG merge] Starting with df0: shape={result.shape}, has總統候選人1＿候選人名稱={'總統候選人1＿候選人名稱' in result.columns}")
    print(f"    Unique merge key combos: {result[merge_keys].drop_duplicates().shape[0]}")
    if '總統候選人1＿候選人名稱' in result.columns:
        print(f"    First value: {result['總統候選人1＿候選人名稱'].iloc[0]}")
    
    for i, df in enumerate(valid_dfs[1:], 1):
        print(f"  [DEBUG merge] Merging df{i}: shape={df.shape}")
        print(f"    Unique merge key combos: {df[merge_keys].drop_duplicates().shape[0]}")
        before_cols = set(result.columns)
        
        # Check for common merge keys
        result_keys = set(result[merge_keys].apply(tuple, axis=1))
        df_keys = set(df[merge_keys].apply(tuple, axis=1))
        common_keys = result_keys & df_keys
        print(f"    Common neighborhoods: {len(common_keys)}, df0 only: {len(result_keys - df_keys)}, df{i} only: {len(df_keys - result_keys)}")
        
        result = result.merge(df, on=merge_keys, how='outer', suffixes=('', '_dup'))
        after_cols = set(result.columns)
        new_cols = after_cols - before_cols
        print(f"    After merge: shape={result.shape}, new_cols={len(new_cols)}")
        if '總統候選人1＿候選人名稱' in result.columns:
            nan_count = result['總統候選人1＿候選人名稱'].isna().sum()
            print(f"    總統候選人1＿候選人名稱 NaN count: {nan_count}/{len(result)}")
        # Remove duplicate columns from merge
        result = result.loc[:, ~result.columns.str.endswith('_dup')]
    
    # Aggregate duplicate rows by summing numeric columns
    if result.duplicated(subset=merge_keys).any():
        print(f"  [INFO] 發現重複鄰里，進行合併聚合...")
        numeric_cols = result.select_dtypes(include=[np.number]).columns.tolist()
        non_numeric_cols = [c for c in result.columns if c not in numeric_cols and c not in merge_keys]
        
        agg_dict = {c: 'sum' for c in numeric_cols}
        for c in non_numeric_cols:
            agg_dict[c] = 'first'
        
        result = result.groupby(merge_keys, as_index=False).agg(agg_dict)
    
    return result

def process_year_data(year, county, data_dir, output_dir):
    print(f"處理 {year} 年 {county} 資料...")
    # Try multiple possible paths for voteData
    votedata_path = data_dir.parent / "voteData"
    if not votedata_path.exists():
        votedata_path = data_dir.parent / "db.cec.gov.tw" / "voteData"
    pres_info = load_full_candidate_info(votedata_path, year, county, 'President')
    leg_info = load_full_candidate_info(votedata_path, year, county, 'Legislator')
    council_info = load_full_candidate_info(votedata_path, year, county, 'Councilor')
    mayor_info = load_full_candidate_info(votedata_path, year, county, 'Mayor')
    rep_info = load_full_candidate_info(votedata_path, year, county, 'TownshipRepresentative')
    
    county_dir = data_dir / county
    if not county_dir.exists(): print(f"  ⚠️ 找不到縣市資料夾: {county_dir}"); return
    dfs_to_merge = []
    
    files = list(county_dir.glob(f'{year}_總統*.csv'))
    if files:
        print(f"  讀取總統資料: {files[0].name}")
        dfs_to_merge.append(format_generic_data(pd.read_csv(files[0], encoding='utf-8-sig'), county, pres_info, '總統', 'President'))
    
    party_files = list(county_dir.glob(f'{year}_*政黨*.csv'))
    if party_files:
        print(f"  讀取不分區政黨資料: {party_files[0].name}")
        df_party = pd.read_csv(party_files[0], encoding='utf-8-sig')
        df_party = clean_df_indices(df_party)
        
        # Rename 村里別 to 鄰里 to match other dataframes
        if '村里別' in df_party.columns:
            df_party = df_party.rename(columns={'村里別': '鄰里'})
        
        party_result = pd.DataFrame()
        party_result['縣市'] = df_party.get('縣市', county)
        party_result['行政區別'] = get_single_col(df_party, '行政區別')
        party_result['鄰里'] = get_single_col(df_party, '鄰里')
        
        if '區域別代碼' in df_party.columns:
            party_result['區域別代碼'] = get_single_col(df_party, '區域別代碼')
        else:
            try:
                codes = df_party.apply(lambda row: generate_area_code(county, row['行政區別'], row['鄰里']), axis=1)
                party_result['區域別代碼'] = codes
            except:
                party_result['區域別代碼'] = ["0"] * len(df_party)
        
        ignore_cols = ['縣市', '行政區別', '村里別', '鄰里', '區域別代碼', '投開票所別']
        party_cols = [c for c in df_party.columns if c not in ignore_cols]
        for col in party_cols:
            party_result[col] = get_single_col(df_party, col)
        
        # Deduplication for party duplicates too!
        if party_result.duplicated(subset=['行政區別', '鄰里']).any():
             print(f"     [INFO] 政黨資料有重複，進行合併...")
             num_cols = [c for c in party_result.columns if c not in ['縣市', '行政區別', '鄰里', '區域別代碼']]
             agg_d = {c: 'sum' for c in num_cols}
             party_result = party_result.groupby(['縣市', '行政區別', '鄰里', '區域別代碼'], as_index=False).agg(agg_d)

        dfs_to_merge.append(party_result)
    
    # Prioritize exact match (without 選區 suffix) for legislator files
    leg_exact = county_dir / f'{year}_立法委員.csv'
    if leg_exact.exists():
        print(f"  讀取立委資料: {leg_exact.name}")
        dfs_to_merge.append(format_generic_data(pd.read_csv(leg_exact, encoding='utf-8-sig'), county, leg_info, '立委', 'Legislator'))
    else:
        files = list(county_dir.glob(f'{year}_立法委員*.csv'))
        if files:
            print(f"  讀取立委資料: {files[0].name}")
            dfs_to_merge.append(format_generic_data(pd.read_csv(files[0], encoding='utf-8-sig'), county, leg_info, '立委', 'Legislator'))
    mayor_files = list(county_dir.glob(f'{year}_*市長.csv')) 
    if mayor_files:
        print(f"  讀取市長資料: {mayor_files[0].name}")
        dfs_to_merge.append(format_generic_data(pd.read_csv(mayor_files[0], encoding='utf-8-sig'), county, mayor_info, '市長', 'Mayor'))
    
    # Councilors - may have multiple types (regional, mountain indigenous, plain indigenous)
    council_files = list(county_dir.glob(f'{year}_*議員.csv'))
    if council_files:
        for council_file in council_files:
            print(f"  讀取議員資料: {council_file.name}")
            dfs_to_merge.append(format_generic_data(pd.read_csv(council_file, encoding='utf-8-sig'), county, council_info, '議員', 'Councilor'))
    
    # Township Representatives
    rep_files = list(county_dir.glob(f'{year}_*代表.csv'))
    if rep_files:
        print(f"  讀取鄉鎮市民代表資料: {rep_files[0].name}")
        dfs_to_merge.append(format_generic_data(pd.read_csv(rep_files[0], encoding='utf-8-sig'), county, rep_info, '鄉鎮市民代表', 'TownshipRepresentative'))

    if dfs_to_merge:
        merged_df = merge_dfs(dfs_to_merge)
        
        print(f"  [DEBUG final] After merge_dfs: shape={merged_df.shape}")
        if '總統候選人1＿候選人名稱' in merged_df.columns:
            print(f"    總統候選人1＿候選人名稱 first value: {merged_df['總統候選人1＿候選人名稱'].iloc[0]}")
            print(f"    總統候選人1＿候選人名稱 NaN count: {merged_df['總統候選人1＿候選人名稱'].isna().sum()}/{len(merged_df)}")
        
        # Ensure all numeric columns (票數) are integers without decimal points
        for col in merged_df.columns:
            if '得票數' in col or '票數' in col:
                merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce').fillna(0).astype(int)
        
        print(f"  [DEBUG final] Before save: shape={merged_df.shape}")
        if '總統候選人1＿候選人名稱' in merged_df.columns:
            print(f"    總統候選人1＿候選人名稱 first value: {merged_df['總統候選人1＿候選人名稱'].iloc[0]}")
        
        output_file = output_dir / county / f'{year}_選舉資料_{county}.csv'
        output_file.parent.mkdir(parents=True, exist_ok=True)
        merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"  [OK] 已輸出: {output_file.name}")
    else:
        print(f"  [WARN] 此年份無相關選取資料，或原始檔案損毀 (如花蓮2018)。")
