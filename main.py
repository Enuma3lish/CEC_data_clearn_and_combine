import pandas as pd
import os
from pathlib import Path
from processors.format_converter import process_year_data

# Config
BASE_DIR = Path(r'c:\Users\melo\OneDrive\Desktop\CEC_data_clearn_and_combine')
RAW_DIR = BASE_DIR / "各縣市候選人分類"
OUTPUT_DIR = BASE_DIR / "鄰里整理輸出"
REPO_DIR = BASE_DIR / "db.cec.gov.tw" / "voteData"
# Try to use Y: drive (subst) if available to avoid path length/encoding issues
if os.path.exists(r"Y:\elctks.csv") or os.path.exists(r"Y:\T1"):
    REPO_2022_PATH = Path(r"Y:")
    print("Using Y: drive for 2022 data")
else:
    REPO_2022_PATH = REPO_DIR / "2022-111年地方公職人員選舉"

def repair_data_generic(county, year, target_filename, repo_subfolder, prv_code, city_code):
    # Standard logic for 2014/2018 (Folder naming is reliable)
    target_path = RAW_DIR / county / target_filename
    if target_path.exists() and os.path.exists(target_path) and os.path.getsize(target_path) > 1000:
        print(f"  [OK] {county} {year} valid. Skip.")
        return

    # Find Source
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

    _process_repair(source_csv, target_path, prv_code, city_code, county, year)

def repair_2022_scan(county, target_filename, prv_code, city_code):
    target_path = RAW_DIR / county / target_filename
    if target_path.exists() and os.path.exists(target_path) and os.path.getsize(target_path) > 1000:
        print(f"  [OK] {county} 2022 valid. Skip.")
        return
        
    if not REPO_2022_PATH.exists():
         print("  [ERROR] 2022 Repo Path not found!")
         return

    found_source = None
    is_councilor = "議員" in target_filename
    is_rep = "代表" in target_filename
    
    # Determine specific councilor type for 2022 (T1=regional, T2=plain, T3=mountain)
    councilor_type_folder = None
    if is_councilor:
        if "山原議員" in target_filename:
            councilor_type_folder = "T3"
        elif "平原議員" in target_filename:
            councilor_type_folder = "T2"
        elif "區域議員" in target_filename or "直轄市議員" in target_filename or "非直轄市議員" in target_filename:
            councilor_type_folder = "T1"
        # else: generic councilor file, will scan all T* folders
    
    print(f"  [SCAN] Scanning 2022 folder for {county} (Prv={prv_code}), type={'councillor' if is_councilor else ('rep' if is_rep else 'mayor')}, specific_folder={councilor_type_folder}...")
    
    # Iterate all subfolders
    for item in REPO_2022_PATH.iterdir():
        if item.is_dir():
            fname = item.name.upper()  # e.g. T1, T2, T3, C1
            
            # Type matching - skip folders that don't match the required type
            if is_rep:
                if not fname.startswith("R"):
                    continue
            elif is_councilor:
                if not fname.startswith("T"):
                    continue
                # If specific councilor type is determined, only check that folder
                if councilor_type_folder and fname != councilor_type_folder:
                    continue
            else:  # Mayor
                if not (fname.startswith("C") or fname.startswith("D")):
                    continue
            
            # Check multiple potential paths for elctks.csv (root, prv, city)
            candidates = [
                item / "elctks.csv",
                item / "prv" / "elctks.csv",
                item / "city" / "elctks.csv"
            ]
            
            # Try ALL candidates, not just the first one found
            for csv_path in candidates:
                if not csv_path.exists():
                    continue
                
                try:
                    # Headerless check - read enough rows to catch all provinces (files sorted by prv)
                    df_chk = pd.read_csv(csv_path, names=['prv','city','a','d','l','t','c','n','r','e'], 
                                        nrows=3000, header=None, quotechar='"', quoting=0)
                    
                    if df_chk.empty:
                        continue
                    
                    # Clean Prv
                    if df_chk['prv'].dtype == object:
                        df_chk['prv'] = df_chk['prv'].astype(str).str.replace("'","").str.replace('"','')
                    
                    # Check Prv match
                    if (pd.to_numeric(df_chk['prv'], errors='coerce') == int(prv_code)).any():
                        found_source = csv_path
                        break
                except Exception as e:
                    continue
                    
            if found_source:
                break
                
    if not found_source:
        print(f"  [ERROR] No source for {county} 2022 found.")
        return

    print(f"  Found source: {found_source.parent.name}")
    _process_repair(found_source, target_path, prv_code, city_code, county, 2022)

def _process_repair(source_csv, target_path, prv_code, city_code, county, year):
    try:
        col_names = ['prv_code','city_code','area_code','dept_code','li_code','tks','cand_no','ticket_num','ratio','elected']
        # Read with dtype=str for code columns to preserve leading zeros
        df_tks = pd.read_csv(source_csv, names=col_names, quotechar='"', quoting=0, header=None,
                            dtype={'area_code': str, 'dept_code': str, 'li_code': str, 'cand_no': str})
        
        for col in df_tks.columns:
            if df_tks[col].dtype == object:
                 df_tks[col] = df_tks[col].astype(str).str.replace("'", "").str.replace('"', "")

        # Handle city_code = -1 (all districts in province)
        if city_code == -1:
            df_target = df_tks[
                (pd.to_numeric(df_tks['prv_code'], errors='coerce') == int(prv_code))
            ].copy()
        else:
            df_target = df_tks[
                (pd.to_numeric(df_tks['prv_code'], errors='coerce') == int(prv_code)) & 
                (pd.to_numeric(df_tks['city_code'], errors='coerce') == int(city_code))
            ].copy()
        
        print(f"     [DEBUG] Filtered {len(df_target)} records for prv_code={prv_code}, city_code={city_code}")
        
        if df_target.empty:
             print("     [WARN] No records found.")
             return
             return

        # Metadata - try different file name patterns (2016 uses _T1 for legislator, _P1 for president)
        meta_dir = source_csv.parent
        elbase = meta_dir / "elbase.csv"
        if not elbase.exists():
            elbase = meta_dir / "elbase_T1.csv"
        if not elbase.exists():
            elbase = meta_dir / "elbase_P1.csv"
        if not elbase.exists():
            elbase = meta_dir / "elbese.csv"  # Typo in 2024 data
        elcand = meta_dir / "elcand.csv"
        if not elcand.exists():
            elcand = meta_dir / "elcand_T1.csv"
        if not elcand.exists():
            elcand = meta_dir / "elcand_P1.csv"

        if not elbase.exists() or not elcand.exists():
             print("     [ERROR] Metadata missing.")
             return

        # Try multiple encodings to handle Chinese characters properly
        # Most CEC files are UTF-8, but some older ones might be CP950/Big5
        encodings_to_try = ['utf-8', 'cp950', 'big5', 'gb2312', 'gbk']
        df_base = None
        df_cand = None
        
        for encoding in encodings_to_try:
            try:
                df_base = pd.read_csv(elbase, names=['prv_code','city_code','area_code','dept_code','li_code','name'], 
                                     quotechar='"', quoting=1, header=None, encoding=encoding, dtype={'li_code': str, 'dept_code': str})
                # elcand has more columns: prv, city, area, dept, li_code, cand_no, name, party, gender, birth, age, address, education, elected, incumbent, note
                df_cand = pd.read_csv(elcand, encoding=encoding, header=None, quotechar='"', quoting=1, dtype={'li_code': str, 'area_code': str, 'dept_code': str, 'cand_no': str})
                # Assign column names only to the columns we need
                df_cand.columns = ['prv_code','city_code','area_code','dept_code','li_code','cand_no','name','party','gender','birth','age','address','education','elected','incumbent'] + [f'extra{i}' for i in range(len(df_cand.columns) - 15)]
                
                # Clean strings
                for df in [df_base, df_cand]:
                    for col in df.columns:
                        if df[col].dtype == object:
                            df[col] = df[col].astype(str).str.replace("'", "").str.replace('"', "").str.strip()
                
                # Test if we got valid names (no replacement chars)
                if len(df_cand) > 0 and not df_cand['name'].str.contains('\ufffd', na=False).any():
                    print(f"     Using encoding: {encoding}")
                    break
            except Exception as e:
                continue
        
        if df_base is None or df_cand is None:
            print("     [WARN] Failed to decode with any encoding, using candidate numbers as fallback")
            return

        # Special handling for President and Indigenous Legislator data: candidates are national (prv=00, city=000)
        # Check if folder name is exactly "總統" or contains "立委" (indigenous legislators)
        is_president = source_csv.parent.name == "總統"
        is_indigenous_leg = "立委" in source_csv.parent.name and source_csv.parent.name != "區域立委"
        

        
        if is_president:
            # President: Base filtered by county, Candidates from national level  
            df_base = df_base[(pd.to_numeric(df_base['prv_code'], errors='coerce')==int(prv_code)) & (pd.to_numeric(df_base['city_code'], errors='coerce')==int(city_code))]
            df_cand = df_cand[(pd.to_numeric(df_cand['prv_code'], errors='coerce')==0) & (pd.to_numeric(df_cand['city_code'], errors='coerce')==0)]
        elif is_indigenous_leg:
            # Indigenous Legislator: Base filtered by county (need village data), Candidates from national level
            df_base = df_base[(pd.to_numeric(df_base['prv_code'], errors='coerce')==int(prv_code)) & (pd.to_numeric(df_base['city_code'], errors='coerce')==int(city_code))]
            df_cand = df_cand[(pd.to_numeric(df_cand['prv_code'], errors='coerce')==0) & (pd.to_numeric(df_cand['city_code'], errors='coerce')==0)]
        elif city_code == -1:
            # Regional Legislator: all districts in province
            df_base = df_base[pd.to_numeric(df_base['prv_code'], errors='coerce')==int(prv_code)]
            df_cand = df_cand[pd.to_numeric(df_cand['prv_code'], errors='coerce')==int(prv_code)]

        else:
            # Normal: specific district
            df_base = df_base[(pd.to_numeric(df_base['prv_code'], errors='coerce')==int(prv_code)) & (pd.to_numeric(df_base['city_code'], errors='coerce')==int(city_code))]
            df_cand = df_cand[(pd.to_numeric(df_cand['prv_code'], errors='coerce')==int(prv_code)) & (pd.to_numeric(df_cand['city_code'], errors='coerce')==int(city_code))]

        # Debug: Show candidate count
        print(f"     Found {len(df_cand)} candidates")


        # Convert all code columns to string to avoid type issues in concatenation
        for col in ['prv_code', 'city_code', 'area_code', 'dept_code', 'li_code']:
            if col in df_base.columns:
                df_base[col] = df_base[col].astype(str)
            if col in df_cand.columns:
                df_cand[col] = df_cand[col].astype(str)
        
        # Also convert cand_no to string
        if 'cand_no' in df_cand.columns:
            df_cand['cand_no'] = df_cand['cand_no'].astype(str)
        


        dist_map = df_base[df_base['li_code'] == '0000'].set_index('dept_code')['name'].to_dict()
        # Clean district names: remove "第XX選舉區" to avoid merge conflicts
        import re
        dist_map = {k: re.sub(r'第\d+選舉區', '', v) for k, v in dist_map.items()}
        df_base['key'] = df_base['dept_code'] + "_" + df_base['li_code']
        village_map = df_base[df_base['li_code'] != '0000'].set_index('key')['name'].to_dict()
        
        # Create party code to party name mapping (CEC standard codes)
        party_code_map = {
            '1': '中國國民黨',
            '2': '民主進步黨',
            '3': '親民黨',
            '4': '台灣團結聯盟',
            '5': '無黨團結聯盟',
            '6': '綠黨',
            '7': '新黨',
            '8': '台灣基進',
            '9': '台灣民眾黨',
            '10': '時代力量',
            '11': '一邊一國行動黨',
            '12': '勞動黨',
            '13': '中華統一促進黨',
            '14': '國會政黨聯盟',
            '15': '台澎黨',
            '16': '民主進步黨',  # 另一個DPP代碼
            '17': '社會民主黨',
            '18': '和平鴿聯盟黨',
            '19': '喜樂島聯盟',
            '20': '安定力量',
            '21': '合一行動聯盟',
            '90': '親民黨',  # 2020宋楚瑜
            '99': '無黨籍及未經政黨推薦',
            '999': '無黨籍及未經政黨推薦',  # 2014/2018使用3位數代碼
            '348': '喜樂島聯盟',  # 原住民政黨代碼
        }
        
        # Create candidate name and party maps using (area_code, cand_no) as key
        # Normalize area_code and cand_no to 2 digits to handle leading zero issues
        df_cand['area_code_norm'] = df_cand['area_code'].str.zfill(2)
        df_cand['cand_no_norm'] = df_cand['cand_no'].str.zfill(2)
        df_cand['map_key'] = df_cand['area_code_norm'] + "_" + df_cand['cand_no_norm']
        
        # Special handling for President data: combine president/vice-president names
        if is_president:
            # Group by cand_no to combine president and vice-president
            combined_names = {}
            for cand_no in df_cand['cand_no'].unique():
                cand_group = df_cand[df_cand['cand_no'] == cand_no]
                if len(cand_group) == 2:
                    # Combine as "President/Vice-President"
                    names = cand_group['name'].tolist()
                    combined_name = f"{names[0]}/{names[1]}"
                    # Use the map_key from the first row (they share the same cand_no)
                    map_key = cand_group.iloc[0]['map_key']
                    combined_names[map_key] = combined_name
                elif len(cand_group) == 1:
                    # Single candidate (shouldn't happen for president, but handle it)
                    map_key = cand_group.iloc[0]['map_key']
                    combined_names[map_key] = cand_group.iloc[0]['name']
            cand_name_map = combined_names
        else:
            cand_name_map = df_cand.set_index('map_key')['name'].to_dict()
        
        # Convert party code to party name
        df_cand['party_name'] = df_cand['party'].astype(str).map(party_code_map).fillna('無黨籍及未經政黨推薦')
        cand_party_map = df_cand.set_index('map_key')['party_name'].to_dict()
        

        
        # Convert ticket_num to numeric, replacing non-numeric with 0
        df_target['ticket_num'] = pd.to_numeric(df_target['ticket_num'], errors='coerce').fillna(0).astype(int)
        
        # Ensure code columns are strings in df_target too
        for col in ['dept_code', 'li_code', 'area_code', 'cand_no']:
            if col in df_target.columns:
                df_target[col] = df_target[col].astype(str)
        
        df_target['行政區別'] = df_target['dept_code'].map(dist_map)
        df_target['village_key'] = df_target['dept_code'] + "_" + df_target['li_code']
        df_target['村里別'] = df_target['village_key'].map(village_map)
        
        # Filter out rows with missing district or village names
        df_target = df_target.dropna(subset=['行政區別', '村里別'])
        df_target = df_target[df_target['村里別'] != '']
        # Filter out "總計" rows (summary rows)
        df_target = df_target[df_target['行政區別'] != '總計']
        df_target = df_target[df_target['村里別'] != '總計']
        
        # Create lookup key for candidate names and parties
        # Normalize to match the map keys (2-digit format)
        df_target['area_code_norm'] = df_target['area_code'].str.zfill(2)
        df_target['cand_no_norm'] = df_target['cand_no'].str.zfill(2)
        df_target['cand_lookup_key'] = df_target['area_code_norm'] + "_" + df_target['cand_no_norm']
        df_target['cand_name'] = df_target['cand_lookup_key'].map(cand_name_map)
        df_target['cand_party'] = df_target['cand_lookup_key'].map(cand_party_map)
        
        # Fallback to candidate number if name not found
        df_target['cand_name'] = df_target['cand_name'].fillna(df_target['cand_no'])
        df_target['cand_party'] = df_target['cand_party'].fillna('')
        
        # Create pivot for vote counts
        pivot = df_target.pivot_table(index=['行政區別', '村里別'], columns='cand_name', values='ticket_num', aggfunc='sum')
        pivot = pivot.reset_index()
        
        # Create name-to-party mapping for all candidates in this election
        name_party_map = df_target.groupby('cand_name')['cand_party'].first().to_dict()
        
        # Add party columns alongside vote count columns
        # For each candidate column, add a corresponding _party column
        party_columns = {}
        for cand_name in pivot.columns:
            if cand_name not in ['行政區別', '村里別']:
                party_name = name_party_map.get(cand_name, '')
                party_columns[f"{cand_name}_黨籍"] = party_name
        
        # Insert party columns into pivot (they will be constant for each candidate)
        for col_name, party_value in party_columns.items():
            pivot[col_name] = party_value
        
        # [NEW] Merge elprof statistics
        elprof = meta_dir / "elprof.csv"
        if elprof.exists():
             try:
                 # Check encoding for elprof? usually numbers, but read safely
                 stat_encoding = 'utf-8'
                 
                 df_prof = pd.read_csv(elprof, header=None, quotechar='"', quoting=0, encoding=stat_encoding, dtype=str) 
                 # Filter by Prv/City
                 df_prof = df_prof[(pd.to_numeric(df_prof[0], errors='coerce') == int(prv_code)) & (pd.to_numeric(df_prof[1], errors='coerce') == int(city_code))]
                 
                 if not df_prof.empty:
                     # Map columns
                     # 5:A, 6:B, 7:C, 8:D, 9:E, 10:F, 11:G, 12:H
                     # Join key
                     df_prof['dept_code'] = df_prof[3].astype(str)
                     df_prof['li_code'] = df_prof[4].astype(str)
                     df_prof['village_key'] = df_prof['dept_code'] + "_" + df_prof['li_code']
                     
                     df_prof['行政區別'] = df_prof['dept_code'].map(dist_map)
                     df_prof['村里別'] = df_prof['village_key'].map(village_map)
                     
                     stat_map = {5:'有效票數A', 6:'無效票數B', 7:'投票數C', 8:'已領未投票數D', 9:'發出票數E', 10:'用餘票數F', 11:'選舉人數G', 12:'投票率H'}
                     keep_cols = ['行政區別', '村里別']
                     for idx, name in stat_map.items():
                         if idx < len(df_prof.columns):
                             df_prof[name] = pd.to_numeric(df_prof[idx], errors='coerce').fillna(0).astype(int)
                             keep_cols.append(name)
                     
                     df_stats = df_prof[keep_cols].copy()
                     # Clean Duplicates in stats if any
                     df_stats = df_stats.drop_duplicates(subset=['行政區別', '村里別'])
                     
                     # Merge with pivot
                     pivot = pd.merge(pivot, df_stats, on=['行政區別', '村里別'], how='left')
             except Exception as e:
                 print(f"     [WARN] Check elprof failed: {e}")

        # Ensure all numeric columns are saved as integers without decimal points
        # Only convert statistics columns, not candidate name columns (which contain vote counts)
        stat_keywords = ['有效票數', '無效票數', '投票數', '選舉人數', '已領未投', '發出票數', '用餘票數', '投票率']
        for col in pivot.columns:
            if col not in ['行政區別', '村里別']:
                # Only convert statistics columns to int, preserve candidate name columns as-is
                if any(kw in col for kw in stat_keywords):
                    pivot[col] = pd.to_numeric(pivot[col], errors='coerce').fillna(0).astype(int)

        target_path.parent.mkdir(parents=True, exist_ok=True)
        pivot.to_csv(target_path, index=False, encoding='utf-8-sig')
        print(f"     [OK] Saved: {target_path.name}")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"     [ERROR] Error: {e}")

def repair_president_data(county, year, target_filename, prv_code, city_code):
    """
    Specific repair logic for 2020/2024 President data.
    Structure: {Year}總統立委/總統/elctks.csv
    """
    target_path = RAW_DIR / county / target_filename
    if target_path.exists() and os.path.exists(target_path) and os.path.getsize(target_path) > 1000:
        print(f"  [OK] {county} {year} President valid. Skip.")
        return

    # Find Source Folder
    match_str = str(year)
    found = [x for x in REPO_DIR.iterdir() if x.name.startswith(match_str) and "總統" in x.name]
    if not found:
        print(f"  [ERROR] Year folder {year} not found.")
        return
    year_folder = found[0]
    
    # Try different file name patterns (2016 uses elctks_P1.csv for president)
    source_csv = year_folder / "總統" / "elctks.csv"
    if not source_csv.exists():
        source_csv = year_folder / "總統" / "elctks_P1.csv"
    if not source_csv.exists():
        source_csv = year_folder / "總統" / "elctks_T1.csv"
    if not source_csv.exists():
         print(f"  [ERROR] Source elctks not found in {year_folder / '總統'}.")
         return

    print(f"  Reparing President Data {year} for {county}...")
    
    # ===  Load source data and process manually (similar to repair_legislator_data) ===
    try:
        source_dir = year_folder / "總統"
        
        # Build mapping from elbase.csv
        elbase_file = source_dir / "elbase.csv"
        dist_map = {}
        village_map = {}
        
        if elbase_file.exists():
            df_base = pd.read_csv(elbase_file, header=None, dtype=str, encoding='utf-8')
            # Filter for target prv_code and city_code
            df_base = df_base[(pd.to_numeric(df_base[0], errors='coerce') == int(prv_code)) & 
                              (pd.to_numeric(df_base[1], errors='coerce') == int(city_code))]
            
            for _, row in df_base.iterrows():
                dept_code = str(row[3])
                li_code = str(row[4])
                name = str(row[5]).strip('"')
                
                # Build dist_map (dept_code -> name for 行政區)
                if li_code == '0000':
                    dist_map[dept_code] = name
                # Build village_map (dept_code_li_code -> name for 村里)
                else:
                    village_key = f"{dept_code}_{li_code}"
                    village_map[village_key] = name
        
        # Load candidate list from elcand.csv
        cand_file = source_dir / "elcand.csv"
        candidate_map = {}
        if cand_file.exists():
            df_cand = pd.read_csv(cand_file, header=None, dtype=str, encoding='utf-8')
            df_cand = df_cand.dropna(subset=[5, 6])  # Ensure candidate number and name exist
            
            # Group by candidate number to handle president/vice president pairs
            grouped = df_cand.groupby(5)
            for cand_no, group in grouped:
                names = group[6].str.strip('"').tolist()
                party_codes = group[7].str.strip('"').unique()
                party_code = party_codes[0] if len(party_codes) > 0 else ''
                
                # Combine names with "/" for presidential pairs
                cand_name = '/'.join(names)
                
                # Map party code to party name (simplified - using code for now)
                party_map = {'1': '中國國民黨', '16': '民主進步黨', '90': '親民黨'}
                party = party_map.get(party_code, party_code)
                
                candidate_map[str(cand_no)] = {'name': cand_name, 'party': party}
        
        print(f"     Found {len(candidate_map)} candidates")
        
        # Read elctks.csv (vote data at polling station level)
        df = pd.read_csv(source_csv, header=None, encoding='utf-8', dtype=str)
        df_filtered = df[(pd.to_numeric(df[0], errors='coerce') == int(prv_code)) & 
                          (pd.to_numeric(df[1], errors='coerce') == int(city_code))].copy()
        
        print(f"     [DEBUG] Filtered {len(df_filtered)} records for prv_code={prv_code}, city_code={city_code}")
        
        if df_filtered.empty:
            print(f"     [WARN] No data found")
            return
        
        # Map codes to names
        df_filtered['dept_code'] = df_filtered[3].astype(str)
        df_filtered['li_code'] = df_filtered[4].astype(str)
        df_filtered['village_key'] = df_filtered['dept_code'] + "_" + df_filtered['li_code']
        
        df_filtered['行政區別'] = df_filtered['dept_code'].map(dist_map)
        df_filtered['村里別'] = df_filtered['village_key'].map(village_map)
        
        df_filtered = df_filtered.dropna(subset=['村里別'])
        
        if df_filtered.empty:
            print(f"     [WARN] No valid villages")
            return
        
        # Extract candidate vote columns and aggregate by village
        cand_cols = []
        agg_dict = {}
        
        for col_idx in range(5, len(df_filtered.columns)):
            col_name = str(col_idx - 4)  # Candidate number
            if col_name in candidate_map:
                cand_name = candidate_map[col_name]['name']
                party = candidate_map[col_name]['party']
                
                df_filtered[cand_name] = pd.to_numeric(df_filtered[col_idx], errors='coerce').fillna(0)
                agg_dict[cand_name] = 'sum'
                
                cand_cols.append({
                    'name': cand_name,
                    'party': party
                })
        
        # Aggregate by village
        pivot = df_filtered.groupby(['行政區別', '村里別'], as_index=False).agg(agg_dict)
        
        # Add party columns
        for cand in cand_cols:
            party_col_name = f"{cand['name']}_黨籍"
            pivot[party_col_name] = cand['party']
        
        # Load statistics from elprof.csv (polling station level, need to aggregate)
        elprof_file = source_dir / "elprof.csv"
        if elprof_file.exists():
            try:
                df_prof = pd.read_csv(elprof_file, header=None, dtype=str)
                df_prof = df_prof[(pd.to_numeric(df_prof[0], errors='coerce') == int(prv_code)) & 
                                   (pd.to_numeric(df_prof[1], errors='coerce') == int(city_code))].copy()
                
                if not df_prof.empty:
                    # Map codes
                    df_prof['dept_code'] = df_prof[3].astype(str)
                    df_prof['li_code'] = df_prof[4].astype(str)
                    df_prof['village_key'] = df_prof['dept_code'] + "_" + df_prof['li_code']
                    
                    df_prof['行政區別'] = df_prof['dept_code'].map(dist_map)
                    df_prof['村里別'] = df_prof['village_key'].map(village_map)
                    
                    # elprof column mapping (same as legislator):
                    # 6:A, 7:B, 8:C, 9:G, 10:D, 11:E, 16:F, 17:H
                    df_prof['有效票數A'] = pd.to_numeric(df_prof[6], errors='coerce').fillna(0)
                    df_prof['無效票數B'] = pd.to_numeric(df_prof[7], errors='coerce').fillna(0)
                    df_prof['投票數C'] = pd.to_numeric(df_prof[8], errors='coerce').fillna(0)
                    df_prof['選舉人數G'] = pd.to_numeric(df_prof[9], errors='coerce').fillna(0)
                    df_prof['已領未投票數D'] = pd.to_numeric(df_prof[10], errors='coerce').fillna(0)
                    df_prof['發出票數E'] = pd.to_numeric(df_prof[11], errors='coerce').fillna(0)
                    df_prof['用餘票數F'] = pd.to_numeric(df_prof[16], errors='coerce').fillna(0)
                    df_prof['投票率H'] = pd.to_numeric(df_prof[17], errors='coerce').fillna(0)
                    
                    # Aggregate by village (sum all statistics)
                    stat_agg = df_prof.groupby(['行政區別', '村里別'], as_index=False).agg({
                        '有效票數A': 'sum',
                        '無效票數B': 'sum',
                        '投票數C': 'sum',
                        '選舉人數G': 'sum',
                        '已領未投票數D': 'sum',
                        '發出票數E': 'sum',
                        '用餘票數F': 'sum',
                        '投票率H': 'max'  # Use max for percentage
                    })
                    
                    # Convert to int
                    for col in ['有效票數A', '無效票數B', '投票數C', '已領未投票數D', '發出票數E', '用餘票數F', '選舉人數G', '投票率H']:
                        stat_agg[col] = stat_agg[col].astype(int)
                    
                    # Merge with pivot
                    pivot = pd.merge(pivot, stat_agg, on=['行政區別', '村里別'], how='left')
                    
            except Exception as e:
                print(f"     [WARN] Failed to load elprof: {e}")
        
        # Convert from pivot format to standard format
        df = pivot
        
        # Identify columns
        keys = ['行政區別', '村里別']
        stat_keywords = ['有效票數', '無效票數', '投票數', '選舉人數', '已領未投', '發出票數', '用餘票數', '投票率']
        
        candidate_name_cols = []
        party_cols = []
        stat_cols = []
        
        for col in df.columns:
            if col in keys:
                continue
            elif any(kw in col for kw in stat_keywords):
                stat_cols.append(col)
            elif '_黨籍' in col or '黨籍' in col:
                party_cols.append(col)
            else:
                # Candidate name
                candidate_name_cols.append(col)
        
        # Create result
        result = df[keys].copy()
        
        # Add candidates in standard format
        for i, cand_name in enumerate(candidate_name_cols, 1):
            result[f'候選人{i}＿候選人名稱'] = cand_name
            
            # Find party column
            party_col = None
            for pcol in party_cols:
                if cand_name in pcol:
                    party_col = pcol
                    break
            
            if party_col and party_col in df.columns:
                result[f'候選人{i}＿黨籍'] = df[party_col]
            else:
                result[f'候選人{i}＿黨籍'] = ''
            
            result[f'候選人{i}_得票數'] = df[cand_name]
        
        # Add statistics
        for col in stat_cols:
            result[col] = df[col]
        
        # Save
        target_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(target_path, index=False, encoding='utf-8-sig')
        print(f"     [OK] Converted to standard format: {len(candidate_name_cols)} candidates")
        
    except Exception as e:
        import traceback
        print(f"     [ERROR] Failed: {e}")
        traceback.print_exc()


def repair_legislator_data(county, year, target_filename, prv_code, city_code):
    """
    Specific repair logic for 2020/2024 Legislator data.
    Aggregates: 區域立委, 平地立委, 山地立委
    """
    target_path = RAW_DIR / county / target_filename
    
    # Always regenerate to ensure correct statistics
    print(f"  Regenerating Legislator Data {year} for {county}...")

    # Find Source Folder
    match_str = str(year)
    found = [x for x in REPO_DIR.iterdir() if x.name.startswith(match_str) and "總統" in x.name]
    if not found:
        print(f"  [ERROR] Year folder {year} not found.")
        return
    year_folder = found[0]

    print(f"  Reparing Legislator Data {year} for {county} (Processing Regional/Plain/Mountain)...")
    
    # Subfolders to process
    subfolders = ['區域立委', '平地立委', '山地立委']
    dfs = []
    
    for sub in subfolders:
        # Try different file name patterns (2016 uses elctks_T1.csv, others use elctks.csv)
        source_csv = year_folder / sub / "elctks.csv"
        if not source_csv.exists():
            source_csv = year_folder / sub / "elctks_T1.csv"
        if not source_csv.exists():
            continue
            
        temp_path = RAW_DIR / county / f"temp_{year}_{sub}.csv"
        
        # Process and save temp
        # All legislator types: use specific city_code to ensure correct county filtering
        _process_repair(source_csv, temp_path, prv_code, city_code, county, year)
        
        if temp_path.exists():
            try:
                df = pd.read_csv(temp_path, encoding='utf-8-sig')
                
                # Identify columns by type
                keys = ['行政區別', '村里別']
                stat_keywords = ['有效票數', '無效票數', '投票數', '選舉人數', '已領未投', '發出票數', '用餘票數', '投票率']
                
                # Get candidate columns (non-key, non-stat, should be candidate names)
                stat_cols = [c for c in df.columns if any(kw in c for kw in stat_keywords)]
                candidate_cols = [c for c in df.columns if c not in keys and c not in stat_cols]
                
                # Keep only keys + candidates, drop stats (we'll recalculate later)
                df_clean = df[keys + candidate_cols].copy()
                
                print(f"     Loaded {temp_path.name}: {len(df_clean)} rows, {df_clean['行政區別'].nunique()} unique districts, samples: {list(df_clean['行政區別'].unique()[:5])}")
                
                dfs.append(df_clean)
                os.remove(temp_path) 
            except Exception as e:
                print(f"  [WARN] Failed to load {temp_path}: {e}")

    if not dfs:
        print("  [WARN] No legislator data found.")
        return

    # Smart Merge - Combine all legislator types into one CSV
    # Strategy: Merge on keys, combine all candidate columns
    keys = ['行政區別', '村里別']
    
    # Start with first dataframe
    merged = dfs[0].copy()
    
    # For subsequent dataframes, merge and combine candidate columns
    for i in range(1, len(dfs)):
        df_next = dfs[i]
        merged = pd.merge(merged, df_next, on=keys, how='outer', suffixes=('', '_dup'))
        
        # Remove duplicate columns (those with _dup suffix)
        dup_cols = [c for c in merged.columns if c.endswith('_dup')]
        merged = merged.drop(columns=dup_cols)
    
    # Fill NaN with 0 for numeric columns (candidate vote counts), but preserve party columns
    for col in merged.columns:
        if col not in keys and '黨籍' not in col:
            merged[col] = pd.to_numeric(merged[col], errors='coerce').fillna(0).astype(int)
    
    # Recalculate統計欄位 - get from one of the original sources
    # We need to re-read stats from the first temp file since we dropped them
    # Actually, let's just add basic stats that format_converter expects
    # Only count candidate vote columns (exclude party columns)
    candidate_vote_cols = [c for c in merged.columns if c not in keys and '黨籍' not in c]
    
    # Read statistics from elprof.csv (or elprof_T1.csv for 2016)
    # Aggregate stats across all three legislator types
    all_stats = []
    for sub in subfolders:
        elprof_path = year_folder / sub / "elprof.csv"
        if not elprof_path.exists():
            elprof_path = year_folder / sub / "elprof_T1.csv"
        if not elprof_path.exists():
            continue
        
        try:
            df_prof = pd.read_csv(elprof_path, header=None, encoding='utf-8-sig', dtype=str)
            # Column mapping for elprof
            df_prof.columns = ['縣市代碼', '鄉鎮代碼', '選區代碼', '鄉鎮市區代碼', '村里投開票所代碼',
                              '區域', '有效票數A', '無效票數B', '投票數C', '選舉人數G',
                              '已領未投票數D', '發出票數E', 'col12', 'col13', 'col14', 'col15',
                              '用餘票數F', '投票率H_raw', 'col18', 'col19']
            
            # Filter for this county and non-summary rows
            prv_str = str(prv_code).zfill(2)
            df_prof = df_prof[df_prof['縣市代碼'] == prv_str].copy()
            df_prof = df_prof[df_prof['村里投開票所代碼'] != '0000'].copy()
            
            # Convert to numeric
            for col in ['有效票數A', '無效票數B', '投票數C', '選舉人數G']:
                df_prof[col] = pd.to_numeric(df_prof[col], errors='coerce').fillna(0).astype(int)
            
            # Get village names from elbase (or elbase_T1 for 2016)
            elbase_path = year_folder / sub / "elbase.csv"
            if not elbase_path.exists():
                elbase_path = year_folder / sub / "elbase_T1.csv"
            if elbase_path.exists():
                df_base = pd.read_csv(elbase_path, header=None, encoding='utf-8-sig', dtype=str)
                df_base.columns = ['縣市代碼', '鄉鎮代碼', '選區代碼', '鄉鎮市區代碼', '村里投開票所代碼', '名稱']
                df_base = df_base[df_base['縣市代碼'] == prv_str].copy()
                
                # Merge to get village names
                df_prof = df_prof.merge(
                    df_base[['鄉鎮市區代碼', '村里投開票所代碼', '名稱']],
                    on=['鄉鎮市區代碼', '村里投開票所代碼'],
                    how='left'
                )
                df_prof['村里別'] = df_prof['名稱']
            
            # Aggregate by village (sum across all 區域 within a village)
            df_stats = df_prof.groupby('村里別').agg({
                '有效票數A': 'sum',
                '無效票數B': 'sum',
                '投票數C': 'sum',
                '選舉人數G': 'sum'
            }).reset_index()
            
            all_stats.append(df_stats)
        except Exception as e:
            print(f"     [WARN] Failed to read stats from {sub}/elprof.csv: {e}")
    
    # Merge all stats
    if all_stats:
        # Combine stats from all three types (sum values for same village)
        combined_stats = pd.concat(all_stats, ignore_index=True)
        combined_stats = combined_stats.groupby('村里別').agg({
            '有效票數A': 'sum',
            '無效票數B': 'sum',
            '投票數C': 'sum',
            '選舉人數G': 'sum'
        }).reset_index()
        
        # Merge with main data
        merged = merged.merge(combined_stats, on='村里別', how='left', suffixes=('', '_stat'))
        
        # Fill any missing stats with 0
        for col in ['有效票數A', '無效票數B', '投票數C', '選舉人數G']:
            if col not in merged.columns:
                merged[col] = 0
            merged[col] = merged[col].fillna(0).astype(int)
        
        # Calculate turnout if possible
        merged['投票率H'] = 0.0
        mask = merged['選舉人數G'] > 0
        if mask.any():
            merged.loc[mask, '投票率H'] = (merged.loc[mask, '投票數C'] / merged.loc[mask, '選舉人數G'] * 100).round(2)
    else:
        # Fallback if no stats available
        if candidate_vote_cols:
            merged['有效票數A'] = merged[candidate_vote_cols].sum(axis=1).astype(int)
            merged['無效票數B'] = 0
            merged['投票數C'] = merged['有效票數A'].astype(int)
            merged['選舉人數G'] = 0
            merged['投票率H'] = 0
    
    # Convert from pivot format to standard format for format_converter
    # Current format: columns are candidate names (e.g., "劉建國", "劉建國_黨籍")
    # Target format: columns are "候選人1＿候選人名稱", "候選人1＿黨籍", "候選人1_得票數"
    
    keys = ['行政區別', '村里別']
    stat_cols = ['有效票數A', '無效票數B', '投票數C', '選舉人數G', '已領未投票數D', '發出票數E', '用餘票數F', '投票率H']
    
    # Get all candidate name columns (those not in keys or stats, and not ending with _黨籍)
    candidate_name_cols = [c for c in merged.columns if c not in keys and c not in stat_cols and not c.endswith('_黨籍')]
    
    # Create a new result dataframe
    result = merged[keys].copy()
    
    # Add each candidate in order
    for i, cand_name in enumerate(candidate_name_cols, 1):
        party_col = f"{cand_name}_黨籍"
        
        result[f'候選人{i}＿候選人名稱'] = cand_name
        result[f'候選人{i}＿黨籍'] = merged[party_col] if party_col in merged.columns else ''
        result[f'候選人{i}_得票數'] = merged[cand_name]
    
    # Add statistics columns
    for stat_col in stat_cols:
        if stat_col in merged.columns:
            result[stat_col] = merged[stat_col]
    
    target_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(target_path, index=False, encoding='utf-8-sig')
    print(f"  [OK] Legislator Combined Saved: {target_path.name}")


def repair_party_list_data(county, year, target_filename, prv_code, city_code):
    """
    Repair party-list (不分區政黨) data for a specific county and year.
    Reads from voteData/{year}總統立委/不分區政黨/elctks.csv (vote data)
    """
    print(f"  [INFO] Processing Party-List data for {county} {year}...")
    
    vote_data_dir = BASE_DIR / "voteData"
    year_folder = vote_data_dir / f"{year}總統立委" / "不分區政黨"
    
    if not year_folder.exists():
        print(f"  [WARN] Party-list data folder not found: {year_folder}")
        return
    
    elctks_path = year_folder / "elctks.csv"  # Vote data file
    target_path = RAW_DIR / county / target_filename
    
    # Need legislator file for district/village names
    legislator_file = RAW_DIR / county / f"{year}_立法委員.csv"
    if not legislator_file.exists():
        print(f"  [WARN] Legislator file not found: {legislator_file}")
        return
    
    try:
        # Read legislator file to get village names
        df_leg = pd.read_csv(legislator_file, encoding='utf-8-sig')
        result = df_leg[['行政區別', '村里別']].drop_duplicates().copy()
        
        # Add county column
        result.insert(0, '縣市', county)
        
        # Read elctks: [0]=prv, [1]=city, [2]=area, [3]=dept, [4]=li, [5]=poll, [6]=party_no, [7]=votes
        df_ctks = pd.read_csv(elctks_path, encoding='utf-8', dtype=str, header=None, keep_default_na=False)
        
        # Filter by province/city
        prv_str = str(prv_code).zfill(2)
        city_str = '000' if city_code == 0 else str(city_code).zfill(3)
        df_filtered = df_ctks[(df_ctks.iloc[:, 0] == prv_str) & (df_ctks.iloc[:, 1] == city_str)].copy()
        df_filtered = df_filtered[df_filtered.iloc[:, 4] != '0000'].copy()  # Village level only
        
        # Get party names from elcand
        elcand_path = year_folder / "elcand.csv"
        party_map = {}  # party_no -> party_name
        if elcand_path.exists():
            df_cand = pd.read_csv(elcand_path, encoding='utf-8', dtype=str, header=None, keep_default_na=False)
            # elcand: [0-4]=location, [5]=party_no, [6]=party_name
            for _, row in df_cand.iterrows():
                party_no = row.iloc[5].strip()
                party_name = row.iloc[6].strip()
                if party_no and party_name:
                    party_map[party_no] = party_name
            print(f"     Found {len(party_map)} parties")
        
        # Get village names from elbase
        elbase_path = year_folder / "elbase.csv"
        village_map = {}  # (dept, li) -> village_name
        if elbase_path.exists():
            df_base = pd.read_csv(elbase_path, encoding='utf-8', dtype=str, keep_default_na=False)
            df_base_f = df_base[(df_base.iloc[:, 0] == prv_str) & (df_base.iloc[:, 1] == city_str)].copy()
            for _, row in df_base_f.iterrows():
                dept = row.iloc[3].strip()
                li = row.iloc[4].strip()
                name = row.iloc[5].strip()
                if li != '0000':
                    village_map[(dept, li)] = name
        
        # Sum votes by village and party
        df_filtered['dept'] = df_filtered.iloc[:, 3]
        df_filtered['li'] = df_filtered.iloc[:, 4]
        df_filtered['party_no'] = df_filtered.iloc[:, 6]
        df_filtered['votes'] = pd.to_numeric(df_filtered.iloc[:, 7], errors='coerce').fillna(0).astype(int)
        
        df_grouped = df_filtered.groupby(['dept', 'li', 'party_no'])['votes'].sum().reset_index()
        df_grouped['村里別'] = df_grouped.apply(lambda r: village_map.get((r['dept'], r['li']), ''), axis=1)
        
        # Pivot to get party columns
        df_pivot = df_grouped.pivot_table(
            index='村里別',
            columns='party_no',
            values='votes',
            fill_value=0,
            aggfunc='sum'
        ).reset_index()
        
        # Rename columns with party names
        df_pivot.columns = [
            party_map.get(str(col), f'政黨{col}') if col != '村里別' else col
            for col in df_pivot.columns
        ]
        
        # Merge with result
        result = result.merge(df_pivot, on='村里別', how='left')
        
        # Fill NaN and calculate sum
        party_cols = [c for c in result.columns if c not in ['縣市', '行政區別', '村里別']]
        for col in party_cols:
            result[col] = result[col].fillna(0).astype(int)
        
        if party_cols:
            result['不分區政黨有效票數A'] = result[party_cols].sum(axis=1).astype(int)
        
        # Save
        target_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(target_path, index=False, encoding='utf-8-sig')
        print(f"  [OK] Party-List Saved: {target_path.name} ({len(result)} rows, {len(party_cols)} parties)")
        
    except Exception as e:
        import traceback
        print(f"  [ERROR] Failed to process party-list data: {e}")
        traceback.print_exc()


import verify_fix_duplicates
import verify_candidate_filters
import verify_township_reps

def main():
    print("=== Election Data Processor (Robust 2022) ===")
    
    # ... (existing repair logic calls) ...
    # 2018 (Legacy Logic)
    repair_data_generic("花蓮縣", 2018, "2018_非直轄市議員.csv", "縣市區域議員", 10, 15)
    repair_data_generic("花蓮縣", 2018, "2018_山原議員.csv", "縣市山原議員", 10, 15)
    repair_data_generic("花蓮縣", 2018, "2018_平原議員.csv", "縣市平原議員", 10, 15)
    repair_data_generic("花蓮縣", 2018, "2018_縣市長.csv", "縣市市長", 10, 15)
    repair_data_generic("花蓮縣", 2018, "2018_鄉鎮市民代表.csv", "縣市鄉鎮市民代表", 10, 15)
    repair_data_generic("臺北市", 2018, "2018_直轄市議員.csv", "直轄市區域議員", 63, 0)
    repair_data_generic("臺北市", 2018, "2018_直轄市長.csv", "直轄市市長", 63, 0)
    
    # 2014 (Legacy Logic)
    repair_data_generic("花蓮縣", 2014, "2014_非直轄市議員.csv", "縣市區域議員", 10, 15)
    repair_data_generic("花蓮縣", 2014, "2014_山原議員.csv", "縣市山原議員", 10, 15)
    repair_data_generic("花蓮縣", 2014, "2014_平原議員.csv", "縣市平原議員", 10, 15)
    repair_data_generic("花蓮縣", 2014, "2014_縣市長.csv", "縣市市長", 10, 15)
    repair_data_generic("花蓮縣", 2014, "2014_鄉鎮市民代表.csv", "縣市鄉鎮市民代表", 10, 15)
    repair_data_generic("臺北市", 2014, "2014_直轄市議員.csv", "直轄市區域議員", 63, 0)
    repair_data_generic("臺北市", 2014, "2014_直轄市長.csv", "直轄市市長", 63, 0)

    # 2022 (Robust Logic)
    repair_2022_scan("花蓮縣", "2022_非直轄市議員.csv", 10, 15)
    repair_2022_scan("花蓮縣", "2022_山原議員.csv", 10, 15)
    repair_2022_scan("花蓮縣", "2022_平原議員.csv", 10, 15)
    repair_2022_scan("花蓮縣", "2022_縣市長.csv", 10, 15)
    repair_2022_scan("花蓮縣", "2022_鄉鎮市民代表.csv", 10, 15)
    repair_2022_scan("臺北市", "2022_直轄市議員.csv", 63, 0)
    repair_2022_scan("臺北市", "2022_直轄市長.csv", 63, 0)

    # 2020 / 2024 (President/Legislator/Party-List)
    # Hualien (10, -1) - Use -1 for all districts in new code system
    repair_president_data("花蓮縣", 2016, "2016_總統.csv", 10, 15)
    repair_legislator_data("花蓮縣", 2016, "2016_立法委員.csv", 10, 15)
    repair_party_list_data("花蓮縣", 2016, "2016_不分區政黨.csv", 10, 15)
    
    repair_president_data("花蓮縣", 2020, "2020_總統.csv", 10, 15)
    repair_legislator_data("花蓮縣", 2020, "2020_立法委員.csv", 10, 15)
    repair_party_list_data("花蓮縣", 2020, "2020_不分區政黨.csv", 10, 15)
    
    repair_president_data("花蓮縣", 2024, "2024_總統.csv", 10, 15)
    repair_legislator_data("花蓮縣", 2024, "2024_立法委員.csv", 10, 15)
    repair_party_list_data("花蓮縣", 2024, "2024_不分區政黨.csv", 10, 15)

    # Taipei (63, 0)
    repair_president_data("臺北市", 2016, "2016_總統.csv", 63, 0)
    repair_legislator_data("臺北市", 2016, "2016_立法委員.csv", 63, 0)
    repair_party_list_data("臺北市", 2016, "2016_不分區政黨.csv", 63, 0)
    
    repair_president_data("臺北市", 2020, "2020_總統.csv", 63, 0)
    repair_legislator_data("臺北市", 2020, "2020_立法委員.csv", 63, 0)
    repair_party_list_data("臺北市", 2020, "2020_不分區政黨.csv", 63, 0)
    
    repair_president_data("臺北市", 2024, "2024_總統.csv", 63, 0)
    repair_legislator_data("臺北市", 2024, "2024_立法委員.csv", 63, 0)
    repair_party_list_data("臺北市", 2024, "2024_不分區政黨.csv", 63, 0)


    counties = ['臺北市', '花蓮縣']
    years = [2014, 2016, 2018, 2020, 2022, 2024]

    for county in counties:
        for year in years:
            try:
                process_year_data(year, county, RAW_DIR, OUTPUT_DIR)
            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"Error {county} {year}: {e}")

    print("\n=== Processing Completed. Starting Verification... ===")
    
    try:
        verify_fix_duplicates.verify()
        verify_candidate_filters.verify()
        verify_township_reps.verify()
        print("\n=== [SUCCESS] All Verification Checks Passed! ===")
    except SystemExit as e:
        if e.code != 0:
            print("\n[FAIL] Verification Failed!")
        # Re-raise if needed, or just exit
        sys.exit(e.code)
    except Exception as e:
        print(f"\n[ERROR] Verification Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
