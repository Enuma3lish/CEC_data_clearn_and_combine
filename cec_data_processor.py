# -*- coding: utf-8 -*-
"""
中選會選舉資料處理器
處理 voteData 資料夾中 2014 年後的選舉資料
輸出格式：依照各縣市分類，村里級別的候選人得票數統計

作者: Claude
版本: 1.0
"""

import os
import pandas as pd
import glob
import re
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')


class CECDataProcessor:
    """中選會選舉資料處理器"""

    def __init__(self, vote_data_dir, output_base_dir):
        """
        初始化處理器

        Args:
            vote_data_dir: voteData 資料夾路徑
            output_base_dir: 輸出根目錄
        """
        self.vote_data_dir = Path(vote_data_dir)
        self.output_base_dir = Path(output_base_dir)

        # 直轄市代碼（用於分類）
        self.municipality_codes = {'63', '64', '65', '66', '67', '68'}

        # 已知縣市列表
        self.known_cities = {
            '臺北市', '新北市', '桃園市', '臺中市', '臺南市', '高雄市',
            '基隆市', '新竹市', '嘉義市',
            '新竹縣', '苗栗縣', '彰化縣', '南投縣', '雲林縣', '嘉義縣',
            '屏東縣', '宜蘭縣', '花蓮縣', '臺東縣', '澎湖縣', '金門縣', '連江縣'
        }

    def get_year_from_dirname(self, dirname):
        """從資料夾名稱提取年份"""
        match = re.match(r"(\d{4})", dirname)
        if match:
            return int(match.group(1))
        return 0

    def read_cec_csv(self, filepath):
        """讀取中選會 CSV 檔案，處理各種編碼"""
        try:
            df = pd.read_csv(filepath, encoding='utf-8', dtype=str, header=None)
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(filepath, encoding='big5', dtype=str, header=None)
            except:
                return None

        # 清除單引號 - 只處理字串類型的列
        for col in df.columns:
            if df[col].dtype == 'object':  # 只處理字串列
                df[col] = df[col].astype(str).str.replace("'", "", regex=False)

        return df

    def identify_election_folders(self, year_dir):
        """識別年份資料夾中的各類選舉資料夾"""
        subfolders = [f.path for f in os.scandir(year_dir) if f.is_dir()]

        election_folders = {
            '總統': [],
            '立法委員': [],
            '直轄市議員': [],
            '非直轄市議員': [],
            '直轄市長': [],
            '非直轄市長縣長': []
        }

        for folder_path in subfolders:
            folder_name = os.path.basename(folder_path)

            # 根據資料夾名稱分類
            if "總統" in folder_name and "副總統" not in folder_name:
                election_folders['總統'].append(folder_path)

            # 立委：區域、山地、平地全部合併為「立法委員」
            elif "立委" in folder_name or "立法委員" in folder_name:
                # 不分區另外處理，區域/山地/平地都合併
                if "不分區" not in folder_name:
                    election_folders['立法委員'].append(folder_path)

            # 直轄市議員：包含山原、平原也合併
            elif "直轄市議員" in folder_name:
                election_folders['直轄市議員'].append(folder_path)

            # 直轄市山原議員、直轄市平原議員 → 直轄市議員
            elif "直轄市山原議員" in folder_name or "直轄市平原議員" in folder_name:
                election_folders['直轄市議員'].append(folder_path)

            # 縣市議員：包含山原、平原也合併
            elif "縣市議員" in folder_name or "縣市區域議員" in folder_name:
                election_folders['非直轄市議員'].append(folder_path)

            # 縣市山原議員、縣市平原議員 → 非直轄市議員
            elif "縣市山原議員" in folder_name or "縣市平原議員" in folder_name:
                election_folders['非直轄市議員'].append(folder_path)

            # 直轄市長
            elif "直轄市長" in folder_name or "直轄市市長" in folder_name:
                election_folders['直轄市長'].append(folder_path)

            # 縣市長
            elif "縣市長" in folder_name:
                election_folders['非直轄市長縣長'].append(folder_path)

        return election_folders

    def process_election_data(self, folder_path, election_type):
        """
        處理單一選舉類型的資料

        Returns:
            DataFrame: 處理後的村里級別資料
        """
        # 尋找必要檔案
        files = {
            "base": None,
            "cand": None,
            "tks": None,
            "prof": None
        }

        # 檢查標準檔案名稱和 _T1 後綴
        for prefix in ["elbase", "elcand", "elctks", "elprof"]:
            for suffix in ["", "_T1", "_P1", "_L1", "_M1"]:
                file_pattern = os.path.join(folder_path, f"{prefix}{suffix}.csv")
                if os.path.exists(file_pattern):
                    key = prefix.replace("el", "").replace("ctks", "tks")
                    files[key] = file_pattern
                    break

        # 檢查是否所有檔案都存在
        if not all(files.values()):
            print(f"    警告：{folder_path} 缺少必要檔案，跳過")
            return None

        # 讀取資料
        df_base = self.read_cec_csv(files["base"])
        df_cand = self.read_cec_csv(files["cand"])
        df_tks = self.read_cec_csv(files["tks"])
        df_prof = self.read_cec_csv(files["prof"])

        if any(df is None for df in [df_base, df_cand, df_tks, df_prof]):
            print(f"    警告：無法讀取 {folder_path} 的檔案")
            return None

        # 處理基本資料（行政區域）
        df_base.columns = ['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'LiCode', 'Name']
        # 注意：不要在這裡過濾 LiCode='0000'，因為需要用它來建立 dept_map
        # df_base = df_base[~df_base['LiCode'].isin(['0000', '0999'])]

        # 處理候選人資料
        if len(df_cand.columns) >= 16:
            df_cand.columns = ['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'ConstCode', 'CandNo',
                               'CandName', 'PartyCode', 'Gender', 'BirthDate', 'Age', 'BirthPlace',
                               'Edu', 'Incumbent', 'Won', 'Vice']
        else:
            print(f"    警告：候選人檔案欄位數不符 ({len(df_cand.columns)} 欄)")
            return None

        # 處理得票數資料
        if len(df_tks.columns) >= 10:
            df_tks.columns = ['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'LiCode',
                              'SiteId', 'CandNo', 'Votes', 'Rate', 'Won']
        else:
            print(f"    警告：得票數檔案欄位數不符 ({len(df_tks.columns)} 欄)")
            return None

        df_tks = df_tks[~df_tks['LiCode'].isin(['0000', '0999'])]
        df_tks = df_tks[~df_tks['GroupCode'].isin(['000', '00'])]
        # 優化：先轉換為字串，去除引號，再轉數值
        df_tks['Votes'] = df_tks['Votes'].astype(str).str.replace('"', '').str.replace("'", '')
        df_tks['Votes'] = pd.to_numeric(df_tks['Votes'], errors='coerce').fillna(0).astype(int)

        # 所有選舉類型都彙總到村里層級（加總同村里的所有投開票所）
        votes_agg = df_tks.groupby(['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'LiCode', 'CandNo'])['Votes'].sum().reset_index()

        # 處理投票統計資料
        if len(df_prof.columns) >= 10:
            df_prof.columns = ['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'LiCode', 'SiteId',
                               'Valid', 'Invalid', 'Total', 'Electorate'] + [f'Col{i}' for i in range(len(df_prof.columns) - 10)]
        else:
            print(f"    警告：統計檔案欄位數不符 ({len(df_prof.columns)} 欄)")
            return None

        df_prof = df_prof[~df_prof['LiCode'].isin(['0000', '0999'])]
        df_prof = df_prof[~df_prof['GroupCode'].isin(['000', '00'])]

        # 數值化統計欄位 - 優化處理
        for col in ['Valid', 'Invalid', 'Total', 'Electorate']:
            df_prof[col] = df_prof[col].astype(str).str.replace('"', '').str.replace("'", '')
            df_prof[col] = pd.to_numeric(df_prof[col], errors='coerce').fillna(0).astype(int)

        # 所有選舉類型都彙總到村里層級
        stats_agg = df_prof.groupby(['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'LiCode'])[['Valid', 'Invalid', 'Total', 'Electorate']].sum().reset_index()

        # 建立候選人映射
        # 針對總統選舉，需要將同一組號的正副總統配對
        if election_type == '總統':
            # 按照 CandNo 分組，將同組候選人組合（總統選舉是全國性的）
            cand_map = {}
            cand_groups = df_cand.groupby('CandNo')
            
            for cand_no, group_df in cand_groups:
                # 排序：Vice 欄位為空字串或 ' ' 的是正職（總統），'Y' 的是副職（副總統）
                sorted_cands = group_df.sort_values('Vice')
                names = sorted_cands['CandName'].tolist()
                
                # 組合名稱：號次_總統/副總統（加上號次避免合併時欄位名稱衝突）
                if len(names) >= 2:
                    combined_name = f"{cand_no}_{names[0]}/{names[1]}"
                else:
                    combined_name = f"{cand_no}_{names[0]}"
                
                # 總統選舉的候選人對所有縣市都通用，使用萬用 key
                cand_map[cand_no] = combined_name
        else:
            # 非總統選舉：使用號次_姓名格式
            cand_map = {}
            for _, row in df_cand.iterrows():
                # 建立多層級的key以處理不同的CityCode/DeptCode/GroupCode組合
                # 對於立委/議員選舉，添加選區前綴避免不同選區候選人混淆
                if election_type in ['直轄市議員', '非直轄市議員', '立法委員']:
                    # 處理選區編號：DeptCode 是選區編號
                    dept_code_clean = str(row['DeptCode']).replace("'", "").strip()
                    try:
                        dept_num = int(dept_code_clean)
                        if dept_num > 0:
                            const_str = str(dept_num).zfill(2)
                            cand_name = f"第{const_str}選區_{row['CandNo']}_{row['CandName']}"
                        else:
                            cand_name = f"{row['CandNo']}_{row['CandName']}"
                    except:
                        cand_name = f"{row['CandNo']}_{row['CandName']}"
                else:
                    cand_name = f"{row['CandNo']}_{row['CandName']}"
                
                # 立委和議員的候選人資料可能在不同層級
                # 候選人檔案的PrvCode可能是0或實際省碼，得票數檔案的PrvCode則是實際值
                # 因此需要建立包含兩種可能性的key組合
                prv_codes = [row['PrvCode']]
                # 處理字串格式的0（"0", "00"等）
                if row['PrvCode'] in ['0', '00', '000'] or row['PrvCode'] == 0:
                    # 如果候選人檔案的PrvCode是0，也建立63（臺灣省）、64（福建省）等可能的key
                    prv_codes.extend(['63', '64'])

                # 原住民立委的候選人檔案GroupCode=0，但得票數檔案有各鄉鎮的GroupCode
                # 因此使用'*'作為萬用符號
                group_codes = [row['GroupCode']]
                # 處理字串格式的0（"0", "00", "000"等）
                if row['GroupCode'] in ['0', '00', '000'] or row['GroupCode'] == 0:
                    group_codes.append('*')  # 萬用符號，匹配任何GroupCode
                
                # 嘗試建立多種可能的key組合
                for prv in prv_codes:
                    for grp in group_codes:
                        keys = [
                            (prv, row['CityCode'], row['DeptCode'], grp, row['CandNo']),
                            (prv, row['CityCode'], row['DeptCode'], '000', row['CandNo']),
                            (prv, row['CityCode'], '000', grp, row['CandNo']),
                            (prv, row['CityCode'], '000', '000', row['CandNo']),
                            (prv, '000', row['DeptCode'], grp, row['CandNo']),
                            (prv, '000', '000', grp, row['CandNo']),
                        ]
                        
                        for key in keys:
                            cand_map[key] = cand_name

        def get_cand_name(prv, city, dept, group, cand_no):
            """獲取候選人姓名"""
            if election_type == '總統':
                # 總統選舉：直接用號次查找
                return cand_map.get(cand_no, f"{cand_no}_候選人")
            else:
                # 其他選舉：嘗試多種key組合（包含萬用符號 '*' 用於原住民立委）
                possible_keys = [
                    (prv, city, dept, group, cand_no),
                    (prv, city, dept, '000', cand_no),
                    (prv, city, '000', group, cand_no),
                    (prv, city, '000', '000', cand_no),
                    (prv, '000', dept, group, cand_no),
                    (prv, '000', '000', group, cand_no),
                    (prv, city, dept, '00', cand_no),
                    (prv, city, '00', group, cand_no),
                    (prv, city, '00', '00', cand_no),
                    (prv, '000', '000', '000', cand_no),
                    # 萬用符號：原住民立委的候選人GroupCode=0，但得票數有各鄉鎮GroupCode
                    (prv, city, dept, '*', cand_no),
                    (prv, city, '000', '*', cand_no),
                    (prv, '000', dept, '*', cand_no),
                    (prv, '000', '000', '*', cand_no),
                ]
                
                for key in possible_keys:
                    if key in cand_map:
                        return cand_map[key]
                        
                return f"{cand_no}_候選人"

        votes_agg['CandName'] = votes_agg.apply(
            lambda x: get_cand_name(x['PrvCode'], x['CityCode'], x['DeptCode'], x['GroupCode'], x['CandNo']), axis=1
        )

        # 轉換為寬格式（每個候選人一欄），所有選舉都使用村里層級
        votes_pivot = votes_agg.pivot_table(
            index=['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'LiCode'],
            columns='CandName',
            values='Votes',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        merge_keys = ['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'LiCode']

        # 合併統計資料
        final_df = pd.merge(votes_pivot, stats_agg, on=merge_keys, how='left')

        # 建立行政區域名稱映射
        # 縣市名稱
        city_df = df_base[
            (df_base['DeptCode'].isin(['00', '000'])) &
            (df_base['GroupCode'].isin(['00', '000'])) &
            (df_base['LiCode'].isin(['00', '000', '0000']))
        ]
        city_map = city_df.set_index(['PrvCode', 'CityCode'])['Name'].to_dict()

        # 鄉鎮區名稱（包含 DeptCode）
        dept_df = df_base[
            (~df_base['DeptCode'].isin(['00', '000'])) &
            (df_base['GroupCode'].isin(['00', '000'])) &
            (df_base['LiCode'].isin(['00', '000', '0000']))
        ]
        dept_map = dept_df.set_index(['PrvCode', 'CityCode', 'DeptCode'])['Name'].to_dict()

        # 村里名稱
        li_df = df_base[~df_base['LiCode'].isin(['00', '000', '0000'])]
        li_map = li_df.set_index(['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'LiCode'])['Name'].to_dict()

        # 添加名稱欄位
        final_df['CityName'] = final_df.apply(lambda x: city_map.get((x['PrvCode'], x['CityCode']), ''), axis=1)
        final_df['DeptName'] = final_df.apply(lambda x: dept_map.get((x['PrvCode'], x['CityCode'], x['DeptCode']), ''), axis=1)
        
        # LiName映射：嘗試多種DeptCode組合（處理elbase和elctks的DeptCode不一致問題）
        def get_li_name(row):
            # 嘗試多種可能的key組合
            possible_keys = [
                (row['PrvCode'], row['CityCode'], row['DeptCode'], row['GroupCode'], row['LiCode']),
                (row['PrvCode'], row['CityCode'], 0, row['GroupCode'], row['LiCode']),  # DeptCode=0
                (row['PrvCode'], row['CityCode'], '0', row['GroupCode'], row['LiCode']),  # DeptCode='0'
                (row['PrvCode'], row['CityCode'], '00', row['GroupCode'], row['LiCode']),  # DeptCode='00'
                (row['PrvCode'], row['CityCode'], '000', row['GroupCode'], row['LiCode']),  # DeptCode='000'
            ]
            for key in possible_keys:
                if key in li_map:
                    return li_map[key]
            return ''
        
        final_df['LiName'] = final_df.apply(get_li_name, axis=1)
        
        # 添加選區資訊
        # 對於立委/議員選舉，DeptCode 是選區編號
        # 對於其他選舉，ConstCode 設為空字串
        if election_type in ['直轄市議員', '非直轄市議員', '立法委員']:
            final_df['ConstCode'] = final_df['DeptCode']
        else:
            final_df['ConstCode'] = ''

        # Debug: 檢查映射結果（已修正）
        # print(f"    Debug: dept_map 有 {len(dept_map)} 個條目")
        # print(f"    Debug: DeptName 非空的行數: {(final_df['DeptName'] != '').sum()} / {len(final_df)}")

        # 過濾無效資料
        final_df = final_df[final_df['LiName'] != '']
        final_df = final_df[final_df['LiName'].notna()]

        # 根據選舉類型過濾直轄市/非直轄市
        def get_effective_city_code(row):
            if row['CityCode'] == '000':
                return row['PrvCode']
            return row['CityCode']

        final_df['EffectiveCityCode'] = final_df.apply(get_effective_city_code, axis=1)

        if election_type in ['直轄市長', '直轄市議員']:
            final_df = final_df[final_df['EffectiveCityCode'].isin(self.municipality_codes)]
        elif election_type in ['非直轄市長縣長', '非直轄市議員']:
            final_df = final_df[~final_df['EffectiveCityCode'].isin(self.municipality_codes)]

        final_df.drop(columns=['EffectiveCityCode'], inplace=True)

        return final_df

    def add_summary_rows(self, df, city_name):
        """添加總計行"""
        # 識別候選人欄位和統計欄位
        meta_cols = ['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'LiCode', 'SiteId', 'ConstCode',
                     'CityName', 'DeptName', 'LiName']
        numeric_cols = [c for c in df.columns if c not in meta_cols]

        # 確保數值欄位為數字
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 計算各行政區總計（需要添加兩行：header 和 total）
        district_rows = []
        for dept_name, dept_df in df.groupby('DeptName'):
            if dept_name == '總計':
                continue

            first_row = dept_df.iloc[0]
            total_data = dept_df[numeric_cols].sum().to_dict()

            # 添加行政區標題行（DeptName=區名，LiName=空）
            header_row = total_data.copy()
            header_row['CityName'] = city_name
            header_row['DeptName'] = dept_name
            header_row['LiName'] = ''
            header_row['PrvCode'] = first_row['PrvCode']
            header_row['CityCode'] = first_row['CityCode']
            header_row['DeptCode'] = first_row['DeptCode']
            header_row['GroupCode'] = first_row['GroupCode']
            header_row['LiCode'] = '0000'  # 用於排序，確保在村里之前

            # 添加行政區總計行（DeptName=區名，LiName='總計'）
            total_row = total_data.copy()
            total_row['CityName'] = city_name
            total_row['DeptName'] = dept_name
            total_row['LiName'] = '總計'
            total_row['PrvCode'] = first_row['PrvCode']
            total_row['CityCode'] = first_row['CityCode']
            total_row['DeptCode'] = first_row['DeptCode']
            total_row['GroupCode'] = first_row['GroupCode']
            total_row['LiCode'] = 'ZZZZ'  # 用於排序，確保在所有村里之後

            district_rows.append(header_row)
            district_rows.append(total_row)

        # 計算總計
        grand_total = df[numeric_cols].sum().to_dict()
        grand_total['CityName'] = city_name
        grand_total['DeptName'] = '總計'
        grand_total['LiName'] = ''
        grand_total['PrvCode'] = df.iloc[0]['PrvCode']
        grand_total['CityCode'] = df.iloc[0]['CityCode']
        grand_total['DeptCode'] = 'ZZZ'
        grand_total['GroupCode'] = 'ZZZ'
        grand_total['LiCode'] = 'ZZZ'

        # 合併總計行
        totals_df = pd.DataFrame(district_rows + [grand_total])
        result_df = pd.concat([df, totals_df], ignore_index=True)

        return result_df

    def format_output(self, df, year, election_type):
        """格式化輸出，符合參考檔案格式"""
        # 計算統計欄位
        df['有效票數A'] = df['Valid']
        df['無效票數B'] = df['Invalid']
        df['投票數C'] = df['Total']
        df['已領未投票數D'] = 0
        df['發出票數E'] = df['投票數C'] + df['已領未投票數D']
        df['選舉人數G'] = df['Electorate']
        df['用餘票數F'] = df['選舉人數G'] - df['發出票數E']
        df['投票率H'] = (df['投票數C'] / df['選舉人數G'] * 100).round(2).fillna(0)

        # 排序：全市總計放在最前面，然後按行政區、村里排序
        df['DeptCode'] = df['DeptCode'].astype(str)
        df['GroupCode'] = df['GroupCode'].astype(str)
        df['LiCode'] = df['LiCode'].astype(str)

        # 分離全市總計和其他資料
        grand_total = df[df['DeptName'] == '總計'].copy()
        others = df[df['DeptName'] != '總計'].copy()

        # 其他資料按行政區、村里排序
        others = others.sort_values(by=['DeptCode', 'GroupCode', 'LiCode'])

        # 合併：全市總計在最前面
        df = pd.concat([grand_total, others], ignore_index=True)

        # 行政區別欄位去重（稀疏顯示）
        df_output = df.copy()

        # 特殊處理：全市總計的行政區別顯示為"總計"
        df_output['行政區別'] = df_output['DeptName']
        df_output['行政區別'] = df_output['行政區別'].mask(
            (df_output['DeptName'].duplicated()) & (df_output['DeptName'] != '總計'),
            ""
        )

        df_output['村里別'] = df_output['LiName']

        # 選擇輸出欄位
        candidate_cols = [c for c in df.columns if c not in [
            'PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'LiCode', 'SiteId', 'ConstCode',
            'CityName', 'DeptName', 'LiName',
            'Valid', 'Invalid', 'Total', 'Electorate',
            '有效票數A', '無效票數B', '投票數C', '已領未投票數D', '發出票數E', '用餘票數F', '選舉人數G', '投票率H'
        ]]
        
        # 候選人欄位按號次排序
        def get_cand_number(col_name):
            if '_' in col_name:
                try:
                    return int(col_name.split('_')[0])
                except:
                    return 999
            return 999
        
        candidate_cols = sorted(candidate_cols, key=get_cand_number)

        # 根據選舉類型決定輸出欄位
        if election_type == '總統':
            # 總統選舉：新增縣市代碼、鄉鎮代碼、村里代碼欄位（用於區分同名村里）
            df_output = df_output.rename(columns={
                'CityCode': '縣市代碼',
                'DeptCode': '鄉鎮市區代碼',
                'LiCode': '村里代碼'
            })
            output_cols = ['縣市代碼', '鄉鎮市區代碼', '村里代碼', '行政區別', '村里別'] + candidate_cols + [
                '有效票數A', '無效票數B', '投票數C', '已領未投票數D',
                '發出票數E', '用餘票數F', '選舉人數G', '投票率H'
            ]
        elif election_type in ['直轄市議員', '非直轄市議員', '立法委員']:
            # 議員/立委選舉：不包含投開票所別（已聚合到村里層級）
            output_cols = ['行政區別', '村里別'] + candidate_cols + [
                '有效票數A', '無效票數B', '投票數C', '已領未投票數D',
                '發出票數E', '用餘票數F', '選舉人數G', '投票率H'
            ]
        else:
            # 其他選舉：維持原有格式
            output_cols = ['行政區別', '村里別'] + candidate_cols + [
                '有效票數A', '無效票數B', '投票數C', '已領未投票數D',
                '發出票數E', '用餘票數F', '選舉人數G', '投票率H'
            ]

        return df_output[output_cols]

    def process_year(self, year_folder_name, year):
        """處理單一年份的所有選舉資料"""
        print(f"\n{'='*80}")
        print(f"處理 {year} 年選舉資料")
        print(f"{'='*80}")

        year_folder = self.vote_data_dir / year_folder_name
        if not year_folder.exists():
            print(f"找不到資料夾: {year_folder}")
            return

        # 識別選舉資料夾
        election_folders = self.identify_election_folders(year_folder)

        # 處理每種選舉類型
        for election_type, folders in election_folders.items():
            if not folders:
                continue

            print(f"\n處理 {election_type} ({len(folders)} 個資料夾)")

            # 顯示合併的資料夾
            folder_names = [os.path.basename(f) for f in folders]
            if len(folders) > 1:
                print(f"  合併來源：{', '.join(folder_names)}")

            all_data = []

            for folder in folders:
                folder_name = os.path.basename(folder)
                print(f"  讀取：{folder_name}")
                df = self.process_election_data(folder, election_type)
                if df is not None:
                    # 為每個資料夾的資料添加來源標記
                    df['_source_folder'] = folder_name
                    all_data.append(df)
                    print(f"    → 成功讀取 {len(df)} 筆村里資料")

            if not all_data:
                print(f"  沒有成功處理任何資料")
                continue

            # 合併所有資料
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"  合併後總計：{len(combined_df)} 筆投開票所資料")

            # 議員/立委選舉：按選區分檔輸出（原住民立委不分選區，但原住民議員仍需分選區）
            if election_type in ['直轄市議員', '非直轄市議員', '立法委員']:
                print(f"  按選區分檔輸出")
                
                # 判斷是否為原住民立委（立委才不分選區，議員的原住民選區還是要分）
                source_folders = combined_df['_source_folder'].unique()
                is_indigenous_legislator = False
                indigenous_type = ''
                if election_type == '立法委員' and len(source_folders) > 0:
                    folder_name = source_folders[0]
                    if '山地' in folder_name or '山原' in folder_name:
                        is_indigenous_legislator = True
                        indigenous_type = '_山地原住民'
                    elif '平地' in folder_name:
                        is_indigenous_legislator = True
                        indigenous_type = '_平地原住民'
                
                # 原住民立委：不分選區，按縣市輸出（山地/平地分別處理）
                if is_indigenous_legislator:
                    print(f"  原住民選舉：不分選區，按縣市輸出")

                    # 按來源資料夾分組（山地/平地）
                    for source_folder in combined_df['_source_folder'].unique():
                        # 判斷是山地還是平地
                        if '山地' in source_folder or '山原' in source_folder:
                            folder_indigenous_type = '_山地原住民'
                        elif '平地' in source_folder:
                            folder_indigenous_type = '_平地原住民'
                        else:
                            continue

                        # 過濾該類型的資料
                        type_data = combined_df[combined_df['_source_folder'] == source_folder].copy()
                        type_data.drop(columns=['_source_folder'], inplace=True, errors='ignore')

                        for city_name in type_data['CityName'].unique():
                            if city_name not in self.known_cities:
                                continue

                            city_data = type_data[type_data['CityName'] == city_name].copy()

                            # 添加總計行
                            city_data_with_totals = self.add_summary_rows(city_data, city_name)

                            # 格式化輸出
                            output_df = self.format_output(city_data_with_totals, year, election_type)

                            # 建立輸出資料夾
                            city_dir = self.output_base_dir / city_name
                            city_dir.mkdir(exist_ok=True)

                            # 儲存檔案
                            output_file = city_dir / f"{year}_{election_type}{folder_indigenous_type}.csv"
                            output_df.to_csv(output_file, index=False, encoding='utf-8-sig')

                            village_count = len(city_data['LiName'].unique())
                            print(f"    [OK] {city_name}/{output_file.name} ({len(output_df)} 筆，{village_count} 個村里)")
                
                # 一般議員：按選區分檔
                else:
                    for city_name in combined_df['CityName'].unique():
                        if city_name not in self.known_cities:
                            continue
                        
                        city_data = combined_df[combined_df['CityName'] == city_name].copy()
                        
                        # 檢查是否有ConstCode（選區代碼）
                        if 'ConstCode' not in city_data.columns or city_data['ConstCode'].isna().all():
                            print(f"    警告：{city_name} 沒有選區資訊，略過")
                            continue
                        
                        # 按選區分組輸出
                        for const_code in sorted(city_data['ConstCode'].unique()):
                            if pd.isna(const_code):
                                continue
                            
                            const_data = city_data[city_data['ConstCode'] == const_code].copy()
                            
                            # 移除來源標記欄位
                            const_data.drop(columns=['_source_folder'], inplace=True, errors='ignore')
                            
                            # 添加總計行
                            const_data_with_totals = self.add_summary_rows(const_data, city_name)
                            
                            # 格式化輸出
                            output_df = self.format_output(const_data_with_totals, year, election_type)
                            
                            # 建立輸出資料夾
                            city_dir = self.output_base_dir / city_name
                            city_dir.mkdir(exist_ok=True)
                            
                            # 儲存檔案（選區別）
                            # 處理選區代碼：DeptCode 是選區編號（如 '01', '02' 等）
                            try:
                                # 移除可能的引號，轉為整數
                                const_code_clean = str(const_code).replace("'", "").strip()
                                const_num = int(const_code_clean)
                                if const_num == 0:  # 如果是'000'或0，代表沒有選區區分
                                    const_str = "全區"
                                else:
                                    const_str = str(const_num).zfill(2)
                            except:
                                const_str = str(const_code).replace("'", "").strip().zfill(2)
                            
                            # 過濾候選人欄位：只保留屬於該選區的候選人
                            # 候選人欄位格式：'第XX選區_號次_姓名' 或 '號次_姓名'
                            # 如果有選區前綴，則只保留符合當前選區的欄位
                            current_prefix = f"第{const_str}選區_"
                            
                            # 找出所有候選人欄位（排除固定欄位）
                            fixed_cols = ['PrvCode', 'CityCode', 'DeptCode', 'GroupCode', 'LiCode', 'SiteId', 'ConstCode',
                                         'CityName', 'DeptName', 'LiName', '行政區別', '村里別', '投開票所別',
                                         'Valid', 'Invalid', 'Total', 'Electorate',
                                         '有效票數A', '無效票數B', '投票數C', '已領未投票數D', '發出票數E', '用餘票數F', '選舉人數G', '投票率H']
                            
                            cols_to_keep = []
                            cols_rename = {}
                            
                            for col in output_df.columns:
                                if col in fixed_cols:
                                    cols_to_keep.append(col)
                                else:
                                    # 檢查是否為候選人欄位
                                    if "選區_" in col:
                                        # 有選區前綴，檢查是否匹配當前選區
                                        if col.startswith(current_prefix):
                                            cols_to_keep.append(col)
                                            # 移除前綴，恢復 '號次_姓名' 格式
                                            cols_rename[col] = col.replace(current_prefix, "")
                                    else:
                                        # 沒有選區前綴的欄位（可能是原住民立委或其他），保留
                                        cols_to_keep.append(col)
                            
                            # 應用過濾和重命名
                            output_df = output_df[cols_to_keep].rename(columns=cols_rename)
                            
                            output_file = city_dir / f"{year}_{election_type}_第{const_str}選區.csv"
                            output_df.to_csv(output_file, index=False, encoding='utf-8-sig')
                            
                            village_count = len(const_data['LiName'].unique())
                            print(f"    [OK] {city_name}/{output_file.name} ({len(output_df)} 筆，{village_count} 個村里)")
                
            else:
                # 其他選舉（總統等）：按縣市輸出（不分選區）
                combined_df.drop(columns=['_source_folder'], inplace=True, errors='ignore')
                
                # 按縣市分組並輸出
                grouped = combined_df.groupby('CityName')

                print(f"\n  按縣市輸出：")
                for city_name, city_df in grouped:
                    if city_name not in self.known_cities:
                        print(f"    跳過未知縣市：{city_name}")
                        continue

                    # 添加總計行
                    city_df_with_totals = self.add_summary_rows(city_df, city_name)

                    # 格式化輸出
                    output_df = self.format_output(city_df_with_totals, year, election_type)

                    # 建立輸出資料夾
                    city_dir = self.output_base_dir / city_name
                    city_dir.mkdir(exist_ok=True)

                    # 儲存檔案
                    output_file = city_dir / f"{year}_{election_type}.csv"
                    output_df.to_csv(output_file, index=False, encoding='utf-8-sig')

                    village_count = len(city_df['LiName'].unique()) if 'LiName' in city_df.columns else len(city_df)
                    print(f"    [OK] {city_name}/{output_file.name} ({len(output_df)} 筆，{village_count} 個村里)")

    def run(self):
        """執行所有年份的資料處理"""
        print("="*80)
        print("中選會選舉資料處理器")
        print("="*80)
        print(f"資料來源：{self.vote_data_dir}")
        print(f"輸出目錄：{self.output_base_dir}")

        # 掃描所有年份資料夾（包含子資料夾）
        year_folders = []
        
        # 掃描第一層資料夾
        for folder in self.vote_data_dir.iterdir():
            if folder.is_dir():
                year = self.get_year_from_dirname(folder.name)
                if year >= 2014:
                    year_folders.append((folder.name, year))
        
        # 如果有 voteData/voteData/ 子資料夾，也掃描
        nested_vote_data = self.vote_data_dir / "voteData"
        if nested_vote_data.exists():
            for folder in nested_vote_data.iterdir():
                if folder.is_dir():
                    year = self.get_year_from_dirname(folder.name)
                    if year >= 2014:
                        # 使用相對路徑保存
                        relative_path = f"voteData/{folder.name}"
                        year_folders.append((relative_path, year))

        # 排序並處理
        year_folders.sort(key=lambda x: x[1])

        print(f"\n找到 {len(year_folders)} 個年份資料夾：")
        for folder_name, year in year_folders:
            print(f"  - {year}: {folder_name}")

        # 處理每個年份
        for folder_name, year in year_folders:
            self.process_year(folder_name, year)

        print(f"\n{'='*80}")
        print("處理完成！")
        print(f"{'='*80}")
        print(f"\n輸出位置：{self.output_base_dir.absolute()}")

        # 統計輸出檔案
        print("\n輸出統計：")
        for city_dir in sorted(self.output_base_dir.iterdir()):
            if city_dir.is_dir():
                files = list(city_dir.glob("*.csv"))
                print(f"  {city_dir.name}: {len(files)} 個檔案")


def main():
    """主程式"""
    # 設定路徑
    BASE_DIR = r"C:\Users\melo\OneDrive\Desktop\CEC_data_clearn_and_combine"
    VOTE_DATA_DIR = os.path.join(BASE_DIR, "voteData")
    OUTPUT_DIR = BASE_DIR  # 直接輸出到根目錄下的各縣市資料夾

    # 建立處理器並執行
    processor = CECDataProcessor(VOTE_DATA_DIR, OUTPUT_DIR)
    processor.run()


if __name__ == "__main__":
    main()
