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

        # 彙總村里級別得票數
        votes_agg = df_tks.groupby(['PrvCode', 'CityCode', 'GroupCode', 'LiCode', 'CandNo'])['Votes'].sum().reset_index()

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

        stats_agg = df_prof.groupby(['PrvCode', 'CityCode', 'GroupCode', 'LiCode'])[['Valid', 'Invalid', 'Total', 'Electorate']].sum().reset_index()

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
                
                # 組合名稱：總統/副總統
                if len(names) >= 2:
                    combined_name = f"{names[0]}/{names[1]}"
                else:
                    combined_name = names[0]
                
                # 總統選舉的候選人對所有縣市都通用，使用萬用 key
                cand_map[cand_no] = combined_name
        else:
            # 非總統選舉：直接使用候選人姓名
            cand_map = {}
            for _, row in df_cand.iterrows():
                key = (row['CityCode'], row['GroupCode'], row['CandNo'])
                cand_map[key] = row['CandName']

        def get_cand_name(city, group, cand_no):
            """獲取候選人姓名"""
            if election_type == '總統':
                # 總統選舉：直接用號次查找
                return cand_map.get(cand_no, f"候選人_{cand_no}")
            else:
                # 其他選舉：按縣市、選區查找
                for key_group in [group, '000', '00']:
                    key = (city, key_group, cand_no)
                    if key in cand_map:
                        return cand_map[key]
                return f"候選人_{cand_no}"

        votes_agg['CandName'] = votes_agg.apply(
            lambda x: get_cand_name(x['CityCode'], x['GroupCode'], x['CandNo']), axis=1
        )

        # 轉換為寬格式（每個候選人一欄）
        votes_pivot = votes_agg.pivot_table(
            index=['PrvCode', 'CityCode', 'GroupCode', 'LiCode'],
            columns='CandName',
            values='Votes',
            aggfunc='sum',
            fill_value=0
        ).reset_index()

        # 合併統計資料
        final_df = pd.merge(votes_pivot, stats_agg, on=['PrvCode', 'CityCode', 'GroupCode', 'LiCode'], how='left')

        # 建立行政區域名稱映射
        # 縣市名稱
        city_df = df_base[
            (df_base['DeptCode'].isin(['00', '000'])) &
            (df_base['GroupCode'].isin(['00', '000'])) &
            (df_base['LiCode'].isin(['00', '000', '0000']))
        ]
        city_map = city_df.set_index(['PrvCode', 'CityCode'])['Name'].to_dict()

        # 鄉鎮區名稱
        dept_df = df_base[
            (df_base['DeptCode'].isin(['00', '000'])) &
            (~df_base['GroupCode'].isin(['00', '000'])) &
            (df_base['LiCode'].isin(['00', '000', '0000']))
        ]
        dept_map = dept_df.set_index(['PrvCode', 'CityCode', 'GroupCode'])['Name'].to_dict()

        # 村里名稱
        li_df = df_base[~df_base['LiCode'].isin(['00', '000', '0000'])]
        li_map = li_df.set_index(['PrvCode', 'CityCode', 'GroupCode', 'LiCode'])['Name'].to_dict()

        # 添加名稱欄位
        final_df['CityName'] = final_df.apply(lambda x: city_map.get((x['PrvCode'], x['CityCode']), ''), axis=1)
        final_df['DeptName'] = final_df.apply(lambda x: dept_map.get((x['PrvCode'], x['CityCode'], x['GroupCode']), ''), axis=1)
        final_df['LiName'] = final_df.apply(lambda x: li_map.get((x['PrvCode'], x['CityCode'], x['GroupCode'], x['LiCode']), ''), axis=1)

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
        meta_cols = ['PrvCode', 'CityCode', 'GroupCode', 'LiCode', 'CityName', 'DeptName', 'LiName']
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
            header_row['GroupCode'] = first_row['GroupCode']
            header_row['LiCode'] = '0000'  # 用於排序，確保在村里之前

            # 添加行政區總計行（DeptName=區名，LiName='總計'）
            total_row = total_data.copy()
            total_row['CityName'] = city_name
            total_row['DeptName'] = dept_name
            total_row['LiName'] = '總計'
            total_row['PrvCode'] = first_row['PrvCode']
            total_row['CityCode'] = first_row['CityCode']
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

        # 排序：全市總計放在最前面，然後按行政區和村里排序
        df['GroupCode'] = df['GroupCode'].astype(str)
        df['LiCode'] = df['LiCode'].astype(str)

        # 分離全市總計和其他資料
        grand_total = df[df['DeptName'] == '總計'].copy()
        others = df[df['DeptName'] != '總計'].copy()

        # 其他資料按行政區和村里排序
        others = others.sort_values(by=['GroupCode', 'LiCode'])

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
            'PrvCode', 'CityCode', 'GroupCode', 'LiCode', 'CityName', 'DeptName', 'LiName',
            'Valid', 'Invalid', 'Total', 'Electorate',
            '有效票數A', '無效票數B', '投票數C', '已領未投票數D', '發出票數E', '用餘票數F', '選舉人數G', '投票率H'
        ]]

        output_cols = ['行政區別', '村里別'] + sorted(candidate_cols) + [
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
                    all_data.append(df)
                    print(f"    → 成功讀取 {len(df)} 筆村里資料")

            if not all_data:
                print(f"  沒有成功處理任何資料")
                continue

            # 合併所有資料
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"  合併後總計：{len(combined_df)} 筆村里資料")

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

                print(f"    [OK] {city_name}/{output_file.name} ({len(output_df)} 筆，{len(city_df)} 個村里)")

    def run(self):
        """執行所有年份的資料處理"""
        print("="*80)
        print("中選會選舉資料處理器")
        print("="*80)
        print(f"資料來源：{self.vote_data_dir}")
        print(f"輸出目錄：{self.output_base_dir}")

        # 掃描所有年份資料夾
        year_folders = []
        for folder in self.vote_data_dir.iterdir():
            if folder.is_dir():
                year = self.get_year_from_dirname(folder.name)
                if year >= 2014:
                    year_folders.append((folder.name, year))

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
