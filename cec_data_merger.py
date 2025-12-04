# -*- coding: utf-8 -*-
"""
中選會選舉資料合併器
合併相同村里的不同年份/選舉類型資料
規則：相同村里別但欄位不同就不合併

作者: Claude
版本: 1.0
"""

import os
import pandas as pd
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')


class CECDataMerger:
    """選舉資料合併器"""

    def __init__(self, base_dir, output_dir):
        """
        初始化合併器

        Args:
            base_dir: 資料來源根目錄（包含各縣市資料夾）
            output_dir: 合併後輸出目錄
        """
        self.base_dir = Path(base_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def get_column_signature(self, df):
        """
        獲取 DataFrame 的欄位簽名（用於判斷是否可合併）

        Returns:
            tuple: 排除統計欄位後的候選人欄位集合
        """
        # 排除統計欄位
        exclude_cols = {
            '行政區別', '村里別',
            '有效票數A', '無效票數B', '投票數C', '已領未投票數D',
            '發出票數E', '用餘票數F', '選舉人數G', '投票率H'
        }

        # 獲取候選人欄位
        candidate_cols = set(df.columns) - exclude_cols

        return frozenset(candidate_cols)

    def can_merge(self, df1, df2):
        """
        判斷兩個 DataFrame 是否可以合併

        Args:
            df1: 第一個 DataFrame
            df2: 第二個 DataFrame

        Returns:
            bool: 是否可以合併
        """
        sig1 = self.get_column_signature(df1)
        sig2 = self.get_column_signature(df2)

        # 欄位完全相同才能合併
        return sig1 == sig2

    def merge_dataframes(self, dfs, merge_info):
        """
        合併多個 DataFrame

        Args:
            dfs: DataFrame 列表
            merge_info: 合併資訊列表（包含年份、選舉類型）

        Returns:
            DataFrame: 合併後的資料
        """
        if not dfs:
            return None

        if len(dfs) == 1:
            return dfs[0]

        # 檢查是否所有 DataFrame 都可以合併
        base_sig = self.get_column_signature(dfs[0])
        for i, df in enumerate(dfs[1:], 1):
            if self.get_column_signature(df) != base_sig:
                print(f"    警告：第 {i+1} 個檔案欄位不同，無法合併")
                return None

        # 合併資料
        merged_df = pd.concat(dfs, ignore_index=True)

        # 按行政區別和村里別排序
        merged_df = merged_df.sort_values(by=['行政區別', '村里別'])

        return merged_df

    def merge_by_election_type(self, city_name):
        """
        按選舉類型合併單一縣市的資料

        Args:
            city_name: 縣市名稱
        """
        city_dir = self.base_dir / city_name
        if not city_dir.exists():
            print(f"  找不到縣市資料夾：{city_name}")
            return

        csv_files = list(city_dir.glob("*.csv"))
        if not csv_files:
            print(f"  {city_name}：沒有 CSV 檔案")
            return

        print(f"\n處理 {city_name} ({len(csv_files)} 個檔案)")

        # 按選舉類型分組
        files_by_type = {}

        for file_path in csv_files:
            # 解析檔案名稱：年份_選舉類型.csv
            parts = file_path.stem.split('_', 1)
            if len(parts) != 2:
                print(f"    警告：無法解析檔案名稱 {file_path.name}")
                continue

            year, election_type = parts
            if election_type not in files_by_type:
                files_by_type[election_type] = []

            files_by_type[election_type].append({
                'year': year,
                'path': file_path,
                'election_type': election_type
            })

        # 處理每種選舉類型
        for election_type, files_info in files_by_type.items():
            print(f"  {election_type}: {len(files_info)} 個檔案")

            if len(files_info) == 1:
                # 只有一個檔案，直接複製
                src = files_info[0]['path']
                dst = self.output_dir / city_name / f"{election_type}_合併.csv"
                dst.parent.mkdir(exist_ok=True)

                df = pd.read_csv(src, encoding='utf-8-sig')
                df.to_csv(dst, index=False, encoding='utf-8-sig')
                print(f"    已複製：{dst.name}")
                continue

            # 多個檔案，嘗試合併
            dfs = []
            merge_info = []

            for file_info in sorted(files_info, key=lambda x: x['year']):
                df = pd.read_csv(file_info['path'], encoding='utf-8-sig')
                dfs.append(df)
                merge_info.append(file_info)

            # 檢查並合併
            if len(dfs) > 1:
                # 檢查第一個和第二個是否可以合併
                if not self.can_merge(dfs[0], dfs[1]):
                    print(f"    警告：{election_type} 的檔案欄位不同，無法合併")
                    print(f"          已保留原始檔案")
                    continue

            merged_df = self.merge_dataframes(dfs, merge_info)

            if merged_df is not None:
                # 儲存合併後的檔案
                output_file = self.output_dir / city_name / f"{election_type}_合併.csv"
                output_file.parent.mkdir(exist_ok=True)

                merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')

                years = [info['year'] for info in merge_info]
                print(f"    已合併：{election_type} ({', '.join(years)}) -> {output_file.name}")

    def merge_all_years_by_type(self, city_name):
        """
        將相同選舉類型的所有年份資料合併成一個檔案

        Args:
            city_name: 縣市名稱
        """
        city_dir = self.base_dir / city_name
        if not city_dir.exists():
            return

        csv_files = list(city_dir.glob("*.csv"))
        if not csv_files:
            return

        # 按選舉類型分組
        files_by_type = {}

        for file_path in csv_files:
            parts = file_path.stem.split('_', 1)
            if len(parts) != 2:
                continue

            year, election_type = parts
            if election_type not in files_by_type:
                files_by_type[election_type] = []

            files_by_type[election_type].append({
                'year': year,
                'path': file_path
            })

        # 合併每種選舉類型
        output_city_dir = self.output_dir / city_name
        output_city_dir.mkdir(exist_ok=True)

        for election_type, files_info in files_by_type.items():
            # 讀取所有年份的資料
            all_dfs = []
            years = []

            for file_info in sorted(files_info, key=lambda x: x['year']):
                df = pd.read_csv(file_info['path'], encoding='utf-8-sig')

                # 添加年份欄位
                df.insert(0, '年份', file_info['year'])

                all_dfs.append(df)
                years.append(file_info['year'])

            # 合併所有資料
            merged_df = pd.concat(all_dfs, ignore_index=True)

            # 儲存
            output_file = output_city_dir / f"{election_type}_所有年份.csv"
            merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')

            print(f"  {election_type}: 已合併 {len(years)} 個年份 ({', '.join(years)})")

    def generate_summary(self):
        """生成合併摘要報告"""
        print("\n" + "="*80)
        print("合併摘要")
        print("="*80)

        for city_dir in sorted(self.output_dir.iterdir()):
            if not city_dir.is_dir():
                continue

            files = list(city_dir.glob("*.csv"))
            if not files:
                continue

            print(f"\n{city_dir.name}:")
            for file in sorted(files):
                df = pd.read_csv(file, encoding='utf-8-sig')
                print(f"  {file.name}: {len(df)} 筆資料")

    def run(self, merge_mode='by_type'):
        """
        執行合併

        Args:
            merge_mode: 合併模式
                - 'by_type': 按選舉類型合併（相同類型合併）
                - 'all_years': 將所有年份合併成一個檔案
        """
        print("="*80)
        print("中選會選舉資料合併器")
        print("="*80)
        print(f"資料來源：{self.base_dir}")
        print(f"輸出目錄：{self.output_dir}")
        print(f"合併模式：{merge_mode}")

        # 掃描所有縣市資料夾
        city_dirs = [d for d in self.base_dir.iterdir() if d.is_dir()]

        print(f"\n找到 {len(city_dirs)} 個縣市資料夾")

        # 處理每個縣市
        for city_dir in sorted(city_dirs):
            if merge_mode == 'by_type':
                self.merge_by_election_type(city_dir.name)
            elif merge_mode == 'all_years':
                print(f"\n處理 {city_dir.name}")
                self.merge_all_years_by_type(city_dir.name)

        # 生成摘要
        self.generate_summary()

        print("\n" + "="*80)
        print("合併完成！")
        print("="*80)


def main():
    """主程式"""
    BASE_DIR = r"C:\Users\melo\OneDrive\Desktop\CEC_data_clearn_and_combine"
    OUTPUT_DIR = os.path.join(BASE_DIR, "merged_data")

    # 建立合併器
    merger = CECDataMerger(BASE_DIR, OUTPUT_DIR)

    # 執行合併（選擇合併模式）
    print("\n請選擇合併模式：")
    print("1. 按選舉類型合併（預設）")
    print("2. 將所有年份合併成一個檔案")

    choice = input("\n請輸入選項 (1 或 2，直接按 Enter 使用預設): ").strip()

    if choice == '2':
        merger.run(merge_mode='all_years')
    else:
        merger.run(merge_mode='by_type')


if __name__ == "__main__":
    main()
