# -*- coding: utf-8 -*-
"""
中選會選舉資料處理系統 - 主程式
完整的資料處理與分析流程

使用方式：
    python main.py

功能選單：
    1. 處理原始資料（voteData）
    2. 合併處理後的資料
    3. 分析和驗證資料
    4. 完整流程（處理 + 合併 + 分析）
"""

import os
import sys
import pandas as pd
from pathlib import Path
from cec_data_processor import CECDataProcessor
from cec_data_merger import CECDataMerger


class CECMainSystem:
    """中選會選舉資料處理系統主程式"""

    def __init__(self):
        self.base_dir = Path(r"C:\Users\melo\OneDrive\Desktop\CEC_data_clearn_and_combine")
        self.vote_data_dir = self.base_dir / "voteData"
        self.output_dir = self.base_dir
        self.merged_dir = self.base_dir / "merged_data"

    def print_header(self):
        """顯示系統標題"""
        print("\n" + "="*80)
        print("=" + " "*78 + "=")
        print("=" + "中選會選舉資料處理系統 v1.0".center(76) + "=")
        print("=" + " "*78 + "=")
        print("="*80)

    def print_menu(self):
        """顯示主選單"""
        print("\n主選單：")
        print("  1. 處理原始資料（從 voteData 資料夾）")
        print("  2. 合併處理後的資料")
        print("  3. 分析和驗證資料")
        print("  4. 完整流程（處理 + 合併 + 分析）")
        print("  5. 查看系統狀態")
        print("  0. 離開")

    def check_environment(self):
        """檢查環境設定"""
        print("\n檢查系統環境...")

        issues = []

        # 檢查 voteData 資料夾
        if not self.vote_data_dir.exists():
            issues.append(f"❌ voteData 資料夾不存在：{self.vote_data_dir}")
        else:
            year_folders = [d for d in self.vote_data_dir.iterdir() if d.is_dir()]
            print(f"✓ voteData 資料夾存在，包含 {len(year_folders)} 個年份資料夾")

        # 檢查 Python 套件
        try:
            import pandas
            print(f"✓ pandas 已安裝（版本：{pandas.__version__}）")
        except ImportError:
            issues.append("❌ pandas 未安裝，請執行：pip install pandas")

        if issues:
            print("\n發現問題：")
            for issue in issues:
                print(f"  {issue}")
            return False
        else:
            print("\n✓ 環境檢查通過！")
            return True

    def process_data(self):
        """選項 1：處理原始資料"""
        print("\n" + "="*80)
        print("處理原始資料")
        print("="*80)

        if not self.vote_data_dir.exists():
            print(f"\n❌ 錯誤：找不到 voteData 資料夾")
            print(f"   路徑：{self.vote_data_dir}")
            return

        print(f"\n資料來源：{self.vote_data_dir}")
        print(f"輸出位置：{self.output_dir}")

        # 詢問是否繼續
        response = input("\n是否開始處理？(y/n): ").strip().lower()
        if response != 'y':
            print("已取消")
            return

        # 執行處理
        try:
            processor = CECDataProcessor(str(self.vote_data_dir), str(self.output_dir))
            processor.run()

            print("\n✓ 處理完成！")
            self.show_output_summary()

        except Exception as e:
            print(f"\n❌ 處理失敗：{e}")
            import traceback
            traceback.print_exc()

    def merge_data(self):
        """選項 2：合併處理後的資料"""
        print("\n" + "="*80)
        print("合併處理後的資料")
        print("="*80)

        print("\n選擇合併模式：")
        print("  1. 按選舉類型合併（只合併欄位相同的檔案）")
        print("  2. 所有年份合併（添加年份欄位）")

        mode_choice = input("\n請選擇模式 (1 或 2): ").strip()

        if mode_choice == '1':
            merge_mode = 'by_type'
        elif mode_choice == '2':
            merge_mode = 'all_years'
        else:
            print("無效的選項")
            return

        # 執行合併
        try:
            merger = CECDataMerger(str(self.output_dir), str(self.merged_dir))
            merger.run(merge_mode=merge_mode)

            print("\n✓ 合併完成！")
            print(f"   輸出位置：{self.merged_dir}")

        except Exception as e:
            print(f"\n❌ 合併失敗：{e}")
            import traceback
            traceback.print_exc()

    def analyze_data(self):
        """選項 3：分析和驗證資料"""
        print("\n" + "="*80)
        print("分析和驗證資料")
        print("="*80)

        # 掃描已處理的資料
        city_dirs = [d for d in self.output_dir.iterdir()
                     if d.is_dir() and d.name not in ['voteData', 'merged_data', '.venv', '.git', '.claude']]

        if not city_dirs:
            print("\n❌ 找不到已處理的資料")
            print("   請先執行「處理原始資料」")
            return

        print(f"\n找到 {len(city_dirs)} 個縣市的資料")

        # 選擇要分析的縣市
        print("\n可用的縣市：")
        for i, city_dir in enumerate(sorted(city_dirs), 1):
            csv_files = list(city_dir.glob("*.csv"))
            print(f"  {i}. {city_dir.name} ({len(csv_files)} 個檔案)")

        choice = input("\n請選擇縣市編號（或按 Enter 分析所有）: ").strip()

        if choice:
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(city_dirs):
                    selected_cities = [sorted(city_dirs)[idx]]
                else:
                    print("無效的選項")
                    return
            except ValueError:
                print("無效的輸入")
                return
        else:
            selected_cities = city_dirs

        # 執行分析
        self.perform_analysis(selected_cities)

    def perform_analysis(self, city_dirs):
        """執行資料分析"""
        print("\n" + "-"*80)
        print("資料分析結果")
        print("-"*80)

        total_records = 0
        total_files = 0

        for city_dir in sorted(city_dirs):
            csv_files = list(city_dir.glob("*.csv"))
            city_records = 0

            print(f"\n【{city_dir.name}】")

            for csv_file in sorted(csv_files):
                try:
                    df = pd.read_csv(csv_file, encoding='utf-8-sig')
                    city_records += len(df)
                    total_files += 1

                    # 找總計行
                    total_row = df[(df['村里別'] == '') & (df['行政區別'] == '總計')]

                    if len(total_row) > 0:
                        row = total_row.iloc[0]
                        print(f"  {csv_file.name}:")
                        print(f"    - 資料筆數: {len(df)}")
                        print(f"    - 投票數: {row['投票數C']:,.0f}")
                        print(f"    - 選舉人數: {row['選舉人數G']:,.0f}")
                        print(f"    - 投票率: {row['投票率H']:.2f}%")
                    else:
                        print(f"  {csv_file.name}: {len(df)} 筆資料")

                except Exception as e:
                    print(f"  ❌ {csv_file.name}: 讀取失敗 ({e})")

            total_records += city_records

        print("\n" + "-"*80)
        print(f"總計：")
        print(f"  - {len(city_dirs)} 個縣市")
        print(f"  - {total_files} 個檔案")
        print(f"  - {total_records:,} 筆資料")
        print("-"*80)

    def full_workflow(self):
        """選項 4：完整流程"""
        print("\n" + "="*80)
        print("執行完整流程")
        print("="*80)

        print("\n將依序執行：")
        print("  1. 處理原始資料")
        print("  2. 合併處理後的資料")
        print("  3. 分析和驗證資料")

        response = input("\n是否繼續？(y/n): ").strip().lower()
        if response != 'y':
            print("已取消")
            return

        # 步驟 1：處理資料
        print("\n【步驟 1/3】處理原始資料...")
        try:
            processor = CECDataProcessor(str(self.vote_data_dir), str(self.output_dir))
            processor.run()
            print("✓ 步驟 1 完成")
        except Exception as e:
            print(f"❌ 步驟 1 失敗：{e}")
            return

        # 步驟 2：合併資料
        print("\n【步驟 2/3】合併資料...")
        try:
            merger = CECDataMerger(str(self.output_dir), str(self.merged_dir))
            merger.run(merge_mode='by_type')
            print("✓ 步驟 2 完成")
        except Exception as e:
            print(f"❌ 步驟 2 失敗：{e}")

        # 步驟 3：分析資料
        print("\n【步驟 3/3】分析資料...")
        city_dirs = [d for d in self.output_dir.iterdir()
                     if d.is_dir() and d.name not in ['voteData', 'merged_data', '.venv', '.git', '.claude']]
        if city_dirs:
            self.perform_analysis(city_dirs)
            print("✓ 步驟 3 完成")

        print("\n" + "="*80)
        print("✓ 完整流程執行完成！")
        print("="*80)

    def show_status(self):
        """選項 5：查看系統狀態"""
        print("\n" + "="*80)
        print("系統狀態")
        print("="*80)

        # voteData 狀態
        print(f"\n【原始資料】")
        if self.vote_data_dir.exists():
            year_folders = [d for d in self.vote_data_dir.iterdir() if d.is_dir()]
            print(f"  位置: {self.vote_data_dir}")
            print(f"  年份數量: {len(year_folders)}")
            for folder in sorted(year_folders):
                print(f"    - {folder.name}")
        else:
            print(f"  ❌ voteData 資料夾不存在")

        # 處理後資料狀態
        print(f"\n【處理後資料】")
        city_dirs = [d for d in self.output_dir.iterdir()
                     if d.is_dir() and d.name not in ['voteData', 'merged_data', '.venv', '.git', '.claude']]

        if city_dirs:
            print(f"  縣市數量: {len(city_dirs)}")
            total_files = 0
            for city_dir in sorted(city_dirs):
                csv_files = list(city_dir.glob("*.csv"))
                total_files += len(csv_files)
                print(f"    - {city_dir.name}: {len(csv_files)} 個檔案")
            print(f"  總檔案數: {total_files}")
        else:
            print(f"  ❌ 尚無處理後的資料")

        # 合併後資料狀態
        print(f"\n【合併後資料】")
        if self.merged_dir.exists():
            merged_cities = [d for d in self.merged_dir.iterdir() if d.is_dir()]
            if merged_cities:
                print(f"  位置: {self.merged_dir}")
                print(f"  縣市數量: {len(merged_cities)}")
                total_merged = 0
                for city_dir in sorted(merged_cities):
                    csv_files = list(city_dir.glob("*.csv"))
                    total_merged += len(csv_files)
                    print(f"    - {city_dir.name}: {len(csv_files)} 個檔案")
                print(f"  總檔案數: {total_merged}")
            else:
                print(f"  ❌ 尚無合併後的資料")
        else:
            print(f"  ❌ merged_data 資料夾不存在")

    def show_output_summary(self):
        """顯示輸出摘要"""
        print("\n" + "-"*80)
        print("輸出摘要")
        print("-"*80)

        city_dirs = [d for d in self.output_dir.iterdir()
                     if d.is_dir() and d.name not in ['voteData', 'merged_data', '.venv', '.git', '.claude']]

        if city_dirs:
            print(f"\n已輸出 {len(city_dirs)} 個縣市的資料：")
            for city_dir in sorted(city_dirs):
                csv_files = list(city_dir.glob("*.csv"))
                print(f"  {city_dir.name}: {len(csv_files)} 個檔案")

    def run(self):
        """主程式執行"""
        self.print_header()

        # 檢查環境
        if not self.check_environment():
            print("\n請解決上述問題後重新執行")
            return

        # 主迴圈
        while True:
            self.print_menu()

            try:
                choice = input("\n請選擇功能 (0-5): ").strip()

                if choice == '1':
                    self.process_data()
                elif choice == '2':
                    self.merge_data()
                elif choice == '3':
                    self.analyze_data()
                elif choice == '4':
                    self.full_workflow()
                elif choice == '5':
                    self.show_status()
                elif choice == '0':
                    print("\n感謝使用！再見！")
                    break
                else:
                    print("\n無效的選項，請重新選擇")

            except KeyboardInterrupt:
                print("\n\n已中斷")
                break
            except Exception as e:
                print(f"\n發生錯誤：{e}")
                import traceback
                traceback.print_exc()

                response = input("\n是否繼續？(y/n): ").strip().lower()
                if response != 'y':
                    break


def main():
    """程式進入點"""
    system = CECMainSystem()
    system.run()


if __name__ == "__main__":
    main()
