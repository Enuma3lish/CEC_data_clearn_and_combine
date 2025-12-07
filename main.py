#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
中選會選舉資料統一處理系統（2010-2024）
主程式入口

使用說明：
    python3 main.py              # 處理所有年份資料
    python3 main.py test         # 測試CSV格式檢測
    python3 main.py --year 2020  # 只處理指定年份
"""

import sys
import shutil
from pathlib import Path
from processors.process_election_unified import UnifiedElectionProcessor, test_csv_reading


def main():
    """主程式入口"""
    base_path = Path('/Users/melowu/Desktop/CEC_data_clearn_and_combine/各縣市候選人分類/votedata/voteData')
    
    # 設定輸出資料夾為「全國各種選舉資料整理」
    output_folder = Path('/Users/melowu/Desktop/CEC_data_clearn_and_combine/全國各種選舉資料整理')
    
    # 如果資料夾存在，先刪除
    if output_folder.exists():
        print(f"刪除現有輸出資料夾: {output_folder}")
        shutil.rmtree(output_folder)
    
    # 建立新的輸出資料夾
    output_folder.mkdir(parents=True, exist_ok=True)
    print(f"建立輸出資料夾: {output_folder}\n")

    # 檢查參數
    if len(sys.argv) > 1:
        if sys.argv[1] == 'test':
            # 測試模式：測試CSV格式檢測
            print("=== CSV格式檢測測試 ===\n")
            test_csv_reading()
            return
        elif sys.argv[1] == '--year' and len(sys.argv) > 2:
            # 處理指定年份
            year = sys.argv[2]
            print(f"=== 處理 {year} 年資料 ===\n")
            processor = UnifiedElectionProcessor(base_path, output_base=output_folder)

            # 根據年份找到對應的資料夾
            year_folder_map = {
                '1996': '9任總統',
                '1998': '3屆立委',
                '2004': '5屆立委',
                '2010': '2010鄉鎮市民代表',
                '2010_5du': '20101127-五都市長議員及里長',
                '2012': '20120114-總統及立委',
                '2014': '2014-103年地方公職人員選舉',
                '2016': '2016總統立委',
                '2018': '2018-107年地方公職人員選舉',
                '2020': '2020總統立委',
                '2022': '2022-111年地方公職人員選舉',
                '2024': '2024總統立委',
            }

            if year not in year_folder_map:
                print(f"錯誤：未知年份 {year}")
                print(f"可用年份：{', '.join(year_folder_map.keys())}")
                return

            folder_name = year_folder_map[year]
            folder_path = base_path / folder_name

            if not folder_path.exists():
                print(f"錯誤：資料夾不存在 - {folder_path}")
                return

            config = processor.config.FOLDER_MAPPINGS.get(folder_name)
            if config:
                structure = config['structure']
                if structure == 'old':
                    processor.process_old_format(folder_path, config)
                elif structure == 'chinese_folder_quote_prefix':
                    processor.process_chinese_folder_quote_prefix(folder_path, config)
                elif structure == 'chinese_folder_quote':
                    processor.process_chinese_folder_quote(folder_path, config)
                elif structure == 'special_2016':
                    processor.process_2016_election(folder_path, config)
                elif structure == 'code_system_2022':
                    processor.process_2022_election(folder_path, config)

                print(f"\n處理完成！輸出目錄: {processor.output_base}")
            else:
                print(f"錯誤：無法找到年份配置")
            return

    # 預設：處理所有年份
    print("=== 中選會選舉資料統一處理系統（2010-2024）===\n")
    processor = UnifiedElectionProcessor(base_path, output_base=output_folder)

    # 處理voteData原始資料
    processor.process_all_years()

    # 匯入各縣市候選人分類資料夾中已處理好的資料
    # 注意：停用此功能，因為預處理檔案含有錯誤（候選人資料重複、命名錯誤等）
    # 所有資料都從voteData原始資料重新處理
    # processor.import_preprocessed_counties()

    # 合併各縣市資料
    processor.merge_all_counties()

    print(f"\n處理完成！")
    print(f"輸出目錄: {processor.output_base}")
    print(f"  - 各縣市資料夾內包含所有年份的分類檔案")
    print(f"  - {{縣市名稱}}_選舉整理_完成版.csv 為該縣市所有資料合併檔案")
    print(f"查看詳細說明: 統一處理系統說明.md")


if __name__ == '__main__':
    main()
