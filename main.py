#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
選舉資料處理系統 - 主程式
Taiwan CEC Election Data Processor

功能：
- 處理 2014 地方公職人員選舉資料（縣市議員、縣市首長、鄉鎮市長）
- 處理 2016 總統立委選舉資料（總統、區域立委、山地/平地原住民立委、政黨票）
- 處理 2018 地方公職人員選舉資料（縣市議員、縣市首長、鄉鎮市長）
- 處理 2020 總統立委選舉資料（總統、區域立委、山地/平地原住民立委、政黨票）
- 處理 2022 地方公職人員選舉資料（縣市議員、縣市首長、鄉鎮市長）
- 處理 2024 總統立委選舉資料（總統、區域立委、山地/平地原住民立委、政黨票）
- 輸出各縣市 Excel 檔案及全縣市合併版本

Usage:
    python main.py                    # 處理所有年份
    python main.py --year 2014        # 只處理 2014 年
    python main.py --year 2020        # 只處理 2020 年
    python main.py --merge-national   # 只合併全國選舉資料
"""

import argparse
import sys

sys.stdout.reconfigure(encoding='utf-8')

from election_processor import (
    # Config
    DATA_DIR,
    OUTPUT_DIR,
    YEAR_FOLDERS,
    MUNICIPALITIES,
    COUNTIES,
    ALL_CITIES,
    # Processor functions
    process_council_municipality,
    process_council_county,
    process_mayor_municipality,
    process_mayor_county,
    process_legislator,
    process_president,
    process_township_mayor,
    process_indigenous_legislator,
    process_party_vote,
    # Output functions
    save_council_excel,
    save_mayor_excel,
    save_legislator_excel,
    save_president_excel,
    save_township_mayor_excel,
    save_indigenous_legislator_excel,
    save_party_vote_excel,
    create_city_combined_file,
    create_national_election_file,
)

# 支援的年份
LOCAL_ELECTION_YEARS = [2014, 2018, 2022]  # 地方公職人員選舉
NATIONAL_ELECTION_YEARS = [2016, 2020, 2024]  # 總統立委選舉
ALL_YEARS = sorted(LOCAL_ELECTION_YEARS + NATIONAL_ELECTION_YEARS)


def process_local_election(year):
    """處理地方公職人員選舉資料（縣市議員、縣市首長、鄉鎮市長）"""
    year_folder = YEAR_FOLDERS.get(year)
    if not year_folder:
        print(f"  [ERROR] 找不到 {year} 年的資料夾設定")
        return

    base_dir = DATA_DIR / year_folder

    # 2022 年使用代碼格式資料夾
    if year == 2022:
        council_muni_folder = 'T1/prv'
        council_county_folder = 'T1/city'
        mayor_muni_folder = 'C1/prv'
        mayor_county_folder = 'C1/city'
        township_folder = 'D1'
    else:
        council_muni_folder = '直轄市區域議員'
        council_county_folder = '縣市區域議員'
        mayor_muni_folder = '直轄市市長'
        mayor_county_folder = '縣市市長'
        township_folder = '縣市鄉鎮市長'

    # 處理直轄市區域議員
    print(f"\n{'=' * 60}")
    print(f"處理 {year} 直轄市區域議員選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in MUNICIPALITIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / council_muni_folder
        results = process_council_municipality(str(data_dir), prv_code, city_name)

        if results:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'{year}_直轄市區域議員_各投開票所得票數_{city_name}.xlsx'
            save_council_excel(results, str(output_path), city_name, year, '直轄市區域議員選舉')

    # 處理縣市區域議員
    print(f"\n{'=' * 60}")
    print(f"處理 {year} 縣市區域議員選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in COUNTIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / council_county_folder
        results = process_council_county(str(data_dir), prv_code, city_code, city_name)

        if results:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'{year}_縣市區域議員_各投開票所得票數_{city_name}.xlsx'
            save_council_excel(results, str(output_path), city_name, year, '縣市區域議員選舉')

    # 處理直轄市市長
    print(f"\n{'=' * 60}")
    print(f"處理 {year} 直轄市市長選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in MUNICIPALITIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / mayor_muni_folder
        result = process_mayor_municipality(str(data_dir), prv_code, city_name)

        if result:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'{year}_直轄市市長_各村里得票數_{city_name}.xlsx'
            save_mayor_excel(result, str(output_path), city_name, year, '直轄市市長選舉')

    # 處理縣市市長
    print(f"\n{'=' * 60}")
    print(f"處理 {year} 縣市市長選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in COUNTIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / mayor_county_folder
        result = process_mayor_county(str(data_dir), prv_code, city_code, city_name)

        if result:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'{year}_縣市市長_各村里得票數_{city_name}.xlsx'
            save_mayor_excel(result, str(output_path), city_name, year, '縣市市長選舉')

    # 處理鄉鎮市長
    print(f"\n{'=' * 60}")
    print(f"處理 {year} 鄉鎮市長選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in COUNTIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / township_folder
        results = process_township_mayor(str(data_dir), prv_code, city_code, city_name)

        if results:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'{year}_鄉鎮市長_各村里得票數_{city_name}.xlsx'
            save_township_mayor_excel(results, str(output_path), city_name, year)


def process_national_election(year):
    """處理總統立委選舉資料（總統、區域立委、山地/平地原住民立委、政黨票）"""
    year_folder = YEAR_FOLDERS.get(year)
    if not year_folder:
        print(f"  [ERROR] 找不到 {year} 年的資料夾設定")
        return

    base_dir = DATA_DIR / year_folder

    # 處理總統選舉
    print(f"\n{'=' * 60}")
    print(f"處理 {year} 總統選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in ALL_CITIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '總統'
        result = process_president(str(data_dir), prv_code, city_code, city_name)

        if result:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'{year}_總統候選人得票數一覽表_各村里_{city_name}.xlsx'
            save_president_excel(result, str(output_path), city_name, year)

    # 處理區域立委
    print(f"\n{'=' * 60}")
    print(f"處理 {year} 區域立委選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in ALL_CITIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '區域立委'
        results = process_legislator(str(data_dir), prv_code, city_code, city_name)

        if results:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'{year}_區域立委_各村里得票數_{city_name}.xlsx'
            save_legislator_excel(results, str(output_path), city_name, year)

    # 處理山地原住民立委
    print(f"\n{'=' * 60}")
    print(f"處理 {year} 山地原住民立委選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in ALL_CITIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '山地立委'
        result = process_indigenous_legislator(str(data_dir), prv_code, city_code, city_name, 'mountain')

        if result:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'{year}_山地原住民立委_各村里得票數_{city_name}.xlsx'
            save_indigenous_legislator_excel(result, str(output_path), city_name, year, 'mountain')

    # 處理平地原住民立委
    print(f"\n{'=' * 60}")
    print(f"處理 {year} 平地原住民立委選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in ALL_CITIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '平地立委'
        result = process_indigenous_legislator(str(data_dir), prv_code, city_code, city_name, 'plain')

        if result:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'{year}_平地原住民立委_各村里得票數_{city_name}.xlsx'
            save_indigenous_legislator_excel(result, str(output_path), city_name, year, 'plain')

    # 處理政黨票
    print(f"\n{'=' * 60}")
    print(f"處理 {year} 政黨票選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in ALL_CITIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '不分區政黨'
        result = process_party_vote(str(data_dir), prv_code, city_code, city_name)

        if result:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'{year}_政黨票_各村里得票數_{city_name}.xlsx'
            save_party_vote_excel(result, str(output_path), city_name, year)


def process_2014():
    """處理 2014 年縣市議員及縣市首長選舉資料"""
    process_local_election(2014)


def process_2016():
    """處理 2016 年總統及立委選舉資料"""
    process_national_election(2016)


def process_2018():
    """處理 2018 年縣市議員及縣市首長選舉資料"""
    process_local_election(2018)


def process_2020():
    """處理 2020 年總統及立委選舉資料"""
    process_national_election(2020)


def process_2022():
    """處理 2022 年縣市議員及縣市首長選舉資料"""
    process_local_election(2022)


def process_2024():
    """處理 2024 年總統及立委選舉資料"""
    process_national_election(2024)


def main():
    """主程式入口"""
    parser = argparse.ArgumentParser(
        description='選舉資料處理系統 - 處理 2014-2024 選舉資料',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
範例:
  python main.py                    # 處理所有年份
  python main.py --year 2014        # 只處理 2014 年（縣市議員、縣市首長、鄉鎮市長）
  python main.py --year 2016        # 只處理 2016 年（總統、立委、政黨票）
  python main.py --year 2018        # 只處理 2018 年（縣市議員、縣市首長、鄉鎮市長）
  python main.py --year 2020        # 只處理 2020 年（總統、立委、政黨票）
  python main.py --year 2022        # 只處理 2022 年（縣市議員、縣市首長、鄉鎮市長）
  python main.py --year 2024        # 只處理 2024 年（總統、立委、政黨票）
  python main.py --merge-national   # 合併全國選舉資料
  python main.py --merge-national --year 2014  # 只合併全國 2014 選舉資料
        '''
    )

    parser.add_argument(
        '--year',
        type=int,
        choices=ALL_YEARS,
        help='只處理指定年份（預設處理所有年份）'
    )

    parser.add_argument(
        '--merge-national',
        action='store_true',
        help='合併全國選舉資料為單一檔案'
    )

    args = parser.parse_args()

    print("=" * 60)
    print("選舉資料處理系統")
    print("=" * 60)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 處理 --merge-national 選項（僅合併，不處理原始資料）
    if args.merge_national:
        print(f"\n{'=' * 60}")
        print("合併全國選舉資料")
        print("=" * 60)

        if args.year:
            print(f"\n合併全國 {args.year} 選舉資料...")
            create_national_election_file(str(OUTPUT_DIR), args.year)
        else:
            for year in ALL_YEARS:
                print(f"\n合併全國 {year} 選舉資料...")
                create_national_election_file(str(OUTPUT_DIR), year)
    elif args.year:
        # 處理指定年份
        year = args.year
        if year in LOCAL_ELECTION_YEARS:
            process_local_election(year)
        else:
            process_national_election(year)

        # 建立全國選舉合併檔案
        print(f"\n{'=' * 60}")
        print(f"合併全國 {year} 選舉資料")
        print("=" * 60)
        create_national_election_file(str(OUTPUT_DIR), year)
    else:
        # 處理所有年份
        for year in ALL_YEARS:
            if year in LOCAL_ELECTION_YEARS:
                process_local_election(year)
            else:
                process_national_election(year)

        # 建立每個縣市的合併版本
        print(f"\n{'=' * 60}")
        print("建立各縣市選舉整理完成版（所有年份合併）")
        print("=" * 60)

        for prv_code, city_code, city_name in ALL_CITIES:
            print(f"\n處理 {city_name}...")
            create_city_combined_file(str(OUTPUT_DIR), city_name, city_code)

        # 建立全國選舉合併檔案
        print(f"\n{'=' * 60}")
        print("合併全國選舉資料")
        print("=" * 60)
        for year in ALL_YEARS:
            print(f"\n合併全國 {year} 選舉資料...")
            create_national_election_file(str(OUTPUT_DIR), year)

    print(f"\n{'=' * 60}")
    print("處理完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
