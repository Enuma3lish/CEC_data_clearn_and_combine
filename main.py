#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
選舉資料處理系統 - 主程式
Taiwan CEC Election Data Processor

功能：
- 處理 2014 地方公職人員選舉資料（縣市議員、縣市首長、鄉鎮市長）
- 處理 2020 總統立委選舉資料（總統、區域立委、山地/平地原住民立委、政黨票）
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


def process_2014():
    """處理 2014 年縣市議員及縣市首長選舉資料"""
    year_folder = YEAR_FOLDERS[2014]
    base_dir = DATA_DIR / year_folder

    # 處理直轄市區域議員
    print("\n" + "=" * 60)
    print("處理 2014 直轄市區域議員選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in MUNICIPALITIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '直轄市區域議員'
        results = process_council_municipality(str(data_dir), prv_code, city_name)

        if results:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'2014_直轄市區域議員_各投開票所得票數_{city_name}.xlsx'
            save_council_excel(results, str(output_path), city_name, 2014, '直轄市區域議員選舉')

    # 處理縣市區域議員
    print("\n" + "=" * 60)
    print("處理 2014 縣市區域議員選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in COUNTIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '縣市區域議員'
        results = process_council_county(str(data_dir), prv_code, city_code, city_name)

        if results:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'2014_縣市區域議員_各投開票所得票數_{city_name}.xlsx'
            save_council_excel(results, str(output_path), city_name, 2014, '縣市區域議員選舉')

    # 處理直轄市市長
    print("\n" + "=" * 60)
    print("處理 2014 直轄市市長選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in MUNICIPALITIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '直轄市市長'
        result = process_mayor_municipality(str(data_dir), prv_code, city_name)

        if result:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'2014_直轄市市長_各村里得票數_{city_name}.xlsx'
            save_mayor_excel(result, str(output_path), city_name, 2014, '直轄市市長選舉')

    # 處理縣市市長
    print("\n" + "=" * 60)
    print("處理 2014 縣市市長選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in COUNTIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '縣市市長'
        result = process_mayor_county(str(data_dir), prv_code, city_code, city_name)

        if result:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'2014_縣市市長_各村里得票數_{city_name}.xlsx'
            save_mayor_excel(result, str(output_path), city_name, 2014, '縣市市長選舉')

    # 處理鄉鎮市長
    print("\n" + "=" * 60)
    print("處理 2014 鄉鎮市長選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in COUNTIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '縣市鄉鎮市長'
        results = process_township_mayor(str(data_dir), prv_code, city_code, city_name)

        if results:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'2014_鄉鎮市長_各村里得票數_{city_name}.xlsx'
            save_township_mayor_excel(results, str(output_path), city_name, 2014)


def process_2020():
    """處理 2020 年總統及區域立委選舉資料"""
    year_folder = YEAR_FOLDERS[2020]
    base_dir = DATA_DIR / year_folder

    print("\n" + "=" * 60)
    print("處理 2020 總統選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in ALL_CITIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '總統'
        result = process_president(str(data_dir), prv_code, city_code, city_name)

        if result:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'2020_總統候選人得票數一覽表_各村里_{city_name}.xlsx'
            save_president_excel(result, str(output_path), city_name, 2020)

    # 處理區域立委
    print("\n" + "=" * 60)
    print("處理 2020 區域立委選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in ALL_CITIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '區域立委'
        results = process_legislator(str(data_dir), prv_code, city_code, city_name)

        if results:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'2020_區域立委_各村里得票數_{city_name}.xlsx'
            save_legislator_excel(results, str(output_path), city_name, 2020)

    # 處理山地原住民立委
    print("\n" + "=" * 60)
    print("處理 2020 山地原住民立委選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in ALL_CITIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '山地立委'
        result = process_indigenous_legislator(str(data_dir), prv_code, city_code, city_name, 'mountain')

        if result:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'2020_山地原住民立委_各村里得票數_{city_name}.xlsx'
            save_indigenous_legislator_excel(result, str(output_path), city_name, 2020, 'mountain')

    # 處理平地原住民立委
    print("\n" + "=" * 60)
    print("處理 2020 平地原住民立委選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in ALL_CITIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '平地立委'
        result = process_indigenous_legislator(str(data_dir), prv_code, city_code, city_name, 'plain')

        if result:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'2020_平地原住民立委_各村里得票數_{city_name}.xlsx'
            save_indigenous_legislator_excel(result, str(output_path), city_name, 2020, 'plain')

    # 處理政黨票
    print("\n" + "=" * 60)
    print("處理 2020 政黨票選舉")
    print("=" * 60)

    for prv_code, city_code, city_name in ALL_CITIES:
        print(f"\n處理 {city_name}...")
        data_dir = base_dir / '不分區政黨'
        result = process_party_vote(str(data_dir), prv_code, city_code, city_name)

        if result:
            city_output_dir = OUTPUT_DIR / city_name
            city_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = city_output_dir / f'2020_政黨票_各村里得票數_{city_name}.xlsx'
            save_party_vote_excel(result, str(output_path), city_name, 2020)


def main():
    """主程式入口"""
    parser = argparse.ArgumentParser(
        description='選舉資料處理系統 - 處理 2014/2020 選舉資料',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
範例:
  python main.py                    # 處理所有年份（2014、2020）
  python main.py --year 2014        # 只處理 2014 年（縣市議員、縣市首長、鄉鎮市長）
  python main.py --year 2020        # 只處理 2020 年（總統、區域立委、山地/平地原住民立委、政黨票）
  python main.py --merge-national   # 合併全國選舉資料（2014、2020）
  python main.py --merge-national --year 2014  # 只合併全國 2014 選舉資料
        '''
    )

    parser.add_argument(
        '--year',
        type=int,
        choices=[2014, 2020],
        help='只處理指定年份（預設處理所有年份）'
    )

    parser.add_argument(
        '--merge-national',
        action='store_true',
        help='合併全國選舉資料為單一檔案（全國2014選舉.xlsx、全國2020選舉.xlsx）'
    )

    args = parser.parse_args()

    print("=" * 60)
    print("選舉資料處理系統")
    print("=" * 60)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 處理 --merge-national 選項（僅合併，不處理原始資料）
    if args.merge_national:
        print("\n" + "=" * 60)
        print("合併全國選舉資料")
        print("=" * 60)

        if args.year == 2014:
            print("\n合併全國 2014 選舉資料...")
            create_national_election_file(str(OUTPUT_DIR), 2014)
        elif args.year == 2020:
            print("\n合併全國 2020 選舉資料...")
            create_national_election_file(str(OUTPUT_DIR), 2020)
        else:
            print("\n合併全國 2014 選舉資料...")
            create_national_election_file(str(OUTPUT_DIR), 2014)
            print("\n合併全國 2020 選舉資料...")
            create_national_election_file(str(OUTPUT_DIR), 2020)
    elif args.year == 2014:
        process_2014()
        # 建立全國 2014 選舉合併檔案
        print("\n" + "=" * 60)
        print("合併全國 2014 選舉資料")
        print("=" * 60)
        create_national_election_file(str(OUTPUT_DIR), 2014)
    elif args.year == 2020:
        process_2020()
        # 建立全國 2020 選舉合併檔案
        print("\n" + "=" * 60)
        print("合併全國 2020 選舉資料")
        print("=" * 60)
        create_national_election_file(str(OUTPUT_DIR), 2020)
    else:
        process_2014()
        process_2020()

        # 建立每個縣市的合併版本（2014+2020）
        print("\n" + "=" * 60)
        print("建立各縣市選舉整理完成版（2014+2020 合併）")
        print("=" * 60)

        for prv_code, city_code, city_name in ALL_CITIES:
            print(f"\n處理 {city_name}...")
            create_city_combined_file(str(OUTPUT_DIR), city_name, city_code)

        # 建立全國選舉合併檔案
        print("\n" + "=" * 60)
        print("合併全國選舉資料")
        print("=" * 60)
        print("\n合併全國 2014 選舉資料...")
        create_national_election_file(str(OUTPUT_DIR), 2014)
        print("\n合併全國 2020 選舉資料...")
        create_national_election_file(str(OUTPUT_DIR), 2020)

    print("\n" + "=" * 60)
    print("處理完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
