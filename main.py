#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
選舉資料處理系統 - 主程式入口
Taiwan CEC Election Data Processor - Main Entry Point

支援年份：2014-2024
輸出格式：符合「鄰里整理範例」格式

Usage:
    python main.py                    # 處理預設縣市（花蓮縣、臺北市）
    python main.py --counties 花蓮縣   # 處理指定縣市
    python main.py --years 2020 2024  # 處理指定年份
"""

import argparse
import pandas as pd
from pathlib import Path

from election_processor import (
    COUNTY_CONFIG,
    LOCAL_YEARS,
    PRESIDENTIAL_YEARS,
    process_local_election,
    process_presidential_election,
)
from election_processor.config import OUTPUT_DIR


def save_and_verify(df: pd.DataFrame, county: str, year: int) -> None:
    """儲存並驗證結果

    Args:
        df: 選舉資料 DataFrame
        county: 縣市名稱
        year: 選舉年份
    """
    if df is None or df.empty:
        return

    output_dir = OUTPUT_DIR / county
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{year}_選舉資料_{county}.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

    # Verify no duplicates
    dups = df[df.duplicated(subset=['行政區別', '鄰里'], keep=False)]
    status = "PASS" if dups.empty else f"FAIL ({len(dups)} dups)"
    print(f"  [SAVED] {output_file.name}: {len(df)} rows, {len(df.columns)} cols - {status}")


def combine_counties(counties: list, output_name: str) -> None:
    """合併多個縣市的資料

    Args:
        counties: 縣市列表
        output_name: 輸出檔名
    """
    print(f"\n合併縣市資料...")

    all_dfs = []
    for county in counties:
        county_dir = OUTPUT_DIR / county
        if not county_dir.exists():
            continue

        for f in sorted(county_dir.glob("*.csv")):
            df = pd.read_csv(f)
            year = f.stem.split('_')[0]
            df.insert(0, '年份', year)
            all_dfs.append(df)
            print(f"  載入: {f.name} ({len(df)} rows)")

    if not all_dfs:
        print("  [ERROR] 無資料可合併")
        return

    # Combine all
    combined = pd.concat(all_dfs, ignore_index=True, sort=False)

    # Fill NaN
    for col in combined.columns:
        if combined[col].dtype == 'object':
            combined[col] = combined[col].fillna('')
        else:
            combined[col] = combined[col].fillna(0)

    output_file = OUTPUT_DIR / output_name
    combined.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n  [COMBINED] {output_file}: {len(combined)} rows, {len(combined.columns)} cols")


def process_county(county: str, years: list) -> None:
    """處理單一縣市所有年份

    Args:
        county: 縣市名稱
        years: 年份列表
    """
    print(f"\n{'='*60}")
    print(f"處理 {county}")
    print(f"{'='*60}")

    for year in years:
        if year in LOCAL_YEARS:
            df = process_local_election(county, year)
        elif year in PRESIDENTIAL_YEARS:
            df = process_presidential_election(county, year)
        else:
            print(f"  [SKIP] 不支援的年份: {year}")
            continue

        if df is not None:
            save_and_verify(df, county, year)


def main():
    """主程式入口"""
    parser = argparse.ArgumentParser(
        description='選舉資料處理系統 (2014-2024)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
範例:
  python main.py                           # 處理預設縣市
  python main.py --counties 花蓮縣 臺北市   # 處理指定縣市
  python main.py --years 2020 2024         # 處理指定年份
  python main.py --no-combine              # 不合併縣市資料
        '''
    )

    parser.add_argument(
        '--counties',
        nargs='+',
        default=['花蓮縣', '臺北市'],
        help='要處理的縣市列表（預設：花蓮縣 臺北市）'
    )
    parser.add_argument(
        '--years',
        nargs='+',
        type=int,
        default=[2014, 2016, 2018, 2020, 2022, 2024],
        help='要處理的年份列表（預設：2014-2024）'
    )
    parser.add_argument(
        '--no-combine',
        action='store_true',
        help='不合併縣市資料'
    )

    args = parser.parse_args()

    # Validate counties
    for county in args.counties:
        if county not in COUNTY_CONFIG:
            print(f"[ERROR] 不支援的縣市: {county}")
            print(f"支援的縣市: {', '.join(COUNTY_CONFIG.keys())}")
            return

    print("=" * 60)
    print("選舉資料處理系統 (2014-2024)")
    print("=" * 60)

    # Process each county
    for county in args.counties:
        process_county(county, args.years)

    # Combine all data
    if not args.no_combine and len(args.counties) > 1:
        output_name = '_'.join(args.counties) + '_合併.csv'
        combine_counties(args.counties, output_name)

    print("\n" + "=" * 60)
    print("處理完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
