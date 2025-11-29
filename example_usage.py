"""
使用範例腳本
展示如何使用 CEC 靜態 API 爬蟲
"""

import sys
from pathlib import Path

# 加入專案路徑
sys.path.insert(0, str(Path(__file__).parent))

from crawlers.cec_crawler_v3 import CECStaticApiCrawler
import pandas as pd


def example_1_basic_crawl():
    """範例 1: 基本爬蟲使用 - 爬取單一年份"""
    print("\n" + "=" * 60)
    print("範例 1: 爬取 2024 年總統選舉資料")
    print("=" * 60)
    
    # 建立爬蟲
    crawler = CECStaticApiCrawler()
    
    # 爬取 2024 年全國資料
    df = crawler.get_presidential_tickets(year=2024, data_level="N")
    
    if df is not None and not df.empty:
        print(f"\n成功爬取 {len(df)} 筆資料")
        print("\n欄位:")
        print(df.columns.tolist())
        print("\n前 5 筆資料:")
        print(df.head())
    else:
        print("\n未取得資料")


def example_2_multiple_levels():
    """範例 2: 爬取多層級資料"""
    print("\n" + "=" * 60)
    print("範例 2: 爬取 2024 年多層級資料")
    print("=" * 60)
    
    crawler = CECStaticApiCrawler()
    
    # 爬取全國 + 縣市層級
    results = crawler.get_all_presidential_data(
        year=2024,
        include_county=True,
        include_district=False
    )
    
    for level, df in results.items():
        if df is not None:
            print(f"\n{level}: {len(df)} 筆資料")


def example_3_data_cleaning():
    """範例 3: 資料清理與中文欄位轉換"""
    print("\n" + "=" * 60)
    print("範例 3: 資料清理與標準化")
    print("=" * 60)
    
    crawler = CECStaticApiCrawler()
    
    # 爬取資料
    df = crawler.get_presidential_tickets(year=2024, data_level="N")
    
    if df is not None and not df.empty:
        print("\n原始欄位:")
        print(df.columns.tolist())
        
        # 清理資料
        df_cleaned = crawler.process_and_clean_data(df)
        
        print("\n清理後欄位 (中文):")
        print(df_cleaned.columns.tolist())
        
        print("\n清理後資料:")
        print(df_cleaned.to_string(index=False))


def example_4_batch_crawl():
    """範例 4: 批次爬取多年資料"""
    print("\n" + "=" * 60)
    print("範例 4: 批次爬取 2012-2024 總統選舉資料")
    print("=" * 60)
    
    crawler = CECStaticApiCrawler()
    
    # 批次爬取
    all_results = crawler.scrape_all_years(
        years=[2024, 2020, 2016, 2012],
        include_county=True,
        include_district=False
    )
    
    # 顯示結果
    for year, year_data in all_results.items():
        print(f"\n{year} 年:")
        for level, df in year_data.items():
            if df is not None:
                print(f"  {level}: {len(df)} 筆資料")


def example_5_summary_report():
    """範例 5: 建立摘要報告"""
    print("\n" + "=" * 60)
    print("範例 5: 建立總統選舉摘要報告")
    print("=" * 60)
    
    crawler = CECStaticApiCrawler()
    
    # 爬取資料
    all_results = crawler.scrape_all_years(
        years=[2024, 2020],
        include_county=False,
        include_district=False
    )
    
    # 建立摘要
    summary_df = crawler.create_summary_report(all_results)
    
    if not summary_df.empty:
        print("\n總統選舉摘要:")
        print(summary_df.to_string(index=False))


def example_6_theme_ids():
    """範例 6: 查看支援的選舉年份與 Theme ID"""
    print("\n" + "=" * 60)
    print("範例 6: 支援的選舉年份與 Theme ID")
    print("=" * 60)
    
    print("\n總統選舉 Theme ID:")
    print("-" * 50)
    
    theme_ids = CECStaticApiCrawler.PRESIDENTIAL_THEME_IDS
    
    for year, theme_id in sorted(theme_ids.items(), reverse=True):
        print(f"  {year}: {theme_id}")
    
    print("\n資料層級代碼:")
    print("-" * 50)
    
    data_levels = CECStaticApiCrawler.DATA_LEVELS
    
    for code, name in data_levels.items():
        print(f"  {code}: {name}")


def main():
    """執行所有範例"""
    print("=" * 60)
    print("CEC 選舉資料爬蟲 - 使用範例")
    print("=" * 60)
    
    try:
        # 範例 6: Theme ID 查詢 (不需網路)
        example_6_theme_ids()
        
        # 範例 1: 基本爬取
        example_1_basic_crawl()
        
        # 範例 3: 資料清理
        example_3_data_cleaning()
        
        # 其他範例會產生較多網路請求，可視需要執行
        # example_2_multiple_levels()
        # example_4_batch_crawl()
        # example_5_summary_report()
        
        print("\n" + "=" * 60)
        print("範例執行完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n執行範例時發生錯誤: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
