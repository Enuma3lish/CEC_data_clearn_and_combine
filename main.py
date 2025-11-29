"""
中選會資料整合專案 - 主程式
使用靜態 JSON API 爬取選舉資料
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional

# 加入專案路徑
sys.path.insert(0, str(Path(__file__).parent))

from crawlers.cec_crawler_v3 import CECStaticApiCrawler
from loguru import logger

# 設定日誌
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logger.add(log_dir / "main_{time}.log", rotation="10 MB", encoding="utf-8", level="INFO")


def main():
    """主程式入口"""
    parser = argparse.ArgumentParser(description='中選會選舉資料爬蟲')
    
    # 爬取選項
    parser.add_argument('--years', type=str, default='2024,2020,2016,2012',
                       help='要爬取的年份，以逗號分隔 (預設: 2024,2020,2016,2012)')
    parser.add_argument('--include-county', action='store_true', default=True,
                       help='包含縣市層級資料 (預設: True)')
    parser.add_argument('--include-district', action='store_true', default=False,
                       help='包含鄉鎮市區層級資料 (預設: False)')
    parser.add_argument('--output-dir', type=str, default='data/raw',
                       help='輸出目錄 (預設: data/raw)')
    
    args = parser.parse_args()
    
    # 解析年份
    years = [int(y.strip()) for y in args.years.split(',')]
    
    print("=" * 70)
    print("中央選舉委員會選舉資料庫爬蟲")
    print("使用靜態 JSON API 獲取資料")
    print("=" * 70)
    
    # 初始化爬蟲
    output_dir = Path(args.output_dir)
    crawler = CECStaticApiCrawler(output_dir=output_dir)
    
    # 爬取資料
    print(f"\n[開始爬取資料]")
    print(f"年份: {years}")
    print(f"包含縣市: {args.include_county}")
    print(f"包含鄉鎮市區: {args.include_district}")
    
    all_results = crawler.scrape_all_years(
        years=years,
        include_county=args.include_county,
        include_district=args.include_district
    )
    
    # 建立摘要
    print("\n[建立摘要報告]")
    summary_df = crawler.create_summary_report(all_results)
    
    if not summary_df.empty:
        # 儲存摘要
        summary_path = output_dir / "presidential_summary.csv"
        summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
        print(f"\n摘要已儲存: {summary_path}")
        
        # 顯示摘要
        print("\n" + "=" * 70)
        print("總統選舉摘要")
        print("=" * 70)
        print(summary_df.to_string(index=False))
    
    # 處理並儲存清理後的資料
    print("\n[處理資料]")
    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    for year, year_data in all_results.items():
        for level, df in year_data.items():
            if df is not None and not df.empty:
                # 清理資料
                cleaned_df = crawler.process_and_clean_data(df)
                
                # 儲存清理後的資料
                filename = f"presidential_{year}_{level}_processed.csv"
                filepath = processed_dir / filename
                cleaned_df.to_csv(filepath, index=False, encoding="utf-8-sig")
                print(f"  已處理並儲存: {filename}")
    
    # 生成最終報告
    crawler.generate_final_report(all_results)
    
    print("\n" + "=" * 70)
    print("爬取完成！")
    print(f"原始資料目錄: {output_dir}")
    print(f"處理後資料目錄: {processed_dir}")
    print("=" * 70)


if __name__ == "__main__":
    main()
