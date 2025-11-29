"""
CEC 選舉資料庫爬蟲 V3 - 直接使用靜態 JSON API
"""

import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any

import pandas as pd
import requests
from loguru import logger

# 設定專案路徑
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# 設定日誌
log_dir = PROJECT_ROOT / "logs"
log_dir.mkdir(exist_ok=True)
logger.add(
    log_dir / "cec_crawler_v3_{time}.log",
    rotation="10 MB",
    encoding="utf-8",
    level="DEBUG"
)


class CECStaticApiCrawler:
    """
    CEC 選舉資料庫靜態 JSON API 爬蟲
    直接從已發現的靜態 JSON 端點獲取資料
    """
    
    BASE_URL = "https://db.cec.gov.tw"
    
    # 各年份總統選舉的 themeId (由 Selenium 探索獲得)
    PRESIDENTIAL_THEME_IDS = {
        2024: "4d83db17c1707e3defae5dc4d4e9c800",
        2020: "1f7d9f4f6bfe06fdaf4db7df2ed4d60c",
        2016: "61b4dda0ebac3332203ef3729a9a0ada",
        2012: "fddf766f2a250a2e3688d644fda346d2"
    }
    
    # 資料層級
    DATA_LEVELS = {
        "N": "全國",
        "P": "省/直轄市",
        "C": "縣市",
        "A": "選區",
        "D": "鄉鎮市區",
        "L": "村里"
    }
    
    # API 端點模式
    API_PATTERNS = {
        "tickets": "/static/elections/data/tickets/ELC/{subject_id}/{legis_id}/{theme_id}/{data_level}/{area_code}.json",
        "areas": "/static/elections/data/areas/ELC/{subject_id}/{legis_id}/{theme_id}/{data_level}/{area_code}.json",
        "profRate": "/static/elections/data/profRate/ELC/{subject_id}/{legis_id}/{theme_id}/{data_level}/{area_code}.json",
        "attachments": "/static/elections/data/attachments/ELC/{subject_id}/{theme_id}/list.json"
    }
    
    def __init__(self, output_dir: Optional[Path] = None):
        """初始化爬蟲"""
        self.output_dir = output_dir or (PROJECT_ROOT / "data" / "raw")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://db.cec.gov.tw/",
            "Origin": "https://db.cec.gov.tw"
        })
        
        logger.info("CEC 靜態 API 爬蟲初始化完成")
    
    def _build_api_url(self, data_type: str, subject_id: str, legis_id: str, 
                       theme_id: str, data_level: str = "N", 
                       area_code: str = "00_000_00_000_0000") -> str:
        """建構 API URL"""
        pattern = self.API_PATTERNS.get(data_type)
        if not pattern:
            raise ValueError(f"未知的資料類型: {data_type}")
        
        url = self.BASE_URL + pattern.format(
            subject_id=subject_id,
            legis_id=legis_id,
            theme_id=theme_id,
            data_level=data_level,
            area_code=area_code
        )
        return url
    
    def fetch_json(self, url: str) -> Optional[Any]:
        """獲取 JSON 資料"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                logger.warning(f"資料不存在: {url}")
            else:
                logger.error(f"HTTP 錯誤: {e}")
            return None
        except Exception as e:
            logger.error(f"獲取資料失敗: {url} - {e}")
            return None
    
    def get_presidential_tickets(self, year: int, data_level: str = "N",
                                  area_code: str = "00_000_00_000_0000") -> Optional[pd.DataFrame]:
        """
        獲取總統選舉票數資料
        
        參數:
        - year: 西元年份 (2012, 2016, 2020, 2024)
        - data_level: 資料層級 (N=全國, C=縣市, D=鄉鎮市區, L=村里)
        - area_code: 區域代碼
        """
        theme_id = self.PRESIDENTIAL_THEME_IDS.get(year)
        if not theme_id:
            logger.error(f"不支援的年份: {year}")
            return None
        
        url = self._build_api_url(
            data_type="tickets",
            subject_id="P0",
            legis_id="00",
            theme_id=theme_id,
            data_level=data_level,
            area_code=area_code
        )
        
        logger.info(f"獲取 {year} 年總統選舉票數資料 ({self.DATA_LEVELS.get(data_level, data_level)})")
        
        data = self.fetch_json(url)
        if not data:
            return None
        
        # 資料格式: {"area_code": [records...]}
        all_records = []
        for key, records in data.items():
            if isinstance(records, list):
                all_records.extend(records)
        
        if not all_records:
            logger.warning(f"無資料: {year} 年 {data_level}")
            return None
        
        df = pd.DataFrame(all_records)
        df['election_year'] = year
        df['data_level'] = data_level
        
        logger.success(f"成功獲取 {len(df)} 筆資料")
        return df
    
    def get_presidential_areas(self, year: int, data_level: str = "N") -> Optional[Dict]:
        """獲取區域選項資料（用於發現子區域）"""
        theme_id = self.PRESIDENTIAL_THEME_IDS.get(year)
        if not theme_id:
            return None
        
        url = self._build_api_url(
            data_type="areas",
            subject_id="P0",
            legis_id="00",
            theme_id=theme_id,
            data_level=data_level
        )
        
        return self.fetch_json(url)
    
    def get_all_presidential_data(self, year: int, include_county: bool = True,
                                   include_district: bool = False) -> Dict[str, pd.DataFrame]:
        """
        獲取完整的總統選舉資料
        
        參數:
        - year: 選舉年份
        - include_county: 是否包含縣市層級資料
        - include_district: 是否包含鄉鎮市區層級資料
        """
        results = {}
        
        # 1. 全國層級
        logger.info(f"\n{'='*50}")
        logger.info(f"爬取 {year} 年總統選舉資料")
        logger.info(f"{'='*50}")
        
        national_df = self.get_presidential_tickets(year, "N")
        if national_df is not None:
            results["national"] = national_df
        
        # 2. 縣市層級
        if include_county:
            time.sleep(0.5)  # 避免請求過快
            county_df = self.get_presidential_tickets(year, "C")
            if county_df is not None:
                results["county"] = county_df
        
        # 3. 鄉鎮市區層級 (資料量大，需要更多時間)
        if include_district:
            time.sleep(0.5)
            district_df = self.get_presidential_tickets(year, "D")
            if district_df is not None:
                results["district"] = district_df
        
        return results
    
    def scrape_all_years(self, years: List[int] = None, 
                         include_county: bool = True,
                         include_district: bool = False) -> Dict[int, Dict[str, pd.DataFrame]]:
        """
        爬取所有指定年份的資料
        """
        if years is None:
            years = list(self.PRESIDENTIAL_THEME_IDS.keys())
        
        all_results = {}
        
        for year in years:
            results = self.get_all_presidential_data(year, include_county, include_district)
            all_results[year] = results
            
            # 儲存資料
            for level, df in results.items():
                if df is not None and not df.empty:
                    filename = f"presidential_{year}_{level}.csv"
                    filepath = self.output_dir / filename
                    df.to_csv(filepath, index=False, encoding="utf-8-sig")
                    logger.info(f"已儲存: {filepath}")
            
            time.sleep(1)  # 年份之間稍作暫停
        
        return all_results
    
    def process_and_clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清理和處理資料
        """
        if df is None or df.empty:
            return df
        
        # 複製以避免修改原始資料
        df = df.copy()
        
        # 欄位名稱對照
        column_mapping = {
            'cand_name': '候選人姓名',
            'cand_no': '候選人號次',
            'party_name': '政黨',
            'ticket_num': '得票數',
            'ticket_percent': '得票率',
            'is_victor': '當選註記',
            'area_name': '區域名稱',
            'prv_code': '省市代碼',
            'city_code': '縣市代碼',
            'area_code': '選區代碼',
            'dept_code': '鄉鎮市區代碼',
            'li_code': '村里代碼',
            'cand_sex': '性別',
            'cand_birthyear': '出生年',
            'cand_edu': '學歷',
            'is_vice': '副手',
            'is_current': '現任',
            'election_year': '選舉年份',
            'data_level': '資料層級'
        }
        
        # 重新命名欄位
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        # 處理當選註記
        if '當選註記' in df.columns:
            df['當選'] = df['當選註記'].apply(lambda x: '是' if x == '*' else '否')
        
        # 處理性別
        if '性別' in df.columns:
            df['性別'] = df['性別'].apply(lambda x: '男' if x == '1' else ('女' if x == '2' else x))
        
        # 處理副手
        if '副手' in df.columns:
            df['副手'] = df['副手'].apply(lambda x: '是' if x == 'Y' else '否')
        
        # 處理現任
        if '現任' in df.columns:
            df['現任'] = df['現任'].apply(lambda x: '是' if x == 'Y' else '否')
        
        return df
    
    def create_summary_report(self, all_results: Dict[int, Dict[str, pd.DataFrame]]) -> pd.DataFrame:
        """
        建立摘要報告
        """
        summary_data = []
        
        for year, year_data in all_results.items():
            for level, df in year_data.items():
                if df is not None and not df.empty:
                    # 僅取正副總統候選人組合的全國資料
                    if level == "national":
                        # 篩選正總統候選人 (is_vice != 'Y')
                        presidents = df[df.get('is_vice', '') != 'Y'].copy()
                        
                        for _, row in presidents.iterrows():
                            summary_data.append({
                                '選舉年份': year,
                                '候選人號次': row.get('cand_no', ''),
                                '候選人姓名': row.get('cand_name', ''),
                                '政黨': row.get('party_name', ''),
                                '得票數': row.get('ticket_num', 0),
                                '得票率': row.get('ticket_percent', 0),
                                '當選': '✓' if row.get('is_victor', '') == '*' else ''
                            })
        
        if summary_data:
            return pd.DataFrame(summary_data)
        return pd.DataFrame()
    
    def generate_final_report(self, all_results: Dict) -> None:
        """
        生成最終報告
        """
        report = {
            "crawl_time": datetime.now().isoformat(),
            "data_source": "中央選舉委員會選舉資料庫",
            "url": "https://db.cec.gov.tw/",
            "years_crawled": list(all_results.keys()),
            "statistics": {}
        }
        
        for year, year_data in all_results.items():
            year_stats = {}
            for level, df in year_data.items():
                if df is not None and not df.empty:
                    year_stats[level] = {
                        "records": len(df),
                        "columns": list(df.columns)
                    }
            report["statistics"][year] = year_stats
        
        report_path = log_dir / "final_crawl_report.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        logger.info(f"最終報告已儲存: {report_path}")


def main():
    """主程式"""
    print("=" * 70)
    print("中央選舉委員會選舉資料庫爬蟲 V3")
    print("直接使用靜態 JSON API 獲取資料")
    print("=" * 70)
    
    crawler = CECStaticApiCrawler()
    
    # 爬取 2010 年後的所有總統選舉資料 (2012, 2016, 2020, 2024)
    print("\n[開始爬取資料]")
    all_results = crawler.scrape_all_years(
        years=[2024, 2020, 2016, 2012],
        include_county=True,
        include_district=False  # 鄉鎮市區資料量大，先不爬取
    )
    
    # 建立摘要
    print("\n[建立摘要報告]")
    summary_df = crawler.create_summary_report(all_results)
    
    if not summary_df.empty:
        # 儲存摘要
        summary_path = crawler.output_dir / "presidential_summary.csv"
        summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
        print(f"\n摘要已儲存: {summary_path}")
        
        # 顯示摘要
        print("\n" + "=" * 70)
        print("總統選舉摘要 (2012-2024)")
        print("=" * 70)
        print(summary_df.to_string(index=False))
    
    # 處理並儲存清理後的資料
    print("\n[處理資料]")
    processed_dir = PROJECT_ROOT / "data" / "processed"
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
    print(f"原始資料目錄: {crawler.output_dir}")
    print(f"處理後資料目錄: {processed_dir}")
    print("=" * 70)


if __name__ == "__main__":
    main()
