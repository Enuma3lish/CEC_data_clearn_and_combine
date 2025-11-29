"""
中選會選舉資料 Selenium 爬蟲
使用 Selenium 爬取動態載入的選舉資料
"""

import time
import json
import re
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
from loguru import logger

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager


class CECSeleniumCrawler:
    """中選會選舉資料 Selenium 爬蟲"""
    
    def __init__(self, headless: bool = True, timeout: int = 30):
        """
        初始化爬蟲
        
        Args:
            headless: 是否使用無頭模式
            timeout: 等待超時時間（秒）
        """
        self.base_url = "https://db.cec.gov.tw"
        self.timeout = timeout
        self.headless = headless
        self.driver = None
        self.wait = None
        
        # 輸出目錄
        self.output_dir = Path("data/raw")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 選舉類型對照
        self.election_types = {
            'President': '總統副總統',
            'Legislator': '區域立法委員',
            'LegislatorParty': '不分區立法委員',
            'LegislatorAboriginal': '原住民立法委員',
            'Mayor': '縣市長',
            'Councilor': '縣市議員',
        }
        
        logger.add("logs/selenium_crawler.log", rotation="10 MB", level="DEBUG")
        
    def setup_driver(self):
        """設定 Chrome WebDriver"""
        logger.info("正在設定 Chrome WebDriver...")
        
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless=new")
        
        # 基本設定
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        # 設定 User-Agent
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        # 禁用自動化標記
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.wait = WebDriverWait(self.driver, self.timeout)
            
            # 執行 CDP 命令隱藏自動化特徵
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                '''
            })
            
            logger.success("WebDriver 設定完成")
            
        except Exception as e:
            logger.error(f"WebDriver 設定失敗: {e}")
            raise
    
    def close_driver(self):
        """關閉 WebDriver"""
        if self.driver:
            self.driver.quit()
            logger.info("WebDriver 已關閉")
    
    def random_delay(self, min_sec: float = 1.0, max_sec: float = 3.0):
        """隨機延遲"""
        import random
        delay = random.uniform(min_sec, max_sec)
        time.sleep(delay)
    
    def get_election_list(self, election_type: str = "President") -> List[Dict]:
        """
        獲取選舉列表
        
        Args:
            election_type: 選舉類型
            
        Returns:
            選舉列表
        """
        url = f"{self.base_url}/ElecTable/Election?type={election_type}"
        logger.info(f"正在獲取選舉列表: {url}")
        
        try:
            self.driver.get(url)
            self.random_delay(2, 4)
            
            # 等待頁面載入
            self.wait.until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # 等待動態內容載入
            time.sleep(3)
            
            # 嘗試多種選擇器找到選舉列表
            elections = []
            
            # 方法 1: 找包含年份的連結
            try:
                links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='Election']")
                for link in links:
                    href = link.get_attribute("href")
                    text = link.text.strip()
                    if text and ("年" in text or "屆" in text):
                        elections.append({
                            "name": text,
                            "url": href,
                            "type": election_type
                        })
                        logger.debug(f"找到選舉: {text}")
            except Exception as e:
                logger.warning(f"方法1失敗: {e}")
            
            # 方法 2: 找所有可點擊的選舉項目
            if not elections:
                try:
                    # 尋找包含選舉資訊的元素
                    items = self.driver.find_elements(By.CSS_SELECTOR, "[class*='election'], [class*='item'], .card, .list-item")
                    for item in items:
                        text = item.text.strip()
                        if text and len(text) > 5:
                            elections.append({
                                "name": text,
                                "element": item,
                                "type": election_type
                            })
                except Exception as e:
                    logger.warning(f"方法2失敗: {e}")
            
            # 方法 3: 直接解析頁面內容
            if not elections:
                page_source = self.driver.page_source
                # 找出年份模式
                year_patterns = re.findall(r'(\d{2,3})年.*?選舉', page_source)
                logger.info(f"從頁面找到年份: {year_patterns}")
            
            logger.info(f"共找到 {len(elections)} 場選舉")
            return elections
            
        except TimeoutException:
            logger.error("頁面載入超時")
            return []
        except Exception as e:
            logger.error(f"獲取選舉列表失敗: {e}")
            return []
    
    def scrape_election_data(self, election_url: str) -> Optional[pd.DataFrame]:
        """
        爬取單場選舉的資料
        
        Args:
            election_url: 選舉頁面 URL
            
        Returns:
            DataFrame 或 None
        """
        logger.info(f"正在爬取: {election_url}")
        
        try:
            self.driver.get(election_url)
            self.random_delay(2, 4)
            
            # 等待表格載入
            try:
                self.wait.until(
                    EC.presence_of_element_located((By.TAG_NAME, "table"))
                )
            except TimeoutException:
                logger.warning("未找到表格元素")
            
            # 額外等待動態內容
            time.sleep(2)
            
            # 獲取所有表格
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            logger.info(f"找到 {len(tables)} 個表格")
            
            all_data = []
            
            for i, table in enumerate(tables):
                try:
                    # 獲取表格 HTML
                    table_html = table.get_attribute("outerHTML")
                    
                    # 使用 pandas 解析
                    dfs = pd.read_html(table_html)
                    if dfs:
                        df = dfs[0]
                        df['table_index'] = i
                        all_data.append(df)
                        logger.debug(f"表格 {i}: {len(df)} 行, {len(df.columns)} 列")
                        
                except Exception as e:
                    logger.warning(f"解析表格 {i} 失敗: {e}")
                    continue
            
            if all_data:
                result_df = pd.concat(all_data, ignore_index=True)
                logger.success(f"成功爬取 {len(result_df)} 筆資料")
                return result_df
            
            # 如果沒有表格，嘗試其他方式
            logger.info("嘗試其他解析方式...")
            return self._scrape_non_table_data()
            
        except Exception as e:
            logger.error(f"爬取失敗: {e}")
            return None
    
    def _scrape_non_table_data(self) -> Optional[pd.DataFrame]:
        """爬取非表格格式的資料"""
        try:
            # 尋找候選人資料
            candidates = []
            
            # 嘗試不同的選擇器
            selectors = [
                "[class*='candidate']",
                "[class*='person']",
                "[class*='result']",
                ".card",
                ".item",
                "div[data-v]"  # Vue.js 組件
            ]
            
            for selector in selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    logger.info(f"使用選擇器 {selector} 找到 {len(elements)} 個元素")
                    for elem in elements:
                        text = elem.text.strip()
                        if text:
                            candidates.append({"raw_text": text})
            
            if candidates:
                return pd.DataFrame(candidates)
            
            return None
            
        except Exception as e:
            logger.error(f"非表格解析失敗: {e}")
            return None
    
    def navigate_and_scrape_presidential(self, year: int) -> Optional[pd.DataFrame]:
        """
        爬取特定年份的總統選舉資料
        
        Args:
            year: 選舉年份（西元年）
            
        Returns:
            DataFrame
        """
        logger.info(f"正在爬取 {year} 年總統選舉資料...")
        
        # 直接構建 URL（根據中選會網站結構）
        # 民國年 = 西元年 - 1911
        roc_year = year - 1911
        
        # 嘗試不同的 URL 格式
        url_patterns = [
            f"{self.base_url}/ElecTable/Election/ElecTickets?dataType=tickets&typeId=ELC&subjectId=P0&legisId=00&thession={roc_year}&ession=00&county=00&district=0000&part=0&exession=0",
            f"{self.base_url}/ElecTable/Election?type=President&year={year}",
            f"{self.base_url}/ElecTable/Election?type=President",
        ]
        
        for url in url_patterns:
            try:
                self.driver.get(url)
                self.random_delay(2, 4)
                
                # 等待內容載入
                time.sleep(3)
                
                # 檢查是否有資料
                page_source = self.driver.page_source
                
                if "選舉" in page_source or "候選人" in page_source:
                    logger.info(f"URL {url} 有效")
                    
                    # 嘗試找到並點擊對應年份
                    self._click_year_option(year)
                    
                    # 爬取資料
                    df = self._extract_election_data()
                    if df is not None and not df.empty:
                        df['election_year'] = year
                        return df
                        
            except Exception as e:
                logger.warning(f"URL {url} 失敗: {e}")
                continue
        
        return None
    
    def _click_year_option(self, year: int):
        """點擊年份選項"""
        roc_year = year - 1911
        year_patterns = [str(year), str(roc_year), f"{roc_year}年", f"第{roc_year}"]
        
        try:
            # 找所有可點擊元素
            clickable = self.driver.find_elements(By.CSS_SELECTOR, "a, button, [onclick], .clickable")
            
            for elem in clickable:
                text = elem.text.strip()
                for pattern in year_patterns:
                    if pattern in text:
                        logger.info(f"點擊年份選項: {text}")
                        elem.click()
                        time.sleep(2)
                        return True
                        
        except Exception as e:
            logger.warning(f"點擊年份失敗: {e}")
        
        return False
    
    def _extract_election_data(self) -> Optional[pd.DataFrame]:
        """從當前頁面提取選舉資料"""
        all_data = []
        
        try:
            # 等待資料載入
            time.sleep(2)
            
            # 1. 嘗試找表格
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            for table in tables:
                try:
                    html = table.get_attribute("outerHTML")
                    dfs = pd.read_html(html)
                    if dfs:
                        all_data.extend(dfs)
                except:
                    continue
            
            # 2. 如果沒有表格，嘗試解析結構化資料
            if not all_data:
                # 找候選人區塊
                candidate_blocks = self.driver.find_elements(
                    By.CSS_SELECTOR, 
                    "[class*='candidate'], [class*='person'], [class*='票'], div.card"
                )
                
                candidates = []
                for block in candidate_blocks:
                    text = block.text.strip()
                    if text:
                        # 解析文字
                        data = self._parse_candidate_text(text)
                        if data:
                            candidates.append(data)
                
                if candidates:
                    return pd.DataFrame(candidates)
            
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            
        except Exception as e:
            logger.error(f"資料提取失敗: {e}")
        
        return None
    
    def _parse_candidate_text(self, text: str) -> Optional[Dict]:
        """解析候選人文字資料"""
        try:
            data = {"raw_text": text}
            
            # 嘗試提取號次
            no_match = re.search(r'(\d+)\s*號', text)
            if no_match:
                data['candidate_no'] = int(no_match.group(1))
            
            # 嘗試提取得票數
            votes_match = re.search(r'([\d,]+)\s*票', text)
            if votes_match:
                data['votes'] = int(votes_match.group(1).replace(',', ''))
            
            # 嘗試提取得票率
            rate_match = re.search(r'([\d.]+)\s*%', text)
            if rate_match:
                data['vote_rate'] = float(rate_match.group(1)) / 100
            
            return data if len(data) > 1 else None
            
        except Exception as e:
            return None
    
    def scrape_all_presidential_elections(self, years: List[int] = None) -> Dict[int, pd.DataFrame]:
        """
        爬取所有總統選舉資料
        
        Args:
            years: 要爬取的年份列表，預設為 2012-2024
            
        Returns:
            {year: DataFrame} 字典
        """
        if years is None:
            years = [2012, 2016, 2020, 2024]
        
        results = {}
        
        for year in years:
            logger.info(f"\n{'='*50}")
            logger.info(f"爬取 {year} 年總統選舉")
            logger.info(f"{'='*50}")
            
            df = self.navigate_and_scrape_presidential(year)
            
            if df is not None and not df.empty:
                results[year] = df
                
                # 儲存到檔案
                output_file = self.output_dir / f"presidential_{year}.csv"
                df.to_csv(output_file, index=False, encoding='utf-8-sig')
                logger.success(f"已儲存: {output_file}")
            else:
                logger.warning(f"{year} 年資料爬取失敗")
            
            # 每次請求後延遲
            self.random_delay(3, 5)
        
        return results
    
    def explore_website_structure(self) -> Dict:
        """
        探索網站結構，用於了解如何導航
        
        Returns:
            網站結構資訊
        """
        logger.info("探索網站結構...")
        
        structure = {
            "pages_visited": [],
            "elements_found": [],
            "navigation_paths": []
        }
        
        try:
            # 訪問主頁
            self.driver.get(f"{self.base_url}/ElecTable/Election?type=President")
            time.sleep(3)
            
            # 記錄頁面資訊
            structure["pages_visited"].append({
                "url": self.driver.current_url,
                "title": self.driver.title
            })
            
            # 找出所有互動元素
            interactive = self.driver.find_elements(By.CSS_SELECTOR, "a, button, select, input")
            
            for elem in interactive[:50]:  # 限制數量
                try:
                    structure["elements_found"].append({
                        "tag": elem.tag_name,
                        "text": elem.text[:100] if elem.text else "",
                        "href": elem.get_attribute("href"),
                        "class": elem.get_attribute("class")
                    })
                except:
                    continue
            
            # 截圖
            screenshot_path = Path("logs/website_structure.png")
            self.driver.save_screenshot(str(screenshot_path))
            logger.info(f"截圖已儲存: {screenshot_path}")
            
            # 儲存頁面 HTML
            html_path = Path("logs/page_source.html")
            html_path.write_text(self.driver.page_source, encoding='utf-8')
            logger.info(f"HTML 已儲存: {html_path}")
            
            # 儲存結構資訊
            structure_path = Path("logs/website_structure.json")
            structure_path.write_text(
                json.dumps(structure, indent=2, ensure_ascii=False),
                encoding='utf-8'
            )
            
            return structure
            
        except Exception as e:
            logger.error(f"探索失敗: {e}")
            return structure
    
    def run(self, election_types: List[str] = None, years: List[int] = None):
        """
        執行爬蟲主程式
        
        Args:
            election_types: 選舉類型列表
            years: 年份列表
        """
        if election_types is None:
            election_types = ["President"]
        
        if years is None:
            years = [2012, 2016, 2020, 2024]
        
        try:
            # 設定 WebDriver
            self.setup_driver()
            
            # 先探索網站結構
            logger.info("步驟 1: 探索網站結構")
            structure = self.explore_website_structure()
            
            # 爬取資料
            logger.info("步驟 2: 開始爬取資料")
            
            all_results = {}
            
            for election_type in election_types:
                logger.info(f"\n爬取選舉類型: {election_type}")
                
                if election_type == "President":
                    results = self.scrape_all_presidential_elections(years)
                    all_results[election_type] = results
            
            # 生成摘要報告
            self._generate_report(all_results)
            
            return all_results
            
        except Exception as e:
            logger.error(f"爬蟲執行失敗: {e}")
            raise
            
        finally:
            self.close_driver()
    
    def _generate_report(self, results: Dict):
        """生成爬取報告"""
        report = {
            "crawl_time": datetime.now().isoformat(),
            "results_summary": {}
        }
        
        for election_type, data in results.items():
            if isinstance(data, dict):
                report["results_summary"][election_type] = {
                    year: len(df) if df is not None else 0
                    for year, df in data.items()
                }
        
        report_path = Path("logs/crawl_report.json")
        report_path.write_text(
            json.dumps(report, indent=2, ensure_ascii=False),
            encoding='utf-8'
        )
        
        logger.info(f"\n{'='*50}")
        logger.info("爬取報告")
        logger.info(f"{'='*50}")
        logger.info(json.dumps(report, indent=2, ensure_ascii=False))


def main():
    """主程式"""
    print("=" * 60)
    print("中選會選舉資料 Selenium 爬蟲")
    print("=" * 60)
    
    # 建立爬蟲實例（設定 headless=False 可以看到瀏覽器操作）
    crawler = CECSeleniumCrawler(headless=True, timeout=30)
    
    try:
        # 執行爬蟲
        results = crawler.run(
            election_types=["President"],
            years=[2012, 2016, 2020, 2024]
        )
        
        print("\n" + "=" * 60)
        print("爬取完成!")
        print("=" * 60)
        
        # 顯示結果
        for election_type, data in results.items():
            print(f"\n{election_type}:")
            if isinstance(data, dict):
                for year, df in data.items():
                    if df is not None:
                        print(f"  {year}年: {len(df)} 筆資料")
                    else:
                        print(f"  {year}年: 無資料")
        
    except KeyboardInterrupt:
        print("\n使用者中斷")
    except Exception as e:
        print(f"\n錯誤: {e}")
        raise


if __name__ == "__main__":
    main()
