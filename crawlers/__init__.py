"""
CEC 選舉資料庫爬蟲模組

主要爬蟲:
- CECStaticApiCrawler: 使用靜態 JSON API 爬取選舉資料 (cec_crawler_v3.py)

輔助工具:
- CECSeleniumCrawler: 用於探索網站結構和發現新的 Theme ID (selenium_crawler.py)
"""

from crawlers.cec_crawler_v3 import CECStaticApiCrawler

__all__ = ['CECStaticApiCrawler']
