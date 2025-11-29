# 台灣中選會選舉資料爬蟲

## 專案簡介

本專案使用靜態 JSON API 從台灣中央選舉委員會(CEC)選舉資料庫爬取選舉資料。

**資料來源**: https://db.cec.gov.tw/

**目前支援**:
- ✅ 總統選舉 (2012, 2016, 2020, 2024)

## 專案結構

```
cec_data_project/
├── config/
│   └── config.yaml           # 設定檔
├── crawlers/
│   ├── __init__.py
│   ├── cec_crawler_v3.py     # 主要爬蟲 (使用靜態 JSON API)
│   └── selenium_crawler.py   # Theme ID 探索工具
├── data/
│   ├── raw/                  # 原始爬取資料
│   │   ├── presidential_2024_national.csv
│   │   ├── presidential_2024_county.csv
│   │   ├── presidential_2020_*.csv
│   │   ├── presidential_2016_*.csv
│   │   ├── presidential_2012_*.csv
│   │   └── presidential_summary.csv
│   └── processed/            # 處理後資料 (中文欄位)
├── main.py                   # 主程式入口
├── example_usage.py          # 使用範例
├── requirements.txt          # Python 依賴套件
└── README.md
```

## 安裝

```bash
pip install -r requirements.txt
```

## 快速開始

### 執行爬蟲

```bash
# 爬取所有年份 (預設: 2024, 2020, 2016, 2012)
python main.py

# 爬取特定年份
python main.py --years 2024,2020

# 包含鄉鎮市區層級資料
python main.py --include-district
```

### 程式碼範例

```python
from crawlers.cec_crawler_v3 import CECStaticApiCrawler

# 建立爬蟲
crawler = CECStaticApiCrawler()

# 爬取 2024 年全國資料
df = crawler.get_presidential_tickets(year=2024, data_level="N")

# 爬取 2024 年縣市資料
df_county = crawler.get_presidential_tickets(year=2024, data_level="C")

# 批次爬取所有年份
all_results = crawler.scrape_all_years(
    years=[2024, 2020, 2016, 2012],
    include_county=True
)
```

## API 說明

### 支援的年份與 Theme ID

| 年份 | Theme ID |
|------|----------|
| 2024 | `4d83db17c1707e3defae5dc4d4e9c800` |
| 2020 | `1f7d9f4f6bfe06fdaf4db7df2ed4d60c` |
| 2016 | `61b4dda0ebac3332203ef3729a9a0ada` |
| 2012 | `fddf766f2a250a2e3688d644fda346d2` |

### 資料層級

| 代碼 | 說明 |
|------|------|
| N | 全國 |
| C | 縣市 |
| D | 鄉鎮市區 |
| L | 村里 |

### API URL 格式

```
https://db.cec.gov.tw/static/elections/data/tickets/ELC/P0/00/{theme_id}/{data_level}/00_000_00_000_0000.json
```

## 輸出資料欄位

### 原始資料欄位

| 欄位 | 說明 |
|------|------|
| cand_name | 候選人姓名 |
| cand_no | 候選人號次 |
| party_name | 政黨 |
| ticket_num | 得票數 |
| ticket_percent | 得票率 |
| is_victor | 當選註記 (*=當選) |
| area_name | 區域名稱 |
| is_vice | 是否為副總統候選人 |

### 處理後資料欄位 (中文)

| 欄位 | 說明 |
|------|------|
| 候選人姓名 | 候選人姓名 |
| 候選人號次 | 候選人號次 |
| 政黨 | 政黨名稱 |
| 得票數 | 得票數 |
| 得票率 | 得票率 |
| 當選 | 是/否 |
| 性別 | 男/女 |

## 擴展其他選舉類型

如需爬取其他選舉類型 (立委、縣市長等)，需要先使用 `selenium_crawler.py` 探索對應的 Theme ID：

```python
from crawlers.selenium_crawler import CECSeleniumCrawler

crawler = CECSeleniumCrawler(headless=False)
crawler.setup_driver()
structure = crawler.explore_website_structure()
crawler.close_driver()
```

## 授權

MIT License

## 作者

Melowu
