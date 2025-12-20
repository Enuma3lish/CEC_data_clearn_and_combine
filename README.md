# 選舉資料處理系統

將中央選舉委員會（CEC）原始選舉資料轉換為 Excel 格式的候選人得票數一覽表。

## 功能說明

本系統處理中選會原始 CSV 資料，輸出格式化的 Excel 檔案：

### 2014 地方公職人員選舉

- 縣市議員選舉：處理直轄市區域議員、縣市區域議員資料
- 縣市首長選舉：處理直轄市市長、縣市市長資料
- 鄉鎮市長選舉：處理縣市鄉鎮市長資料
- 輸出各投開票所/村里得票數一覽表
- 輸出全縣市合併版本

### 2020 總統立委選舉

- 總統選舉：處理全國 22 縣市總統選舉資料
- 區域立委選舉：處理區域立法委員選舉資料
- 山地原住民立委選舉：處理山地原住民立法委員選舉資料
- 平地原住民立委選舉：處理平地原住民立法委員選舉資料
- 政黨票：處理不分區政黨票選舉資料
- 輸出各村里候選人得票數一覽表
- 輸出全縣市合併版本

## 快速開始

```bash
# 安裝依賴
pip install pandas openpyxl

# 處理所有年份（2014、2020）
python main.py

# 只處理 2014 年
python main.py --year 2014

# 只處理 2020 年
python main.py --year 2020

# 只合併全國選舉資料（不處理原始資料）
python main.py --merge-national
```

## 專案結構

```text
CEC_data_clearn_and_combine/
├── main.py                          # 主程式入口
├── election_processor/              # 選舉資料處理模組
│   ├── __init__.py                 # 模組匯出
│   ├── config.py                   # 設定檔（路徑、縣市代碼）
│   ├── election_types.py           # 選舉類型配置（便於新增選舉類型）
│   ├── base.py                     # 基礎處理函數（可重用）
│   ├── utils.py                    # 工具函數
│   ├── processor.py                # 資料處理函數
│   └── output.py                   # 輸出函數
├── voteData/                        # 原始 CEC 選舉資料
│   ├── 2014-103年地方公職人員選舉/
│   │   ├── 直轄市區域議員/
│   │   ├── 縣市區域議員/
│   │   ├── 直轄市市長/
│   │   ├── 縣市市長/
│   │   └── 縣市鄉鎮市長/
│   └── 2020總統立委/
│       ├── 總統/
│       ├── 區域立委/
│       ├── 山地立委/
│       ├── 平地立委/
│       └── 不分區政黨/
├── output/                          # 處理後輸出
│   ├── 臺北市/
│   ├── 花蓮縣/
│   ├── ...（其他縣市）
│   ├── 全國2014選舉.xlsx
│   └── 全國2020選舉.xlsx
└── examples/                        # 輸出格式範例
```

## 模組架構

本系統採用模組化設計，便於維護和擴充：

### election_processor/config.py

設定檔，包含：

- `DATA_DIR` - 資料目錄路徑
- `OUTPUT_DIR` - 輸出目錄路徑
- `YEAR_FOLDERS` - 年份資料夾對照
- `MUNICIPALITIES` - 直轄市設定（6 個）
- `COUNTIES` - 縣市設定（16 個）
- `ALL_CITIES` - 所有縣市（22 個）

### election_processor/election_types.py

選舉類型配置，便於新增和修改選舉類型：

- `ElectionType` - 選舉類型基礎類別
- `ELECTION_TYPES_2014` - 2014 年選舉類型配置
- `ELECTION_TYPES_2020` - 2020 年選舉類型配置
- `MERGE_CONFIGS` - 合併檔案配置
- `MAX_CANDIDATES` - 最大候選人數（50）
- `STAT_FIELDS` - 統計欄位名稱

### election_processor/base.py

基礎處理函數，提供可重用的資料處理邏輯：

- `load_election_data(data_dir)` - 載入選舉原始 CSV 資料
- `filter_by_city(dfs, prv_code, city_code)` - 依縣市過濾資料
- `build_name_maps(df_base, include_area)` - 建立區域名稱對照表
- `build_candidate_list(df_cand, ...)` - 建立候選人列表
- `build_stats_map(df_prof, ...)` - 建立統計資料對照表
- `build_votes_map(df_tks, ...)` - 建立票數資料對照表
- `calculate_totals(votes_by_village, ...)` - 計算區級和總計彙總
- `generate_rows(...)` - 生成輸出資料列
- `process_single_area_election(...)` - 處理單選區選舉
- `process_multi_area_election(...)` - 處理多選區選舉

### election_processor/utils.py

工具函數：

- `clean_val(x)` - 清理 CSV 值（移除引號）
- `clean_number(x)` - 清理並轉換數字
- `read_csv_clean(filepath)` - 讀取並清理 CSV
- `load_party_map(elpaty_file)` - 載入政黨對照表
- `get_party_name(code)` - 取得政黨名稱

### election_processor/processor.py

資料處理函數（對外接口）：

- `process_council_municipality(...)` - 處理直轄市區域議員
- `process_council_county(...)` - 處理縣市區域議員
- `process_mayor_municipality(...)` - 處理直轄市市長
- `process_mayor_county(...)` - 處理縣市市長
- `process_township_mayor(...)` - 處理鄉鎮市長
- `process_president(...)` - 處理總統選舉
- `process_legislator(...)` - 處理區域立委
- `process_indigenous_legislator(...)` - 處理原住民立委（山地/平地）
- `process_party_vote(...)` - 處理政黨票

### election_processor/output.py

輸出函數：

- `save_council_excel(...)` - 儲存議員選舉
- `save_mayor_excel(...)` - 儲存市長選舉
- `save_township_mayor_excel(...)` - 儲存鄉鎮市長選舉
- `save_president_excel(...)` - 儲存總統選舉
- `save_legislator_excel(...)` - 儲存區域立委選舉
- `save_indigenous_legislator_excel(...)` - 儲存原住民立委選舉
- `save_party_vote_excel(...)` - 儲存政黨票選舉
- `create_city_combined_file(...)` - 建立縣市合併版本
- `create_national_election_file(...)` - 建立全國合併版本

## 使用範例

```python
from election_processor import (
    DATA_DIR,
    OUTPUT_DIR,
    YEAR_FOLDERS,
    MUNICIPALITIES,
    process_council_municipality,
    save_council_excel,
)

# 處理臺北市 2014 直轄市區域議員
data_dir = DATA_DIR / YEAR_FOLDERS[2014] / '直轄市區域議員'
results = process_council_municipality(str(data_dir), '63', '臺北市')

if results:
    output_path = OUTPUT_DIR / '臺北市' / '2014_直轄市區域議員_各投開票所得票數_臺北市.xlsx'
    save_council_excel(results, str(output_path), '臺北市', 2014, '直轄市區域議員選舉')
```

## 資料處理方法

### 資料處理流程

1. 讀取原始 CSV 資料
   - 從 CEC 原始資料讀取 `elbase.csv`（基本資料）、`elcand.csv`（候選人）、`elctks.csv`（得票數）、`elprof.csv`（投票統計）
   - 清理資料中的引號和空白字元

2. 建立對照表
   - 從 `elbase.csv` 建立行政區、村里名稱對照
   - 從 `elpaty.csv` 載入政黨代碼對照表
   - 從 `elcand.csv` 建立候選人資訊（號次、姓名、政黨）

3. 處理得票數資料
   - 根據選舉類型選擇彙總層級：
     - 縣市議員選舉：按投開票所記錄，輸出時按村里彙總
     - 總統/市長/立委選舉：使用村里彙總列（`tbox=0`）
   - 計算統計欄位（有效票數、無效票數、投票率等）

4. 輸出 Excel 檔案
   - 每個選舉類型產生獨立 Excel 檔案
   - 候選人欄位格式：`(號次)\n姓名\n政黨`
   - 單一工作表檔案（總統、市長）：工作表名稱為縣市名稱
   - 多工作表檔案（議員、立委）：工作表名稱為選區名稱

5. 建立合併檔案
   - 合併多年度、多選舉類型資料
   - 從 `elbase.csv` 生成 11 位區域代碼
   - 刪除村里別為空的彙總行
   - 自動刪除空的候選人欄位

### 區域代碼格式

區域代碼由以下部分組成：

| 縣市類型 | 格式 | 範例 |
|----------|------|------|
| 直轄市 | 省市代碼(2) + 鄉鎮區代碼(3) + 村里代碼(4) + 補位(2) | `63010000100` (臺北市中正區...) |
| 縣市 | 省代碼(2) + 縣市代碼(3) + 鄉鎮區代碼(2) + 村里代碼(4) | `10005090005` (苗栗縣公館鄉中義村) |

### 村里彙總處理

對於按投開票所記錄的選舉資料（如縣市議員選舉），系統會自動按村里彙總：

- 同一村里的候選人得票數相加
- 統計欄位（有效票數、無效票數、投票數等）相加
- 投票率重新計算

### 政黨資訊處理

- 從 `elpaty.csv` 讀取政黨代碼對照表
- 在 Excel 輸出的候選人欄位中包含政黨名稱
- 合併檔案中每位候選人都有對應的政黨欄位

## 原始資料格式

### elbase.csv（基本資料）

| 欄位 | 說明 |
|------|------|
| 0 | 省市代碼 |
| 1 | 縣市代碼 |
| 2 | 選區代碼 |
| 3 | 鄉鎮區代碼 |
| 4 | 村里代碼 |
| 5 | 名稱 |

### elcand.csv（候選人資料）

| 欄位 | 說明 |
|------|------|
| 0 | 省市代碼 |
| 1 | 縣市代碼 |
| 2 | 選區代碼 |
| 5 | 候選人號次 |
| 6 | 候選人姓名 |
| 7 | 政黨代碼 |

### elctks.csv（得票數資料）

| 欄位 | 說明 |
|------|------|
| 0-5 | 同上 + 投開票所代碼 |
| 6 | 候選人號次 |
| 7 | 得票數 |

### elprof.csv（投票統計）

| 欄位 | 說明 |
|------|------|
| 6 | 有效票數 |
| 7 | 無效票數 |
| 8 | 投票數 |
| 9 | 選舉人數 |

## 支援縣市

### 直轄市（6 個）

| 縣市 | 省市代碼 |
|------|----------|
| 臺北市 | 63 |
| 新北市 | 65 |
| 臺中市 | 66 |
| 臺南市 | 67 |
| 高雄市 | 64 |
| 桃園市 | 68 |

### 縣市（16 個）

| 縣市 | 省代碼 | 縣市代碼 |
|------|--------|----------|
| 宜蘭縣 | 10 | 002 |
| 新竹縣 | 10 | 004 |
| 苗栗縣 | 10 | 005 |
| 彰化縣 | 10 | 007 |
| 南投縣 | 10 | 008 |
| 雲林縣 | 10 | 009 |
| 嘉義縣 | 10 | 010 |
| 屏東縣 | 10 | 013 |
| 臺東縣 | 10 | 014 |
| 花蓮縣 | 10 | 015 |
| 澎湖縣 | 10 | 016 |
| 基隆市 | 10 | 017 |
| 新竹市 | 10 | 018 |
| 嘉義市 | 10 | 020 |
| 金門縣 | 09 | 007 |
| 連江縣 | 09 | 020 |

## 擴充新功能

### 新增選舉類型

1. 在 `election_types.py` 新增選舉類型配置：

```python
# 在 ELECTION_TYPES_2024 中新增
'new_election': ElectionType(
    key='new_election',
    name='新選舉類型',
    year=2024,
    data_folder='新選舉資料夾',
    output_template='{year}_新選舉_{city_name}.xlsx',
    is_multi_area=False,
    use_village_summary=True,
)
```

2. 在 `processor.py` 新增處理函數（如需特殊處理邏輯）
3. 在 `output.py` 新增對應的儲存函數（如需特殊輸出格式）
4. 在 `__init__.py` 匯出新函數
5. 在 `main.py` 呼叫新函數

### 新增年份

1. 在 `config.py` 的 `YEAR_FOLDERS` 新增年份對照：

```python
YEAR_FOLDERS = {
    2014: '2014-103年地方公職人員選舉',
    2020: '2020總統立委',
    2024: '2024-113年地方公職人員選舉',  # 新增
}
```

2. 在 `election_types.py` 新增該年份的選舉類型配置
3. 在 `main.py` 新增處理函數（如 `process_2024()`）

## 依賴套件

- Python 3.7+
- pandas
- openpyxl

## 版本資訊

- 版本：2.1.0
- 最後更新：2025-12-20
- 支援年份：2014、2020
- 支援選舉類型：
  - 2014：縣市議員、縣市首長、鄉鎮市長
  - 2020：總統、區域立委、山地原住民立委、平地原住民立委、政黨票
