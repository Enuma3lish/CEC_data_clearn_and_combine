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
│   ├── 全國2014選舉.csv
│   ├── 全國2020選舉.xlsx
│   └── 全國2020選舉.csv
└── examples/                        # 輸出格式範例
```

## 模組架構

本系統採用模組化設計，便於維護和擴充。核心設計理念：

1. **配置驅動**：所有選舉類型的行為由 `ElectionType` 配置決定
2. **統一入口**：`process_election()` 和 `save_election_excel()` 處理所有選舉類型
3. **向後相容**：舊版函數仍可使用，內部轉發至統一入口

### 統一 API（推薦）

```python
from election_processor import (
    process_election,
    save_election_excel,
    get_election_config,
    DATA_DIR,
    YEAR_FOLDERS,
)

# 處理任何選舉類型 - 只需指定 key
election_type = get_election_config('president')
data_dir = DATA_DIR / YEAR_FOLDERS[2020] / election_type.data_folder

result = process_election(election_type, str(data_dir), '63', '000', '臺北市')
save_election_excel(result, 'output/president.xlsx', election_type, '臺北市')
```

### 向後相容 API

```python
from election_processor import process_president, save_president_excel

result = process_president(data_dir, '63', '000', '臺北市')
save_president_excel(result, 'output/president.xlsx', '臺北市', 2020)
```

### election_processor/config.py

設定檔，包含：

- `DATA_DIR` - 資料目錄路徑
- `OUTPUT_DIR` - 輸出目錄路徑
- `YEAR_FOLDERS` - 年份資料夾對照
- `MUNICIPALITIES` - 直轄市設定（6 個）
- `COUNTIES` - 縣市設定（16 個）
- `ALL_CITIES` - 所有縣市（22 個）

### election_processor/election_types.py

選舉類型配置，便於新增和修改選舉類型。所有選舉行為由配置決定：

- `ElectionType` - 選舉類型基礎類別
- `ELECTION_TYPES_2014` - 2014 年選舉類型配置
- `ELECTION_TYPES_2020` - 2020 年選舉類型配置
- `MERGE_CONFIGS` - 合併檔案配置
- `MAX_CANDIDATES` - 最大候選人數（50）
- `STAT_FIELDS` - 統計欄位名稱

### election_processor/base.py

基礎處理函數，提供統一處理入口和可重用的資料處理邏輯：

**統一入口（推薦）**：
- `process_election(election_type, data_dir, prv_code, city_code, city_name)` - 統一選舉資料處理入口

**低階函數**：
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

資料處理函數（向後相容包裝器，內部呼叫 `process_election()`）：

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

**統一入口（推薦）**：
- `save_election_excel(result, output_path, election_type, city_name)` - 統一選舉結果輸出

**向後相容函數**：
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

### 使用統一 API（推薦）

```python
from election_processor import (
    process_election,
    save_election_excel,
    get_election_config,
    DATA_DIR,
    OUTPUT_DIR,
    YEAR_FOLDERS,
)

# 處理任何選舉類型 - 只需指定 key
election_type = get_election_config('president')  # 或 'legislator', 'council_municipality' 等
data_dir = DATA_DIR / YEAR_FOLDERS[election_type.year] / election_type.data_folder

result = process_election(election_type, str(data_dir), '63', '000', '臺北市')

if result:
    output_path = OUTPUT_DIR / '臺北市' / election_type.get_output_filename('臺北市')
    save_election_excel(result, str(output_path), election_type, '臺北市')
```

### 使用向後相容 API

```python
from election_processor import (
    DATA_DIR,
    OUTPUT_DIR,
    YEAR_FOLDERS,
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

### 輸出檔案格式

全國選舉合併檔案同時提供兩種格式：

| 格式 | 檔案名稱 | 編碼 | 說明 |
|------|----------|------|------|
| Excel | `全國{年份}選舉.xlsx` | - | 使用 openpyxl 引擎 |
| CSV | `全國{年份}選舉.csv` | UTF-8 | 標準 UTF-8 編碼 |

### 特殊 Unicode 字元

資料中包含以下特殊 Unicode 字元，這些是台灣官方地名和原住民姓名的正式用字：

| 字元 | Unicode | 說明 | 出現位置 |
|------|---------|------|----------|
| 𥕢 | U+25562 | CJK Extension B | 新北市坪林區「石𥕢里」 |
| 𦰡 | U+26C21 | CJK Extension B | 臺南市新化區「𦰡拔里」 |
| ． | U+FF0E | 全形句點 | 原住民候選人姓名（如 Saidhai．Tahovecahe） |

這些字元可能在某些舊版軟體或字型中無法正確顯示，但它們是正確的官方用字。如需處理這些字元，請確保使用支援 Unicode 的工具和字型。

### 全國選舉合併檔案欄位結構

全國選舉合併檔案採用「一村里一列」的設計，同一年度的所有選舉類型橫向合併，方便分析單一鄰里的完整投票情況。

#### 基礎欄位

| 欄位 | 說明 |
|------|------|
| 時間 | 選舉年份（如 2014、2020） |
| 縣市 | 縣市名稱 |
| 行政區別 | 鄉鎮市區名稱 |
| 鄰里 | 村里名稱 |
| 區域別代碼 | 11 位區域代碼 |

#### 2014 選舉欄位順序

欄位依序為：縣市長 → 縣市議員 → 鄉鎮市長

每個選舉類型包含：
- `{選舉類型}_候選人N` - 候選人姓名
- `{選舉類型}_政黨N` - 候選人政黨
- `{選舉類型}_得票數N` - 候選人得票數
- `{選舉類型}_得票率N` - 候選人得票率
- `{選舉類型}_有效票數` 等 8 個統計欄位

範例欄位：`縣市長_候選人1`, `縣市長_政黨1`, `縣市長_得票數1`, `縣市長_得票率1`, ...

#### 2020 選舉欄位順序

欄位依序為：總統 → 區域立法委員 → 山地原住民立委 → 平地原住民立委 → 政黨票

每個選舉類型包含：
- `{選舉類型}_候選人N` / `{選舉類型}_政黨N` (政黨票)
- `{選舉類型}_政黨N`
- `{選舉類型}_得票數N`
- `{選舉類型}_得票率N`
- 統計欄位

範例欄位：`總統_候選人1`, `總統_政黨1`, `總統_得票數1`, `總統_得票率1`, ...

**注意**：區域立委選舉會額外包含 `區域立法委員_選區` 欄位。

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
| 金門縣 | 09 | 020 |
| 連江縣 | 09 | 007 |

## 擴充新功能

### 新增選舉類型

由於採用配置驅動設計，新增選舉類型非常簡單，只需 1 步：

1. 在 `election_types.py` 新增選舉類型配置：

```python
# 在 ELECTION_TYPES_2024 中新增
'village_chief': ElectionType(
    key='village_chief',
    name='村里長選舉',
    year=2024,
    data_folder='村里長',
    output_template='{year}_村里長_{city_name}.xlsx',
    is_multi_area=True,
    use_village_summary=True,
    election_category='village_chief',
    merge_key='village_chief',
)
```

完成！不需要新增任何處理函數，`process_election()` 會根據配置自動處理。

### 使用新選舉類型

```python
from election_processor import process_election, save_election_excel, get_election_config

election_type = get_election_config('village_chief')
result = process_election(election_type, data_dir, prv_code, city_code, city_name)
save_election_excel(result, output_path, election_type, city_name)
```

### 特殊處理邏輯

如需特殊處理邏輯（大多數情況不需要）：

1. 在 `processor.py` 新增包裝函數
2. 在 `output.py` 新增儲存函數（如需特殊輸出格式）
3. 在 `__init__.py` 匯出新函數

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

- 版本：3.0.0
- 最後更新：2025-12-20
- 支援年份：2014、2020
- 支援選舉類型：
  - 2014：縣市長、縣市議員、鄉鎮市長
  - 2020：總統、區域立委、山地原住民立委、平地原住民立委、政黨票

## 版本歷史

### 3.0.0 (2025-12-20)

- **重大架構重構**：模組化設計
  - 新增 `process_election()` 統一選舉資料處理入口
  - 新增 `save_election_excel()` 統一選舉結果輸出入口
  - processor.py 從 1,580 行精簡至 170 行（90% 程式碼減少）
  - 所有處理邏輯集中於 base.py，便於維護和測試
- 增強 `ElectionType` 配置類別
  - 新增 `is_party_vote`、`election_category`、`merge_key` 屬性
  - 新增 `get_output_filename()` 方法
- 向後相容：舊版函數仍可正常使用
- 新增選舉類型只需在 `election_types.py` 新增配置，無需修改處理邏輯

### 2.5.0 (2025-12-20)

- 修正 CSV 檔案編碼問題
  - 移除 BOM (Byte Order Mark) 字元，改用標準 UTF-8 編碼
  - 避免部分工具將 BOM 誤判為欄位名稱的一部分
- 修正政黨票資料中政黨名稱顯示為「無黨籍」的問題
  - 正確解析政黨票的 2 行格式：`(號次)\n政黨名稱`
- 新增特殊 Unicode 字元說明文件
  - 記錄 CJK Extension B 字元（地名用字）
  - 記錄全形句點（原住民姓名用字）

### 2.4.0 (2025-12-20)

- 重新設計全國選舉合併檔案結構
  - 採用「一村里一列」設計，同一年度所有選舉類型橫向合併
  - 2014 欄位順序：縣市長 → 縣市議員 → 鄉鎮市長
  - 2020 欄位順序：總統 → 區域立委 → 山地原住民立委 → 平地原住民立委 → 政黨票
- 每個選舉類型包含完整的候選人、政黨、得票數、得票率欄位
- 方便分析單一鄰里在不同選舉類型中的投票情況

### 2.3.0 (2025-12-20)

- 修正金門縣與連江縣的縣市代碼對調問題
  - 金門縣：09-020（原錯誤設為 09-007）
  - 連江縣：09-007（原錯誤設為 09-020）
- 修正 2020 總統選舉合併檔案中候選人姓名缺少副總統的問題
  - 現在候選人姓名格式為「正總統/副總統」（如：蔡英文/賴清德）
- 新增全國選舉 CSV 檔案輸出（UTF-8 with BOM 編碼）

### 2.2.0 (2025-12-20)

- 修正 2020 總統選舉合併檔案中「選舉候選人政黨N」欄位顯示副總統姓名的問題
  - 總統選舉候選人格式為 4 行：`(號次)\n正總統\n副總統\n政黨`
  - 現在正確解析第 4 行為政黨名稱
- 修正 2014 鄉鎮市長選舉合併檔案中「行政區別」欄位為空的問題
  - 合併檔案中鄉鎮市長選舉資料現在會正確顯示鄉鎮市區名稱
  - 「鄰里」欄位格式為 `鄉鎮市區_村里` (如：花蓮市_民立里)
- 新增全國選舉 CSV 檔案輸出
  - 同時輸出 `全國2014選舉.csv` 和 `全國2020選舉.csv`
  - 使用 UTF-8 with BOM 編碼，確保 Excel 正確開啟中文字元

### 2.1.0 (2025-12-20)

- 重構專案結構，建立模組化架構
- 新增 election_types.py 便於管理選舉類型配置
- 新增 base.py 提供可重用的資料處理函數
- 2014 年合併檔案不再包含「立委選區」欄位（僅 2020 年需要）
- 全國合併檔案如已存在會先刪除再重新建立
