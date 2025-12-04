# 中選會選舉資料處理系統

> 台灣中央選舉委員會（CEC）選舉資料處理與分析工具  
> 將原始選舉資料轉換為標準化的村里級別統計格式

[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 📋 目錄

- [快速開始](#-快速開始)
- [系統需求](#-系統需求)
- [核心功能](#️-核心功能)
- [專案結構](#-專案結構)
- [支援的選舉類型](#-支援的選舉類型)
- [輸出格式](#-輸出格式)
- [使用說明](#-使用說明)
- [範例分析](#-範例分析)
- [常見問題](#-常見問題)
- [進階功能](#-進階功能)

---

## 🚀 快速開始

### 方法 1：一鍵執行（推薦）

```bash
python main.py
```

互動式選單會引導您完成所有操作：
- 📥 處理原始資料
- 🔗 合併處理後的資料
- 📊 分析和驗證資料
- ⚡ 完整流程（一鍵處理）
- 📈 查看系統狀態

### 方法 2：分步執行

```bash
# 步驟 1：處理原始資料
python cec_data_processor.py

# 步驟 2：合併資料（可選）
python cec_data_merger.py

# 步驟 3：查看範例分析
python example_usage.py
```

---

## 💻 系統需求

### 必要環境
- **Python** 3.7 或更高版本
- **pandas** 套件

### 安裝依賴

```bash
pip install -r requirements.txt
```

或手動安裝：

```bash
pip install pandas
```

### 選擇性套件（進階功能）
- `openpyxl` - Excel 檔案匯出
- `matplotlib` - 資料視覺化
- `jupyter` - 互動式分析

---

## ⚙️ 核心功能

### 1. 📦 cec_data_processor.py - 資料處理器

**主要功能**
- 處理 voteData 資料夾中的原始選舉資料
- 自動識別並分類選舉類型
- 轉換為標準化村里級別統計
- 按縣市分類輸出 CSV 檔案

**特殊處理**
- ✅ 總統選舉：正副總統自動配對（如：`蔡英文/賴清德`）
- ✅ 立委選舉：自動合併區域、山地、平地資料
- ✅ 議員選舉：自動合併區域、山原、平原資料
- ✅ 智慧清理：自動去除單引號、統一編碼格式

**輸入**
```
voteData/
├── 2014-103年地方公職人員選舉/
├── 2016總統立委/
├── 2018-107年地方公職人員選舉/
├── 2020總統立委/
├── 2022-111年地方公職人員選舉/
└── 2024總統立委/
```

**輸出範例**
```
臺北市/
├── 2016_總統.csv
├── 2016_立法委員.csv
├── 2020_總統.csv
├── 2020_立法委員.csv
└── 2024_總統.csv

南投縣/
├── 2016_總統.csv
├── 2016_立法委員.csv
└── ...
```

### 2. 🔗 cec_data_merger.py - 資料合併器

**主要功能**
- 合併相同選舉類型的不同年份資料
- 智慧檢查欄位相容性
- 支援兩種合併模式

**合併規則**
- ✅ 候選人欄位相同 → 自動合併
- ❌ 候選人欄位不同 → 保持獨立檔案
- 📊 附加年份欄位標記資料來源

**輸出**
```
merged_data/
└── 臺北市/
    ├── 總統_所有年份.csv
    └── 立法委員_所有年份.csv
```

### 3. 💡 example_usage.py - 使用範例

**包含 6 個實用範例**
1. 📖 讀取單一檔案
2. 📊 分析投票率
3. 🗳️ 候選人得票分析
4. 🗺️ 比較各行政區
5. 📄 匯出摘要報告
6. 🔄 批次處理多個檔案

### 4. 🎯 main.py - 互動式主程式

**完整整合系統**
- 友善的互動式介面
- 整合所有處理流程
- 即時狀態顯示
- 錯誤處理與提示

---

## 📁 專案結構

```
CEC_data_clearn_and_combine/
│
├── 📂 核心程式
│   ├── main.py                      ⭐ 互動式主程式
│   ├── cec_data_processor.py        🔧 資料處理器
│   ├── cec_data_merger.py           🔗 資料合併器
│   └── example_usage.py             💡 使用範例
│
├── 📂 原始資料（需自行準備）
│   └── voteData/
│       ├── 2014-103年地方公職人員選舉/
│       ├── 2016總統立委/
│       ├── 2018-107年地方公職人員選舉/
│       ├── 2020總統立委/
│       ├── 2022-111年地方公職人員選舉/
│       └── 2024總統立委/
│
├── 📂 處理後資料（自動產生）
│   ├── 臺北市/
│   ├── 新北市/
│   ├── 南投縣/
│   └── ... (其他縣市)
│
├── 📂 合併資料（可選產生）
│   └── merged_data/
│
├── 📂 輔助模組
│   ├── processors/
│   │   ├── data_processor.py        📋 進階處理模組
│   │   └── data_consolidator.py     📋 資料整合模組
│   ├── utils/
│   │   ├── regional_codes.py        🗺️ 區域代碼管理
│   │   └── schemas.py               📊 資料結構定義
│   └── config/
│       └── config.yaml              ⚙️ 設定檔
│
└── 📂 文件
    ├── README.md                     📖 本檔案
    └── requirements.txt              📦 依賴套件清單
```

---

## 📊 支援的選舉類型

| 選舉類型 | 自動處理 | 說明 |
|---------|---------|------|
| 🏛️ **總統選舉** | ✅ | 正副總統自動配對 |
| 🗳️ **立法委員** | ✅ | 自動合併區域/山地/平地 |
| 🏢 **直轄市議員** | ✅ | 自動合併區域/山原/平原 |
| 🏘️ **非直轄市議員** | ✅ | 自動合併區域/山原/平原 |
| 👔 **直轄市長** | ✅ | 按縣市分類輸出 |
| 🎖️ **非直轄市長縣長** | ✅ | 按縣市分類輸出 |

**支援年份：** 2014 年至今

---

## 📄 輸出格式

### CSV 檔案結構

```csv
行政區別,村里別,蔡英文/賴清德,韓國瑜/張善政,宋楚瑜/余湘,有效票數A,無效票數B,投票數C,已領未投票數D,發出票數E,用餘票數F,選舉人數G,投票率H
總計,,304092,267582,26630,598304,7110,605414,0,605414,221556,826970,73.21
南投市,,61820,53380,6154,121354,1386,122740,0,122740,40966,163706,74.98
,龍泉里,782,482,52,1316,16,1332,0,1332,456,1788,74.5
,康壽里,1144,910,110,2164,24,2188,0,2188,564,2752,79.51
...
```

### 欄位說明

#### 基本欄位
| 欄位 | 說明 | 範例 |
|-----|------|-----|
| `行政區別` | 鄉鎮市區名稱 | 南投市 |
| `村里別` | 村里名稱 | 龍泉里 |

#### 候選人欄位
- **總統選舉**：`正總統/副總統`（如：`蔡英文/賴清德`）
- **其他選舉**：候選人姓名

#### 統計欄位
| 欄位 | 說明 |
|-----|------|
| `有效票數A` | 有效票總數 |
| `無效票數B` | 無效票總數 |
| `投票數C` | 投票總數 |
| `已領未投票數D` | 已領選票但未投票數 |
| `發出票數E` | 發出選票總數 |
| `用餘票數F` | 剩餘選票數 |
| `選舉人數G` | 選舉人總數 |
| `投票率H` | 投票率（%） |

**編碼格式：** UTF-8 with BOM (utf-8-sig)  
**相容性：** 可直接用 Excel、Google Sheets 開啟

---

## 💻 使用說明

### 基本使用流程

#### 1️⃣ 準備資料
將中選會下載的資料放入 `voteData/` 資料夾

#### 2️⃣ 執行處理
```bash
python main.py
```

#### 3️⃣ 選擇功能
```
╔══════════════════════════════════════╗
║   中選會選舉資料處理系統            ║
╚══════════════════════════════════════╝

請選擇功能：
1. 處理原始資料 (voteData → 各縣市)
2. 合併處理後資料 (各縣市 → merged_data)
3. 分析範例展示
4. 完整流程 (推薦)
5. 查看系統狀態
0. 離開

請輸入選項 [0-5]: 
```

#### 4️⃣ 查看結果
處理完成後，資料會自動分類到各縣市資料夾

---

## 📊 範例分析

### 範例 1：讀取並分析投票率

```python
import pandas as pd

# 讀取資料
df = pd.read_csv('南投縣/2020_總統.csv', encoding='utf-8-sig')

# 查看總計
total = df[df['村里別'] == '總計'].iloc[0]
print(f"全縣投票率: {total['投票率H']:.2f}%")

# 找出投票率最高的村里
villages = df[(df['村里別'] != '總計') & (df['村里別'] != '')]
top_village = villages.loc[villages['投票率H'].idxmax()]
print(f"最高投票率: {top_village['行政區別']} {top_village['村里別']} ({top_village['投票率H']:.2f}%)")
```

### 範例 2：候選人得票分析

```python
import pandas as pd

df = pd.read_csv('臺北市/2024_總統.csv', encoding='utf-8-sig')

# 取得候選人欄位（排除統計欄位）
stat_cols = ['行政區別', '村里別', '有效票數A', '無效票數B', '投票數C', 
             '已領未投票數D', '發出票數E', '用餘票數F', '選舉人數G', '投票率H']
candidate_cols = [col for col in df.columns if col not in stat_cols]

# 計算各候選人總得票
total_row = df[df['村里別'] == '總計'].iloc[0]
for cand in candidate_cols:
    votes = total_row[cand]
    percentage = (votes / total_row['有效票數A']) * 100
    print(f"{cand}: {votes:,} 票 ({percentage:.2f}%)")
```

### 範例 3：跨縣市比較

```python
import pandas as pd
import glob

# 讀取所有縣市的 2024 總統選舉資料
results = []
for file in glob.glob('*/2024_總統.csv'):
    df = pd.read_csv(file, encoding='utf-8-sig')
    total = df[df['村里別'] == '總計'].iloc[0]
    county = file.split('/')[0]
    results.append({
        '縣市': county,
        '投票率': total['投票率H'],
        '有效票數': total['有效票數A']
    })

# 轉換為 DataFrame 並排序
summary = pd.DataFrame(results)
summary = summary.sort_values('投票率', ascending=False)
print(summary.to_string(index=False))
```

---

## ❓ 常見問題

### Q1: 如何開始使用？
**A:** 執行 `python main.py`，選擇「完整流程」即可自動處理所有步驟。

### Q2: 某些年份的資料沒有處理？
**A:** 請檢查：
- voteData 資料夾名稱是否包含年份（如 2020、2024）
- 年份必須 >= 2014
- 資料夾內是否包含必要檔案（elbase, elcand, elctks, elprof）

### Q3: 為什麼合併失敗？
**A:** 合併器會檢查候選人欄位是否相同。如果不同年份的候選人不同，系統會保留原始檔案不合併，這是正常行為。

### Q4: 缺少必要檔案怎麼辦？
**A:** 系統會自動跳過並顯示警告。請確認原始資料完整性，或跳過該年份/類型。

### Q5: 輸出檔案用 Excel 開啟出現亂碼？
**A:** 系統已使用 UTF-8 with BOM 編碼，應可正常開啟。如仍有問題，請使用 Google Sheets 或其他支援 UTF-8 的軟體。

### Q6: 如何只處理特定縣市或年份？
**A:** 可以修改 `cec_data_processor.py` 中的過濾條件，或使用 `main.py` 處理後手動刪除不需要的檔案。

### Q7: 處理速度很慢怎麼辦？
**A:** 
- 檢查硬碟空間是否充足
- 關閉不必要的程式釋放記憶體
- 考慮分批處理年份
- 典型處理時間：單一年份約 30-60 秒，全部年份約 5-10 分鐘

### Q8: 可以匯出成 Excel 格式嗎？
**A:** 可以！使用 pandas：
```python
df = pd.read_csv('臺北市/2024_總統.csv', encoding='utf-8-sig')
df.to_excel('output.xlsx', index=False)
```

---

## 🔧 進階功能

### 自訂輸出路徑

編輯 `cec_data_processor.py`：

```python
def __init__(self):
    self.base_dir = Path(r"C:\Your\Custom\Path")
    self.source_dir = self.base_dir / "voteData"
    self.output_dir = self.base_dir / "output"
```

### 整合資料庫

```python
import sqlite3
import pandas as pd

# 建立資料庫連接
conn = sqlite3.connect('election_data.db')

# 讀取並寫入資料庫
df = pd.read_csv('臺北市/2024_總統.csv', encoding='utf-8-sig')
df.to_sql('taipei_2024_president', conn, if_exists='replace', index=False)

# 查詢
result = pd.read_sql('SELECT * FROM taipei_2024_president WHERE 投票率H > 75', conn)
print(result)
```

---

## 📈 效能參考

### 處理時間（參考：Intel i5, 8GB RAM）

| 操作 | 時間 |
|-----|-----|
| 單一年份處理 | 30-60 秒 |
| 全部年份處理 (2014-2024) | 5-10 分鐘 |
| 資料合併 | 10-30 秒 |

### 檔案大小參考

| 類型 | 大小 |
|-----|-----|
| 原始資料 (voteData) | 100-500 MB |
| 處理後單一 CSV | 50-500 KB |
| 合併後 CSV | 100 KB - 5 MB |
| 全部處理後資料 | 50-200 MB |

---

## 📝 更新日誌

### v1.1.0 (2024-12-04)
- ✨ 新增總統選舉正副總統自動配對功能
- 🐛 修正大量資料處理時的效能問題
- 🔧 優化數值轉換邏輯
- 📝 完善 README 文件
- 🧹 清理專案結構，移除多餘檔案

### v1.0.0 (2024-12)
- 🎉 初始版本發布
- ✅ 支援 2014-2024 年選舉資料處理
- ✅ 自動合併不同類型選舉資料
- ✅ 互動式主程式介面

---

## 🤝 貢獻

歡迎提交 Issue 和 Pull Request！

---

## 📄 授權

MIT License - 僅供學術研究和個人使用

---

## 📧 聯絡資訊

如有問題或建議，歡迎開 Issue 討論。

---

**版本：** 1.1.0  
**最後更新：** 2024-12-04
