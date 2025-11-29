# Vidgo AI - 專案整合計畫書與技術規格 (MVP 最終版)

**版本:** 1.0 (MVP Final)  
**日期:** 2025年 11月 26日  
**預計驗收日:** 2025年 12月 10日  
**開發者:** 1 人 (全端工程師)  
**專案目標:** 發布一個可擴展的 AI 影片生成平台，具備高效能的社群媒體格式轉檔能力

---

## 1. 執行摘要 (Executive Summary)

Vidgo AI 是一個 SaaS 平台，允許用戶生成 AI 影片（使用 Replicate/Fal.ai 模型）並將其匯出為社群媒體專用格式。

**關鍵技術優勢:** 為防止高負載影片處理導致伺服器崩潰，系統採用 **混合運算架構 (Hybrid Architecture)**：

1. **Python (Django/Celery):** 處理商業邏輯、API 路由與 IO 密集型任務
2. **C++ (pybind11):** 處理 CPU 密集型的影片編碼與縮放，確保極致效能
3. **Docker 隔離 (Isolation):** 將 IO Worker 與 CPU Worker 分離，防止資源搶佔

---

## 2. 系統架構 (System Architecture)

### 2.1 架構概述

系統利用不同的佇列 (Queue) 與容器 (Container) 將「輕量級」任務（API 呼叫）與「重量級」任務（影片編碼）分離。

**核心組件:**

- **前端層:** Streamlit UI (用戶介面)
- **後端層:** Django API Server + PostgreSQL 資料庫
- **任務分派:** Redis Broker 負責任務分配
- **Worker 層:**
  - IO Worker: 處理 API 呼叫 (Replicate/Fal.ai)
  - CPU Worker: 處理 C++ 影片轉檔
- **儲存層:** Object Storage (S3) 儲存生成影片

**資料流向:**

1. 用戶透過 Streamlit 發送 HTTP 請求
2. Django API 驗證並寫入 PostgreSQL
3. 任務分派至 Redis Broker
4. IO Worker 呼叫 AI 模型 API 生成影片
5. CPU Worker 執行 C++ 影片轉檔
6. 結果儲存至 Object Storage
7. 前端輪詢狀態並顯示結果

### 2.2 技術棧 (Tech Stack)

**詳細技術清單:**

| 層級 | 技術 | 版本 | 用途 |
|:---:|:---|:---:|:---|
| **前端** | Streamlit | 1.29+ | 快速原型開發 UI |
| **後端** | Django + DRF | 5.0 | API 服務與商業邏輯 |
| **任務佇列** | Celery | 5.3+ | 異步任務處理 |
| | Redis | 7.2+ | 訊息佇列 & 快取 |
| **運算核心** | C++17 + pybind11 | - | 高效能影片處理 |
| | FFmpeg | 6.0+ | 影片編解碼 |
| **資料庫** | PostgreSQL | 15 | 主資料庫 |
| **儲存** | Object Storage | - | 影片檔案儲存 |
| **基礎設施** | Docker Compose | - | 容器編排 |
| | GCP e2-standard-4 | - | 雲端運算資源 |

**系統分層架構:**

- **前端層:** Streamlit UI
- **後端層:** Django 5.0 + DRF, 認證系統, 智慧路由
- **任務層:** Redis Broker, Celery Worker (IO-Bound), Celery Worker (CPU-Bound)
- **運算層:** C++17 Module (pybind11), FFmpeg/OpenCV
- **資料層:** PostgreSQL 15, Redis Cache, Object Storage
- **外部服務:** Replicate API, Fal.ai API, ECPay 金流

---

## 3. 功能規格 (Feature Specifications)

### 3.1 核心功能流程

**用戶請求處理流程:**

1. **用戶身份驗證**
   - 免費用戶 → 路由至 Replicate
   - 付費用戶 → 路由至 Fal.ai/Kling

2. **額度檢查**
   - 免費用戶: 檢查每日額度 (3/3 已用完 → 拒絕請求)
   - 付費用戶: 檢查積分 (不足 → 拒絕請求)

3. **影片生成**
   - 免費用戶: 生成影片 + 浮水印
   - 付費用戶: 生成高品質影片 (無浮水印)

4. **格式轉換 (可選)**
   - 若需要轉檔 → C++ 影片核心處理
   - 選擇格式: 9:16 (Shorts) / 4:5 (Feed) / 16:9 (寬螢幕)

5. **完成交付**

### 3.2 功能詳細規格

#### A. 智慧模型路由 (Smart Model Router)

| 用戶類型 | 路由模型 | 限制條件 | 品質特性 |
|:---:|:---|:---|:---|
| **免費用戶** | Replicate (Wan 2.2) | (1) 每日 3 部影片 (2) 強制浮水印 | 標準解析度 |
| **付費用戶** | Fal.ai (Wan 2.5) / Kling | (1) 基於積分扣點 (2) 無浮水印 | 高解析度、高品質 |

#### B. 高效能社群轉檔 (C++ Core)

**核心技術:**

- 🎯 **功能:** 將生成的影片轉換為 9:16 (Shorts)、4:5 (Feed) 或 16:9 格式
- ⚙️ **實作:** 自定義 C++ 模組 `video_core` 處理縮放與裁切
- 🚀 **效能:** 在處理過程中手動釋放 Python **GIL (全域直譯器鎖)**，保持伺服器回應能力

**支援格式:**

| 格式 | 比例 | 用途 | 解析度範例 |
|:---:|:---:|:---|:---|
| **Shorts** | 9:16 | YouTube Shorts, IG Reels | 1080x1920 |
| **Feed** | 4:5 | Instagram Feed | 1080x1350 |
| **Wide** | 16:9 | YouTube 標準影片 | 1920x1080 |

#### C. 優惠券與兌換系統

**兌換流程:**

1. **用戶提交優惠券代碼**
2. **檢查兌換頻率限制 (Redis)**
   - 超過限制 → 拒絕 (429 Too Many Requests)
3. **開始資料庫交易 (BEGIN TRANSACTION)**
4. **鎖定優惠券 (SELECT ... FOR UPDATE)**
5. **驗證券的有效性**
   - 券已用完或無效 → ROLLBACK → 優惠券無效
6. **檢查用戶是否已兌換**
   - 已兌換過 → ROLLBACK → 已兌換過此券
7. **執行兌換操作**
   - 扣減券使用次數
   - 新增兌換記錄
   - 增加用戶積分/天數
8. **提交交易 (COMMIT)**
9. **兌換成功**

**安全機制:**

- 🔒 使用資料庫原子交易 (Atomic Transaction) 防止高併發下的「重複兌換」(Double-spending)
- 🛡️ 使用 `SELECT FOR UPDATE` 實現悲觀鎖 (Pessimistic Locking)
- ⏱️ Redis 速率限制 (Rate Limiting) 防止暴力破解

---

## 4. 技術實作細節 (Technical Implementation Details)

### 4.1 C++ 核心模組 (`video_core`)

為了確保轉檔時不會卡死 Web Server，我們將重度運算下放至 C++ 並釋放 GIL。

**檔案:** `cpp_src/processor.cpp`

```cpp
#include <pybind11/pybind11.h>
#include <thread>
namespace py = pybind11;

// 高負載 CPU 任務
std::string process_video(std::string input_path, std::string output_format) {
    // [關鍵] 釋放 GIL，讓 Python 的其他執行緒 (如 Celery Heartbeat) 能繼續運作
    py::gil_scoped_release release;

    // --- C++ CPU 密集運算開始 ---
    // 範例：呼叫 FFmpeg 函式庫進行縮放/裁切
    // 這裡模擬 5-10 秒的 100% CPU 佔用
    std::this_thread::sleep_for(std::chrono::seconds(5)); 
    // --- C++ CPU 密集運算結束 ---

    return "processed_video_path.mp4"; // 離開區塊時自動取回 GIL
}

PYBIND11_MODULE(video_core, m) {
    m.def("process", &process_video, "Process video with C++ backend");
}
```

### 4.2 資源隔離策略 (Resource Isolation)

我們在 `docker-compose.yml` 與 `settings.py` 中嚴格分離 Worker。

**設定:** `settings.py`

```python
CELERY_TASK_ROUTES = {
    'apps.core.tasks.generate_ai_video': {'queue': 'default'},      # IO 密集
    'apps.core.tasks.send_email':        {'queue': 'default'},      # IO 密集
    'apps.video.tasks.convert_format':   {'queue': 'video_convert'} # CPU 密集
}
```

**設定:** `docker-compose.yml`

```yaml
services:
  # IO Worker: 處理 API 呼叫。高併發。
  worker-io:
    image: vidgo-app
    command: celery -A config worker -Q default -c 4
    deploy:
      resources:
        limits:
          cpus: '1.0' # 限制最多使用 1 核心

  # CPU Worker: 處理 C++ 轉檔。低併發。
  worker-cpu:
    image: vidgo-app
    command: celery -A config worker -Q video_convert -c 1 # 併發數 = 1 (排隊制)
    deploy:
      resources:
        limits:
          cpus: '2.0' # 允許使用 2 核心以發揮最大效能
```

### 4.3 資料庫 Schema (優惠券)

**檔案:** `apps/users/models.py`

```python
class Coupon(models.Model):
    code = models.CharField(max_length=20, unique=True)
    value = models.IntegerField() # 積分數量或天數
    max_usage = models.IntegerField(default=1)
    current_usage = models.IntegerField(default=0)

class CouponRedemption(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    coupon = models.ForeignKey(Coupon, on_delete=models.CASCADE)
    redeemed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        # 防止同一用戶重複兌換同一張券
        unique_together = ('user', 'coupon') 
```

---

## 5. 開發時程表 (Development Timeline)

### 5.1 時程概覽 (2025/11/26 - 12/10)

**核心功能階段 (11/26-11/28):**
- 11/26: AI 串接
- 11/27: 路由邏輯
- 11/28: 優惠券系統

**C++ 開發階段 (11/29-12/01):**
- 11/29: 環境設定
- 11/30: 影片處理核心
- 12/01: Celery 架構

**前端與整合階段 (12/02-12/03):**
- 12/02: Streamlit UI
- 12/03: 金流串接

**測試與部署階段 (12/04-12/10):**
- 12/04: 壓力測試
- 12/05: 雲端部署
- 12/06-12/09: 優化緩衝
- 12/10: **MVP 交付** 🎯

### 5.2 詳細階段規劃

| 階段 | 日期 | 重點領域 | 關鍵產出 | 優先級 |
| :---: | :--- | :--- | :--- | :---: |
| **1** | 11/26 (二) | **AI 串接** | Replicate/Fal.ai API Client 封裝 | 🔴 P0 |
| **2** | 11/27 (三) | **路由邏輯** | 基於用戶方案/積分的智慧路由 | 🔴 P0 |
| **3** | 11/28 (四) | **優惠券** | 後端 Models 與原子兌換邏輯 | 🟡 P1 |
| **4** | 11/29 (五) | **C++ 設定** | CMake 與 pybind11 環境配置 | 🔴 P0 |
| **5** | 11/30 (六) | **C++ 開發** | 實作 C++ 影片縮放邏輯 | 🔴 P0 |
| **6** | 12/01 (日) | **Celery 架構** | 實作佇列隔離與 Docker 資源限制 | 🔴 P0 |
| **7** | 12/02 (一) | **前端** | Streamlit 儀表板 (生成 + 轉檔 + 積分) | 🟡 P1 |
| **8** | 12/03 (二) | **金流** | ECPay 串接與付款回調 (Callback) | 🟡 P1 |
| **9** | 12/04 (三) | **測試** | 壓力測試:Worker-CPU 滿載測試 | 🔴 P0 |
| **10**| 12/05 (四) | **部署** | 部署至雲端 (GCP/AWS) | 🔴 P0 |
| **緩衝**| 12/06-09 | **優化** | 修復 Bug、撰寫文件 | 🟢 P2 |
| **🎯**| **12/10 (二)** | **發布** | **MVP 正式交付** | 🔴 P0 |

---

## 6. 風險管理 (Risk Management)

### 6.1 風險評估矩陣

**風險分布 (按影響 vs 發生機率):**

**立即處理區 (高影響 + 高機率):**
- 資源耗盡 (70% 機率, 85% 影響)
- 併發 Bug (60% 機率, 75% 影響)

**預防措施區 (高影響 + 低機率):**
- 成本超支 (40% 機率, 70% 影響)

**監控觀察區 (低影響 + 高機率):**
- 生成超時 (50% 機率, 60% 影響)

**接受風險區 (低影響 + 低機率):**
- API 限流 (30% 機率, 50% 影響)

### 6.2 風險項目與緩解策略

| 風險項目 | 嚴重性 | 發生機率 | 緩解策略 | 應急方案 |
| :--- | :---: | :---: | :--- | :--- |
| **資源耗盡** (Resource Hogging) | 🔴 高 | 70% | (1) Docker 硬限制 (cpus: 2.0) (2) 分離 Celery 佇列 IO/CPU (3) 監控 CPU/Memory 使用率 | 自動重啟過載容器 |
| **併發 Bug** (Concurrency) | 🔴 高 | 60% | (1) select_for_update() 悲觀鎖 (2) transaction.atomic() 原子操作 (3) 單元測試模擬高併發 | 資料庫交易回滾 |
| **生成超時** (Timeouts) | 🟡 中 | 50% | (1) 前端每 5 秒輪詢狀態 (2) 後端立即回傳 Task ID (3) Celery 任務超時設為 300s | 顯示錯誤訊息並允許重試 |
| **成本超支** (Cost Overrun) | 🟡 中 | 40% | (1) 免費用戶 Redis 速率限制 3次/日 (2) 付費用戶積分預警機制 (3) 每日成本監控報表 | 暫停新請求直到充值 |
| **API 限流** (Rate Limiting) | 🟢 低 | 30% | (1) 使用多個 API Key 輪替 (2) 實作指數退避重試 (3) 降級至備用模型 | 通知用戶稍後重試 |

### 6.3 監控與告警系統

**監控指標:**
- CPU 監控
- Memory 監控
- Queue 深度
- API 成本

**告警機制:**
- 超過閾值 → 觸發告警
- 未超過 → 正常運作

**通知方式:**
- Email 通知
- Slack 通知
- 自動擴展

---

## 7. 首月預算 (Budget)

### 7.1 成本分析

**預算分配 (總計 $200 USD):**
- 基礎設施 (GCP): $100 (50%)
- AI API 額度: $80 (40%)
- 金流手續費: $10 (5%)
- CDN & 儲存: $10 (5%)

### 7.2 詳細預算表

| 項目 | 細項 | 月費 (USD) | 說明 |
|:---|:---|---:|:---|
| **雲端基礎設施** | GCP e2-standard-4 | $100 | 4 vCPU, 16GB RAM |
| | Cloud Storage | $5 | 影片暫存 (50GB) |
| | Cloud CDN | $5 | 內容分發加速 |
| **AI 服務** | Replicate API | $50 | 免費用戶流量 |
| | Fal.ai API | $30 | 付費用戶流量 |
| **金流服務** | ECPay 手續費 | $10 | 2.5% 交易費 (估算) |
| **其他** | 網域 & SSL | 已有 | - |
| **總計** | | **$200** | **≈ NT$ 6,500** |

### 7.3 擴展預算預估

| 用戶規模 | 月活躍用戶 | 預估成本 (USD) | 備註 |
|:---:|---:|---:|:---|
| **MVP 階段** | < 100 | $200 | 當前方案 |
| **成長期** | 500-1000 | $500 | 需升級至 e2-standard-8 |
| **擴展期** | 5000+ | $2,000+ | 考慮 Kubernetes 自動擴展 |

---

## 8. 成功指標 (Success Metrics)

### 8.1 技術指標

| 指標 | 目標值 | 測量方式 |
|:---|---:|:---|
| **API 回應時間** | < 200ms | P95 延遲 |
| **影片生成成功率** | > 95% | 成功任務 / 總任務 |
| **轉檔處理時間** | < 30s | 1080p 影片 |
| **系統可用性** | > 99% | Uptime 監控 |
| **併發處理能力** | 10 個同時任務 | 負載測試 |

### 8.2 業務指標與轉換漏斗

**用戶轉換流程:**

1. **用戶註冊**
2. **首次生成**
3. **轉換率分析:**
   - 免費用戶 → 日活躍 (DAU) → 留存率
     - 7日留存: > 30%
     - 30日留存: > 15%
   - 付費轉換 → 付費用戶 → 月經常性收入 (MRR)

**關鍵指標:**
- 註冊轉換率
- 免費用戶日活躍 (DAU)
- 付費轉換率
- 月經常性收入 (MRR)
- 7日留存率 (> 30%)
- 30日留存率 (> 15%)

---

## 9. 下一步規劃 (Roadmap)

### Phase 2 (2026 Q1)

- 🎨 **進階編輯器** - 文字疊加、特效
- 🤖 **更多 AI 模型支援** - Stability AI, Runway
- 📱 **移動端 App** - React Native
- 🔄 **自動化社群發布**

### Phase 3 (2026 Q2)

- 👥 **多用戶協作功能**
- 📊 **數據分析儀表板**
- 🌐 **多語言支援**
- 🎯 **企業方案** - White Label

---

## 附錄 (Appendix)

### A. 系統需求

**開發環境:**

- Python 3.11+
- CMake 3.20+
- GCC 11+ / Clang 14+
- Docker 24+
- PostgreSQL 15+
- Redis 7+

**生產環境:**

- 最低: 4 vCPU, 16GB RAM
- 建議: 8 vCPU, 32GB RAM (高流量)
- 儲存: 100GB SSD (系統) + Object Storage (影片)

### B. 參考資源

- Django Best Practices: https://docs.djangoproject.com/
- Celery Documentation: https://docs.celeryq.dev/
- pybind11 Guide: https://pybind11.readthedocs.io/
- FFmpeg Documentation: https://ffmpeg.org/documentation.html

---

**文件版本控制:**
- v1.0 (2025-11-26): 初版發布
- 最後更新: 2025年 11月 26日
- 下次審查: 2025年 12月 1日
