# -*- coding: utf-8 -*-
"""
測試2024年資料處理
"""
import os
from pathlib import Path
from cec_data_processor import CECDataProcessor

# 設定路徑
BASE_DIR = r"C:\Users\melo\OneDrive\Desktop\CEC_data_clearn_and_combine"
VOTE_DATA_DIR = os.path.join(BASE_DIR, "voteData")
OUTPUT_DIR = BASE_DIR

# 建立處理器
processor = CECDataProcessor(VOTE_DATA_DIR, OUTPUT_DIR)

# 只處理2024年
processor.process_year("voteData/2024總統立委", 2024)

print("\n處理完成！")
