"""
資料處理模組 - 讀取、清理和標準化下載的選舉資料檔案
"""

import pandas as pd
from pathlib import Path
from loguru import logger
from typing import List, Dict, Optional
import re


class DataProcessor:
    """處理和標準化中選會選舉資料"""

    def __init__(self, input_dir: str = "data/downloaded", output_dir: str = "data/processed"):
        """
        初始化資料處理器

        Args:
            input_dir: 輸入目錄（下載的原始檔案）
            output_dir: 輸出目錄（處理後的標準化資料）
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 設定日誌
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        logger.add(
            log_dir / "data_processor.log",
            rotation="10 MB",
            level="INFO"
        )

    def parse_filename(self, filename: str) -> Optional[Dict]:
        """
        解析標準化檔案名稱

        Args:
            filename: 檔案名稱，格式如 "president_臺北市_2024.csv"

        Returns:
            dict: 包含 election_type, city, year 的字典，若解析失敗返回 None
        """
        # 移除副檔名
        name_without_ext = filename.rsplit('.', 1)[0]

        # 嘗試解析標準格式: election_type_city_year
        pattern = r'^([a-z]+)_(.+)_(\d{4})$'
        match = re.match(pattern, name_without_ext)

        if match:
            return {
                'election_type': match.group(1),
                'city': match.group(2),
                'year': int(match.group(3))
            }

        logger.warning(f"無法解析檔案名稱: {filename}")
        return None

    def read_csv_file(self, file_path: Path) -> pd.DataFrame:
        """
        讀取 CSV 檔案（處理各種編碼）

        Args:
            file_path: CSV 檔案路徑

        Returns:
            DataFrame: 讀取的資料
        """
        # 嘗試不同的編碼
        encodings = ['utf-8-sig', 'utf-8', 'big5', 'cp950', 'gb18030']

        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                logger.debug(f"成功讀取 {file_path.name}（編碼: {encoding}）")
                return df
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue

        raise ValueError(f"無法讀取檔案 {file_path.name}（嘗試過的編碼: {', '.join(encodings)}）")

    def standardize_columns(self, df: pd.DataFrame, election_type: str) -> pd.DataFrame:
        """
        標準化欄位名稱

        Args:
            df: 原始 DataFrame
            election_type: 選舉類型

        Returns:
            DataFrame: 標準化後的 DataFrame
        """
        # 定義欄位對應（中文欄位名 -> 標準英文欄位名）
        column_mapping = {
            # 基本資訊
            '縣市別': 'city',
            '縣市名稱': 'city',
            '行政區別': 'district',
            '行政區': 'district',
            '鄉鎮市區': 'district',
            '村里別': 'village',
            '村里': 'village',
            '村里名稱': 'village',

            # 投票統計
            '投票數': 'votes_cast',
            '投票人數': 'votes_cast',
            '有效票數': 'valid_votes',
            '有效票': 'valid_votes',
            '無效票數': 'invalid_votes',
            '無效票': 'invalid_votes',
            '投票率': 'turnout_rate',
            '投票率(%)': 'turnout_rate',

            # 選民資訊
            '選舉人數': 'eligible_voters',
            '選舉人': 'eligible_voters',
        }

        # 重新命名欄位
        df_renamed = df.rename(columns=column_mapping)

        # 保留原始候選人得票欄位（通常包含候選人姓名）
        # 這些欄位會在後續處理中轉換為長格式

        logger.debug(f"標準化後的欄位: {df_renamed.columns.tolist()}")

        return df_renamed

    def extract_candidate_votes(self, df: pd.DataFrame, metadata: Dict) -> pd.DataFrame:
        """
        提取候選人得票資料並轉換為長格式

        Args:
            df: 標準化後的 DataFrame
            metadata: 檔案元資料（election_type, city, year）

        Returns:
            DataFrame: 長格式的候選人得票資料
        """
        # 識別候選人得票欄位（通常是數字且不在基本欄位中）
        base_columns = ['city', 'district', 'village', 'votes_cast', 'valid_votes',
                       'invalid_votes', 'turnout_rate', 'eligible_voters']

        # 找出候選人得票欄位
        candidate_columns = [col for col in df.columns if col not in base_columns]

        if not candidate_columns:
            logger.warning(f"找不到候選人得票欄位: {metadata}")
            return df

        # 保留基本欄位
        base_df = df[base_columns].copy() if all(col in df.columns for col in base_columns) else pd.DataFrame()

        # 轉換候選人得票為長格式
        candidate_data = []

        for col in candidate_columns:
            # 為每個候選人創建記錄
            temp_df = base_df.copy() if not base_df.empty else pd.DataFrame(index=df.index)

            temp_df['candidate_name'] = col
            temp_df['candidate_votes'] = df[col]

            # 添加元資料
            temp_df['election_type'] = metadata['election_type']
            temp_df['city_original'] = metadata['city']
            temp_df['year'] = metadata['year']

            candidate_data.append(temp_df)

        # 合併所有候選人資料
        result_df = pd.concat(candidate_data, ignore_index=True)

        logger.debug(f"提取候選人資料: {len(candidate_columns)} 位候選人, {len(result_df)} 筆記錄")

        return result_df

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清理資料（移除空值、格式化等）

        Args:
            df: 原始 DataFrame

        Returns:
            DataFrame: 清理後的 DataFrame
        """
        # 移除完全空白的行
        df = df.dropna(how='all')

        # 清理數字欄位（移除逗號、百分比符號等）
        numeric_columns = ['votes_cast', 'valid_votes', 'invalid_votes',
                          'eligible_voters', 'candidate_votes']

        for col in numeric_columns:
            if col in df.columns:
                # 轉換為字串並清理
                df[col] = df[col].astype(str).str.replace(',', '').str.replace('%', '')

                # 轉換為數字（無法轉換的設為 NaN）
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 清理投票率欄位
        if 'turnout_rate' in df.columns:
            df['turnout_rate'] = df['turnout_rate'].astype(str).str.replace('%', '')
            df['turnout_rate'] = pd.to_numeric(df['turnout_rate'], errors='coerce')

        # 清理文字欄位（移除前後空白）
        text_columns = ['city', 'district', 'village', 'candidate_name']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        logger.debug(f"資料清理完成: {len(df)} 筆記錄")

        return df

    def process_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        處理單一檔案

        Args:
            file_path: 檔案路徑

        Returns:
            DataFrame: 處理後的資料，失敗返回 None
        """
        try:
            logger.info(f"處理檔案: {file_path.name}")

            # 解析檔案名稱
            metadata = self.parse_filename(file_path.name)
            if not metadata:
                logger.error(f"無法解析檔案名稱: {file_path.name}")
                return None

            # 讀取檔案
            df = self.read_csv_file(file_path)
            logger.info(f"讀取資料: {len(df)} 筆, {len(df.columns)} 欄")

            # 標準化欄位
            df = self.standardize_columns(df, metadata['election_type'])

            # 提取候選人得票資料
            df = self.extract_candidate_votes(df, metadata)

            # 清理資料
            df = self.clean_data(df)

            logger.success(f"✅ 處理完成: {file_path.name} ({len(df)} 筆記錄)")

            return df

        except Exception as e:
            logger.error(f"❌ 處理失敗: {file_path.name} - {type(e).__name__}: {str(e)}")
            return None

    def process_all_files(self) -> List[pd.DataFrame]:
        """
        處理所有下載的檔案

        Returns:
            list: 處理後的 DataFrame 列表
        """
        csv_files = list(self.input_dir.glob("*.csv"))

        # 排除進度追蹤檔案
        csv_files = [f for f in csv_files if f.name != "download_progress.json"]

        logger.info(f"找到 {len(csv_files)} 個 CSV 檔案")

        processed_data = []
        success_count = 0
        fail_count = 0

        for file_path in csv_files:
            df = self.process_file(file_path)

            if df is not None:
                processed_data.append(df)
                success_count += 1

                # 儲存處理後的檔案
                output_path = self.output_dir / file_path.name
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
                logger.info(f"已儲存: {output_path.name}")
            else:
                fail_count += 1

        logger.info(f"\n處理完成: 成功 {success_count}, 失敗 {fail_count}")

        return processed_data


if __name__ == "__main__":
    # 測試用
    processor = DataProcessor()
    processed_data = processor.process_all_files()

    print(f"\n處理完成，共 {len(processed_data)} 個檔案")

    if processed_data:
        print("\n第一個檔案的資料預覽:")
        print(processed_data[0].head(10))
