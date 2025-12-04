"""
資料合併模組 - 將所有處理後的資料合併成統一的最終格式
"""

import pandas as pd
from pathlib import Path
from loguru import logger
from typing import List, Dict, Optional
from datetime import datetime


class DataConsolidator:
    """合併和整合選舉資料"""

    def __init__(self, input_dir: str = "data/processed", output_dir: str = "data/final"):
        """
        初始化資料合併器

        Args:
            input_dir: 輸入目錄（處理後的資料）
            output_dir: 輸出目錄（最終整合資料）
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 設定日誌
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        logger.add(
            log_dir / "data_consolidator.log",
            rotation="10 MB",
            level="INFO"
        )

    def load_all_processed_files(self) -> List[pd.DataFrame]:
        """載入所有處理後的檔案"""
        csv_files = list(self.input_dir.glob("*.csv"))
        logger.info(f"找到 {len(csv_files)} 個處理後的檔案")

        dataframes = []

        for file_path in csv_files:
            try:
                df = pd.read_csv(file_path, encoding='utf-8-sig')
                logger.debug(f"載入: {file_path.name} ({len(df)} 筆)")
                dataframes.append(df)
            except Exception as e:
                logger.error(f"載入失敗: {file_path.name} - {e}")

        logger.success(f"✅ 成功載入 {len(dataframes)} 個檔案")
        return dataframes

    def consolidate_all(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """
        合併所有資料

        Args:
            dataframes: DataFrame 列表

        Returns:
            DataFrame: 合併後的完整資料
        """
        if not dataframes:
            logger.warning("沒有資料可以合併")
            return pd.DataFrame()

        logger.info(f"開始合併 {len(dataframes)} 個資料集...")

        # 合併所有 DataFrame
        consolidated_df = pd.concat(dataframes, ignore_index=True)

        logger.success(f"✅ 合併完成: {len(consolidated_df)} 筆記錄")

        return consolidated_df

    def add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加衍生欄位（計算欄位）

        Args:
            df: 原始 DataFrame

        Returns:
            DataFrame: 添加衍生欄位後的 DataFrame
        """
        logger.info("添加衍生欄位...")

        # 計算候選人得票率（相對於有效票數）
        if 'candidate_votes' in df.columns and 'valid_votes' in df.columns:
            df['candidate_vote_share'] = (
                df['candidate_votes'] / df['valid_votes'] * 100
            ).round(2)

        # 計算候選人得票率（相對於投票數）
        if 'candidate_votes' in df.columns and 'votes_cast' in df.columns:
            df['candidate_turnout_share'] = (
                df['candidate_votes'] / df['votes_cast'] * 100
            ).round(2)

        # 添加處理時間戳記
        df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        logger.debug(f"新增欄位: candidate_vote_share, candidate_turnout_share, processed_at")

        return df

    def validate_data(self, df: pd.DataFrame) -> Dict:
        """
        驗證資料完整性和一致性

        Args:
            df: 待驗證的 DataFrame

        Returns:
            dict: 驗證結果統計
        """
        logger.info("驗證資料...")

        validation_results = {
            'total_records': len(df),
            'missing_values': {},
            'data_range': {},
            'unique_counts': {}
        }

        # 檢查缺失值
        missing = df.isnull().sum()
        validation_results['missing_values'] = {
            col: int(count) for col, count in missing.items() if count > 0
        }

        # 檢查關鍵欄位的唯一值數量
        key_columns = ['election_type', 'year', 'city_original', 'candidate_name']
        for col in key_columns:
            if col in df.columns:
                validation_results['unique_counts'][col] = int(df[col].nunique())

        # 檢查數值欄位的範圍
        numeric_columns = ['year', 'candidate_votes', 'valid_votes', 'votes_cast']
        for col in numeric_columns:
            if col in df.columns:
                validation_results['data_range'][col] = {
                    'min': float(df[col].min()),
                    'max': float(df[col].max()),
                    'mean': float(df[col].mean())
                }

        # 輸出驗證結果
        logger.info("\n驗證結果:")
        logger.info(f"  總記錄數: {validation_results['total_records']:,}")

        if validation_results['missing_values']:
            logger.warning(f"  缺失值: {validation_results['missing_values']}")
        else:
            logger.success("  ✅ 無缺失值")

        logger.info(f"  唯一值統計: {validation_results['unique_counts']}")

        return validation_results

    def save_consolidated_data(self, df: pd.DataFrame, filename: str = "all_elections_consolidated.csv"):
        """
        儲存合併後的資料

        Args:
            df: 合併後的 DataFrame
            filename: 輸出檔案名稱
        """
        output_path = self.output_dir / filename

        logger.info(f"儲存合併資料: {output_path}")

        # 儲存為 CSV
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.success(f"✅ 已儲存: {output_path} ({len(df):,} 筆記錄)")

        # 同時儲存為 Excel（如果資料量不太大）
        if len(df) < 1_048_576:  # Excel 最大行數限制
            excel_path = output_path.with_suffix('.xlsx')
            df.to_excel(excel_path, index=False, engine='openpyxl')
            logger.success(f"✅ 已儲存: {excel_path}")
        else:
            logger.warning("資料量太大，跳過 Excel 儲存")

    def create_summary_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        生成摘要報告

        Args:
            df: 合併後的 DataFrame

        Returns:
            DataFrame: 摘要統計
        """
        logger.info("生成摘要報告...")

        summary_data = []

        # 按選舉類型和年份分組統計
        if 'election_type' in df.columns and 'year' in df.columns:
            grouped = df.groupby(['election_type', 'year'])

            for (election_type, year), group in grouped:
                summary = {
                    '選舉類型': election_type,
                    '年份': year,
                    '記錄數': len(group),
                    '候選人數': group['candidate_name'].nunique() if 'candidate_name' in group.columns else 0,
                    '縣市數': group['city_original'].nunique() if 'city_original' in group.columns else 0,
                    '村里數': group['village'].nunique() if 'village' in group.columns else 0,
                    '總投票數': int(group['votes_cast'].sum()) if 'votes_cast' in group.columns else 0,
                    '平均投票率': round(group['turnout_rate'].mean(), 2) if 'turnout_rate' in group.columns else 0
                }
                summary_data.append(summary)

        summary_df = pd.DataFrame(summary_data)

        # 儲存摘要報告
        summary_path = self.output_dir / "summary_report.csv"
        summary_df.to_csv(summary_path, index=False, encoding='utf-8-sig')
        logger.success(f"✅ 摘要報告已儲存: {summary_path}")

        return summary_df

    def consolidate_and_save(self):
        """完整的合併流程：載入 -> 合併 -> 驗證 -> 儲存"""
        logger.info("="*70)
        logger.info("開始資料合併流程")
        logger.info("="*70)

        # 1. 載入所有處理後的檔案
        dataframes = self.load_all_processed_files()

        if not dataframes:
            logger.error("沒有找到任何處理後的檔案")
            return

        # 2. 合併所有資料
        consolidated_df = self.consolidate_all(dataframes)

        if consolidated_df.empty:
            logger.error("合併後的資料為空")
            return

        # 3. 添加衍生欄位
        consolidated_df = self.add_derived_columns(consolidated_df)

        # 4. 驗證資料
        validation_results = self.validate_data(consolidated_df)

        # 5. 儲存合併後的資料
        self.save_consolidated_data(consolidated_df)

        # 6. 生成摘要報告
        summary_df = self.create_summary_report(consolidated_df)

        # 7. 顯示摘要
        print("\n" + "="*70)
        print("資料合併完成摘要:")
        print("="*70)
        print(f"總記錄數: {len(consolidated_df):,}")
        print(f"檔案位置: {self.output_dir.absolute()}")
        print("\n摘要統計:")
        print(summary_df.to_string(index=False))
        print("="*70)

        logger.info("✅ 資料合併流程完成")


if __name__ == "__main__":
    # 測試用
    consolidator = DataConsolidator()
    consolidator.consolidate_and_save()
