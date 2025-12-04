# -*- coding: utf-8 -*-
"""
使用範例：讀取和分析處理後的選舉資料

這個腳本示範如何：
1. 讀取處理後的 CSV 檔案
2. 進行基本的資料分析
3. 輸出統計摘要
"""

import pandas as pd
from pathlib import Path
import os


def example_1_read_single_file():
    """範例 1：讀取單一檔案"""
    print("=" * 70)
    print("範例 1：讀取南投縣 2020 年總統選舉資料")
    print("=" * 70)

    file_path = "南投縣/2020_總統.csv"

    if not os.path.exists(file_path):
        print(f"檔案不存在：{file_path}")
        print("請先執行 cec_data_processor.py 處理資料")
        return

    # 讀取資料
    df = pd.read_csv(file_path, encoding='utf-8-sig')

    print(f"\n檔案資訊：")
    print(f"  資料筆數：{len(df)} 筆")
    print(f"  欄位數量：{len(df.columns)} 欄")

    # 顯示前 10 筆
    print(f"\n前 10 筆資料：")
    print(df.head(10).to_string())

    # 顯示總計行
    total_row = df[df['村里別'] == '總計'].iloc[0] if len(df[df['村里別'] == '總計']) > 0 else None

    if total_row is not None:
        print(f"\n總計資訊：")
        print(f"  有效票數：{total_row['有效票數A']:,.0f}")
        print(f"  投票數：{total_row['投票數C']:,.0f}")
        print(f"  選舉人數：{total_row['選舉人數G']:,.0f}")
        print(f"  投票率：{total_row['投票率H']:.2f}%")


def example_2_analyze_turnout():
    """範例 2：分析投票率"""
    print("\n" + "=" * 70)
    print("範例 2：分析各村里投票率")
    print("=" * 70)

    file_path = "南投縣/2020_總統.csv"

    if not os.path.exists(file_path):
        print(f"檔案不存在：{file_path}")
        return

    df = pd.read_csv(file_path, encoding='utf-8-sig')

    # 過濾掉總計行
    df_villages = df[df['村里別'] != '總計'].copy()
    df_villages = df_villages[df_villages['村里別'] != ''].copy()

    if len(df_villages) == 0:
        print("沒有村里資料")
        return

    # 找出投票率最高和最低的村里
    df_villages['投票率H'] = pd.to_numeric(df_villages['投票率H'], errors='coerce')
    df_sorted = df_villages.sort_values('投票率H', ascending=False)

    print(f"\n投票率統計：")
    print(f"  平均投票率：{df_villages['投票率H'].mean():.2f}%")
    print(f"  最高投票率：{df_villages['投票率H'].max():.2f}%")
    print(f"  最低投票率：{df_villages['投票率H'].min():.2f}%")

    print(f"\n投票率最高的 5 個村里：")
    top5 = df_sorted.head(5)[['行政區別', '村里別', '投票率H']]
    print(top5.to_string(index=False))

    print(f"\n投票率最低的 5 個村里：")
    bottom5 = df_sorted.tail(5)[['行政區別', '村里別', '投票率H']]
    print(bottom5.to_string(index=False))


def example_3_candidate_analysis():
    """範例 3：候選人得票分析"""
    print("\n" + "=" * 70)
    print("範例 3：候選人得票分析")
    print("=" * 70)

    file_path = "南投縣/2020_總統.csv"

    if not os.path.exists(file_path):
        print(f"檔案不存在：{file_path}")
        return

    df = pd.read_csv(file_path, encoding='utf-8-sig')

    # 找出候選人欄位
    exclude_cols = {
        '行政區別', '村里別',
        '有效票數A', '無效票數B', '投票數C', '已領未投票數D',
        '發出票數E', '用餘票數F', '選舉人數G', '投票率H'
    }

    candidate_cols = [col for col in df.columns if col not in exclude_cols]

    if not candidate_cols:
        print("找不到候選人欄位")
        return

    print(f"\n候選人列表：")
    for i, cand in enumerate(candidate_cols, 1):
        print(f"  {i}. {cand}")

    # 計算各候選人總得票
    total_row = df[df['村里別'] == '總計']

    if len(total_row) > 0:
        print(f"\n各候選人總得票數：")
        for cand in candidate_cols:
            votes = total_row[cand].iloc[0]
            print(f"  {cand}: {votes:,.0f} 票")


def example_4_compare_districts():
    """範例 4：比較各行政區"""
    print("\n" + "=" * 70)
    print("範例 4：比較各行政區投票狀況")
    print("=" * 70)

    file_path = "南投縣/2020_總統.csv"

    if not os.path.exists(file_path):
        print(f"檔案不存在：{file_path}")
        return

    df = pd.read_csv(file_path, encoding='utf-8-sig')

    # 找出各行政區的總計行
    district_totals = df[
        (df['村里別'] == '總計') &
        (df['行政區別'] != '') &
        (df['行政區別'] != '總計')
    ].copy()

    if len(district_totals) == 0:
        print("找不到行政區總計資料")
        return

    print(f"\n各行政區投票統計：")
    print(f"{'行政區別':<10} {'投票數':>10} {'選舉人數':>10} {'投票率':>8}")
    print("-" * 50)

    for _, row in district_totals.iterrows():
        district = row['行政區別']
        votes = row['投票數C']
        eligible = row['選舉人數G']
        turnout = row['投票率H']

        print(f"{district:<10} {votes:>10,.0f} {eligible:>10,.0f} {turnout:>7.2f}%")


def example_5_export_summary():
    """範例 5：匯出摘要報告"""
    print("\n" + "=" * 70)
    print("範例 5：匯出摘要報告到 Excel")
    print("=" * 70)

    file_path = "南投縣/2020_總統.csv"
    output_file = "南投縣_2020總統_摘要.xlsx"

    if not os.path.exists(file_path):
        print(f"檔案不存在：{file_path}")
        return

    df = pd.read_csv(file_path, encoding='utf-8-sig')

    try:
        # 建立摘要
        summary_data = []

        # 各行政區總計
        district_totals = df[
            (df['村里別'] == '總計') &
            (df['行政區別'] != '') &
            (df['行政區別'] != '總計')
        ].copy()

        for _, row in district_totals.iterrows():
            summary_data.append({
                '行政區別': row['行政區別'],
                '投票數': row['投票數C'],
                '選舉人數': row['選舉人數G'],
                '投票率': row['投票率H']
            })

        summary_df = pd.DataFrame(summary_data)

        # 匯出到 Excel
        summary_df.to_excel(output_file, index=False, sheet_name='摘要')

        print(f"\n摘要報告已匯出：{output_file}")
        print(f"  包含 {len(summary_df)} 個行政區的資料")

    except ImportError:
        print("\n需要安裝 openpyxl 來匯出 Excel 檔案：")
        print("  pip install openpyxl")


def example_6_batch_processing():
    """範例 6：批次處理多個檔案"""
    print("\n" + "=" * 70)
    print("範例 6：批次讀取所有縣市的 2020 年總統選舉資料")
    print("=" * 70)

    base_dir = Path(".")
    city_dirs = [d for d in base_dir.iterdir() if d.is_dir()]

    results = []

    for city_dir in city_dirs:
        file_path = city_dir / "2020_總統.csv"

        if not file_path.exists():
            continue

        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')

            # 找總計行
            total_row = df[
                (df['村里別'] == '') &
                (df['行政區別'] == '總計')
            ]

            if len(total_row) > 0:
                row = total_row.iloc[0]
                results.append({
                    '縣市': city_dir.name,
                    '投票數': row['投票數C'],
                    '選舉人數': row['選舉人數G'],
                    '投票率': row['投票率H']
                })

        except Exception as e:
            print(f"處理 {city_dir.name} 時發生錯誤：{e}")

    if results:
        print(f"\n找到 {len(results)} 個縣市的資料：\n")
        print(f"{'縣市':<12} {'投票數':>12} {'選舉人數':>12} {'投票率':>8}")
        print("-" * 50)

        for r in results:
            print(f"{r['縣市']:<12} {r['投票數']:>12,.0f} {r['選舉人數']:>12,.0f} {r['投票率']:>7.2f}%")

        # 計算全國統計
        total_votes = sum(r['投票數'] for r in results)
        total_eligible = sum(r['選舉人數'] for r in results)
        overall_turnout = (total_votes / total_eligible * 100) if total_eligible > 0 else 0

        print("-" * 50)
        print(f"{'全國總計':<12} {total_votes:>12,.0f} {total_eligible:>12,.0f} {overall_turnout:>7.2f}%")
    else:
        print("\n找不到任何 2020 年總統選舉資料")


def main():
    """執行所有範例"""
    print("\n")
    print("*" * 70)
    print("*" + " " * 68 + "*")
    print("*" + "  選舉資料分析範例".center(66) + "*")
    print("*" + " " * 68 + "*")
    print("*" * 70)

    try:
        example_1_read_single_file()
        example_2_analyze_turnout()
        example_3_candidate_analysis()
        example_4_compare_districts()
        example_5_export_summary()
        example_6_batch_processing()

        print("\n" + "=" * 70)
        print("所有範例執行完成！")
        print("=" * 70)
        print("\n提示：")
        print("  - 修改檔案路徑以分析其他縣市或年份")
        print("  - 參考這些範例建立自己的分析腳本")
        print("  - 詳細說明請參閱 README.md 和 QUICK_START.md")

    except Exception as e:
        print(f"\n執行過程中發生錯誤：{e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
