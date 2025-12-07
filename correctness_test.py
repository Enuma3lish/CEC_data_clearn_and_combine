#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
所有測試腳本整合
All Tests Integration Script

執行所有驗證測試：
1. 資料格式驗證 - 檢查所有縣市的合併檔案格式是否正確
2. 候選人資料完整性驗證 - 確認候選人姓名、政黨、得票數等欄位完整
3. 統計資料驗證 - 確認有效票數、投票率等統計欄位正確

使用方式：
    python correctness_test.py                    # 執行所有測試
    python correctness_test.py --detail 南投縣   # 顯示指定縣市的詳細報告
    python correctness_test.py --county 南投縣     # 只測試指定縣市
"""

import pandas as pd
import os
import sys
from pathlib import Path


class ComprehensiveDataValidator:
    """綜合資料驗證器"""
    
    def __init__(self, processed_dir: str):
        """
        初始化驗證器
        
        Args:
            processed_dir: 處理後資料的目錄
        """
        self.processed_dir = processed_dir
        self.all_counties = []
        self.test_results = {}
        
    def discover_counties(self):
        """發現所有縣市目錄"""
        if not os.path.exists(self.processed_dir):
            print(f"❌ 找不到資料目錄: {self.processed_dir}")
            return []
        
        counties = []
        for item in os.listdir(self.processed_dir):
            county_path = os.path.join(self.processed_dir, item)
            if os.path.isdir(county_path):
                merged_file = os.path.join(county_path, f'{item}_選舉整理_完成版.csv')
                if os.path.exists(merged_file):
                    counties.append(item)
        
        self.all_counties = sorted(counties)
        return self.all_counties
    
    def test_single_county(self, county: str):
        """測試單一縣市的資料"""
        county_file = os.path.join(self.processed_dir, county, f'{county}_選舉整理_完成版.csv')
        
        if not os.path.exists(county_file):
            return {
                'county': county,
                'status': '❌',
                'error': '檔案不存在'
            }
        
        try:
            df = pd.read_csv(county_file, encoding='utf-8-sig', low_memory=False)
            
            # 基本資訊
            election_types = df['選舉名稱'].unique().tolist() if '選舉名稱' in df.columns else []
            
            # 檢查候選人欄位（注意：數字在後面，如 "選舉候選人得票數1"）
            candidate_cols = [col for col in df.columns if col.startswith('選舉候選人')]
            name_cols = [col for col in candidate_cols if '政黨' not in col and '得票數' not in col and '得票率' not in col]
            party_cols = [col for col in candidate_cols if '政黨' in col]
            vote_cols = [col for col in candidate_cols if '得票數' in col]
            rate_cols = [col for col in candidate_cols if '得票率' in col]
            
            # 檢查統計欄位
            required_stat_cols = ['有效票數', '無效票數', '投票數', '選舉人數', '投票率']
            has_all_stats = all(col in df.columns for col in required_stat_cols)
            
            # 檢查候選人欄位數量一致性（姓名、得票數、得票率必須相同，政黨欄位可選）
            candidate_cols_consistent = (len(name_cols) == len(vote_cols) == len(rate_cols))
            
            # 檢查候選人姓名完整性
            candidate_names_ok = True
            missing_elections = []
            
            for election_type in ['總統', '立法委員', '縣市議員', '鄉鎮市民代表', '縣市長', '鄉鎮市長']:
                election_data = df[df['選舉名稱'] == election_type]
                if len(election_data) > 0 and len(name_cols) > 0:
                    # 檢查第一個候選人欄位是否有資料
                    if name_cols[0] in election_data.columns:
                        non_null = election_data[name_cols[0]].notna().sum()
                        if non_null == 0:
                            candidate_names_ok = False
                            missing_elections.append(election_type)
            
            # 檢查投票率計算
            turnout_calc_ok = True
            if all(col in df.columns for col in ['投票數', '選舉人數', '投票率']):
                sample = df[(df['投票數'].notna()) & (df['選舉人數'] > 0)].head(10)
                for _, row in sample.iterrows():
                    calculated_rate = (row['投票數'] / row['選舉人數']) * 100
                    actual_rate = row['投票率']
                    diff = abs(calculated_rate - actual_rate) if pd.notna(actual_rate) else float('inf')
                    if diff > 0.1:  # 容許誤差 0.1%
                        turnout_calc_ok = False
                        break
            
            # 綜合判定
            all_ok = (has_all_stats and candidate_cols_consistent and 
                     candidate_names_ok and turnout_calc_ok)
            
            result = {
                'county': county,
                'status': '✅' if all_ok else '⚠️' if (has_all_stats and candidate_names_ok) else '❌',
                'rows': len(df),
                'columns': len(df.columns),
                'election_types': election_types,
                'candidate_name_cols': len(name_cols),
                'candidate_party_cols': len(party_cols),
                'candidate_vote_cols': len(vote_cols),
                'candidate_rate_cols': len(rate_cols),
                'has_all_stats': has_all_stats,
                'candidate_cols_consistent': candidate_cols_consistent,
                'candidate_names_ok': candidate_names_ok,
                'missing_elections': missing_elections,
                'turnout_calc_ok': turnout_calc_ok,
                'issues': []
            }
            
            # 記錄問題
            if not has_all_stats:
                result['issues'].append('缺少統計欄位')
            if not candidate_cols_consistent:
                result['issues'].append('候選人欄位數量不一致')
            if not candidate_names_ok:
                result['issues'].append(f'候選人姓名缺失: {", ".join(missing_elections)}')
            if not turnout_calc_ok:
                result['issues'].append('投票率計算錯誤')
            
            return result
            
        except Exception as e:
            return {
                'county': county,
                'status': '❌',
                'error': str(e)
            }
    
    def run_all_tests(self, target_counties=None):
        """執行所有縣市的測試"""
        print(f"\n{'='*80}")
        print("中選會選舉資料 - 綜合驗證測試")
        print(f"{'='*80}\n")
        
        # 發現所有縣市
        counties = self.discover_counties()
        
        if not counties:
            print("❌ 找不到任何縣市資料")
            return
        
        # 如果指定了特定縣市，只測試指定的
        if target_counties:
            counties = [c for c in counties if c in target_counties]
            if not counties:
                print(f"❌ 找不到指定的縣市: {target_counties}")
                return
        
        print(f"找到 {len(counties)} 個縣市的合併資料檔案")
        print(f"縣市列表: {', '.join(counties)}\n")
        
        # 執行測試
        print("開始批次測試...\n")
        
        results = {}
        for i, county in enumerate(counties, 1):
            print(f"[{i}/{len(counties)}] 測試 {county}...", end=' ')
            result = self.test_single_county(county)
            results[county] = result
            
            if 'error' in result:
                print(f"❌ {result['error']}")
            else:
                print(f"{result['status']}")
        
        self.test_results = results
        return results
    
    def print_summary(self):
        """列印測試摘要"""
        if not self.test_results:
            print("❌ 沒有測試結果")
            return
        
        print(f"\n{'='*80}")
        print("測試結果總覽")
        print(f"{'='*80}\n")
        
        # 表格標題
        print(f"{'縣市':10s} {'狀態':5s} {'列數':>10s} {'欄位':>6s} {'選舉類型':>10s} "
              f"{'候選人欄位':>12s} {'問題':30s}")
        print("-" * 100)
        
        # 表格內容
        for county, result in self.test_results.items():
            if 'error' in result:
                print(f"{county:10s} {result['status']:5s} {'ERROR':>10s} {result.get('error', '')[:60]}")
            else:
                issues_str = '; '.join(result['issues']) if result['issues'] else '無'
                print(f"{county:10s} {result['status']:5s} {result['rows']:>10,} "
                      f"{result['columns']:>6d} {len(result['election_types']):>10d} "
                      f"{result['candidate_name_cols']:>12d} {issues_str[:30]}")
        
        print("-" * 100)
        
        # 統計
        total = len(self.test_results)
        success = sum(1 for r in self.test_results.values() if r['status'] == '✅')
        warning = sum(1 for r in self.test_results.values() if r['status'] == '⚠️')
        error = sum(1 for r in self.test_results.values() if r['status'] == '❌')
        
        print(f"\n總計: {total} 個縣市")
        print(f"  ✅ 完全正常: {success} 個 ({success/total*100:.1f}%)")
        print(f"  ⚠️  部分問題: {warning} 個 ({warning/total*100:.1f}%)")
        print(f"  ❌ 嚴重錯誤: {error} 個 ({error/total*100:.1f}%)")
        
        # 詳細問題列表
        if warning > 0 or error > 0:
            print(f"\n問題詳情:")
            for county, result in self.test_results.items():
                if result['status'] in ['⚠️', '❌'] and result.get('issues'):
                    print(f"\n  {county}:")
                    for issue in result['issues']:
                        print(f"    - {issue}")
        
        print(f"\n{'='*80}\n")
    
    def print_detailed_report(self, county: str):
        """列印指定縣市的詳細報告"""
        if county not in self.test_results:
            print(f"❌ 找不到 {county} 的測試結果")
            return
        
        result = self.test_results[county]
        
        print(f"\n{'='*80}")
        print(f"{county} - 詳細測試報告")
        print(f"{'='*80}\n")
        
        if 'error' in result:
            print(f"❌ 錯誤: {result['error']}")
            return
        
        print(f"整體狀態: {result['status']}")
        print(f"總列數: {result['rows']:,}")
        print(f"總欄位數: {result['columns']}")
        print(f"\n選舉類型 ({len(result['election_types'])} 種):")
        for et in result['election_types']:
            print(f"  - {et}")
        
        print(f"\n候選人欄位統計:")
        print(f"  姓名欄位: {result['candidate_name_cols']} 個")
        print(f"  政黨欄位: {result['candidate_party_cols']} 個")
        print(f"  得票數欄位: {result['candidate_vote_cols']} 個")
        print(f"  得票率欄位: {result['candidate_rate_cols']} 個")
        print(f"  欄位數量一致: {'✅' if result['candidate_cols_consistent'] else '❌'}")
        
        print(f"\n資料完整性檢查:")
        print(f"  統計欄位完整: {'✅' if result['has_all_stats'] else '❌'}")
        print(f"  候選人姓名完整: {'✅' if result['candidate_names_ok'] else '❌'}")
        print(f"  投票率計算正確: {'✅' if result['turnout_calc_ok'] else '❌'}")
        
        if result['issues']:
            print(f"\n發現的問題:")
            for issue in result['issues']:
                print(f"  ❌ {issue}")
        else:
            print(f"\n✅ 無問題")
        
        print(f"\n{'='*80}\n")


def main():
    """主程式"""
    import argparse
    
    parser = argparse.ArgumentParser(description='中選會選舉資料綜合驗證測試')
    parser.add_argument('--county', type=str, help='只測試指定縣市（例如：南投縣）')
    parser.add_argument('--detail', type=str, help='顯示指定縣市的詳細報告')
    parser.add_argument('--dir', type=str, 
                       default='/Users/melowu/Desktop/CEC_data_clearn_and_combine/全國各種選舉資料整理',
                       help='處理後資料的目錄路徑')
    
    args = parser.parse_args()
    
    # 建立驗證器
    validator = ComprehensiveDataValidator(args.dir)
    
    # 執行測試
    target_counties = [args.county] if args.county else None
    validator.run_all_tests(target_counties)
    
    # 顯示摘要
    validator.print_summary()
    
    # 如果指定了詳細報告
    if args.detail:
        validator.print_detailed_report(args.detail)


if __name__ == '__main__':
    main()
