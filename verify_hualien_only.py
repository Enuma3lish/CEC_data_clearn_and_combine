import pandas as pd

print("="*60)
print("花蓮縣 統計數據驗證")
print("="*60)

for year in [2014, 2016, 2018, 2020, 2022, 2024]:
    file_path = f'鄰里整理輸出/花蓮縣/{year}_選舉資料_花蓮縣.csv'
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        print(f"\n【{year} 年】")
        
        # Check what columns exist
        has_president = any('總統候選人投票數C' in c for c in df.columns)
        has_legislator = any('區域立委投票數C' in c for c in df.columns)
        
        if has_president:
            vote_c = df['總統候選人投票數C'].iloc[0]
            voter_g = df['總統候選人選舉人數G'].iloc[0]
            valid_a = df['總統候選人有效票數A'].iloc[0]
            invalid_b = df['總統候選人無效票數B'].iloc[0]
            
            calc_match = (vote_c == valid_a + invalid_b)
            voter_valid = (voter_g > 0)
            
            status = '✓' if (calc_match and voter_valid) else '✗'
            print(f"  總統候選人 {status}")
            print(f"    - 投票數C = {vote_c} (應等於 {valid_a} + {invalid_b} = {valid_a + invalid_b}) {'✓' if calc_match else '✗'}")
            print(f"    - 選舉人數G = {voter_g} {'✓' if voter_valid else '✗ (不應為0)'}")
        
        if has_legislator:
            vote_c = df['區域立委投票數C'].iloc[0]
            voter_g = df['區域立委選舉人數G'].iloc[0]
            
            vote_valid = (vote_c > 0)
            voter_valid = (voter_g > 0)
            
            status = '✓' if (vote_valid and voter_valid) else '✗'
            print(f"  區域立委 {status}")
            print(f"    - 投票數C = {vote_c} {'✓' if vote_valid else '✗'}")
            print(f"    - 選舉人數G = {voter_g} {'✓' if voter_valid else '✗ (不應為0)'}")
        
        if not has_president and not has_legislator:
            print(f"  (此年份無總統/立委選舉)")
            
    except FileNotFoundError:
        print(f"\n【{year} 年】檔案不存在")
    except Exception as e:
        print(f"\n【{year} 年】錯誤: {e}")

print("\n" + "="*60)
