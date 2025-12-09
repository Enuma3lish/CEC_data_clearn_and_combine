import pandas as pd

for county in ['花蓮縣', '臺北市']:
    for year in [2016, 2020, 2024]:
        file_path = f'鄰里整理輸出/{county}/{year}_選舉資料_{county}.csv'
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            
            # Check president and legislator stats
            stats = []
            for prefix in ['總統候選人', '區域立委']:
                vote_c = df[f'{prefix}投票數C'].iloc[0]
                voter_g = df[f'{prefix}選舉人數G'].iloc[0]
                
                valid = False
                if prefix == '總統候選人':
                    valid_a = df[f'{prefix}有效票數A'].iloc[0]
                    invalid_b = df[f'{prefix}無效票數B'].iloc[0]
                    valid = (vote_c == valid_a + invalid_b) and (voter_g > 0)
                else:
                    valid = (vote_c > 0) and (voter_g > 0)
                
                stats.append(f"{prefix}: C={vote_c}, G={voter_g} {'✓' if valid else '✗'}")
            
            status = '✓ PASS' if all('✓' in s for s in stats) else '✗ FAIL'
            print(f"{county} {year}: {status}")
            for s in stats:
                print(f"  - {s}")
        
        except FileNotFoundError:
            print(f"{county} {year}: File not found")
        except Exception as e:
            print(f"{county} {year}: Error - {e}")
