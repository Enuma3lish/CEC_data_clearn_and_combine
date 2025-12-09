import pandas as pd

# 嘗試不同編碼
for enc in ['utf-8', 'big5', 'cp950', 'utf-8-sig']:
    try:
        df = pd.read_csv('voteData/2014-103年地方公職人員選舉/縣市市長/elcand.csv', encoding=enc)
        print(f"成功讀取 (encoding={enc})")
        print(f"Columns: {df.columns.tolist()}")
        
        # 檢查花蓮縣資料
        hualien = df[df['prvCode'] == '15']
        print(f"\n花蓮縣資料筆數: {len(hualien)}")
        if len(hualien) > 0:
            print("前3筆:")
            print(hualien[['prvCode', 'distCode', 'num', 'candName', 'party']].head(3))
        break
    except Exception as e:
        print(f"encoding={enc} 失敗: {e}")
