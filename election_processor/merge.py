"""
選舉資料合併功能
Election data merge functions
"""

import pandas as pd
from .config import COUNTY_CONFIG
from .processor import ElectionProcessor


def merge_election_results(results: list, county: str) -> pd.DataFrame:
    """合併選舉結果

    Args:
        results: ElectionProcessor 處理結果列表
        county: 縣市名稱

    Returns:
        合併後的 DataFrame
    """
    if not results:
        return None

    valid_results = [r for r in results if r is not None and not r.empty]
    if not valid_results:
        return None

    config = COUNTY_CONFIG[county]
    prv_code = config['prv_code']
    city_code = config['city_code']

    # Merge on 行政區別 + 鄰里
    merged = valid_results[0]
    for df in valid_results[1:]:
        merge_cols = ['行政區別', '鄰里']
        if '_dept' in df.columns:
            merge_cols.extend(['_dept', '_li'])

        common_cols = [c for c in merge_cols if c in merged.columns and c in df.columns]

        if '_area' in df.columns:
            df = df.drop(columns=['_area'])
        if '_area' in merged.columns:
            merged = merged.drop(columns=['_area'])

        merged = merged.merge(df, on=common_cols, how='outer', suffixes=('', '_dup'))
        merged = merged.loc[:, ~merged.columns.str.endswith('_dup')]

    # Add county column
    merged.insert(0, '縣市', county)

    # Add 區域別代碼 - format: {prv:02d}{city:03d}{dept:02d}{li:04d} = 11 digits
    if '_dept' in merged.columns and '_li' in merged.columns:
        def build_region_code(row):
            dept_raw = int(row['_dept']) if pd.notna(row['_dept']) else 0
            dept = str(dept_raw // 10).zfill(2)
            li = str(row['_li']).zfill(4)
            return f"{prv_code:02d}{city_code:03d}{dept}{li}"

        merged['區域別代碼'] = merged.apply(build_region_code, axis=1)
        merged = merged.drop(columns=['_dept', '_li'], errors='ignore')
    else:
        merged['區域別代碼'] = ''

    # Reorder columns
    base_cols = ['縣市', '行政區別', '鄰里', '區域別代碼']
    other_cols = [c for c in merged.columns if c not in base_cols and not c.startswith('_')]
    merged = merged[base_cols + other_cols]

    return merged


def process_local_election(county: str, year: int) -> pd.DataFrame:
    """處理地方公職選舉

    Args:
        county: 縣市名稱
        year: 選舉年份

    Returns:
        合併後的 DataFrame
    """
    print(f"\n處理 {county} {year} 地方公職選舉...")

    processor = ElectionProcessor(county, year)
    results = []

    if year == 2022:
        sub = 'prv' if processor.is_municipality else 'city'
        results.append(processor.process_election(f'C1/{sub}', '縣市長' if not processor.is_municipality else '直轄市長'))
        results.append(processor.process_election(f'T1/{sub}', '區域縣市議員' if not processor.is_municipality else '區域直轄市議員'))
        results.append(processor.process_election(f'T2/{sub}', '平原縣市議員' if not processor.is_municipality else '平原直轄市議員'))
        results.append(processor.process_election(f'T3/{sub}', '山原縣市議員' if not processor.is_municipality else '山原直轄市議員'))
        if not processor.is_municipality:
            results.append(processor.process_election('R1', '鄉鎮市民代表', key_by_dept=True))
    else:
        if processor.is_municipality:
            results.append(processor.process_election('直轄市市長', '直轄市長'))
            results.append(processor.process_election('直轄市區域議員', '區域直轄市議員'))
            results.append(processor.process_election('直轄市平原議員', '平原直轄市議員'))
            results.append(processor.process_election('直轄市山原議員', '山原直轄市議員'))
        else:
            results.append(processor.process_election('縣市市長', '縣市長'))
            results.append(processor.process_election('縣市鄉鎮市長', '鄉鎮市長', key_by_dept=True))
            results.append(processor.process_election('縣市區域議員', '區域縣市議員'))
            results.append(processor.process_election('縣市平原議員', '平原縣市議員'))
            results.append(processor.process_election('縣市山原議員', '山原縣市議員'))
            results.append(processor.process_election('縣市鄉鎮市民代表', '鄉鎮市民代表', key_by_dept=True))

    return merge_election_results(results, county)


def process_presidential_election(county: str, year: int) -> pd.DataFrame:
    """處理總統立委選舉

    Args:
        county: 縣市名稱
        year: 選舉年份

    Returns:
        合併後的 DataFrame
    """
    print(f"\n處理 {county} {year} 總統立委選舉...")

    processor = ElectionProcessor(county, year)
    results = []

    results.append(processor.process_election('總統', '總統候選人', is_president=True))
    results.append(processor.process_election('區域立委', '區域立委'))
    results.append(processor.process_election('平地立委', '平原立委'))
    results.append(processor.process_election('山地立委', '山原立委'))
    results.append(processor.process_party_list())

    return merge_election_results(results, county)
