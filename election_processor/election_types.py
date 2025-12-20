# -*- coding: utf-8 -*-
"""
選舉類型配置
Election Type Configuration

定義所有選舉類型的參數和處理方式，便於新增和修改選舉類型。
"""

# 統計欄位名稱
STAT_FIELDS = ['有效票數', '無效票數', '投票數', '已領未投票數', '發出票數', '用餘票數', '選舉人數', '投票率']

# 最大候選人/政黨數量（用於合併檔案）
MAX_CANDIDATES = 50


class ElectionType:
    """選舉類型基礎類別"""

    def __init__(
        self,
        key,                      # 選舉類型 key（如 'council', 'president'）
        name,                     # 選舉名稱（如 '縣市議員選舉'）
        year,                     # 年份
        data_folder,              # 資料夾名稱
        output_template,          # 輸出檔名模板
        is_multi_area=False,      # 是否多選區（如議員、立委）
        use_polling_station=False, # 是否輸出投開票所層級
        use_village_summary=True,  # 是否使用村里彙總列
        is_national_candidate=False, # 是否全國候選人（如總統）
        candidate_name_col=6,     # 候選人姓名欄位索引
        candidate_party_col=7,    # 候選人政黨欄位索引
        has_combined_name=False,  # 是否合併名稱（如總統副總統）
    ):
        self.key = key
        self.name = name
        self.year = year
        self.data_folder = data_folder
        self.output_template = output_template
        self.is_multi_area = is_multi_area
        self.use_polling_station = use_polling_station
        self.use_village_summary = use_village_summary
        self.is_national_candidate = is_national_candidate
        self.candidate_name_col = candidate_name_col
        self.candidate_party_col = candidate_party_col
        self.has_combined_name = has_combined_name


# 2014 選舉類型配置
ELECTION_TYPES_2014 = {
    # 直轄市區域議員
    'council_municipality': ElectionType(
        key='council_municipality',
        name='直轄市區域議員選舉',
        year=2014,
        data_folder='直轄市區域議員',
        output_template='{year}_直轄市區域議員_各投開票所得票數_{city_name}.xlsx',
        is_multi_area=True,
        use_polling_station=True,
        use_village_summary=False,
    ),
    # 縣市區域議員
    'council_county': ElectionType(
        key='council_county',
        name='縣市區域議員選舉',
        year=2014,
        data_folder='縣市區域議員',
        output_template='{year}_縣市區域議員_各投開票所得票數_{city_name}.xlsx',
        is_multi_area=True,
        use_polling_station=True,
        use_village_summary=False,
    ),
    # 直轄市市長
    'mayor_municipality': ElectionType(
        key='mayor_municipality',
        name='直轄市市長選舉',
        year=2014,
        data_folder='直轄市市長',
        output_template='{year}_直轄市市長_各村里得票數_{city_name}.xlsx',
        is_multi_area=False,
        use_village_summary=True,
    ),
    # 縣市市長
    'mayor_county': ElectionType(
        key='mayor_county',
        name='縣市市長選舉',
        year=2014,
        data_folder='縣市市長',
        output_template='{year}_縣市市長_各村里得票數_{city_name}.xlsx',
        is_multi_area=False,
        use_village_summary=True,
    ),
    # 鄉鎮市長
    'township_mayor': ElectionType(
        key='township_mayor',
        name='鄉鎮市長選舉',
        year=2014,
        data_folder='縣市鄉鎮市長',
        output_template='{year}_鄉鎮市長_各村里得票數_{city_name}.xlsx',
        is_multi_area=True,
        use_village_summary=True,
    ),
}

# 2020 選舉類型配置
ELECTION_TYPES_2020 = {
    # 總統
    'president': ElectionType(
        key='president',
        name='總統選舉',
        year=2020,
        data_folder='總統',
        output_template='{year}_總統候選人得票數一覽表_各村里_{city_name}.xlsx',
        is_multi_area=False,
        use_village_summary=True,
        is_national_candidate=True,
        has_combined_name=True,
    ),
    # 區域立委
    'legislator': ElectionType(
        key='legislator',
        name='區域立法委員選舉',
        year=2020,
        data_folder='區域立委',
        output_template='{year}_區域立委_各村里得票數_{city_name}.xlsx',
        is_multi_area=True,
        use_village_summary=True,
    ),
    # 山地原住民立委
    'mountain_legislator': ElectionType(
        key='mountain_legislator',
        name='山地原住民立委選舉',
        year=2020,
        data_folder='山地立委',
        output_template='{year}_山地原住民立委_各村里得票數_{city_name}.xlsx',
        is_multi_area=False,
        use_village_summary=True,
        is_national_candidate=True,
    ),
    # 平地原住民立委
    'plain_legislator': ElectionType(
        key='plain_legislator',
        name='平地原住民立委選舉',
        year=2020,
        data_folder='平地立委',
        output_template='{year}_平地原住民立委_各村里得票數_{city_name}.xlsx',
        is_multi_area=False,
        use_village_summary=True,
        is_national_candidate=True,
    ),
    # 政黨票
    'party_vote': ElectionType(
        key='party_vote',
        name='政黨票',
        year=2020,
        data_folder='不分區政黨',
        output_template='{year}_政黨票_各村里得票數_{city_name}.xlsx',
        is_multi_area=False,
        use_village_summary=True,
        is_national_candidate=True,
    ),
}

# 所有選舉類型
ALL_ELECTION_TYPES = {**ELECTION_TYPES_2014, **ELECTION_TYPES_2020}

# 依年份分類
ELECTION_TYPES_BY_YEAR = {
    2014: ELECTION_TYPES_2014,
    2020: ELECTION_TYPES_2020,
}


def get_election_types(year=None):
    """取得選舉類型列表

    Args:
        year: 年份（None 表示所有年份）

    Returns:
        dict: 選舉類型配置字典
    """
    if year is None:
        return ALL_ELECTION_TYPES
    return ELECTION_TYPES_BY_YEAR.get(year, {})


def get_election_config(key):
    """取得指定選舉類型配置

    Args:
        key: 選舉類型 key

    Returns:
        ElectionType or None
    """
    return ALL_ELECTION_TYPES.get(key)


# 合併檔案配置
MERGE_CONFIGS = {
    2014: [
        ('council', '縣市議員選舉'),
        ('mayor', '縣市長選舉'),
        ('township_mayor', '鄉鎮市長選舉'),
    ],
    2020: [
        ('president', '總統選舉'),
        ('legislator', '區域立法委員選舉'),
        ('mountain_legislator', '山地原住民立委選舉'),
        ('plain_legislator', '平地原住民立委選舉'),
        ('party_vote', '政黨票'),
    ],
}


# 檔案路徑映射（用於合併檔案）
FILE_PATTERNS = {
    2014: {
        'council': {
            'municipality': '{year}_直轄市區域議員_各投開票所得票數_{city_name}.xlsx',
            'county': '{year}_縣市區域議員_各投開票所得票數_{city_name}.xlsx',
        },
        'mayor': {
            'municipality': '{year}_直轄市市長_各村里得票數_{city_name}.xlsx',
            'county': '{year}_縣市市長_各村里得票數_{city_name}.xlsx',
        },
        'township_mayor': {
            'county': '{year}_鄉鎮市長_各村里得票數_{city_name}.xlsx',
        },
    },
    2020: {
        'president': '{year}_總統候選人得票數一覽表_各村里_{city_name}.xlsx',
        'legislator': '{year}_區域立委_各村里得票數_{city_name}.xlsx',
        'mountain_legislator': '{year}_山地原住民立委_各村里得票數_{city_name}.xlsx',
        'plain_legislator': '{year}_平地原住民立委_各村里得票數_{city_name}.xlsx',
        'party_vote': '{year}_政黨票_各村里得票數_{city_name}.xlsx',
    },
}
