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
    """選舉類型基礎類別

    所有選舉類型的配置都在此定義，處理函數會根據這些配置自動調整行為。
    新增選舉類型只需在此定義配置，無需修改處理邏輯。

    Attributes:
        key: 選舉類型識別碼（如 'council', 'president'）
        name: 選舉名稱（如 '縣市議員選舉'）
        year: 選舉年份
        data_folder: 資料夾名稱
        output_template: 輸出檔名模板，支援 {year}, {city_name} 變數
        is_multi_area: 是否多選區（議員、立委等）
        use_polling_station: 是否輸出投開票所層級資料
        use_village_summary: 是否使用村里彙總列（tbox=0）
        is_national_candidate: 是否全國層級候選人（總統、原住民立委等）
        has_combined_name: 是否合併候選人名稱（總統副總統組合）
        is_party_vote: 是否為政黨票（候選人就是政黨）
        election_category: 選舉類別（用於合併檔案分類）
        merge_key: 合併檔案時使用的 key
    """

    def __init__(
        self,
        key,                        # 選舉類型 key
        name,                       # 選舉名稱
        year,                       # 年份
        data_folder,                # 資料夾名稱
        output_template,            # 輸出檔名模板
        is_multi_area=False,        # 是否多選區
        use_polling_station=False,  # 是否輸出投開票所層級
        use_village_summary=True,   # 是否使用村里彙總列
        is_national_candidate=False, # 是否全國候選人
        candidate_name_col=6,       # 候選人姓名欄位索引
        candidate_party_col=7,      # 候選人政黨欄位索引
        has_combined_name=False,    # 是否合併名稱
        is_party_vote=False,        # 是否為政黨票
        election_category=None,     # 選舉類別
        merge_key=None,             # 合併檔案 key
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
        self.is_party_vote = is_party_vote
        self.election_category = election_category or key
        self.merge_key = merge_key or key

    def get_output_filename(self, city_name):
        """取得輸出檔名"""
        return self.output_template.format(year=self.year, city_name=city_name)

    def __repr__(self):
        return f"ElectionType({self.key}, {self.year}, multi_area={self.is_multi_area})"


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
        election_category='council',
        merge_key='council',
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
        election_category='council',
        merge_key='council',
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
        election_category='mayor',
        merge_key='mayor',
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
        election_category='mayor',
        merge_key='mayor',
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
        election_category='township_mayor',
        merge_key='township_mayor',
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
        election_category='president',
        merge_key='president',
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
        election_category='legislator',
        merge_key='legislator',
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
        election_category='mountain_legislator',
        merge_key='mountain_legislator',
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
        election_category='plain_legislator',
        merge_key='plain_legislator',
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
        is_party_vote=True,
        election_category='party_vote',
        merge_key='party_vote',
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
        ('mayor', '縣市長選舉'),
        ('council', '縣市議員選舉'),
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
