"""
選舉資料處理配置
Configuration for election data processing
"""

from pathlib import Path

# 基本路徑設定
BASE_DIR = Path(__file__).parent.parent
VOTE_DATA_DIR = BASE_DIR / "voteData"
OUTPUT_DIR = BASE_DIR / "output"

# 縣市配置
# prv_code: 省/直轄市代碼, city_code: 縣市代碼, is_municipality: 是否為直轄市
COUNTY_CONFIG = {
    '花蓮縣': {'prv_code': 10, 'city_code': 15, 'is_municipality': False},
    '臺北市': {'prv_code': 63, 'city_code': 0, 'is_municipality': True},
    '新北市': {'prv_code': 65, 'city_code': 0, 'is_municipality': True},
    '桃園市': {'prv_code': 68, 'city_code': 0, 'is_municipality': True},
    '臺中市': {'prv_code': 66, 'city_code': 0, 'is_municipality': True},
    '臺南市': {'prv_code': 67, 'city_code': 0, 'is_municipality': True},
    '高雄市': {'prv_code': 64, 'city_code': 0, 'is_municipality': True},
    '基隆市': {'prv_code': 10, 'city_code': 17, 'is_municipality': False},
    '新竹市': {'prv_code': 10, 'city_code': 18, 'is_municipality': False},
    '嘉義市': {'prv_code': 10, 'city_code': 20, 'is_municipality': False},
    '宜蘭縣': {'prv_code': 10, 'city_code': 2, 'is_municipality': False},
    '新竹縣': {'prv_code': 10, 'city_code': 4, 'is_municipality': False},
    '苗栗縣': {'prv_code': 10, 'city_code': 5, 'is_municipality': False},
    '彰化縣': {'prv_code': 10, 'city_code': 7, 'is_municipality': False},
    '南投縣': {'prv_code': 10, 'city_code': 8, 'is_municipality': False},
    '雲林縣': {'prv_code': 10, 'city_code': 9, 'is_municipality': False},
    '嘉義縣': {'prv_code': 10, 'city_code': 10, 'is_municipality': False},
    '屏東縣': {'prv_code': 10, 'city_code': 13, 'is_municipality': False},
    '臺東縣': {'prv_code': 10, 'city_code': 14, 'is_municipality': False},
    '澎湖縣': {'prv_code': 10, 'city_code': 16, 'is_municipality': False},
    '金門縣': {'prv_code': 9, 'city_code': 20, 'is_municipality': False},
    '連江縣': {'prv_code': 9, 'city_code': 7, 'is_municipality': False},
}

# 選舉年份類型
LOCAL_YEARS = [2014, 2018, 2022]  # 地方公職人員選舉
PRESIDENTIAL_YEARS = [2016, 2020, 2024]  # 總統立委選舉

# 資料夾映射
YEAR_FOLDER_MAP = {
    2014: '2014-103年地方公職人員選舉',
    2016: '2016總統立委',
    2018: '2018-107年地方公職人員選舉',
    2020: '2020總統立委',
    2022: '2022-111年地方公職人員選舉',
    2024: '2024總統立委',
}

# 政黨代碼映射
PARTY_CODE_MAP = {
    '1': '中國國民黨',
    '2': '民主進步黨',
    '3': '親民黨',
    '4': '台灣團結聯盟',
    '5': '無黨團結聯盟',
    '6': '綠黨',
    '7': '新黨',
    '8': '台灣基進',
    '9': '台灣民眾黨',
    '10': '時代力量',
    '11': '一邊一國行動黨',
    '12': '勞動黨',
    '13': '中華統一促進黨',
    '14': '國會政黨聯盟',
    '15': '台澎黨',
    '16': '民主進步黨',
    '17': '社會民主黨',
    '18': '和平鴿聯盟黨',
    '19': '喜樂島聯盟',
    '20': '安定力量',
    '21': '合一行動聯盟',
    '90': '親民黨',
    '99': '無黨籍',
    '999': '無黨籍',
    '348': '喜樂島聯盟',
}
