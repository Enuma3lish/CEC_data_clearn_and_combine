import sys
sys.path.append('processors')
from processors.format_converter import load_full_candidate_info
from pathlib import Path

info = load_full_candidate_info(Path('db.cec.gov.tw/voteData'), 2022, '花蓮縣', 'Councilor')
test_names = ['傅國淵', '吳建志', '吳東昇', '張峻', '張正治']

for name in test_names:
    result = info.get(name, 'NOT FOUND')
    print(f'{name}: {result}')

print(f'\n山原議員總數: {len([k for k,v in info.items() if v.get("type")=="mountain"])}')
print(f'平原議員總數: {len([k for k,v in info.items() if v.get("type")=="plain"])}')
