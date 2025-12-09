import pandas as pd

df = pd.read_csv('voteData/2020總統立委/總統/elcand.csv', header=None, encoding='utf-8', quotechar='"', quoting=1)
print('總統elcand前6筆 (prv, cand_no, name, party欄):')
df.columns = ['prv','city','area','dept','li','cand_no','name','party'] + [f'extra{i}' for i in range(len(df.columns)-8)]
print(df[['prv', 'cand_no', 'name', 'party']].head(6))
