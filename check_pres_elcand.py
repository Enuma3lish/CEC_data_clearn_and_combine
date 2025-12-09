import pandas as pd

df = pd.read_csv('voteData/2020總統立委/總統/elcand.csv', header=None, encoding='utf-8', quotechar='"', quoting=1)
print('總統elcand前3筆 (prv, city, name欄):')
print(df[[0, 1, 6]].head(3))
