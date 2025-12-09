import pandas as pd

# Read raw elprof.csv
df = pd.read_csv('voteData/2020總統立委/總統/elprof.csv', header=None, encoding='utf-8-sig', dtype=str)

print('Column count:', len(df.columns))
print('\nAll values from first row:')
for i, val in enumerate(df.iloc[0]):
    print(f'Col {i}: {val}')
