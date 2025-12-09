with open('voteData/2014-103年地方公職人員選舉/縣市市長/elcand.csv', 'rb') as f:
    line = f.readline()
    cols = len(line.split(b','))
    print(f'2014縣市市長 elcand欄位數: {cols}')
    print(f'前100字元: {line[:100]}')
