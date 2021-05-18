import pandas as pd
rename = {}
for line in open('cold/GMGC10.rename.table.txt'):
    nname,oname = line.split()
    rename[oname] = nname
out = open('cold/GMGC10.taxonomic.map', 'wt')
first = True
for chunk in pd.read_table('cold/GMGC.95nr.taxonomic.map', index_col=0, chunksize=100000):
    chunk.rename(index=rename, inplace=True)
    chunk.to_csv(out, sep='\t', header=first)
    first = False


