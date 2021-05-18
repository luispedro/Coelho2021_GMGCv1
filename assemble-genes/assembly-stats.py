from os import path
from glob import glob
import pandas as pd

biome = pd.read_table('/g/bork1/coelho/DD_DeCaF/genecats.cold/biome.txt', index_col=0)
header = True
with open('orf-stats.tsv', 'wt') as output:
    for f in sorted(glob('orfs/*.tab')):
        cur_biome = biome.loc[path.basename(f).split('-')[0]][0]
        for chunk in pd.read_table(f,
                                    chunksize=1000000,
                                    index_col=0,
                                    header=None,
                                    names=['gene','strand','start','end','complete']):
            chunk['length'] = chunk.end - chunk.start + 1
            chunk['biome'] = cur_biome
            chunk[['strand', 'length', 'complete', 'biome']].to_csv(output, header=header, sep='\t')
            header = None
