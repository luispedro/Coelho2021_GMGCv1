from itertools import groupby
import pandas as pd

cog525 = set(line.strip() for line in open('cold/single.copy.MGs/COG0525.IDs.txt'))
print("Loaded COG list")
rtable = pd.read_table('cold/GMGC10.rename.table.txt', index_col=0, squeeze=True, header=None)
print("Loaded rtable")

cog525names = rtable[rtable.index.map(cog525.__contains__)]
cog525olds = set(cog525names.values)

species = pd.read_table('/g/bork1/coelho/DD_DeCaF/genecats.cold/GMGC10.species.match.map',  header=None, usecols=[1,2], index_col=0, squeeze=True, names=['gene', 'TaxID'], engine='c')
taxonomic = pd.read_table('cold/GMCG10.taxonomic.reconciliation.map', index_col=0)

taxonomic525 = taxonomic.loc[cog525names.index]
species525 = species[cog525names.index]
species525 = species525.dropna().astype(int)
taxonomic525['rank'][species525.index] = 'species'
d_taxonomic525 = taxonomic525.T.to_dict()
print("Loaded taxonomic table")

abund = pd.read_feather('tables/cog525.feather', nthreads=8)
print("Loaded Abunduncance")
abund.set_index('index', inplace=True)
biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)


def read_lines():
    for line in open('cold/GMGC.relationships.txt'):
        yield line.rstrip().split()
old2new = {o:n for n,o in cog525names.to_dict().items()}
biome = biome.to_dict()
biome['Fr12'] = 'genome'


output = open('outputs/cog525.meta.txt', 'wt')
w = 0
seen = set()
for o,rs in groupby(read_lines(), lambda c:c[2]):
    if o in cog525olds:
        orb = biome[o.split('_')[0]]
        others = set(biome[r.split('_')[0]] for r,_,_ in rs)
        others.add(orb)
        g = old2new[o]
        otb = '/'.join(others)
        if 'genome' not in others:
            t = 'meta'
        elif len(others) > 1:
            t = 'mixed'
        else:
            t = 'isolate'
        taxinfo = d_taxonomic525[g]
        output.write(f'{g}\t{o}\t{taxinfo["taxid"]}\t{taxinfo["rank"]}\t{t}\t{orb}\t{otb}\t{taxinfo["name"]}\n')
        w += 1
        seen.add(g)
        if w % 100 == 0:
            print(f'Done {w}')
for o in cog525olds:
    g = old2new[o]
    if g not in seen:
        orb = biome[o.split('_')[0]]
        otb = orb
        t = ('isolate' if orb == 'genome' else 'meta')
        taxinfo = d_taxonomic525[g]
        output.write(f'{g}\t{o}\t{taxinfo["taxid"]}\t{taxinfo["rank"]}\t{t}\t{orb}\t{otb}\t{taxinfo["name"]}\n')

output.close()
