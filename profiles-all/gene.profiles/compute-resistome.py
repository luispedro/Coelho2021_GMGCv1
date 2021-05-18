import pandas as pd
from glob import glob
from jug import CachedFunction, TaskGenerator

from concurrent.futures import ThreadPoolExecutor

files = CachedFunction(glob, 'outputs/*.feather')
files.sort()

def load1(fname):
    f = pd.read_feather(fname)
    f.set_index('index', inplace=True)
    return f

@TaskGenerator
def load_card():
    from diskhash import Str2int
    card = pd.read_table('cold/annotations/GMGC.card_resfam_updated.out.r', index_col=1)
    gene2index = Str2int('/local/coelho/genecats.cold/gene2index.s2i', 0, 'r')
    card = card.rename(index=gene2index.lookup)
    card = card[['AROs']]
    return card.copy()

_card = None

@TaskGenerator
def card1(fname, card):
    return load1(fname).groupby(card.AROs).sum()

@TaskGenerator
def save_results(data, oname):
    data = pd.DataFrame(data)
    data.fillna(0, inplace=True)
    data.to_csv(oname, sep='\t')
    data.reset_index(inplace=True)
    data.to_feather(oname.replace('.txt', '.feather'))


card = load_card()
results = {}
for f in files:
    sample = f.split('/')[1].split('.')[0]
    results[sample] = card1(f, card)


for col, oname in [('scaled', 'tables/resistome.txt'),
        ('unique_scaled', 'tables/resistome.unique.txt')]:
    save_results({k:v[col] for k,v in results.items()}, oname)
