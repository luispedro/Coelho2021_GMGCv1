from scipy import stats
import subsamplex
import numpy as np
from glob import glob
import pandas as pd
from jug import mapreduce, CachedFunction, TaskGenerator

def load_cog525(names):
    cog525 = set()
    for line in open('cold/single.copy.MGs/all-matches/COG0525.IDs.txt'):
        nid = int(line.split('.')[1], 10)
        cog525.add(names[nid])
    return cog525

def load_gene2bactNOG():
    gene2bactNOG = {}
    for line in open('cold/annotations/GMGC.95nr.emapper.annotations'):
        tokens = line.strip('\n').split('\t')
        bactNOG = [b for b in tokens[9].split(',') if 'bactNOG' in b]
        if len(bactNOG):
            gene2bactNOG[tokens[0]] = bactNOG[0]
    return gene2bactNOG

def load_gene2ko():
    return load_annotation(6)
def load_gene2highNOG():
    return load_annotation(11)

def load_annotation(ix):
    group = {}
    for line in open('cold/annotations/GMGC.95nr.emapper.annotations'):
        tokens = line.strip('\n').split('\t')
        tok = tokens[ix]
        if len(tok):
            group[tokens[0]] = tok
    return group

_gene2bactNOG = None
_gene2ko = None
_cog525 = None
_names = None

def load_all():
    global _gene2bactNOG
    global _gene2ko
    global _names
    global _cog525
    if _gene2bactNOG is None:
        _gene2bactNOG = load_gene2bactNOG()
    if _gene2ko is None:
        _gene2ko = load_gene2ko()
    if _names is None:
        _names = [line.strip() for line in open('cold/derived/GMGC10.old-headers')]
    if _cog525 is None:
        _cog525 = load_cog525(_names)


def groupby_kos(s):
    from collections import defaultdict
    kos = s.groupby(_gene2ko).sum()
    summed = defaultdict(float)
    for k in kos.index:
        cur = kos.loc[k]
        for t in k.split(','):
            summed[t] = summed[t] + cur
    return pd.Series(summed)


def compute_diversity(f):
    print(f'compute_diversity({f})')
    load_all()
    s = pd.read_feather(f)
    s['index'] = [_names[i] for i in s['index'].values]
    s.set_index('index', inplace=True)
    s = s['raw_unique'].astype(np.int).copy()

    gene_shannon = stats.entropy(s.values)
    bactNOG = s.groupby(_gene2bactNOG).sum()
    bactNOG_shannon = stats.entropy(bactNOG.values)
    kos = groupby_kos(s)
    ko_shannon = stats.entropy(kos.values)
    cog525genes = s[s.index.map(_cog525.__contains__)]
    cog525_shannon = stats.entropy(cog525genes.values)

    if s.sum() < 1000*1000:
        return (gene_shannon,
                    bactNOG_shannon,
                    ko_shannon,
                    cog525_shannon,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0)

    ss = subsamplex.subsample(s.values.ravel(), 1000*1000, copy_data=False)
    s.data[:] = ss
    s = s[s > 0]
    gene_1m_rich = len(s)
    gene_1m_shannon = stats.entropy(s.values)

    bactNOG = s.groupby(_gene2bactNOG).sum()
    bactNOG_1m_shannon = stats.entropy(bactNOG.values)
    bactNOG_1m_rich = len(bactNOG)

    kos = groupby_kos(s)
    ko_1m_shannon = stats.entropy(kos.values)
    ko_1m_rich = len(kos)
    cog525genes = s[s.index.map(_cog525.__contains__)]
    cog525_1m_shannon = stats.entropy(cog525genes.values)
    cog525_1m_rich = len(cog525genes)

    return (gene_shannon,
                bactNOG_shannon,
                ko_shannon,
                cog525_shannon,

                gene_1m_shannon, gene_1m_rich,
                bactNOG_1m_shannon, bactNOG_1m_rich,
                ko_1m_shannon, ko_1m_rich,
                cog525_1m_shannon, cog525_1m_rich)


@TaskGenerator
def save_table(diversity, files, oname):
    samples = [f.split('/')[1].split('.')[0] for f in files]
    cols = ['gene_shannon',
                'bactNOG_shannon',
                'ko_shannon',
                'cog525_shannon',

                'gene_1m_shannon', 'gene_1m_rich',
                'bactNOG_1m_shannon', 'bactNOG_1m_rich',
                'ko_1m_shannon', 'ko_1m_rich',
                'cog525_1m_shannon', 'cog525_1m_rich']
    diversity = pd.DataFrame(diversity, columns=cols, index=samples)
    diversity.to_csv(oname, sep='\t')


files = CachedFunction(glob, 'outputs/*.feather')
files.sort()
diversity = mapreduce.map(compute_diversity, files, 32)
save_table(diversity, files, 'tables/diversity.tsv')
