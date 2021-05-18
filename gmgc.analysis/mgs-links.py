from jug import TaskGenerator
import numpy as np
import pandas as pd


BASEDIR = f'/g/scb2/bork/myers/GMGC/MGS'
BASEDIR ='/g/scb2/bork/myers/GMGC/MGS/MGS_filter'

@TaskGenerator
def find_best_pairs(compares, share_threshold):
    ordered = compares.values.ravel().argsort()[::-1]
    paired = []
    paired_a = set()
    paired_b = set()
    for ix in ordered:
        if compares.values.flat[ix] < share_threshold:
            break
        c0,c1 = np.unravel_index(ix, compares.shape)
        if c0 in paired_a or c1 in paired_b: continue
        paired.append((compares.index[c0],compares.columns[c1]))
        paired_a.add(c0)
        paired_b.add(c1)
    return paired


@TaskGenerator
def compare_mgs_sets(biome_a, biome_b):
    genesets = {}
    for b in [biome_a, biome_b]:
        genesets[b] = pd.read_table(f'{BASEDIR}/{b}MGS_sets.tsv', squeeze=True, index_col=0, header=None)

    dict_a = {k:set(v.split(',')) for k,v in genesets[biome_a].items()}
    dict_b = {k:set(v.split(',')) for k,v in genesets[biome_b].items()}


    compares = np.zeros((len(dict_a), len(dict_b)))

    for ci,c in enumerate(dict_a):
        for di,d in enumerate(dict_b):
            compares[ci,di] = len(dict_a[c] & dict_b[d])/min(len(dict_a[c]), len(dict_b[d]))
    return pd.DataFrame(compares, index=list(dict_a.keys()), columns=list(dict_b.keys()))

@TaskGenerator
def fraction(compares, share_threshold):

    share0 = np.mean(compares.max(0) >= share_threshold)
    share1 = np.mean(compares.max(1) >= share_threshold)
    return share0, share1

mgs_name = {
    'built-environment' : 'builtenv',
    'cat gut': 'catgut',
    'dog gut': 'doggut',
    'freshwater': 'freshwater',
    'human nose': 'humannose',
    'human oral': 'humanoral',
    'human skin': 'humanskin',
    'human vagina': 'humanvagina',
    'marine': 'marine',
    'mouse gut': 'mousegut',
    'pig gut': 'piggut',
    'human gut': 'humangut',
    'soil': 'soil',
    'wastewater': 'wastewater',
}

shared = {}
fractions = {}
pairs = {}
for ma in mgs_name.values():
    for mb in mgs_name.values():
        if ma < mb:
            c = compare_mgs_sets(ma, mb)
            shared[ma, mb] = c
            pairs[ma, mb] = find_best_pairs(c, 0.5)
            fractions[ma, mb] = fraction(c, 0.5)
