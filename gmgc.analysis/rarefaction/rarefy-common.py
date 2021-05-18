from glob import glob
import numpy as np
from jug import TaskGenerator, CachedFunction
import pandas as pd
THRESHOLDS = [0, 0.01, 0.05, 0.1, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

base = 'samples_20180516'

biomes = [
        'marine',
        'human gut',
        'built-environment',
        'cat gut',
        'dog gut',
        'freshwater',
        'human nose',
        'human oral',
        'human skin',
        'human vagina',
        'mouse gut',
        'pig gut',
        'soil',
        'wastewater',
        'all',
]

_cache = None
def load_data():
    singletons = set(line.strip() for line in open('cold/derived/GMGC.singletons'))
    blacklist = set(line.strip() for line in open('cold/redundant.complete.sorted.txt'))

    prevalence = pd.read_feather('tables/genes.unique.prevalence.feather', nthreads=8)
    prevalence.set_index('index', inplace=True)

    biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)
    biome_counts = biome.value_counts()
    biome_counts['all'] = biome_counts.sum()
    prevalence = prevalence/biome_counts

    rename = {}
    for line in open('cold/GMGC10.rename.table.txt'):
        new,old = line.strip().split('\t')
        rename[old] = int(new.split('.')[1], base=10)
    return rename, prevalence, blacklist, singletons

@TaskGenerator
def rarefy(perm, b):
    global _cache
    if _cache is None:
        _cache = load_data()
    rename, prevalence, blacklist, singletons = _cache
    freq = np.zeros(311197450)
    freq_sparse = prevalence[b]
    freq[freq_sparse.index] = freq_sparse.values

    perm = [line.strip() for line in open(perm)]

    seen = [set() for _ in THRESHOLDS]
    seen.append(set())
    sizes = []
    for sample in perm:
        for line in open(f'{base}/{sample}'):
            gene = line.strip()
            if gene in blacklist:
                continue
            ix = rename[gene]
            f = freq[ix]
            for s, t in zip(seen, THRESHOLDS):
                if f < t:
                    break
                s.add(ix)
            if gene not in singletons:
                seen[-1].add(ix)
        cur = [len(s) for s in seen]
        sizes.append(cur)
        print(len(sizes), cur)
    return np.array(sizes)


@TaskGenerator
def save_results(data, b):
    data = np.array(data)
    oname = 'results/'+b.replace(' ', '-')
    np.save(oname, data)
    return oname

results = {}
for b in biomes:
    permuts = CachedFunction(glob, 'outputs/rarefaction_{}/perm_*'.format(b.replace(' ', '_')))
    permuts.sort()
    results[b] = []
    for p in permuts:
        results[b].append(rarefy(p, b))
    save_results(results[b], b)
