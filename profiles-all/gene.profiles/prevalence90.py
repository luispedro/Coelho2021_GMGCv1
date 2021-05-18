from glob import glob
from jug import TaskGenerator, CachedFunction


USE_1M = True
EXCLUDE_DUPS = True

def blocks_of(xs, n):
    xs = list(xs)
    while xs:
        yield xs[:n]
        xs = xs[n:]

def load_c90d():
    import pandas as pd
    import pickle

    print(0)
    c90 = pd.read_table('cold/GMGC.95nr.90lc_cluster.tsv', header=None, index_col=1, squeeze=True)
    print(1)
    r_rename = pickle.load(open('cold/r_rename.pkl', 'rb'))
    print(2)
    print(3)
    print(4)
    c90.index = c90.index.map(r_rename.get)
    print(5)
    c90d = c90.to_dict()
    print(6)
    return c90d


_c90d = None

@TaskGenerator
def prevalences(us, filter_1m=False):
    import pandas as pd
    import subsamplex
    global _c90d
    if _c90d is None:
        _c90d = load_c90d()
    from collections import defaultdict, Counter
    biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)
    prevalence = defaultdict(Counter)
    used = []
    for u in us:
        s = u.split('/')[1].split('.')[0]
        b = biome[s]
        f = pd.read_table(u, index_col=0, squeeze=True)
        if filter_1m:
            if f.sum() < 1_000_000:
                continue
            f.values.flat[:] = subsamplex.subsample(f.values.ravel(), 1000*1000)
            f = f[f>0]
        used.append(s)
        present = set(f.index.map(_c90d.get))
        prevalence[b].update(present)
        prevalence['all'].update(present)
    return prevalence, used


@TaskGenerator
def build_presence_histograms(prevs, filter_1m=False, exclude_dups=False):
    import pandas as pd
    from collections import Counter, defaultdict
    final = defaultdict(Counter)
    for p,_ in prevs:
        for b,cs in p.items():
            cur = final[b]
            for g,c in cs.items():
                cur[g] += c
        for b, k in final.items():
            print(b, len(k))
        print()
    dfinal = {}
    for k,v in final.items():
        dfinal[k] = pd.Series(v)

    final = pd.DataFrame(dfinal)
    final.fillna(0, inplace=True)
    final = final.astype(int)
    hists = pd.DataFrame({b:final[b].value_counts() for b in final.columns}).fillna(0)
    ext = ''
    if filter_1m:
        ext += '1m.'
    if exclude_dups:
        ext += 'no-dups.'
    final.to_csv('tables/GMGC.90nr.gene.counts.{}txt'.format(ext), sep='\t')
    hists = hists.astype(int)
    hists.to_csv('tables/GMGC.90nr.gene.hists.{}txt'.format(ext), sep='\t')

uniques = CachedFunction(glob, 'outputs.txt/*.unique.txt.gz')
uniques.sort()
if EXCLUDE_DUPS:
    duplicates = set(line.strip() for line in open('cold/duplicates.txt'))
    filtered = []
    for u in uniques:
        s = u.split('/')[1].split('.')[0]
        if s not in duplicates:
            filtered.append(u)
    print(len(uniques))
    uniques = filtered
    print(len(uniques))
prevs = []
for us in blocks_of(uniques, 512):
    prevs.append(prevalences(us, USE_1M))

build_presence_histograms(prevs, USE_1M, EXCLUDE_DUPS)
