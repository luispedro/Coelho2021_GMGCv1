from glob import glob
from jug import TaskGenerator, CachedFunction
from projections import load_gf_projection


def blocks_of(xs, n):
    xs = list(xs)
    while xs:
        yield xs[:n]
        xs = xs[n:]


_gfd = None

@TaskGenerator
def prevalences(us, filter_1m=False):
    import pandas as pd
    import subsamplex
    global _gfd
    if _gfd is None:
        _gfd = load_gf_projection()
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
        present = set(f.index.map(_gfd.get))
        prevalence[b].update(present)
        prevalence['all'].update(present)
    return prevalence, used


@TaskGenerator
def presences(us, filter_1m=False):
    import pandas as pd
    import subsamplex
    global _gfd
    if _gfd is None:
        _gfd = load_gf_projection()
    from collections import defaultdict, Counter
    biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)
    presences = {}
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
        present = set(f.index.map(_gfd.get))
        presences[s] = present
    return presences

@TaskGenerator
def richness(us):
    import pandas as pd
    import subsamplex
    global _gfd
    if _gfd is None:
        _gfd = load_gf_projection()
    richness = {}
    for u in us:
        s = u.split('/')[1].split('.')[0]
        f = pd.read_table(u, index_col=0, squeeze=True)
        detect = len(set(f.index.map(_gfd.get)))
        if f.sum() < 1_000_000:
            detect1m = 0
        else:
            f.values.flat[:] = subsamplex.subsample(f.values.ravel(), 1000*1000)
            f = f[f>0]
            detect1m = len(set(f.index.map(_gfd.get)))
        richness[s] = pd.Series({'gf_rich': detect, 'gf_1m_rich': detect1m})
    return pd.DataFrame(richness).T


@TaskGenerator
def build_presence_histograms(prevs, filter_1m=False):
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
    final.to_csv('tables/GMGC.gf.gene.counts.{}txt'.format('1m.' if filter_1m else ''), sep='\t')
    hists = hists.astype(int)
    hists.to_csv('tables/GMGC.gf.gene.hists.{}txt'.format('1m.' if filter_1m else ''), sep='\t')


@TaskGenerator
def extract_used(prevs):
    final = []
    for _, u in prevs:
        final.extend(u)
    return final


@TaskGenerator
def save_riches(riches):
    import pandas as pd
    pd.concat(riches).to_csv('tables/gf.richness.tsv', sep='\t')


@TaskGenerator
def build_histograms(ps):
    import numpy as np
    import pandas as pd
    from collections import defaultdict, Counter
    merged = {}
    for x in ps:
        merged.update(x)

    for line in open('tables/duplicates.txt'):
        line = line.strip()
        if line in merged:
            del merged[line]
    biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)
    prev = defaultdict(Counter)
    for s,vs in merged.items():
        b = biome[s]
        if b in {'amplicon', 'isolate'} : continue
        prev[b].update(vs)
        prev['all'].update(vs)

    prev_f = pd.DataFrame({k:pd.Series(v) for k,v in prev.items()})
    prev_f.to_csv('tables/GMGC.gf.gene.counts.1m.no-dup.txt', sep='\t')

    hist = pd.DataFrame({k:pd.Series(v).value_counts() for k,v in prev.items()})
    hist.fillna(0, inplace=True)
    hist = hist.reindex(np.arange(hist.index.max()+1)).fillna(0).astype(int)
    hist.to_csv('tables/GMGC.gf.gene.hists.1m.no-dup.txt', sep='\t')


@TaskGenerator
def n_biome(used, remove_dups=True):
    import pandas as pd
    biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)
    samples = set(used)
    if remove_dups:
        duplicates = set(line.strip() for line in open('cold/duplicates.txt'))
        samples-= duplicates
    return biome.reindex(samples).value_counts().to_dict()

uniques = CachedFunction(glob, 'outputs.txt/*.unique.txt.gz')
uniques.sort()
used = {}
ps = []
riches = []
for use_1m in [True, False]:
    prevs = []
    used[use_1m] = []
    for us in blocks_of(uniques, 256):
        prevs.append(prevalences(us, use_1m))
        if use_1m:
            ps.append(presences(us, use_1m))
            riches.append(richness(us))
    build_presence_histograms(prevs, use_1m)
    used[use_1m] = extract_used(prevs)
save_riches(riches)
n_biome(used[True])
build_histograms(ps)
