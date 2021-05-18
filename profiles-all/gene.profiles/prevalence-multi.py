from glob import glob
from jug import TaskGenerator, CachedFunction

def blocks_of(xs, n):
    xs = list(xs)
    while xs:
        yield xs[:n]
        xs = xs[n:]

_completes = None

@TaskGenerator
def complete_richness(us):
    import pandas as pd
    import subsamplex
    global _completes
    if not _completes:
        print ("Loading completes...")
        _completes = set(line.strip() for line in open('cold/derived/GMGC10.complete.original_names.txt'))
        print("Loaded")
    richness = {}
    for u in us:
        sample = u.split('/')[1].split('.')[0]
        f = pd.read_table(u, index_col=0, squeeze=True)
        cur = {}
        cur['complete_richness'] = len(set(f.index) & _completes)
        if f.sum() < 1_000_000:
            cur['complete_richness_1m'] = 0
        else:
            f.values.flat[:] = subsamplex.subsample(f.values.ravel(), 1000*1000)
            f = f[f>0]
            cur['complete_richness_1m'] = len(set(f.index) & _completes)
        richness[sample] = cur
    return richness


_cache = None

def load_tables():
    import pickle
    import pandas as pd
    print("Reading r_rename")
    r_rename = pickle.load(open('cold/r_rename.pkl', 'rb'))
    print("Reading clusters families")
    gf = pd.read_table('cold/GMGC10.protein-clusters-families.tsv', index_col=0)
    name2ix = {}
    for i,ix in enumerate(gf.index):
        name2ix[r_rename[ix]] = i
        if len(name2ix) % 5000000 == 0:
            print("Building name mapping: ", len(name2ix)/1000/1000)


    print("Loading cluster families (complete)")
    gf_c = pd.read_table('cold/GMGC10.complete-orfs.protein-families.tsv', index_col=0)

    print("Building name mapping (complete)")
    name2ix_c = {}
    for i,ix in enumerate(gf_c.index):
        name2ix_c[r_rename[ix]] = i
        if len(name2ix_c) % 5000000 == 0:
            print("Building name mapping (complete): ", len(name2ix_c)/1000/1000)
    print("Loaded")
    return gf, name2ix, gf_c, name2ix_c


@TaskGenerator
def presences(us, filter_1m=False):
    import pandas as pd
    import subsamplex
    global _cache
    if _cache is None:
        _cache = load_tables()
    gf, name2ix, gf_c, name2ix_c = _cache
    from collections import defaultdict, Counter
    biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)
    counters = defaultdict(Counter)
    richness = {}
    for u in us:
        sample = u.split('/')[1].split('.')[0]
        b = biome[sample]
        f = pd.read_table(u, index_col=0, squeeze=True)
        for filter_1m in [False, True]:
            if filter_1m:
                if f.sum() < 1_000_000:
                    continue
                f.values.flat[:] = subsamplex.subsample(f.values.ravel(), 1000*1000)
                f = f[f>0]
            ext = '.1m' if filter_1m else ''
            for name,g,n2ix in [
                    ('full'+ext, gf, name2ix),
                    ('complete' + ext, gf_c, name2ix_c),
                    ]:
                sel = g.iloc[f.index.map(n2ix.get).dropna().astype(int)]
                sel_s = {c:set(sel[c]) for c in sel.columns}
                richness[sample, name] = {c:len(s) for c,s in sel_s.items()}
                for c,s in sel_s.items():
                    counters[c, filter_1m].update(s)
    return richness, counters


@TaskGenerator
def n_biome(used, remove_dups=True):
    import pandas as pd
    biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)
    samples = set(used)
    if remove_dups:
        duplicates = set(line.strip() for line in open('cold/duplicates.txt'))
        samples-= duplicates
    return biome.reindex(samples).value_counts().to_dict()


def combine_prevalences2(r0, r1):
    print('combine_prevalences2')
    import pandas as pd
    res = {}
    assert set(r0.keys()) == set(r1.keys())
    for k in r0.keys():
        if k[0] == 'id90':
            print('SKIPPING ', k)
            continue
        print('combine_prevalences2', k)
        s0 = pd.Series(r0[k])
        s1 = pd.Series(r1[k])
        ss = s0.add(s1, fill_value=0)
        ss = ss.astype(int)
        res[k] = ss
    return res

def paired(xs):
    xs = list(xs)
    while xs:
        if len(xs) >= 2:
            x0,x1 = xs[:2]
            yield (x0,x1)
            xs = xs[2:]
        elif len(xs) == 1:
            [x] = xs
            yield (x,)
            xs = []


@TaskGenerator
def combine_prevalences(rs):
    print('combine_prevalences')
    while len(rs) > 1:
        print('combine_prevalences', len(rs))
        rs_ = []
        for p in paired(rs):
            if len(p) == 2:
                rs_.append(combine_prevalences2(*p))
            else:
                rs_.extend(p)
        rs = rs_
    [r] = rs
    return r
    
@TaskGenerator
def rebuild_richness(p):
    import pandas as pd
    p = pd.DataFrame(p)
    return pd.concat(
                [  p.xs('full', axis=1, level=1, drop_level=True).dropna()
                ,  p.xs('complete', axis=1, level=1, drop_level=True).dropna()
                ], axis=0).astype(int).T

@TaskGenerator
def rebuild_richness_1m(p):
    import pandas as pd
    p = pd.DataFrame(p)
    return pd.concat(
                [  p.xs('full.1m', axis=1, level=1, drop_level=True).dropna()
                ,  p.xs('complete.1m', axis=1, level=1, drop_level=True).dropna()
                ], axis=0).astype(int).T


@TaskGenerator
def concat_save(ps, oname):
    import pandas as pd
    pd.concat(ps).to_csv(oname, sep='\t')

@TaskGenerator
def save_outputs(r):
    import pandas as pd
    for k,v in r.items():
        pd.DataFrame({
            'counts':pd.Series(v)
            }).to_csv('tables/multi-counts/'+str(v)+'.tsv',sep='\t')

@TaskGenerator
def save_richness(criches):
    import pandas as pd
    res = {}
    for c in criches:
        res.update(c)
        
    res = pd.DataFrame(res).T
    res.to_csv('tables/gene.richness.complete-orfs.tsv', sep='\t')

uniques = CachedFunction(glob, 'outputs.txt/*.unique.txt.gz')
uniques.sort()
prevs = []
riches = []
criches = []
for us in blocks_of(uniques, 256):
    p = presences(us)
    riches.append(p[0])
    prevs.append(p[1])
    criches.append(complete_richness(us))
    #used = extract_used(ps)
#n_biome(used[True])
#build_histograms(ps)

while len(prevs) > 1:
    prevs = [combine_prevalences(ps)
                for ps in blocks_of(prevs, 4)]

[prevalence] = prevs

save_outputs(prevalence)

riches_1m = [rebuild_richness_1m(r) for r in riches]
riches = [rebuild_richness(r) for r in riches]
concat_save(riches_1m, 'tables/richness-multi.1m.tsv')
concat_save(riches, 'tables/richness-multi.tsv')
save_richness(criches)
