from jug import CachedFunction, TaskGenerator
from glob import glob

import pandas as pd
from collections import Counter
scaled = [f for f in CachedFunction(glob,'outputs.txt/*.txt.gz') if '.unique.' not in f]
scaled.sort()

_rename = None
@TaskGenerator
def sum_used(fs, strict=False):
    from os import path
    global _rename
    if _rename is None:
        import pickle
        print("Loading rename...")
        _rename = pickle.load(open('cold/rename.pkl', 'rb'))
        print("Renaming rename...")
        _rename = {o:int(n.split('.')[1], 10) for o, n in _rename.items()}
    total = 0
    used = Counter()
    for i,fname in enumerate(fs):
        print(f'sum_used( [{i} ])')
        f = pd.read_table(fname, index_col=0, squeeze=True)
        f_sum = f.sum()
        if strict:
            fname_ = fname.replace('.txt.gz', '.unique.txt.gz')
            if not path.exists(fname_):
                print('WHERE IS {}??'.format(fname_))
                f_ = f.iloc[:0]
            else:
                f_ = pd.read_table(fname_, index_col=0, squeeze=True)
            f = f.reindex(f_.index).fillna(0)
        used.update(f.index)
        f /= f_sum
        total = f.add(total, fill_value=0)
    used = pd.Series(used)
    df = pd.DataFrame({'sum': total, 'used': used})
    df.rename(index=_rename, inplace=True)
    return df.reset_index().values

@TaskGenerator
def add_partials(ps):
    total = 0
    for p in ps:
        p = pd.DataFrame(p, columns=['gene', 'sum', 'used']).set_index('gene')
        total = p.add(total, fill_value=0)
    return total.reset_index().values


@TaskGenerator
def divide(final):
    final = pd.DataFrame(final, columns=['gene', 'total', 'used']).set_index('gene')
    return final.eval('total/used').reset_index().values

@TaskGenerator
def save_table(f, oname):
    import pickle
    import pandas as pd
    f = pd.DataFrame(f, columns=['index','total', 'used'])
    f['avg'] = f.eval('total/used')

    f['index'] = f['index'].astype(int)
    f.to_feather(oname + '.feather')

    rename = pickle.load(open('cold/rename.pkl', 'rb'))
    r_newname = {}
    r_name = {}
    for k,v in rename.items():
        n = int(v.split('.')[1], 10)
        r_newname[n] = v
        r_name[n] = k

    f['new_name'] = f['index'].map(r_newname.get)

    f['old_name'] = f['index'].map(r_name.get)
    f.rename(columns={'index':'n_index', 'new_name':'gene'}, inplace=True)

    f.set_index('gene', inplace=True)
    f.to_csv(oname + '.tsv', sep='\t')

def blocks_of(xs, n):
    xs = list(xs)
    while xs:
        yield xs[:n]
        xs = xs[n:]

partials = []
for fs in blocks_of(scaled, 32):
    partials.append(sum_used(fs))

while len(partials) > 1:
    nps = []
    for ps in blocks_of(partials, 8):
        nps.append(add_partials(ps))
    partials = nps
final = divide(partials[0])

for strict in [False, True]:
    biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)
    allbs = []
    for b in set(biome.values):
        if b in {'amplicon', 'isolate'}: continue
        cur = [s for s in scaled if biome[s.split('/')[1].split('.')[0]] == b]
        partials = []
        for fs in blocks_of(cur, 32):
            if strict:
                partials.append(sum_used(fs, strict=True))
            else:
                partials.append(sum_used(fs))

        while len(partials) > 1:
            nps = []
            for ps in blocks_of(partials, 8):
                nps.append(add_partials(ps))
            partials = nps
        save_table(partials[0], 'tables/gene-avg/average-{}{}'.format(b.replace(' ', '-'), ('-strict' if strict else '')))
        allbs.append(partials[0])
        #finals[b] = divide(partials[0])
    save_table(add_partials(allbs), 'tables/gene-avg/average-all{}'.format(('-strict' if strict else '')))
