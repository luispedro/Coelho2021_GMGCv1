from glob import  glob

from jug import TaskGenerator, CachedFunction
from jug import mapreduce

COGFILES = [
    'cold/fetchMG.output/COG0012.faa',
    'cold/fetchMG.output/COG0016.faa',
    'cold/fetchMG.output/COG0018.faa',
    'cold/fetchMG.output/COG0048.faa',
    'cold/fetchMG.output/COG0049.faa',
    'cold/fetchMG.output/COG0052.faa',
    'cold/fetchMG.output/COG0080.faa',
    'cold/fetchMG.output/COG0081.faa',
    'cold/fetchMG.output/COG0085.faa',
    'cold/fetchMG.output/COG0087.faa',
    'cold/fetchMG.output/COG0088.faa',
    'cold/fetchMG.output/COG0090.faa',
    'cold/fetchMG.output/COG0091.faa',
    'cold/fetchMG.output/COG0092.faa',
    'cold/fetchMG.output/COG0093.faa',
    'cold/fetchMG.output/COG0094.faa',
    'cold/fetchMG.output/COG0096.faa',
    'cold/fetchMG.output/COG0097.faa',
    'cold/fetchMG.output/COG0098.faa',
    'cold/fetchMG.output/COG0099.faa',
    'cold/fetchMG.output/COG0100.faa',
    'cold/fetchMG.output/COG0102.faa',
    'cold/fetchMG.output/COG0103.faa',
    'cold/fetchMG.output/COG0124.faa',
    'cold/fetchMG.output/COG0172.faa',
    'cold/fetchMG.output/COG0184.faa',
    'cold/fetchMG.output/COG0185.faa',
    'cold/fetchMG.output/COG0186.faa',
    'cold/fetchMG.output/COG0197.faa',
    'cold/fetchMG.output/COG0200.faa',
    'cold/fetchMG.output/COG0201.faa',
    'cold/fetchMG.output/COG0202.faa',
    'cold/fetchMG.output/COG0215.faa',
    'cold/fetchMG.output/COG0256.faa',
    'cold/fetchMG.output/COG0495.faa',
    'cold/fetchMG.output/COG0522.faa',
    'cold/fetchMG.output/COG0525.faa',
    'cold/fetchMG.output/COG0533.faa',
    'cold/fetchMG.output/COG0541.faa',
    'cold/fetchMG.output/COG0552.faa',
]
files = [f for f in CachedFunction(glob, 'outputs/*.feather') if 'unique' not in f]
files.sort()

@TaskGenerator
def load_cogs():
    matches = []
    for c in COGFILES:
        matches.append(set())
        for line in open(c):
            if line[0] == '>':
                matches[-1].add(line.strip().split()[0][1:])
    return matches

def compute_shannon(f, matches):
    import pandas as pd
    from scipy import stats
    shannon = {}
    sample = f.split('/')[1].split('.')[0]
    f = pd.read_feather(f, nthreads=4)
    f1 = f[f.columns[1]] # The data column
    for c,m in zip(COGFILES, matches):
        shannon[sample,c] = stats.entropy(f1[f['index'].map(m.__contains__)])
    return shannon

@TaskGenerator
def merge_dics(ds):
    r = {}
    for d in ds:
        r.update(d)
    return r

@TaskGenerator
def save_table(final, oname):
    import pandas as pd
    samples = set(s for s,_ in final)
    cogs = set(c for _,c in final)
    final = pd.DataFrame({c: {s:final[s,c] for s in samples} for c in cogs})
    final.rename(columns= lambda c: c.split('/')[-1][:-len('.faa')], inplace=True)
    final.to_csv(oname, sep='\t')

cogs = load_cogs()
final = merge_dics(
                mapreduce.currymap(compute_shannon,
                                        [(f,cogs) for f in files],
                                        map_step=64))
save_table(final, 'tables/shannon.div.tsv')
