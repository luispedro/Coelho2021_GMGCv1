from safeout import safeout
def generate_biome_count(include_singletons=True):

    from itertools import groupby
    from collections import Counter
    blacklist = set(line.strip() for line in open('cold/redundant.complete.sorted.txt'))

    biome = {}
    for line in open('cold/biome.txt'):
        if line[0] == '\t': continue
        s, b = line.strip().split('\t')
        biome[s] = b
    biome['Fr12'] = 'Freeze12'


    counts = Counter()
    seen = set()

    for k,ells in groupby(open('cold/GMGC.relationships.txt'), lambda ell: ell.split()[-1]):
        if k in blacklist:
            continue
        cur = set()
        for ell in ells:
            g,_,_ = ell.split()
            s,_ = g.split('_')
            cur.add(biome[s])
        cur.add(biome[k.split('_')[0]])
        seen.add(k)
        counts[frozenset(cur)] += 1


    if include_singletons:
        for g in open('cold/derived/GMGC.95nr.headers.sorted'):
            g = g.strip()
            if g not in seen:
                s,_ = g.split('_')
                counts[frozenset([biome[s]])] += 1

    return counts

def find_travelers(oname, fr12name):
    from itertools import groupby
    blacklist = set(line.strip() for line in open('cold/redundant.complete.sorted.txt'))
    biome = {}
    for line in open('cold/biome.txt'):
        if line[0] == '\t': continue
        s, b = line.strip().split('\t')
        biome[s] = b
    biome['Fr12'] = 'Freeze12'

    with safeout(oname, 'wt') as output:
        with safeout(fr12name, 'wt') as output12:
            for k,ells in groupby(open('cold/GMGC.relationships.txt'), lambda ell: ell.split()[-1]):
                if k in blacklist:
                    continue
                cur = set()
                for ell in ells:
                    g,_,_ = ell.split()
                    s,_ = g.split('_')
                    cur.add(biome[s])
                cur.add(biome[k.split('_')[0]])
                if len(cur) > 1:
                    if len(cur - set(['Freeze12'])) > 1:
                        output.write(f'{k}')
                        for b in cur:
                            if b != 'Freeze12':
                                output.write(f'\t{b}')
                        output.write('\n')
                    elif k.startswith('Fr12'):
                        [b0,b1] = cur
                        b = (b0 if b0 != 'Freeze12' else b1)
                        output12.write(f'{k}\t{b}\n')
    return oname

def count_shuffled_travelers(i):
    import numpy as np
    import pandas as pd
    from itertools import groupby
    blacklist = set(line.strip() for line in open('cold/redundant.complete.sorted.txt'))

    r =  np.random.RandomState(123 + 77*i)
    biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)
    r.shuffle(biome)

    biome = biome.to_dict()
    biome['Fr12'] = 'Freeze12'
    n = 0
    for k,ells in groupby(open('cold/GMGC.relationships.txt'), lambda ell: ell.split()[-1]):
        if k in blacklist:
            continue
        cur = set()
        for ell in ells:
            g,_,_ = ell.split()
            s,_ = g.split('_')
            if biome[s] != 'Freeze12': cur.add(biome[s])
        b = biome[k.split('_')[0]]
        if b != 'Freeze12': cur.add(b)
        if len(cur) > 1:
            n += 1
            if n % 100_000 == 0:
                print(n//1000)
    return n


def get_pair_counts(counts):
    from collections import defaultdict
    def pairs(k):
        k = list(k)
        for i, a in enumerate(k):
            for b in k[i+1:]:
                yield (a,b)

    biomes = set()
    for k in counts:
        biomes.update(k)
    biomes = list(biomes)
    biomes.sort()
    paired = defaultdict(int)

    for k in counts:
        if len(k) == 1: continue
        for b0,b1 in pairs(k):
            paired[frozenset([b0,b1])] += counts[k]
    return paired


def write_out(counts, oname):
    from collections import defaultdict
    summary = defaultdict(int)
    for bs,k in counts.items():
        for b in bs:
            summary[b] += k
        if len(bs - set(['Freeze12'])) > 1:
            summary['multiple'] += k

    summary['freeze12only'] = counts[frozenset(['Freeze12'])]
    bs = list(summary.keys())
    bs.sort(key=summary.get, reverse=True)
    with safeout(oname, 'wt') as output:
        for b in bs:
            if b in ['multiple', 'freeze12only']:
                continue
            output.write('{:40} {:-11,}\n'.format(b, summary[b]))
        output.write('\n\n')

        for b in ['multiple', 'freeze12only']:
            output.write('{:40} {:-11,}\n'.format(b, summary[b]))


