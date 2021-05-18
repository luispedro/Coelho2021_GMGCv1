from jug import CachedFunction, TaskGenerator
from glob import glob

import pandas as pd
from collections import Counter
scaled = [f for f in CachedFunction(glob,'outputs.txt/*.txt.gz') if '.unique.' not in f]
scaled.sort()

@TaskGenerator
def sum_used(fs):
    total = 0
    used = Counter()
    for f in fs:
        f = pd.read_table(f, index_col=0, squeeze=True)
        used.update(f.index)
        f /= f.sum()
        total =  f.add(total, fill_value=0)
    used = pd.Series(used)
    return pd.DataFrame({'sum': total, 'used': used})

@TaskGenerator
def add_partials(ps):
    total = 0
    for p in ps:
        total = p.add(total, fill_value=0)
    return ps


@TaskGenerator
def divide(final):
    return final.eval('sum/used')

def blocks_of(xs, n):
    while xs:
        yield xs[:n]
        xs = xs[n:]

partials = []
for fs in blocks_of(scaled, 256):
    partials.append(sum_used(fs))

while len(partials) > 1:
    nps = []
    for ps in blocks_of(partials, 8):
        nps.append(add_partials(ps))
    partials = nps
final = divide(partials[0])
