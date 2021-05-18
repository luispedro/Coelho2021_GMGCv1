from collections import Counter
from collections import defaultdict
import pandas as pd
biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)
reps_of = defaultdict(list)
for line in open('cold/GMGC.meta.100nr.rel.txt'):
    g0,_,g1=line.strip().split('\t')
    if biome[g0.split('_')[0]] != biome[g1.split('_')[0]]:
        reps_of[g1].append(g0)

gcounts = Counter()
n = 0
rcounts = Counter()
for  line in open('cold/GMGC.100nr.faa'):
    if line[0] == '>':
        g = line[1:-1]
        s = g.split('_')[0]
        gcounts[biome[s]] += 1
        rcounts[biome[s]] += 1
        other = set()
        for ot in reps_of.get(g, []):
            other.add(biome[ot.split('_')[0]])
        for b in other:
            rcounts[b] += 1
        n += 1
        if n % 1000_000 ==0:
            print(n//1_000_000)
print(gcounts)
print(rcounts)
