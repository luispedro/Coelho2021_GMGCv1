import pandas as pd
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

]

total = {}
for b in biomes:
    s = pd.read_feather('tables/totals/{}.feather'.format(b.replace(' ', '-')), nthreads=8)
    s.set_index('index', inplace=True)
    total[b] = s['scaled']

total = pd.DataFrame(total)
total.fillna(0, inplace=True)

rel = total/total.sum()

gene_names = [line.strip()  for line in open('cold/derived/GMGC10.old-headers')]
gene_ix_name = dict(enumerate(gene_names))
rel.rename(index=gene_ix_name, inplace=True)

biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)
travelers = {}
for line in open('../outputs/travelers.txt'):
    tokens = line.strip().split('\t')
    travelers[tokens[0]] = frozenset(tokens[1:])

traveled = {}
for b in rel.columns:
    u = frozenset([b])
    traveled[b] = rel[b].groupby(lambda g: travelers.get(g, u)).sum()

traveled = pd.DataFrame(traveled)
flow = pd.DataFrame([], index=rel.columns, columns=rel.columns)
for b0 in rel.columns:
    for b1 in rel.columns:
         flow.loc[b0, b1] = traveled[b0].loc[traveled[b0].index.map(lambda c : b1 in c)].sum()
flow['unshared'] = pd.Series({b:traveled[b][frozenset([b])] for b in rel.columns })
flow.to_csv('tables/flow.tsv', sep='\t')

up = {
   'cat gut': 'mammal gut',
   'dog gut': 'mammal gut',
   'human blood plasma': 'other human',
   'human gut': 'mammal gut',
   'human nose': 'other human',
   'human oral': 'other human',
   'human skin': 'other human',
   'human vagina': 'other human',
   'mouse gut': 'mammal gut',
   'pig gut': 'mammal gut',
}
uptravelers = {}
for k,v in travelers.items():
    v = frozenset(up.get(b,b) for b in v)
    if len(v) > 1:
        uptravelers[k] = v

uptotal = total.T.groupby(lambda e : up.get(e,e)).sum().T

uprel = uptotal/uptotal.sum()
uprel.rename(index=gene_ix_name, inplace=True)

uptraveled = {}
for b in uprel.columns:
    u = frozenset([b])
    uptraveled[b] = uprel[b].groupby(uptravelers).sum()
    uptraveled[b] = uptraveled[b].append(pd.Series({u: uprel[b].sum() - uptraveled[b].sum()}))

upflow = pd.DataFrame([], index=uprel.columns, columns=uprel.columns)
for b0 in uprel.columns:
    for b1 in uprel.columns:
        upflow.loc[b0, b1] = uptraveled[b0].loc[uptraveled[b0].index.map(lambda c : b1 in c)].sum()

upflow['unshared'] = pd.Series({b:uptraveled[b][frozenset([b])] for b in uprel.columns })

upflow.to_csv('tables/upflow.tsv', sep='\t')



