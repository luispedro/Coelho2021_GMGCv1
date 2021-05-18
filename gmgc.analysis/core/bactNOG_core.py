import pandas as pd
computed = pd.read_table('cold/sample.computed.tsv', index_col=0)   
mapped = computed.insertsHQ        

biome = pd.read_table('../../gmgc.analysis/cold/biome.txt', squeeze=True, index_col=0)
bactNOG = pd.read_feather('tables/bactNOGS.unique.feather', nthreads=24)
bactNOG.set_index('index', inplace=True)

bactDetect = bactNOG > 0
bsel = bactDetect.T[mapped > 1e6]
biome = biome.reindex(bsel.index)
bactprevs = {}
for b in set(biome.values):
    bactprevs[b] = bsel.loc[biome==b].mean()
    print(f'Done {b}')

bactprevs['all'] = bsel.mean()
bactprevs['genomes' ] = pd.read_table('outputs/genomic.relative.txt', index_col=0, squeeze=True)
bactprevs = pd.DataFrame(bactprevs)
bactprevs.to_csv('tables/bactNOGS.prevalence.tsv', sep='\t')

if False:
    
    rare = bp.genomes <.1
    bp = bactprevs
    rare = bp.genomes <.1
    for c in bp.columns:
        print(c,(bp[c] > .9).sum(), (rare & (bp[c] > .9)).sum())
    
    ball = bsel.mean()
    
    commcore = ball[((ball > .9) & rare)].copy()
    commcore.sort_values()
    commcore = ball[((ball > .9) & rare)].copy().sort_values()
    
    info = pd.read_table('bactNOG.annotations.tsv.gz', header=None, index_col=1)
    
    
    
    
    info.loc[commcore.index.map(lambda c: 'ENOG41'+c.split('@')[0])]
    urare =info.loc[commcore.index.map(lambda c: 'ENOG41'+c.split('@')[0])]
    
    
    
    info.loc[commcore.index.map(lambda c: 'ENOG41'+c.split('@')[0])].values
    info.loc[commcore.index.map(lambda c: 'ENOG41'+c.split('@')[0])]
    
    
    
    bp['genome'][commcore.index]
    bp['genomes'][commcore.index]
    genomes = bp['genomes'][commcore.index]
    
    
    
    
    data = pd.DataFrame({'metagenomes': commcore, 'genomes' : bp.genomes[commcore.index]})
    
    
    
    
    data = data.sort_values(by='metagenomes', ascending=False)
    info.loc[data.index.map(lambda c: 'ENOG41'+c.split('@')[0])]
    info.loc[data.index.map(lambda c: 'ENOG41'+c.split('@')[0])][5]
    info.loc[data.index.map(lambda c: 'ENOG41'+c.split('@')[0])][5].values
    data['name'] = info.loc[data.index.map(lambda c: 'ENOG41'+c.split('@')[0])][5].values
    
    
    
    
    open('test.html', 'wt').write(data.to_html())
    
