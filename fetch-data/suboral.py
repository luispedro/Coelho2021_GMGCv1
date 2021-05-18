import pandas as pd
from itertools import chain
biome = pd.read_table('/g/scb2/bork/coelho/DD_DeCaF/genecats.cold/biome.txt', index_col=0, squeeze=True)
meta = pd.read_table('/g/scb2/bork/coelho/DD_DeCaF/genecats.cold/selected-cleaned-metadata-100.tsv', index_col=0)
oral = biome.index[biome == 'human oral']


hmp = pd.read_table('HMP/2017-09-06-hmp-samples.csv', index_col=0, comment='#')
hmp.loc['765013792-BUCC-0']
hmp_oral = hmp[hmp.ena_ers_sample_id.map(set(oral).__contains__)]
hmp_oral = hmp_oral[['ena_ers_sample_id', 'environment_material']].copy()

hmp_tag = {}
for (name,ub) in hmp_oral.iterrows():
    tag = name.split('-')[1]
    ub = ub.environment_material
    if tag in hmp_tag:
        assert hmp_tag[tag] == ub
    else:
        hmp_tag[tag] = ub
hmp_oral.set_index('ena_ers_sample_id', inplace=True)

suboral = hmp_oral.squeeze()

zhang_RA = pd.read_table('Zhang_RA/Zhang.rheumatoid_arthritis.csv', index_col=0,  comment='#')
zhang_oral = zhang_RA[zhang_RA.index.map( set(oral).__contains__)]

suboral = suboral.append(zhang_oral['environment_material'])

suboral = suboral.append(zhang_oral.loc[zhang_oral.aliases.dropna().index].set_index('aliases')['environment_material'])

aagaard = pd.read_table('Aagaard/aagaard_template.csv', comment='#', index_col=0)
aagard_oral = aagaard[aagaard.index.map(set(oral).__contains__)]
suboral = suboral.append(pd.Series({s:'saliva [ENVO:02000036]' for s in chain(aagard_oral.index.values, aagard_oral.aliases.values)}))

fiji_meta = meta[meta.study_accession == 'SRP029441']
fiji_meta = fiji_meta[fiji_meta.index.map(set(oral).__contains__)]

suboral = suboral.append(pd.Series({s:'saliva [ENVO:02000036]' for s in fiji_meta.index}))

maturation = meta[meta.study_accession == 'SRP078001']
maturation = maturation[maturation.index.map(set(oral).__contains__)]
suboral = suboral.append(pd.Series({s:'gingiva [UBERON:0001828]' for s in maturation.index}))

hmp = pd.read_table('HMP/2017-09-07-all-experiments-merged.tsv', index_col=0, comment='#')
hmp_oral = hmp[hmp.sample_ena_accession.map(set(oral).__contains__)]
hmp_oral.set_index('sample_ena_accession', inplace=True)
hmp_sub = hmp_oral.sample_alias.map(lambda al: hmp_tag[al.split('-')[1]])
hmp_sub = hmp_sub[~hmp_sub.index.map(set(suboral.index).__contains__).values.astype(bool)]
suboral = suboral.append(hmp_sub)
suboral = suboral.append(pd.Series({
        'SRS018661': 'buccal mucosa [UBERON:0006956]',
        'SRS062878': 'hard palate [UBERON:0003216]',
        'SRS077736': 'dorsum of tongue [UBERON:0009471]',
        }))
suboral = suboral[~suboral.index.duplicated().astype(bool)]

short = {
'buccal mucosa [UBERON:0006956]': 'buccal-mucosa',
 'dental plaque [UBERON:0016482]': 'dental-plaque',
 'dorsum of tongue [UBERON:0009471]': 'dorsum-of-tongue',
 'gingiva [UBERON:0001828]': 'gingiva',
 'hard palate [UBERON:0003216]': 'hard-palate',
 'palatine tonsil [UBERON:0002373]': 'palatine-tonsil',
 'saliva [ENVO:02000036]': 'saliva',
 'subgingival dental plaque [UBERON:0016484]': 'subgingival-dental-plaque',
 'supragingival dental plaque [UBERON:0016485]': 'supragingival-dental-plaque',
 'throat [UBERON:0000341]': 'throat',
 'tongue keratinized epithelium [UBERON:0004650]': 'tongue-keratinized-epithelium'}

pd.DataFrame({'short': suboral.map(short), 'full': suboral}).to_csv('suboral.tsv', sep='\t')
