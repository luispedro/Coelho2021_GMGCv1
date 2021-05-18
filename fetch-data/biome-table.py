import numpy as np
import pandas as pd
from pdutils import pdselect
from curation import load_curated_samples_data, load_curated_studies_data
import ena

vocabulary = [

     'human skin',
     'human blood plasma',
     'human oral',
     'human nose',
     'human vagina',
     'human gut',
     'pig gut',
     'mouse gut',
     'mouse skin',
     'dog gut',
     'animal gut',

     'built-environment',
     'freshwater',
     'wastewater',
     'marine',
     'water',
     'soil',

     'other',
     ]

biome = []

necessary = set([line.strip() for line in open('../../data/samples.txt')])
smeta = load_curated_samples_data()
studies = load_curated_studies_data()
hmp = pd.read_excel('./data/humansample_upload_template.xlsx', skiprows=7, index_col=0)
hmpsel = hmp.select(lambda s : s in necessary)
HMP_TRANSLATION_MAP = {
        'intestine environment [ENVO:2100002]' : 'human gut',
        'mouth environment [ENVO:08000002]' : 'human oral',
        'nose [UBERON:0000004]' : 'human nose',
        'skin environment [ENVO:2100003]' : 'human skin',
        'vagina [UBERON:0000996]' : 'human vagina'
    }

biome.append(hmpsel.environment_feature.map(HMP_TRANSLATION_MAP))
necessary -= set(biome[-1].index)

hmp_combined = hmp.select(lambda ix: ',' in str(ix))
hmp_combined_biome = {}
for ix,b in hmp_combined.environment_feature.iteritems():
    for s in ix.split(', '):
        if s in necessary:
            hmp_combined_biome[s] = b
hmp_combined_biome = pd.Series(hmp_combined_biome).map(HMP_TRANSLATION_MAP)
biome.append(hmp_combined_biome)
necessary -= set(biome[-1].index)

whole_study_biome = studies.env_biome.map(lambda g: {
	'forest' : 'soil',
	'human blood plasma' : 'human blood plasma',
	'human gut' : 'human gut',
	'human skin' : 'human skin',
	'marine biome (ENVO:00000447)' : 'marine',
	}.get(g, np.nan)).dropna()

whole_study_biome_sample = pdselect(smeta, study_accession__in=whole_study_biome.index).study_accession
biome.append(whole_study_biome_sample.map(whole_study_biome))
necessary -= set(biome[-1].index)

dog = pd.Series({s:'dog gut' for s in necessary if s.startswith('ExDog')})
biome.append(dog)
necessary -= set(biome[-1].index)

smeta['sample_accession'] = smeta.index
sel = pdselect(smeta, sample_accession__in=list(necessary), env_biome__in=vocabulary)
biome.append(sel.env_biome)
necessary -= set(biome[-1].index)

amb = set(smeta.loc[list(necessary)].study_accession)
for s in amb:
    sel = pdselect(smeta, study_accession=s, env_biome__in=set(vocabulary)).env_biome
    if len(sel):
        biome.append(sel)
        necessary -= set(biome[-1].index)

sel = smeta.loc[list(necessary)].env_biome.map({
        'city' : 'built-environment',
        'ENVO:Temperate grasslands, savannas, and shrubland biome' : 'soil',
        'laboratory biome' : 'other',
        'ENVO:Temperate Desert Division (340)' : 'soil',
        'human saliva' : 'human oral',
        'mock community' : 'other',
        'sewage system' : 'wastewater',
        'ENVO:Temperate coniferous forest biome' : 'soil'
        }).dropna()
biome.append(sel)
necessary -= set(biome[-1].index)

exp = [n for n in necessary if n.startswith('SRX')]
exp_biome = {}
for e in exp:
    s = ena.parse_experiment_meta(ena.get_data_xml(e), is_string=True)
    s = s[e]
    sample = ena.parse_sample_meta(ena.get_data_xml(s['sample_accession']), is_string=True)
    exp_biome[e] = sample[s['sample_accession']]['env_biome']
    assert exp_biome[e] in set(vocabulary)

biome.append(pd.Series(exp_biome))
necessary -= set(biome[-1].index)

sel = pdselect(smeta.loc[list(necessary)], study_accession='SRP066479')
SRP066479_TRANSLATION_MAP = {
            ('periurban shantytown', 'animal feces', 'feces') : 'animal gut',
            ('periurban shantytown', 'human', 'feces') : 'human gut',
            ('rural village', 'agricultural plot', 'soil') : 'soil',
            ('rural village', 'animal feces', 'feces') : 'animal gut',
            ('rural village', 'composting latrine chamber', 'composting feces with ash') : 'other',
            ('rural village', 'human', 'feces') : 'human gut',
}
SRP066479_biome = {}

for s,vals in sel[['env_biome', 'env_feature', 'env_material']].iterrows():
    if s in necessary:
        SRP066479_biome[s] = SRP066479_TRANSLATION_MAP[tuple(vals)]
biome.append(pd.Series(SRP066479_biome))
necessary -= set(biome[-1].index)

ERP012880_study = {}
sel = pdselect(smeta.loc[list(necessary)], study_accession='ERP012880')

for ix in pdselect(sel, host_taxid=9606, sample_type='skin').index:
    ERP012880_study[ix] = 'human skin'

for ix in pdselect(sel, sample_type='soil').index:
    ERP012880_study[ix] = 'soil'
biome.append(pd.Series(ERP012880_study))
necessary -= set(biome[-1].index)

sel = pdselect(smeta.loc[list(necessary)], study_accession='ERP012866')
ERP012866_study = sel.body_habitat.map({'UBERON:cecum' : 'mouse gut'
        ,'UBERON:feces' : 'mouse gut'
        ,'UBERON:skin' : 'mouse skin'
        })
biome.append(ERP012866_study)
necessary -= set(biome[-1].index)

# 10090 is Mus musculus.
# 10900 is "Bluetongue virus (serotype 10 / American isolate)"
# Thus, the assumption is that 10900 is a typo

sel = pdselect(smeta.loc[list(necessary)], study_accession='SRP059841', env_material='fresh water')
biome.append(pd.Series({ ix: 'freshwater' for ix in sel.index}))
necessary -= set(biome[-1].index)

for pr in ['PRJEB7626',
        'PRJNA254830',
        'PRJNA183510',
        'PRJNA225837',
        ]:
    pr = ena.get_project_reads_table(pr, as_pandas_DataFrame=True)
    biome.append(pd.Series({ix: 'soil' for ix in pr.sample_accession if ix in necessary}))
    necessary -= set(biome[-1].index)

sel = pdselect(smeta.loc[list(necessary)], study_accession='SRP029441')
selc = sel[[c for c in sel.columns if not sel[c].isnull().all()]]
biome.append(selc.isolation_source.map({"gut microbe": "human gut"}))
necessary -= set(biome[-1].index)


byhand = pd.Series({
    'SRS062878' : 'human oral',
    'SRS058105' : 'human oral',
    'SRS056519' : 'human gut',
    'SAMN03271453' : 'other',
    'SAMN03840426' : 'human skin',
    'SAMN03840428' : 'human skin',
    })
biome.append(byhand)
necessary -= set(biome[-1].index)

necessary = set([line.strip() for line in open('../../data/samples.txt')])
for b in biome:
    b.name = 'Biome'
pd_biome = pd.concat(biome)
pd_biome = pd_biome[list(necessary)]
pd.DataFrame({'biome': pd_biome}).to_csv('data/biome.tsv', sep='\t')
