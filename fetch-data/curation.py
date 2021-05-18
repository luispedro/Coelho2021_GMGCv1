import numpy as np
import pandas as pd

def add_studies_extra(studies):
    studies_extra = {
     'ERP012929' : { 'env_biome': 'human gut' },
     'SRP001634' : { 'env_biome': 'human gut' },
     'SRP049645' : { 'env_biome': 'human skin' },
     'ERP012880' : { 'env_biome': 'dead humans' },
     'ERP002469' : { 'env_biome': 'human gut' },
     'SRP040146' : { 'env_biome': 'human gut' },
     'SRP056054' : { 'env_biome': 'human gut' },
     'SRP056821' : { 'env_biome': 'human skin' },
     'ERP016013' : { 'env_biome': 'human blood plasma' },
     'ERP005534' : { 'env_biome': 'human gut', 'env_material' : 'feces' },
     'ERP013933' : { 'env_biome': 'human gut', },

     'SRP029441' : { 'env_biome': 'human gut, human saliva' },

     'SRP078001' : { 'env_biome': 'human gut, human oral' },

     # HMP
     'SRP002163' : { 'env_biome': 'human' },
     'SRP002163,SRP002395' : { 'env_biome': 'human' },
     'SRP002163,SRP002860' : { 'env_biome': 'human' },



    }
    studies.loc['ERP013933',['env_biome']] = np.nan
    studies.loc['ERP005534',['env_biome', 'env_material']] = np.nan
    studies.loc['SRP090628', 'env_biome'] = 'human gut'
    studies.loc['SRP064400', 'env_biome'] = 'human gut'

    return studies.combine_first(pd.DataFrame(studies_extra).T)


def add_samples_extra(smetadata):
    from pdutils import pdselect
    fmeta = pdselect(smetadata, study_accession='SRP029441')
    fmeta_env_biome = fmeta['isolation-source'].map(lambda isrc: {
        'Stool' : 'human gut',
        'Saliva': 'human saliva',
        'Soil': 'soil',
        }.get(isrc, isrc))
    pure_studies = {
            'SRP040146': 'human gut',
            'ERP002469': 'human gut',
            'SRP049645': 'human skin',
            'ERP016013': 'human blood plasma',
            'SRP056054': 'human gut',
            'SRP001634': 'human gut',
            'ERP012929': 'human gut',
            'SRP056821': 'human skin',
            'ERP012880': 'dead humans',
            'SRP090628': 'human gut',
            'ERP005534': 'human gut',
            'ERP013933': 'human gut',
            }

    for st,biome in pure_studies.items():
        smetadata.loc[smetadata.study_accession == st, 'env_biome'] = biome

    is_SRP078001 = smetadata.study_accession == 'SRP078001'
    smetadata.loc[is_SRP078001, 'env_biome'] = smetadata[is_SRP078001].env_material.map({'stool':'human gut', 'Oral': 'human oral'})

    return smetadata.combine_first(pd.DataFrame({'env_biome': fmeta_env_biome}))

def load_curated_studies_data():
    studies = pd.read_table('data/studies.tsv', index_col=0)
    return add_studies_extra(studies)

def load_curated_samples_data():
    import pandas as pd
    metadata = pd.read_table('data/selected-cleaned-metadata-100.tsv', index_col=0)
    return add_samples_extra(metadata)
