import numpy as np
import pandas as pd

def merge_cols(df, target, sources):
    for source in sources:
        if source not in df.columns:
            continue
        overlap = df[[target, source]].dropna()
        if len(overlap):
            if np.all(overlap[target] == overlap[source]):
                df[source][overlap.index] = np.nan

        new_col = pd.concat([df[target].dropna(), df[source].dropna()], verify_integrity=True)
        df[target] = new_col
        df.drop(source, axis=1, inplace=True)



def cleanup_metadata(selected):
    hasdata = selected.shape[0] - selected.isnull().sum()
    # remove rarely used features:
    selected = selected.T[hasdata >= 100].T
    selected.rename(columns={
        'study name': 'study_name',
        }, inplace=True)


    for c in ['SAMN02302271', 'SAMN02302272', 'SAMN02302397', 'SAMN02302398']:
        assert selected.loc[c]['biome'] == 'Lake'
        assert selected.loc[c]['env_biome'] == 'lake'
        selected.loc[c]['biome'] = np.nan


    merge_cols(selected, 'project_name', ['project name'])
    merge_cols(selected, 'isolation_source', ['isolation-source'])

    merge_cols(selected, 'env_biome',    ['environment (biome)',   'Environment (Biome)', 'biome'])
    merge_cols(selected, 'env_feature',  ['environment (feature)', 'Environment (Feature)', 'feature'])
    merge_cols(selected, 'env_material', ['environment (material)'])

    merge_cols(selected, 'host_sex', ['Sex', 'sex'])
    merge_cols(selected, 'host_age', ['age'])
    merge_cols(selected, 'host_taxid', ['host_tax_id', 'Host taxID', 'host taxid'])

    # special case: this is "Water" and the env_material entry is "water"
    assert selected['material']['SAMN02786246'] in ['water', 'Water']
    assert selected['env_material']['SAMN02786246'] == 'water'
    selected['material']['SAMN02786246'] = np.nan
    # Some exceptional cases do not allow this merging:
    #merge_cols(selected, 'env_material', ['material'])

    selected['env_biome'] = selected['env_biome'].map(lambda f: {
        'faeces' : 'feces', # feces is the preferred name
        'Human gut stool' : 'human gut',
        'Forest' : 'forest',
        }.get(f,f))

    selected['env_feature'] = selected['env_feature'].map(lambda f: {
        'faeces' : 'feces', # feces is the preferred name
        }.get(f,f))


    hasdata = selected.shape[0] - selected.isnull().sum()
    hasdata.sort_values(inplace=True)
    selected = selected[hasdata.index[::-1]]

    return selected
