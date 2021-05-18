import numpy as np
import pandas as pd

def frame(s, ix, biome, label):
    s = pd.DataFrame(np.hstack([np.zeros((s.shape[0], 1)), s[:,:,ix]]).astype(int))
    s['Biome'] = biome
    s['Selection'] = label
    return s

biomes = {
        'all.npy': 'all',
        'built-environment.npy': 'built-environment',
        'cat-gut.npy': 'cat-gut',
        'dog-gut.npy': 'dog-gut',
        'freshwater.npy': 'freshwater',
        'human-gut.npy': 'human-gut',
        'human-nose.npy': 'human-nose',
        'human-oral.npy': 'human-oral',
        'human-skin.npy': 'human-skin',
        'human-vagina.npy': 'human-vagina',
        'marine.npy': 'marine',
        'mouse-gut.npy': 'mouse-gut',
        'pig-gut.npy': 'pig-gut',
        'soil.npy': 'soil',
        'wastewater.npy': 'wastewater',
        }

tables = {}
with pd.ExcelWriter('preprocessed/SourceData_Rarefy.xlsx') as out:
    for fname, b in biomes.items():
        data = np.load('results/'+fname)
        data = pd.concat([
                    frame(data,  0, b, 'All genes'),
                    frame(data,  1, b, r'≥ 1% of samples'),
                    frame(data,  2, b, r'≥ 5% of samples'),
                    frame(data, -1, b, 'No singletons'),
                    ]).reset_index().drop('index', axis=1)
        data.to_excel(out, sheet_name=b)
