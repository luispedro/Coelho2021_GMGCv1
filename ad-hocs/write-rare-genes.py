import pickle 
import pandas as pd
from glob import glob
import pandas as pd
from jug import TaskGenerator, CachedFunction


UNIQUES  = glob('outputs.txt/*.unique.txt.gz')
UNIQUES.sort()
rename = pickle.load(open('cold/rename.pkl', 'rb'))
prevalence = pd.read_feather('tables/genes.unique.prevalence.feather', nthreads=24)
pall = prevalence['all']
is_rare = pall <=10
rares = prevalence['index'][pall]
rares = prevalence['index'][is_rare]
rares_ix = set(rares.values)

rare_names = {k for k,v in rename.items() if int(v.split('.')[1], 10) in rares_ix}
with open('tables/rare-genes.txt', 'wt') as output:
    for g in rare_names:
        output.write(f'{g}\n')
        
