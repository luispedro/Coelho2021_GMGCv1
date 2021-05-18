import pandas as pd
from jug import TaskGenerator
from glob import glob

@TaskGenerator
def load_cog525():
    cog525 = set()
    for line in open("cold/single.copy.MGs/all-matches/COG0525.IDs.txt"):
        cog525.add(int(line.split('.')[1], 10))
    return cog525

@TaskGenerator
def load1cog525(f, cog525):
    f = pd.read_feather(f, nthreads=8)
    f.set_index('index', inplace=True)
    f = f.loc[f.index.map(cog525.__contains__)]
    return f['scaled']

@TaskGenerator
def save_matrix(dense):
    dense = pd.DataFrame(dense)
    dense.to_csv('tables/cog525.txt', sep='\t')
    
    dense.reset_index(inplace=True)
    dense.to_feather('tables/cog525.feather')

files = glob('outputs/*.feather')
files.sort()
cog525 = load_cog525()
dense = {}
for i,f in enumerate(files):
    sample = f.split('/')[1].split('.')[0]
    dense[sample] = load1cog525(f, cog525)

save_matrix(dense)
