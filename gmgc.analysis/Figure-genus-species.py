from glob import glob
from jug import TaskGenerator, CachedFunction
from jug import mapreduce

GENUS_BASE = '/g/scb2/bork/maistren/BLASTN_rawout_forGenecatalog/gene_cat_WITHIN_GENUS'
GENUS_BASE2 = '/g/scb2/bork/maistren/BLASTN_rawout_forGenecatalog/gene_cat_WITHIN_GENUS_beta'
SPECIES_BASE = '/g/scb2/bork/maistren/BLASTN_rawout_forGenecatalog/gene_cat_WITHIN_SPECIES'


@TaskGenerator
def extract(f):
    import pandas as pd
    data = pd.read_table(f, header=None)
    data = data[data[4]>= 100]
    return data.groupby(by=0).max()[2].values

@TaskGenerator
def random_sample(species_perc, genus_perc):
    N = 1_000_000
    import numpy as np
    r = np.random.RandomState(123)

    sp = np.concatenate(species_perc)
    g = np.concatenate(genus_perc)
    r.shuffle(g)
    r.shuffle(sp)
    gs = g[:N]
    ss = sp[:N]
    return np.vstack([ss, gs])


@TaskGenerator
def create_plot(data):
    import matplotlib
    matplotlib.use('Agg')

    import numpy as np
    from matplotlib import pyplot as plt
    import seaborn as sns

    sp,g = data

    fig,ax = plt.subplots()
    sns.distplot(sp, ax=ax, label='Within species')
    sns.distplot(g, ax=ax, label='Within genus')
    ax.set_xlabel('Identity (%)')
    ax.set_ylabel('Density')
    ax.plot([95,95],[0,1], c='r')
    sns.despine(fig, trim=True, offset=4)
    ax.legend(loc='best')
    fig.tight_layout()
    fig.savefig("plots/species-genus.svg")


within_genus = CachedFunction(glob, GENUS_BASE + '/*.blast_out_tab') + \
            CachedFunction(glob, GENUS_BASE2 + '/*.blast_out_tab')
within_species = CachedFunction(glob, SPECIES_BASE + '/*.blast_out_tab')

within_genus.sort()
within_species.sort()

species_perc = mapreduce.map(extract, within_species, 96)
genus_perc = mapreduce.map(extract, within_genus, 96)

data = random_sample(species_perc, genus_perc)
create_plot(data)
