from matplotlib import pyplot as plt

import seaborn as sns
import pandas as pd
from colors import biome2color

biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)
csamples = pd.read_table('tables/cogs.counts.txt', index_col=0)

divs = pd.read_table('tables/diversity.tsv', index_col=0)
gf_divs = pd.read_table('tables/gf.richness.tsv', index_col=0)

pd.DataFrame(
        {
            'Taxonomic richness': csamples.iloc[:,1:].mean(1),
            'Unigenes richness': csamples['0'],
            'Protein Family richness (1 million reads)': gf_divs['gf_1m_rich'],
            'Unigene richness (1 million reads)': divs['gene_1m_rich'],
            'Habitat': biome,
        }).to_excel('preprocessed/SourceData_Richness.xlsx')


