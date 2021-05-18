import seaborn as sns
from matplotlib import pyplot as plt
import numpy as np
from matplotlib import cm

import numpy as np
import pandas as pd
from collections import Counter
ranked = Counter({'phylum': 23157473,
         None: 80134736,
         'species': 44276292,
         'family': 21970125,
         'class': 19678161,
         'genus': 85520512,
         'order': 15463648,
         'superkingdom': 20996496})

RANKS = ['superkingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
RANKS = RANKS[::-1]

fig,ax = plt.subplots()

ax.grid(False)
left = 0
for i,r in enumerate(RANKS):
    n = ranked[r]
    ax.barh(i+1, n, left=left, color='#3366cc')
    left += n
    
    
    
ax.set_yticks(np.arange(len(RANKS))+1)

ax.set_yticklabels(RANKS)
ax.set_xlabel('Number of genes (millions)')
ax.set_xticklabels(['0', '50', '100', '150', '200', '250'])
ax.set_xticks([0, 50e6, 100e6, 150e6, 200e6, 250e6])
fig.tight_layout()
sns.despine(fig, trim=True, offset=4)
fig.tight_layout()
fig.savefig('SupplFigure-taxonomic-ranks.svg')
