#import matplotlib
#matplotlib.use('Agg')

from matplotlib import style
style.use('default')

from matplotlib import pyplot as plt
from colors import biome2color
import numpy as np
import seaborn as sns


fig, ax = plt.subplots()
fig15,axes15  = plt.subplots(3,5, figsize=[8,5])

i = 0
for b in biome2color:
    if b in {'mammal gut', 'other human'}:continue
    s = np.load('results/{}.npy'.format(b.replace(' ', '-')))
    s = np.concatenate([np.zeros((24,1,15)), s], axis=1)

    ax.plot(s[:,:,0].mean(0), c=biome2color[b])

    cfig, cax = plt.subplots()
    cax.plot(s[:,:,0].mean(0), label='All genes')
    cax.plot(s[:,:,-1].mean(0), label='No singletons')
    cax.plot(s[:,:,1].mean(0), label=r'≥ 1% of samples')
    cax.plot(s[:,:,2].mean(0), label=r'≥ 5% of samples')
    cax.legend(loc='best')
    cax.set_xlabel("Number of samples")
    cax.set_ylabel("Number of genes (millions)")
    sns.despine(cfig, offset=4)
    cfig.tight_layout()
    cfig.savefig(f'plots/rarefy-{b}-split.png', dpi=300)
    cfig.savefig(f'plots/rarefy-{b}-split.svg')
    plt.close(cfig)


    s = s / 1_000_000.
    cax = axes15.flat[i]
    i += 1
    cax.set_title(b)
    cax.plot(s[:,:,0].mean(0), label='All genes')
    cax.plot(s[:,:,-1].mean(0), label='No singletons')
    cax.plot(s[:,:,1].mean(0), label=r'≥ 1% of samples')
    cax.plot(s[:,:,2].mean(0), label=r'≥ 5% of samples')


ax.set_yticks([0,20e6,40e6,60e6,80e6,100e6])
ax.set_yticklabels([0,20,40,60,80,100])
ax.set_xlabel("Number of samples")
ax.set_ylabel("Number of genes (millions)")
sns.despine(fig, offset=4)
fig.tight_layout()
fig.savefig(f'plots/rarefy-all-colored.png', dpi=300)
fig.savefig(f'plots/rarefy-all-colored.svg')

ax.set_xlim(0, 200)
sns.despine(fig, offset=4)
fig.tight_layout()
fig.savefig(f'plots/rarefy-all-colored-inset.png', dpi=300)
fig.savefig(f'plots/rarefy-all-colored-inset.svg')


ax.set_xlim(0,150)
fig.tight_layout()
fig.savefig(f'plots/rarefy-all-colored-to150.png', dpi=300)
fig.savefig(f'plots/rarefy-all-colored-to150.svg')

b = 'all'
s = np.load('results/all.npy')
s = np.concatenate([np.zeros((24,1,15)), s], axis=1)
s = s / 1_000_000.
cax = axes15.flat[i]
cax.set_title(b)
cax.plot(s[:,:,0].mean(0), label='All genes')
cax.plot(s[:,:,-1].mean(0), label='No singletons')
cax.plot(s[:,:,1].mean(0), label=r'≥ 1% of samples')
cax.plot(s[:,:,2].mean(0), label=r'≥ 5% of samples')
cax.legend(loc='best')


sns.despine(fig15, offset=4)
fig15.tight_layout()
fig15.savefig(f'plots/rarefy-all-15.png', dpi=300)
fig15.savefig(f'plots/rarefy-all-15.svg')



