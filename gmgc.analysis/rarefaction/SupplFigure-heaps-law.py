from sklearn import linear_model
from matplotlib import pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

TICKS_95 = [0, 50, 100, 150, 200, 250, 300]
TICKS_90 = [0, 25, 50, 75, 100, 125, 150, 175]
TICKS_GF  = [0,  10, 20, 30]


def load_rarefaction(g):
    if g == '95':
        s = np.load('results/all.npy')
        s = np.concatenate([np.zeros((24,1,15)), s], axis=1)
        return s[:,:,0]
    if g == '90':
        return pd.read_table('outputs/p90.all.tsv', header=None)
    if g == 'gf':
        return pd.read_table('outputs/gf.all.tsv', header=None)

fig,axes  = plt.subplots(1,3, figsize=[7.4, 2.6])
for gtype,ticks,gname,ax in [
        ('95', TICKS_95, 'unigenes', axes[0]),
        ('90', TICKS_90, 'proteins', axes[1]),
        ('gf', TICKS_GF, 'gene families', axes[2]),
        ]:
    rar = load_rarefaction(gtype)
    lN = np.log(rar.mean(0)[1:])
    lS = np.log(np.arange(rar.shape[1])[1:])

    lr = linear_model.LinearRegression()
    lr.fit(np.atleast_2d(lS).T, lN)

    ax.plot(rar.T, c='#cccccc')
    ax.plot(rar.mean(0),  c='k')
    ax.plot(np.exp(lS*lr.coef_+lr.intercept_), ':', c='r')
    ax.set_yticks([t * 1e6 for t in ticks])
    ax.set_yticklabels([str(t) for t in ticks])

    ax.set_xlabel('Number of samples')
    ax.set_ylabel('Number of {}\n(millions)'.format(gname))
    print('Coeficient: {:.3} (in original space: {:.3})'.format(*lr.coef_, *np.exp(lr.coef_)))
sns.despine(fig, trim=True)
fig.tight_layout()
fig.savefig('plots/fit-heaps.3.svg')
fig.savefig('plots/fit-heaps.3.png', dpi=300)
