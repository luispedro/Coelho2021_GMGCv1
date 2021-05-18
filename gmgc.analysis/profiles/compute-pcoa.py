from jug import TaskGenerator
import matplotlib
matplotlib.use('Agg')

import seaborn as sns
from scipy import stats

import pandas as pd
import numpy as np
from scipy import stats
from colors import biome2color
print("Imported")

up = {
   'cat gut': 'mammal gut',
   'dog gut': 'mammal gut',
   'human blood plasma': 'other human',
   'human gut': 'mammal gut',
   'human nose': 'other human',
   'human oral': 'other human',
   'human skin': 'other human',
   'human vagina': 'other human',
   'mouse gut': 'mammal gut',
   'pig gut': 'mammal gut',
}


biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)
upome = biome.map(lambda b: up.get(b,b))
computed = pd.read_table('cold/sample.computed.tsv', index_col=0)
total = computed.insertsHQ
divs = pd.read_table('tables/diversity.tsv', index_col=0)
divs['ko_1m_shannon'] = np.exp(divs['ko_1m_shannon'])
#divs['bactNOG_1m_shannon'] = np.exp(divs['bactNOG_1m_shannon'])

print("Loaded basic")
valid = (total > 1e6) & ~biome.map({'isolate', 'amplicon'}.__contains__)
valid &= divs.reindex(valid.index[valid]).gene_1m_rich > 0
valid = valid.index[valid]
total = total.reindex(valid)
biome = biome.reindex(valid)
upome = upome.reindex(valid)
divs = divs.reindex(valid)

def do_decomposition(values, metric):
    if metric == 'l2':
        from sklearn import decomposition
        pnog = decomposition.PCA(2)
        v2NOG = pnog.fit_transform(values)
        return pnog, v2NOG
    elif metric == 'l1':
        import matplotlib
        matplotlib.use('Agg')
        from skbio.stats import ordination
        from scipy.spatial import distance
        dist = distance.pdist(values, 'cityblock')
        r = ordination.pcoa(dist)
        return r.proportion_explained, r.samples.iloc[:,:3]

@TaskGenerator
def pcoa_bnog(do_log, metric='l2'):
    bactNOG = pd.read_feather('tables/bactNOGS.feather', nthreads=24)
    print("Loaded bactNOG")
    bactNOG.set_index('index', inplace=True)
    bactNOG = bactNOG.T
    bactNOG.drop('-uncharacterized-', axis=1, inplace=True)
    print("Cleaned bactNOG")

    bactNOG = bactNOG.reindex(valid)

    bactNOG = (bactNOG.T / total).T
    meanNOG = bactNOG.mean()

    selected = bactNOG.T[meanNOG > 1e-4].T
    if do_log:
        normed10NOG = np.log10(1e-6 + selected)
    else:
        normed10NOG = selected
    return do_decomposition(normed10NOG.values, metric)

@TaskGenerator
def pcoa_ko(do_log, metric='l2'):
    ko = pd.read_feather('tables/kos.feather', nthreads=24)
    print("Loaded KO")
    ko.set_index('index', inplace=True)
    ko = ko.T
    ko.drop('-uncharacterized-', axis=1, inplace=True)
    print("Cleaned KO")

    ko = ko.reindex(valid)
    ko = (ko.T /total).T


    selko = ko.T[ko.mean() > 1e-4].T
    if do_log:
        normed10ko = np.log10(1e-6 + selko)
    else:
        normed10ko = selko
    return do_decomposition(normed10ko.values, metric)


@TaskGenerator
def corrs(e_r, ix):
    e,r = e_r
    if hasattr(r, 'values'):
        r = r.values
    return pd.Series({c:stats.spearmanr(r.T[ix], divs[c])[0] for c in divs.columns})

results = {}
for log in [True, False]:
    for metric in ['l1', 'l2']:
        results["{}_{}_{}".format('ko', log, metric)] = pcoa_ko(log, metric=metric)
        results["{}_{}_{}".format('nog', log, metric)] = pcoa_bnog(log, metric=metric)



@TaskGenerator
def plot(tag, e_r, c1, c2):
    from matplotlib import pyplot as plt
    e,r = e_r
    r = getattr(r, 'values', r)
    e = getattr(e, 'values', e)
    e = getattr(e, 'explained_variance_ratio_', e)

    c1 = c1.abs().idxmax()
    c2 = c2.abs().idxmax()

    fig,axes = plt.subplots(2, 2, sharex='col', sharey='row')
    for b in set(upome.values):
        sel = r[upome.values == b]
        axes[0,0].scatter(sel.T[0], sel.T[1], s=1, label=b, c=biome2color[b])

    axes[0,0].legend(loc='best')

    axes[1,0].scatter(r.T[0], divs[c1], s=1, alpha=.25, c='k')
    axes[1,0].set_ylabel(c1)

    axes[0,1].scatter(divs[c2], r.T[1], s=1, alpha=.25, c='k')
    axes[0,1].set_xlabel(c2)


    from sklearn import linear_model
    lreg = linear_model.LinearRegression()

    x , y = r.T[0], divs[c1]
    lreg.fit(np.atleast_2d(x).T, y)
    X = np.array([[x.min()], [x.max()]])
    axes[1,0].plot(X, lreg.predict(X), 'r-')

    x , y = divs[c2], r.T[1]
    lreg.fit(np.atleast_2d(x).T, y)
    X = np.array([[x.min()], [x.max()]])
    axes[0, 1].plot(X, lreg.predict(X), 'r-')

    axes[1, 0].set_xlabel('PC1 [{:.1%} explained var.]'.format(e[0]))
    axes[0, 0].set_ylabel('PC2 [{:.1%} explained var.]'.format(e[1]))

    sns.despine(fig, offset=4, trim=True)
    fig.tight_layout()
    fig.savefig(f'plots/pcoas/{tag}.svg')
    fig.savefig(f'plots/pcoas/{tag}.png')

corrs1 = {}
corrs2 = {}
for tag in results:
    corrs1[tag] = corrs(results[tag], 0)
    corrs2[tag] = corrs(results[tag], 1)
    plot(tag, results[tag], corrs1[tag], corrs2[tag])
