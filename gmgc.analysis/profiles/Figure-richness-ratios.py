from matplotlib import pyplot as plt

import seaborn as sns
import pandas as pd
from colors import biome2color

biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)
csamples = pd.read_table('tables/cogs.counts.txt', index_col=0)

divs = pd.read_table('tables/diversity.tsv', index_col=0)
gf_divs = pd.read_table('tables/gf.richness.tsv', index_col=0)

def scatter_plot(X, Y, xlabel, ylabel, oname):
    from sklearn import linear_model
    lr = linear_model.LinearRegression()
    fig, ax = plt.subplots()
    plots = []
    i = 0
    for b in set(biome.values):
        x = X[biome == b]
        x = x[x > 40]
        y = Y[x.index]
        if len(x) <= 40:
            continue
        ax.scatter(x, y , s=0.1, c=biome2color[b])
        lr.fit(x.values.reshape((-1, 1)), y.ravel())
        xmin = x.min()
        xmax = x.max()
        plots.append((
            ax.plot([xmin, xmax], lr.predict([[xmin], [xmax]]),
            label=b, c=biome2color[b], zorder=1), b))
        i += 1
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend([a for [a], _ in plots], [n for _, n in plots], loc='best')
    sns.despine(fig, offset=True)
    fig.tight_layout()
    fig.savefig(oname, dpi=300)




def ratio_plot(X, Y, label, min_x=100, min_n=50, y_order=None):
    data = organize_data(X, Y, min_x=min_x, min_n=min_n)
    if y_order is None:
        y_order = data.groupby('habitat')['value'].mean().squeeze().sort_values().index
    fig, axes = plt.subplots(len(y_order), 1, sharex=True, figsize=[4,4])
    for i, b in enumerate(y_order):
        ax = axes[i]
        r = data.query('habitat == @b')['value']
        ax.patch.set_visible(False)
        sns.kdeplot(r, label=b, ax=ax, shade=True, bw='silverman', kernel='gau', color=biome2color[b])
        ax.set_yticks([])
        ax.set_xlabel(None)
        sns.despine(ax=ax, bottom=(i != len(y_order)-1), left=True, trim=(i == len(y_order)-1))
        ax.legend(loc='best')
    for ax in axes[:-1]:
        for t in list(ax.get_xticklines()):
            t.set_visible(False)
    fig.tight_layout(h_pad=-2)
    return fig, y_order


def remove_outliers(xs):
    xsv = xs.values.copy()
    xsv.sort()
    n = len(xsv)//20
    xmin = xsv[n]
    xmax = xsv[-n]
    return xs[(xs >= xmin) & (xs <= xmax)]

def organize_data(X, Y, min_x, min_n):
    data = []
    for b in set(biome.values):
        if b in {'amplicon', 'isolate'}: continue
        x = X[biome == b]
        x = x[x >= min_x]
        if len(x) < min_n:
           continue
        y = Y[x.index]
        vs = remove_outliers(y/x)
        data.append(pd.DataFrame({'value': vs, 'x': x.reindex(vs.index), 'y': y.reindex(vs.index), 'habitat': b}))

    return pd.concat(data)

#scatter_plot(csamples.iloc[:,1:].mean(1), csamples['0'], 'Nr scOTUs', 'Nr genes', 'plots/scatter-nGenes-per-scOTU')
#scatter_plot(csamples.iloc[:,1:].mean(1), n_families, 'Nr scOTUs', 'Nr bactNOG', 'plots/scatter-nBactNOG-per-scOTU')

plt.rcParams['font.size'] = 8
fig1, y_order = ratio_plot(csamples.iloc[:,1:].mean(1), csamples['0'], 'Nr genes per scOTU',)
fig1.savefig('plots/Figure3a.svg')
fig2,_ = ratio_plot(gf_divs['gf_1m_rich'], divs['gene_1m_rich'], 'Nr GF per unigene', y_order=y_order)
fig2.savefig('plots/Figure3b.svg')


