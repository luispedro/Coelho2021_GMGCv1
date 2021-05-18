import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

from glob import glob
import seaborn as sns
from matplotlib import style
style.use('default')

POS_COLOUR = '#d95f02'
NEG_COLOUR = '#a6cee3'
NR_SAMPLES = 1000
COMPLETE_ONLY = True
INCLUDE_30 = False

def start_p(c):
    if c[0] == '>': return 1+int(c[1:])
    if '_' in c: return int(c.split('_')[0])
    return int(c.split('-')[0])

def category(c):
    if c <= 10: return '0-10'
    if c <= 100: return '11-100'
    if c <= 200: return '101-200'
    return '>200'

category_order = list(sorted(set([category(i) for i in range(1002)]), key=start_p))
ecoli_pos = pd.read_table('gmgc_selection/ecoli.absrel.pval.detections.with_header.tsv', index_col=1)


ecoli_pos['detections_cat'] = ecoli_pos.all_samples_NoIsolateNoAmplicon.map(category)
ecoli_pos['is_selected'] = ecoli_pos.eval('absrel_p < .05')

ecoli = pd.read_table('./tables/Ecoli.selection_per_site.detection_number.txt',
        sep=' ',
        index_col=0)

ecoli.detections_cat = ecoli.all_samples_NoIsolateNoAmplicon.map(category)
def balance_cl_size(to_sel):
    sel = []
    cs = to_sel. \
            groupby(['cl_size', 'detections_cat']) \
            .count()
    cs = cs[cs.columns[0]]
    categories = (set(to_sel.detections_cat))
    for i in range(to_sel['cl_size'].max()):
        try:
            n = cs.loc[i].min()
            if len(cs.loc[i]) != len(categories):
                continue
        except:
            continue
        for cat in categories:
            sel.extend(np.random.choice(
              to_sel.query('detections_cat == @cat and cl_size == @i').index ,n ))
    return sel
ecoli_sel = balance_cl_size(ecoli.query('msa_len >= 109 & msa_len <= 361'))
ecoli = ecoli.loc[ecoli_sel]

def do_fig(data, order, ax):
    for i,c in enumerate(order):
        sel = data.query('detections_cat == @c')
        for s,vs,c in [(-.25, sel.ratio_pos, POS_COLOUR), (0, sel.ratio_neg, NEG_COLOUR)]:
            vs = vs.values
            vs = np.random.choice(vs, NR_SAMPLES)
            vs *= 100

            x = i+s
            ax.scatter([x + 0.025 + np.random.random()/5 for _ in vs], vs, c=c, alpha=.35, s=1, zorder=-10)
    melt = pd.melt(data,
            id_vars=['detections_cat'],
            value_vars=['ratio_pos', 'ratio_neg'])
    melt['value'] *= 100
    sns.boxplot(
            x='detections_cat',
            data=melt,
            hue='variable',
            y='value',
            order=order,
            showfliers=False,
            width=.5,
            boxprops={'fill':0, 'color':'black'},
            saturation=1,
            linewidth=1,
            ax=ax,
            )
    has_sel = data.eval('(pos + neg)> 0').astype(float).groupby(data['detections_cat']).describe()
    has_sel = has_sel.loc[order]
    print(has_sel)
    y=100*has_sel['mean']
    yerr=196*has_sel['std']/np.sqrt(has_sel['count'])
    print(pd.DataFrame({
        'm - err': y - yerr,
        'm ': y,
        'm + err': y + yerr,}))
    ax.errorbar(x=np.arange(len(order)),
            y=100*has_sel['mean'],
            yerr=196*has_sel['std']/np.sqrt(has_sel['count']),
            fmt='-',
            capsize=2,
            c='#666666')
    ax.set_ylabel('Fraction (%)')

fscore = pd.read_table('tables/fscore.prevalence.update.tsv.gz', index_col=0)
fscore['detections_cat'] = fscore.prev.map(category)
fscore['c'] *= 100

unig = pd.read_table('tables/metasel.tsv', index_col=0)
unig.is_complete.fillna(False, inplace=True)
if COMPLETE_ONLY:
    unig = unig.query('is_complete')
unig['detections_cat'] = unig['detections'].map(category)
unig['is_selected'] = unig.eval('pval < .05')

fig,axes = plt.subplots(3,1, tight_layout=True, figsize=[3.2, 5], sharex=True)
fscore_ax, unig_ax, ecoli_ax = axes

sns.boxplot(x='detections_cat', y='c', ax=fscore_ax, data=fscore, order=category_order, boxprops={'fill':0, 'color':'black'}, width=.25, fliersize=1, linewidth=1)
fscore_ax.set_ylabel('K score')# Neighbour\nKEGG\nconcordance (%)')

if INCLUDE_30:
    unig30 = pd.read_table('tables/selection_complete30_w_prev.tsv', index_col=0)
    unig30.rename(columns={'1':'pval'}, inplace=True)
    unig30.eval('is_selected = pval < .05', inplace=True)
    unig30['detections_cat'] = unig30['prevalence'].map(category)

    unig_ax.plot(100*
            unig30.groupby('detections_cat').mean().loc[category_order]['is_selected'],
            label='Complete Only (ID 30%)',
            lw=2,
            )

unig_ax.plot(100*
        unig.groupby('detections_cat').mean().loc[category_order]['is_selected'],
        label='All ORFs',
        lw=2,
        )
unig_ax.set_ylabel('Pos. selection\n(% of genes)')
unig_ax.plot(100*
        ecoli_pos.groupby('detections_cat').mean().loc[category_order]['is_selected'],
        '-',
        label='E. coli',
        lw=2,
        c='r',
        )


do_fig(ecoli, category_order, ax=ecoli_ax)

ecoli_ax.legend().remove()
fscore_ax.set_xlabel(None)
#fscore_ax.set_xticks([])
unig_ax.set_xlabel(None)
#unig_ax.set_xticks([])
#axes[2].text(0,50, 'E. coli')

ecoli_ax.set_xlabel('Number of detections')
sns.despine(fig,trim=True)
fig.tight_layout()

fig.savefig('plots/Figure-selection.4.png', dpi=600)
fig.savefig('plots/Figure-selection.4.pdf')
fig.savefig('plots/Figure-selection.4.svg')
