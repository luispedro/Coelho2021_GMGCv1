from scipy import stats
import pandas as pd
motus2 = pd.read_table('cold/profiles/motus.v2.relabund.tsv', index_col=0)

Rmotus2 = (motus2 > 0).sum() - 1

divs = pd.read_table('../gmgc.analysis/tables/diversity.tsv', index_col=0)
biome = pd.read_table('cold/biome.txt', squeeze=True, index_col=0)

data= {}
for b in set(biome):
    if b in ['isolate', 'amplicon']: continue
    sel = (biome ==b) & (divs.cog525_1m_rich > 0)
    sel = divs.loc[sel]
    data[b] = pd.Series(stats.spearmanr(sel.cog525_1m_rich, Rmotus2.reindex(sel.index)))
print("Spearman r/p-value between richness with mOTU2 and COG 525")
print(pd.DataFrame(data).rename(index={0:'r',1:'pval'}).T.sort_values(by='r'))
