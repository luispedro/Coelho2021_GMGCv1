import pandas as pd
import numpy as np
from sklearn import model_selection
from sklearn import ensemble

print("""
Building/testing models...

Note that, as it depends on random initialization, this analysis will return
(slightly) different results each time it is run.

""")


biome = pd.read_table('../../gmgc.analysis/cold/biome.txt', squeeze=True, index_col=0)
crich = pd.read_table('tables/gene.richness.complete-orfs.tsv', index_col=0)
computed = pd.read_table('cold/sample.computed.tsv', index_col=0)
total = computed.insertsHQ
divs = pd.read_table('tables/diversity.tsv', index_col=0)
multi = pd.read_table('tables/richness-multi.1m.tsv', index_col=0)
f = pd.read_table('tables/faithPD.tsv', index_col=0, squeeze=True)
divs['faithPD'] = f.reindex(divs.index).fillna(f.mean())

riches = pd.read_table('tables/gf.richness.tsv', index_col=0)
divs['gf_1m_rich'] = riches['gf_1m_rich']

print("Loaded basic")
valid = (total > 1e6) & ~biome.map({'isolate', 'amplicon'}.__contains__) & (divs.bactNOG_1m_rich > 0)
valid = valid.index[valid]
biome = biome.reindex(valid)
divs = divs.reindex(valid)
c1m = [c for c in divs.columns if '_1m_' in c]

pred = ensemble.RandomForestClassifier(n_jobs=24, n_estimators=100)
X = divs[['gene_1m_rich', 'cog525_1m_rich', 'gf_1m_rich', 'ko_1m_rich']].values
y = biome.values
print('+ KO/ - Faith / ALL (4)')
print(np.mean(y == model_selection.cross_val_predict(pred, X, y, cv=model_selection.KFold(10, shuffle=True, random_state=12345))))

X = divs[['faithPD', 'gene_1m_rich', 'cog525_1m_rich', 'gf_1m_rich', 'ko_1m_rich']].values
y = biome.values
print('+ KO/ +  Faith / ALL (5)')
print(np.mean(y == model_selection.cross_val_predict(pred, X, y, cv=model_selection.KFold(10, shuffle=True, random_state=12345))))

X = divs[['faithPD', 'gene_1m_rich', 'cog525_1m_rich', 'gf_1m_rich']].values
y = biome.values
print('- KO/ +  Faith / ALL (5)')
print(np.mean(y == model_selection.cross_val_predict(pred, X, y, cv=model_selection.KFold(10, shuffle=True, random_state=12345))))


duplicates = set(line.strip() for line in open('cold/duplicates.txt'))
divs.drop([ix for ix in divs.index if ix in duplicates], inplace=True)
biome.drop([ix for ix in biome.index if ix in duplicates], inplace=True)

hs = list(sorted(set(y)))
samples = []
for h in hs:
    sel = biome.index[biome == h]
    if len(sel) > 200:
        sel = np.random.choice(sel, 200)
    samples.extend(sel)
    
print('- KO/ + Faith / <200-no-dups (4)')
X = divs.reindex(samples)[['faithPD', 'gene_1m_rich', 'cog525_1m_rich', 'gf_1m_rich']].values
y = biome.reindex(samples).values
print(np.mean(y == model_selection.cross_val_predict(pred, X, y, cv=model_selection.KFold(10, shuffle=True, random_state=12345))))

print('- KO/ - Faith / <200-no-dups (4)')
X = divs.reindex(samples)[['gene_1m_rich', 'cog525_1m_rich', 'gf_1m_rich']].values
y = biome.reindex(samples).values
print(np.mean(y == model_selection.cross_val_predict(pred, X, y, cv=model_selection.KFold(10, shuffle=True, random_state=12345))))

print("Alternative protein family definitions")
crich_sel = crich.reindex(samples)['complete_richness_1m']

Xbase = divs.reindex(samples)[['cog525_1m_rich', 'faithPD']].values
Xalt = multi.reindex(samples).dropna()
for c in Xalt.columns:
    X = np.hstack([Xbase, np.atleast_2d(crich_sel).T, np.atleast_2d(Xalt[c].values).T])
    print(c, np.mean(y == model_selection.cross_val_predict(pred, X, y, cv=model_selection.KFold(10, shuffle=True, random_state=12345))))

