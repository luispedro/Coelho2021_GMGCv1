from glob import glob
import pandas as pd

statfiles = glob('stats/*.stats')
statfiles.sort()

def read_stats(fname):
    st = pd.read_table(fname, index_col=0)
    st.index = pd.MultiIndex.from_tuples(st.index.map(lambda ix: tuple(ix.split(':'))))
    filename = st.loc[(slice(None), 'file'), :].stats
    preproced = st[st.index.get_level_values(0).isin(filename.index[filename.map(lambda f: f.startswith('preproc.lno11.pairs.1') or f.startswith('preproc.lno11.singles'))].get_level_values(0))]
    numSeqs = preproced.loc[(slice(None), 'numSeqs'),:]
    ni = numSeqs.stats.astype(int).sum()

    preproced = st[st.index.get_level_values(0).isin(filename.index[filename.map(lambda f: f.startswith('preproc.lno11'))].get_level_values(0))]
    [nbp] = preproced.loc[(slice(None), 'numBasepairs'), :].astype(int).sum()
    return pd.Series({'inserts': ni, 'basepairs': nbp})

def get_sample(f):
    return f.split('/')[1].split('_')[0]
qc = pd.DataFrame({get_sample(f): read_stats(f) for f in statfiles})
meta = pd.read_table('cold/sample.computed.tsv', index_col=0)
meta['basepairsHQ'] = qc.loc['basepairs']
meta['insertsHQ'] = qc.loc['inserts']
meta.to_csv('remeta.txt', sep='\t')
