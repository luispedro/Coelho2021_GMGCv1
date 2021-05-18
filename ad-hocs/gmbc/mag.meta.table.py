import pandas as pd
qc = pd.read_table('/g/scb2/bork/orakov/GMGC/GMGC.check.completion.contamination.only.tsv', index_col=0)
bin_stats = pd.read_table('/g/scb2/bork/orakov/GMGC/Projects/GMGC.bin.stats',  header=None, index_col=0, names=['nr_contigs', 'total_bp_size', 'N50', 'min_contig_size', 'max_contig_size'])

bin_stats.index = bin_stats.index.map(lambda ix: ix.split('/')[1].replace('.fa.gz', ''))

assert set(bin_stats.index)  == set(qc.index)
qc['quality'] = qc.eval(' - 5 * contamination + completeness')
qc = qc.reset_index()
qc.sort_values(by=['quality','genome',], ascending=False, inplace=True)
qc = qc.reset_index().drop('index', axis=1)
def f6(x):
    x = '{:06}'.format(x)
    return x[:3] + '_' + x[3:]

qc.index = qc.index.map(lambda ix: 'GMBC10.{}'.format(f6(ix)))
qc = qc.join(bin_stats, on='genome')

qc['category'] = 'low-quality'
qc['category'].loc[qc.eval('completeness > 50 & contamination < 10')] = 'medium-quality'
qc['category'].loc[qc.eval('completeness > 90 & contamination < 5')] = 'high-quality'

qc.to_csv('cold/GMBC10.meta.tsv', sep='\t', index_label='binID')
