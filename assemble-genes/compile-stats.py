import pandas as pd

def extract_gc(cur):

    gc1 = cur.T[cur.loc['file'] == 'preproc.lno8.pairs.1'].gcContent
    gc2 = cur.T[cur.loc['file'] == 'preproc.lno8.pairs.2'].gcContent
    gcs = cur.T[cur.loc['file'] == 'preproc.lno8.singles'].gcContent
    n1 = cur.T[cur.loc['file'] == 'preproc.lno8.pairs.1'].numSeqs
    n2 = cur.T[cur.loc['file'] == 'preproc.lno8.pairs.2'].numSeqs
    ns = cur.T[cur.loc['file'] == 'preproc.lno8.singles'].numSeqs
    n1 = int(n1)
    n2 = int(n2)
    ns = int(ns)
    gc1 = float(gc1)
    gc2 = float(gc2)
    gcs = float(gcs)
    if n1 == 0 and n2 == 0:
        return gcs
    return (gc1*n1+gc2*n2+gcs*ns)/(n1+n2+ns)

biome = pd.read_table('/g/scb2/bork/coelho/DD_DeCaF/genecats.cold/biome.txt', index_col=0)
stats =  pd.read_table('./input-stats.txt', index_col=0).T
inserts = {}
initseqs = {}
bps = {}
initbps = {}
gc = {}
for k, v in stats.iterrows():
    cur  = pd.DataFrame({str(k):v.select(lambda ix : ix.split(':')[0] == str(k)).rename(lambda ix: ix.split(':')[1]) for k in range(73)})
    cur_inserts = int(cur.T[cur.loc['file'] == 'preproc.lno8.pairs.1'].numSeqs)
    cur_inserts += int(cur.T[cur.loc['file'] == 'preproc.lno8.singles'].numSeqs)
    after_bps = cur.T[cur.loc['file'].isin(['preproc.lno8.pairs.1', 'preproc.lno8.pairs.2', 'preproc.lno8.singles'])].numBasepairs.astype(int).sum()
    total_bps = cur.T['numBasepairs'].astype(int).sum()

    bps[k] = after_bps
    initbps[k] = total_bps - after_bps

    inserts[k] = cur_inserts
    cur_total = 0
    for i in range(73):
      f = v['{}:file'.format(i)]
      if f.startswith('preproc.lno'):
          break
      if 'single' in f:
         cur_total += int(v['{}:numSeqs'.format(i)])
      elif '1.fastq' in f or '.1.fq.' in f or '_1.fastq' in f:
         cur_total += int(v['{}:numSeqs'.format(i)])
      elif not( '2.fastq' in f or '.2.fq' in f ):
         cur_total += int(v['{}:numSeqs'.format(i)])
      initseqs[k] = cur_total
      gc[k] = extract_gc(cur)

output = pd.DataFrame({
        'insertsRaw': initseqs,
        'insertsHQ': inserts,
        'basepairsRaw': initbps,
        'basepairsHQ': bps,
        'biome': biome['biome'],
        'GCfrac': gc
        })
output.to_csv('outputs/sample.computed.tsv', sep='\t')

