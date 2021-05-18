from glob import glob
import pandas as pd
biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)
computed = pd.read_table('cold/sample.computed.tsv', index_col=0)
fnas = glob('cold/subcatalogs/*.fna')

counts = {}
for f in fnas:
    if 'complete' in f: continue
    counts[f] = sum(1 for line in open(f) if line[0] =='>')
    print(f'Done {f}')
total = computed['nr_orfs'].groupby(biome).sum()
c95nr = {b:counts.get('cold/subcatalogs/GMGC.{}.95nr.fna'.format(b.replace(' ', '-')), 0) for b in total.index}

fr12_95nr = sum(1 for line in open('cold/freeze12.non_ENV.95nr.fna') if line[0] == '>')
fr12_100nr = sum(1 for line in open('cold/freeze12.non_ENV.100nr.faa') if line[0] == '>')
total['ProGenomes2']= 312020842
c95nr['ProGenomes2'] = fr12_95nr


rare_orfs = set(line.strip() for line in open('tables/rare-orfs.txt') if line.startswith('Fr12_'))
fr12_nr_no_rare = sum(1 for line in open('cold/freeze12.non_ENV.100nr.faa')  if line[0] == '>' and line[1:-1] not in rare_orfs)
fr12_95nr_no_rare = sum(1 for line in open('cold/freeze12.non_ENV.95nr.faa')  if line[0] == '>' and line[1:-1] not in rare_orfs)

c95nr_e_rare = {b:counts.get('cold/subcatalogs/GMGC.{}.95nr.no-rare.fna'.format(b.replace(' ', '-')), 0) for b in total.index}
c95nr_e_rare['ProGenomes2'] = fr12_95nr_no_rare


everything = pd.DataFrame({'total': total , 'nr95': c95nr, 'nr95_no_rare': c95nr_e_rare}).sum()
everything['nr95'] = sum(1 for line in open('cold/GMGC.95nr.no-human.no-rare.txt'))
everything['nr95_no_rare'] = sum(1 for line in open('cold/GMGC.95nr.no-human.no-rare.txt'))
everything['nr95_no_rare'] //=2


everything['nr95'] = 302_655_267
everything['nr100'] = 966108540

nr100 = pd.Series({'human gut': 320191349,
         'human nose': 1272765,
         'soil': 100016289,
         'human oral': 110295637,
         'human vagina': 1171023,
         'built-environment': 21622539,
         'human skin': 19956624,
         'pig gut': 73932728,
         'dog gut': 13124375,
         'marine': 205749068,
         'amplicon': 33118,
         'cat gut': 7227596,
         'mouse gut': 8095208,
         'freshwater': 3380188,
         'wastewater': 1546369,
         'isolate': 14822})

nr100['ProGenomes2'] = fr12_100nr
pd.concat([pd.DataFrame({'total': total , 'nr95': c95nr, 'nr95_no_rare': c95nr_e_rare, 'nr100': nr100}), pd.DataFrame({'everything':everything}).T], axis=0).drop(['amplicon', 'isolate'])[['total', 'nr100', 'nr95', 'nr95_no_rare']].to_csv('tables/gene.counts.tsv', sep='\t')

nr100i = pd.Series({'human gut': 345387931,
         'human nose': 5555989,
         'soil': 100801608,
         'human oral': 119673080,
         'human vagina': 3481435,
         'built-environment': 24904181,
         'human skin': 32900373,
         'pig gut': 91914581,
         'isolate': 161064,
         'dog gut': 26113679,
         'marine': 205847152,
         'amplicon': 38008,
         'cat gut': 31242937,
         'mouse gut': 13707012,
         'freshwater': 3897485,
         'wastewater': 5887749})

nr100i['ProGenomes2'] = fr12_100nr
pd.concat([pd.DataFrame({'total': total , 'nr95': c95nr, 'nr95_no_rare': c95nr_e_rare, 'nr100': nr100i}), pd.DataFrame({'everything':everything}).T], axis=0).drop(['amplicon', 'isolate'])[['total', 'nr100', 'nr95', 'nr95_no_rare']].to_csv('tables/gene.counts.inclusive.tsv', sep='\t')
