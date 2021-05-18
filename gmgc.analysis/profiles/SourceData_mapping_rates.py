import pandas as pd
biome = pd.read_table('cold/biome.txt', index_col=0, squeeze=True)

mapstats_fr12 = pd.read_table('cold/profiles/freeze12only.mapstats.txt', index_col=0, comment='#').T
computed = pd.read_table('cold/sample.computed.tsv', index_col=0)
input_fr12 = pd.read_table('../../profiles-all/freeze12only.input-stats.parsed.txt', index_col=0)
mapstats_fr12['total'] = input_fr12['inserts']

fractions_fr12 = mapstats_fr12['aligned']/mapstats_fr12['total']


total = pd.read_table('tables/total.tsv', index_col=0)
fractions_gmgc = total['total']/computed.insertsHQ

mapped = pd.DataFrame({'fr12' : fractions_fr12, 'gmgc': fractions_gmgc, 'biome' : biome})
mapped = mapped[mapped.biome != 'isolate']
mapped.to_excel('preprocessed/SourceData_mapping_rates.xlsx')
