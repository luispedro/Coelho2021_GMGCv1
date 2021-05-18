import pandas as pd
computed = pd.read_table('cold/sample.computed.tsv', index_col=0)
computed.to_excel('tables/SupplTable-SampleProperties.xlsx')
