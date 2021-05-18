import pandas as pd 
sizes = pd.read_table('tables/GMGC.95nr.sizes', index_col=0)

rename = {}
for line in open('/g/bork1/coelho/DD_DeCaF/genecats.cold/GMGC10.rename.table.txt'):
    new, old = line.split()
    rename[old] = new
    
new_names = sizes.index.map(rename.get)


sizes['new_names'] = new_names
complete = set(line.strip() for line in open('cold/derived/complete.txt'))

data = sizes

is_complete = data.new_names.map(complete.__contains__)

data['is_complete'] = is_complete

data.reset_index(inplace=True)




data.set_index('new_names', inplace=True)


data.rename(columns={'index': 'original_name'}, inplace=True)


