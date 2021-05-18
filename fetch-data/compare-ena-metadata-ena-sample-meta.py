'''This script compares the projects found using the method in ena-metadata.py
with those from ena-sample-meta.py'''

import xml.etree.ElementTree as ET
from six import StringIO
import requests
import pandas as pd
from pdutils import pdselect

selected = pd.read_table('./data/sample-meta-selected.tsv', index_col=0)
c = selected.groupby('study_accession').count()
c = c.max(1)
c.sort_values(inplace=1)
selected_projects = set(c[c > 100].index)

sample_meta = pd.read_table('./data/sample-meta.tsv', index_col=0)

MIN_NR_READS = 1000*1000
MIN_AVG_READLEN = 75
sample_meta['avg_readlen']  = sample_meta['ENA-BASE-COUNT']/sample_meta['ENA-SPOT-COUNT']
sample_meta['spot_count'] = sample_meta['ENA-SPOT-COUNT']
sample_meta = pdselect(sample_meta, avg_readlen__ge=MIN_AVG_READLEN, spot_count__ge=MIN_NR_READS)
c = sample_meta.groupby('study_accession').count().max(1).copy()
c.sort_values(inplace=1)

for p in c[c > 100].index:
    if p not in selected_projects:
        print("Project found in sample metadata: {}".format(p))
        selected_projects.add(p)

for s in selected_projects:
    et = ET.parse(StringIO(requests.get(f'http://www.ebi.ac.uk/ena/data/view/{s}&display=xml').text))
    title = et.findtext("STUDY/DESCRIPTOR/STUDY_TITLE")
    print(f'{s}\t{title}')
