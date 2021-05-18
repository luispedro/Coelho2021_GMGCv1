from jug import TaskGenerator
import xml.etree.ElementTree as ET
from six import StringIO
import requests
import pandas as pd
from pdutils import pdselect
import pandas as pd

MIN_NR_SAMPLES = 20

selected = pd.read_table('./data/selected-cleaned-metadata.tsv', index_col=0)
c = selected.groupby('study_accession').count().max(1)
c.sort_values(inplace=True)
counts = c[c > MIN_NR_SAMPLES]

def summarize_col(col):
    col = col.dropna()
    if len(col):
        val = set(col.values)
        return ','.join(list(val))

selected_projects = set(c[c > MIN_NR_SAMPLES].index)
title = {}
for i,s in enumerate(selected_projects):
    print("Getting study info ({}/{})".format(i+1, len(selected_projects)))
    et = ET.parse(StringIO(requests.get(f'http://www.ebi.ac.uk/ena/data/view/{s}&display=xml').text))
    title[s] = et.findtext("STUDY/DESCRIPTOR/STUDY_TITLE")

studies = {}

for colname in ['env_biome', 'env_feature', 'env_material']:
    col = selected[colname].groupby(selected.study_accession).agg(summarize_col)
    studies[colname] = col[list(selected_projects)]

studies['title'] = title
studies['nr_samples'] = counts

studies = pd.DataFrame(studies)
studies.sort_values(by='nr_samples', inplace=True)


# blacklist : [k] -> (reason, title)
# The title is reproduced to avoid errors
blacklist = {
  'ERP003628,ERP006156-ERP006157,ERP018626':
        ('protists', 'Shotgun Sequencing of Tara Oceans DNA samples corresponding to size fractions for protist'),
  'ERP003628,ERP006157,ERP018626':
        ('protists', 'Shotgun Sequencing of Tara Oceans DNA samples corresponding to size fractions for protist'),
  'ERP006157,ERP018626':
        ('protists/amplicon', 'Amplicon sequencing of Tara Oceans DNA samples corresponding to size fractions for protists.'),
  'ERP012262':
        ('amplicon', 'The SCALEMIC-Experiment: Spatiotemporal distributions of microbial communities at the plot scale in a grassland soil'),
  'SRP070688':
        ('S. cerevisiae', 'Saccharomyces cerevisiae Raw sequence reads'),
  'SRP045211':
        ('Amplicon', 'Human gut Targeted Locus (Loci)'),
 }


remove = []
for k in blacklist:
    (reason, t) = blacklist[k]
    if title[k] != t:
        raise ValueError("Title Mismatch")
    remove.append(k)
    selected_projects.remove(k)

studies.drop(remove, inplace=True)

studies.sort_values(by='nr_samples', ascending=False, inplace=True)
studies.to_csv('data/studies-{}.tsv'.format(MIN_NR_SAMPLES), sep='\t')

selected = selected[[(pr in selected_projects) for pr in selected.study_accession]]
selected.to_csv('./data/selected-cleaned-metadata-{}.tsv'.format(MIN_NR_SAMPLES), sep='\t')


