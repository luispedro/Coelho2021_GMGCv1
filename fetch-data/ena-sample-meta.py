'''The goal of this script is to recover all samples where investigation_type
is [metagenome|metagenomic|metagenomics]'''

from jug import TaskGenerator

@TaskGenerator
def getdata(i):
    import requests
    url = 'http://www.ebi.ac.uk/ena/data/warehouse/search?domain=&result=SAMPLE&query=%28investigation_type%3D%22metagenome%22+OR+investigation_type%3D%22metagenomic%22+OR+investigation_type%3D%22metagenomics%22%29&md5=&length=1000&offset={}&limit=&display=xml&download=xml'.format(i * 1000 + 1)
    return requests.get(url).text


@TaskGenerator
def samplemeta(data):
    import ena
    from six import StringIO
    return ena.parse_sample_meta(StringIO(data))

@TaskGenerator
def build_table(data):
    import pandas as pd
    merged = {}
    for d in data:
        merged.update(d)
    return pd.DataFrame(merged).T

@TaskGenerator
def save_table(table):
    table.to_csv('data/sample-meta.tsv', sep='\t')

data = []
for i in range(55):
    data.append(samplemeta(getdata(i)))

table = build_table(data)
save_table(table)


