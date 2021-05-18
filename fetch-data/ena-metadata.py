from collections import namedtuple
from jug import TaskGenerator, bvalue
import ena
from cleanup import cleanup_metadata
from jug.hooks import exit_checks

exit_checks.exit_if_file_exists('jug.exit')

cleanup_metadata = TaskGenerator(cleanup_metadata)
get_sample_xml = TaskGenerator(ena.get_sample_xml)
get_data_xml = TaskGenerator(ena.get_data_xml)
parse_experiment_meta = TaskGenerator(ena.parse_experiment_meta)

Project = namedtuple('Project', ['accession', 'samples'])
PROJECT_DATA_URL = 'http://www.ebi.ac.uk/ena/data/warehouse/search?' + \
                    'query="library_source="METAGENOMIC""&result=read_study&download=xml&display=xml'
PROJECTS_DATA_FILE = 'data/project-list.xml'
SAMPLE_BY_TAXID_FILE = 'data/sample-408169-table.tsv'

@TaskGenerator
def download_project_data():
    '''Download reads with library_source = METAGENOMIC'''
    import requests
    from contextlib import closing
    from os import makedirs
    makedirs('data', exist_ok=True)
    with closing(requests.get(PROJECT_DATA_URL, stream=True)) as ifile, \
                open(PROJECTS_DATA_FILE, 'wb') as ofile:
        for chunk in ifile.iter_content():
            ofile.write(chunk)
    return PROJECTS_DATA_FILE

@TaskGenerator
def download_sample_408169_table():
    '''Download samples by TaxID below 408169 (which is "metagenome")'''
    import requests
    from contextlib import closing
    from os import makedirs
    from ena import ENA_BASE_URL

    makedirs('data', exist_ok=True)
    url = ENA_BASE_URL + 'data/warehouse/search?query=%22tax_tree(408169)%22&limit=522911&length=522911&offset=1&display=report&result=sample&fields=accession,secondary_sample_accession,first_public,tax_id,scientific_name,sample_alias&download=txt'
    with closing(requests.get(url, stream=True)) as ifile, \
                open(SAMPLE_BY_TAXID_FILE, 'wb') as ofile:
        for chunk in ifile.iter_content():
            ofile.write(chunk)
    return SAMPLE_BY_TAXID_FILE


@TaskGenerator
def getprojects(datafile):
    import xml.etree.ElementTree as ET

    tree = ET.parse(datafile)
    root = tree.getroot()
    projects = []
    for it in root.iter('PROJECT'):
        accession = it.get('accession')
        for lk in it.findall('PROJECT_LINKS/PROJECT_LINK/XREF_LINK'):
            d = lk.find('DB')
            if d.text == 'ENA-SAMPLE':
                projects.append(Project(accession, lk.find('ID').text))
    return projects

@TaskGenerator
def projectreads(pr):
    return ena.get_project_reads_table(pr.accession)

@TaskGenerator
def astable(data):
    import numpy as np
    import pandas as pd
    from six import StringIO
    from collections import Counter
    from pdutils import pdselect
    table = pd.concat([pd.read_table(StringIO(d)) for d in data])
    repeats = [k for k,v in Counter(table['run_accession']).items() if v > 1]
    for r in repeats:
        r = pdselect(table, run_accession=r)
        if not np.all( (r == r.iloc[0]) | r.isnull() ):
            raise ValueError("Runs with same ID have different data")
    table.drop_duplicates(subset='run_accession', inplace=True)
    table.index = table.run_accession
    table.drop('run_accession', inplace=True, axis=1)
    return table


@TaskGenerator
def experiment_table(metamerged_ex):
    import pandas as pd
    from collections import Counter
    hasdata = Counter()
    for k,vs in metamerged_ex.items():
        hasdata.update(vs.keys())
    used = set(k for k,v in hasdata.items() if v >=100)
    for k,vs in metamerged_ex.items():
        for k in list(vs.keys()):
            if k not in used:
                del vs[k]
    m = pd.DataFrame.from_dict(metamerged_ex, orient='index')
    hasdata = m.shape[0] - m.isnull().sum()
    hasdata.sort_values(inplace=True)
    m = m[hasdata.index[::-1]]
    return m

@TaskGenerator
def samples_with_wgs(table_ex):
    library = table_ex[['sample_accession', 'LIBRARY_STRATEGY', 'LIBRARY_SOURCE', 'LIBRARY_SELECTION']]
    library = library.query('LIBRARY_STRATEGY == "WGS"').query('LIBRARY_SOURCE != "GENOMIC"')
    return set(library.sample_accession)

@TaskGenerator
def parsei(ptable):
    import pandas as pd
    from six import StringIO
    return pd.read_table(StringIO(ptable)).groupby('instrument_model').count()

@TaskGenerator
def parse_sample_meta(data):
    from six import StringIO
    return ena.parse_sample_meta(StringIO(data))


@TaskGenerator
def mergedicts(ds):
    merged = {}
    for d in ds:
        merged.update(d)
    return merged


@TaskGenerator
def merge_metadatable(metamerged, selected_samples):
    import pandas as pd
    print("MISSING: ", sum(1 for k in selected_samples if k not in metamerged))
    metasample = pd.DataFrame({k:metamerged[k] for k in selected_samples if k in metamerged})
    metasample = metasample.T
    return metasample

@TaskGenerator
def save_to_tsv(table, oname):
    table.to_csv(oname, sep='\t')


ACCEPTED_INSTRUMENTS = [
 'HiSeq X Five',
 'HiSeq X Ten',

 'Illumina Genome Analyzer',
 'Illumina Genome Analyzer II',
 'Illumina Genome Analyzer IIx',

 'Illumina HiScanSQ',
 'Illumina HiSeq 1000',
 'Illumina HiSeq 1500',
 'Illumina HiSeq 2000',
 'Illumina HiSeq 2500',
 'Illumina HiSeq 3000',
 'Illumina HiSeq 4000',

 'Illumina NovaSeq 6000',
 'NextSeq 500',
 'NextSeq 550',
]



MIN_NR_READS = 1000*1000
MIN_AVG_READLEN = 75

@TaskGenerator
def select_samples(table):
    from pdutils import pdselect
    with open('outputs/filter-stats.txt', 'w') as ostats:
        ostats.write("Nr samples annotated as METAGENOMICS:             {}\n".format(len(table)))
        table = pdselect(table, instrument_model__in=ACCEPTED_INSTRUMENTS)
        ostats.write("Nr samples sequenced with HiSeq:                  {}\n".format(len(table)))
        per_s = table.groupby('sample_accession').sum()[['base_count', 'read_count']]
        per_s['avg_readlen'] = per_s['base_count']/per_s['read_count']
        selected = pdselect(per_s.dropna(), read_count__ge=MIN_NR_READS, avg_readlen__ge=MIN_AVG_READLEN)
        selected = list(selected.index)
        selected = [s for s in selected if ' samples' not in s]
        ostats.write("Nr samples (nr reads >= {}k & avg len >= {}):   {}\n".format(MIN_NR_READS//1000, MIN_AVG_READLEN, len(selected)))
        return selected


projects = bvalue(getprojects(download_project_data()))
instruments = []
preads = []
for pr in projects:
    preads.append(projectreads(pr))
    instruments.append(parsei(preads[-1]))

table = astable(preads)

@TaskGenerator
def group_samples(table):
    from utils import group
    samples = list(set(table.sample_accession.values))
    return [','.join(s) for s in group(samples, 25)]

@TaskGenerator
def group_experiments(table):
    from utils import group
    samples = list(set(table.experiment_accession.values))
    return [','.join(s) for s in group(samples, 25)]


@TaskGenerator
def select_wgs(table_ex):
    return table_ex.query('LIBRARY_STRATEGY == "WGS"').query('LIBRARY_SOURCE != "GENOMIC"')

download_sample_408169_table()

chunks = bvalue(group_samples(table))
chunks_ex = bvalue(group_experiments(table))

metasample = []
for ch in chunks:
    metasample.append(parse_sample_meta(get_sample_xml(ch)))
metamerged = mergedicts(metasample)

metaexperiment = []
for ch in chunks_ex:
    metaexperiment.append(parse_experiment_meta(get_data_xml(ch), is_string=True))
metamerged_ex = mergedicts(metaexperiment)
table_ex = experiment_table(metamerged_ex)
has_wgs = samples_with_wgs(table_ex)
sel_table_ex = select_wgs(table_ex)

selected_samples = select_samples(table)
metamerged = merge_metadatable(metamerged, selected_samples)
cleaned = cleanup_metadata(metamerged)
save_to_tsv(cleaned, './data/selected-cleaned-metadata.tsv')
