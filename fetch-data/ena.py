import requests
ENA_BASE_URL = 'http://www.ebi.ac.uk/ena/'
ENA_DATA_VIEW_URL = ENA_BASE_URL + 'data/view/'
ENA_FILEREPORT_URL = ENA_BASE_URL + 'data/warehouse/filereport'

def parse_sample_meta(data, is_string=False):
    import xml.etree.ElementTree as ET
    if is_string:
        from six import StringIO
        data = StringIO(data)
    tree = ET.parse(data)
    root = tree.getroot()
    sample_meta = {}
    for s in root.iter('SAMPLE'):
        meta = {}
        primary_id = s.findtext('IDENTIFIERS/PRIMARY_ID')
        if primary_id is not None:
            meta['primary_id'] = primary_id
        for c in s.iter('SAMPLE_ATTRIBUTE'):
            tag = c.find('TAG')
            value = c.find('VALUE')
            if tag is not None and value is not None:
                meta[tag.text] = value.text
        for lnk in s.findall('./SAMPLE_LINKS/SAMPLE_LINK/XREF_LINK'):
            if lnk.find('DB').text == 'ENA-STUDY':
                meta['study_accession'] = lnk.find('ID').text
                break
        else:
            # Some samples are not linked to a study
            meta['study_accession'] = None
        for ext in s.iter('EXTERNAL_ID'):
            if ext.get('namespace') == "BioSample":
                accession = ext.text
                break
        else:
            raise ValueError("No BioSample EXTERNAL_ID")
        sample_meta[accession] = meta
    return sample_meta

def parse_experiment_meta(data, is_string=False):
    import xml.etree.ElementTree as ET
    if is_string:
        from six import StringIO
        data = StringIO(data)
    tree = ET.parse(data)
    root = tree.getroot()
    experiment_meta = {}
    for s in root.iter('EXPERIMENT'):
        meta = {}
        primary_id = s.findtext('IDENTIFIERS/PRIMARY_ID')
        if primary_id is not None:
            meta['primary_id'] = primary_id
        for c in s.iter('EXPERIMENT_ATTRIBUTE'):
            tag = c.find('TAG')
            value = c.find('VALUE')
            if tag is not None and value is not None:
                meta[tag.text] = value.text
        for lnk in s.findall('./DESIGN/SAMPLE_DESCRIPTOR/IDENTIFIERS'):
            for ext in lnk.iter('EXTERNAL_ID'):
                if ext.get('namespace') == "BioSample":
                    meta['sample_accession'] = ext.text
        for lnk in s.findall('./EXPERIMENT_LINKS/EXPERIMENT_LINK/XREF_LINK'):
            if lnk.find('DB').text == 'ENA-STUDY':
                meta['study_accession'] = lnk.find('ID').text
            elif lnk.find('DB').text == 'ENA-SAMPLE':
                meta['sample_accession_alternative'] = lnk.find('ID').text
        for e in ['study_accession' ,'sample_accession']:
            if e not in meta:
                meta[e] = None
        meta['LIBRARY_STRATEGY'] = s.findtext('DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_STRATEGY')
        meta['LIBRARY_SOURCE'] = s.findtext('DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_SOURCE')
        meta['LIBRARY_SELECTION'] = s.findtext('DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_SELECTION')

        experiment_meta[primary_id] = meta
    return experiment_meta

def get_data_xml(sample, parse=False):
    return requests.get("{}{}&display=xml".format(ENA_DATA_VIEW_URL, sample)).text
get_sample_xml  = get_data_xml

def get_project_reads_table(accession, as_pandas_DataFrame=False):
    '''Returns a TSV table with all reads for a given project

    Parameters
    ----------
    accession : str
        Project accession
    as_pandas_DataFrame : boolean (default: False)
        If true, returns a pandas DataFrame

    Returns
    -------
    table : str or pandas.DataFrame
        TSV table with information on all runs
    '''
    import requests
    fields = ','.join([
                'study_accession',
                'run_accession',
                'sample_accession',
                'experiment_accession',
                'instrument_model',
                'fastq_ftp',
                'fastq_md5',
                'fastq_bytes',
                'submitted_ftp',
                'submitted_md5',
                'submitted_bytes',
                'read_count',
                'base_count',
                ])
    url = '{base_url}?accession={accession}&&result=read_run&fields={fields}'.format(
                base_url=ENA_FILEREPORT_URL,
                accession=accession,
                fields=fields)
    data = requests.get(url).text
    if as_pandas_DataFrame:
        import pandas as pd
        from six import StringIO
        return pd.read_table(StringIO(data))
    return data


def expand_fastq_columns(filetable):
    '''Expand fastq related columns'''
    import pandas as pd
    import numpy as np
    filetable['ftp'] = filetable.fastq_ftp.fillna(filetable.submitted_ftp)
    filetable['bytes'] = filetable.fastq_bytes.fillna(filetable.submitted_bytes)
    filetable['md5'] = filetable.fastq_md5.fillna(filetable.submitted_md5)

    file_cols = [
            'fastq_ftp',
            'fastq_bytes',
            'fastq_md5',
            'submitted_ftp',
            'submitted_bytes',
            'submitted_md5']
    split_cols = ['ftp', 'bytes', 'md5']
    expanded = {}
    for c in split_cols:
        col = filetable[c]
        if col.dtype == np.object_:
            expanded[c] = col.str.split(';', expand=True).stack().reset_index(drop=True, level=1)
        else:
            expanded[c] = col
    expanded = pd.DataFrame(expanded)
    return filetable.drop(split_cols + file_cols, axis=1).join(expanded)

