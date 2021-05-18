import ena

RAW_EXAMPLE_DATA = '''\
study_accession\trun_accession\tsample_accession\tfastq_ftp\tfastq_md5\tfastq_bytes\tsubmitted_ftp\tsubmitted_md5\tsubmitted_bytes
PRJEB8286\tERR738544\tSAMEA3216836\t\t\t\tftp.sra.ebi.ac.uk/vol1/ERA404/ERA404284/fastq/setA1_1.fq.gz;ftp.sra.ebi.ac.uk/vol1/ERA404/ERA404284/fastq/setA1_2.fq.gz\t89a456cb341455656e1ede1cea87937f;4038bced5554cc2e381d489aede590ef\t2788773224;2754769022
PRJEB8286\tERR738548\tSAMEA3216840\t\t\t\tftp.sra.ebi.ac.uk/vol1/ERA404/ERA404284/fastq/setB2_1.fq.gz;ftp.sra.ebi.ac.uk/vol1/ERA404/ERA404284/fastq/setB2_2.fq.gz\td39e0e0a9b6e282661c58fa2ba906412;dc9de6f79e7f46d4e3f48e256f9c5bb6\t2610218369;2577544309
PRJEB8286\tERR738549\tSAMEA3216841\tftp.sra.ebi.ac.uk/vol1/ERA404/ERA404284/fastq/setB3_1.fq.gz;ftp.sra.ebi.ac.uk/vol1/ERA404/ERA404284/fastq/setB3_2.fq.gz\t67ebc4967b1ea2b14918825471f984bc;6b77925423dd43185f0dd501f4219e0b\t2573643536;2544623916\t\t
'''

def test_expand_fastq_cols():
    import pandas as pd
    from six import StringIO
    data = pd.read_table(StringIO(RAW_EXAMPLE_DATA))
    expanded = ena.expand_fastq_columns(data)
    assert len(expanded) == 6
    assert len(expanded.ftp.dropna()) == 6
    assert len(expanded.bytes.dropna()) == 6
    assert len(expanded.md5.dropna()) == 6
    assert not expanded.ftp.map(lambda s: ';' in s).any()
    assert not expanded.bytes.map(lambda s: ';' in s).any()
    assert not expanded.md5.map(lambda s: ';' in s).any()
