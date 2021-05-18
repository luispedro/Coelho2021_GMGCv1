from jug import TaskGenerator
import pandas as pd
from mirror import mirror_all_files, build_link_structure

USE_ASPERA = True
MIRROR_BASEDIR = '/g/bork5/mocat/EBI_MIRROR'

@TaskGenerator
def create_mirror(study_accession, target_directory):
    import ena
    filetable = ena.get_project_reads_table(study_accession, as_pandas_DataFrame=True)
    filetable = ena.expand_fastq_columns(filetable)
    mirror_all_files(filetable, MIRROR_BASEDIR, use_aspera=USE_ASPERA)
    build_link_structure(filetable, MIRROR_BASEDIR, target_directory, study_accession)

studies = pd.read_table("studies.txt", comment='#')
for _, row in list(studies.iterrows()):
    create_mirror(row.study_accession, '../' + row.directory_name)
