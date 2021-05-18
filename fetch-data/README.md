# Process

    jug execute ena-metadata.py

The file `ena-sample-meta.py` contains an alternative method to obtain relevant
samples and `compare-ena-metadata-ena-sample-meta.py` compares the results of
the two methods. As of April '17, only two projects were not recovered by
ena-metadata.py and, upon inspection, these are not relevant:

ERP008725 : MiSeq study
ERP012971 : actually a bunch of genomes from single cell isolates

    python select_studies.py

This will now retrieve the studies with >100 samples.
