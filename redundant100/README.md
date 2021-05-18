# 100 Redundancy detection

Set up a config.py file with 3 variables

    INPUT = 'data/input.fna'
    OUTPUT = 'data/output.fna'
    TAG = 'my-job'


The tag will be used to distinguish multiple jobs co-existing on the same
directory.

Alternatively, INPUT can point to a file containing a *list of input files* and
set the variable IS_FILE_LIST to True:

    INPUT = 'data/filelist.txt'
    IS_FILE_LIST = True
    OUTPUT = 'data/output.fna'
    TAG = 'my-job'

In this case, the input file should contain a file per line.
