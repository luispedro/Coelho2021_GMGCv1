import os
from os import path, makedirs
import ena
import pandas as pd
from six import StringIO
import urllib
import pathlib
import requests
from contextlib import closing
from sys import argv

study_accession = argv[1]
existing_data_dir = argv[2]

base_data_dir = pathlib.Path.cwd()
filetable = pd.read_table(StringIO(ena.get_project_reads_table(study_accession))).dropna()
n = len(filetable)
md5_table = pd.read_table(existing_data_dir + 'MD5SUM.txt', index_col=0, header=None, sep=' ')
md5_table = md5_table[2]

for i in range(n):
    if (i+1) % 25 == 0:
        print("Checking {}/{}".format(i+1,n))
    source = filetable.iloc[i]

    for fastq_ftp, fastq_bytes, fastq_md5 in zip(
                source.fastq_ftp.split(';'),
                source.fastq_bytes.split(';'),
                source.fastq_md5.split(';')):
        urlraw = 'http://' + fastq_ftp
        url = urllib.parse.urlparse(urlraw)
        p = pathlib.PurePath(url.path)
        target_dir = base_data_dir / p.parent.relative_to('/')
        makedirs(target_dir, exist_ok=True)
        ofile = target_dir / p.name
        if fastq_md5 in md5_table.index:
            if path.exists("{}/{}".format(existing_data_dir, md5_table.ix[fastq_md5])):
                os.rename("{}/{}".format(existing_data_dir, md5_table.ix[fastq_md5]) , str(ofile))
