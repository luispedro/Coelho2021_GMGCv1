import os
from os import path, makedirs
import ena
import pandas as pd
from six import StringIO
import urllib
import pathlib
import requests
from contextlib import closing

ASPERA_BINARY = '/home/mocat/.aspera/connect/bin/ascp'
ASPERA_KEY = '/home/mocat/.aspera/connect/etc/asperaweb_id_dsa.openssh'

def mirror_path(mirror_basedir, ftp):
    import urllib
    import pathlib
    url = urllib.parse.urlparse('http://' + ftp)
    p = pathlib.PurePath(url.path)
    target_dir = mirror_basedir / p.parent.relative_to('/')
    return target_dir / p.name



def md5sum_file(ifile):
    '''Computes MD5 sum of ifile'''
    import hashlib
    m = hashlib.md5()
    BLOCK_SIZE = 8192
    with open(ifile, 'rb') as ifile:
        while True:
            data = ifile.read(BLOCK_SIZE)
            if not data:
                return m.hexdigest()
            m.update(data)

def http_download_file(url, ofile):
    with closing(requests.get(url, stream=True)) as ifile, \
                open(ofile, 'wb') as ofile:
        for chunk in ifile.iter_content():
            ofile.write(chunk)

def aspera_download_file(aspera_url, ofile):
    '''Call ascp on the command line to download `aspera_url` to `ofile`'''
    import subprocess
    cmdline = [
            ASPERA_BINARY,
            '-T', # No encryption
            '-l', '300m',
            '-i', ASPERA_KEY,
            aspera_url,
            str(ofile)]
    subprocess.run(cmdline, check=True)

def mirror_all_files(filetable, mirror_basedir, *, progress=True, use_aspera=False):
    n = len(filetable)
    for i in range(n):
        if progress:
            print("Processing file {} of {}.".format(i + 1, n))
        source = filetable.iloc[i]

        urlraw = 'http://' + source.ftp
        url = urllib.parse.urlparse(urlraw)
        p = pathlib.PurePath(url.path)
        target_dir = mirror_basedir / p.parent.relative_to('/')

        makedirs(target_dir, exist_ok=True)
        ofile = target_dir / p.name

        if path.exists(ofile):
            if os.stat(ofile).st_size != int(source.bytes):
                print("Existing output file has wrong size. Removing...")
                os.unlink(ofile)
            elif md5sum_file(ofile) != source.md5:
                print("Existing output file has wrong hash. Removing...")
                os.unlink(ofile)
            else:
                print("Correct output file exists. Skipping...")
                continue
        if use_aspera:
            aspera_url = 'era-fasp@fasp.sra.ebi.ac.uk:'+url.path
            aspera_download_file(aspera_url, ofile)
        else:
            http_download_file(urlraw, ofile)

def build_link_structure(filetable, mirror_basedir, data_basedir, sample_fname):
    data_basedir = pathlib.PurePath(data_basedir)
    makedirs(data_basedir, exist_ok=True)
    with open(data_basedir / sample_fname, 'w') as samplefile:
        for s in set(filetable.sample_accession):
            makedirs(data_basedir / s, exist_ok=True)
            samplefile.write("{}\n".format(s))
    p = mirror_path(mirror_basedir, filetable.ftp.iloc[0])
    def norm_path(p):
        p = str(p)
        if p.endswith('_1.fastq.gz'):
            return pathlib.PurePath(p[:-len('_1.fastq.gz')]+'.pair.1.fq.gz')
        if p.endswith('_2.fastq.gz'):
            return pathlib.PurePath(p[:-len('_2.fastq.gz')]+'.pair.2.fq.gz')
        if p.endswith('.fastq.gz'):
            return pathlib.PurePath(p[:-len('.fastq.gz')] + '.single.fq.gz')
        raise ValueError("Cannot normalize {}".format(p))

    n = len(filetable)
    for i in range(n):
        source = filetable.iloc[i]
        target = data_basedir / source.sample_accession / norm_path(source.ftp).name
        os.symlink(mirror_path(mirror_basedir, source.ftp), target)
