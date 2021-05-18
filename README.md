# Towards the biogeography of prokaryotic genes

This is the Supplemental Software package to the manuscript "Towards the
biogeography of prokaryotic genes" by Coelho _et al._ (_forthcoming_).

The purpose of this repository is to archive the code that generated both the
resource and the analyses in the manuscript.

The **G**lobal **M**icrobial **G**ene **C**atalogue is available at
[https://gmgc.embl.de](https://gmgc.embl.de).

## Dependencies

The initial processing of the metagenomes is performed with
[NGLess](https://ngless.embl.de/), using
[MEGAHIT](https://github.com/voutcn/megahit). ORF calling was performed using
[MetaGeneMark](https://github.com/voutcn/megahit).

The catalog building was performed with a mixture of custom Haskell/C++ code
and [mmseqs2](http://mmseqs.com/).

The subsquent analyses were performed in Python, using NumPy, Pandas, and
[Jug](https://jug.readthedocs.io).

**License**: MIT


## Data Availability

**Sequence data**: The full raw data (metagenomes) is available from ENA (see
Supplemental Table 1 in the manuscript for a comprehensive list of all
accession numbers.

**Gene catalog & annotations**: The gene catalog and its annotations is
available at [https://gmgc.embl.de](https://gmgc.embl.de).

**Preprocessed data**: For convenience, preprocessed derived data is also
available under the `preprocessed/` directory. These were computed with the
code 


