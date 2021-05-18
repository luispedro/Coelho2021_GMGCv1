#!/usr/bin/env bash

set -e
export PATH=/g/scb2/bork/coelho/DD_DeCaF/genecats/sources/kmer-find/bin/:$PATH

input_faa="$1"

mkdir kmer.index
echo "starting..."
EncodeKmers -i ${input_faa} -o kmer.index/${input_faa}.kmer.ix -t8

echo "Encoding kmers done"
extract-names32.py ${input_faa} kmer.index/${input_faa}.names32
cd kmer.index
du -sh *

for k in *.kmer.ix.*; do
    sort32pairs $k ${k}.sorted
    rm ${k}
    echo "Sorted ${k}"
done

cat ${input_faa}.kmer.ix.0.sorted \
    ${input_faa}.kmer.ix.1.sorted \
    ${input_faa}.kmer.ix.2.sorted \
    ${input_faa}.kmer.ix.3.sorted \
    ${input_faa}.kmer.ix.4.sorted \
    ${input_faa}.kmer.ix.5.sorted \
    ${input_faa}.kmer.ix.6.sorted \
    ${input_faa}.kmer.ix.7.sorted \
    ${input_faa}.kmer.ix.8.sorted \
    ${input_faa}.kmer.ix.9.sorted \
    ${input_faa}.kmer.ix.10.sorted \
    ${input_faa}.kmer.ix.11.sorted \
    ${input_faa}.kmer.ix.12.sorted \
    ${input_faa}.kmer.ix.13.sorted \
    ${input_faa}.kmer.ix.14.sorted \
    ${input_faa}.kmer.ix.15.sorted \
        > ${input_faa}.kmer.ix.sorted

BuildIndex2 -i ${input_faa}.kmer.ix.sorted -o ${input_faa}.kmer.ix1 -p ${input_faa}.kmer.ix2 -v -t8
