#!/usr/bin/env bash
docker run -v $PWD/../:/sources -v $HOME/.ssh:/secrets "$(docker build ../c++build -q)" make -C /sources/filter-seqs
