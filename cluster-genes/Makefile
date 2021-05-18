all: bin/dominant haskell bin/sort-triplets

bin/dominant: dominant.cpp
	g++ -O2 -std=c++11 -o $@ -Wall $<

bin/sort-triplets: sort-big.cpp
	g++ -O2 -std=c++11 -o $@ -Wall $<

haskell:
	mkdir -p bin
	stack install --local-bin-path $$PWD/bin

build:
	stack build
