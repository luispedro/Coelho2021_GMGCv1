filter-seqs: filter-seqs.o
	g++ $(CFLAGS) $< -O2 -static -pthread -o $@ -lPocoFoundation

filter-seqs.o: filter-seqs.cpp
	g++ $(CFLAGS) -c $< -O2 -pthread -o $@
