#include <iostream>
#include <cassert>
#include <vector>
#include <string>
#include <fstream>
#include <inttypes.h>

struct UnionFind {
    void link(int ix0, int ix1) {
        grow(std::max<int>(ix0, ix1) + 1);
        int r0 = rep(ix0);
        int r1 = rep(ix1);
        uf_[r0] = r1;
    }

    void flatten() {
        for (unsigned i = 0; i < uf_.size(); ++i) {
            rep(i);
        }
    }
    int rep(int ix) {
        int r = uf_[ix];
        while (uf_[r] != r) {
            uf_[r] = uf_[uf_[r]];
            r = uf_[r];
        }
        uf_[ix] = r;
        return r;
    }

    void grow(int ns) {
        assert(ns >= 0);
        if (uf_.size() >= ns) return;
        unsigned i = uf_.size();
        uf_.resize(ns);
        for ( ; i < ns; ++i) uf_[i] = i;
    }

    int at(const int ix) const { return uf_[ix]; }
    std::vector<int>::size_type size() const { return uf_.size(); }


    private:
    std::vector<int> uf_;
};


int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " FILE-LIST" << std::endl;
        return 1;
    }
    std::ifstream flist(argv[1]);
    std::string f;
    UnionFind uf;
    uint32_t buffer[4096];
    while (std::getline(flist, f)) {
        std::cout << "Processing file " << f << std::endl;
        std::ifstream fdata(f.c_str());
        while (!fdata.eof()) {
            void* bufferp = buffer;
            fdata.read((char*)bufferp, sizeof(buffer));
            int read = fdata.gcount()/sizeof(buffer[0]);
            if (!read) {
                break;
            }
            for (int j = 0; j < read/2; ++j) {
                const int f = buffer[2*j + 0];
                const int s = buffer[2*j + 1];
                uf.link(f, s);
            }
        }
    }
    uf.flatten();
    const char* oname = argc >= 3 ? argv[2] : "results/uf.txt";
    std::ofstream ofname(oname);
    for (int i = 0; i < uf.size(); ++i) {
        if (argc <= 2 || uf.at(i) != i) {
            ofname << i << '\t' << uf.at(i) << '\n';
        }
    }
    return 0;
}

