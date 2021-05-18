#include <iostream>
#include <algorithm>
#include <fstream>
#include <vector>
#include <inttypes.h>

struct Word32Pair {
    Word32Pair(uint32_t t, uint32_t i)
        :token(t)
        ,ix(i)
          { }
    uint32_t token;
    uint32_t ix;
};

bool operator <(const Word32Pair& a, const Word32Pair& b) {
    if (a.token == b.token) return a.ix < b.ix;
    return a.token < b.token;
}

std::vector<Word32Pair> read_vector(const char* ifname) {
    uint32_t buffer[1024 * 2 * sizeof(uint32_t)];
    std::vector<Word32Pair> res;
    std::ifstream fin(ifname, std::ios::binary | std::ios::ate);
    size_t size_of_file = fin.tellg();
    std::cerr << "Got a file with " << size_of_file << " Bytes.\n";
    res.reserve(size_of_file/2/4);
    fin.seekg(0, std::ios::beg);
    while (true) {
        fin.read((char*)buffer, sizeof(buffer));
        const unsigned nbytes = fin.gcount();
        if (!nbytes) break;

        if (nbytes % (2 * sizeof(buffer[0])) != 0) {
            std::cerr << "Bad input file " << ifname
                        << " size is incorrect (read " << nbytes << " Bytes; not multiple of " << (2*sizeof(buffer[0])) << ")." << std::endl;
            throw "Bad input";
        }
        const int n_pairs = nbytes/(2 * sizeof(buffer[0]));
        for (int i = 0; i < n_pairs; ++i) {
            res.push_back(Word32Pair(buffer[2*i], buffer[2*i + 1]));
        }
    }
    std::cerr << "Finished reading.\n";
    return res;
}

int write_vector(const char* ofname, const std::vector<Word32Pair>& pairs) {
    std::ofstream fout(ofname);
    size_t unwritten = 2 * 4 * pairs.size();
    const char* data = (const char*)pairs.data();
    while (unwritten > 0) {
        const size_t wsize = (unwritten > 8192 ? 8192 : unwritten);
        fout.write(data, wsize);
        if (!fout) {
            std::cerr << "FAILED";
            return 1;
        }
        unwritten -= wsize;
        data += wsize;
    }
    return 0;
}

int main(int argc, const char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " INFILE " << " OFILE\n";
        return 1;
    }
    std::vector<Word32Pair> vector = read_vector(argv[1]);
    std::cerr << "Read data.\n";
    std::sort(vector.begin(), vector.end());
    std::cerr << "Finished sorting.\n";
    return write_vector(argv[2], vector);
}

