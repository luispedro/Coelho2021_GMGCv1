#include <iostream>
#include <vector>
#include <fstream>
#include <exception>
#include <unordered_set>
#include <algorithm>
#include <thread>
#include <seqan/seq_io.h>
#include <Poco/Glob.h>

struct FastaSeq {
    FastaSeq(seqan::CharString id,
            seqan::Dna5String seq)
        :id(id)
         ,seq(seq)
    { }
    size_t size() const {
        return seqan::end(seq) - seqan::begin(seq);
    }
    seqan::CharString id;
    seqan::Dna5String seq;
};

std::vector<std::string> read_short() {
    std::vector<std::string> valid;
    std::ifstream ifile("duplicate.info/short-sort.txt");
    std::string line;
    int n = 0;
    while (std::getline(ifile, line)) {
        if (n % 10000000 == 0) {
            std::cerr << "Read " << n / 10000000 << " blocks." << std::endl;
        }
        const size_t sep = line.find('|');
        valid.push_back(line.substr(0, sep));
        ++n;
    }
    return valid;
}

bool compare_seq_size(const FastaSeq& a, const FastaSeq& b) { return a.size() < b.size(); }
const unsigned BLOCK_SIZE = 80;

std::vector<FastaSeq> read_filter(const std::vector<std::string>* valid, const char* ifilename) {
    std::vector<FastaSeq> seqs;
    std::cerr << "Opening file: " << ifilename << std::endl;
    seqan::SeqFileIn ifile(ifilename);
    seqan::CharString id;
    seqan::Dna5String seq;
    while (!seqan::atEnd(ifile)) {
        seqan::readRecord(id, seq, ifile);
        std::string ids = seqan::toCString(id);
        const size_t sep = ids.find('|');
        ids = ids.substr(0, sep);
        if (std::binary_search(valid->begin(), valid->end(), ids)) {
            seqs.push_back(FastaSeq(ids, seq));
        }
    }
    std::sort(seqs.begin(), seqs.end(), compare_seq_size);
    return seqs;
}


struct merger {
    const std::vector<std::vector<FastaSeq>>& inputs;
    std::vector<unsigned> positions;
    unsigned left;
    explicit merger(const std::vector<std::vector<FastaSeq>>& inputs)
        :inputs(inputs) {
            positions.resize(inputs.size());
            left = 0;
            for (const auto& p : inputs) { left += p.size(); }
        }
    bool empty() const { return left == 0; }
    const FastaSeq& next() {
        std::vector<FastaSeq>::iterator res;
        int best_i = -1;
        int i = 0;
        for ( ; i != positions.size(); ++i) {
            if (positions[i] < inputs[i].size()) {
                best_i = i;
                ++i;
                break;
            }
        }

        for ( ; i != positions.size(); ++i) {
            if (positions[i] < inputs[i].size()) {
                if (compare_seq_size(inputs[i][positions[i]], inputs[best_i][positions[best_i]])) {
                    best_i = i;
                }
            }
        }
        if (best_i == -1) {
            throw std::runtime_error("Could not find an element when merging...");
        }
        --left;
        return inputs[best_i][positions[best_i]++];
    }
};


int main(int argc, char* argv[]) {
    try {
        if (argc < 3) {
            std::cerr << "Usage " << argv[0] << " OFILE INPUT1 [INPUT2 ...]" << std::endl;
            return 1;
        }
        const std::vector<std::string> valid = read_short();
        std::cerr << "Read valid list" << std::endl;
        std::vector<std::future<std::vector<FastaSeq>>> partials;
        for (int i = 2; i < argc; ++i) {
            partials.emplace_back(std::async(std::launch::async,
                        read_filter, &valid, argv[i]));
        }
        std::vector<std::vector<FastaSeq>> partialresults;
        for (auto& f: partials) { partialresults.emplace_back(f.get()); }

        merger finalresults(partialresults);

        seqan::SeqFileOut ofile(argv[1]);
        while (!finalresults.empty()) {
            const FastaSeq& seq = finalresults.next();
            seqan::writeRecord(ofile, seq.id, seq.seq);
        }
        return 0;
    } catch (std::exception& e) {
        std::cerr << "Something bad happened: " << e.what() << "\n";
        return 1;
    }
}
