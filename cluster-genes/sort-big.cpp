#include <fstream>
#include <algorithm>
#include <parallel/algorithm>
#include <iostream>
#include <queue>
#include <exception>
#include <fstream>
#include <vector>
#include <map>


const int BlockSize = 3;

template <int N>
struct Uint32Block {
    uint32_t data[N];
};


template <int N>
bool operator <(const Uint32Block<N>& a, const Uint32Block<N>& b) {
    for (int i = 0; i != N; ++i) {
        if (a.data[i] != b.data[i]) return a.data[i] < b.data[i];
    }
    return false;
}
template <int N>
bool operator == (const Uint32Block<N>& a, const Uint32Block<N>& b) {
    for (int i = 0; i != N; ++i) {
        if (a.data[i] != b.data[i]) return false;
    }
    return true;
}

template <int N>
bool operator != (const Uint32Block<N>& a, const Uint32Block<N>& b) { return !(a == b); }

template <int N>
std::ostream& operator << (std::ostream& out, const Uint32Block<N>& b) {
    out << '[';
    for (int i = 0; i != N; ++i) {
        out << b.data[i] << ' ';
    }
    return out << ']';
}



template <int N>
class Reader {

        Reader()
            :fdata_(0)
            ,buffer_(0)
            ,next_(0)
            ,block_count_(0) { }
    public:
        explicit Reader(std::ifstream& fin)
            :fdata_(&fin)
            ,buffer_(new uint32_t[8192 * N])
            ,next_(0)
            ,block_count_(0)
        { this->read(); }

        Reader(Reader<N>&& other)
            :fdata_(other.fdata_)
            ,buffer_(other.buffer_)
            ,next_(other.next_)
            ,block_count_(other.block_count_) {
                other.fdata_ = nullptr;
                other.buffer_ = nullptr;
                other.next_ = 0;
                other.block_count_ = 0;
        }


        ~Reader() { delete [] buffer_; }

        unsigned buffer_size() const { return 1024 * N; }

        bool eof() const {
            if (!buffer_) return true;
            return block_count_ == 0;
        }

        void read() {
            if (!fdata_) {
                std::cerr << "read() on end() iterator\n";
                throw std::runtime_error("invalid operation on end() iterator");
            }

            fdata_->read((char*)buffer_, buffer_size());
            const unsigned nbytes = fdata_->gcount();
            if (nbytes % (N * sizeof(uint32_t)) != 0) {
                throw std::runtime_error("Bad input file");
            }
            next_ = 0;
            block_count_ = nbytes / (N * sizeof(uint32_t));
        }

        Reader& operator ++ () {
            ++next_;
            if (next_ == block_count_) this->read();
            return *this;
        }

        Uint32Block<N> operator * () const {
            Uint32Block<N> r;
            for (int i = 0; i != N; ++i) {
                r.data[i] = buffer_[next_ * N + i];
            }
            return r;
        }

        Uint32Block<N> next() {
            Uint32Block<N> r = **this;
            ++(*this);
            return r;
        }

    private:
        std::ifstream* fdata_;
        uint32_t* buffer_;
        unsigned next_;
        unsigned block_count_;
};

template<int N>
class ReaderIterator : public std::iterator<std::input_iterator_tag, Uint32Block<N> >{
        ReaderIterator():input_(nullptr) { }
    public:
        explicit ReaderIterator(Reader<N>& input)
            :input_(&input)
            { }
        Uint32Block<N> operator * () const {
            return **input_;
        }

        static ReaderIterator end() { return ReaderIterator(); }
        ReaderIterator& operator ++ () {
            ++(*input_);
            return *this;
        }
        bool operator == (const ReaderIterator& other) const {
            if (other.input_ && this->input_) return false;
            if (!other.input_) { return this->eof(); }
            return other.eof();
        }
        bool operator != (const ReaderIterator& other) const { return !(*this == other); }
        bool eof() const {
            return !input_ || input_->eof();
        }
    private:
        Reader<N>* input_;
};

template <int N>
class Writer : public std::iterator<std::output_iterator_tag, Uint32Block<N> > {
    public:
        explicit Writer(std::ofstream& dest)
            :fdata_(dest)
        { }

        void operator = (const Uint32Block<N>& v) {
            fdata_.write(reinterpret_cast<const char*>(v.data), sizeof(v.data));
        }

        const Writer<N>& operator * () const {
            return *this;
        }
        Writer<N>& operator * () {
            return *this;
        }

        Writer<N>& operator ++ () {
            return *this;
        }

    private:
        std::ofstream& fdata_;
};

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Bad args\n";
        return 1;
    }
    if (std::string(argv[1]) == "sort") {
        if (argc < 4) {
            std::cerr << "Bad args\n";
            return 1;
        }
        std::ifstream fin(argv[2]);
        Reader<BlockSize> rfin(fin);
        std::vector<Uint32Block<BlockSize> > data(ReaderIterator<BlockSize>(rfin), ReaderIterator<BlockSize>::end());
        std::cerr << "Read in " << data.size() << " blocks\n";
        std::sort(data.begin(), data.end());
        data.erase(
                std::unique(data.begin(), data.end()),
                data.end());
        std::ofstream fout(argv[3]);
        std::copy(data.begin(), data.end(), Writer<BlockSize>(fout));
    } else if (std::string(argv[1]) == "merge") {
        if (argc < 5) {
            std::cerr << "Bad args\n";
            return 1;
        }
        std::ifstream fin1(argv[2]);
        Reader<BlockSize> rfin1(fin1);
        std::ifstream fin2(argv[3]);
        Reader<BlockSize> rfin2(fin2);
        std::ofstream fout(argv[4]);
        std::merge(
                ReaderIterator<BlockSize>(rfin1), ReaderIterator<BlockSize>::end(),
                ReaderIterator<BlockSize>(rfin2), ReaderIterator<BlockSize>::end(),
                Writer<BlockSize>(fout));
    } else if (std::string(argv[1]) == "mergemany") {
        if (argc < 5) {
            std::cerr << "Bad args\n";
            return 1;
        }
        std::vector<std::ifstream*> fins;
        std::vector<Reader<BlockSize>*> rfins;
        for (int i = 2; i != argc-1; ++i) {
            fins.push_back(new std::ifstream(argv[i], std::ios::in | std::ios::binary));
            rfins.push_back(new Reader<BlockSize>(*fins.back()));
        }
        std::ofstream fout(argv[argc-1], std::ios::out | std::ios::binary);
        Writer<BlockSize> rout(fout);

        typedef std::pair<Uint32Block<BlockSize>, int> block_ix;
        std::priority_queue<block_ix, std::vector<block_ix>, std::greater<block_ix> > q;
        for (unsigned int i = 0; i != rfins.size(); ++i) {
            if (!rfins[i]->eof()) {
                q.push(std::make_pair(rfins[i]->next(), i));
                if (rfins[i]->eof()) {
                    delete rfins[i];
                    rfins[i] = nullptr;
                }
            } else {
                std::cerr << "Source " << i << " is completely empty.\n";
            }
        }
        Uint32Block<BlockSize> prev;
        prev.data[0] = -1;
        while (!q.empty()) {
            block_ix next = q.top();
            q.pop();
            if (next.first != prev) {
                *rout = next.first;
                prev = next.first;
            }
            if (rfins[next.second]) {
                if (q.empty()) {
                    while (!rfins[next.second]->eof()) {
                        *rout = rfins[next.second]->next();
                    }
                } else {
                    while (true) {
                        if (rfins[next.second]->eof()) {
                            delete rfins[next.second];
                            rfins[next.second] = nullptr;
                            break;
                        }
                        Uint32Block<BlockSize> next_next = rfins[next.second]->next();
                        if (next_next == prev) {
                            continue;
                        } else if (next_next < q.top().first) {
                            *rout = next_next;
                            prev = next_next;
                        } else {
                            q.push(std::make_pair(next_next, next.second));
                            break;
                        }
                    }
                }
            }
        }
        for (unsigned int i = 0; i != fins.size(); ++i) {
            delete rfins[i];
            delete fins[i];
        }
    } else {
        std::cerr << "Expected first argument to be 'sort' or 'merge'" << std::endl;
        return 1;
    }
    return 0;
}

