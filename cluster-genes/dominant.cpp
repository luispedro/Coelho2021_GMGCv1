#include <algorithm>
#include <queue>
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <assert.h>


typedef std::vector<int> renumber_t;
typedef std::vector<int>::size_type index_t;

struct Int1dSlice {
    typedef std::vector<int>::const_iterator base_iterator;

    Int1dSlice(base_iterator start, base_iterator end)
        :first_(start)
        ,past_(end)
    { }

    base_iterator begin() const { return first_; }
    base_iterator end() const { return past_; }

    base_iterator first_;
    base_iterator past_;
};

struct Int2d {
    Int2d() { }
    Int2d(Int2d&& other) {
        indices_.swap(other.indices_);
        data_.swap(other.data_);
    }
    static Int2d make_int2d(std::vector<index_t>& indices, std::vector<int>& data) {
        Int2d r;
        r.indices_.swap(indices);
        r.data_.swap(data);
        return r;
    }
    std::vector<int>::const_iterator start_of(index_t ix) const { return data_.begin() + (ix > 0 ? indices_[ix - 1] : 0); }
    std::vector<int>::const_iterator end_of(index_t ix) const { return start_of(ix + 1); }
    Int1dSlice neighbors(index_t ix) const { return Int1dSlice(start_of(ix), end_of(ix)); }

    index_t size() const { return data_.size(); }

    std::vector<index_t> indices_;
    std::vector<int> data_;

};


inline
int lazy_vertex_add(std::vector<int>& vertex_renumber, std::vector<int>& cur, int orig) {
    //std::cerr << "lazy_vertex_add(" << orig << ")\n";
    if (vertex_renumber[orig] != -1) return vertex_renumber[orig];
    int n = cur.size();
    vertex_renumber[orig] = n;
    cur.push_back(orig);
    return n;
}


struct DiGraphBuilderRenamer {
    DiGraphBuilderRenamer()
        : prev_(-1)
        { }
    void add_edge(unsigned int v0, unsigned int v1) {
        if (int(v0) != prev_) {
            assert(int(v0) == (prev_+1));
            if (!data_.empty()) {
                indices_.push_back(data_.size());
            }
            prev_ = v0;
        }
        data_.push_back(v1);
    }

    Int2d finish(std::vector<int>& renamer, std::vector<int>& rmap) {
        indices_.push_back(data_.size());
        for (index_t i = 0; i != data_.size(); ++i) {
            data_[i] = lazy_vertex_add(renamer, rmap, data_[i]);
        }
        return Int2d::make_int2d(indices_, data_);
    }

    void clear() {
        prev_ = -1;
        indices_.clear();
        data_.clear();
    }
    bool empty() const { return data_.empty(); }

    int prev_;
    std::vector<index_t> indices_;
    std::vector<int> data_;
};


struct DiGraph {
    DiGraph(DiGraphBuilderRenamer& builder, std::vector<int>& renamer, std::vector<int>& rmap)
        :edges_(builder.finish(renamer, rmap))
        { nv_ = rmap.size(); }
    unsigned n_vertices() const { return nv_; }
    unsigned n_edges() const { return edges_.data_.size(); }

    Int1dSlice neighbors(index_t ix) const {
        if (ix < edges_.indices_.size()) return edges_.neighbors(ix);
        return Int1dSlice(edges_.data_.begin(), edges_.data_.begin());
    }

    unsigned  nv_;
    Int2d edges_;
};


typedef std::vector<int> vertex_set;
typedef int dset_value;

struct VertexWeight {
    VertexWeight(unsigned v, int w)
        :vertex_(v)
        ,weight_(w)
    { }

    bool operator < (const VertexWeight& other) const {
        if (weight_ == other.weight_) return vertex_ > other.vertex_;
        return weight_ < other.weight_;
    }
    unsigned vertex_;
    int weight_;
};

index_t weight(const DiGraph& g, const std::vector<bool>& covered, index_t i) {
    index_t r = 0;
    if (!covered[i]) ++r;
    for (auto j : g.neighbors(i)) {
        if (!covered[j]) ++r;
    }
    //std::cerr << "weight(" << i << "): " << r << '\n';
    return r;
}
int extract_next(const DiGraph& g, const std::vector<bool>& covered, std::priority_queue<VertexWeight>& max_bound, bool noise) {
    if (max_bound.size() == 1) {
        while (!max_bound.empty()) {
            VertexWeight n = max_bound.top();
            max_bound.pop();
            if (!covered[n.vertex_]) return n.vertex_;
        }
        return -1;
    }
    VertexWeight next = max_bound.top();
    max_bound.pop();
    const int new_weight = weight(g, covered, next.vertex_);
    if (new_weight == next.weight_) {
        return next.vertex_;
    }

    next.weight_ = new_weight;
    std::vector<VertexWeight> candidates;
    candidates.push_back(next);

    VertexWeight best = next;
    while (!max_bound.empty() && best < max_bound.top()) {
        next = max_bound.top();
        max_bound.pop();
        next.weight_ = weight(g, covered, next.vertex_);
        if (best < next) best = next;
        candidates.push_back(next);
    }
    for (auto first : candidates) {
        if (first.vertex_ != best.vertex_ && first.weight_ > 0) {
            max_bound.push(first);
        }
    }
    return best.vertex_;
}


vertex_set greedy_find_dset_queue(const DiGraph& g) {
    vertex_set r;
    std::priority_queue<VertexWeight> max_bound;

    index_t uncovered = g.n_vertices();
    std::vector<bool> covered(g.n_vertices(), true);
    for (index_t i = 0; i < g.n_vertices(); ++i) {
        for (auto n : g.neighbors(i)) {
            covered.at(n) = false;
        }
    }

    for (index_t i = 0; i < g.n_vertices(); ++i) {
        if (covered.at(i)) {
            r.push_back(i);
            --uncovered;
        } else {
            max_bound.push(VertexWeight(i, weight(g, covered, i)));
        }
    }
    for (auto vertex : r) {
        for (auto n : g.neighbors(vertex)) {
            if (!covered.at(n)) {
                covered[n] = true;
                --uncovered;
            }
        }
    }
    while (uncovered) {
        index_t vid = extract_next(g, covered, max_bound, true);
        assert(vid != index_t(-1));
        r.push_back(vid);

        if (!covered.at(vid)) {
            covered[vid] = true;
            --uncovered;
        }
        for (auto n : g.neighbors(vid)) {
            if (!covered.at(n)) {
                covered[n] = true;
                --uncovered;
            }
        }
    }
    return r;
}

vertex_set greedy_find_dset_brute(const DiGraph& g) {
    vertex_set r;
    std::vector<int> weight(g.n_vertices());
    std::vector<int> used(g.n_vertices());
    std::vector<bool> covered(g.n_vertices());
    index_t uncovered = g.n_vertices();
     while (1) {
        // #pragma omp parallel for
        for (index_t i = 0; i < g.n_vertices(); ++i) {
            if (used[i]) continue;
            if (!covered[i]) ++weight[i];
            for (auto c: g.neighbors(i)) {
                if (!covered[c]) ++weight[i];
            }
        }
        auto next = max_element(weight.begin(), weight.end());
        index_t vid = next - weight.begin();
        r.push_back(vid);
        used[vid] = true;

        if (!covered[vid]) {
            covered[vid] = true;
            --uncovered;
        }
        for (auto n : g.neighbors(vid)) {
            if (!covered[n]) {
                covered[n] = true;
                --uncovered;
            }
        }
        if (!uncovered) return r;

        std::fill(weight.begin(), weight.end(), 0);
    }
}
vertex_set greedy_find_dset(const DiGraph& g) {
    if (g.n_vertices() > 10) return greedy_find_dset_queue(g);
    else return greedy_find_dset_brute(g);
}


void dominant(DiGraphBuilderRenamer& gb, renumber_t& vertex_renumber, std::vector<int>& rmap) {
    //std::cerr << "Handling graph of size " << gb.nv_  << "/" << gb.edges_.size() << '\n';
    if (gb.data_.size() == 1) {
        std::cout << rmap.at(0) << '\n';
        return;
    }

    DiGraph g(gb, vertex_renumber, rmap);
    if (g.n_vertices()  > 10000) {
        std::cerr << "Built Graph: " << g.n_vertices()  << "/" << g.n_edges() << ".\n";
    }
    //std::cerr << "Built Graph: " << g.n_vertices()  << "/" << g.n_edges() << ".\n";
    vertex_set r = greedy_find_dset(g);
    for (auto v : r) {
        std::cout << rmap.at(v) << '\n';
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage : " << argv[0] << " TRIPLETS-FILE" << std::endl;
        return 1;
    }
    int active = -1;
    uint32_t buffer[4096 * 3];
    DiGraphBuilderRenamer gb;

    long long vi = 0;
    int next = 100000000;
    renumber_t vertex_renumber;

    std::vector<int> rmap;
    std::ifstream fdata(argv[1]);
    while (!fdata.eof()) {
        void* bufferp = buffer;
        fdata.read((char*)bufferp, sizeof(buffer));
        const unsigned nbytes = fdata.gcount();
        if (nbytes % (3 * sizeof(buffer[0])) != 0) {
            std::cerr << "Bad input file " << argv[1]
                        << " size is incorrect (read " << nbytes << " Bytes; not multiple of " << (3*sizeof(buffer[0])) << ")." << std::endl;
            return 2;
        }

        const int n_triplets = nbytes/(3 * sizeof(buffer[0]));
        if (!n_triplets) break;
        for (int i = 0; i < n_triplets; ++i) {
            ++vi;
            --next;
            if (!next) {
                std::cerr << "Loaded " << vi/1000000000. << "g entries.\n";
                next = 100000000;
            }
            const int group = buffer[i*3 + 0];
            const int v0 = buffer[i*3 + 1];
            const int v1 = buffer[i*3 + 2];
            //std::cerr << "group: " << group << '\n';
            if (group != active) {
                if (active != -1) {
                    dominant(gb, vertex_renumber, rmap);
                }
                gb.clear();
                rmap.clear();
                active = group;
            }

            if (unsigned(v0) >= vertex_renumber.size() || unsigned(v1) >= vertex_renumber.size()) {
                unsigned new_size = 1 + (v0 > v1 ? v0 : v1);
                vertex_renumber.resize(new_size, -1);
            }
            const int e0 = lazy_vertex_add(vertex_renumber, rmap, v0);
            gb.add_edge(e0, v1);
        }
    }
    if (!rmap.empty()) {
        dominant(gb, vertex_renumber, rmap);
    }
    return 0;
}

