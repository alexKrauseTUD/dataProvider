#pragma once

#include <Column.h>

#include <vector>

class Operators {
   public:
    template <bool isFirst = false>
    static inline std::vector<size_t> less_than(uint64_t* data, const uint64_t predicate, const size_t blockSize, const std::vector<size_t> in_pos) {
        std::vector<size_t> out_vec;
        out_vec.reserve(blockSize);
        if (isFirst) {
            for (auto e = 0; e < blockSize; ++e) {
                if (data[e] < predicate) {
                    out_vec.push_back(e);
                }
            }
        } else {
            for (auto e : in_pos) {
                if (data[e] < predicate) {
                    out_vec.push_back(e);
                }
            }
        }

        return out_vec;
    }

    template <bool isFirst = false>
    static inline std::vector<size_t> less_equal(uint64_t* data, const uint64_t predicate, const size_t blockSize, const std::vector<size_t> in_pos) {
        std::vector<size_t> out_vec;
        out_vec.reserve(blockSize);
        if (isFirst) {
            for (auto e = 0; e < blockSize; ++e) {
                if (data[e] <= predicate) {
                    out_vec.push_back(e);
                }
            }
        } else {
            for (auto e : in_pos) {
                if (data[e] <= predicate) {
                    out_vec.push_back(e);
                }
            }
        }

        return out_vec;
    }

    template <bool isFirst = false>
    static inline std::vector<size_t> greater_than(uint64_t* data, const uint64_t predicate, const size_t blockSize, const std::vector<size_t> in_pos) {
        std::vector<size_t> out_vec;
        out_vec.reserve(blockSize);
        if (isFirst) {
            for (auto e = 0; e < blockSize; ++e) {
                if (data[e] > predicate) {
                    out_vec.push_back(e);
                }
            }
        } else {
            for (auto e : in_pos) {
                if (data[e] > predicate) {
                    out_vec.push_back(e);
                }
            }
        }

        return out_vec;
    }

    template <bool isFirst = false>
    static inline std::vector<size_t> greater_equal(uint64_t* data, const uint64_t predicate, const size_t blockSize, const std::vector<size_t> in_pos) {
        std::vector<size_t> out_vec;
        out_vec.reserve(blockSize);
        if (isFirst) {
            for (auto e = 0; e < blockSize; ++e) {
                if (data[e] >= predicate) {
                    out_vec.push_back(e);
                }
            }
        } else {
            for (auto e : in_pos) {
                if (data[e] >= predicate) {
                    out_vec.push_back(e);
                }
            }
        }

        return out_vec;
    }

    template <bool isFirst = false>
    static inline std::vector<size_t> equal(uint64_t* data, const uint64_t predicate, const size_t blockSize, const std::vector<size_t> in_pos) {
        std::vector<size_t> out_vec;
        out_vec.reserve(blockSize);
        if (isFirst) {
            for (auto e = 0; e < blockSize; ++e) {
                if (data[e] == predicate) {
                    out_vec.push_back(e);
                }
            }
        } else {
            for (auto e : in_pos) {
                if (data[e] == predicate) {
                    out_vec.push_back(e);
                }
            }
        }

        return out_vec;
    }

    template <bool isFirst = false>
    static inline std::vector<size_t> between_incl(uint64_t* data, const uint64_t predicate_1, const uint64_t predicate_2, const size_t blockSize, const std::vector<size_t> in_pos) {
        std::vector<size_t> out_vec;
        out_vec.reserve(blockSize);
        if (isFirst) {
            for (auto e = 0; e < blockSize; ++e) {
                if (predicate_1 <= data[e] && data[e] <= predicate_2) {
                    out_vec.push_back(e);
                }
            }
        } else {
            for (auto e : in_pos) {
                if (predicate_1 <= data[e] && data[e] <= predicate_2) {
                    out_vec.push_back(e);
                }
            }
        }

        return out_vec;
    }

    template <bool isFirst = false>
    static inline std::vector<size_t> between_excl(uint64_t* data, const uint64_t predicate_1, const uint64_t predicate_2, const size_t blockSize, const std::vector<size_t> in_pos) {
        std::vector<size_t> out_vec;
        out_vec.reserve(blockSize);
        if (isFirst) {
            for (auto e = 0; e < blockSize; ++e) {
                if (predicate_1 < data[e] && data[e] < predicate_2) {
                    out_vec.push_back(e);
                }
            }
        } else {
            for (auto e : in_pos) {
                if (predicate_1 < data[e] && data[e] < predicate_2) {
                    out_vec.push_back(e);
                }
            }
        }

        return out_vec;
    }
};
