#include <Column.h>
#include <DataCatalog.h>
#include <Queries.h>
#include <omp.h>

#include <future>

inline void wait_col_data_ready(col_t* _col, char* _data) {
    std::unique_lock<std::mutex> lk(_col->iteratorLock);
    if (!(_data < static_cast<char*>(_col->current_end))) {
        _col->iterator_data_available.wait(lk, [_col, _data] { return reinterpret_cast<uint64_t*>(_data) < static_cast<uint64_t*>(_col->current_end); });
    }
};

template <bool remote, bool chunked, bool paxed, bool isFirst = false>
inline std::vector<size_t> less_than(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (reload) {
            if (chunked && !paxed) {
                column->request_data(!chunked);
            }
        }
    }

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
};

template <bool remote, bool chunked, bool paxed, bool isFirst = false>
inline std::vector<size_t> less_equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (reload) {
            if (chunked && !paxed) {
                column->request_data(!chunked);
            }
        }
    }

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
};

template <bool remote, bool chunked, bool paxed, bool isFirst = false>
inline std::vector<size_t> greater_than(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (reload) {
            if (chunked && !paxed) {
                column->request_data(!chunked);
            }
        }
    }

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
};

template <bool remote, bool chunked, bool paxed, bool isFirst = false>
inline std::vector<size_t> greater_equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (reload) {
            if (chunked && !paxed) {
                column->request_data(!chunked);
            }
        }
    }

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
};

template <bool remote, bool chunked, bool paxed, bool isFirst = false>
inline std::vector<size_t> equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (reload) {
            if (chunked && !paxed) {
                column->request_data(!chunked);
            }
        }
    }

    auto s_ts = std::chrono::high_resolution_clock::now();
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
};

template <bool remote, bool chunked, bool paxed, bool isFirst = false>
inline std::vector<size_t> between_incl(col_t* column, const uint64_t predicate_1, const uint64_t predicate_2, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (reload) {
            if (chunked && !paxed) {
                column->request_data(!chunked);
            }
        }
    }

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
};

template <bool remote, bool chunked, bool paxed, bool isFirst = false>
inline std::vector<size_t> between_excl(col_t* column, const uint64_t predicate_1, const uint64_t predicate_2, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (reload) {
            if (chunked && !paxed) {
                column->request_data(!chunked);
            }
        }
    }

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
};

template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t pipeTempOne(col_t* column1, col_t* column2, col_t* column3, const uint64_t predicate, const std::vector<std::string> idents) {
    size_t OPTIMAL_BLOCK_SIZE_MT = 262144;

    if (remote && !prefetching) {
        if (paxed) {
            DataCatalog::getInstance().fetchPseudoPax(1, idents);
        } else {
            column1->request_data(!chunked);
            column2->request_data(!chunked);
            column3->request_data(!chunked);
        }
    }

    const size_t columnSize = column1->size;

    size_t max_elems_per_chunk = 0;
    size_t standard_block_elements = 0;
    if (paxed) {
        size_t total_id_len = 0;
        for (auto& id : idents) {
            total_id_len += id.size();
        }

        const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
        const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(1)->maxBytesInPayload(appMetaSize);

        max_elems_per_chunk = ((maximumPayloadSize / idents.size()) / (sizeof(uint64_t) * 4)) * 4;
        standard_block_elements = max_elems_per_chunk;
    } else if (!(remote && (chunked || paxed))) {
        max_elems_per_chunk = columnSize;
        standard_block_elements = OPTIMAL_BLOCK_SIZE_MT / sizeof(uint64_t);
    } else {
        max_elems_per_chunk = DataCatalog::getInstance().dataCatalog_chunkMaxSize / sizeof(uint64_t);
        if (max_elems_per_chunk <= OPTIMAL_BLOCK_SIZE_MT / sizeof(uint64_t)) {
            standard_block_elements = max_elems_per_chunk;
        } else {
            standard_block_elements = OPTIMAL_BLOCK_SIZE_MT / sizeof(uint64_t);
        }
    }

    standard_block_elements /= 4;

    size_t num_blocks = (columnSize / standard_block_elements) + (columnSize % standard_block_elements == 0 ? 0 : 1);
    std::array<uint64_t, 4> sums{0, 0, 0, 0};

#pragma omp parallel for schedule(static, 1) num_threads(4)
    for (size_t i = 0; i < num_blocks; ++i) {
        size_t baseOffset = i * standard_block_elements;
        size_t currentBlockElems = standard_block_elements;

        auto data_2 = reinterpret_cast<uint64_t*>(column2->data) + baseOffset;
        auto data_3 = reinterpret_cast<uint64_t*>(column3->data) + baseOffset;

        bool reloading = baseOffset % max_elems_per_chunk == 0;

        if (remote && paxed) {
            wait_col_data_ready(column3, reinterpret_cast<char*>(data_3));
            if (reloading) DataCatalog::getInstance().fetchPseudoPax(1, idents);
        }

        const size_t elem_diff = columnSize - baseOffset;
        if (elem_diff < currentBlockElems) {
            currentBlockElems = elem_diff;
        }

        auto le_idx = less_than<remote, chunked, paxed, true>(column1, predicate, baseOffset, currentBlockElems, {}, reloading);

        if (remote && !paxed) {
            wait_col_data_ready(column2, reinterpret_cast<char*>(data_2));
            wait_col_data_ready(column3, reinterpret_cast<char*>(data_3));
            if (reloading) {
                if (chunked) {
                    column2->request_data(!chunked);
                    column3->request_data(!chunked);
                }
            }
        }

        int tid = omp_get_thread_num();

        for (auto idx : le_idx) {
            sums[tid] += (data_2[idx] * data_3[idx]);
            // ++sum;
        }
    }

    uint64_t sum = 0;

    for (auto s : sums) {
        sum += s;
    }

    return sum;
}

template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t pipeTempTwo(col_t* column1, col_t* column2, col_t* column3, const uint64_t predicate, const std::vector<std::string> idents) {
    size_t OPTIMAL_BLOCK_SIZE_MT = 131072;

    if (remote && !prefetching) {
        if (paxed) {
            DataCatalog::getInstance().fetchPseudoPax(1, idents);
        } else {
            column1->request_data(!chunked);
            column2->request_data(!chunked);
            column3->request_data(!chunked);
        }
    }

    const size_t columnSize = column1->size;

    size_t max_elems_per_chunk = 0;
    size_t standard_block_elements = 0;
    if (paxed) {
        size_t total_id_len = 0;
        for (auto& id : idents) {
            total_id_len += id.size();
        }

        const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
        const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(1)->maxBytesInPayload(appMetaSize);

        max_elems_per_chunk = ((maximumPayloadSize / idents.size()) / (sizeof(uint64_t) * 4)) * 4;
        standard_block_elements = max_elems_per_chunk;
    } else if (!(remote && (chunked || paxed))) {
        max_elems_per_chunk = columnSize;
        standard_block_elements = OPTIMAL_BLOCK_SIZE_MT / sizeof(uint64_t);
    } else {
        max_elems_per_chunk = DataCatalog::getInstance().dataCatalog_chunkMaxSize / sizeof(uint64_t);
        if (max_elems_per_chunk <= OPTIMAL_BLOCK_SIZE_MT / sizeof(uint64_t)) {
            standard_block_elements = max_elems_per_chunk;
        } else {
            standard_block_elements = OPTIMAL_BLOCK_SIZE_MT / sizeof(uint64_t);
        }
    }

    standard_block_elements /= 2;

    size_t num_blocks = (columnSize / standard_block_elements) + (columnSize % standard_block_elements == 0 ? 0 : 1);
    std::array<uint64_t, 2> sums{0, 0};

#pragma omp parallel for schedule(static, 1) num_threads(2)
    for (size_t i = 0; i < num_blocks; ++i) {
        size_t baseOffset = i * standard_block_elements;
        size_t currentBlockElems = standard_block_elements;

        auto data_2 = reinterpret_cast<uint64_t*>(column2->data) + baseOffset;
        auto data_3 = reinterpret_cast<uint64_t*>(column3->data) + baseOffset;

        bool reloading = baseOffset % max_elems_per_chunk == 0;

        if (remote && paxed) {
            wait_col_data_ready(column3, reinterpret_cast<char*>(data_3));
            if (reloading) DataCatalog::getInstance().fetchPseudoPax(1, idents);
        }

        const size_t elem_diff = columnSize - baseOffset;
        if (elem_diff < currentBlockElems) {
            currentBlockElems = elem_diff;
        }

        auto le_idx = less_than<remote, chunked, paxed, true>(column1, predicate, baseOffset, currentBlockElems, {}, reloading);

        if (remote && !paxed) {
            wait_col_data_ready(column2, reinterpret_cast<char*>(data_2));
            wait_col_data_ready(column3, reinterpret_cast<char*>(data_3));
            if (reloading) {
                if (chunked) {
                    column2->request_data(!chunked);
                    column3->request_data(!chunked);
                }
            }
        }

        int tid = omp_get_thread_num();

        for (auto idx : le_idx) {
            sums[tid] += (data_2[idx] * data_3[idx]);
            // ++sum;
        }
    }

    uint64_t sum = 0;

    for (auto s : sums) {
        sum += s;
    }

    return sum;
}

template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t pipeTempThree(col_t* column1, col_t* column2, col_t* column3, const uint64_t predicate, const std::vector<std::string> idents) {
    size_t OPTIMAL_BLOCK_SIZE_MT = 65536;

    if (remote && !prefetching) {
        if (paxed) {
            DataCatalog::getInstance().fetchPseudoPax(1, idents);
        } else {
            column1->request_data(!chunked);
            column2->request_data(!chunked);
            column3->request_data(!chunked);
        }
    }

    const size_t columnSize = column1->size;

    size_t max_elems_per_chunk = 0;
    size_t standard_block_elements = 0;
    if (paxed) {
        size_t total_id_len = 0;
        for (auto& id : idents) {
            total_id_len += id.size();
        }

        const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
        const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(1)->maxBytesInPayload(appMetaSize);

        max_elems_per_chunk = ((maximumPayloadSize / idents.size()) / (sizeof(uint64_t) * 4)) * 4;
        standard_block_elements = max_elems_per_chunk;
    } else if (!(remote && (chunked || paxed))) {
        max_elems_per_chunk = columnSize;
        standard_block_elements = OPTIMAL_BLOCK_SIZE_MT / sizeof(uint64_t);
    } else {
        max_elems_per_chunk = DataCatalog::getInstance().dataCatalog_chunkMaxSize / sizeof(uint64_t);
        if (max_elems_per_chunk <= OPTIMAL_BLOCK_SIZE_MT / sizeof(uint64_t)) {
            standard_block_elements = max_elems_per_chunk;
        } else {
            standard_block_elements = OPTIMAL_BLOCK_SIZE_MT / sizeof(uint64_t);
        }
    }

    uint64_t sum = 0;

    size_t num_blocks = (columnSize / standard_block_elements) + (columnSize % standard_block_elements == 0 ? 0 : 1);

    for (size_t i = 0; i < num_blocks; ++i) {
        size_t baseOffset = i * standard_block_elements;
        size_t currentBlockElems = standard_block_elements;

        auto data_2 = reinterpret_cast<uint64_t*>(column2->data) + baseOffset;
        auto data_3 = reinterpret_cast<uint64_t*>(column3->data) + baseOffset;

        bool reloading = baseOffset % max_elems_per_chunk == 0;

        if (remote && paxed) {
            wait_col_data_ready(column3, reinterpret_cast<char*>(data_3));
            if (reloading) DataCatalog::getInstance().fetchPseudoPax(1, idents);
        }

        const size_t elem_diff = columnSize - baseOffset;
        if (elem_diff < currentBlockElems) {
            currentBlockElems = elem_diff;
        }

        auto le_idx = less_than<remote, chunked, paxed, true>(column1, predicate, baseOffset, currentBlockElems, {}, reloading);

        if (remote && !paxed) {
            wait_col_data_ready(column2, reinterpret_cast<char*>(data_2));
            wait_col_data_ready(column3, reinterpret_cast<char*>(data_3));
            if (reloading) {
                if (chunked) {
                    column2->request_data(!chunked);
                    column3->request_data(!chunked);
                }
            }
        }

        int tid = omp_get_thread_num();

        for (auto idx : le_idx) {
            sum += (data_2[idx] * data_3[idx]);
            // ++sum;
        }
    }

    return sum;
}

// 4 Pipelines with 4 Threads no parallel execution -> 1 Pipeline executed by 4 Threads
template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t orchBenchmark1(const std::vector<std::string> idents, const std::array<std::array<uint8_t, 3>, 4> idx) {
    std::vector<col_t*> columns;
    const std::array predicates{50, 75, 25, 100};
    std::array<uint64_t, 4> sums;
    uint64_t sum = 0;

    for (auto ident : idents) {
        col_t* col;
        if (remote) {
            col = DataCatalog::getInstance().find_remote(ident);
            if (prefetching && !paxed) col->request_data(!chunked);
        } else {
            col = DataCatalog::getInstance().find_local(ident);
        }

        columns.push_back(col);
    }

    if (paxed && prefetching) {
        DataCatalog::getInstance().fetchPseudoPax(1, idents);
    }

    for (size_t i = 0; i < 4; ++i) {
        if (prefetching) {
            sums[i] = pipeTempOne<remote, chunked, paxed, prefetching>(columns[idx[i][0]], columns[idx[i][1]], columns[idx[i][2]], predicates[i], idents);
        } else {
            sums[i] = pipeTempOne<remote, chunked, paxed, prefetching>(columns[idx[i][0]], columns[idx[i][1]], columns[idx[i][2]], predicates[i], {idents[idx[i][0]], idents[idx[i][1]], idents[idx[i][2]]});
        }
    }

    for (auto s : sums) {
        sum += s;
    }

    return sum;
}

// 4 Pipelines with 4 Threads half parallel execution -> 1 Pipeline executed by 2 Threads
template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t orchBenchmark2(const std::vector<std::string> idents, const std::array<std::array<uint8_t, 3>, 4> idx) {
    std::vector<col_t*> columns;
    const std::array predicates{50, 25, 75, 100};
    std::array<uint64_t, 4> sums;
    uint64_t sum = 0;

    for (auto ident : idents) {
        col_t* col;
        if (remote) {
            col = DataCatalog::getInstance().find_remote(ident);
            if (prefetching && !paxed) col->request_data(!chunked);
        } else {
            col = DataCatalog::getInstance().find_local(ident);
        }

        columns.push_back(col);
    }

    if (paxed && prefetching) {
        DataCatalog::getInstance().fetchPseudoPax(1, idents);
    }

#pragma omp parallel for schedule(static, 2) num_threads(2)
    for (size_t i = 0; i < 4; ++i) {
        if (prefetching) {
            sums[i] = pipeTempTwo<remote, chunked, paxed, prefetching>(columns[idx[i][0]], columns[idx[i][1]], columns[idx[i][2]], predicates[i], idents);
        } else {
            sums[i] = pipeTempTwo<remote, chunked, paxed, prefetching>(columns[idx[i][0]], columns[idx[i][1]], columns[idx[i][2]], predicates[i], {idents[idx[i][0]], idents[idx[i][1]], idents[idx[i][2]]});
        }
    }

    for (auto s : sums) {
        sum += s;
    }

    return sum;
}

// 4 Pipelines with 4 Threads full parallel execution
template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t orchBenchmark3(const std::vector<std::string> idents, const std::array<std::array<uint8_t, 3>, 4> idx) {
    std::vector<col_t*> columns;
    const std::array predicates{50, 25, 75, 100};
    std::array<uint64_t, 4> sums;
    uint64_t sum = 0;

    for (auto ident : idents) {
        col_t* col;
        if (remote) {
            col = DataCatalog::getInstance().find_remote(ident);
            if (prefetching && !paxed) col->request_data(!chunked);
        } else {
            col = DataCatalog::getInstance().find_local(ident);
        }

        columns.push_back(col);
    }

    if (paxed && prefetching) {
        DataCatalog::getInstance().fetchPseudoPax(1, idents);
    }

#pragma omp parallel for schedule(static, 1) num_threads(4)
    for (size_t i = 0; i < 4; ++i) {
        if (prefetching) {
            sums[i] = pipeTempThree<remote, chunked, paxed, prefetching>(columns[idx[i][0]], columns[idx[i][1]], columns[idx[i][2]], predicates[i], idents);
        } else {
            sums[i] = pipeTempThree<remote, chunked, paxed, prefetching>(columns[idx[i][0]], columns[idx[i][1]], columns[idx[i][2]], predicates[i], {idents[idx[i][0]], idents[idx[i][1]], idents[idx[i][2]]});
        }
    }

    for (auto s : sums) {
        sum += s;
    }

    return sum;
}

// 4 Pipelines with 1 Thread sequentially executed -> Baseline
template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t orchBenchmark4(const std::vector<std::string> idents, const std::array<std::array<uint8_t, 3>, 4> idx) {
    std::vector<col_t*> columns;
    const std::array predicates{50, 25, 75, 100};
    std::array<uint64_t, 4> sums;
    uint64_t sum = 0;

    for (auto ident : idents) {
        col_t* col;
        if (remote) {
            col = DataCatalog::getInstance().find_remote(ident);
            if (prefetching && !paxed) col->request_data(!chunked);
        } else {
            col = DataCatalog::getInstance().find_local(ident);
        }

        columns.push_back(col);
    }

    if (paxed && prefetching) {
        DataCatalog::getInstance().fetchPseudoPax(1, idents);
    }

    for (size_t i = 0; i < 4; ++i) {
        if (prefetching) {
            sums[i] = pipeTempThree<remote, chunked, paxed, prefetching>(columns[idx[i][0]], columns[idx[i][1]], columns[idx[i][2]], predicates[i], idents);
        } else {
            sums[i] = pipeTempThree<remote, chunked, paxed, prefetching>(columns[idx[i][0]], columns[idx[i][1]], columns[idx[i][2]], predicates[i], {idents[idx[i][0]], idents[idx[i][1]], idents[idx[i][2]]});
        }
    }

    for (auto s : sums) {
        sum += s;
    }

    return sum;
}

template <typename Fn>
void doBenchmark(Fn&& f1, Fn&& f2, Fn&& f3, Fn&& f4, Fn&& f5, Fn&& f6, std::ofstream& out, const std::string benchIdent, const std::string overlapIdent) {
    uint64_t sum = 0;
    std::chrono::time_point<std::chrono::high_resolution_clock> s_ts;
    std::chrono::time_point<std::chrono::high_resolution_clock> e_ts;
    std::chrono::duration<double> secs;

    for (size_t i = 0; i < 5; ++i) {
        DataCatalog::getInstance().fetchRemoteInfo();
        s_ts = std::chrono::high_resolution_clock::now();
        sum = f1();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\tPipeline\t65536\t" << benchIdent << "\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Remote\tFull\tPipeline\t65536\t" << benchIdent << "\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        DataCatalog::getInstance().fetchRemoteInfo();
        s_ts = std::chrono::high_resolution_clock::now();
        sum = f2();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\tPrefetch\t65536\t" << benchIdent << "\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Remote\tFull\tPrefetch\t65536\t" << benchIdent << "\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        for (uint64_t chunkSize = 1ull << 19; chunkSize <= 1ull << 28; chunkSize <<= 1) {
            DataCatalog::getInstance().reconfigureChunkSize(chunkSize, chunkSize);

            DataCatalog::getInstance().fetchRemoteInfo();
            s_ts = std::chrono::high_resolution_clock::now();
            sum = f3();
            e_ts = std::chrono::high_resolution_clock::now();

            secs = e_ts - s_ts;

            out << "Remote\tChunked\tPipeline\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << benchIdent << "\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl
                << std::flush;
            std::cout << "Remote\tChunked\tPipeline\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << benchIdent << "\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl;

            DataCatalog::getInstance().eraseAllRemoteColumns();

            DataCatalog::getInstance().fetchRemoteInfo();
            s_ts = std::chrono::high_resolution_clock::now();
            sum = f4();
            e_ts = std::chrono::high_resolution_clock::now();

            secs = e_ts - s_ts;

            out << "Remote\tChunked\tPrefetch\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << benchIdent << "\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl
                << std::flush;
            std::cout << "Remote\tChunked\tPrefetch\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << benchIdent << "\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl;

            DataCatalog::getInstance().eraseAllRemoteColumns();
        }

        // DataCatalog::getInstance().fetchRemoteInfo();
        // s_ts = std::chrono::high_resolution_clock::now();
        // sum = f5();
        // e_ts = std::chrono::high_resolution_clock::now();

        // secs = e_ts - s_ts;

        // out << "Remote\tPaxed\tPipeline\t000000\t" << benchIdent << "\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl
        //     << std::flush;
        // std::cout << "Remote\tPaxed\tPipeline\t000000\t" << benchIdent << "\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl;

        // DataCatalog::getInstance().eraseAllRemoteColumns();

        // DataCatalog::getInstance().fetchRemoteInfo();
        // s_ts = std::chrono::high_resolution_clock::now();
        // sum = f6();
        // e_ts = std::chrono::high_resolution_clock::now();

        // secs = e_ts - s_ts;

        // out << "Remote\tPaxed\tPrefetch\t000000\t" << benchIdent << "\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl
        //     << std::flush;
        // std::cout << "Remote\tPaxed\tPrefetch\t000000\t" << benchIdent << "\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl;

        // DataCatalog::getInstance().eraseAllRemoteColumns();
    }
}

void benchmark(const std::vector<std::string> idents, const std::array<std::array<uint8_t, 3>, 4> idx, std::ofstream& out, const std::string overlapIdent) {
    // <bool remote, bool chunked, bool paxed, bool prefetching>

    doBenchmark(std::bind(orchBenchmark1<true, false, false, false>, idents, idx),  // Remote Full Pipe
                std::bind(orchBenchmark1<true, false, false, true>, idents, idx),   // Remote Full Prefetch
                std::bind(orchBenchmark1<true, true, false, false>, idents, idx),   // Remote Chunked Pipe
                std::bind(orchBenchmark1<true, true, false, true>, idents, idx),    // Remote Chunked Prefetch
                std::bind(orchBenchmark1<true, false, true, false>, idents, idx),   // Remote Paxed Pipe
                std::bind(orchBenchmark1<true, false, true, true>, idents, idx),    // Remote Paxed Prefetch
                out, "1-4", overlapIdent);

    doBenchmark(std::bind(orchBenchmark2<true, false, false, false>, idents, idx),  // Remote Full Pipe
                std::bind(orchBenchmark2<true, false, false, true>, idents, idx),   // Remote Full Prefetch
                std::bind(orchBenchmark2<true, true, false, false>, idents, idx),   // Remote Chunked Pipe
                std::bind(orchBenchmark2<true, true, false, true>, idents, idx),    // Remote Chunked Prefetch
                std::bind(orchBenchmark2<true, false, true, false>, idents, idx),   // Remote Paxed Pipe
                std::bind(orchBenchmark2<true, false, true, true>, idents, idx),    // Remote Paxed Prefetch
                out, "2-4", overlapIdent);

    doBenchmark(std::bind(orchBenchmark3<true, false, false, false>, idents, idx),  // Remote Full Pipe
                std::bind(orchBenchmark3<true, false, false, true>, idents, idx),   // Remote Full Prefetch
                std::bind(orchBenchmark3<true, true, false, false>, idents, idx),   // Remote Chunked Pipe
                std::bind(orchBenchmark3<true, true, false, true>, idents, idx),    // Remote Chunked Prefetch
                std::bind(orchBenchmark3<true, false, true, false>, idents, idx),   // Remote Paxed Pipe
                std::bind(orchBenchmark3<true, false, true, true>, idents, idx),    // Remote Paxed Prefetch
                out, "4-4", overlapIdent);

    doBenchmark(std::bind(orchBenchmark4<true, false, false, false>, idents, idx),  // Remote Full Pipe
                std::bind(orchBenchmark4<true, false, false, true>, idents, idx),   // Remote Full Prefetch
                std::bind(orchBenchmark4<true, true, false, false>, idents, idx),   // Remote Chunked Pipe
                std::bind(orchBenchmark4<true, true, false, true>, idents, idx),    // Remote Chunked Prefetch
                std::bind(orchBenchmark4<true, false, true, false>, idents, idx),   // Remote Paxed Pipe
                std::bind(orchBenchmark4<true, false, true, true>, idents, idx),    // Remote Paxed Prefetch
                out, "4-1", overlapIdent);
}

void executeRemoteMTBenchmarkingQueries(std::string& logName) {
    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;
    std::cout << std::fixed << std::setprecision(7) << std::endl;

    benchmark({"col_0", "col_1", "col_2"}, {{{0, 1, 2}, {0, 1, 2}, {0, 1, 2}, {0, 1, 2}}}, out, "3-3");
    benchmark({"col_0", "col_1", "col_2", "col_3", "col_4", "col_5"}, {{{0, 1, 2}, {0, 1, 3}, {0, 1, 4}, {0, 1, 5}}}, out, "2-3");
    benchmark({"col_0", "col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8"}, {{{0, 1, 2}, {0, 3, 4}, {0, 5, 6}, {0, 7, 8}}}, out, "1-3");
    benchmark({"col_0", "col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8", "col_9", "col_10", "col_11"}, {{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {9, 10, 11}}}, out, "0-3");

    out.close();
}

void localBenchmark(const std::vector<std::string> idents, const std::array<std::array<uint8_t, 3>, 4> idx, std::ofstream& out, const std::string overlapIdent, const std::string& locality) {
    std::chrono::_V2::system_clock::time_point s_ts;
    std::chrono::_V2::system_clock::time_point e_ts;
    std::chrono::duration<double> secs;
    uint64_t sum = 0;

    for (size_t i = 0; i < 20; ++i) {
        s_ts = std::chrono::high_resolution_clock::now();
        sum = orchBenchmark1<false, false, false, false>(idents, idx);
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << locality << "\tFull\tPrefetch\t65536\t1-4\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << locality << "\tFull\tPrefetch\t65536\t1-4\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl;

        s_ts = std::chrono::high_resolution_clock::now();
        sum = orchBenchmark2<false, false, false, false>(idents, idx);
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << locality << "\tFull\tPrefetch\t65536\t2-4\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << locality << "\tFull\tPrefetch\t65536\t2-4\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl;

        s_ts = std::chrono::high_resolution_clock::now();
        sum = orchBenchmark3<false, false, false, false>(idents, idx);
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << locality << "\tFull\tPrefetch\t65536\t4-4\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << locality << "\tFull\tPrefetch\t65536\t4-4\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl;

        s_ts = std::chrono::high_resolution_clock::now();
        sum = orchBenchmark4<false, false, false, false>(idents, idx);
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << locality << "\tFull\tPrefetch\t65536\t4-1\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << locality << "\tFull\tPrefetch\t65536\t4-1\t" << overlapIdent << "\t" << sum << "\t" << secs.count() << std::endl;
    }
}

void executeLocalMTBenchmarkingQueries(std::string& logName, std::string locality) {
    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;
    std::cout << std::fixed << std::setprecision(7) << std::endl;

    localBenchmark({"col_0", "col_1", "col_2"}, {{{0, 1, 2}, {0, 1, 2}, {0, 1, 2}, {0, 1, 2}}}, out, "3-3", locality);
    localBenchmark({"col_0", "col_1", "col_2", "col_3", "col_4", "col_5"}, {{{0, 1, 2}, {0, 1, 3}, {0, 1, 4}, {0, 1, 5}}}, out, "2-3", locality);
    localBenchmark({"col_0", "col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8"}, {{{0, 1, 2}, {0, 3, 4}, {0, 5, 6}, {0, 7, 8}}}, out, "1-3", locality);
    localBenchmark({"col_0", "col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8", "col_9", "col_10", "col_11"}, {{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {9, 10, 11}}}, out, "0-3", locality);

    out.close();
}