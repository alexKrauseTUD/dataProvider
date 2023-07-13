#include "Benchmarks.hpp"

#include <numa.h>
#include <omp.h>

#include <barrier>
#include <future>

#include "Operators.hpp"

Benchmarks::Benchmarks() {
    for (auto& worker : workers) {
        worker.start();
    }
}

Benchmarks::~Benchmarks() {}

std::chrono::duration<double> waitingTime = std::chrono::duration<double>::zero();
std::chrono::duration<double> workingTime = std::chrono::duration<double>::zero();

inline void reset_timer() {
    waitingTime = std::chrono::duration<double>::zero();
    workingTime = std::chrono::duration<double>::zero();
}

inline void wait_col_data_ready(col_t* _col, char* _data) {
    auto s_ts = std::chrono::high_resolution_clock::now();
    std::unique_lock<std::mutex> lk(_col->iteratorLock);
    if (!(_data < static_cast<char*>(_col->current_end))) {
        _col->iterator_data_available.wait(lk, [_col, _data] { return reinterpret_cast<uint64_t*>(_data) < static_cast<uint64_t*>(_col->current_end); });
    }
    waitingTime += (std::chrono::high_resolution_clock::now() - s_ts);
}

template <bool remote, bool chunked, bool paxed, bool prefetching>
inline void fetch_data(col_t* column, uint64_t* data, const bool reload) {
    if (remote) {
        if (reload) {
            if (!prefetching && !paxed) {
                column->request_data(!chunked);
            }
        }
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (reload) {
            if (prefetching && chunked && !paxed) {
                column->request_data(!chunked);
            }
        }
    }
}

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false, bool timings = false>
inline std::vector<size_t> less_than(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;

    std::vector<std::size_t> out_vec;

    fetch_data<remote, chunked, paxed, prefetching>(column, data, reload);

    if (timings) {
        auto s_ts = std::chrono::high_resolution_clock::now();
        out_vec = Operators::less_than<isFirst>(data, predicate, blockSize, in_pos);
        workingTime += (std::chrono::high_resolution_clock::now() - s_ts);
    } else {
        out_vec = Operators::less_than<isFirst>(data, predicate, blockSize, in_pos);
    }

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false, bool timings = false>
inline std::vector<size_t> less_equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;

    std::vector<std::size_t> out_vec;

    fetch_data<remote, chunked, paxed, prefetching>(column, data, reload);

    if (timings) {
        auto s_ts = std::chrono::high_resolution_clock::now();
        out_vec = Operators::less_equal<isFirst>(data, predicate, blockSize, in_pos);
        workingTime += (std::chrono::high_resolution_clock::now() - s_ts);
    } else {
        out_vec = Operators::less_equal<isFirst>(data, predicate, blockSize, in_pos);
    }

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false, bool timings = false>
inline std::vector<size_t> greater_than(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;

    std::vector<std::size_t> out_vec;

    fetch_data<remote, chunked, paxed, prefetching>(column, data, reload);

    if (timings) {
        auto s_ts = std::chrono::high_resolution_clock::now();
        out_vec = Operators::greater_than<isFirst>(data, predicate, blockSize, in_pos);
        workingTime += (std::chrono::high_resolution_clock::now() - s_ts);
    } else {
        out_vec = Operators::greater_than<isFirst>(data, predicate, blockSize, in_pos);
    }

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false, bool timings = false>
inline std::vector<size_t> greater_equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;

    std::vector<std::size_t> out_vec;

    fetch_data<remote, chunked, paxed, prefetching>(column, data, reload);

    if (timings) {
        auto s_ts = std::chrono::high_resolution_clock::now();
        out_vec = Operators::greater_equal<isFirst>(data, predicate, blockSize, in_pos);
        workingTime += (std::chrono::high_resolution_clock::now() - s_ts);
    } else {
        out_vec = Operators::greater_equal<isFirst>(data, predicate, blockSize, in_pos);
    }

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false, bool timings = false>
inline std::vector<size_t> equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;

    std::vector<std::size_t> out_vec;

    fetch_data<remote, chunked, paxed, prefetching>(column, data, reload);

    if (timings) {
        auto s_ts = std::chrono::high_resolution_clock::now();
        out_vec = Operators::equal<isFirst>(data, predicate, blockSize, in_pos);
        workingTime += (std::chrono::high_resolution_clock::now() - s_ts);
    } else {
        out_vec = Operators::equal<isFirst>(data, predicate, blockSize, in_pos);
    }

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false, bool timings = false>
inline std::vector<size_t> between_incl(col_t* column, const uint64_t predicate_1, const uint64_t predicate_2, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;

    std::vector<std::size_t> out_vec;

    fetch_data<remote, chunked, paxed, prefetching>(column, data, reload);

    if (timings) {
        auto s_ts = std::chrono::high_resolution_clock::now();
        out_vec = Operators::between_incl<isFirst>(data, predicate_1, predicate_2, blockSize, in_pos);
        workingTime += (std::chrono::high_resolution_clock::now() - s_ts);
    } else {
        out_vec = Operators::between_incl<isFirst>(data, predicate_1, predicate_2, blockSize, in_pos);
    }

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false, bool timings = false>
inline std::vector<size_t> between_excl(col_t* column, const uint64_t predicate_1, const uint64_t predicate_2, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;

    std::vector<std::size_t> out_vec;

    fetch_data<remote, chunked, paxed, prefetching>(column, data, reload);

    if (timings) {
        auto s_ts = std::chrono::high_resolution_clock::now();
        out_vec = Operators::between_excl<isFirst>(data, predicate_1, predicate_2, blockSize, in_pos);
        workingTime += (std::chrono::high_resolution_clock::now() - s_ts);
    } else {
        out_vec = Operators::between_excl<isFirst>(data, predicate_1, predicate_2, blockSize, in_pos);
    }

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t pipe_1(const uint64_t predicate, const std::vector<std::string> idents) {
    col_t* col_0;
    col_t* col_1;
    col_t* col_2;

    if (idents.size() != 3) {
        LOG_ERROR("The size of 'idents' was not equal to 3" << std::endl;)
        return 0;
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> s_ts;

    if (remote) {
        col_0 = DataCatalog::getInstance().find_remote(idents[0]);
        if (prefetching && !paxed) col_0->request_data(!chunked);
        col_1 = DataCatalog::getInstance().find_remote(idents[1]);
        if (prefetching && !paxed) col_1->request_data(!chunked);
        col_2 = DataCatalog::getInstance().find_remote(idents[2]);
        if (prefetching && !paxed) col_2->request_data(!chunked);

        // if (prefetching && paxed) DataCatalog::getInstance().fetchPseudoPax(1, idents);
    } else {
        col_0 = DataCatalog::getInstance().find_local(idents[0]);
        col_1 = DataCatalog::getInstance().find_local(idents[1]);
        col_2 = DataCatalog::getInstance().find_local(idents[2]);
    }

    size_t columnSize = col_0->size;

    size_t max_elems_per_chunk = 0;
    size_t currentBlockSize = max_elems_per_chunk;
    // if (paxed) {
    //     size_t total_id_len = 0;
    //     for (auto& id : idents) {
    //         total_id_len += id.size();
    //     }

    //     const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
    //     const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(1)->maxBytesInPayload(appMetaSize);

    //     max_elems_per_chunk = ((maximumPayloadSize / idents.size()) / (sizeof(uint64_t) * 4)) * 4;
    //     currentBlockSize = max_elems_per_chunk;
    // } else
    if (!(remote && (chunked || paxed))) {
        max_elems_per_chunk = columnSize;
        currentBlockSize = Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
    } else {
        max_elems_per_chunk = DataCatalog::getInstance().dataCatalog_chunkMaxSize / sizeof(uint64_t);
        if (max_elems_per_chunk <= Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t)) {
            currentBlockSize = max_elems_per_chunk;
        } else {
            currentBlockSize = Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
        }
    }

    uint64_t sum = 0;
    size_t baseOffset = 0;
    size_t currentChunkElementsProcessed = 0;

    auto data_col_2 = reinterpret_cast<uint64_t*>(col_2->data);
    auto data_col_0 = reinterpret_cast<uint64_t*>(col_0->data);

    while (baseOffset < columnSize) {
        // if (remote && paxed) {
        //     if (!prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
        //     wait_col_data_ready(col_2, reinterpret_cast<char*>(data_col_2));
        //     if (prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
        // }

        const size_t elem_diff = columnSize - baseOffset;
        if (elem_diff < currentBlockSize) {
            currentBlockSize = elem_diff;
        }

        auto le_idx = greater_than<remote, chunked, paxed, prefetching>(col_2, 5, baseOffset, currentBlockSize,
                                                                        less_than<remote, chunked, paxed, prefetching>(col_1, 25, baseOffset, currentBlockSize,
                                                                                                                       between_incl<remote, chunked, paxed, prefetching, true>(col_0, 10, 30, baseOffset, currentBlockSize, {}, currentChunkElementsProcessed == 0), currentChunkElementsProcessed == 0),
                                                                        currentChunkElementsProcessed == 0);

        s_ts = std::chrono::high_resolution_clock::now();
        for (auto idx : le_idx) {
            sum += (data_col_0[idx] * data_col_2[idx]);
            // ++sum;
        }
        workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

        baseOffset += currentBlockSize;
        data_col_0 += currentBlockSize;
        data_col_2 += currentBlockSize;
        currentChunkElementsProcessed = baseOffset % max_elems_per_chunk;
    }

    return sum;
}

template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t pipe_2(const uint64_t predicate, const std::vector<std::string> idents) {
    col_t* col_0;
    col_t* col_1;
    col_t* col_2;

    if (idents.size() != 3) {
        LOG_ERROR("The size of 'idents' was not equal to 3" << std::endl;)
        return 0;
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> s_ts;

    if (remote) {
        col_1 = DataCatalog::getInstance().find_remote(idents[1]);
        if (prefetching && !paxed) col_1->request_data(!chunked);
        col_0 = DataCatalog::getInstance().find_remote(idents[0]);
        if (prefetching && !paxed) col_0->request_data(!chunked);
        col_2 = DataCatalog::getInstance().find_remote(idents[2]);
        if (prefetching && !paxed) col_2->request_data(!chunked);

        // if (prefetching && paxed) DataCatalog::getInstance().fetchPseudoPax(1, idents);
    } else {
        col_0 = DataCatalog::getInstance().find_local(idents[0]);
        col_1 = DataCatalog::getInstance().find_local(idents[1]);
        col_2 = DataCatalog::getInstance().find_local(idents[2]);
    }

    size_t columnSize = col_1->size;

    size_t max_elems_per_chunk = 0;
    size_t currentBlockSize = max_elems_per_chunk;
    // if (paxed) {
    //     size_t total_id_len = 0;
    //     for (auto& id : idents) {
    //         total_id_len += id.size();
    //     }

    //     const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
    //     const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(1)->maxBytesInPayload(appMetaSize);

    //     max_elems_per_chunk = ((maximumPayloadSize / idents.size()) / (sizeof(uint64_t) * 4)) * 4;
    //     currentBlockSize = max_elems_per_chunk;
    // } else
    if (!(remote && (chunked || paxed))) {
        max_elems_per_chunk = columnSize;
        currentBlockSize = Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
    } else {
        max_elems_per_chunk = DataCatalog::getInstance().dataCatalog_chunkMaxSize / sizeof(uint64_t);
        if (max_elems_per_chunk <= Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t)) {
            currentBlockSize = max_elems_per_chunk;
        } else {
            currentBlockSize = Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
        }
    }

    uint64_t sum = 0;
    size_t baseOffset = 0;
    size_t currentChunkElementsProcessed = 0;

    auto data_col_2 = reinterpret_cast<uint64_t*>(col_2->data);
    auto data_col_0 = reinterpret_cast<uint64_t*>(col_0->data);

    while (baseOffset < columnSize) {
        // if (remote && paxed) {
        //     if (!prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
        //     wait_col_data_ready(col_2, reinterpret_cast<char*>(data_col_2));
        //     if (prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
        // }

        const size_t elem_diff = columnSize - baseOffset;
        if (elem_diff < currentBlockSize) {
            currentBlockSize = elem_diff;
        }

        auto le_idx = less_than<remote, chunked, paxed, prefetching, true>(col_1, predicate, baseOffset, currentBlockSize, {}, currentChunkElementsProcessed == 0);

        if (remote && !paxed) {
            if (currentChunkElementsProcessed == 0) {
                if (!prefetching && chunked) {
                    col_2->request_data(!chunked);
                    col_0->request_data(!chunked);
                }
                wait_col_data_ready(col_2, reinterpret_cast<char*>(data_col_2));
                wait_col_data_ready(col_0, reinterpret_cast<char*>(data_col_0));
                if (prefetching && chunked) {
                    col_2->request_data(!chunked);
                    col_0->request_data(!chunked);
                }
            }
        }

        s_ts = std::chrono::high_resolution_clock::now();
        for (auto idx : le_idx) {
            sum += (data_col_0[idx] * data_col_2[idx]);
            // ++sum;
        }
        workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

        baseOffset += currentBlockSize;
        data_col_0 += currentBlockSize;
        data_col_2 += currentBlockSize;
        currentChunkElementsProcessed = baseOffset % max_elems_per_chunk;
    }

    return sum;
}

template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t pipe_3(const uint64_t predicate, const std::vector<std::string> idents) {
    col_t* column_0;
    col_t* column_1;
    col_t* column_2;
    col_t* column_3;

    if (idents.size() != 4) {
        LOG_ERROR("The size of 'idents' was not equal to 4" << std::endl;)
        return 0;
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> s_ts;

    if (remote) {
        column_0 = DataCatalog::getInstance().find_remote(idents[0]);
        if (prefetching && !paxed) column_0->request_data(!chunked);
        column_1 = DataCatalog::getInstance().find_remote(idents[1]);
        if (prefetching && !paxed) column_1->request_data(!chunked);
        column_2 = DataCatalog::getInstance().find_remote(idents[2]);
        if (prefetching && !paxed) column_2->request_data(!chunked);
        column_3 = DataCatalog::getInstance().find_remote(idents[3]);
        if (prefetching && !paxed) column_3->request_data(!chunked);

        // if (prefetching && paxed) DataCatalog::getInstance().fetchPseudoPax(1, idents);
    } else {
        column_0 = DataCatalog::getInstance().find_local(idents[0]);
        column_1 = DataCatalog::getInstance().find_local(idents[1]);
        column_2 = DataCatalog::getInstance().find_local(idents[2]);
        column_3 = DataCatalog::getInstance().find_local(idents[3]);
    }

    size_t columnSize = column_0->size;

    size_t max_elems_per_chunk = 0;
    size_t currentBlockSize = max_elems_per_chunk;
    // if (paxed) {
    //     size_t total_id_len = 0;
    //     for (auto& id : idents) {
    //         total_id_len += id.size();
    //     }

    //     const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
    //     const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(1)->maxBytesInPayload(appMetaSize);

    //     max_elems_per_chunk = ((maximumPayloadSize / idents.size()) / (sizeof(uint64_t) * 4)) * 4;
    //     currentBlockSize = max_elems_per_chunk;
    // } else
    if (!(remote && (chunked || paxed))) {
        max_elems_per_chunk = columnSize;
        currentBlockSize = Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
    } else {
        max_elems_per_chunk = DataCatalog::getInstance().dataCatalog_chunkMaxSize / sizeof(uint64_t);
        if (max_elems_per_chunk <= Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t)) {
            currentBlockSize = max_elems_per_chunk;
        } else {
            currentBlockSize = Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
        }
    }

    uint64_t sum = 0;
    size_t baseOffset = 0;
    size_t currentChunkElementsProcessed = 0;

    auto data_2 = reinterpret_cast<uint64_t*>(column_2->data);
    auto data_3 = reinterpret_cast<uint64_t*>(column_3->data);

    while (baseOffset < columnSize) {
        // if (remote && paxed) {
        //     if (!prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
        //     wait_col_data_ready(column_3, reinterpret_cast<char*>(data_3));
        //     if (prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
        // }

        const size_t elem_diff = columnSize - baseOffset;
        if (elem_diff < currentBlockSize) {
            currentBlockSize = elem_diff;
        }

        auto le_idx = equal<remote, chunked, paxed, prefetching>(column_3, 16, baseOffset, currentBlockSize,
                                                                 greater_than<remote, chunked, paxed, prefetching>(column_2, 5, baseOffset, currentBlockSize,
                                                                                                                   less_than<remote, chunked, paxed, prefetching>(column_1, 25, baseOffset, currentBlockSize,
                                                                                                                                                                  between_incl<remote, chunked, paxed, prefetching, true>(column_0, 10, 30, baseOffset, currentBlockSize, {}, currentChunkElementsProcessed == 0), currentChunkElementsProcessed == 0),
                                                                                                                   currentChunkElementsProcessed == 0),
                                                                 currentChunkElementsProcessed == 0);

        s_ts = std::chrono::high_resolution_clock::now();
        for (auto idx : le_idx) {
            sum += (data_2[idx] * data_3[idx]);
            // ++sum;
        }
        workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

        baseOffset += currentBlockSize;
        data_2 += currentBlockSize;
        data_3 += currentBlockSize;
        currentChunkElementsProcessed = baseOffset % max_elems_per_chunk;
    }

    return sum;
}

uint64_t pipe_4(std::string& ident) {
    col_t* column_0;

    column_0 = DataCatalog::getInstance().find_local(ident);

    size_t columnSize = column_0->size;

    uint64_t sum = 0;
    auto data = reinterpret_cast<uint64_t*>(column_0->data);

    for (size_t i = 0; i < columnSize; ++i) {
        sum += data[i];
    }

    return sum;
}

uint64_t pipe_5(std::string& ident) {
    col_t* column_0;

    column_0 = DataCatalog::getInstance().find_local(ident);

    size_t columnSize = column_0->size;

    uint64_t cnt = 0;
    auto data = reinterpret_cast<uint64_t*>(column_0->data);

    for (size_t i = 0; i < columnSize; ++i) {
        if (data[i] >= 5 && data[i] <= 15) {
            ++cnt;
        }
    }

    return cnt;
}

uint64_t pipe_6(std::string& ident) {
    col_t* column_0;

    column_0 = DataCatalog::getInstance().find_local(ident);

    size_t columnSize = column_0->size;

    uint64_t sum = 0;
    size_t baseOffset = 0;
    uint64_t* data = reinterpret_cast<uint64_t*>(column_0->data);

    size_t currentBlockSize = Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);

    while (baseOffset < columnSize) {
        const size_t elem_diff = columnSize - baseOffset;
        if (elem_diff < currentBlockSize) {
            currentBlockSize = elem_diff;
        }

        for (size_t i = 0; i < currentBlockSize; ++i, ++data) {
            sum += *data;
        }

        baseOffset += currentBlockSize;
    }

    return sum;
}

uint64_t pipe_7(std::string& ident) {
    col_t* column_0;

    column_0 = DataCatalog::getInstance().find_remote(ident);
    column_0->request_data(false);

    size_t columnSize = column_0->size;

    uint64_t sum = 0;
    size_t baseOffset = 0;

    uint64_t* data = reinterpret_cast<uint64_t*>(column_0->data);

    size_t currentBlockSize = Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);

    while (baseOffset < columnSize) {
        const size_t elem_diff = columnSize - baseOffset;
        if (elem_diff < currentBlockSize) {
            currentBlockSize = elem_diff;
        }

        wait_col_data_ready(column_0, reinterpret_cast<char*>(data));
        column_0->request_data(false);

        for (size_t i = 0; i < currentBlockSize; ++i, ++data) {
            sum += *data;
        }

        baseOffset += currentBlockSize;
    }

    return sum;
}

size_t hash_join_1(std::pair<std::string, std::string> idents) {
    col_t* column_0;
    col_t* column_1;

    column_1 = DataCatalog::getInstance().find_remote(idents.second);
    column_1->request_data(true);

    column_0 = DataCatalog::getInstance().find_local(idents.first);

    size_t columnSize0 = column_0->size;
    size_t columnSize1 = column_1->size;

    std::unordered_map<uint64_t, std::vector<size_t>> hashMap;
    size_t joinResult = 0;

    uint64_t* data_0 = reinterpret_cast<uint64_t*>(column_0->data);

    for (size_t i = 0; i < columnSize0; i++) {
        hashMap[data_0[i]].push_back(i);
    }

    uint64_t* data_1 = reinterpret_cast<uint64_t*>(column_1->data);
    wait_col_data_ready(column_1, reinterpret_cast<char*>(data_1));

    for (size_t i = 0; i < columnSize1; i++) {
        auto it = hashMap.find(data_1[i]);
        if (it != hashMap.end()) {
            for (const auto& matchingIndex : it->second) {
                ++joinResult;
            }
        }
    }

    return joinResult;
}

size_t hash_join_2(std::pair<std::string, std::string> idents) {
    col_t* column_0;
    col_t* column_1;

    column_0 = DataCatalog::getInstance().find_remote(idents.first);
    column_0->request_data(true);

    column_1 = DataCatalog::getInstance().find_local(idents.second);

    size_t columnSize0 = column_0->size;
    size_t columnSize1 = column_1->size;

    std::unordered_map<uint64_t, std::vector<size_t>> hashMap;
    size_t joinResult = 0;

    uint64_t* data_0 = reinterpret_cast<uint64_t*>(column_0->data);
    wait_col_data_ready(column_0, reinterpret_cast<char*>(data_0));

    for (size_t i = 0; i < columnSize0; i++) {
        hashMap[data_0[i]].push_back(i);
    }

    uint64_t* data_1 = reinterpret_cast<uint64_t*>(column_1->data);

    for (size_t i = 0; i < columnSize1; i++) {
        auto it = hashMap.find(data_1[i]);
        if (it != hashMap.end()) {
            for (const auto& matchingIndex : it->second) {
                ++joinResult;
            }
        }
    }

    return joinResult;
}

size_t hash_join_3(std::pair<std::string, std::string> idents) {
    col_t* column_0;
    col_t* column_1;

    column_0 = DataCatalog::getInstance().find_local(idents.first);
    column_1 = DataCatalog::getInstance().find_local(idents.second);

    size_t columnSize0 = column_0->size;
    size_t columnSize1 = column_1->size;

    std::unordered_map<uint64_t, std::vector<size_t>> hashMap;
    size_t joinResult = 0;

    uint64_t* data_0 = reinterpret_cast<uint64_t*>(column_0->data);

    for (size_t i = 0; i < columnSize0; i++) {
        hashMap[data_0[i]].push_back(i);
    }

    uint64_t* data_1 = reinterpret_cast<uint64_t*>(column_1->data);

    for (size_t i = 0; i < columnSize1; i++) {
        auto it = hashMap.find(data_1[i]);
        if (it != hashMap.end()) {
            for (const auto& matchingIndex : it->second) {
                ++joinResult;
            }
        }
    }

    return joinResult;
}

void hash_join_pg(std::shared_future<void>* ready_future, const size_t tid, const size_t local_worker_count, std::atomic<size_t>* ready_workers, std::atomic<size_t>* complete_workers, std::condition_variable* done_cv, std::mutex* done_cv_lock, bool* all_done, uint64_t* result_ptr, long* out_time, std::pair<std::string, std::vector<std::string>> idents) {
    col_t* column_0;
    col_t* column_1;
    size_t joinResult = 0;
    size_t joinCount = idents.second.size();
    size_t chunkSize = DataCatalog::getInstance().dataCatalog_chunkMaxSize;
    size_t currentBlockSize = chunkSize / sizeof(uint64_t);
    size_t baseOffset = 0;

    ++(*ready_workers);
    ready_future->wait();
    auto start = std::chrono::high_resolution_clock::now();

    column_1 = DataCatalog::getInstance().find_remote(idents.second[0]);
    column_1->request_data(false);

    column_0 = DataCatalog::getInstance().find_local(idents.first);

    size_t columnSize0 = column_0->size;
    size_t columnSize1 = column_1->size;

    uint64_t* data_0 = reinterpret_cast<uint64_t*>(column_0->data);
    uint64_t* data_1 = reinterpret_cast<uint64_t*>(column_1->data);

    for (size_t join_cnt = 0; join_cnt < joinCount; ++join_cnt) {
        std::unordered_map<uint64_t, std::vector<size_t>> hashMap;

        while (baseOffset < columnSize1) {
            const size_t elem_diff = columnSize1 - baseOffset;
            if (elem_diff < currentBlockSize) {
                currentBlockSize = elem_diff;
            }

            wait_col_data_ready(column_1, reinterpret_cast<char*>(data_1));
            column_1->request_data(false);

            for (size_t i = 0; i < currentBlockSize; ++i) {
                hashMap[data_1[i]].push_back(i);
            }

            baseOffset += currentBlockSize;
            data_1 += currentBlockSize;
        }

        if (join_cnt + 1 < joinCount) {
            column_1 = DataCatalog::getInstance().find_remote(idents.second[join_cnt + 1]);
            column_1->request_data(false);
            columnSize1 = column_1->size;
            data_1 = reinterpret_cast<uint64_t*>(column_1->data);
        }

        for (size_t i = 0; i < columnSize0; ++i) {
            auto it = hashMap.find(data_0[i]);
            if (it != hashMap.end()) {
                for (const auto& _ : it->second) {
                    ++joinResult;
                }
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    out_time[tid] = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    result_ptr[tid] = joinResult;
    if (++(*complete_workers) == local_worker_count) {
        *all_done = true;
        std::unique_lock<std::mutex> lk(*done_cv_lock);
        done_cv->notify_all();
    }
}

void hash_join_pg_alt(std::shared_future<void>* ready_future, const size_t tid, const size_t local_worker_count, std::atomic<size_t>* ready_workers, std::atomic<size_t>* complete_workers, std::condition_variable* done_cv, std::mutex* done_cv_lock, bool* all_done, uint64_t* result_ptr, long* out_time, std::pair<std::string, std::vector<std::string>> idents) {
    col_t* column_0;
    col_t* column_1;
    size_t joinResult = 0;
    size_t joinCount = idents.second.size();
    size_t chunkSize = DataCatalog::getInstance().dataCatalog_chunkMaxSize;
    size_t currentBlockSize = chunkSize / sizeof(uint64_t);
    size_t baseOffset = 0;

    ++(*ready_workers);
    ready_future->wait();
    auto start = std::chrono::high_resolution_clock::now();

    column_0 = DataCatalog::getInstance().find_remote(idents.first);
    column_0->request_data(false);

    size_t columnSize0 = column_0->size;

    uint64_t* data_0 = reinterpret_cast<uint64_t*>(column_0->data);

    for (size_t join_cnt = 0; join_cnt < joinCount; ++join_cnt) {
        std::unordered_map<uint64_t, std::vector<size_t>> hashMap;

        column_1 = DataCatalog::getInstance().find_local(idents.second[join_cnt]);
        size_t columnSize1 = column_1->size;
        uint64_t* data_1 = reinterpret_cast<uint64_t*>(column_1->data);

        for (size_t i = 0; i < columnSize1; ++i) {
            hashMap[data_1[i]].push_back(i);
        }

        if (join_cnt == 0) {
            while (baseOffset < columnSize0) {
                const size_t elem_diff = columnSize0 - baseOffset;
                if (elem_diff < currentBlockSize) {
                    currentBlockSize = elem_diff;
                }

                wait_col_data_ready(column_0, reinterpret_cast<char*>(data_0));
                column_0->request_data(false);

                for (size_t i = 0; i < currentBlockSize; ++i) {
                    auto it = hashMap.find(data_0[i]);
                    if (it != hashMap.end()) {
                        for (const auto& _ : it->second) {
                            ++joinResult;
                        }
                    }
                }

                baseOffset += currentBlockSize;
                data_0 += currentBlockSize;
            }
            data_0 = reinterpret_cast<uint64_t*>(column_0->data);
        } else {
            for (size_t i = 0; i < columnSize0; ++i) {
                auto it = hashMap.find(data_0[i]);
                if (it != hashMap.end()) {
                    for (const auto& _ : it->second) {
                        ++joinResult;
                    }
                }
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    out_time[tid] = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    result_ptr[tid] = joinResult;
    if (++(*complete_workers) == local_worker_count) {
        *all_done = true;
        std::unique_lock<std::mutex> lk(*done_cv_lock);
        done_cv->notify_all();
    }
}

void Benchmarks::execLocalBenchmark(std::string& logName, std::string locality) {
    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;
    std::cout << std::fixed << std::setprecision(7) << std::endl;
    const std::array predicates{0, 1, 25, 50, 75, 100};
    std::vector<std::string> idents;

    if (locality == "Local") {
        idents = std::vector<std::string>{"col_0", "col_1", "col_2"};
    } else if (locality == "NUMA") {
        idents = std::vector<std::string>{"col_18", "col_19", "col_20"};
    }

    uint64_t sum = 0;
    std::chrono::_V2::system_clock::time_point s_ts;
    std::chrono::_V2::system_clock::time_point e_ts;

    for (const auto predicate : predicates) {
        for (size_t i = 0; i < 20; ++i) {
            reset_timer();

            if (predicate == 0) {
                s_ts = std::chrono::high_resolution_clock::now();
                sum = pipe_1<false, false, false, false>(predicate, idents);
                e_ts = std::chrono::high_resolution_clock::now();
            } else {
                s_ts = std::chrono::high_resolution_clock::now();
                sum = pipe_2<false, false, false, false>(predicate, idents);
                e_ts = std::chrono::high_resolution_clock::now();
            }

            std::chrono::duration<double> secs = e_ts - s_ts;
            auto additional_time = secs.count() - (workingTime.count() + waitingTime.count());

            out << locality << "\tFull\tPipe\t" << OPTIMAL_BLOCK_SIZE << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl
                << std::flush;
            std::cout << locality << "\tFull\tPipe\t" << OPTIMAL_BLOCK_SIZE << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << "\t" << additional_time << std::endl;
        }
    }

    out.close();
}

void Benchmarks::execRemoteBenchmark(std::string& logName, std::string locality) {
    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;
    std::cout << std::fixed << std::setprecision(7) << std::endl;
    const std::array predicates{0, 1, 25, 50, 75, 100};
    std::vector<std::string> idents;

    if (locality == "RemoteLocal") {
        idents = std::vector<std::string>{"col_0", "col_1", "col_2"};
    } else if (locality == "RemoteNUMA") {
        idents = std::vector<std::string>{"col_18", "col_19", "col_20"};
    }

    uint64_t sum = 0;
    std::chrono::_V2::system_clock::time_point s_ts;
    std::chrono::_V2::system_clock::time_point e_ts;

    for (const auto predicate : predicates) {
        for (size_t i = 0; i < 10; ++i) {
            DataCatalog::getInstance().eraseAllRemoteColumns();
            reset_timer();
            DataCatalog::getInstance().fetchRemoteInfo();

            if (predicate == 0) {
                s_ts = std::chrono::high_resolution_clock::now();
                sum = pipe_1<true, false, false, true>(predicate, idents);
                e_ts = std::chrono::high_resolution_clock::now();
            } else {
                s_ts = std::chrono::high_resolution_clock::now();
                sum = pipe_2<true, false, false, true>(predicate, idents);
                e_ts = std::chrono::high_resolution_clock::now();
            }

            std::chrono::duration<double> secs = e_ts - s_ts;
            auto additional_time = secs.count() - (workingTime.count() + waitingTime.count());

            out << locality << "\tFull\tPipe\t" << OPTIMAL_BLOCK_SIZE << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl
                << std::flush;
            std::cout << locality << "\tFull\tPipe\t" << OPTIMAL_BLOCK_SIZE << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << "\t" << additional_time << std::endl;
        }
    }

    for (uint64_t chunkSize = 1ull << 18; chunkSize <= 1ull << 27; chunkSize <<= 1) {
        DataCatalog::getInstance().reconfigureChunkSize(chunkSize, chunkSize);

        for (const auto predicate : predicates) {
            for (size_t i = 0; i < 10; ++i) {
                DataCatalog::getInstance().eraseAllRemoteColumns();
                reset_timer();
                DataCatalog::getInstance().fetchRemoteInfo();

                if (predicate == 0) {
                    s_ts = std::chrono::high_resolution_clock::now();
                    sum = pipe_1<true, true, false, true>(predicate, idents);
                    e_ts = std::chrono::high_resolution_clock::now();
                } else {
                    s_ts = std::chrono::high_resolution_clock::now();
                    sum = pipe_2<true, true, false, true>(predicate, idents);
                    e_ts = std::chrono::high_resolution_clock::now();
                }

                std::chrono::duration<double> secs = e_ts - s_ts;
                auto additional_time = secs.count() - (workingTime.count() + waitingTime.count());

                out << locality << "\tChunked\tPipe\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl
                    << std::flush;
                std::cout << locality << "\tChunked\tPipe\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << "\t" << additional_time << std::endl;
            }
        }
    }

    out.close();
}

void Benchmarks::execLocalBenchmarkMW(std::string& logName, std::string locality) {
    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;
    std::cout << std::fixed << std::setprecision(7) << std::endl;
    const std::array predicates{0, 1, 25, 50, 75, 100};
    std::vector<std::string> idents;

    if (locality == "Local") {
        idents = std::vector<std::string>{"col_0", "col_1", "col_2"};
    } else if (locality == "NUMA") {
        idents = std::vector<std::string>{"col_18", "col_19", "col_20"};
    }

    Worker workers[WORKER_NUMBER];

    for (auto& worker : workers) {
        worker.start();
    }

    for (size_t i = 0; i < 10; ++i) {
        std::barrier sync_point(WORKER_NUMBER * predicates.size() + 1);

        auto do_work = [&](int predicate) {
            if (predicate == 0) {
                pipe_1<false, false, false, false>(predicate, idents);
            } else {
                pipe_2<false, false, false, false>(predicate, idents);
            }
            sync_point.arrive_and_drop();
        };

        std::chrono::_V2::system_clock::time_point s_ts = std::chrono::high_resolution_clock::now();
        for (const auto predicate : predicates) {
            for (auto& worker : workers) {
                worker.push_task(do_work, predicate);
            }
        }

        sync_point.arrive_and_wait();

        std::chrono::_V2::system_clock::time_point e_ts = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> secs = e_ts - s_ts;

        out << locality << "\tFull\tPipe\t" << OPTIMAL_BLOCK_SIZE << "\t" << WORKER_NUMBER << "\t" << predicates.size() << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << locality << "\tFull\tPipe\t" << OPTIMAL_BLOCK_SIZE << "\t" << WORKER_NUMBER << "\t" << predicates.size() << "\t" << secs.count() << std::endl;
    }

    for (auto& worker : workers) {
        worker.stop();
    }

    out.close();
}

void Benchmarks::execRemoteBenchmarkMW(std::string& logName, std::string locality) {
    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;
    std::cout << std::fixed << std::setprecision(7) << std::endl;
    const std::array predicates{0, 1, 25, 50, 75, 100};
    std::vector<std::vector<std::string>> idents;

    if (locality == "LocalRemoteLocal" || locality == "NUMARemoteLocal") {
        idents = std::vector<std::vector<std::string>>{{"col_0", "col_1", "col_2"}, {"col_3", "col_4", "col_5"}, {"col_6", "col_7", "col_8"}, {"col_9", "col_10", "col_11"}, {"col_12", "col_13", "col_14"}, {"col_15", "col_16", "col_17"}};
    } else if (locality == "LocalRemoteNUMA" || locality == "NUMARemoteNUMA") {
        idents = std::vector<std::vector<std::string>>{{"col_18", "col_19", "col_20"}, {"col_21", "col_22", "col_23"}, {"col_24", "col_25", "col_26"}, {"col_27", "col_28", "col_29"}, {"col_30", "col_31", "col_32"}, {"col_33", "col_34", "col_35"}};
    }

    if (locality == "NUMARemoteLocal" || locality == "NUMARemoteNUMA") {
        struct bitmask* mask = numa_bitmask_alloc(numa_num_possible_nodes());
        numa_bitmask_setbit(mask, 1);
        numa_run_on_node_mask(mask);
        numa_bitmask_free(mask);
    } else if (locality == "LocalRemoteLocal" || locality == "LocalRemoteNUMA") {
        struct bitmask* mask = numa_bitmask_alloc(numa_num_possible_nodes());
        numa_bitmask_setbit(mask, 0);
        numa_run_on_node_mask(mask);
        numa_bitmask_free(mask);
    }

    Worker workers[WORKER_NUMBER];

    for (auto& worker : workers) {
        worker.start();
    }

    for (size_t i = 0; i < 10; ++i) {
        DataCatalog::getInstance().eraseAllRemoteColumns();
        DataCatalog::getInstance().fetchRemoteInfo();
        std::barrier sync_point(WORKER_NUMBER * predicates.size() + 1);

        auto do_work = [&](int predicate, size_t index) {
            if (predicate == 0) {
                pipe_1<true, false, false, true>(predicate, idents[index]);
            } else {
                pipe_2<true, false, false, true>(predicate, idents[index]);
            }
            sync_point.arrive_and_drop();
        };

        size_t k = 0;
        std::chrono::_V2::system_clock::time_point s_ts = std::chrono::high_resolution_clock::now();
        for (const auto predicate : predicates) {
            for (auto& worker : workers) {
                worker.push_task(do_work, predicate, k);
            }
            ++k;
        }

        sync_point.arrive_and_wait();

        std::chrono::_V2::system_clock::time_point e_ts = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> secs = e_ts - s_ts;

        out << locality << "\tFull\tPipe\t" << OPTIMAL_BLOCK_SIZE << "\t" << WORKER_NUMBER << "\t" << predicates.size() << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << locality << "\tFull\tPipe\t" << OPTIMAL_BLOCK_SIZE << "\t" << WORKER_NUMBER << "\t" << predicates.size() << "\t" << secs.count() << std::endl;
    }

    for (uint64_t chunkSize = 1ull << 18; chunkSize <= 1ull << 27; chunkSize <<= 1) {
        DataCatalog::getInstance().reconfigureChunkSize(chunkSize, chunkSize);

        for (size_t i = 0; i < 10; ++i) {
            DataCatalog::getInstance().eraseAllRemoteColumns();
            DataCatalog::getInstance().fetchRemoteInfo();
            std::barrier sync_point(WORKER_NUMBER * predicates.size() + 1);

            auto do_work = [&](int predicate, size_t index) {
                if (predicate == 1) {
                    pipe_1<true, true, false, true>(predicate, idents[index]);
                } else {
                    pipe_2<true, true, false, true>(predicate, idents[index]);
                }
                sync_point.arrive_and_drop();
            };

            size_t k = 0;
            std::chrono::_V2::system_clock::time_point s_ts = std::chrono::high_resolution_clock::now();
            for (const auto predicate : predicates) {
                for (auto& worker : workers) {
                    worker.push_task(do_work, predicate, k);
                }
                ++k;
            }

            sync_point.arrive_and_wait();

            std::chrono::_V2::system_clock::time_point e_ts = std::chrono::high_resolution_clock::now();

            std::chrono::duration<double> secs = e_ts - s_ts;

            out << locality << "\tChunked\tPipe\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << WORKER_NUMBER << "\t" << predicates.size() << "\t" << secs.count() << std::endl
                << std::flush;
            std::cout << locality << "\tChunked\tPipe\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << WORKER_NUMBER << "\t" << predicates.size() << "\t" << secs.count() << std::endl;
        }
    }

    for (auto& worker : workers) {
        worker.stop();
    }

    struct bitmask* mask = numa_bitmask_alloc(numa_num_possible_nodes());
    numa_bitmask_setbit(mask, 0);
    numa_run_on_node_mask(mask);
    numa_bitmask_free(mask);

    out.close();
}

double calculate_MiB_per_s(const size_t size_in_bytes, const size_t time_in_ns) {
    return (static_cast<double>(size_in_bytes) / 1024 / 1024)  // B-to-MiB
           /
           (static_cast<double>(time_in_ns) / 10e8);  // ns-to-s
}

double get_bandwidth(const size_t numBuffers, const size_t buffer_size_in_bytes, long* time_out_ptr, size_t offset) {
    double bwd = 0.0;
    for (size_t i = offset; i < numBuffers + offset; ++i) {
        bwd += calculate_MiB_per_s(buffer_size_in_bytes, time_out_ptr[i]);
    }
    return bwd / numBuffers;
}

template <bool filter>
void Benchmarks::execUPIBenchmark(std::string& logName) {
    cpu_set_t cpuset;

    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;
    std::cout << std::fixed << std::setprecision(7) << std::endl;
    std::vector<std::string> idents;  // = std::vector<std::string>{"col_0", "col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8", "col_9", "col_10", "col_11", "col_12", "col_13", "col_14", "col_15", "col_16", "col_17", "col_18", "col_19", "col_20", "col_21", "col_22", "col_23", "col_24", "col_25", "col_26", "col_27", "col_28", "col_29", "col_30", "col_31", "col_32", "col_33", "col_34", "col_35"};
    for (size_t i = 0; i < 36; ++i) {
        std::stringstream nameStream;
        nameStream << "col_" << i;
        idents.push_back(nameStream.str());
    }
    const size_t maxRuns = 100;
    std::chrono::_V2::system_clock::time_point s_ts;
    std::chrono::_V2::system_clock::time_point e_ts;
    out << "local_buffer_cnt\tremote_buffer_cnt\tlocal_column_size\tremote_column_size\tlocal_bwdh\tremote_bwdh\twallclock_bwdh\n"
        << std::flush;
    size_t localColumnSize = DataCatalog::getInstance().find_local(idents[0])->sizeInBytes;
    size_t remoteColumnSize = DataCatalog::getInstance().find_local(idents[idents.size() / 2])->sizeInBytes;

    for (size_t localBuffers = 0; localBuffers <= 16; ++localBuffers) {
        for (size_t remoteBuffers = 0; remoteBuffers <= 16; ++remoteBuffers) {
            if (localBuffers == 0 && remoteBuffers == 0) {
                continue;
            }
            const size_t numWorkers = localBuffers + remoteBuffers;

            for (size_t run = 0; run < maxRuns; ++run) {
                std::barrier sync_point_1(numWorkers + 1);
                std::barrier sync_point_2(numWorkers + 1);
                double local_bwdh = 0.0;
                double remote_bwdh = 0.0;
                double wallclock = 0.0;
                long* time_out_ptr = reinterpret_cast<long*>(numa_alloc_onnode(numWorkers, 0));
                std::vector<std::unique_ptr<std::thread>> localWorkers;
                std::vector<std::unique_ptr<std::thread>> remoteWorkers;

                auto do_work = [&](std::string ident, size_t index) {
                    // std::cout << "Working on " << ident << std::endl;
                    sync_point_1.arrive_and_wait();
                    // auto s_ts = std::chrono::high_resolution_clock::now();
                    if (filter) {
                        pipe_5(ident);
                    } else {
                        pipe_4(ident);
                    }
                    time_out_ptr[index] = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - s_ts).count();
                    sync_point_2.arrive_and_wait();
                };

                for (size_t tid = 0; tid < localBuffers; ++tid) {
                    localWorkers.emplace_back(std::make_unique<std::thread>(do_work, idents[tid], tid));
                    CPU_ZERO(&cpuset);
                    CPU_SET(tid, &cpuset);
                    int rc = pthread_setaffinity_np(localWorkers.back()->native_handle(), sizeof(cpu_set_t), &cpuset);
                    if (rc != 0) {
                        std::cerr << "Error calling pthread_setaffinity_np in copy_pool assignment: " << rc << std::endl
                                  << std::endl;
                        exit(-10);
                    }
                }

                for (size_t tid = localBuffers; tid < numWorkers; ++tid) {
                    remoteWorkers.emplace_back(std::make_unique<std::thread>(do_work, idents[tid - localBuffers + (idents.size() / 2)], tid));
                    if (tid < 16) {
                        CPU_ZERO(&cpuset);
                        CPU_SET(tid, &cpuset);
                    } else {
                        CPU_ZERO(&cpuset);
                        CPU_SET(tid + 48, &cpuset);
                    }
                    int rc = pthread_setaffinity_np(remoteWorkers.back()->native_handle(), sizeof(cpu_set_t), &cpuset);
                    if (rc != 0) {
                        std::cerr << "Error calling pthread_setaffinity_np in copy_pool assignment: " << rc << std::endl
                                  << std::endl;
                        exit(-10);
                    }
                }

                {
                    using namespace std::chrono_literals;
                    std::this_thread::sleep_for(100ms);
                }

                s_ts = std::chrono::high_resolution_clock::now();
                sync_point_1.arrive_and_wait();
                sync_point_2.arrive_and_wait();
                e_ts = std::chrono::high_resolution_clock::now();

                wallclock += calculate_MiB_per_s((localColumnSize * localBuffers) + (remoteColumnSize * remoteBuffers), std::chrono::duration_cast<std::chrono::nanoseconds>(e_ts - s_ts).count());

                local_bwdh += (localBuffers == 0) ? 0 : get_bandwidth(localBuffers, localColumnSize, time_out_ptr, 0);
                remote_bwdh += (remoteBuffers == 0) ? 0 : get_bandwidth(remoteBuffers, remoteColumnSize, time_out_ptr, localBuffers);

                std::cout << "Local:\t" << localBuffers << "\tRemote:\t" << remoteBuffers << std::endl;
                std::cout << "Local aggregated bandwidth: " << local_bwdh << std::endl;
                std::cout << "Remote aggregated bandwidth: " << remote_bwdh << std::endl;
                std::cout << "Wallclock bandwidth: " << wallclock << std::endl;
                out << localBuffers << "\t" << remoteBuffers << "\t" << localColumnSize << "\t" << remoteColumnSize << "\t" << local_bwdh << "\t" << remote_bwdh << "\t" << wallclock << std::endl
                    << std::flush;

                std::for_each(localWorkers.begin(), localWorkers.end(), [](std::unique_ptr<std::thread>& t) { t->join(); });
                std::for_each(remoteWorkers.begin(), remoteWorkers.end(), [](std::unique_ptr<std::thread>& t) { t->join(); });
                {
                    using namespace std::chrono_literals;
                    std::this_thread::sleep_for(100ms);
                }
            }
        }
    }
}

void Benchmarks::execRDMABenchmark(std::string& logName) {
    cpu_set_t cpuset;

    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;
    std::cout << std::fixed << std::setprecision(7) << std::endl;
    std::vector<std::string> idents;  // = std::vector<std::string>{"col_0", "col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8", "col_9", "col_10", "col_11", "col_12", "col_13", "col_14", "col_15", "col_16", "col_17", "col_18", "col_19", "col_20", "col_21", "col_22", "col_23", "col_24", "col_25", "col_26", "col_27", "col_28", "col_29", "col_30", "col_31", "col_32", "col_33", "col_34", "col_35"};
    for (size_t i = 0; i < 36; ++i) {
        std::stringstream nameStream;
        nameStream << "col_" << i;
        idents.push_back(nameStream.str());
    }
    const size_t maxRuns = 10;
    std::chrono::_V2::system_clock::time_point s_ts;
    std::chrono::_V2::system_clock::time_point e_ts;
    out << "local_buffer_cnt\tremote_buffer_cnt\tlocal_column_size\tremote_column_size\tlocal_bwdh\tremote_bwdh\twallclock_bwdh\n"
        << std::flush;
    size_t localColumnSize = DataCatalog::getInstance().find_local(idents[0])->sizeInBytes;
    DataCatalog::getInstance().fetchRemoteInfo();
    size_t remoteColumnSize = DataCatalog::getInstance().find_remote(idents[idents.size() / 2])->sizeInBytes;

    DataCatalog::getInstance().reconfigureChunkSize(4194304, 4194304);

    for (size_t localBuffers = 0; localBuffers <= 16; ++localBuffers) {
        for (size_t remoteBuffers = 0; remoteBuffers <= 16; ++remoteBuffers) {
            if (localBuffers == 0 && remoteBuffers == 0) {
                continue;
            }
            const size_t numWorkers = localBuffers + remoteBuffers;

            for (size_t run = 0; run < maxRuns; ++run) {
                std::barrier sync_point_1(numWorkers + 1);
                std::barrier sync_point_2(numWorkers + 1);
                double local_bwdh = 0.0;
                double remote_bwdh = 0.0;
                double wallclock = 0.0;
                long* time_out_ptr = reinterpret_cast<long*>(numa_alloc_onnode(numWorkers, 0));
                std::vector<std::unique_ptr<std::thread>> localWorkers;
                std::vector<std::unique_ptr<std::thread>> remoteWorkers;
                DataCatalog::getInstance().eraseAllRemoteColumns();
                DataCatalog::getInstance().fetchRemoteInfo();

                auto do_work_local = [&](std::string ident, size_t index) {
                    sync_point_1.arrive_and_wait();
                    pipe_6(ident);
                    time_out_ptr[index] = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - s_ts).count();
                    sync_point_2.arrive_and_wait();
                };

                auto do_work_remote = [&](std::string ident, size_t index) {
                    sync_point_1.arrive_and_wait();
                    pipe_7(ident);
                    time_out_ptr[index] = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - s_ts).count();
                    sync_point_2.arrive_and_wait();
                };

                for (size_t tid = 0; tid < localBuffers; ++tid) {
                    localWorkers.emplace_back(std::make_unique<std::thread>(do_work_local, idents[tid], tid));
                    CPU_ZERO(&cpuset);
                    CPU_SET(tid, &cpuset);
                    int rc = pthread_setaffinity_np(localWorkers.back()->native_handle(), sizeof(cpu_set_t), &cpuset);
                    if (rc != 0) {
                        std::cerr << "Error calling pthread_setaffinity_np in copy_pool assignment: " << rc << std::endl
                                  << std::endl;
                        exit(-10);
                    }
                }

                for (size_t tid = localBuffers; tid < numWorkers; ++tid) {
                    remoteWorkers.emplace_back(std::make_unique<std::thread>(do_work_remote, idents[tid - localBuffers + idents.size() / 2], tid));
                    if (tid < 16) {
                        CPU_ZERO(&cpuset);
                        CPU_SET(tid, &cpuset);
                    } else {
                        CPU_ZERO(&cpuset);
                        CPU_SET(tid + 48, &cpuset);
                    }
                    int rc = pthread_setaffinity_np(remoteWorkers.back()->native_handle(), sizeof(cpu_set_t), &cpuset);
                    if (rc != 0) {
                        std::cerr << "Error calling pthread_setaffinity_np in copy_pool assignment: " << rc << std::endl
                                  << std::endl;
                        exit(-10);
                    }
                }

                {
                    using namespace std::chrono_literals;
                    std::this_thread::sleep_for(100ms);
                }

                s_ts = std::chrono::high_resolution_clock::now();
                sync_point_1.arrive_and_wait();
                sync_point_2.arrive_and_wait();
                e_ts = std::chrono::high_resolution_clock::now();

                wallclock += calculate_MiB_per_s((localColumnSize * localBuffers) + (remoteColumnSize * remoteBuffers), std::chrono::duration_cast<std::chrono::nanoseconds>(e_ts - s_ts).count());

                local_bwdh += (localBuffers == 0) ? 0 : get_bandwidth(localBuffers, localColumnSize, time_out_ptr, 0);
                remote_bwdh += (remoteBuffers == 0) ? 0 : get_bandwidth(remoteBuffers, remoteColumnSize, time_out_ptr, localBuffers);

                std::cout << "Local:\t" << localBuffers << "\tRemote:\t" << remoteBuffers << std::endl;
                std::cout << "Local per core bandwidth: " << local_bwdh << std::endl;
                std::cout << "Remote per core bandwidth: " << remote_bwdh << std::endl;
                std::cout << "Wallclock bandwidth: " << wallclock << std::endl;
                out << localBuffers << "\t" << remoteBuffers << "\t" << localColumnSize << "\t" << remoteColumnSize << "\t" << local_bwdh << "\t" << remote_bwdh << "\t" << wallclock << std::endl
                    << std::flush;

                std::for_each(localWorkers.begin(), localWorkers.end(), [](std::unique_ptr<std::thread>& t) { t->join(); });
                std::for_each(remoteWorkers.begin(), remoteWorkers.end(), [](std::unique_ptr<std::thread>& t) { t->join(); });
                localWorkers.clear();
                remoteWorkers.clear();
                {
                    using namespace std::chrono_literals;
                    std::this_thread::sleep_for(100ms);
                }
            }
        }
    }
}

void Benchmarks::execRDMAHashJoinBenchmark(std::string& logName) {
    cpu_set_t cpuset;

    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;
    std::cout << std::fixed << std::setprecision(7) << std::endl;
    std::vector<std::string> idents;  // = std::vector<std::string>{"col_0", "col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8", "col_9", "col_10", "col_11", "col_12", "col_13", "col_14", "col_15", "col_16", "col_17", "col_18", "col_19", "col_20", "col_21", "col_22", "col_23", "col_24", "col_25", "col_26", "col_27", "col_28", "col_29", "col_30", "col_31", "col_32", "col_33", "col_34", "col_35"};
    for (size_t i = 0; i < 16; ++i) {
        std::string name = "col_" + std::to_string(i);
        idents.push_back(name);
    }
    const size_t maxRuns = 10;
    std::chrono::_V2::system_clock::time_point s_ts;
    std::chrono::_V2::system_clock::time_point e_ts;
    out << "mode\tlocal_buffer_cnt\tremote_buffer_count\tlarger_column_size\tsmaller_column_size\ttime\tbwdh\n"
        << std::flush;
    size_t largerColumnSize = DataCatalog::getInstance().find_local(idents[0])->sizeInBytes;
    size_t smallerColumnSize = DataCatalog::getInstance().find_local(idents[0] + "_0")->sizeInBytes;

    // DataCatalog::getInstance().reconfigureChunkSize(1024 * 1024 * 4, 1024 * 1024 * 4);

    for (size_t bufferCount = 1; bufferCount <= 16; ++bufferCount) {
        for (size_t remote_buffer_count = 1; remote_buffer_count <= 10; ++remote_buffer_count) {
            for (size_t func_indicator : {1, 2, 3}) {
                for (size_t run = 0; run < maxRuns; ++run) {
                    std::barrier sync_point_1(bufferCount + 1);
                    std::barrier sync_point_2(bufferCount + 1);
                    double bwdh = 0.0;
                    size_t duration = 0;

                    std::vector<std::unique_ptr<std::thread>> workers;

                    DataCatalog::getInstance().eraseAllRemoteColumns();
                    DataCatalog::getInstance().fetchRemoteInfo();

                    auto do_work = [&](std::string ident, size_t rbc, size_t func_indicator) {
                        size_t res = 0;
                        size_t (*func)(std::pair<std::string, std::string> idents);
                        if (func_indicator == 1) {
                            func = hash_join_1;
                        } else if (func_indicator == 2) {
                            func = hash_join_2;
                        } else if (func_indicator == 3) {
                            func = hash_join_3;
                        } else {
                            return;
                        }

                        sync_point_1.arrive_and_wait();
                        for (size_t i = 0; i < rbc; ++i) {
                            std::string sub_id = ident + "_" + std::to_string(i);
                            res += func(std::make_pair(sub_id, ident));
                        }
                        sync_point_2.arrive_and_wait();
                        // std::cout << res.size() << std::endl;
                    };

                    for (size_t tid = 0; tid < bufferCount; ++tid) {
                        workers.emplace_back(std::make_unique<std::thread>(do_work, idents[tid], remote_buffer_count, func_indicator));
                        CPU_ZERO(&cpuset);
                        CPU_SET(tid, &cpuset);
                        int rc = pthread_setaffinity_np(workers.back()->native_handle(), sizeof(cpu_set_t), &cpuset);
                        if (rc != 0) {
                            std::cerr << "Error calling pthread_setaffinity_np in copy_pool assignment: " << rc << std::endl
                                      << std::endl;
                            exit(-10);
                        }
                    }

                    {
                        using namespace std::chrono_literals;
                        std::this_thread::sleep_for(100ms);
                    }

                    s_ts = std::chrono::high_resolution_clock::now();
                    sync_point_1.arrive_and_wait();
                    sync_point_2.arrive_and_wait();
                    e_ts = std::chrono::high_resolution_clock::now();

                    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(e_ts - s_ts).count();

                    bwdh = calculate_MiB_per_s((largerColumnSize + (smallerColumnSize * remote_buffer_count)) * bufferCount, duration);

                    std::cout << "Mode: " << func_indicator << std::endl;
                    std::cout << "Buffer Count: " << bufferCount << std::endl;
                    std::cout << "Join Count: " << remote_buffer_count << std::endl;
                    std::cout << "Bandwidth: " << bwdh << std::endl;
                    out << func_indicator << "\t" << bufferCount << "\t" << remote_buffer_count << "\t" << largerColumnSize << "\t" << smallerColumnSize << "\t" << duration << "\t" << bwdh << std::endl
                        << std::flush;

                    std::for_each(workers.begin(), workers.end(), [](std::unique_ptr<std::thread>& t) { t->join(); });
                    workers.clear();
                    {
                        using namespace std::chrono_literals;
                        std::this_thread::sleep_for(100ms);
                    }
                }
            }
        }
    }
}

typedef std::function<void(std::shared_future<void>* ready_future, const size_t tid, const size_t local_worker_count, std::atomic<size_t>* ready_workers, std::atomic<size_t>* complete_workers, std::condition_variable* done_cv, std::mutex* done_cv_lock, bool* all_done, uint64_t* result_ptr, long* out_time, std::pair<std::string, std::vector<std::string>> idents)> BenchKernel;

void spawn_threads(BenchKernel kernel, std::vector<size_t>& pin_list, std::vector<std::thread>& pool, std::shared_future<void>* ready_future, const size_t local_worker_count, const size_t join_cnt, std::atomic<size_t>* ready_workers, std::atomic<size_t>* complete_workers, std::condition_variable* done_cv, std::mutex* done_cv_lock, bool* all_done, uint64_t* result_ptr, long* out_time) {
    cpu_set_t cpuset;
    size_t buffer_counter = 0;
    for (auto pin : pin_list) {
        std::vector<std::string> small_tabs;
        for (size_t j = 0; j < join_cnt; ++j) {
            small_tabs.push_back("col_" + std::to_string(pin) + "_" + std::to_string(j));
        }
        std::pair<std::string, std::vector<std::string>> idents = std::make_pair("col_" + std::to_string(pin), small_tabs);

        pool.emplace_back(std::thread(kernel, ready_future, pin, local_worker_count, ready_workers, complete_workers, done_cv, done_cv_lock, all_done, result_ptr, out_time, idents));

        ++buffer_counter;

        CPU_ZERO(&cpuset);
        CPU_SET(pin, &cpuset);
        int rc = pthread_setaffinity_np(pool.back().native_handle(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np in copy_pool assignment: " << rc << std::endl
                      << std::endl;
            exit(-10);
        }
    }
}

void Benchmarks::execRDMAHashJoinPGBenchmark() {
    const uint64_t numWorkers = 16;
    const uint64_t maximalJoinCnt = 20;
    const uint64_t localColumnElements = 16000000;
    const uint64_t minBufferRatio = 5;
    const uint64_t maxBufferRatio = 5;
    const uint64_t localNumaNode = 0;
    const uint64_t remoteNumaNode = 0;
    const uint64_t maxRuns = 5;
    const uint64_t experimentCode = 1;

    const std::vector<size_t> global_pins = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79};

    // cpu_set_t cpuset;
    std::chrono::_V2::system_clock::time_point s_ts;
    std::chrono::_V2::system_clock::time_point e_ts;

    auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::stringstream logNameStream;
    logNameStream << "results/pg_hashjoin/" << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "RDMApghashjoin_" << remoteNumaNode << "_" << std::to_string(numWorkers) << "_" << std::to_string(localColumnElements * sizeof(uint64_t)) << "_" << std::to_string(experimentCode) << ".tsv";
    std::string logName = logNameStream.str();

    std::cout << "[Task] Set name: " << logName << std::endl;

    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7);
    std::cout << std::fixed << std::setprecision(7) << std::endl;
    std::vector<std::string> idents;  // = std::vector<std::string>{"col_0", "col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8", "col_9", "col_10", "col_11", "col_12", "col_13", "col_14", "col_15", "col_16", "col_17", "col_18", "col_19", "col_20", "col_21", "col_22", "col_23", "col_24", "col_25", "col_26", "col_27", "col_28", "col_29", "col_30", "col_31", "col_32", "col_33", "col_34", "col_35"};
    for (size_t i = 0; i < 16; ++i) {
        std::string name = "col_" + std::to_string(i);
        idents.push_back(name);
    }

    out << "local_buffer_cnt\tjoin_cnt\tbuffer_ratio\ttime[ns]\tbwdh\n"
        << std::flush;

    std::pair<BenchKernel, std::string> kernel_pair;

    if (experimentCode == 0) {
        kernel_pair = std::make_pair<BenchKernel, std::string>(hash_join_pg, "hashjoin");
    } else {
        kernel_pair = std::make_pair<BenchKernel, std::string>(hash_join_pg_alt, "hashjoin");
    }

    DataCatalog::getInstance().clear(true);

    DataCatalog::getInstance().reconfigureChunkSize(1024 * 1024 * 4, 1024 * 1024 * 4);

    for (size_t bufferRatio = minBufferRatio; bufferRatio <= maxBufferRatio; bufferRatio += 5) {
        std::cout << " --- Running Kernel: " << kernel_pair.second << " / Buffer Ratio: " << bufferRatio << " --- " << std::endl;

        DataCatalog::getInstance().generateBenchmarkData(numWorkers, maximalJoinCnt, localColumnElements, bufferRatio, localNumaNode, remoteNumaNode, true);

        const uint64_t remoteColumnElements = localColumnElements * bufferRatio * 0.01;
        const uint64_t largerColumnSize = localColumnElements * sizeof(uint64_t);
        const uint64_t smallerColumnSize = remoteColumnElements * sizeof(uint64_t);

        for (size_t local_buffer_cnt = 1; local_buffer_cnt <= numWorkers; local_buffer_cnt += 1) {
            for (size_t join_cnt = 1; join_cnt <= maximalJoinCnt; ++join_cnt) {
                for (size_t runs = 0; runs < maxRuns; ++runs) {
                    DataCatalog::getInstance().eraseAllRemoteColumns();
                    DataCatalog::getInstance().fetchRemoteInfo();

                    std::atomic<size_t> ready_workers = {0};
                    std::atomic<size_t> complete_workers = {0};
                    std::condition_variable done_cv;
                    std::mutex done_cv_lock;
                    bool all_done = false;

                    std::vector<std::thread> worker_pool;

                    std::promise<void> p;
                    std::shared_future<void> ready_future(p.get_future());

                    const size_t res_ptr_size = sizeof(uint64_t) * local_buffer_cnt;
                    const size_t time_out_ptr_size = sizeof(long) * local_buffer_cnt;
                    uint64_t* result_out_ptr = reinterpret_cast<uint64_t*>(numa_alloc_onnode(res_ptr_size, 0));
                    long* time_out_ptr = reinterpret_cast<long*>(numa_alloc_onnode(time_out_ptr_size, 0));

                    std::vector<size_t> local_pin_list;

                    for (size_t i = 0; i < local_buffer_cnt; ++i) {
                        local_pin_list.emplace_back(global_pins[i]);
                    }

                    std::cout << "---" << std::endl;

                    spawn_threads(kernel_pair.first, local_pin_list, worker_pool, &ready_future, local_buffer_cnt, join_cnt, &ready_workers, &complete_workers, &done_cv, &done_cv_lock, &all_done, result_out_ptr, time_out_ptr);

                    {
                        using namespace std::chrono_literals;
                        while (ready_workers != local_buffer_cnt) {
                            std::this_thread::sleep_for(1ms);
                        }
                    }

                    auto start = std::chrono::high_resolution_clock::now();
                    p.set_value();

                    {
                        std::unique_lock<std::mutex> lk(done_cv_lock);
                        done_cv.wait(lk, [&all_done] { return all_done; });
                    }

                    auto end = std::chrono::high_resolution_clock::now();
                    const size_t duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                    const double wallclock_bwdh = calculate_MiB_per_s(((largerColumnSize + smallerColumnSize) * join_cnt) * local_buffer_cnt, duration);

                    std::for_each(worker_pool.begin(), worker_pool.end(), [](std::thread& t) { t.join(); });
                    worker_pool.clear();

                    numa_free(result_out_ptr, res_ptr_size);
                    numa_free(time_out_ptr, time_out_ptr_size);

                    std::cout << "Wallclock bandwidth: " << wallclock_bwdh << std::endl;
                    out << std::to_string(local_buffer_cnt) << "\t" << std::to_string(join_cnt) << "\t" << std::to_string(bufferRatio) << "\t" << std::to_string(duration) << "\t" << wallclock_bwdh << std::endl
                        << std::flush;
                }
            }
        }
        DataCatalog::getInstance().clear(true);
    }

    std::cout << std::endl;
    std::cout << "RDMA Benchmark Hash Join PG ended." << std::endl;

    out.close();
}

void Benchmarks::executeAllBenchmarks() {
    // auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    // std::stringstream logNameStreamSW;
    // logNameStreamSW << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "AllBenchmarks_SW.log";
    // std::string logNameSW = logNameStreamSW.str();

    // std::cout << "[Task] Set name: " << logNameSW << std::endl;

    // execLocalBenchmark(logNameSW, "Local");
    // execLocalBenchmark(logNameSW, "NUMA");
    // execRemoteBenchmark(logNameSW, "RemoteLocal");
    // execRemoteBenchmark(logNameSW, "RemoteNUMA");

    // std::cout << std::endl;
    // std::cout << "Single Worker (SW) Benchmarks ended." << std::endl;

    // in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    // std::stringstream logNameStreamMW;
    // logNameStreamMW << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "AllBenchmarks_MW.log";
    // std::string logNameMW = logNameStreamMW.str();

    // std::cout << "[Task] Set name: " << logNameMW << std::endl;

    // execLocalBenchmarkMW(logNameMW, "Local");
    // execLocalBenchmarkMW(logNameMW, "NUMA");
    // execRemoteBenchmarkMW(logNameMW, "LocalRemoteLocal");
    // execRemoteBenchmarkMW(logNameMW, "LocalRemoteNUMA");
    // execRemoteBenchmarkMW(logNameMW, "NUMARemoteLocal");
    // execRemoteBenchmarkMW(logNameMW, "NUMARemoteNUMA");

    // std::cout << std::endl;
    // std::cout << "Multiple Worker (MW) Benchmarks ended." << std::endl;

    // in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    // std::stringstream logNameStreamUPIFilter;
    // logNameStreamUPIFilter << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "UPIBenchmark_filter.tsv";
    // std::string logNameUPIFilter = logNameStreamUPIFilter.str();

    // std::cout << "[Task] Set name: " << logNameUPIFilter << std::endl;

    // execUPIBenchmark<true>(logNameUPIFilter);

    // std::cout << std::endl;
    // std::cout << "UPI Benchmark Filter ended." << std::endl;

    // in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    // std::stringstream logNameStreamUPIAgg;
    // logNameStreamUPIAgg << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "UPIBenchmark_aggregate.tsv";
    // std::string logNameUPIAgg = logNameStreamUPIAgg.str();

    // std::cout << "[Task] Set name: " << logNameUPIAgg << std::endl;

    // execUPIBenchmark<false>(logNameUPIAgg);

    // std::cout << std::endl;
    // std::cout << "UPI Benchmark Aggregation ended." << std::endl;

    // in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    // std::stringstream logNameStreamRDMAAgg;
    // logNameStreamRDMAAgg << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "RDMABenchmark_aggregate.tsv";
    // std::string logNameRDMAAgg = logNameStreamRDMAAgg.str();

    // std::cout << "[Task] Set name: " << logNameRDMAAgg << std::endl;

    // execRDMABenchmark(logNameRDMAAgg);

    // std::cout << std::endl;
    // std::cout << "RDMA Benchmark Aggregation ended." << std::endl;

    // in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    // std::stringstream logNameStreamRDMAHJ;
    // logNameStreamRDMAHJ << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "RDMABenchmark_hashjoin.tsv";
    // std::string logNameRDMAHJ = logNameStreamRDMAHJ.str();

    // std::cout << "[Task] Set name: " << logNameRDMAHJ << std::endl;

    // execRDMAHashJoinBenchmark(logNameRDMAHJ);

    // std::cout << std::endl;
    // std::cout << "RDMA Benchmark Hash Join ended." << std::endl;

    execRDMAHashJoinPGBenchmark();
}