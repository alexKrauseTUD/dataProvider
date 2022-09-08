#include <Column.h>
#include <DataCatalog.h>
#include <Queries.h>

#include <future>

size_t OPTIMAL_BLOCK_SIZE = 65536;

std::chrono::duration<double> waitingTime = std::chrono::duration<double>::zero();
std::chrono::duration<double> workingTime = std::chrono::duration<double>::zero();

void reset_timer() {
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
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> less_than(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
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

    auto s_ts = std::chrono::high_resolution_clock::now();
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

    workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> less_equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (reload) {
            if (!prefetching && !paxed) {
                column->request_data(!chunked);
            }
            wait_col_data_ready(column, reinterpret_cast<char*>(data));
            if (prefetching && chunked && !paxed) {
                column->request_data(!chunked);
            }
        }
    }

    auto s_ts = std::chrono::high_resolution_clock::now();
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

    workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> greater_than(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (reload) {
            if (!prefetching && !paxed) {
                column->request_data(!chunked);
            }
            wait_col_data_ready(column, reinterpret_cast<char*>(data));
            if (prefetching && chunked && !paxed) {
                column->request_data(!chunked);
            }
        }
    }

    auto s_ts = std::chrono::high_resolution_clock::now();
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

    workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> greater_equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (reload) {
            if (!prefetching && !paxed) {
                column->request_data(!chunked);
            }
            wait_col_data_ready(column, reinterpret_cast<char*>(data));
            if (prefetching && chunked && !paxed) {
                column->request_data(!chunked);
            }
        }
    }

    auto s_ts = std::chrono::high_resolution_clock::now();
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

    workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (reload) {
            if (!prefetching && !paxed) {
                column->request_data(!chunked);
            }
            wait_col_data_ready(column, reinterpret_cast<char*>(data));
            if (prefetching && chunked && !paxed) {
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

    workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> between_incl(col_t* column, const uint64_t predicate_1, const uint64_t predicate_2, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (reload) {
            if (!prefetching && !paxed) {
                column->request_data(!chunked);
            }
            wait_col_data_ready(column, reinterpret_cast<char*>(data));
            if (prefetching && chunked && !paxed) {
                column->request_data(!chunked);
            }
        }
    }

    auto s_ts = std::chrono::high_resolution_clock::now();
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

    workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> between_excl(col_t* column, const uint64_t predicate_1, const uint64_t predicate_2, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (reload) {
            if (!prefetching && !paxed) {
                column->request_data(!chunked);
            }
            wait_col_data_ready(column, reinterpret_cast<char*>(data));
            if (prefetching && chunked && !paxed) {
                column->request_data(!chunked);
            }
        }
    }

    auto s_ts = std::chrono::high_resolution_clock::now();
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

    workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t bench_1(const uint64_t predicate) {
    col_t* lo_discount;
    col_t* lo_quantity;
    col_t* lo_extendedprice;
    std::chrono::time_point<std::chrono::high_resolution_clock> s_ts;
    const std::vector<std::string> idents{"col_0", "col_1", "col_2"};

    if (remote) {
        lo_discount = DataCatalog::getInstance().find_remote("col_0");
        if (prefetching && !paxed) lo_discount->request_data(!chunked);
        lo_quantity = DataCatalog::getInstance().find_remote("col_1");
        if (prefetching && !paxed) lo_quantity->request_data(!chunked);
        lo_extendedprice = DataCatalog::getInstance().find_remote("col_2");
        if (prefetching && !paxed) lo_extendedprice->request_data(!chunked);

        if (prefetching && paxed) DataCatalog::getInstance().fetchPseudoPax(1, idents);
    } else {
        lo_discount = DataCatalog::getInstance().find_local("col_0");
        lo_quantity = DataCatalog::getInstance().find_local("col_1");
        lo_extendedprice = DataCatalog::getInstance().find_local("col_2");
    }

    size_t columnSize = lo_discount->size;

    size_t max_elems_per_chunk = 0;
    size_t currentBlockSize = max_elems_per_chunk;
    if (paxed) {
        size_t total_id_len = 0;
        for (auto& id : idents) {
            total_id_len += id.size();
        }

        const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
        const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(1)->maxBytesInPayload(appMetaSize);

        max_elems_per_chunk = ((maximumPayloadSize / idents.size()) / (sizeof(uint64_t) * 4)) * 4;
        currentBlockSize = max_elems_per_chunk;
    } else if (!(remote && (chunked || paxed))) {
        max_elems_per_chunk = columnSize;
        currentBlockSize = OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
    } else {
        max_elems_per_chunk = DataCatalog::getInstance().dataCatalog_chunkMaxSize / sizeof(uint64_t);
        if (max_elems_per_chunk <= OPTIMAL_BLOCK_SIZE / sizeof(uint64_t)) {
            currentBlockSize = max_elems_per_chunk;
        } else {
            currentBlockSize = OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
        }
    }

    uint64_t sum = 0;
    size_t baseOffset = 0;
    size_t currentChunkElementsProcessed = 0;

    auto data_le = reinterpret_cast<uint64_t*>(lo_extendedprice->data);
    auto data_ld = reinterpret_cast<uint64_t*>(lo_discount->data);

    while (baseOffset < columnSize) {
        if (remote && paxed) {
            if (!prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
            wait_col_data_ready(lo_extendedprice, reinterpret_cast<char*>(data_le));
            if (prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
        }

        const size_t elem_diff = columnSize - baseOffset;
        if (elem_diff < currentBlockSize) {
            currentBlockSize = elem_diff;
        }

        auto le_idx = greater_than<remote, chunked, paxed, prefetching>(lo_extendedprice, 5, baseOffset, currentBlockSize,
                                                                        less_than<remote, chunked, paxed, prefetching>(lo_quantity, 25, baseOffset, currentBlockSize,
                                                                                                                       between_incl<remote, chunked, paxed, prefetching, true>(lo_discount, 10, 30, baseOffset, currentBlockSize, {}, currentChunkElementsProcessed == 0), currentChunkElementsProcessed == 0),
                                                                        currentChunkElementsProcessed == 0);

        s_ts = std::chrono::high_resolution_clock::now();
        for (auto idx : le_idx) {
            sum += (data_ld[idx] * data_le[idx]);
            // ++sum;
        }
        workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

        baseOffset += currentBlockSize;
        data_ld += currentBlockSize;
        data_le += currentBlockSize;
        currentChunkElementsProcessed = baseOffset % max_elems_per_chunk;
    }

    return sum;
}

template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t bench_2(const uint64_t predicate) {
    col_t* lo_discount;
    col_t* lo_quantity;
    col_t* lo_extendedprice;
    std::chrono::time_point<std::chrono::high_resolution_clock> s_ts;
    const std::vector<std::string> idents{"col_0", "col_1", "col_2"};

    if (remote) {
        lo_quantity = DataCatalog::getInstance().find_remote("col_1");
        if (prefetching && !paxed) lo_quantity->request_data(!chunked);
        lo_discount = DataCatalog::getInstance().find_remote("col_0");
        if (prefetching && !paxed) lo_discount->request_data(!chunked);
        lo_extendedprice = DataCatalog::getInstance().find_remote("col_2");
        if (prefetching && !paxed) lo_extendedprice->request_data(!chunked);

        if (prefetching && paxed) DataCatalog::getInstance().fetchPseudoPax(1, idents);
    } else {
        lo_discount = DataCatalog::getInstance().find_local("col_0");
        lo_quantity = DataCatalog::getInstance().find_local("col_1");
        lo_extendedprice = DataCatalog::getInstance().find_local("col_2");
    }

    size_t columnSize = lo_quantity->size;

    size_t max_elems_per_chunk = 0;
    size_t currentBlockSize = max_elems_per_chunk;
    if (paxed) {
        size_t total_id_len = 0;
        for (auto& id : idents) {
            total_id_len += id.size();
        }

        const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
        const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(1)->maxBytesInPayload(appMetaSize);

        max_elems_per_chunk = ((maximumPayloadSize / idents.size()) / (sizeof(uint64_t) * 4)) * 4;
        currentBlockSize = max_elems_per_chunk;
    } else if (!(remote && (chunked || paxed))) {
        max_elems_per_chunk = columnSize;
        currentBlockSize = OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
    } else {
        max_elems_per_chunk = DataCatalog::getInstance().dataCatalog_chunkMaxSize / sizeof(uint64_t);
        if (max_elems_per_chunk <= OPTIMAL_BLOCK_SIZE / sizeof(uint64_t)) {
            currentBlockSize = max_elems_per_chunk;
        } else {
            currentBlockSize = OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
        }
    }

    uint64_t sum = 0;
    size_t baseOffset = 0;
    size_t currentChunkElementsProcessed = 0;

    auto data_le = reinterpret_cast<uint64_t*>(lo_extendedprice->data);
    auto data_ld = reinterpret_cast<uint64_t*>(lo_discount->data);

    while (baseOffset < columnSize) {
        if (remote && paxed) {
            if (!prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
            wait_col_data_ready(lo_extendedprice, reinterpret_cast<char*>(data_le));
            if (prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
        }

        const size_t elem_diff = columnSize - baseOffset;
        if (elem_diff < currentBlockSize) {
            currentBlockSize = elem_diff;
        }

        auto le_idx = less_than<remote, chunked, paxed, prefetching, true>(lo_quantity, predicate, baseOffset, currentBlockSize, {}, currentChunkElementsProcessed == 0);

        if (remote && !paxed) {
            if (currentChunkElementsProcessed == 0) {
                if (!prefetching && chunked) {
                    lo_extendedprice->request_data(!chunked);
                    lo_discount->request_data(!chunked);
                }
                wait_col_data_ready(lo_extendedprice, reinterpret_cast<char*>(data_le));
                wait_col_data_ready(lo_discount, reinterpret_cast<char*>(data_ld));
                if (prefetching && chunked) {
                    lo_extendedprice->request_data(!chunked);
                    lo_discount->request_data(!chunked);
                }
            }
        }

        s_ts = std::chrono::high_resolution_clock::now();
        for (auto idx : le_idx) {
            sum += (data_ld[idx] * data_le[idx]);
            // ++sum;
        }
        workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

        baseOffset += currentBlockSize;
        data_ld += currentBlockSize;
        data_le += currentBlockSize;
        currentChunkElementsProcessed = baseOffset % max_elems_per_chunk;
    }

    return sum;
}

template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t bench_3(const uint64_t predicate) {
    col_t* column_0;
    col_t* column_1;
    col_t* column_2;
    col_t* column_3;
    std::chrono::time_point<std::chrono::high_resolution_clock> s_ts;
    const std::vector<std::string> idents{"col_0", "col_1", "col_2", "col_3"};

    if (remote) {
        column_0 = DataCatalog::getInstance().find_remote("col_0");
        if (prefetching && !paxed) column_0->request_data(!chunked);
        column_1 = DataCatalog::getInstance().find_remote("col_1");
        if (prefetching && !paxed) column_1->request_data(!chunked);
        column_2 = DataCatalog::getInstance().find_remote("col_2");
        if (prefetching && !paxed) column_2->request_data(!chunked);
        column_3 = DataCatalog::getInstance().find_remote("col_3");
        if (prefetching && !paxed) column_3->request_data(!chunked);

        if (prefetching && paxed) DataCatalog::getInstance().fetchPseudoPax(1, idents);
    } else {
        column_0 = DataCatalog::getInstance().find_local("col_0");
        column_1 = DataCatalog::getInstance().find_local("col_1");
        column_2 = DataCatalog::getInstance().find_local("col_2");
        column_3 = DataCatalog::getInstance().find_local("col_3");
    }

    size_t columnSize = column_0->size;

    size_t max_elems_per_chunk = 0;
    size_t currentBlockSize = max_elems_per_chunk;
    if (paxed) {
        size_t total_id_len = 0;
        for (auto& id : idents) {
            total_id_len += id.size();
        }

        const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
        const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(1)->maxBytesInPayload(appMetaSize);

        max_elems_per_chunk = ((maximumPayloadSize / idents.size()) / (sizeof(uint64_t) * 4)) * 4;
        currentBlockSize = max_elems_per_chunk;
    } else if (!(remote && (chunked || paxed))) {
        max_elems_per_chunk = columnSize;
        currentBlockSize = OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
    } else {
        max_elems_per_chunk = DataCatalog::getInstance().dataCatalog_chunkMaxSize / sizeof(uint64_t);
        if (max_elems_per_chunk <= OPTIMAL_BLOCK_SIZE / sizeof(uint64_t)) {
            currentBlockSize = max_elems_per_chunk;
        } else {
            currentBlockSize = OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
        }
    }

    uint64_t sum = 0;
    size_t baseOffset = 0;
    size_t currentChunkElementsProcessed = 0;

    auto data_2 = reinterpret_cast<uint64_t*>(column_2->data);
    auto data_3 = reinterpret_cast<uint64_t*>(column_3->data);

    while (baseOffset < columnSize) {
        if (remote && paxed) {
            if (!prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
            wait_col_data_ready(column_3, reinterpret_cast<char*>(data_3));
            if (prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
        }

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

template <typename Fn>
void doBenchmarkRemotes(Fn&& f1, Fn&& f2, Fn&& f3, Fn&& f4, Fn&& f5, std::ofstream& out, const uint64_t predicate) {
    uint64_t sum = 0;
    std::chrono::time_point<std::chrono::high_resolution_clock> s_ts;
    std::chrono::time_point<std::chrono::high_resolution_clock> e_ts;
    std::chrono::duration<double> secs;

    for (size_t i = 0; i < 5; ++i) {
        reset_timer();
        DataCatalog::getInstance().fetchRemoteInfo();
        s_ts = std::chrono::high_resolution_clock::now();
        sum = f1(predicate);
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\tPipe\t" << OPTIMAL_BLOCK_SIZE << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Remote\tFull\tPipe\t" << OPTIMAL_BLOCK_SIZE << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        for (uint64_t chunkSize = 1ull << 18; chunkSize <= 1ull << 27; chunkSize <<= 1) {
            DataCatalog::getInstance().reconfigureChunkSize(chunkSize, chunkSize);

            reset_timer();
            DataCatalog::getInstance().fetchRemoteInfo();
            s_ts = std::chrono::high_resolution_clock::now();
            sum = f2(predicate);
            e_ts = std::chrono::high_resolution_clock::now();

            secs = e_ts - s_ts;

            out << "Remote\tChunked\tOper\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl
                << std::flush;
            std::cout << "Remote\tChunked\tOper\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl;

            DataCatalog::getInstance().eraseAllRemoteColumns();

            reset_timer();
            DataCatalog::getInstance().fetchRemoteInfo();
            s_ts = std::chrono::high_resolution_clock::now();
            sum = f3(predicate);
            e_ts = std::chrono::high_resolution_clock::now();

            secs = e_ts - s_ts;

            out << "Remote\tChunked\tPipe\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl
                << std::flush;
            std::cout << "Remote\tChunked\tPipe\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl;

            DataCatalog::getInstance().eraseAllRemoteColumns();
        }

        reset_timer();
        DataCatalog::getInstance().fetchRemoteInfo();
        s_ts = std::chrono::high_resolution_clock::now();
        sum = f4(predicate);
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tPaxed\tOper\t000000\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Remote\tPaxed\tOper\t000000\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        reset_timer();
        DataCatalog::getInstance().fetchRemoteInfo();
        s_ts = std::chrono::high_resolution_clock::now();
        sum = f5(predicate);
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tPaxed\tPipe\t000000\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Remote\tPaxed\tPipe\t000000\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }
}

template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t frontPage_1() {
    uint64_t sum_1 = 0;
    uint64_t sum_2 = 0;

    DataCatalog::getInstance().fetchRemoteInfo();
    sum_1 = bench_1<remote, chunked, paxed, prefetching>(0);
    DataCatalog::getInstance().eraseAllRemoteColumns();
    DataCatalog::getInstance().fetchRemoteInfo();
    sum_2 = bench_3<remote, chunked, paxed, prefetching>(0);

    return sum_1 + sum_2;
}

template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t frontPage_2() {
    uint64_t sum_1 = 0;
    uint64_t sum_2 = 0;

    DataCatalog::getInstance().fetchRemoteInfo();
    sum_1 = bench_1<remote, chunked, paxed, prefetching>(0);
    sum_2 = bench_3<remote, chunked, paxed, prefetching>(0);

    return sum_1 + sum_2;
}

template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t frontPage_3() {
    uint64_t sum_1 = 0;
    uint64_t sum_2 = 0;

    DataCatalog::getInstance().fetchRemoteInfo();
#pragma omp parallel for schedule(static, 1) num_threads(2)
    for (size_t i = 0; i < 2; ++i) {
        if (i == 0) {
            sum_1 = bench_1<remote, chunked, paxed, prefetching>(0);
        } else if (i == 1) {
            sum_2 = bench_3<remote, chunked, paxed, prefetching>(0);
        }
    }

    return sum_1 + sum_2;
}

void doBenchmarkFrontPage(std::ofstream& out) {
    uint64_t sum = 0;
    std::chrono::time_point<std::chrono::high_resolution_clock> s_ts;
    std::chrono::time_point<std::chrono::high_resolution_clock> e_ts;
    std::chrono::duration<double> secs;
    uint64_t chunkSize = 1ull << 22;

    DataCatalog::getInstance().reconfigureChunkSize(chunkSize, chunkSize);

    for (size_t i = 0; i < 5; ++i) {
        reset_timer();
        s_ts = std::chrono::high_resolution_clock::now();
        sum = frontPage_1<true, true, false, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << +chunkSize << "\t1\t" << +sum << "\t" << waitingTime.count() << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << +chunkSize << "\t1\t" << +sum << "\t" << waitingTime.count() << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
        reset_timer();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = frontPage_2<true, true, false, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << +chunkSize << "\t2\t" << +sum << "\t" << waitingTime.count() << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << +chunkSize << "\t2\t" << +sum << "\t" << waitingTime.count() << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
        reset_timer();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = frontPage_2<true, true, false, true>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << +chunkSize << "\t3\t" << +sum << "\t" << waitingTime.count() << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << +chunkSize << "\t3\t" << +sum << "\t" << waitingTime.count() << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
        reset_timer();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = frontPage_3<true, true, false, true>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << +chunkSize << "\t4\t" << +sum << "\t" << waitingTime.count() / 2 << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << +chunkSize << "\t4\t" << +sum << "\t" << waitingTime.count() / 2 << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
        reset_timer();
    }
}

void executeRemoteBenchmarkingQueries(std::string& logName) {
    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;
    std::cout << std::fixed << std::setprecision(7) << std::endl;
    const std::array predicates{1, 25, 50, 75, 100};

    // <bool remote, bool chunked, bool paxed, bool prefetching>

    doBenchmarkRemotes(bench_1<true, false, false, true>,  // Remote Full Pipe/Oper (difference due to block-wise evaluation not significant)
                       bench_1<true, true, false, false>,  // Remote Chunked Oper
                       bench_1<true, true, false, true>,   // Remote Chunked Pipe
                       bench_1<true, false, true, false>,  // Remote Paxed Oper
                       bench_1<true, false, true, true>,   // Remote Paxed Pipe
                       out, 0);

    for (const auto predicate : predicates) {
        doBenchmarkRemotes(bench_2<true, false, false, true>,  // Remote Full Pipe/Oper (difference due to block-wise evaluation not significant)
                           bench_2<true, true, false, false>,  // Remote Chunked Oper
                           bench_2<true, true, false, true>,   // Remote Chunked Pipe
                           bench_2<true, false, true, false>,  // Remote Paxed Oper
                           bench_2<true, false, true, true>,   // Remote Paxed Pipe
                           out, predicate);
    }

    out.close();
}

void executeFrontPageBenchmarkingQueries(std::string& logName) {
    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;
    std::cout << std::fixed << std::setprecision(7) << std::endl;

    // <bool remote, bool chunked, bool paxed, bool prefetching>

    doBenchmarkFrontPage(out);

    out.close();
}

void executeLocalBenchmarkingQueries(std::string& logName, std::string locality) {
    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;
    std::cout << std::fixed << std::setprecision(7) << std::endl;
    const std::array predicates{0, 1, 25, 50, 75, 100};
    uint64_t sum = 0;
    std::chrono::_V2::system_clock::time_point s_ts;
    std::chrono::_V2::system_clock::time_point e_ts;

    for (const auto predicate : predicates) {
        for (size_t i = 0; i < 20; ++i) {
            reset_timer();

            if (predicate == 0) {
                s_ts = std::chrono::high_resolution_clock::now();
                sum = bench_1<false, false, false, false>(predicate);
                e_ts = std::chrono::high_resolution_clock::now();
            } else {
                s_ts = std::chrono::high_resolution_clock::now();
                sum = bench_2<false, false, false, false>(predicate);
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