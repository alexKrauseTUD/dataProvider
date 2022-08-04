#include <Column.h>
#include <DataCatalog.h>
#include <Queries.h>

inline void wait_col_data_ready(col_t* _col, char* _data) {
    std::unique_lock<std::mutex> lk(_col->iteratorLock);
    if (!(_data < static_cast<char*>(_col->current_end))) {
        _col->iterator_data_available.wait(lk, [_col, _data] { return reinterpret_cast<uint64_t*>(_data) < static_cast<uint64_t*>(_col->current_end); });
    }
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> less_than(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (!prefetching && !paxed) column->request_data(!chunked);
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (prefetching && !paxed) column->request_data(!chunked);
    }

    std::vector<size_t> out_vec;
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

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> less_equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (!prefetching && !paxed) column->request_data(!chunked);
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (prefetching && !paxed) column->request_data(!chunked);
    }

    std::vector<size_t> out_vec;
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

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> greater_than(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (!prefetching && !paxed) column->request_data(!chunked);
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (prefetching && !paxed) column->request_data(!chunked);
    }

    std::vector<size_t> out_vec;
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

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> greater_equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (!prefetching && !paxed) column->request_data(!chunked);
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (prefetching && !paxed) column->request_data(!chunked);
    }

    std::vector<size_t> out_vec;
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

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (!prefetching && !paxed) column->request_data(!chunked);
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (prefetching && !paxed) column->request_data(!chunked);
    }

    std::vector<size_t> out_vec;
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

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> between_incl(col_t* column, const uint64_t predicate_1, const uint64_t predicate_2, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (!prefetching && !paxed) column->request_data(!chunked);
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (prefetching && !paxed) column->request_data(!chunked);
    }

    std::vector<size_t> out_vec;
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

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> between_excl(col_t* column, const uint64_t predicate_1, const uint64_t predicate_2, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (!prefetching && !paxed) column->request_data(!chunked);
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (prefetching && !paxed) column->request_data(!chunked);
    }

    std::vector<size_t> out_vec;
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
uint64_t bench_1(uint64_t predicate) {
    col_t* lo_discount;
    col_t* lo_quantity;
    col_t* lo_extendedprice;
    std::vector<std::string> idents{"lo_discount", "lo_quantity", "lo_extendedprice"};

    if (remote) {
        DataCatalog::getInstance().fetchRemoteInfo();

        lo_discount = DataCatalog::getInstance().find_remote("lo_discount");
        if (prefetching && !paxed) lo_discount->request_data(!chunked);
        lo_quantity = DataCatalog::getInstance().find_remote("lo_quantity");
        if (prefetching && !paxed) lo_quantity->request_data(!chunked);
        lo_extendedprice = DataCatalog::getInstance().find_remote("lo_extendedprice");
        if (prefetching && !paxed) lo_extendedprice->request_data(!chunked);

        if (prefetching && paxed) DataCatalog::getInstance().fetchPseudoPax(1, idents);
    } else {
        lo_discount = DataCatalog::getInstance().find_local("lo_discount");
        lo_quantity = DataCatalog::getInstance().find_local("lo_quantity");
        lo_extendedprice = DataCatalog::getInstance().find_local("lo_extendedprice");
    }

    size_t max_elems_per_chunk = 0;
    if (chunked) {
        max_elems_per_chunk = DataCatalog::getInstance().dataCatalog_chunkMaxSize / sizeof(uint64_t);
    } else if (paxed) {
        size_t total_id_len = 0;
        for (auto& id : idents) {
            total_id_len += id.size();
        }

        const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
        const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(1)->maxBytesInPayload(appMetaSize);

        max_elems_per_chunk = maximumPayloadSize / sizeof(uint64_t) / idents.size();
    } else {
        max_elems_per_chunk = lo_discount->size;
    }

    uint64_t sum = 0;
    size_t baseOffset = 0;
    size_t currentChunkElements = max_elems_per_chunk;

    size_t columnSize = lo_discount->size;

    auto data_le = reinterpret_cast<uint64_t*>(lo_extendedprice->data);
    auto data_ld = reinterpret_cast<uint64_t*>(lo_discount->data);

    while (baseOffset < lo_discount->size) {
        if (remote && paxed) {
            if (!prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
            wait_col_data_ready(lo_extendedprice, reinterpret_cast<char*>(data_le));
            if (prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
        }

        const size_t elem_diff = columnSize - baseOffset;
        if (elem_diff < max_elems_per_chunk) {
            currentChunkElements = elem_diff;
        }

        auto le_idx = greater_than<remote, chunked, paxed, prefetching>(lo_extendedprice, 5, baseOffset, currentChunkElements,
                                                                        less_than<remote, chunked, paxed, prefetching>(lo_quantity, 25, baseOffset, currentChunkElements,
                                                                                                                       between_incl<remote, chunked, paxed, prefetching, true>(lo_discount, 10, 30, baseOffset, currentChunkElements, {})));

        for (auto idx : le_idx) {
            sum += (data_ld[idx] * data_le[idx]);
            // ++sum;
        }

        baseOffset += currentChunkElements;
        data_ld += currentChunkElements;
        data_le += currentChunkElements;
    }

    return sum;
}

template <bool remote, bool chunked, bool paxed, bool prefetching>
uint64_t bench_2(const uint64_t predicate) {
    col_t* lo_discount;
    col_t* lo_quantity;
    col_t* lo_extendedprice;
    std::vector<std::string> idents{"lo_discount", "lo_quantity", "lo_extendedprice"};

    if (remote) {
        DataCatalog::getInstance().fetchRemoteInfo();

        lo_discount = DataCatalog::getInstance().find_remote("lo_discount");
        if (prefetching && !paxed) lo_discount->request_data(!chunked);
        lo_quantity = DataCatalog::getInstance().find_remote("lo_quantity");
        if (prefetching && !paxed) lo_quantity->request_data(!chunked);
        lo_extendedprice = DataCatalog::getInstance().find_remote("lo_extendedprice");
        if (prefetching && !paxed) lo_extendedprice->request_data(!chunked);

        if (prefetching && paxed) DataCatalog::getInstance().fetchPseudoPax(1, idents);
    } else {
        lo_discount = DataCatalog::getInstance().find_local("lo_discount");
        lo_quantity = DataCatalog::getInstance().find_local("lo_quantity");
        lo_extendedprice = DataCatalog::getInstance().find_local("lo_extendedprice");
    }

    size_t max_elems_per_chunk = 0;
    if (chunked) {
        max_elems_per_chunk = DataCatalog::getInstance().dataCatalog_chunkMaxSize / sizeof(uint64_t);
    } else if (paxed) {
        size_t total_id_len = 0;
        for (auto& id : idents) {
            total_id_len += id.size();
        }

        const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
        const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(1)->maxBytesInPayload(appMetaSize);

        max_elems_per_chunk = maximumPayloadSize / sizeof(uint64_t) / idents.size();
    } else {
        max_elems_per_chunk = lo_discount->size;
    }

    uint64_t sum = 0;
    size_t baseOffset = 0;
    size_t currentChunkElements = max_elems_per_chunk;

    size_t columnSize = lo_discount->size;

    auto data_le = reinterpret_cast<uint64_t*>(lo_extendedprice->data);
    auto data_ld = reinterpret_cast<uint64_t*>(lo_discount->data);

    while (baseOffset < columnSize) {
        if (remote && paxed) {
            if (!prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
            wait_col_data_ready(lo_extendedprice, reinterpret_cast<char*>(data_le));
            if (prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
        }

        const size_t elem_diff = columnSize - baseOffset;
        if (elem_diff < max_elems_per_chunk) {
            currentChunkElements = elem_diff;
        }

        auto le_idx = less_than<remote, chunked, paxed, prefetching, true>(lo_quantity, predicate, baseOffset, currentChunkElements, {});

        for (auto idx : le_idx) {
            if (remote && !paxed) {
                if (!prefetching) {
                    lo_extendedprice->request_data(!chunked);
                    lo_discount->request_data(!chunked);
                }
                wait_col_data_ready(lo_extendedprice, reinterpret_cast<char*>(data_le));
                wait_col_data_ready(lo_discount, reinterpret_cast<char*>(data_ld));
                if (prefetching) {
                    lo_extendedprice->request_data(!chunked);
                    lo_discount->request_data(!chunked);
                }
            }

            sum += (data_ld[idx] * data_le[idx]);
            // ++sum;
        }

        baseOffset += currentChunkElements;
        data_ld += currentChunkElements;
        data_le += currentChunkElements;
    }

    return sum;
}

template <typename Fn>
void doBenchmark(Fn&& f1, Fn&& f2, Fn&& f3, Fn&& f4, Fn&& f5, Fn&& f6, Fn&& f7, std::ofstream& out, uint64_t predicate) {
    for (size_t i = 0; i < 5; ++i) {
        auto s_ts = std::chrono::high_resolution_clock::now();
        auto sum = f1(predicate);
        auto e_ts = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> secs = e_ts - s_ts;

        out << "Local\tFull\tPipe\t000000\t" << +predicate << "\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Local\tFull\tPipe\t000000\t" << +predicate << "\t" << sum << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = f2(predicate);
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\tOper\t000000\t" << +predicate << "\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Remote\tFull\tOper\t000000\t" << +predicate << "\t" << sum << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = f3(predicate);
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\tPipe\t000000\t" << +predicate << "\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Remote\tFull\tPipe\t000000\t" << +predicate << "\t" << sum << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        for (uint64_t chunkSize = 1ull << 15; chunkSize <= 1ull << 25; chunkSize <<= 1) {
            DataCatalog::getInstance().reconfigureChunkSize(chunkSize, chunkSize);

            s_ts = std::chrono::high_resolution_clock::now();
            sum = f4(predicate);
            e_ts = std::chrono::high_resolution_clock::now();

            secs = e_ts - s_ts;

            out << "Remote\tChunked\tOper\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << +predicate << "\t" << sum << "\t" << secs.count() << std::endl
                << std::flush;
            std::cout << "Remote\tChunked\tOper\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << +predicate << "\t" << sum << "\t" << secs.count() << std::endl;

            DataCatalog::getInstance().eraseAllRemoteColumns();

            s_ts = std::chrono::high_resolution_clock::now();
            sum = f5(predicate);
            e_ts = std::chrono::high_resolution_clock::now();

            secs = e_ts - s_ts;

            out << "Remote\tChunked\tPipe\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << +predicate << "\t" << sum << "\t" << secs.count() << std::endl
                << std::flush;
            std::cout << "Remote\tChunked\tPipe\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << +predicate << "\t" << sum << "\t" << secs.count() << std::endl;

            DataCatalog::getInstance().eraseAllRemoteColumns();
        }

        s_ts = std::chrono::high_resolution_clock::now();
        sum = f6(predicate);
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tPaxed\tOper\t000000\t" << +predicate << "\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Remote\tPaxed\tOper\t000000\t" << +predicate << "\t" << sum << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = f7(predicate);
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tPaxed\tPipe\t000000\t" << +predicate << "\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Remote\tPaxed\tPipe\t000000\t" << +predicate << "\t" << sum << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }
}

void executeAllBenchmarkingQueries(std::string& logName) {
    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;

    doBenchmark(bench_1<false, false, false, false>,
                bench_1<true, false, false, false>,
                bench_1<true, false, false, true>,
                bench_1<true, true, false, false>,
                bench_1<true, true, false, true>,
                bench_1<true, false, true, false>,
                bench_1<true, false, true, true>,
                out, 0);

    doBenchmark(bench_2<false, false, false, false>,
                bench_2<true, false, false, false>,
                bench_2<true, false, false, true>,
                bench_2<true, true, false, false>,
                bench_2<true, true, false, true>,
                bench_2<true, false, true, false>,
                bench_2<true, false, true, true>,
                out, 100);
    doBenchmark(bench_2<false, false, false, false>,
                bench_2<true, false, false, false>,
                bench_2<true, false, false, true>,
                bench_2<true, true, false, false>,
                bench_2<true, true, false, true>,
                bench_2<true, false, true, false>,
                bench_2<true, false, true, true>,
                out, 75);
    doBenchmark(bench_2<false, false, false, false>,
                bench_2<true, false, false, false>,
                bench_2<true, false, false, true>,
                bench_2<true, true, false, false>,
                bench_2<true, true, false, true>,
                bench_2<true, false, true, false>,
                bench_2<true, false, true, true>,
                out, 50);
    doBenchmark(bench_2<false, false, false, false>,
                bench_2<true, false, false, false>,
                bench_2<true, false, false, true>,
                bench_2<true, true, false, false>,
                bench_2<true, true, false, true>,
                bench_2<true, false, true, false>,
                bench_2<true, false, true, true>,
                out, 25);
    doBenchmark(bench_2<false, false, false, false>,
                bench_2<true, false, false, false>,
                bench_2<true, false, false, true>,
                bench_2<true, true, false, false>,
                bench_2<true, true, false, true>,
                bench_2<true, false, true, false>,
                bench_2<true, false, true, true>,
                out, 1);

    out.close();
}