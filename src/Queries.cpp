#include <Column.h>
#include <DataCatalog.h>
#include <Queries.h>

inline void wait_col_data_ready(col_t* _col, char* _data) {
    std::unique_lock<std::mutex> lk(_col->iteratorLock);

    while (!(_data < static_cast<char*>(_col->current_end))) {
        using namespace std::chrono_literals;
        if (!_col->iterator_data_available.wait_for(lk, 500ms, [_col, _data] { return reinterpret_cast<uint64_t*>(_data) < static_cast<uint64_t*>(_col->current_end); })) {
            std::cout << "retrying...(" << reinterpret_cast<uint64_t*>(_data) << "/" << static_cast<uint64_t*>(_col->current_end) << " -- " << _col->requested_chunks << "/" << _col->received_chunks << ") " << std::flush;
        }
    }
    // std::cout << "Done. (" << reinterpret_cast<uint64_t*>(_data) << "/" << static_cast<uint64_t*>(_col->current_end) << " -- " << _col->requested_chunks << "/" << _col->received_chunks << ") " << std::endl;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> less_than(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, std::vector<size_t> in_pos) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (!prefetching && !paxed) column->request_data(!chunked);
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (prefetching && !paxed) column->request_data(!chunked);
    }

    std::vector<size_t> out_vec;
    if (isFirst) {
        out_vec.reserve(column->size);
        for (auto e = 0; e < blockSize; ++e) {
            if (data[e] < predicate) {
                out_vec.push_back(e);
            }
        }
    } else {
        out_vec.reserve(in_pos.size());
        for (auto e : in_pos) {
            if (data[e] < predicate) {
                out_vec.push_back(e);
            }
        }
    }

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> less_equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, std::vector<size_t> in_pos) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (!prefetching && !paxed) column->request_data(!chunked);
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (prefetching && !paxed) column->request_data(!chunked);
    }

    std::vector<size_t> out_vec;
    if (isFirst) {
        out_vec.reserve(column->size);
        for (auto e = 0; e < blockSize; ++e) {
            if (data[e] <= predicate) {
                out_vec.push_back(e);
            }
        }
    } else {
        out_vec.reserve(in_pos.size());
        for (auto e : in_pos) {
            if (data[e] <= predicate) {
                out_vec.push_back(e);
            }
        }
    }

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> greater_than(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, std::vector<size_t> in_pos) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (!prefetching && !paxed) column->request_data(!chunked);
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (prefetching && !paxed) column->request_data(!chunked);
    }

    std::vector<size_t> out_vec;
    if (isFirst) {
        out_vec.reserve(column->size);
        for (auto e = 0; e < blockSize; ++e) {
            if (data[e] > predicate) {
                out_vec.push_back(e);
            }
        }
    } else {
        out_vec.reserve(in_pos.size());
        for (auto e : in_pos) {
            if (data[e] > predicate) {
                out_vec.push_back(e);
            }
        }
    }

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> greater_equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, std::vector<size_t> in_pos) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (!prefetching && !paxed) column->request_data(!chunked);
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (prefetching && !paxed) column->request_data(!chunked);
    }

    std::vector<size_t> out_vec;
    if (isFirst) {
        out_vec.reserve(column->size);
        for (auto e = 0; e < blockSize; ++e) {
            if (data[e] >= predicate) {
                out_vec.push_back(e);
            }
        }
    } else {
        out_vec.reserve(in_pos.size());
        for (auto e : in_pos) {
            if (data[e] >= predicate) {
                out_vec.push_back(e);
            }
        }
    }

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, std::vector<size_t> in_pos) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (!prefetching && !paxed) column->request_data(!chunked);
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (prefetching && !paxed) column->request_data(!chunked);
    }

    std::vector<size_t> out_vec;
    if (isFirst) {
        out_vec.reserve(column->size);
        for (auto e = 0; e < blockSize; ++e) {
            if (data[e] == predicate) {
                out_vec.push_back(e);
            }
        }
    } else {
        out_vec.reserve(in_pos.size());
        for (auto e : in_pos) {
            if (data[e] == predicate) {
                out_vec.push_back(e);
            }
        }
    }

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> between_incl(col_t* column, const uint64_t predicate_1, const uint64_t predicate_2, const uint64_t offset, const size_t blockSize, std::vector<size_t> in_pos) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (!prefetching && !paxed) column->request_data(!chunked);
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (prefetching && !paxed) column->request_data(!chunked);
    }

    std::vector<size_t> out_vec;
    if (isFirst) {
        out_vec.reserve(column->size);
        for (auto e = 0; e < blockSize; ++e) {
            if (predicate_1 <= data[e] && data[e] <= predicate_2) {
                out_vec.push_back(e);
            }
        }
    } else {
        out_vec.reserve(in_pos.size());
        for (auto e : in_pos) {
            if (predicate_1 <= data[e] && data[e] <= predicate_2) {
                out_vec.push_back(e);
            }
        }
    }

    return out_vec;
};

template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false>
inline std::vector<size_t> between_excl(col_t* column, const uint64_t predicate_1, const uint64_t predicate_2, const uint64_t offset, const size_t blockSize, std::vector<size_t> in_pos) {
    auto data = reinterpret_cast<uint64_t*>(column->data) + offset;
    if (remote) {
        if (!prefetching && !paxed) column->request_data(!chunked);
        wait_col_data_ready(column, reinterpret_cast<char*>(data));
        if (prefetching && !paxed) column->request_data(!chunked);
    }

    std::vector<size_t> out_vec;
    if (isFirst) {
        out_vec.reserve(column->size);
        for (auto e = 0; e < blockSize; ++e) {
            if (predicate_1 < data[e] && data[e] < predicate_2) {
                out_vec.push_back(e);
            }
        }
    } else {
        out_vec.reserve(in_pos.size());
        for (auto e : in_pos) {
            if (predicate_1 < data[e] && data[e] < predicate_2) {
                out_vec.push_back(e);
            }
        }
    }

    return out_vec;
};

template <bool remote, bool chunked>
uint64_t bench_1_1() {
    col_t* lo_orderdate;
    col_t* lo_discount;
    col_t* lo_quantity;
    col_t* lo_extendedprice;

    col_t* d_datekey;
    col_t* d_year;

    if (remote) {
        DataCatalog::getInstance().fetchRemoteInfo();

        d_year = DataCatalog::getInstance().find_remote("d_year");
        d_year->request_data(!chunked);

        lo_discount = DataCatalog::getInstance().find_remote("lo_discount");
        lo_discount->request_data(!chunked);
        lo_quantity = DataCatalog::getInstance().find_remote("lo_quantity");
        lo_quantity->request_data(!chunked);
        lo_orderdate = DataCatalog::getInstance().find_remote("lo_orderdate");
        lo_orderdate->request_data(!chunked);
        lo_extendedprice = DataCatalog::getInstance().find_remote("lo_extendedprice");
        lo_extendedprice->request_data(!chunked);

        d_datekey = DataCatalog::getInstance().find_remote("d_datekey");
        d_datekey->request_data(!chunked);
    } else {
        lo_orderdate = DataCatalog::getInstance().find_local("lo_orderdate");
        lo_discount = DataCatalog::getInstance().find_local("lo_discount");
        lo_quantity = DataCatalog::getInstance().find_local("lo_quantity");
        lo_extendedprice = DataCatalog::getInstance().find_local("lo_extendedprice");

        d_datekey = DataCatalog::getInstance().find_local("d_datekey");
        d_year = DataCatalog::getInstance().find_local("d_year");
    }

    std::vector<col_t::col_iterator_t<uint64_t, chunked>> relevant_d;
    relevant_d.reserve(d_year->size);

    auto it_dd = d_datekey->begin<uint64_t, chunked>();
    for (auto it_dy = d_year->begin<uint64_t, chunked>(); it_dy != d_year->end<uint64_t, chunked>(); ++it_dy, ++it_dd) {
        if (*it_dy == 93) {
            relevant_d.push_back(it_dd);
        }
    }

    uint64_t sum = 0;

    size_t idx_l = 0;
    auto it_lq = lo_quantity->begin<uint64_t, chunked>();
    auto it_lo = lo_orderdate->begin<uint64_t, chunked>();
    auto it_le = lo_extendedprice->begin<uint64_t, chunked>();
    for (auto it_ld = lo_discount->begin<uint64_t, chunked>(); it_ld != lo_discount->end<uint64_t, chunked>(); ++it_ld, ++it_lq, ++it_lo, ++it_le) {
        if (10 <= *it_ld && *it_ld <= 30 && *it_lq < 25) {
            for (auto it_dd : relevant_d) {
                if (*it_lo == *it_dd) {
                    sum += ((*it_le) * (*it_ld));
                }
            }
        }
    }

    // auto chunk_counts = [](col_t* col) {
    //     std::stringstream ss;
    //     ss << col->requested_chunks << "/" << col->received_chunks << " ";
    //     return ss.str();
    // };

    // std::cout << chunk_counts(lo_orderdate);
    // std::cout << chunk_counts(lo_discount);
    // std::cout << chunk_counts(lo_quantity);
    // std::cout << chunk_counts(lo_extendedprice) << std::endl;

    return sum;
}

template <bool remote, bool chunked>
uint64_t bench_2() {
    col_t* lo_discount;
    col_t* lo_quantity;
    col_t* lo_extendedprice;

    if (remote) {
        DataCatalog::getInstance().fetchRemoteInfo();

        lo_discount = DataCatalog::getInstance().find_remote("lo_discount");
        lo_discount->request_data(!chunked);
        lo_quantity = DataCatalog::getInstance().find_remote("lo_quantity");
        lo_quantity->request_data(!chunked);
        lo_extendedprice = DataCatalog::getInstance().find_remote("lo_extendedprice");
        lo_extendedprice->request_data(!chunked);
    } else {
        lo_discount = DataCatalog::getInstance().find_local("lo_discount");
        lo_quantity = DataCatalog::getInstance().find_local("lo_quantity");
        lo_extendedprice = DataCatalog::getInstance().find_local("lo_extendedprice");
    }

    uint64_t sum = 0;

    size_t idx_l = 0;
    auto it_lq = lo_quantity->begin<uint64_t, chunked>();
    auto it_le = lo_extendedprice->begin<uint64_t, chunked>();
    auto end_it = lo_discount->end<uint64_t, chunked>();
    for (auto it_ld = lo_discount->begin<uint64_t, chunked>(); it_ld != end_it; ++it_ld, ++it_lq, ++it_le) {
        if (10 <= *it_ld && *it_ld <= 30 && *it_lq < 25) {
            sum += ((*it_le) * (*it_ld));
        }
    }

    // auto chunk_counts = [](col_t* col) {
    //     std::stringstream ss;
    //     ss << col->requested_chunks << "/" << col->received_chunks << " ";
    //     return ss.str();
    // };

    // std::cout << chunk_counts(lo_discount);
    // std::cout << chunk_counts(lo_quantity);
    // std::cout << chunk_counts(lo_extendedprice) << std::endl;

    return sum;
}

template <bool remote, bool chunked, bool paxed, bool prefetching, size_t qid>
uint64_t bench_3() {
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

        std::vector<size_t> le_idx;

        if (qid == 1) {
            le_idx = greater_than<remote, chunked, paxed, prefetching>(lo_extendedprice, 5, baseOffset, currentChunkElements,
                                                                       less_than<remote, chunked, paxed, prefetching>(lo_quantity, 25, baseOffset, currentChunkElements,
                                                                                                                      between_incl<remote, chunked, paxed, prefetching, true>(lo_discount, 10, 30, baseOffset, currentChunkElements, {})));

        } else if (qid == 2) {
            le_idx = less_than<remote, chunked, paxed, prefetching, true>(lo_quantity, 100, baseOffset, currentChunkElements, {});
        } else if (qid == 3) {
            le_idx = less_than<remote, chunked, paxed, prefetching, true>(lo_quantity, 75, baseOffset, currentChunkElements, {});
        } else if (qid == 4) {
            le_idx = less_than<remote, chunked, paxed, prefetching, true>(lo_quantity, 50, baseOffset, currentChunkElements, {});
        } else if (qid == 5) {
            le_idx = less_than<remote, chunked, paxed, prefetching, true>(lo_quantity, 25, baseOffset, currentChunkElements, {});
        } else if (qid == 6) {
            le_idx = less_than<remote, chunked, paxed, prefetching, true>(lo_quantity, 1, baseOffset, currentChunkElements, {});
        }

        for (auto idx : le_idx) {
            // sum += (data_ld[idx] * data_le[idx]);
            ++sum;
        }

        baseOffset += currentChunkElements;
        data_ld += currentChunkElements;
        data_le += currentChunkElements;
    }

    return sum;
}

template <typename Fn>
void doBenchmark(Fn&& f1, Fn&& f2, Fn&& f3, Fn&& f4, Fn&& f5, Fn&& f6, Fn&& f7, std::string logName) {
    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;

    for (size_t i = 0; i < 1; ++i) {
        auto s_ts = std::chrono::high_resolution_clock::now();
        auto sum = f1();
        auto e_ts = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> secs = e_ts - s_ts;

        out << "Local\tFull\tPipe\t000000\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Local\tFull\tPipe\t000000\t" << sum << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = f2();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\tOper\t000000\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Remote\tFull\tOper\t000000\t" << sum << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = f3();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\tPipe\t000000\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Remote\tFull\tPipe\t000000\t" << sum << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        for (uint64_t chunkSize = 1ull << 15; chunkSize < 1ull << 24; chunkSize <<= 1) {
            DataCatalog::getInstance().reconfigureChunkSize(chunkSize, chunkSize);

            s_ts = std::chrono::high_resolution_clock::now();
            sum = f4();
            e_ts = std::chrono::high_resolution_clock::now();

            secs = e_ts - s_ts;

            out << "Remote\tChunked\tOper\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << sum << "\t" << secs.count() << std::endl
                << std::flush;
            std::cout << "Remote\tChunked\tOper\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << sum << "\t" << secs.count() << std::endl;

            DataCatalog::getInstance().eraseAllRemoteColumns();

            s_ts = std::chrono::high_resolution_clock::now();
            sum = f5();
            e_ts = std::chrono::high_resolution_clock::now();

            secs = e_ts - s_ts;

            out << "Remote\tChunked\tPipe\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << sum << "\t" << secs.count() << std::endl
                << std::flush;
            std::cout << "Remote\tChunked\tPipe\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << sum << "\t" << secs.count() << std::endl;

            DataCatalog::getInstance().eraseAllRemoteColumns();
        }

        s_ts = std::chrono::high_resolution_clock::now();
        sum = f6();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tPaxed\tOper\t000000\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Remote\tPaxed\tOper\t000000\t" << sum << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = f7();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tPaxed\tPipe\t000000\t" << sum << "\t" << secs.count() << std::endl
            << std::flush;
        std::cout << "Remote\tPaxed\tPipe\t000000\t" << sum << "\t" << secs.count() << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }

    out.close();
}

void executeAllBenchmarkingQueries(std::string logName) {
    // doBenchmark(std::forward<decltype(bench_1_1<false, false>)>(bench_1_1<false, false>), std::forward<decltype(bench_1_1<true, false>)>(bench_1_1<true, false>), std::forward<decltype(bench_1_1<true, true>)>(bench_1_1<true, true>), std::string(logName + "_q1.log"));
    // doBenchmark(std::forward<decltype(bench_2<false, false>)>(bench_2<false, false>), std::forward<decltype(bench_2<true, false>)>(bench_2<true, false>), std::forward<decltype(bench_2<true, true>)>(bench_2<true, true>), std::string(logName + "_q2.log"));
    doBenchmark(std::forward<decltype(bench_3<false, false, false, false, 1>)>(bench_3<false, false, false, false, 1>),
                std::forward<decltype(bench_3<true, false, false, false, 1>)>(bench_3<true, false, false, false, 1>),
                std::forward<decltype(bench_3<true, false, false, true, 1>)>(bench_3<true, false, false, true, 1>),
                std::forward<decltype(bench_3<true, true, false, false, 1>)>(bench_3<true, true, false, false, 1>),
                std::forward<decltype(bench_3<true, true, false, true, 1>)>(bench_3<true, true, false, true, 1>),
                std::forward<decltype(bench_3<true, false, true, false, 1>)>(bench_3<true, false, true, false, 1>),
                std::forward<decltype(bench_3<true, false, true, true, 1>)>(bench_3<true, false, true, true, 1>),
                std::string(logName + "_q3_1.log"));

    doBenchmark(std::forward<decltype(bench_3<false, false, false, false, 2>)>(bench_3<false, false, false, false, 2>),
                std::forward<decltype(bench_3<true, false, false, false, 2>)>(bench_3<true, false, false, false, 2>),
                std::forward<decltype(bench_3<true, false, false, true, 2>)>(bench_3<true, false, false, true, 2>),
                std::forward<decltype(bench_3<true, true, false, false, 2>)>(bench_3<true, true, false, false, 2>),
                std::forward<decltype(bench_3<true, true, false, true, 2>)>(bench_3<true, true, false, true, 2>),
                std::forward<decltype(bench_3<true, false, true, false, 2>)>(bench_3<true, false, true, false, 2>),
                std::forward<decltype(bench_3<true, false, true, true, 2>)>(bench_3<true, false, true, true, 2>),
                std::string(logName + "_q3_2.log"));
    doBenchmark(std::forward<decltype(bench_3<false, false, false, false, 3>)>(bench_3<false, false, false, false, 3>),
                std::forward<decltype(bench_3<true, false, false, false, 3>)>(bench_3<true, false, false, false, 3>),
                std::forward<decltype(bench_3<true, false, false, true, 3>)>(bench_3<true, false, false, true, 3>),
                std::forward<decltype(bench_3<true, true, false, false, 3>)>(bench_3<true, true, false, false, 3>),
                std::forward<decltype(bench_3<true, true, false, true, 3>)>(bench_3<true, true, false, true, 3>),
                std::forward<decltype(bench_3<true, false, true, false, 3>)>(bench_3<true, false, true, false, 3>),
                std::forward<decltype(bench_3<true, false, true, true, 3>)>(bench_3<true, false, true, true, 3>),
                std::string(logName + "_q3_3.log"));
    doBenchmark(std::forward<decltype(bench_3<false, false, false, false, 4>)>(bench_3<false, false, false, false, 4>),
                std::forward<decltype(bench_3<true, false, false, false, 4>)>(bench_3<true, false, false, false, 4>),
                std::forward<decltype(bench_3<true, false, false, true, 4>)>(bench_3<true, false, false, true, 4>),
                std::forward<decltype(bench_3<true, true, false, false, 4>)>(bench_3<true, true, false, false, 4>),
                std::forward<decltype(bench_3<true, true, false, true, 4>)>(bench_3<true, true, false, true, 4>),
                std::forward<decltype(bench_3<true, false, true, false, 4>)>(bench_3<true, false, true, false, 4>),
                std::forward<decltype(bench_3<true, false, true, true, 4>)>(bench_3<true, false, true, true, 4>),
                std::string(logName + "_q3_4.log"));
    doBenchmark(std::forward<decltype(bench_3<false, false, false, false, 5>)>(bench_3<false, false, false, false, 5>),
                std::forward<decltype(bench_3<true, false, false, false, 5>)>(bench_3<true, false, false, false, 5>),
                std::forward<decltype(bench_3<true, false, false, true, 5>)>(bench_3<true, false, false, true, 5>),
                std::forward<decltype(bench_3<true, true, false, false, 5>)>(bench_3<true, true, false, false, 5>),
                std::forward<decltype(bench_3<true, true, false, true, 5>)>(bench_3<true, true, false, true, 5>),
                std::forward<decltype(bench_3<true, false, true, false, 5>)>(bench_3<true, false, true, false, 5>),
                std::forward<decltype(bench_3<true, false, true, true, 5>)>(bench_3<true, false, true, true, 5>),
                std::string(logName + "_q3_5.log"));
    doBenchmark(std::forward<decltype(bench_3<false, false, false, false, 6>)>(bench_3<false, false, false, false, 6>),
                std::forward<decltype(bench_3<true, false, false, false, 6>)>(bench_3<true, false, false, false, 6>),
                std::forward<decltype(bench_3<true, false, false, true, 6>)>(bench_3<true, false, false, true, 6>),
                std::forward<decltype(bench_3<true, true, false, false, 6>)>(bench_3<true, true, false, false, 6>),
                std::forward<decltype(bench_3<true, true, false, true, 6>)>(bench_3<true, true, false, true, 6>),
                std::forward<decltype(bench_3<true, false, true, false, 6>)>(bench_3<true, false, true, false, 6>),
                std::forward<decltype(bench_3<true, false, true, true, 6>)>(bench_3<true, false, true, true, 6>),
                std::string(logName + "_q3_6.log"));
}