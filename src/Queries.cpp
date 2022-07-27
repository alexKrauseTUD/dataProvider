#include <Column.h>
#include <DataCatalog.h>
#include <Queries.h>

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

template <bool remote, bool chunked>
uint64_t bench_3() {
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

    auto chunk_counts = [](col_t* col) {
        std::stringstream ss;
        ss << col->requested_chunks << "/" << col->received_chunks << " ";
        return ss.str();
    };

    auto wait_col_data_ready = [](col_t* _col, char* _data) {
        std::unique_lock<std::mutex> lk(_col->iteratorLock);

        while (!(_data < static_cast<char*>(_col->current_end))) {
            using namespace std::chrono_literals;
            if (!_col->iterator_data_available.wait_for(lk, 500ms, [_col, _data] { return reinterpret_cast<uint64_t*>(_data) < static_cast<uint64_t*>(_col->current_end); })) {
                std::cout << "retrying...(" << reinterpret_cast<uint64_t*>(_data) << "/" << static_cast<uint64_t*>(_col->current_end) << " -- " << _col->requested_chunks << "/" << _col->received_chunks << ") " << std::flush;
            }
        }
        // std::cout << "Done. (" << reinterpret_cast<uint64_t*>(_data) << "/" << static_cast<uint64_t*>(_col->current_end) << " -- " << _col->requested_chunks << "/" << _col->received_chunks << ") " << std::endl;
    };

    auto between_ld = [lo_discount, wait_col_data_ready, chunk_counts](uint64_t* data, size_t elem_count) -> std::vector<size_t> {
        if (remote) {
            // if (chunked) { std::cout << "Waiting LD..." << std::flush; }
            wait_col_data_ready(lo_discount, reinterpret_cast<char*>(data));
            if (chunked) {
                lo_discount->request_data(!chunked);
                // std::cout << chunk_counts( lo_discount ) << std::flush;
            }
        }

        std::vector<size_t> out_vec;
        out_vec.reserve(elem_count);
        for (size_t i = 0; i < elem_count; ++i) {
            if (10 <= data[i] && data[i] <= 30) {
                out_vec.push_back(i);
            }
        }
        return out_vec;
    };

    auto lt_lq = [lo_quantity, wait_col_data_ready, chunk_counts](uint64_t* data, std::vector<size_t> in_pos) -> std::vector<size_t> {
        if (remote) {
            // if (chunked) { std::cout << "LQ..." << std::flush; }
            wait_col_data_ready(lo_quantity, reinterpret_cast<char*>(data));
            if (chunked) {
                lo_quantity->request_data(!chunked);
                // std::cout << chunk_counts( lo_quantity ) << std::flush;
            }
        }

        std::vector<size_t> out_vec;
        out_vec.reserve(in_pos.size());
        for (auto e : in_pos) {
            if (data[e] < 25) {
                out_vec.push_back(e);
            }
        }
        return out_vec;
    };

    auto gt_le = [lo_extendedprice, wait_col_data_ready, chunk_counts](uint64_t* data, std::vector<size_t> in_pos) -> std::vector<size_t> {
        if (remote) {
            // if (chunked) { std::cout << "LE..." << std::flush; }
            wait_col_data_ready(lo_extendedprice, reinterpret_cast<char*>(data));
            if (chunked) {
                lo_extendedprice->request_data(!chunked);
                // std::cout << chunk_counts( lo_extendedprice ) << std::flush;
            }
            // if (chunked) { std::cout << " Done." << std::endl; }
        }

        std::vector<size_t> out_vec;
        out_vec.reserve(in_pos.size());
        for (auto e : in_pos) {
            if (data[e] > 5) {
                out_vec.push_back(e);
            }
        }
        return out_vec;
    };

    uint64_t* data_ld = static_cast<uint64_t*>(lo_discount->data);
    uint64_t* data_lq = static_cast<uint64_t*>(lo_quantity->data);
    uint64_t* data_le = static_cast<uint64_t*>(lo_extendedprice->data);

    size_t max_elems_per_chunk = 0;
    if (chunked) {
        max_elems_per_chunk = DataCatalog::getInstance().chunkMaxSize / sizeof(uint64_t);
    } else {
        max_elems_per_chunk = lo_discount->size;
    }

    uint64_t* data_end = reinterpret_cast<uint64_t*>(static_cast<char*>(lo_discount->data) + lo_discount->sizeInBytes);

    if (remote) {
        // std::cout << "Waiting LD..." << std::flush;
        wait_col_data_ready(lo_discount, static_cast<char*>(lo_discount->data));
        // std::cout << chunk_counts( lo_discount ) << std::flush;
        // std::cout << "LQ..." << std::flush;
        wait_col_data_ready(lo_quantity, static_cast<char*>(lo_quantity->data));
        // std::cout << chunk_counts( lo_quantity ) << std::flush;
        // std::cout << "LE..." << std::flush;
        wait_col_data_ready(lo_extendedprice, static_cast<char*>(lo_extendedprice->data));
        // std::cout << chunk_counts( lo_extendedprice ) << std::flush;
        // std::cout << "Done." << std::endl;
    }

    uint64_t sum = 0;
    size_t chunk = 0;
    size_t base_offset = 0;

    while (data_ld < data_end) {
        // std::cout << "Iteration " << chunk << std::endl;
        const size_t elem_diff = data_end - data_ld;
        if (elem_diff < max_elems_per_chunk) {
            base_offset += chunk * max_elems_per_chunk;
            max_elems_per_chunk = elem_diff;
        } else {
            base_offset += chunk * max_elems_per_chunk;
        }

        auto le_idx = gt_le(data_le, lt_lq(data_lq, between_ld(data_ld, max_elems_per_chunk)));
        for (auto idx : le_idx) {
            // sum += base_offset + idx;  // fix idx for chunk vs full, hopefully
            ++sum;
        }
        ++chunk;
        data_ld = data_ld + max_elems_per_chunk;
        data_lq = data_lq + max_elems_per_chunk;
        data_le = data_le + max_elems_per_chunk;
    }

    // std::cout << chunk_counts(lo_discount);
    // std::cout << chunk_counts(lo_quantity);
    // std::cout << chunk_counts(lo_extendedprice) << std::endl;

    return sum;
}

using bench_func = std::function<uint64_t()>;

void doBenchmark(bench_func& f1, bench_func& f2, bench_func& f3, std::string ident1, std::string ident2, std::string ident3, std::ofstream& out) {
    for (size_t i = 0; i < 10; ++i) {
        auto s_ts = std::chrono::high_resolution_clock::now();
        auto sum = f1();
        auto e_ts = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> secs = e_ts - s_ts;

        out << ident1 << +DataCatalog::getInstance().chunkMaxSize << "\t" << +DataCatalog::getInstance().chunkThreshold << "\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << ident1 << +DataCatalog::getInstance().chunkMaxSize << "\t" << +DataCatalog::getInstance().chunkThreshold << "\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = f2();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << ident2 << +DataCatalog::getInstance().chunkMaxSize << "\t" << +DataCatalog::getInstance().chunkThreshold << "\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << ident2 << +DataCatalog::getInstance().chunkMaxSize << "\t" << +DataCatalog::getInstance().chunkThreshold << "\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = f3();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << ident3 << +DataCatalog::getInstance().chunkMaxSize << "\t" << +DataCatalog::getInstance().chunkThreshold << "\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << ident3 << +DataCatalog::getInstance().chunkMaxSize << "\t" << +DataCatalog::getInstance().chunkThreshold << "\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }
}

void executeBenchmarkingQuery_1(std::string logName) {
    uint64_t sum;
    std::chrono::_V2::system_clock::time_point s_ts;
    std::chrono::_V2::system_clock::time_point e_ts;
    std::chrono::duration<double> secs;

    logName = logName + "_q1.log";

    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;

    bench_func call_local = []() -> uint64_t {
        return bench_1_1<false, false>();
    };
    bench_func call_remote_full = []() -> uint64_t {
        return bench_1_1<true, false>();
    };
    bench_func call_remote_chunk = []() -> uint64_t {
        return bench_1_1<true, true>();
    };

    doBenchmark(call_local, call_remote_full, call_remote_chunk, "Local\tFull\t", "Remote\tFull\t", "Remote\tChunked\t", out);
    doBenchmark(call_local, call_remote_chunk, call_remote_full, "Local\tFull\t", "Remote\tChunked\t", "Remote\tFull\t", out);
    doBenchmark(call_remote_full, call_local, call_remote_chunk, "Remote\tFull\t", "Local\tFull\t", "Remote\tChunked\t", out);
    doBenchmark(call_remote_full, call_remote_chunk, call_local, "Remote\tFull\t", "Remote\tChunked\t", "Local\tFull\t", out);
    doBenchmark(call_remote_chunk, call_remote_full, call_local, "Remote\tChunked\t", "Remote\tFull\t", "Local\tFull\t", out);
    doBenchmark(call_remote_chunk, call_local, call_remote_full, "Remote\tChunked\t", "Local\tFull\t", "Remote\tFull\t", out);

    out.close();
}

void executeBenchmarkingQuery_2(std::string logName) {
    uint64_t sum;
    std::chrono::_V2::system_clock::time_point s_ts;
    std::chrono::_V2::system_clock::time_point e_ts;
    std::chrono::duration<double> secs;

    logName = logName + "_q2.log";

    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;

    bench_func call_local = []() -> uint64_t {
        return bench_2<false, false>();
    };
    bench_func call_remote_full = []() -> uint64_t {
        return bench_2<true, false>();
    };
    bench_func call_remote_chunk = []() -> uint64_t {
        return bench_2<true, true>();
    };

    doBenchmark(call_local, call_remote_full, call_remote_chunk, "Local\tFull\t", "Remote\tFull\t", "Remote\tChunked\t", out);
    doBenchmark(call_local, call_remote_chunk, call_remote_full, "Local\tFull\t", "Remote\tChunked\t", "Remote\tFull\t", out);
    doBenchmark(call_remote_full, call_local, call_remote_chunk, "Remote\tFull\t", "Local\tFull\t", "Remote\tChunked\t", out);
    doBenchmark(call_remote_full, call_remote_chunk, call_local, "Remote\tFull\t", "Remote\tChunked\t", "Local\tFull\t", out);
    doBenchmark(call_remote_chunk, call_remote_full, call_local, "Remote\tChunked\t", "Remote\tFull\t", "Local\tFull\t", out);
    doBenchmark(call_remote_chunk, call_local, call_remote_full, "Remote\tChunked\t", "Local\tFull\t", "Remote\tFull\t", out);

    out.close();
}

void executeBenchmarkingQuery_3(std::string logName) {
    uint64_t sum;
    std::chrono::_V2::system_clock::time_point s_ts;
    std::chrono::_V2::system_clock::time_point e_ts;
    std::chrono::duration<double> secs;

    logName = logName + "_q3.log";

    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;

    bench_func call_local = []() -> uint64_t {
        return bench_3<false, false>();
    };
    bench_func call_remote_full = []() -> uint64_t {
        return bench_3<true, false>();
    };
    bench_func call_remote_chunk = []() -> uint64_t {
        return bench_3<true, true>();
    };

    doBenchmark(call_local, call_remote_full, call_remote_chunk, "Local\tFull\t", "Remote\tFull\t", "Remote\tChunked\t", out);
    doBenchmark(call_local, call_remote_chunk, call_remote_full, "Local\tFull\t", "Remote\tChunked\t", "Remote\tFull\t", out);
    doBenchmark(call_remote_full, call_local, call_remote_chunk, "Remote\tFull\t", "Local\tFull\t", "Remote\tChunked\t", out);
    doBenchmark(call_remote_full, call_remote_chunk, call_local, "Remote\tFull\t", "Remote\tChunked\t", "Local\tFull\t", out);
    doBenchmark(call_remote_chunk, call_remote_full, call_local, "Remote\tChunked\t", "Remote\tFull\t", "Local\tFull\t", out);
    doBenchmark(call_remote_chunk, call_local, call_remote_full, "Remote\tChunked\t", "Local\tFull\t", "Remote\tFull\t", out);

    out.close();
}

void executeAllBenchmarkingQueries(std::string logName) {
    executeBenchmarkingQuery_1(logName);
    executeBenchmarkingQuery_2(logName);
    // executeBenchmarkingQuery_3(logName);
}