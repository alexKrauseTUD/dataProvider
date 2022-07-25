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

    return sum;
}

template <bool remote, bool chunked>
uint64_t bench_2() {
    col_t* lo_orderdate;
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

    return sum;
}

void executeBenchmarkingQuery_1() {
    uint64_t sum;
    std::chrono::_V2::system_clock::time_point s_ts;
    std::chrono::_V2::system_clock::time_point e_ts;
    std::chrono::duration<double> secs;

    auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::stringstream logNameStream;
    logNameStream << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "query_1_1.log";
    std::string logName = logNameStream.str();
    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;

    // L, RF, RC
    for (size_t i = 0; i < 10; ++i) {
        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<false, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<true, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<true, true>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }

    // L, RC, RF
    for (size_t i = 0; i < 10; ++i) {
        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<false, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<true, true>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<true, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }

    // RF, L, RC
    for (size_t i = 0; i < 10; ++i) {
        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<true, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<false, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<true, true>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }

    // RF, RC, L
    for (size_t i = 0; i < 10; ++i) {
        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<true, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<true, true>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<false, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }

    // RC, RF, L
    for (size_t i = 0; i < 10; ++i) {
        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<true, true>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<true, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<false, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }

    //  RC, L, RF
    for (size_t i = 0; i < 10; ++i) {
        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<true, true>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<false, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_1_1<true, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }

    out.close();
}

void executeBenchmarkingQuery_2() {
    uint64_t sum;
    std::chrono::_V2::system_clock::time_point s_ts;
    std::chrono::_V2::system_clock::time_point e_ts;
    std::chrono::duration<double> secs;

    auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::stringstream logNameStream;
    logNameStream << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "query_2.log";
    std::string logName = logNameStream.str();
    std::ofstream out;
    out.open(logName, std::ios_base::app);
    out << std::fixed << std::setprecision(7) << std::endl;

    // L, RF, RC
    for (size_t i = 0; i < 10; ++i) {
        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<false, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<true, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<true, true>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }

    // L, RC, RF
    for (size_t i = 0; i < 10; ++i) {
        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<false, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<true, true>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<true, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }

    // RF, L, RC
    for (size_t i = 0; i < 10; ++i) {
        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<true, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<false, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<true, true>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }

    // RF, RC, L
    for (size_t i = 0; i < 10; ++i) {
        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<true, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<true, true>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<false, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }

    // RC, RF, L
    for (size_t i = 0; i < 10; ++i) {
        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<true, true>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<true, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<false, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }

    //  RC, L, RF
    for (size_t i = 0; i < 10; ++i) {
        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<true, true>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tChunked\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<false, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Local\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();

        s_ts = std::chrono::high_resolution_clock::now();
        sum = bench_2<true, false>();
        e_ts = std::chrono::high_resolution_clock::now();

        secs = e_ts - s_ts;

        out << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl
            << std::flush;
        std::cout << "Remote\tFull\t" << secs.count() << "\t" << sum << std::endl;

        DataCatalog::getInstance().eraseAllRemoteColumns();
    }

    out.close();
}

void executeAllBenchmarkingQueries() {
    executeBenchmarkingQuery_1();
    executeBenchmarkingQuery_2();
}