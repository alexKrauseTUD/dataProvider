#include <Column.h>
#include <ConnectionManager.h>
#include <DataCatalog.h>
#include <Queries.h>
#include <TaskManager.h>
#include <Utility.h>

#include <thread>

#include "Benchmarks.hpp"
#include "Worker.hpp"

DataCatalog::DataCatalog() {
    auto createColLambda = [this]() -> void {
        std::cout << "[DataCatalog]";

        std::size_t elemCnt;
        char dataType;
        std::string ident;
        std::string input;

        std::cout << " Which datatype? uint8_t [s] uint64_t [l] float [f] double [d]" << std::endl;
        std::cin >> dataType;
        std::cin.clear();
        std::cin.ignore(10000, '\n');
        std::cout << "How many data elements?" << std::endl;
        std::cin >> elemCnt;
        std::cin.clear();
        std::cin.ignore(10000, '\n');
        std::cout << "Column name" << std::endl;
        std::cin >> ident;
        std::cin.clear();
        std::cin.ignore(10000, '\n');

        col_data_t type;
        switch (dataType) {
            case 's': {
                type = col_data_t::gen_smallint;
                break;
            }
            case 'l': {
                type = col_data_t::gen_bigint;
                break;
            }
            case 'f': {
                type = col_data_t::gen_float;
                break;
            }
            case 'd': {
                type = col_data_t::gen_double;
                break;
            }
            default: {
                std::cout << "Incorrect datatype, aborting." << std::endl;
                return;
            }
        }

        this->generate(ident, type, elemCnt, 0);
    };

    auto printColLambda = [this]() -> void {
        std::cout << "Print info for [1] local [2] remote" << std::endl;
        size_t locality;
        std::cin >> locality;
        std::cin.clear();
        std::cin.ignore(10000, '\n');

        col_dict_t dict;
        switch (locality) {
            case 1: {
                this->print_all();
                dict = cols;
                break;
            }
            case 2: {
                dict = remote_cols;
                this->print_all_remotes();
                break;
            }
            default: {
                std::cout << "[DataCatalog] No valid value selected, aborting." << std::endl;
                return;
            }
        }

        std::string ident;
        std::cout << "Which column?" << std::endl;
        std::cin >> ident;
        std::cin.clear();
        std::cin.ignore(10000, '\n');

        auto col_it = dict.find(ident);
        if (col_it != dict.end()) {
            std::cout << col_it->second->print_data_head() << std::endl;
        } else {
            std::cout << "[DataCatalog] Invalid column name." << std::endl;
        }
    };

    auto iteratorTestLambda = [this]() -> void {
        fetchRemoteInfo();
        std::cout << "Print info for [1] local [2] remote" << std::endl;
        size_t locality;
        std::cin >> locality;
        std::cin.clear();
        std::cin.ignore(10000, '\n');

        col_dict_t dict;
        switch (locality) {
            case 1: {
                this->print_all();
                dict = cols;
                break;
            }
            case 2: {
                dict = remote_cols;
                this->print_all_remotes();
                break;
            }
            default: {
                std::cout << "[DataCatalog] No valid value selected, aborting." << std::endl;
                return;
            }
        }

        std::string ident;
        std::cout << "Which column?" << std::endl;
        std::cin >> ident;
        std::cin.clear();
        std::cin.ignore(10000, '\n');

        auto col_it = dict.find(ident);
        if (col_it != dict.end()) {
            for (size_t i = 0; i < 10; ++i) {
                {
                    std::size_t count = 0;
                    auto cur_col = find_remote(ident);
                    auto t_start = std::chrono::high_resolution_clock::now();
                    cur_col->request_data(true);
                    auto cur_end = cur_col->end<uint64_t, false>();
                    auto it = cur_col->begin<uint64_t, false>();
                    std::cout << "Starting..." << std::endl;
                    for (; it != cur_end; ++it) {
                        count += *it;
                    }
                    auto t_end = std::chrono::high_resolution_clock::now();
                    std::cout << "(Full) I found " << count << " CS (" << cur_col->calc_checksum() << ") in " << static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(t_end - t_start).count()) / 1000 << "ms" << std::endl;
                    std::cout << "\t" << (static_cast<double>(cur_col->sizeInBytes) / 1024 / 1024 / 1024) / (static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(t_end - t_start).count()) / 1000 / 1000) << "GB/s" << std::endl;
                }
                eraseAllRemoteColumns();
                fetchRemoteInfo();
                {
                    std::size_t count = 0;
                    auto cur_col = find_remote(ident);
                    auto t_start = std::chrono::high_resolution_clock::now();
                    cur_col->request_data(false);
                    auto cur_end = cur_col->end<uint64_t, true>();
                    auto it = cur_col->begin<uint64_t, true>();
                    std::cout << "Starting..." << std::endl;
                    for (; it != cur_end; ++it) {
                        count += *it;
                    }
                    auto t_end = std::chrono::high_resolution_clock::now();
                    std::cout << "(Chunked) I found " << count << " CS (" << cur_col->calc_checksum() << ") in " << static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(t_end - t_start).count()) / 1000 << "ms" << std::endl;
                    std::cout << "\t" << (static_cast<double>(cur_col->sizeInBytes) / 1024 / 1024 / 1024) / (static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(t_end - t_start).count()) / 1000 / 1000) << "GB/s" << std::endl;
                }
                eraseAllRemoteColumns();
                fetchRemoteInfo();
            }
        } else {
            std::cout << "[DataCatalog] Invalid column name." << std::endl;
        }
    };

    auto retrieveRemoteColsLambda = [this]() -> void {
        ConnectionManager::getInstance().sendOpCode(1, static_cast<uint8_t>(catalog_communication_code::send_column_info), true);
    };

    auto logLambda = [this]() -> void {
        std::cout << "Print info for [1] local [2] remote" << std::endl;
        size_t locality;
        std::cin >> locality;
        std::cin.clear();
        std::cin.ignore(10000, '\n');

        col_dict_t dict;
        switch (locality) {
            case 1: {
                this->print_all();
                dict = cols;
                break;
            }
            case 2: {
                dict = remote_cols;
                this->print_all_remotes();
                break;
            }
            default: {
                std::cout << "[DataCatalog] No valid value selected, aborting." << std::endl;
                return;
            }
        }

        std::string ident;
        std::cout << "Which column?" << std::endl;
        std::cin >> ident;
        std::cin.clear();
        std::cin.ignore(10000, '\n');

        auto col_it = dict.find(ident);
        if (col_it != dict.end()) {
            std::string s(col_it->first + ".log");
            col_it->second->log_to_file(s);
        } else {
            std::cout << "[DataCatalog] Invalid column name." << std::endl;
        }
    };

    auto pseudoPaxLambda = [this]() -> void {
        col_t* ld = find_remote("lo_orderdate");

        if (ld == nullptr) {
            fetchRemoteInfo();
        }

        ld = find_remote("lo_orderdate");
        col_t* lq = find_remote("lo_quantity");
        col_t* le = find_remote("lo_extendedprice");

        std::cout << "[DataCatalog] Fetching PseudoPAX for lo_orderdate, lo_quantity, lo_extendedprice" << std::endl;
        fetchPseudoPax(1, {"lo_orderdate", "lo_quantity", "lo_extendedprice"});
    };

    // auto benchQueriesRemote = [this]() -> void {
    //     using namespace std::chrono_literals;

    //     for (uint8_t num_rb = 2; num_rb <= 2; ++num_rb) {
    //         for (uint64_t bytes = 1ull << 18; bytes <= 1ull << 20; bytes <<= 1) {
    //             auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    //             std::stringstream logNameStream;
    //             logNameStream << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "QB_" << +num_rb << "_" << +bytes << "_Remote.log";
    //             std::string logName = logNameStream.str();

    //             std::cout << "[Task] Set name: " << logName << std::endl;

    //             buffer_config_t bufferConfig = {.num_own_send_threads = num_rb,
    //                                             .num_own_receive_threads = num_rb,
    //                                             .num_remote_send_threads = num_rb,
    //                                             .num_remote_receive_threads = num_rb,
    //                                             .num_own_receive = num_rb,
    //                                             .size_own_receive = bytes,
    //                                             .num_remote_receive = num_rb,
    //                                             .size_remote_receive = bytes,
    //                                             .num_own_send = num_rb,
    //                                             .size_own_send = bytes,
    //                                             .num_remote_send = num_rb,
    //                                             .size_remote_send = bytes,
    //                                             .meta_info_size = 16};

    //             ConnectionManager::getInstance().reconfigureBuffer(1, bufferConfig);

    //             std::cout << "[main] Used connection with id '1' and " << +num_rb << " remote receive buffer (size for one remote receive: " << memordma::Utility::GetBytesReadable(bytes) << ")" << std::endl;
    //             std::cout << std::endl;

    //             executeRemoteBenchmarkingQueries(logName);
    //         }

    //         std::cout << std::endl;
    //         std::cout << "QueryBench ended." << std::endl;
    //     }
    // };

    // auto benchQueriesLocal = [this]() -> void {
    //     using namespace std::chrono_literals;

    //     auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    //     std::stringstream logNameStream;
    //     logNameStream << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "Local.log";
    //     std::string logName = logNameStream.str();

    //     std::cout << "[Task] Set name: " << logName << std::endl;

    //     executeLocalBenchmarkingQueries(logName, "Local");

    //     std::cout << std::endl;
    //     std::cout << "NUMAQueryBench ended." << std::endl;
    // };

    // auto benchQueriesNUMA = [this]() -> void {
    //     using namespace std::chrono_literals;

    //     auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    //     std::stringstream logNameStream;
    //     logNameStream << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "NUMA.log";
    //     std::string logName = logNameStream.str();

    //     std::cout << "[Task] Set name: " << logName << std::endl;

    //     executeLocalBenchmarkingQueries(logName, "NUMA");

    //     std::cout << std::endl;
    //     std::cout << "NUMAQueryBench ended." << std::endl;
    // };

    // auto benchQueriesFrontPage = [this]() -> void {
    //     using namespace std::chrono_literals;
    //     uint8_t num_rb = 2;
    //     uint64_t bytes = 1ull << 19;

    //     auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    //     std::stringstream logNameStream;
    //     logNameStream << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "FP_" << +num_rb << "_" << +bytes << "_Remote.log";
    //     std::string logName = logNameStream.str();

    //     std::cout << "[Task] Set name: " << logName << std::endl;

    //     buffer_config_t bufferConfig = {.num_own_send_threads = num_rb,
    //                                     .num_own_receive_threads = num_rb,
    //                                     .num_remote_send_threads = num_rb,
    //                                     .num_remote_receive_threads = num_rb,
    //                                     .num_own_receive = num_rb,
    //                                     .size_own_receive = bytes,
    //                                     .num_remote_receive = num_rb,
    //                                     .size_remote_receive = bytes,
    //                                     .num_own_send = num_rb,
    //                                     .size_own_send = bytes,
    //                                     .num_remote_send = num_rb,
    //                                     .size_remote_send = bytes,
    //                                     .meta_info_size = 16};

    //     ConnectionManager::getInstance().reconfigureBuffer(1, bufferConfig);

    //     std::cout << "[main] Used connection with id '1' and " << +num_rb << " remote receive buffer (size for one remote receive: " << memordma::Utility::GetBytesReadable(bytes) << ")" << std::endl;
    //     std::cout << std::endl;

    //     executeFrontPageBenchmarkingQueries(logName);

    //     std::cout << std::endl;
    //     std::cout << "QueryBench ended." << std::endl;
    // };

    // auto benchQueriesRemoteMT = [this]() -> void {
    //     using namespace std::chrono_literals;

    //     for (uint8_t num_rb = 2; num_rb <= 2; ++num_rb) {
    //         for (uint64_t bytes = 1ull << 18; bytes <= 1ull << 20; bytes <<= 1) {
    //             auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    //             std::stringstream logNameStream;
    //             logNameStream << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "QB-MT_" << +num_rb << "_" << +bytes << "_Remote.log";
    //             std::string logName = logNameStream.str();

    //             std::cout << "[Task] Set name: " << logName << std::endl;

    //             buffer_config_t bufferConfig = {.num_own_send_threads = num_rb,
    //                                             .num_own_receive_threads = num_rb,
    //                                             .num_remote_send_threads = num_rb,
    //                                             .num_remote_receive_threads = num_rb,
    //                                             .num_own_receive = num_rb,
    //                                             .size_own_receive = bytes,
    //                                             .num_remote_receive = num_rb,
    //                                             .size_remote_receive = bytes,
    //                                             .num_own_send = num_rb,
    //                                             .size_own_send = bytes,
    //                                             .num_remote_send = num_rb,
    //                                             .size_remote_send = bytes,
    //                                             .meta_info_size = 16};

    //             ConnectionManager::getInstance().reconfigureBuffer(1, bufferConfig);

    //             std::cout << "[main] Used connection with id '1' and " << +num_rb << " remote receive buffer (size for one remote receive: " << memordma::Utility::GetBytesReadable(bytes) << ")" << std::endl;
    //             std::cout << std::endl;

    //             executeRemoteMTBenchmarkingQueries(logName);
    //         }

    //         std::cout << std::endl;
    //         std::cout << "QueryBench ended." << std::endl;
    //     }
    // };

    // auto benchQueriesLocalMT = [this]() -> void {
    //     using namespace std::chrono_literals;

    //     auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    //     std::stringstream logNameStream;
    //     logNameStream << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "QB-MT_Local.log";
    //     std::string logName = logNameStream.str();

    //     std::cout << "[Task] Set name: " << logName << std::endl;

    //     executeLocalMTBenchmarkingQueries(logName, "Local");

    //     std::cout << std::endl;
    //     std::cout << "NUMAQueryBench ended." << std::endl;
    // };

    // auto benchQueriesNUMAMT = [this]() -> void {
    //     using namespace std::chrono_literals;

    //     auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    //     std::stringstream logNameStream;
    //     logNameStream << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "QB-MT_NUMA.log";
    //     std::string logName = logNameStream.str();

    //     std::cout << "[Task] Set name: " << logName << std::endl;

    //     executeLocalMTBenchmarkingQueries(logName, "NUMA");

    //     std::cout << std::endl;
    //     std::cout << "NUMAQueryBench ended." << std::endl;
    // };

    auto benchmarksAllLambda = [this]() -> void {
        Benchmarks::getInstance().executeAllBenchmarks();
    };

    TaskManager::getInstance().registerTask(std::make_shared<Task>("createColumn", "[DataCatalog] Create new column", createColLambda));
    TaskManager::getInstance().registerTask(std::make_shared<Task>("printAllColumn", "[DataCatalog] Print all stored columns", [this]() -> void { this->print_all(); this->print_all_remotes(); }));
    TaskManager::getInstance().registerTask(std::make_shared<Task>("printColHead", "[DataCatalog] Print first 10 values of column", printColLambda));
    TaskManager::getInstance().registerTask(std::make_shared<Task>("retrieveRemoteCols", "[DataCatalog] Ask for remote columns", retrieveRemoteColsLambda));
    TaskManager::getInstance().registerTask(std::make_shared<Task>("logColumn", "[DataCatalog] Log a column to file", logLambda));
    // TaskManager::getInstance().registerTask(std::make_shared<Task>("benchmarkRemote", "[DataCatalog] Execute Single Pipeline Remote", benchQueriesRemote));
    // TaskManager::getInstance().registerTask(std::make_shared<Task>("benchmarkLocal", "[DataCatalog] Execute Single Pipeline Local", benchQueriesLocal));
    // TaskManager::getInstance().registerTask(std::make_shared<Task>("benchmarkNUMA", "[DataCatalog] Execute Single Pipeline NUMA", benchQueriesNUMA));
    // TaskManager::getInstance().registerTask(std::make_shared<Task>("benchmarkFrontPage", "[DataCatalog] Execute Pipeline FrontPage", benchQueriesFrontPage));
    // TaskManager::getInstance().registerTask(std::make_shared<Task>("benchmarkMTMP", "[DataCatalog] Execute Multi Pipeline Remote", benchQueriesRemoteMT));
    // TaskManager::getInstance().registerTask(std::make_shared<Task>("benchmarkLocalMTMP", "[DataCatalog] Execute Multi Pipeline MT Local", benchQueriesLocalMT));
    // TaskManager::getInstance().registerTask(std::make_shared<Task>("benchmarkNUMAMTMP", "[DataCatalog] Execute Multi Pipeline MT NUMA", benchQueriesNUMAMT));
    TaskManager::getInstance().registerTask(std::make_shared<Task>("benchmarksAll", "[DataCatalog] Execute All Benchmarks", benchmarksAllLambda));
    TaskManager::getInstance().registerTask(std::make_shared<Task>("itTest", "[DataCatalog] IteratorTest", iteratorTestLambda));
    TaskManager::getInstance().registerTask(std::make_shared<Task>("pseudoPaxTest", "[DataCatalog] PseudoPaxTest", pseudoPaxLambda));

    /* Message Layout
     * [ header_t | payload ]
     * Payload layout
     * [ columnInfoCount | [col_network_info, identLength, ident]* ]
     */
    CallbackFunction cb_sendInfo = [this](const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) -> void {
        reset_buffer();
        // std::cout << "[DataCatalog] Hello from the Data Catalog" << std::endl;
        const uint8_t code = static_cast<uint8_t>(catalog_communication_code::receive_column_info);
        const size_t columnCount = cols.size();

        size_t totalPayloadSize = sizeof(size_t) + (sizeof(col_network_info) * cols.size());
        for (auto col : cols) {
            totalPayloadSize += sizeof(size_t);    // store size of ident length in a 64bit int
            totalPayloadSize += col.first.size();  // actual c_string
        }
        // std::cout << "[DataCatalog] Callback - allocating " << totalPayloadSize << " for column data." << std::endl;
        char* data = (char*)malloc(totalPayloadSize);
        char* tmp = data;

        // How many columns does the receiver need to read
        memcpy(tmp, &columnCount, sizeof(size_t));
        tmp += sizeof(size_t);

        for (auto col : cols) {
            col_network_info cni(col.second->size, col.second->datatype);
            // Meta data of column, element count and data type
            memcpy(tmp, &cni, sizeof(cni));
            tmp += sizeof(cni);

            // Length of column name
            size_t identlen = col.first.size();
            memcpy(tmp, &identlen, sizeof(size_t));
            tmp += sizeof(size_t);

            // Actual column name
            memcpy(tmp, col.first.c_str(), identlen);
            tmp += identlen;
        }
        ConnectionManager::getInstance().sendData(conId, data, totalPayloadSize, nullptr, 0, code);

        // Release temporary buffer
        free(data);
    };

    /* Message Layout
     * [ header_t | payload ]
     * Payload layout
     * [ columnInfoCount | [col_network_info, identLength, ident]* ]
     */
    CallbackFunction cb_receiveInfo = [this](const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) -> void {
        // package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->buf);
        char* data = rcv_buffer->getBufferPtr() + sizeof(package_t::header_t);

        size_t colCnt;
        memcpy(&colCnt, data, sizeof(size_t));
        data += sizeof(size_t);
        // std::stringstream ss;
        // ss << "[DataCatalog] Received data for " << colCnt << " columns" << std::endl;

        col_network_info cni(0, col_data_t::gen_void);
        size_t identlen;
        for (size_t i = 0; i < colCnt; ++i) {
            memcpy(&cni, data, sizeof(cni));
            data += sizeof(cni);

            memcpy(&identlen, data, sizeof(size_t));
            data += sizeof(size_t);

            std::string ident(data, identlen);
            data += identlen;

            std::lock_guard<std::mutex> _lkb(remote_info_lock);
            // ss << "[DataCatalog] Column: " << ident << " - " << cni.size_info << " elements of type " << col_network_info::col_data_type_to_string(cni.type_info) << std::endl;
            if (!remote_col_info.contains(ident)) {
                // ss << "Ident not found!";
                remote_col_info.insert({ident, cni});
                if (!find_remote(ident)) {
                    add_remote_column(ident, cni);
                }
            }
        }

        reset_buffer();

        if (colCnt > 0) {
            /* Message Layout
             * [ header_t | AppMetaData | payload ]
             * AppMetaData Layout
             * <empty> == 0
             * Payload layout
             * [ columnNameLength, columnName ]
             */
            auto fetchLambda = [this, conId]() -> void {
                print_all_remotes();
                std::cout << "[DataCatalog] Fetch data for which column?" << std::endl;
                std::string ident;
                std::cin >> ident;
                std::cin.clear();
                std::cin.ignore(10000, '\n');

                std::cout << "Fetch mode [1] whole column [2] (next) chunk" << std::endl;
                size_t mode;
                std::cin >> mode;
                std::cin.clear();
                std::cin.ignore(10000, '\n');

                col_dict_t dict;
                switch (mode) {
                    case 1: {
                        fetchColStub(conId, ident, true);
                        break;
                    }
                    case 2: {
                        fetchColStub(conId, ident, false);
                        break;
                    }
                    default: {
                        std::cout << "[DataCatalog] No valid value selected, aborting." << std::endl;
                        return;
                    }
                }
            };

            if (!TaskManager::getInstance().hasTask("fetchColDataFromRemote")) {
                TaskManager::getInstance().registerTask(std::make_shared<Task>("fetchColDataFromRemote", "[DataCatalog] Fetch data from specific remote column", fetchLambda));
                std::cout << "[DataCatalog] Registered new Task!" << std::endl;
            }
            remoteInfoReady();
        }
        // std::cout << ss.str() << std::endl;
    };

    /* Extract column name and prepare sending its data
     * Message Layout
     * [ header_t | AppMetaData | payload ]
     * AppMetaData Layout
     * <empty> == 0
     * Payload layout
     * [ columnNameLength, columnName ]
     */
    CallbackFunction cb_fetchCol = [this](const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) -> void {
        // package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->buf);
        char* data = rcv_buffer->getBufferPtr() + sizeof(package_t::header_t);
        // char* column_data = data + head->payload_start;

        size_t identSz;
        memcpy(&identSz, data, sizeof(size_t));
        data += sizeof(size_t);

        std::string ident(data, identSz);

        auto col = cols.find(ident);

        // std::cout << "[DataCatalog] Remote requested data for column '" << ident << "' with ident len " << identSz << " and CS " << col->second->calc_checksum() << std::endl;

        reset_buffer();

        if (col != cols.end()) {
            /* Message Layout
             * [ header_t | ident_len, ident, col_data_type | col_data ]
             */
            const size_t appMetaSize = sizeof(size_t) + identSz + sizeof(col_data_t);
            char* appMetaData = (char*)malloc(appMetaSize);
            char* tmp = appMetaData;

            memcpy(tmp, &identSz, sizeof(size_t));
            tmp += sizeof(size_t);

            memcpy(tmp, ident.c_str(), identSz);
            tmp += identSz;

            memcpy(tmp, &col->second->datatype, sizeof(col_data_t));

            ConnectionManager::getInstance().sendData(conId, (char*)col->second->data, col->second->sizeInBytes, appMetaData, appMetaSize, static_cast<uint8_t>(catalog_communication_code::receive_column_data));

            free(appMetaData);
        }
    };

    /* Message Layout
     * [ header_t | ident_len, ident, col_data_type | col_data ]
     */
    CallbackFunction cb_receiveCol = [this](const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) -> void {
        // Package header
        package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->getBufferPtr());
        // Start of AppMetaData
        char* data = rcv_buffer->getBufferPtr() + sizeof(package_t::header_t);
        // Actual column data payload
        char* column_data = data + head->payload_start;

        size_t identSz;
        memcpy(&identSz, data, sizeof(size_t));
        data += sizeof(size_t);

        std::string ident(data, identSz);
        data += identSz;

        col_data_t data_type;
        memcpy(&data_type, data, sizeof(col_data_t));

        // uint64_t* ptr = reinterpret_cast<uint64_t*>(column_data);
        // // for (size_t i = 0; i < head->current_payload_size / sizeof(uint64_t); ++i) {
        // //     if (ptr[i] == 0 && ptr[i + 1] == 0 && ptr[i + 2] == 0) {
        // //         std::cout << ident << "\t" << i << "\t";
        // //         for (size_t k = 0; k < 10; ++k, ++i) {
        // //             if (i >= head->current_payload_size / sizeof(uint64_t)) break;
        // //             std::cout << ptr[i] << "\t";
        // //         }
        // //         std::cout << std::endl;
        // //     }
        // // }
        // bool allZero = true;
        // for (size_t i = 0; i < head->current_payload_size / sizeof(uint64_t); ++i) {
        //     allZero = allZero && ptr[i] == 0;
        // }

        // if (allZero) {
        //     std::cout << "There was a whole Buffer filled with 0 for " << ident << std::endl;
        // }

        // std::cout << "Total Message size - header_t: " << sizeof(package_t::header_t) << " AppMetaDataSize: " << head->payload_start << " Payload size: " << head->current_payload_size << " Sum: " << sizeof(package_t::header_t) + head->payload_start + head->current_payload_size << std::endl;
        // std::cout << "Received data for column: " << ident
        //           << " of type " << col_network_info::col_data_type_to_string(data_type)
        //           << ": " << head->current_payload_size
        //           << " Bytes of " << head->total_data_size
        //           << " current message offset to Base: " << head->payload_position_offset
        //           << " AppMetaDataSize: " << head->payload_start << " Bytes"
        //           << std::endl;

        std::unique_lock<std::mutex> lk(remote_info_lock);
        auto col = find_remote(ident);

        auto col_network_info_iterator = remote_col_info.find(ident);
        lk.unlock();

        // Column object already created?
        if (col == nullptr) {
            // No Col object, did we even fetch remote info beforehand?
            if (col_network_info_iterator != remote_col_info.end()) {
                col = add_remote_column(ident, col_network_info_iterator->second);
            } else {
                std::cout << "[DataCatalog] No Network info for received column " << ident << ", fetch column info first -- discarding message" << std::endl;
                return;
            }
        }

        // Write currently received data to the column object
        col->append_chunk(head->payload_position_offset, head->current_payload_size, column_data);
        // Update network info struct to check if we received all data
        lk.lock();
        std::lock_guard<std::mutex> lg(col->iteratorLock);
        col_network_info_iterator->second.received_bytes += head->current_payload_size;

        // std::cout << ident << "\t" << head->package_number << "\t" << head->payload_position_offset << "\t" << head->total_data_size << "\t" << col_network_info_iterator->second.received_bytes << std::endl;

        if (col_network_info_iterator->second.received_bytes == col->sizeInBytes) {
            col->is_complete = true;
            ++col->received_chunks;
            col->advance_end_pointer(head->total_data_size);
            // std::cout << "[DataCatalog] Received all data for column: " << ident << std::endl;
        }

        reset_buffer();
    };

    // Send a chunk of a column to the requester
    CallbackFunction cb_fetchColChunk = [this](const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) -> void {
        // package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->buf);
        char* data = rcv_buffer->getBufferPtr() + sizeof(package_t::header_t);
        // char* column_data = data + head->payload_start;

        size_t identSz;
        memcpy(&identSz, data, sizeof(size_t));
        data += sizeof(size_t);

        std::string ident(data, identSz);

        reset_buffer();

        // std::cout << "Looking for column " << ident << " to send over." << std::endl;
        inflightLock.lock();
        auto col_info_it = cols.find(ident);

        // Column is available
        if (col_info_it != cols.end()) {
            inflight_col_info_t* info;

            auto inflight_info_it = inflight_cols.find(ident);
            // No intermediate for requested column. Creating a new entry in the dict.
            if (inflight_info_it == inflight_cols.end()) {
                inflight_col_info_t new_info;
                new_info.col = col_info_it->second;
                new_info.curr_offset = 0;
                inflight_cols.insert({ident, new_info});
                info = &inflight_cols.find(ident)->second;
            } else {
                info = &inflight_info_it->second;
            }
            inflightLock.unlock();

            if (info->curr_offset == (info->col)->sizeInBytes) {
                // std::cout << "[DataCatalog] Column " << ident << " reset offset to 0." << std::endl;
                info->curr_offset = 0;
            }

            /* Message Layout
             * [ header_t | chunk_offset ident_len, ident, col_data_type | col_data ]
             */
            const size_t appMetaSize = sizeof(size_t) + sizeof(size_t) + identSz + sizeof(col_data_t);
            char* appMetaData = (char*)malloc(appMetaSize);
            char* tmp = appMetaData;

            // Write chunk offset relative to column start into meta data
            memcpy(tmp, &info->curr_offset, sizeof(size_t));
            tmp += sizeof(size_t);

            // Write size of following ident string into meta data
            memcpy(tmp, &identSz, sizeof(size_t));
            tmp += sizeof(size_t);

            // Write ident string into meta data
            memcpy(tmp, ident.c_str(), identSz);
            tmp += identSz;

            // Append underlying column data type
            memcpy(tmp, &info->col->datatype, sizeof(col_data_t));

            const size_t remaining_size = info->col->sizeInBytes - info->curr_offset;

            const size_t chunk_size = (remaining_size > dataCatalog_chunkMaxSize) ? dataCatalog_chunkMaxSize : remaining_size;
            char* data_start = static_cast<char*>(info->col->data) + info->curr_offset;

            // Increment offset after setting message variables
            info->curr_offset += chunk_size;
            // std::cout << "Sent chunk. Offset now: " << info->curr_offset << " Total col size: " << info->col->sizeInBytes << std::endl;

            ConnectionManager::getInstance().sendData(conId, data_start, chunk_size, appMetaData, appMetaSize, static_cast<uint8_t>(catalog_communication_code::receive_column_chunk));

            free(appMetaData);
        }
        inflightLock.unlock();
    };

    /* Message Layout
     * [ header_t | chunk_offset ident_len, ident, col_data_type | col_data ]
     */
    CallbackFunction cb_receiveColChunk = [this](const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) -> void {
        // std::cout << "[DataCatalog] Received a message with a (part of a) column chnunk." << std::endl;
        // Package header
        package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->getBufferPtr());
        // Start of AppMetaData
        char* data = rcv_buffer->getBufferPtr() + sizeof(package_t::header_t);
        // Actual column data payload
        char* column_data = data + head->payload_start;

        size_t chunk_offset;
        memcpy(&chunk_offset, data, sizeof(size_t));
        data += sizeof(size_t);

        size_t identSz;
        memcpy(&identSz, data, sizeof(size_t));
        data += sizeof(size_t);

        std::string ident(data, identSz);
        data += identSz;

        col_data_t data_type;
        memcpy(&data_type, data, sizeof(col_data_t));

        std::unique_lock<std::mutex> lk(remote_info_lock);
        auto col = find_remote(ident);

        auto col_network_info_iterator = remote_col_info.find(ident);
        lk.unlock();

        // Column object already created?
        if (col == nullptr) {
            // No Col object, did we even fetch remote info beforehand?
            if (col_network_info_iterator != remote_col_info.end()) {
                col = add_remote_column(ident, col_network_info_iterator->second);
            } else {
                std::cout << "[DataCatalog] No Network info for received column " << ident << ", fetch column info first -- discarding message. Current CNI:" << std::endl;
                for (auto k : remote_col_info) {
                    std::cout << k.first << " " << &k.second << std::endl;
                }
                return;
            }
        }

        /*
         * chunk_offset / head->total_data_size denotes this is the n^th chunk
         * head->payload_position_offset describes the position of this message
         * inside the column chunk, if the buffer was not large enough to send the whole chunk.
         */
        const size_t chunk_total_offset = chunk_offset + head->payload_position_offset;

        // Write currently received data to the column object
        col->append_chunk(chunk_total_offset, head->current_payload_size, column_data);
        // Update network info struct to check if we received all data
        lk.lock();
        std::lock_guard<std::mutex> lg(col->iteratorLock);
        col_network_info_iterator->second.received_bytes += head->current_payload_size;
        // lk.unlock();

        if (col_network_info_iterator->second.received_bytes == col->sizeInBytes) {
            col->advance_end_pointer(head->total_data_size);
            col->is_complete = true;
            ++col->received_chunks;
            // std::cout << "[DataCatalog] Received all data for column: " << ident << std::endl;
        } else if (col_network_info_iterator->second.received_bytes % head->total_data_size == 0) {
            col->advance_end_pointer(head->total_data_size);
            ++col->received_chunks;
            // std::cout << "[DataCatalog] Latest chunk of '" << ident << "' received completely." << std::endl;
        }

        reset_buffer();
    };

    /* Extract column name and prepare sending its data
     * Message Layout
     * [ header_t | col_cnt | [col_ident_size]+ | [col_ident]+ ]
     */
    CallbackFunction cb_fetchPseudoPax = [this](const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) -> void {
        // package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->buf);
        char* data = rcv_buffer->getBufferPtr() + sizeof(package_t::header_t);
        size_t* ident_lens = reinterpret_cast<size_t*>(data);

        // Advance data to the first ident character
        data += (ident_lens[0] + 1) * sizeof(size_t);

        // Prepare ident vector
        std::vector<std::string> idents;
        idents.reserve(ident_lens[0]);

        for (size_t i = 0; i < ident_lens[0]; ++i) {
            idents.emplace_back(data, ident_lens[i + 1]);
            data += ident_lens[i + 1];
            // std::cout << "Requesting pseudo pax for " << idents.back() << std::endl;
        }

        paxInflightLock.lock();
        bool allPresent = true;
        // All columns present?
        std::vector<col_dict_t::iterator> col_its;
        col_its.reserve(idents.size());
        size_t total_id_len = 0;
        for (auto& id : idents) {
            auto col_info_it = cols.find(id);
            allPresent &= col_info_it != cols.end();
            col_its.push_back(col_info_it);
            total_id_len += id.size();
            // std::cout << "Column '" << id << "' found? " << ((col_info_it != cols.end()) ? "Yes" : "No") << std::endl;
        }

        // Build key to identify currently fetched columns - Order Preserving!
        std::string global_ident;
        global_ident.reserve(total_id_len + idents.size() - 1);
        {
            size_t offset = 0;
            const char delim = '-';
            for (size_t i = 0; i < idents.size(); ++i) {
                const auto& id = idents[i];
                global_ident += id;
                if (i < idents.size() - 1) {
                    global_ident += delim;
                }
            }
        }  // lo_orderdate-lo_quantity-lo_extendedprice

        // std::cout << "Global Identifier built: " << global_ident << std::endl;

        // All columns are available
        if (allPresent) {
            pax_inflight_col_info_t* info;

            auto inflight_info_it = pax_inflight_cols.find(global_ident);
            // No intermediate for requested column. Creating a new entry in the dict.
            if (inflight_info_it == pax_inflight_cols.end()) {
                pax_inflight_col_info_t* new_info = new pax_inflight_col_info_t();
                for (auto col_it : col_its) {
                    new_info->cols.push_back(col_it->second);
                }
                pax_inflight_cols.insert({global_ident, new_info});
                info = new_info;
            } else {
                info = inflight_info_it->second;
            }
            paxInflightLock.unlock();

            // // Checking first column suffices, all have same length
            // if (info->curr_offset == (info->cols[0])->sizeInBytes) {
            //     // std::cout << "[DataCatalog] PAX for " << global_ident << " reset offset to 0." << std::endl;
            //     info->curr_offset = 0;
            // }

            info->offset_lock.lock();
            if (info->metadata_size == 0) {
                /* Message Layout
                 * [ header_t | chunk_offset bytes_per_column col_cnt [ident_len]+, [ident] | [payload] ]
                 */
                const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
                const size_t ident_metainfo_size = (1 + idents.size()) * sizeof(size_t) + total_id_len;

                // std::cout << "Init info->metadata_buf " << appMetaSize << " Bytes" << std::endl;
                info->metadata_buf = (char*)malloc(appMetaSize);
                char* tmp = info->metadata_buf;
                tmp += sizeof(size_t);  // Placeholder for offset, later.
                tmp += sizeof(size_t);  // Placeholder for bytes_per_column, later.

                memcpy(tmp, ident_lens, ident_metainfo_size);
                // std::cout << "Metadata size: " << ident_metainfo_size << std::endl;
                // for (size_t test = 0; test < idents.size() + 1; ++test) {
                //     std::cout << "tmp[" << test << "]: " << reinterpret_cast<size_t*>(tmp)[test] << std::endl;
                // }

                info->metadata_size = appMetaSize;
            }
            info->offset_lock.unlock();

            auto prepare_pax = [](pax_inflight_col_info_t* my_info, size_t conId) -> void {
                my_info->offset_lock.lock();
                if (my_info->payload_buf == nullptr) {
                    my_info->payload_buf = (char*)malloc(my_info->cols[0]->sizeInBytes * my_info->cols.size());
                } else {
                    my_info->offset_lock.unlock();
                    return;
                }
                my_info->offset_lock.unlock();

                char* tmp = my_info->payload_buf;

                size_t data_left_to_write = my_info->cols[0]->sizeInBytes;
                size_t curr_col_offset = 0;
                size_t curr_payload_offset = 0;

                using element_type = uint64_t;
                const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(conId)->maxBytesInPayload(my_info->metadata_size);
                const size_t max_bytes_per_column = ((maximumPayloadSize / my_info->cols.size()) / (sizeof(element_type) * 4)) * 4 * sizeof(element_type);

                // std::cout << "Preparing " << my_info->cols[0]->sizeInBytes * my_info->cols.size() << " Bytes of data" << std::endl;
                size_t written_bytes = 0;

                while (data_left_to_write > 0) {
                    // We will always only send 1 message, see maximumPayloadSize. Normalize to 8 Byte members
                    // We just <know> that we use 64bit values
                    // TODO: Infer from column data_type member
                    const size_t bytes_per_column = (data_left_to_write > max_bytes_per_column) ? max_bytes_per_column : data_left_to_write;
                    const size_t bytes_in_payload = my_info->cols.size() * bytes_per_column;

                    // std::cout << curr_payload_offset << " " << data_left_to_write << " " << bytes_per_column << " " << bytes_in_payload << std::endl;

                    for (auto cur_col : my_info->cols) {
                        // std::cout << "Writing " << bytes_per_column << " Bytes for " << cur_col->ident << std::endl;
                        const char* col_data = reinterpret_cast<char*>(cur_col->data) + curr_col_offset;
                        memcpy(tmp, col_data, bytes_per_column);
                        tmp += bytes_per_column;
                        written_bytes += bytes_per_column;
                    }

                    my_info->offset_lock.lock();
                    my_info->prepared_offsets.push({curr_payload_offset, bytes_in_payload});
                    my_info->offset_cv.notify_one();
                    my_info->offset_lock.unlock();

                    curr_col_offset += bytes_per_column;
                    curr_payload_offset += bytes_per_column * my_info->cols.size();
                    data_left_to_write -= bytes_per_column;
                }
                my_info->offset_lock.lock();
                my_info->prepare_complete = true;
                my_info->offset_lock.unlock();
                // std::cout << "Prepared all messages, written Bytes: " << written_bytes << std::endl;
            };

            info->offset_lock.lock();
            if (!info->prepare_triggered) {
                info->prepare_triggered = true;
                info->prepare_thread = new std::thread(prepare_pax, info, conId);  // Will be joined when reseting/deleting the info state
            }
            info->offset_lock.unlock();

            reset_buffer();

            std::unique_lock<std::mutex> lk(info->offset_lock);
            info->offset_cv.wait(lk, [info] { return !info->prepared_offsets.empty(); });
            // Semantically we want to "unlock" after waiting, yet this is a performance gain to not do it.
            // lk.unlock();
            // lk.lock();
            auto offset_size_pair = info->prepared_offsets.front();
            info->prepared_offsets.pop();
            lk.unlock();

            char* tmp_meta = (char*)malloc(info->metadata_size);
            char* tmp = tmp_meta;
            memcpy(tmp, info->metadata_buf, info->metadata_size);

            memcpy(tmp, &offset_size_pair.first, sizeof(size_t));
            tmp += sizeof(size_t);

            const size_t bpc = offset_size_pair.second / info->cols.size();  // Normalize to bytes per column because pair contains total payload size
            memcpy(tmp, &bpc, sizeof(size_t));
            tmp += sizeof(size_t);

            ConnectionManager::getInstance().sendData(conId, info->payload_buf + offset_size_pair.first, offset_size_pair.second, tmp_meta, info->metadata_size, static_cast<uint8_t>(catalog_communication_code::receive_pseudo_pax));
            free(tmp_meta);
            if (info->prepare_complete && info->prepared_offsets.empty()) {
                info->reset();
            }
        } else {
            reset_buffer();
        }
        paxInflightLock.unlock();
    };

    /* Message Layout
     * [ header_t | chunk_offset bytes_per_column col_cnt [ident_len]+, [ident] | [payload] ]
     */
    CallbackFunction cb_receivePseudoPax = [this](const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) -> void {
        // Package header
        package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->getBufferPtr());
        // Start of AppMetaData
        char* data = rcv_buffer->getBufferPtr() + sizeof(package_t::header_t);
        // Start of actual payload
        char* pax_ptr = data + head->payload_start;

        size_t chunk_offset;
        memcpy(&chunk_offset, data, sizeof(size_t));
        data += sizeof(size_t);

        size_t bytes_per_column;
        memcpy(&bytes_per_column, data, sizeof(size_t));
        data += sizeof(size_t);

        // Advance data to the first ident character
        size_t* ident_infos = reinterpret_cast<size_t*>(data);
        data += (ident_infos[0] + 1) * sizeof(size_t);

        // Prepare ident vector
        std::vector<std::string> idents;
        idents.reserve(ident_infos[0]);

        // std::cout << "[PseudoPax] message with " << bytes_per_column << " bytes per column for columns: " << std::flush;
        for (size_t i = 0; i < ident_infos[0]; ++i) {
            idents.emplace_back(data, ident_infos[i + 1]);
            data += ident_infos[i + 1];
            // std::cout << idents.back() << " ";
        }
        // std::cout << std::endl;

        std::vector<col_t*> remote_cols;
        remote_cols.reserve(idents.size());
        bool allPresent = true;
        for (auto& id : idents) {
            auto remote_col_it = find_remote(id);
            allPresent &= remote_col_it != nullptr;
            remote_cols.push_back(remote_col_it);
        }

        for (auto col : remote_cols) {
            auto col_network_info_iterator = remote_col_info.find(col->ident);
            if (col_network_info_iterator == remote_col_info.end()) {
                // std::cout << "[PseudoPax] No Network info for received column " << col->ident << ", fetch column info first -- discarding message" << std::endl;
                return;
            }

            const size_t current_offset = col_network_info_iterator->second.received_bytes;
            // std::cout << "Current offset for col (" << col << ") " << col->ident << ": " << current_offset << std::endl;
            col->append_chunk(current_offset, bytes_per_column, pax_ptr);
            pax_ptr += bytes_per_column;

            std::lock_guard<std::mutex> lk(col->iteratorLock);
            // Update network info struct to check if we received all data
            col_network_info_iterator->second.received_bytes += bytes_per_column;

            col->advance_end_pointer(bytes_per_column);
            if (col_network_info_iterator->second.check_complete()) {
                col->is_complete = true;
                // std::cout << "[PseudoPax] Received all data for column: " << col->ident << std::endl;
            }
            ++col->received_chunks;
        }

        reset_buffer();
    };

    CallbackFunction cb_reconfigureChunkSize = [this](const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) -> void {
        // package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->buf);
        char* data = rcv_buffer->getBufferPtr() + sizeof(package_t::header_t);

        uint64_t newChunkSize;
        memcpy(&newChunkSize, data, sizeof(uint64_t));
        data += sizeof(uint64_t);

        uint64_t newChunkThreshold;
        memcpy(&newChunkThreshold, data, sizeof(uint64_t));

        reset_buffer();

        std::lock_guard<std::mutex> lk(reconfigure_lock);
        if (newChunkSize > 0) {
            dataCatalog_chunkMaxSize = newChunkSize;
            dataCatalog_chunkThreshold = newChunkThreshold > 0 ? newChunkThreshold : newChunkSize;
        }

        ConnectionManager::getInstance().sendOpCode(1, static_cast<uint8_t>(catalog_communication_code::ack_reconfigure_chunk_size), true);
    };

    CallbackFunction cb_ackReconfigureChunkSize = [this](const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) -> void {
        reset_buffer();
        std::lock_guard<std::mutex> lk(reconfigure_lock);
        reconfigured = true;
        reconfigure_done.notify_all();
    };

    registerCallback(static_cast<uint8_t>(catalog_communication_code::send_column_info), cb_sendInfo);
    registerCallback(static_cast<uint8_t>(catalog_communication_code::receive_column_info), cb_receiveInfo);
    registerCallback(static_cast<uint8_t>(catalog_communication_code::fetch_column_data), cb_fetchCol);
    registerCallback(static_cast<uint8_t>(catalog_communication_code::receive_column_data), cb_receiveCol);
    registerCallback(static_cast<uint8_t>(catalog_communication_code::fetch_column_chunk), cb_fetchColChunk);
    registerCallback(static_cast<uint8_t>(catalog_communication_code::receive_column_chunk), cb_receiveColChunk);
    registerCallback(static_cast<uint8_t>(catalog_communication_code::fetch_pseudo_pax), cb_fetchPseudoPax);
    registerCallback(static_cast<uint8_t>(catalog_communication_code::receive_pseudo_pax), cb_receivePseudoPax);
    registerCallback(static_cast<uint8_t>(catalog_communication_code::reconfigure_chunk_size), cb_reconfigureChunkSize);
    registerCallback(static_cast<uint8_t>(catalog_communication_code::ack_reconfigure_chunk_size), cb_ackReconfigureChunkSize);
}

DataCatalog&
DataCatalog::getInstance() {
    static DataCatalog instance;
    return instance;
}

DataCatalog::~DataCatalog() {
    clear();
}

void DataCatalog::clear() {
    for (auto it : cols) {
        delete it.second;
    }
    cols.clear();
    for (auto it : remote_cols) {
        delete it.second;
    }
    remote_cols.clear();
}

void DataCatalog::registerCallback(uint8_t code, CallbackFunction cb) const {
    if (ConnectionManager::getInstance().registerCallback(code, cb)) {
        std::cout << "[DataCatalog] Successfully added callback for code " << static_cast<uint64_t>(code) << std::endl;
    } else {
        std::cout << "[DataCatalog] Error adding callback for code " << static_cast<uint64_t>(code) << std::endl;
    }
}

col_dict_t::iterator DataCatalog::generate(std::string ident, col_data_t type, size_t elemCount, int node) {
    // std::cout << "Calling gen with type " << type << std::endl;
    auto it = cols.find(ident);

    if (it != cols.end()) {
        std::cout << "Column width ident " << ident << " already present, returning old data." << std::endl;
        return it;
    }

    col_t* tmp = new col_t();
    tmp->ident = ident;
    tmp->size = elemCount;

    std::default_random_engine generator;
    switch (type) {
        case col_data_t::gen_smallint: {
            std::uniform_int_distribution<uint8_t> distribution(0, 99);
            tmp->datatype = col_data_t::gen_smallint;
            tmp->allocate_on_numa<uint8_t>(elemCount, node);
            auto data = reinterpret_cast<uint8_t*>(tmp->data);
            for (size_t i = 0; i < elemCount; ++i) {
                data[i] = distribution(generator);
            }
            tmp->readableOffset = elemCount * sizeof(uint8_t);
            break;
        }
        case col_data_t::gen_bigint: {
            std::uniform_int_distribution<uint64_t> distribution(0, 99);
            tmp->datatype = col_data_t::gen_bigint;
            tmp->allocate_on_numa<uint64_t>(elemCount, node);
            auto data = reinterpret_cast<uint64_t*>(tmp->data);
            for (size_t i = 0; i < elemCount; ++i) {
                data[i] = distribution(generator);
            }
            tmp->readableOffset = elemCount * sizeof(uint64_t);
            break;
        }
        case col_data_t::gen_float: {
            std::uniform_real_distribution<float> distribution(0, 50);
            tmp->datatype = col_data_t::gen_float;
            tmp->allocate_on_numa<float>(elemCount, node);
            auto data = reinterpret_cast<float*>(tmp->data);
            for (size_t i = 0; i < elemCount; ++i) {
                data[i] = distribution(generator);
            }
            tmp->readableOffset = elemCount * sizeof(float);
            break;
        }
        case col_data_t::gen_double: {
            std::uniform_real_distribution<double> distribution(0, 50);
            tmp->datatype = col_data_t::gen_double;
            tmp->allocate_on_numa<double>(elemCount, node);
            auto data = reinterpret_cast<double*>(tmp->data);
            for (size_t i = 0; i < elemCount; ++i) {
                data[i] = distribution(generator);
            }
            tmp->readableOffset = elemCount * sizeof(double);
            break;
        }
    }
    tmp->is_remote = false;
    tmp->is_complete = true;
    cols.insert({ident, tmp});
    return cols.find(ident);
}

col_t* DataCatalog::find_local(std::string ident) const {
    auto it = cols.find(ident);
    if (it != cols.end()) {
        return (*it).second;
    }
    return nullptr;
}

col_t* DataCatalog::find_remote(std::string ident) const {
    std::lock_guard<std::mutex> l(appendLock);
    auto it = remote_cols.find(ident);
    if (it != remote_cols.end()) {
        return (*it).second;
    }
    return nullptr;
}

col_t* DataCatalog::add_remote_column(std::string name, col_network_info ni) {
    std::lock_guard<std::mutex> _lka(appendLock);

    auto it = remote_cols.find(name);
    if (it != remote_cols.end()) {
        // std::cout << "[DataCatalog] Column with same ident ('" << name << "') already present, cannot add remote column." << std::endl;
        return it->second;
    } else {
        // std::cout << "[DataCatalog] Creating new remote column: " << name << std::endl;
        col_t* col = new col_t();
        col->ident = name;
        col->is_remote = true;
        col->datatype = (col_data_t)ni.type_info;
        col->allocate_on_numa((col_data_t)ni.type_info, ni.size_info, 0);
        remote_cols.insert({name, col});
        return col;
    }
}

void DataCatalog::print_column(std::string& ident) const {
    auto it = cols.find(ident);
    if (it != cols.end()) {
        std::cout << "[DataCatalog]" << (*it).second->print_data_head() << std::endl;
    } else {
        std::cout << "[DataCatalog] No Entry for ident " << ident << std::endl;
    }
}

void DataCatalog::print_all() const {
    std::cout << "### Local Data Catalog ###" << std::endl;
    if (cols.size() == 0) {
        std::cout << "<empty>" << std::endl;
    }
    for (auto it : cols) {
        std::cout << "[" << it.first << "]: " << it.second->print_identity() << std::endl;
    }
}

void DataCatalog::print_all_remotes() const {
    std::cout << "### Remote Data Catalog ###" << std::endl;
    if (remote_col_info.size() == 0) {
        std::cout << "<empty>" << std::endl;
    }
    for (auto it : remote_col_info) {
        std::cout << "[" << it.first << "]: ";
        auto remote_col = remote_cols.find(it.first);
        if (remote_col != remote_cols.end()) {
            std::cout << remote_col->second->print_identity();
            auto rem_info = remote_col_info.find(it.first);
            std::cout << " (" << rem_info->second.received_bytes << " received)";
        } else {
            std::cout << it.second.print_identity() << " [nothing local]";
        }
        std::cout << std::endl;
    }
}

void DataCatalog::eraseAllRemoteColumns() {
    std::lock_guard<std::mutex> _lka(appendLock);
    std::lock_guard<std::mutex> _lkb(remote_info_lock);
    std::lock_guard<std::mutex> _lkc(inflightLock);

    for (auto col : remote_cols) {
        delete col.second;
        remote_col_info.erase(col.first);
    }

    remote_cols.clear();
    remote_col_info.clear();
}

void DataCatalog::fetchColStub(std::size_t conId, std::string& ident, bool wholeColumn) const {
    char* payload = (char*)malloc(ident.size() + sizeof(size_t));
    const size_t sz = ident.size();
    memcpy(payload, &sz, sizeof(size_t));
    memcpy(payload + sizeof(size_t), ident.c_str(), sz);
    catalog_communication_code code = wholeColumn ? catalog_communication_code::fetch_column_data : catalog_communication_code::fetch_column_chunk;
    ConnectionManager::getInstance().sendData(conId, payload, sz + sizeof(size_t), nullptr, 0, static_cast<uint8_t>(code));
    free(payload);
}

// Fetches a chunk of data sized CHUNK_MAX_SIZE containing information for all columns, equal amount of values
void DataCatalog::fetchPseudoPax(std::size_t conId, std::vector<std::string> idents) const {
    size_t string_sizes = 0;

    bool all_fetchable = true;
    bool all_complete = true;
    std::vector<col_t*> ptrs;
    std::vector<std::mutex*> locks;
    for (auto& id : idents) {
        string_sizes += id.size();

        ptrs.push_back(find_remote(id));
        ptrs.back()->iteratorLock.lock();
        locks.push_back(&ptrs.back()->iteratorLock);
        const bool fetchable = !(ptrs.back()->requested_chunks > ptrs.back()->received_chunks);
        const bool complete = ptrs.back()->is_complete;
        // std::cout << ptrs.back()->ident << " " << ptrs.back()->requested_chunks << " " << ptrs.back()->received_chunks << " " << ptrs.back()->is_complete << std::endl;
        all_fetchable &= fetchable;
        all_complete &= complete;
    }

    if (all_complete || !all_fetchable) {
        // if (!all_complete) {
        //     std::cout << "Pending chunks of current pax request or complete, ignoring." << std::endl;
        // }
        for (auto lk : locks) {
            lk->unlock();
        }
        return;
    }

    for (auto col : ptrs) {
        ++col->requested_chunks;
    }
    for (auto lk : locks) {
        lk->unlock();
    }

    /* col_cnt | [col_ident_size]+ | [col_ident]+ */
    char* payload = (char*)malloc(sizeof(size_t) + idents.size() * sizeof(size_t) + string_sizes);
    char* tmp = payload;
    size_t sz;

    // col_cnt
    const size_t col_cnt = idents.size();
    memcpy(tmp, &col_cnt, sizeof(size_t));
    tmp += sizeof(size_t);

    // col_ident_sizes
    for (auto& id : idents) {
        sz = id.size();
        memcpy(tmp, &sz, sizeof(size_t));
        tmp += sizeof(size_t);
    }
    for (auto& id : idents) {
        memcpy(tmp, id.c_str(), id.size());
        tmp += id.size();
    }
    const size_t total_payload_size = (sizeof(size_t) * (idents.size() + 1)) + string_sizes;
    ConnectionManager::getInstance().sendData(conId, payload, total_payload_size, nullptr, 0, static_cast<uint8_t>(catalog_communication_code::fetch_pseudo_pax));
    free(payload);
}

void DataCatalog::remoteInfoReady() {
    std::lock_guard<std::mutex> lk(remote_info_lock);
    col_info_received = true;
    remote_info_available.notify_all();
}

void DataCatalog::fetchRemoteInfo() {
    std::unique_lock<std::mutex> lk(remote_info_lock);
    col_info_received = false;
    ConnectionManager::getInstance().sendOpCode(1, static_cast<uint8_t>(catalog_communication_code::send_column_info), true);
    // while (!col_info_received) {
    //     using namespace std::chrono_literals;
    //     if (!remote_info_available.wait_for(lk, 1s, [this] { return col_info_received; })) {
    //         ConnectionManager::getInstance().sendOpCode(1, static_cast<uint8_t>(catalog_communication_code::send_column_info));
    //     }
    // }
    remote_info_available.wait(lk, [this] { return col_info_received; });
}

void DataCatalog::reconfigureChunkSize(const uint64_t newChunkSize, const uint64_t newChunkThreshold) {
    using namespace std::chrono_literals;

    if (newChunkSize == 0 || newChunkThreshold == 0) {
        std::cout << "Either the new Chunk Size or the new Chunk Threshold was 0!" << std::endl;
        return;
    }

    dataCatalog_chunkMaxSize = newChunkSize;
    dataCatalog_chunkThreshold = newChunkThreshold;

    char* ptr = reinterpret_cast<char*>(&dataCatalog_chunkMaxSize);

    std::unique_lock<std::mutex> lk(reconfigure_lock);
    reconfigured = false;
    ConnectionManager::getInstance().sendData(1, ptr, sizeof(uint64_t), nullptr, 0, static_cast<uint8_t>(catalog_communication_code::reconfigure_chunk_size));
    reconfigure_done.wait(lk, [this] { return reconfigured; });
}