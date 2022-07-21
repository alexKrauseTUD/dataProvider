#include <Column.h>
#include <ConnectionManager.h>
#include <DataCatalog.h>
#include <TaskManager.h>

// uint64_t* convertPointer(bool& remote, col_t* column, std::string ident) {
//     if (remote) {
//         while (!column || !column->is_complete) {
//             using namespace std::chrono_literals;
//             std::this_thread::sleep_for(1ns);
//             column = DataCatalog::getInstance().find_remote(ident);
//         }
//     }

//     return reinterpret_cast<uint64_t*>(column->data);
// }

template <bool chunked>
uint64_t bench_1_1(bool remote) {
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

void executeBenchmarkingQueries() {
    uint64_t sum;

    // // local
    auto s_ts = std::chrono::high_resolution_clock::now();
    sum = bench_1_1<false>(false);
    auto e_ts = std::chrono::high_resolution_clock::now();

    typedef std::chrono::duration<double> d_sec;
    d_sec secs = e_ts - s_ts;

    std::cout << "Local: " << secs.count() << "\t" << sum << std::endl;

    // remote
    s_ts = std::chrono::high_resolution_clock::now();
    sum = bench_1_1<false>(true);
    e_ts = std::chrono::high_resolution_clock::now();

    secs = e_ts - s_ts;

    std::cout << "Remote/Full: " << secs.count() << "\t" << sum << std::endl;

    DataCatalog::getInstance().eraseAllRemoteColumns();

    // remote after deleting columns + chunked
    s_ts = std::chrono::high_resolution_clock::now();
    sum = bench_1_1<true>(true);
    e_ts = std::chrono::high_resolution_clock::now();

    secs = e_ts - s_ts;

    std::cout << "Remote/Chunked: " << secs.count() << "\t" << sum << std::endl;

    DataCatalog::getInstance().eraseAllRemoteColumns();
}

DataCatalog::DataCatalog() {
    auto createColLambda = [this]() -> void {
        std::cout << "[DataCatalog]";

        std::size_t elemCnt;
        char dataType;
        std::string ident;
        bool correct;
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

        this->generate(ident, type, elemCnt);
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
            auto cur_col = col_it->second;
            std::size_t count = 0;
            auto t_start = std::chrono::high_resolution_clock::now();
            for (auto it = cur_col->begin<uint64_t, true>(); it != cur_col->end<uint64_t, true>(); ++it) {
                count++;
            }
            auto t_end = std::chrono::high_resolution_clock::now();
            std::cout << "I found " << count << " Elements in " << std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count() << "ms" << std::endl;
        } else {
            std::cout << "[DataCatalog] Invalid column name." << std::endl;
        }
    };

    auto retrieveRemoteColsLambda = [this]() -> void {
        ConnectionManager::getInstance().sendOpCode(1, static_cast<uint8_t>(catalog_communication_code::send_column_info));
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

    auto benchQueries = [this]() -> void { executeBenchmarkingQueries(); };

    TaskManager::getInstance().registerTask(new Task("createColumn", "[DataCatalog] Create new column", createColLambda));
    TaskManager::getInstance().registerTask(new Task("printAllColumn", "[DataCatalog] Print all stored columns", [this]() -> void { this->print_all(); this->print_all_remotes(); }));
    TaskManager::getInstance().registerTask(new Task("printColHead", "[DataCatalog] Print first 10 values of column", printColLambda));
    TaskManager::getInstance().registerTask(new Task("retrieveRemoteCols", "[DataCatalog] Ask for remote columns", retrieveRemoteColsLambda));
    TaskManager::getInstance().registerTask(new Task("logColumn", "[DataCatalog] Log a column to file", logLambda));
    TaskManager::getInstance().registerTask(new Task("benchmark", "[DataCatalog] Execute benchmarking Queries", benchQueries));
    TaskManager::getInstance().registerTask(new Task("itTest", "[DataCatalog] IteratorTest", iteratorTestLambda));

    /* Message Layout
     * [ header_t | payload ]
     * Payload layout
     * [ columnInfoCount | [col_network_info, identLength, ident]* ]
     */
    CallbackFunction cb_sendInfo = [this](size_t conId, ReceiveBuffer* rcv_buffer) -> void {
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
    CallbackFunction cb_receiveInfo = [this](size_t conId, ReceiveBuffer* rcv_buffer) -> void {
        package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->buf);
        char* data = rcv_buffer->buf + sizeof(package_t::header_t);

        size_t colCnt;
        memcpy(&colCnt, data, sizeof(size_t));
        data += sizeof(size_t);
        std::stringstream ss;
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

            // ss << "[DataCatalog] Column: " << ident << " - " << cni.size_info << " elements of type " << col_network_info::col_data_type_to_string(cni.type_info) << std::endl;
            if (!remote_col_info.contains(ident)) {
                // ss << "Ident not found!";
                remote_col_info.insert({ident, cni});
                if (!find_remote(ident)) {
                    add_remote_column(ident, cni);
                }
            }
        }

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
                TaskManager::getInstance().registerTask(new Task("fetchColDataFromRemote", "[DataCatalog] Fetch data from specific remote column", fetchLambda));
                std::cout << "[DataCatalog] Registered new Task!" << std::endl;
            }
            // TaskManager::getInstance().printAll();
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
    CallbackFunction cb_fetchCol = [this](size_t conId, ReceiveBuffer* rcv_buffer) -> void {
        package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->buf);
        char* data = rcv_buffer->buf + sizeof(package_t::header_t);
        char* column_data = data + head->payload_start;

        size_t identSz;
        memcpy(&identSz, data, sizeof(size_t));
        data += sizeof(size_t);

        std::string ident(data, identSz);

        // std::cout << "[DataCatalog] Remote requested data for column '" << ident << "' with ident len " << identSz << std::endl;
        auto col = cols.find(ident);
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
    CallbackFunction cb_receiveCol = [this](size_t conId, ReceiveBuffer* rcv_buffer) -> void {
        // Package header
        package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->buf);
        // Start of AppMetaData
        char* data = rcv_buffer->buf + sizeof(package_t::header_t);
        // Actual column data payload
        char* column_data = data + head->payload_start;

        size_t identSz;
        memcpy(&identSz, data, sizeof(size_t));
        data += sizeof(size_t);

        std::string ident(data, identSz);
        data += identSz;

        col_data_t data_type;
        memcpy(&data_type, data, sizeof(col_data_t));

        // std::cout << "Total Message size - header_t: " << sizeof(package_t::header_t) << " AppMetaDataSize: " << head->payload_start << " Payload size: " << head->current_payload_size << " Sum: " << sizeof(package_t::header_t) + head->payload_start + head->current_payload_size << std::endl;
        // std::cout << "Received data for column: " << ident
        //           << " of type " << col_network_info::col_data_type_to_string(data_type)
        //           << ": " << head->current_payload_size
        //           << " Bytes of " << head->total_data_size
        //           << " current message offset to Base: " << head->payload_position_offset
        //           << " AppMetaDataSize: " << head->payload_start << " Bytes"
        //           << std::endl;

        auto col = find_remote(ident);
        auto col_network_info_iterator = remote_col_info.find(ident);
        // Column object already created?
        if (col == nullptr) {
            // No Col object, did we even fetch remote info beforehand?
            if (col_network_info_iterator != remote_col_info.end()) {
                col = add_remote_column(ident, col_network_info_iterator->second);
            } else {
                // std::cout << "[DataCatalog] No Network info for received column " << ident << ", fetch column info first -- discarding message" << std::endl;
                return;
            }
        }
        // Write currently received data to the column object
        col->append_chunk(head->payload_position_offset, head->current_payload_size, column_data);
        // Update network info struct to check if we received all data
        col_network_info_iterator->second.received_bytes += head->current_payload_size;

        if (col_network_info_iterator->second.received_bytes == head->total_data_size) {
            col->is_complete = true;
            ++col->received_chunks;
            // std::cout << "[DataCatalog] Received all data for column: " << ident << std::endl;
        }
    };

    // Send a chunk of a column to the requester
    CallbackFunction cb_fetchColChunk = [this](size_t conId, ReceiveBuffer* rcv_buffer) -> void {
        package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->buf);
        char* data = rcv_buffer->buf + sizeof(package_t::header_t);
        char* column_data = data + head->payload_start;

        size_t identSz;
        memcpy(&identSz, data, sizeof(size_t));
        data += sizeof(size_t);

        std::string ident(data, identSz);

        // std::cout << "Looking for column " << ident << " to send over." << std::endl;
        auto col_info_it = cols.find(ident);

        // TODO: Make this function threadsafe.
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

            const size_t CHUNK_MAX_SIZE = 4096 * 8;  // 4 Pages
            const size_t remaining_size = info->col->sizeInBytes - info->curr_offset;

            // If we have at least 16k left to write, chunk size is 16k, rest otherwise.
            const size_t chunk_size = (remaining_size > CHUNK_MAX_SIZE) ? CHUNK_MAX_SIZE : remaining_size;
            char* data_start = static_cast<char*>(info->col->data) + info->curr_offset;

            // Increment offset after setting message variables
            info->curr_offset += chunk_size;
            // std::cout << "Sent chunk. Offset now: " << info->curr_offset << " Total col size: " << info->col->sizeInBytes << std::endl;

            ConnectionManager::getInstance().sendData(conId, data_start, chunk_size, appMetaData, appMetaSize, static_cast<uint8_t>(catalog_communication_code::receive_column_chunk));

            free(appMetaData);
        }
    };

    /* Message Layout
     * [ header_t | chunk_offset ident_len, ident, col_data_type | col_data ]
     */
    CallbackFunction cb_receiveColChunk = [this](size_t conId, ReceiveBuffer* rcv_buffer) -> void {
        // std::cout << "[DataCatalog] Received a message with a (part of a) column chnunk." << std::endl;
        // Package header
        package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->buf);
        // Start of AppMetaData
        char* data = rcv_buffer->buf + sizeof(package_t::header_t);
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

        auto col = find_remote(ident);
        auto col_network_info_iterator = remote_col_info.find(ident);
        // Column object already created?
        if (col == nullptr) {
            // No Col object, did we even fetch remote info beforehand?
            if (col_network_info_iterator != remote_col_info.end()) {
                col = add_remote_column(ident, col_network_info_iterator->second);
            } else {
                std::cout << "[DataCatalog] No Network info for received column " << ident << ", fetch column info first -- discarding message" << std::endl;
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
        col_network_info_iterator->second.received_bytes += head->current_payload_size;

        if (col_network_info_iterator->second.received_bytes % head->total_data_size == 0) {
            if (chunk_total_offset + head->current_payload_size == col->sizeInBytes) {
                col->is_complete = true;
            }
            ++col->received_chunks;
            // std::cout << "[DataCatalog] Latest chunk of '" << ident << "' received completely." << std::endl;
        } else if (chunk_total_offset + head->current_payload_size == col->sizeInBytes) {
            col->is_complete = true;
            ++col->received_chunks;
            // std::cout << "[DataCatalog] Received all data for column: " << ident << std::endl;
        }
    };

    registerCallback(static_cast<uint8_t>(catalog_communication_code::send_column_info), cb_sendInfo);
    registerCallback(static_cast<uint8_t>(catalog_communication_code::receive_column_info), cb_receiveInfo);
    registerCallback(static_cast<uint8_t>(catalog_communication_code::fetch_column_data), cb_fetchCol);
    registerCallback(static_cast<uint8_t>(catalog_communication_code::receive_column_data), cb_receiveCol);
    registerCallback(static_cast<uint8_t>(catalog_communication_code::fetch_column_chunk), cb_fetchColChunk);
    registerCallback(static_cast<uint8_t>(catalog_communication_code::receive_column_chunk), cb_receiveColChunk);
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

col_dict_t::iterator DataCatalog::generate(std::string ident, col_data_t type, size_t elemCount) {
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
            std::uniform_int_distribution<uint8_t> distribution(0, 100);
            tmp->datatype = col_data_t::gen_smallint;
            tmp->allocate_aligned_internal<uint8_t>(elemCount);
            auto data = reinterpret_cast<uint8_t*>(tmp->data);
            for (size_t i = 0; i < elemCount; ++i) {
                data[i] = distribution(generator);
            }
            tmp->readableOffset = elemCount * sizeof(uint8_t);
            break;
        }
        case col_data_t::gen_bigint: {
            std::uniform_int_distribution<uint64_t> distribution(0, 100);
            tmp->datatype = col_data_t::gen_bigint;
            tmp->allocate_aligned_internal<uint64_t>(elemCount);
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
            tmp->allocate_aligned_internal<float>(elemCount);
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
            tmp->allocate_aligned_internal<double>(elemCount);
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
    std::lock_guard<std::mutex> _lk(appendLock);
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
        col->allocate_aligned_internal((col_data_t)ni.type_info, ni.size_info);
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
        } else {
            std::cout << it.second.print_identity() << " [nothing local]";
        }
        std::cout << std::endl;
    }
}

void DataCatalog::eraseRemoteColumn(std::string ident) {
    if (remote_cols.contains(ident)) {
        delete remote_cols[ident];
        // remote_cols.erase(ident);
    }
    if (remote_col_info.contains(ident)) {
        remote_col_info.erase(ident);
    }
}

void DataCatalog::eraseAllRemoteColumns() {
    for (auto col : remote_cols) {
        eraseRemoteColumn(col.first);
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

void DataCatalog::remoteInfoReady() {
    std::lock_guard<std::mutex> lk(remote_info_lock);
    col_info_received = true;
    remote_info_available.notify_all();
}

void DataCatalog::fetchRemoteInfo() {
    std::unique_lock<std::mutex> lk(remote_info_lock);
    col_info_received = false;
    ConnectionManager::getInstance().sendOpCode(1, static_cast<uint8_t>(catalog_communication_code::send_column_info));
    remote_info_available.wait(lk, [this] { return col_info_received; });
};