#include <ConnectionManager.h>
#include <DataCatalog.h>
#include <TaskManager.h>

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
        std::cout << "How mana data elements?" << std::endl;
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

    auto retrieveRemoteColsLambda = [this]() -> void {
        ConnectionManager::getInstance().sendOpCode(1, send_column_info);
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

    TaskManager::getInstance().registerTask(new Task("createColumn", "[DataCatalog] Create new column", createColLambda));
    TaskManager::getInstance().registerTask(new Task("printAllColumn", "[DataCatalog] Print all stored columns", [this]() -> void { this->print_all(); this->print_all_remotes(); }));
    TaskManager::getInstance().registerTask(new Task("printColHead", "[DataCatalog] Print first 10 values of column", printColLambda));
    TaskManager::getInstance().registerTask(new Task("retrieveRemoteCols", "[DataCatalog] Ask for remote columns", retrieveRemoteColsLambda));
    TaskManager::getInstance().registerTask(new Task("logColumn", "[DataCatalog] Log a column to file", logLambda));

    /* Message Layout
     * [ header_t | payload ]
     * Payload layout
     * [ columnInfoCount | [col_network_info, identLength, ident]* ]
     */
    CallbackFunction cb_sendInfo = [this](size_t conId, ReceiveBuffer* rcv_buffer) -> void {
        // std::cout << "[DataCatalog] Hello from the Data Catalog" << std::endl;
        const uint8_t code = catalog_communication_code::receive_column_info;
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
        size_t colCnt;
        char* data = rcv_buffer->buf + sizeof(package_t::header_t);
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

            // ss << "[DataCatalog] Column: " << ident << " - " << cni.size_info << " elements of type " << col_network_info::col_data_type_to_string(cni.type_info) << std::endl;
            if (!remote_col_info.contains(ident)) {
                remote_col_info.insert({ident, cni});
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
                char* payload = (char*)malloc(ident.size() + sizeof(size_t));
                const size_t sz = ident.size();
                memcpy(payload, &sz, sizeof(size_t));
                memcpy(payload + sizeof(size_t), ident.c_str(), sz);
                ConnectionManager::getInstance().sendData(conId, payload, sz + sizeof(size_t), nullptr, 0, catalog_communication_code::fetch_column_data);
            };

            if (!TaskManager::getInstance().hasTask("fetchColDataFromRemote")) {
                TaskManager::getInstance().registerTask(new Task("fetchColDataFromRemote", "[DataCatalog] Fetch data from specific remote column", fetchLambda));
                std::cout << "[DataCatalog] Registered new Task!" << std::endl;
            }
            TaskManager::getInstance().printAll();
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

        char* ident = (char*)malloc(identSz);
        memcpy(ident, data, identSz);
        std::string ident_s(ident, identSz);

        // std::cout << "[DataCatalog] Remote requested data for column '" << ident_s << "' with ident len " << identSz << std::endl;
        auto col = cols.find(ident_s);
        if (col != cols.end()) {
            /* Message Layout
             * [ header_t | ident_len, ident, col_data_type | col_data ]
             */
            const size_t appMetaSize = sizeof(size_t) + identSz + sizeof(uint8_t);
            char* appMetaData = (char*)malloc(appMetaSize);
            char* tmp = appMetaData;

            memcpy(tmp, &identSz, sizeof(size_t));
            tmp += sizeof(size_t);

            memcpy(tmp, ident_s.c_str(), identSz);
            tmp += identSz;

            memcpy(tmp, &col->second->datatype, sizeof(uint8_t));

            ConnectionManager::getInstance().sendData(conId, (char*)col->second->data, col->second->sizeInBytes, appMetaData, appMetaSize, receive_column_data);

            free(appMetaData);
        }
        free(ident);
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

        char* ident = (char*)malloc(identSz);
        memcpy(ident, data, identSz);
        data += identSz;

        uint8_t data_type;
        memcpy(&data_type, data, sizeof(uint8_t));

        std::string ident_s(ident, identSz);
        // std::cout << "Total Message size - header_t: " << sizeof(package_t::header_t) << " AppMetaDataSize: " << head->payload_start << " Payload size: " << head->current_payload_size << " Sum: " << sizeof(package_t::header_t) + head->payload_start + head->current_payload_size << std::endl;
        // std::cout << "Received data for column: " << ident_s
        //           << " of type " << col_network_info::col_data_type_to_string(data_type)
        //           << ": " << head->current_payload_size
        //           << " Bytes of " << head->total_data_size
        //           << " current message offset to Base: " << head->payload_position_offset
        //           << " AppMetaDataSize: " << head->payload_start << " Bytes"
        //           << std::endl;

        auto col = find_remote(ident_s);
        auto col_network_info_iterator = remote_col_info.find(ident_s);
        // Column object already created?
        if (col == nullptr) {
            // No Col object, did we even fetch remote info beforehand?
            if (col_network_info_iterator != remote_col_info.end()) {
                col = add_remote_column(ident_s, col_network_info_iterator->second);
            } else {
                std::cout << "[DataCatalog] No Network info for received column, fetch column info first -- discarding message" << std::endl;
            }
        }
        // Write currently received data to the column object
        col->append_chunk(head->payload_position_offset, head->current_payload_size, column_data);
        // Update network info struct to check if we received all data
        col_network_info_iterator->second.received_bytes += head->current_payload_size;

        if (col_network_info_iterator->second.received_bytes == head->total_data_size) {
            col->is_complete = true;
            std::cout << "[DataCatalog] Received all data for column: " << ident_s << std::endl;
        }
    };

    registerCallback(send_column_info, cb_sendInfo);
    registerCallback(receive_column_info, cb_receiveInfo);
    registerCallback(fetch_column_data, cb_fetchCol);
    registerCallback(receive_column_data, cb_receiveCol);
}

DataCatalog& DataCatalog::getInstance() {
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
        std::cout << "[DataCatalog] Column with same ident ('" << name << "') already present, cannot add remote column." << std::endl;
        return it->second;
    } else {
        std::cout << "[DataCatalog] Creating new remote column: " << name << std::endl;
        col_t* col = new col_t();
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