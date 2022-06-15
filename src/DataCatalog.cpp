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

        std::cout << "Which datatype? uint8_t [s] uint64_t [l] float [f] double [d]" << std::endl;
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
        this->print_all();
        std::string ident;
        std::cout << "Which column?" << std::endl;
        std::cin >> ident;
        std::cin.clear();
        std::cin.ignore(10000, '\n');
        this->print_column(ident);
    };

    auto retrieveRemoteColsLambda = [this]() -> void {
        ConnectionManager::getInstance().sendOpCode(1, send_column_info);
    };

    TaskManager::getInstance().registerTask(new Task("createColumn", "[DataCatalog] Create new column", createColLambda));
    TaskManager::getInstance().registerTask(new Task("printAllColumn", "[DataCatalog] Print all stored columns", [this]() -> void { this->print_all(); }));
    TaskManager::getInstance().registerTask(new Task("printColHead", "[DataCatalog] Print first 10 values of column", printColLambda));
    TaskManager::getInstance().registerTask(new Task("retrieveRemoteCols", "[DataCatalog] Ask for remote columns", retrieveRemoteColsLambda));

    CallbackFunction cb_sendInfo = [this](size_t conId, ReceiveBuffer* rcv_buffer) -> void {
        std::cout << "[DataCatalog] Hello from the Data Catalog" << std::endl;
        const uint8_t code = catalog_communication_code::receive_column_info;
        const size_t columnCount = cols.size();

        size_t totalPayloadSize = sizeof(size_t) + (sizeof(col_network_info) * cols.size());
        for (auto col : cols) {
            totalPayloadSize += sizeof(size_t);    // store size of ident length in a 64bit int
            totalPayloadSize += col.first.size();  // actual c_string
        }
        std::cout << "[DataCatalog] Callback - allocating " << totalPayloadSize << " for column data." << std::endl;
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

    CallbackFunction cb_receiveInfo = [this](size_t conId, ReceiveBuffer* rcv_buffer) -> void {
        package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->buf);
        size_t colCnt;
        char* data = rcv_buffer->buf + sizeof(package_t::header_t);
        memcpy(&colCnt, data, sizeof(size_t));
        data += sizeof(size_t);
        std::stringstream ss;
        ss << "[DataCatalog] Received data for " << colCnt << " columns" << std::endl;

        col_network_info cni(0, col_data_t::gen_void);
        size_t identlen;
        for (size_t i = 0; i < colCnt; ++i) {
            memcpy(&cni, data, sizeof(cni));
            data += sizeof(cni);

            memcpy(&identlen, data, sizeof(size_t));
            data += sizeof(size_t);

            std::string ident(data, identlen);
            data += identlen;

            ss << "[DataCatalog] Column: " << ident << " - " << cni.size_info << " elements of type " << cni.col_data_type_to_string() << std::endl;
            remote_cols.insert({ident, cni});
        }

        if (colCnt > 0) {
            auto fetchLambda = [&]() -> void {
                print_all();
                std::cout << "[DataCatalog] Fetch data for which column?" << std::endl;
                std::string ident;
                std::cin >> ident;
                std::cin.clear();
                std::cin.ignore(10000, '\n');
                char* payload = (char*) malloc( ident.size() + sizeof(size_t) );
                ConnectionManager::getInstance().sendData(conId, payload, ident.size() + sizeof(size_t), nullptr, 0, catalog_communication_code::fetch_column_data );
            };

            if (!TaskManager::getInstance().hasTask("fetchColDataFromRemote")) {
                TaskManager::getInstance().registerTask(new Task("fetchColDataFromRemote", "[DataCatalog] Fetch data from specific remote column", fetchLambda));
                std::cout << "[DataCatalog] Registered new Task!" << std::endl;
            }
            TaskManager::getInstance().printAll();
        }
        std::cout << ss.str() << std::endl;
    };

    CallbackFunction cb_receiveCol = [this](size_t conId, ReceiveBuffer* rcv_buffer) -> void {
        /* Expected message layout
         *   Msg:     [ header_t, payload ]
         *   payload: [ column_offset, data ]
         */

        package_t::header_t* head = reinterpret_cast<package_t::header_t*>(rcv_buffer->buf);
        char* data = rcv_buffer->buf + sizeof(package_t::header_t);
        char* column_data = data + +head->payload_offset;
        std::cout << "[DataCatalog] Received Data for a column";
    };

    registerCallback(send_column_info, cb_sendInfo);
    registerCallback(receive_column_info, cb_receiveInfo);
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
}

void DataCatalog::registerCallback(uint8_t code, CallbackFunction cb) const {
    if (ConnectionManager::getInstance().registerCallback(code, cb)) {
        std::cout << "[DataCatalog] Successfully added callback for code " << static_cast<uint64_t>(code) << std::endl;
    } else {
        std::cout << "[DataCatalog] Error adding callback for code " << static_cast<uint64_t>(code) << std::endl;
    }
}

col_dict_t::iterator DataCatalog::generate(std::string ident, col_data_t type, size_t elemCount) {
    std::cout << "Calling gen with type " << type << std::endl;
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

col_t* DataCatalog::find(std::string ident) const {
    auto it = cols.find(ident);
    if (it != cols.end()) {
        return (*it).second;
    }
    return nullptr;
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
    std::cout << "### Data Catalog ###" << std::endl;
    for (auto it : cols) {
        std::cout << "[" << it.first << "]: " << it.second->print_identity() << std::endl;
    }
}