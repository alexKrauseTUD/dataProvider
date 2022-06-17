#pragma once

#include <fstream>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>

enum col_data_t {
    gen_void = 0,
    gen_float = 1,
    gen_double = 2,
    gen_smallint = 3,
    gen_bigint = 4
};

struct col_network_info {
    size_t size_info;
    uint8_t type_info;
    size_t received_bytes;

    col_network_info() = default;

    col_network_info(size_t sz, col_data_t dt) {
        size_info = sz;
        type_info = dt;
        received_bytes = 0;
    }

    col_network_info(const col_network_info& other) = default;
    col_network_info& operator=(const col_network_info& other) = default;

    static std::string col_data_type_to_string(uint8_t info) {
        switch (static_cast<col_data_t>(info)) {
            case gen_float:
                return "float";
            case gen_double:
                return "double";
            case gen_smallint:
                return "uint8_t";
            case gen_bigint:
                return "uint64_t";
            default:
                return "Datatype case not implemented!";
        }
    };

    std::string print_identity() const {
        std::stringstream ss;
        ss << size_info << " elements of type ";
        switch (type_info) {
            case col_data_t::gen_smallint: {
                ss << "uint8_t"
                   << " " << size_info * sizeof(uint8_t) << " Bytes";
                break;
            }
            case col_data_t::gen_bigint: {
                ss << "uint64_t"
                   << " " << size_info * sizeof(uint64_t) << " Bytes";
                break;
            }
            case col_data_t::gen_float: {
                ss << "float"
                   << " " << size_info * sizeof(float) << " Bytes";
                break;
            }
            case col_data_t::gen_double: {
                ss << "double"
                   << " " << size_info * sizeof(double) << " Bytes";
                break;
            }
        }
        return std::move(ss.str());
    }
};

enum catalog_communication_code : uint8_t {
    send_column_info = 0xf0,
    receive_column_info = 0xf1,
    fetch_column_data = 0xf2,
    receive_column_data = 0xf3
};

struct col_t {
    void* data = nullptr;
    col_data_t datatype = col_data_t::gen_void;
    size_t size = 0;
    size_t sizeInBytes = 0;
    bool is_remote = false;
    bool is_complete = false;
    std::mutex appendLock;

    ~col_t() {
        delete reinterpret_cast<char*>(data);
    }

    template <typename T>
    void allocate_aligned_internal(size_t _size) {
        std::lock_guard<std::mutex> _lk(appendLock);
        if (data == nullptr) {
            size = _size;
            data = aligned_alloc(alignof(T), _size * sizeof(T));
            sizeInBytes = _size * sizeof(T);
            //std::cout << "[col_t] Allocated " << _size * sizeof(T) << " bytes." << std::endl;
        }
    }

    void allocate_aligned_internal(col_data_t type, size_t _size) {
        switch (type) {
            case col_data_t::gen_smallint: {
                allocate_aligned_internal<uint8_t>(_size);
                break;
            }
            case col_data_t::gen_bigint: {
                allocate_aligned_internal<uint64_t>(_size);
                break;
            }
            case col_data_t::gen_float: {
                allocate_aligned_internal<float>(_size);
                break;
            }
            case col_data_t::gen_double: {
                allocate_aligned_internal<double>(_size);
                break;
            }
            default: {
                // std::cout << "[col_t] Error allocating data: Invalid datatype submitted. Nothing was allocated." << std::endl;
            }
        }
    }

    void append_chunk(size_t offset, size_t chunkSize, char* remoteData) {
        std::lock_guard<std::mutex> _lk(appendLock);
        if (data == nullptr) {
            std::cout << "!!! Implement allocation handling in append_chunk, aborting." << std::endl;
            return;
        }
        memcpy(reinterpret_cast<char*>(data) + offset, remoteData, chunkSize);
    }

    std::string print_data_head() const {
        switch (datatype) {
            case col_data_t::gen_smallint: {
                return print_data_head_typed<uint8_t*>();
            }
            case col_data_t::gen_bigint: {
                return print_data_head_typed<uint64_t*>();
            }
            case col_data_t::gen_float: {
                return print_data_head_typed<float*>();
            }
            case col_data_t::gen_double: {
                return print_data_head_typed<double*>();
            }
            default: {
                return "Error [strange datatype, nothing to print]";
            }
        }
    }

    std::string print_identity() const {
        std::stringstream ss;
        ss << size << " elements of type ";
        switch (datatype) {
            case col_data_t::gen_smallint: {
                ss << "uint8_t"
                   << " " << size * sizeof(uint8_t) << " Bytes";
                break;
            }
            case col_data_t::gen_bigint: {
                ss << "uint64_t"
                   << " " << size * sizeof(uint64_t) << " Bytes";
                break;
            }
            case col_data_t::gen_float: {
                ss << "float"
                   << " " << size * sizeof(float) << " Bytes";
                break;
            }
            case col_data_t::gen_double: {
                ss << "double"
                   << " " << size * sizeof(double) << " Bytes";
                break;
            }
        }
        ss << " [" << (is_remote ? "remote," : "local,") << (is_complete ? "complete" : "incomplete") << "]"
           << " CS: " << calc_checksum();
        return std::move(ss.str());
    }

    size_t calc_checksum() const {
        switch (datatype) {
            case col_data_t::gen_smallint: {
                return checksum<uint8_t>();
            }
            case col_data_t::gen_bigint: {
                return checksum<uint64_t>();
            }
            case col_data_t::gen_float: {
                return checksum<float>();
            }
            case col_data_t::gen_double: {
                return checksum<double>();
            }
        }
        return 0;
    }

    void log_to_file(std::string logfile) const {
        switch (datatype) {
            case col_data_t::gen_smallint: {
                std::cout << "Printing uint8_t column" << std::endl;
                log_to_file_typed<uint8_t>(logfile);
                break;
            }
            case col_data_t::gen_bigint: {
                std::cout << "Printing uint64_t column" << std::endl;
                log_to_file_typed<uint64_t>(logfile);
                break;
            }
            case col_data_t::gen_float: {
                std::cout << "Printing float column" << std::endl;
                log_to_file_typed<float>(logfile);
                break;
            }
            case col_data_t::gen_double: {
                std::cout << "Printing double column" << std::endl;
                log_to_file_typed<double>(logfile);
                break;
            }
        };
    }

   private:
    template <typename T>
    std::string print_data_head_typed() const {
        std::stringstream ss;
        ss << print_identity() << std::endl
           << "\t";
        auto tmp = static_cast<T>(data);
        for (size_t i = 0; i < size && i < 10; ++i) {
            if (datatype == gen_smallint) {
                ss << " " << (uint64_t)tmp[i];
            } else {
                ss << " " << tmp[i];
            }
        }
        return std::move(ss.str());
    }

    template <typename T>
    size_t checksum() const {
        size_t cs = 0;
        const auto tmp = static_cast<const T*>(data);
        for (size_t i = 0; i < size; ++i) {
            cs += tmp[i];
        }
        return cs;
    }

    template <typename T>
    void log_to_file_typed(std::string& logname) const {
        const auto tmp = static_cast<const T*>(data);
        std::ofstream log(logname);
        for (size_t i = 0; i < size; ++i) {
            if (datatype == gen_smallint) {
                log << " " << (uint64_t)tmp[i];
            } else {
                log << " " << tmp[i];
            }
        }
        log << std::endl;
        log.close();
    }
};

typedef std::unordered_map<std::string, col_t*> col_dict_t;
typedef std::unordered_map<std::string, col_network_info> col_remote_dict_t;

class DataCatalog {
   private:
    col_dict_t cols;
    col_dict_t remote_cols;
    col_remote_dict_t remote_col_info;
    DataCatalog();
    std::mutex appendLock;

   public:
    static DataCatalog& getInstance();

    DataCatalog(DataCatalog const&) = delete;
    void operator=(DataCatalog const&) = delete;
    ~DataCatalog();

    void clear();

    void registerCallback(uint8_t code, CallbackFunction cb) const;

    col_dict_t::iterator generate(std::string ident, col_data_t type, size_t elemCount);
    col_t* find_local(std::string ident) const;
    col_t* find_remote(std::string ident) const;
    col_t* add_remote_column(std::string name, col_network_info ni);

    void print_column(std::string& ident) const;
    void print_all() const;
    void print_all_remotes() const;
};