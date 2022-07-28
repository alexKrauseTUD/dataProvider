#pragma once

#include <condition_variable>
#include <fstream>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>

#include "ConnectionManager.h"

enum class catalog_communication_code : uint8_t {
    send_column_info = 0xA0,
    receive_column_info,
    fetch_column_data,
    receive_column_data,
    fetch_column_chunk,
    receive_column_chunk,
    fetch_pseudo_pax,
    receive_pseudo_pax,
    receive_last_pseudo_pax,
    reconfigure_chunk_size,
    ack_reconfigure_chunk_size
};

enum class col_data_t : unsigned char {
    gen_void,
    gen_float,
    gen_double,
    gen_smallint,
    gen_bigint
};

struct col_network_info {
    size_t size_info;
    col_data_t type_info;
    size_t received_bytes;

    col_network_info() = default;

    col_network_info(size_t sz, col_data_t dt) {
        size_info = sz;
        type_info = dt;
        received_bytes = 0;
    }

    col_network_info(const col_network_info& other) = default;
    col_network_info& operator=(const col_network_info& other) = default;

    bool check_complete() const {
        return received_bytes == sizeInBytes();
    }

    size_t sizeInBytes() const {
        switch (type_info) {
            case col_data_t::gen_float:
                return size_info * sizeof(float);
            case col_data_t::gen_double:
                return size_info * sizeof(double);
            case col_data_t::gen_smallint:
                return size_info * sizeof(uint8_t);
            case col_data_t::gen_bigint:
                return size_info * sizeof(uint64_t);
            default:
                std::cout << "[col_network_info] Datatype case not implemented! Column size not calculated." << std::endl;
                return 0;
        }
    }

    static std::string col_data_type_to_string(col_data_t info) {
        switch (info) {
            case col_data_t::gen_float:
                return "float";
            case col_data_t::gen_double:
                return "double";
            case col_data_t::gen_smallint:
                return "uint8_t";
            case col_data_t::gen_bigint:
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

struct col_t;

struct inflight_col_info_t {
    col_t* col;
    std::size_t curr_offset;
};

struct pax_inflight_col_info_t {
    std::vector<col_t*> cols;
    std::size_t curr_offset;
};

typedef std::unordered_map<std::string, col_t*> col_dict_t;
typedef std::unordered_map<std::string, col_network_info> col_remote_dict_t;
typedef std::unordered_map<std::string, inflight_col_info_t> incomplete_transimssions_dict_t;
typedef std::unordered_map<std::string, pax_inflight_col_info_t> incomplete_pax_transimssions_dict_t;

class DataCatalog {
   private:
    col_dict_t cols;
    col_dict_t remote_cols;
    col_remote_dict_t remote_col_info;
    bool col_info_received = false;
    bool reconfigured = false;
    mutable std::mutex remote_info_lock;
    mutable std::mutex reconfigure_lock;
    mutable std::mutex appendLock;
    mutable std::mutex inflightLock;
    mutable std::mutex paxInflightLock;
    std::condition_variable remote_info_available;
    std::condition_variable reconfigure_done;

    incomplete_transimssions_dict_t inflight_cols;
    incomplete_pax_transimssions_dict_t pax_inflight_cols;

    DataCatalog();

   public:
    uint64_t dataCatalog_chunkMaxSize = 1024 * 512 * 4;
    uint64_t dataCatalog_chunkThreshold = 1024 * 512 * 4;

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

    void remoteInfoReady();
    void fetchRemoteInfo();
    void print_column(std::string& ident) const;
    void print_all() const;
    void print_all_remotes() const;

    void eraseAllRemoteColumns();

    // Communication stubs
    void fetchColStub(std::size_t conId, std::string& ident, bool whole_column = true) const;
    void fetchPseudoPax(std::size_t conId, std::vector<std::string> idents) const;
};