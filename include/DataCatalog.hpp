#pragma once

#include <condition_variable>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>

#include "ConnectionManager.hpp"

enum class catalog_communication_code : uint8_t {
    send_column_info = 0xA0,
    receive_column_info,
    fetch_column_data,
    receive_column_data,
    fetch_column_chunk,
    receive_column_chunk,
    fetch_column_as_stream,
    receive_column_as_stream,
    fetch_pseudo_pax,
    fetch_pseudo_pax_stream,
    receive_pseudo_pax,
    receive_pseudo_pax_stream,
    receive_last_pseudo_pax,
    reconfigure_chunk_size,
    ack_reconfigure_chunk_size,
    generate_benchmark_data,
    ack_generate_benchmark_data,
    clear_catalog,
    ack_clear_catalog
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
                LOG_WARNING("[col_network_info] Datatype case not implemented! Column size not calculated." << std::endl;)
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
            default: {
                using namespace memConnect;
                LOG_ERROR("Saw gen_void but its not handled." << std::endl;)
            }
        }
        return ss.str();
    }
};

struct col_t;
struct table_t;

struct inflight_col_info_t {
    col_t* col;
    std::size_t curr_offset;
};

struct pax_inflight_col_info_t {
    std::vector<col_t*> cols;
    std::queue<std::pair<size_t, size_t> > prepared_offsets;
    std::mutex offset_lock;
    std::thread* prepare_thread = nullptr;
    std::condition_variable offset_cv;
    size_t metadata_size = 0;
    bool prepare_triggered = false;
    bool prepare_complete = false;
    char* metadata_buf = nullptr;
    char* payload_buf = nullptr;

    void reset() {
        std::lock_guard<std::mutex> lk(offset_lock);
        std::queue<std::pair<size_t, size_t> > empty;
        prepared_offsets.swap(empty);
        if (metadata_buf) delete metadata_buf;
        metadata_buf = nullptr;
        if (payload_buf) delete payload_buf;
        payload_buf = nullptr;
        if (prepare_thread) {
            prepare_thread->join();
            delete prepare_thread;
        }
        prepare_triggered = false;
        prepare_complete = false;
        metadata_size = 0;
    }

    ~pax_inflight_col_info_t() {
        reset();
    }
};

typedef std::unordered_map<std::string, col_t*> col_dict_t;
typedef std::unordered_map<std::string, col_network_info> col_remote_dict_t;
typedef std::unordered_map<std::string, inflight_col_info_t> incomplete_transimssions_dict_t;
typedef std::unordered_map<std::string, pax_inflight_col_info_t*> incomplete_pax_transimssions_dict_t;

class DataCatalog {
   private:
    col_dict_t cols;
    col_dict_t remote_cols;
    col_remote_dict_t remote_col_info;
    bool col_info_received = false;
    bool reconfigured = false;
    bool dataGenerationDone = false;
    bool clearCatalogDone = false;
    mutable std::mutex remote_info_lock;
    mutable std::mutex reconfigure_lock;
    mutable std::mutex appendLock;
    mutable std::mutex inflightLock;
    mutable std::mutex paxInflightLock;
    mutable std::mutex dataGenerationLock;
    mutable std::mutex clearCatalogLock;
    std::condition_variable remote_info_available;
    std::condition_variable reconfigure_done;
    std::condition_variable data_generation_done;
    std::condition_variable clear_catalog_done;

    incomplete_transimssions_dict_t inflight_cols;
    incomplete_pax_transimssions_dict_t pax_inflight_cols;

    DataCatalog();

   public:
    uint64_t dataCatalog_chunkMaxSize = 1024 * 1024 * 2;
    uint64_t dataCatalog_chunkThreshold = 1024 * 1024 * 2;
    std::map<std::string, table_t*> tables;

    static DataCatalog& getInstance();

    DataCatalog(DataCatalog const&) = delete;
    void operator=(DataCatalog const&) = delete;
    ~DataCatalog();

    void clear(bool sendRemot = false, bool destructor = false);

    void registerCallback(uint8_t code, CallbackFunction cb) const;

    col_dict_t::iterator generate(std::string ident, col_data_t type, size_t elemCount, int node);
    col_t* find_local(std::string ident) const;
    col_t* find_remote(std::string ident) const;
    col_t* add_column(std::string ident, col_t* col);
    col_t* add_remote_column(std::string name, col_network_info ni);

    void remoteInfoReady();
    void fetchRemoteInfo();
    void print_column(std::string& ident) const;
    void print_all() const;
    void print_all_remotes() const;

    std::vector<std::string> getLocalColumnNames() const;
    std::vector<std::string> getRemoteColumnNames() const;

    void eraseAllRemoteColumns();

    void reconfigureChunkSize(const uint64_t newChunkSize, const uint64_t newChunkThreshold);

    void generateBenchmarkData(const uint64_t distinctLocalColumns, const uint64_t remoteColumnsForLocal, const uint64_t localColumnElements, const uint64_t percentageOfRemote, const uint64_t localNumaNode = 0, const uint64_t remoteNumaNode = 0, bool sendToRemote = false, bool createTables = false);

    // Communication stubs
    void fetchColStub(const std::size_t conId, const std::string& ident, bool whole_column = false, bool asStream = false) const;
    void fetchPseudoPax(const std::size_t conId, const std::vector<std::string>& idents, const bool asStream = false) const;
};