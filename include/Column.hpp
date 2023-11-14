#pragma once

#include <numa.h>

#include <Logger.hpp>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <mutex>

#include "DataCatalog.hpp"

struct col_t {
    template <typename T, bool chunk_iterator>
    struct col_iterator_t {
       public:
        col_iterator_t(col_t* p_col, T* p_data)
            : col{p_col}, data{p_data} {
                              // Nothing else to do for now.
                          };

        T& operator*() const {
            return *data;
        };

        T* operator->() const {
            return data;
        };

        void request_next() {
            if (chunk_iterator) {
                if (!col->is_complete && reinterpret_cast<char*>(col->current_end) <= reinterpret_cast<char*>(data) + DataCatalog::getInstance().dataCatalog_chunkThreshold) {
                    col->request_data(!chunk_iterator);
                }
            }
        }

        void check_end() {
            if (
                !col->is_complete &&                                                          // column not fully loaded
                (reinterpret_cast<char*>(data) == reinterpret_cast<char*>(col->current_end))  // Last readable element reached
            ) {
                std::unique_lock<std::mutex> lk(col->iteratorLock);
                LOG_DEBUG2("Stalling <" << (chunk_iterator ? "Chunked>" : "Full>") << std::endl;)
                col->iterator_data_available.wait(lk, [this] { return reinterpret_cast<char*>(data) < reinterpret_cast<char*>(col->current_end); });
            }
        }

        col_iterator_t& operator++() {
            data++;
            request_next();
            check_end();
            return *this;
        }

        col_iterator_t operator++(int) {
            col_iterator_t tmp = *this;
            ++(*this);
            return tmp;
        }

        friend bool operator==(const col_iterator_t& a, const col_iterator_t& b) {
            return a.data == b.data;
        };

        friend bool operator!=(const col_iterator_t& a, const col_iterator_t& b) {
            return a.data != b.data;
        };

       private:
        col_t* col;
        T* data;
    };

    void* data = nullptr;
    void* current_end = nullptr;
    col_data_t datatype = col_data_t::gen_void;
    size_t size = 0;
    size_t sizeInBytes = 0;
    size_t readableOffset = 0;
    std::string ident = "";
    bool is_remote = false;
    bool is_complete = false;
    size_t requested_chunks = 0;
    size_t received_chunks = 0;
    std::mutex iteratorLock;
    std::mutex appendLock;
    std::condition_variable iterator_data_available;

    ~col_t() {
        // May be a problem when freeing memory not allocated with numa_alloc
        numa_free(data, sizeInBytes);
    }

    template <typename T>
    void allocate_aligned_internal(size_t _size) {
        if (data == nullptr) {
            size = _size;
            data = aligned_alloc(alignof(T), _size * sizeof(T));
            sizeInBytes = _size * sizeof(T);
            LOG_DEBUG2("[col_t] Allocated " << _size * sizeof(T) << " bytes." << std::endl;)
        }
    }

    template <typename T>
    void allocate_on_numa(size_t _size, int node) {
        if (data == nullptr) {
            size = _size;
            data = numa_alloc_onnode(_size * sizeof(T), node);
            sizeInBytes = _size * sizeof(T);
        }
    }

    template <typename T, bool chunked>
    col_iterator_t<T, chunked> begin() {
        std::unique_lock<std::mutex> lk(iteratorLock);
        iterator_data_available.wait(lk, [this] { return current_end != data; });
        return col_iterator_t<T, chunked>(
            this,
            static_cast<T*>(data));
    }

    template <typename T, bool chunked>
    col_iterator_t<T, chunked> end() {
        char* tmp = static_cast<char*>(data);
        return col_iterator_t<T, chunked>(
            this,
            reinterpret_cast<T*>(tmp + sizeInBytes));
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
                LOG_ERROR("[col_t] Error allocating data: Invalid datatype submitted. Nothing was allocated." << std::endl;)
            }
        }

        memset(reinterpret_cast<char*>(data), 0, _size);
        current_end = data;
    }

    void allocate_on_numa(col_data_t type, size_t _size, int node) {
        switch (type) {
            case col_data_t::gen_smallint: {
                allocate_on_numa<uint8_t>(_size, node);
                break;
            }
            case col_data_t::gen_bigint: {
                allocate_on_numa<uint64_t>(_size, node);
                break;
            }
            case col_data_t::gen_float: {
                allocate_on_numa<float>(_size, node);
                break;
            }
            case col_data_t::gen_double: {
                allocate_on_numa<double>(_size, node);
                break;
            }
            default: {
                LOG_ERROR("[col_t] Error allocating data: Invalid datatype submitted. Nothing was allocated." << std::endl;)
            }
        }

        // memset(reinterpret_cast<char*>(data), 0, _size);
        current_end = data;
    }

    void request_data(bool fetch_complete_column) {
        std::unique_lock<std::mutex> _lk(iteratorLock);
        if (is_complete || requested_chunks > received_chunks) {
            LOG_DEBUG2("<data request ignored: " << (is_complete ? "is_complete" : "not_complete") << ">" << std::endl;)
            // Do Nothing, ignore.
            return;
        }
        ++requested_chunks;

        DataCatalog::getInstance().fetchColStub(1, ident, fetch_complete_column);
    }

    void append_chunk(size_t offset, size_t chunkSize, char* remoteData) {
        if (data == nullptr) {
            LOG_WARNING("!!! Implement allocation handling in append_chunk, aborting." << std::endl;)
            return;
        }
        memcpy(reinterpret_cast<char*>(data) + offset, remoteData, chunkSize);
    }

    void advance_end_pointer(size_t _size) {
        current_end = reinterpret_cast<void*>(reinterpret_cast<char*>(current_end) + _size);
        iterator_data_available.notify_all();
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
            default: {
                using namespace memConnect;
                LOG_ERROR("Saw gen_void but its not handled." << std::endl;)
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
            default: {
                using namespace memConnect;
                LOG_ERROR("Saw gen_void but its not handled." << std::endl;)
            }
        }
        return 0;
    }

    void log_to_file(std::string logfile) const {
        switch (datatype) {
            case col_data_t::gen_smallint: {
                LOG_DEBUG1("Printing uint8_t column" << std::endl;)
                log_to_file_typed<uint8_t>(logfile);
                break;
            }
            case col_data_t::gen_bigint: {
                LOG_DEBUG1("Printing uint64_t column" << std::endl;)
                log_to_file_typed<uint64_t>(logfile);
                break;
            }
            case col_data_t::gen_float: {
                LOG_DEBUG1("Printing float column" << std::endl;)
                log_to_file_typed<float>(logfile);
                break;
            }
            case col_data_t::gen_double: {
                LOG_DEBUG1("Printing double column" << std::endl;)
                log_to_file_typed<double>(logfile);
                break;
            }
            default: {
                using namespace memConnect;
                LOG_ERROR("Saw gen_void but its not handled." << std::endl;)
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
            if (datatype == col_data_t::gen_smallint) {
                ss << " " << static_cast<uint64_t>(tmp[i]);
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
            if (datatype == col_data_t::gen_smallint) {
                log << " " << static_cast<uint64_t>(tmp[i]);
            } else {
                log << " " << tmp[i];
            }
        }
        log << std::endl;
        log.close();
    }
};

struct table_t {
   public:
    std::vector<col_t*> columns;
    std::string ident;
    size_t numCols;
    size_t numRows;
    size_t onNode;
    size_t bufferRatio;
    bool isFactTable;

    explicit table_t(std::string _ident, size_t _onNode) : ident{_ident}, numCols{0}, numRows{0}, onNode{_onNode}, bufferRatio{0}, isFactTable{true} {};

    table_t(std::string _ident, size_t _numCols, size_t _numRows, size_t _onNode, size_t _bufferRatio, bool _isFactTable) : ident{_ident}, numCols{_numCols}, numRows{_numRows}, onNode{_onNode}, bufferRatio{_bufferRatio}, isFactTable{_isFactTable} {
        for (size_t i = 0; i < numCols; ++i) {
            columns.emplace_back(new col_t);
        }

        std::default_random_engine generator;

        size_t colId = 0;

        for (auto& col : columns) {
            col->ident = ident + "_col_" + std::to_string(colId);
            col->size = numRows;
            col->sizeInBytes = numRows * sizeof(uint64_t);

            if (onNode != 0) {
                col->is_remote = true;
                col->is_complete = false;
            } else {
                col->is_remote = false;
                col->is_complete = true;
            }

            col->datatype = col_data_t::gen_bigint;
            col->data = numa_alloc_onnode(col->sizeInBytes, onNode);

            auto data = reinterpret_cast<uint64_t*>(col->data);

            if (colId == 0) {
                for (size_t i = 0; i < numRows; ++i) {
                    data[i] = i;
                }
            } else {
                std::uniform_int_distribution<uint64_t> distribution;

                if (isFactTable) {
                    distribution = std::uniform_int_distribution<uint64_t>(0, (numRows * bufferRatio * 0.01) - 1);
                } else {
                    distribution = std::uniform_int_distribution<uint64_t>(0, 100);
                }

                for (size_t i = 0; i < numRows; ++i) {
                    data[i] = distribution(generator);
                }
            }

            if (col->is_complete) {
                col->readableOffset = numRows * sizeof(uint64_t);
            }

            col->current_end = col->data;
            DataCatalog::getInstance().add_column(col->ident, col);

            ++colId;
        }
    }

    ~table_t() {
        for (auto col : columns) {
            delete col;
        }

        columns.clear();
    };

    void addColumn(uint64_t* data, size_t elementCount) {
        if (numRows == 0) {
            numRows = elementCount;
        } else {
            if (numRows != elementCount) {
                LOG_ERROR("Dimension of column does not match with table!" << std::endl;)
                return;
            }
        }

        col_t* tmp = new col_t();

        tmp->ident = ident + "_col_" + std::to_string(numCols);
        tmp->size = numRows;
        tmp->sizeInBytes = numRows * sizeof(uint64_t);

        if (onNode != 0) {
            tmp->is_remote = true;
            tmp->is_complete = false;
        } else {
            tmp->is_remote = false;
            tmp->is_complete = true;
        }

        tmp->datatype = col_data_t::gen_bigint;
        tmp->data = numa_alloc_onnode(tmp->sizeInBytes, onNode);

        std::copy(data, data + elementCount, reinterpret_cast<uint64_t*>(tmp->data));

        if (tmp->is_complete) {
            tmp->readableOffset = numRows * sizeof(uint64_t);
        }

        columns.emplace_back(tmp);

        ++numCols;
    }

    col_t* getPrimaryKeyColumn() {
        return columns[0];
    }
};