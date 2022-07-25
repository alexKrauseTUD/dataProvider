#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <mutex>

#include "DataCatalog.h"

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
                if (!col->is_complete && reinterpret_cast<char*>(col->current_end) <= reinterpret_cast<char*>(data) + CHUNK_MAX_SIZE) {
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
                // std::cout << "Stalling <" << (chunk_iterator ? "Chunked>" : "Full>") << std::endl;
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
        T* data;
        col_t* col;
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
        delete reinterpret_cast<char*>(data);
    }

    template <typename T>
    void allocate_aligned_internal(size_t _size) {
        std::lock_guard<std::mutex> _lk(appendLock);
        if (data == nullptr) {
            size = _size;
            data = aligned_alloc(alignof(T), _size * sizeof(T));
            sizeInBytes = _size * sizeof(T);
            // std::cout << "[col_t] Allocated " << _size * sizeof(T) << " bytes." << std::endl;
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
                std::cout << "[col_t] Error allocating data: Invalid datatype submitted. Nothing was allocated." << std::endl;
            }
        }

        memset(reinterpret_cast<char*>(data), 0, _size);
        current_end = data;
    }

    void request_data(bool fetch_complete_column) {
        std::lock_guard<std::mutex> _lk(iteratorLock);
        if (is_complete || requested_chunks > received_chunks) {
            // Do Nothing, ignore.
            return;
        }
        ++requested_chunks;

        // std::cout << "Col is requesting a new chunk." << std::endl;
        DataCatalog::getInstance().fetchColStub(1, ident, fetch_complete_column);
    }

    void append_chunk(size_t offset, size_t chunkSize, char* remoteData) {
        if (data == nullptr) {
            std::cout << "!!! Implement allocation handling in append_chunk, aborting." << std::endl;
            return;
        }
        memcpy(reinterpret_cast<char*>(data) + offset, remoteData, chunkSize);
    }

    void advance_end_pointer(size_t size) {
        std::lock_guard<std::mutex> _lk_i(iteratorLock);
        current_end = reinterpret_cast<void*>(reinterpret_cast<char*>(current_end) + size);
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
            if (datatype == col_data_t::gen_smallint) {
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
            if (datatype == col_data_t::gen_smallint) {
                log << " " << (uint64_t)tmp[i];
            } else {
                log << " " << tmp[i];
            }
        }
        log << std::endl;
        log.close();
    }
};