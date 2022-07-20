#pragma once

#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <thread>

#include "DataCatalog.h"

struct col_t {
    template <typename T>
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
            if (col->is_remote && (static_cast<char*>(col->data) + col->readableOffset) < reinterpret_cast<char*>(data) + 12288) {
                col->request_next_chunk();
            }
        }

        void check_end() {
            if (
                col->is_remote &&
                (reinterpret_cast<char*>(data) == reinterpret_cast<char*>(col->data) + col->readableOffset) &&
                *this != col->end<T>()) {
                std::unique_lock<std::mutex> lk(col->iteratorLock);
                col->iteratorCv.wait(lk, [this] { return reinterpret_cast<char*>(data) != reinterpret_cast<char*>(col->data) + col->readableOffset; });
            }
        }

        col_iterator_t& operator++() {
            request_next();
            data++;
            check_end();
            return *this;
        }

        col_iterator_t operator++(int) {
            request_next();
            col_iterator_t tmp = *this;
            ++(*this);
            check_end();
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
    std::condition_variable iteratorCv;

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

    template <typename T>
    col_iterator_t<T> begin() {
        return col_iterator_t<T>(
            this,
            static_cast<T*>(data));
    }

    template <typename T>
    col_iterator_t<T> end() {
        char* tmp = static_cast<char*>(data);
        return col_iterator_t<T>(
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
    }

    void request_next_chunk() {
        std::lock_guard<std::mutex> _lk(iteratorLock);
        if (requested_chunks > received_chunks) {
            // Do Nothing, ignore.
            return;
        }
        ++requested_chunks;

        // std::cout << "Col is requesting a new chunk." << std::endl;
        DataCatalog::getInstance().fetchColStub(1, ident, false);
    }

    void append_chunk(size_t offset, size_t chunkSize, char* remoteData) {
        std::lock_guard<std::mutex> _lk_a(appendLock);
        if (data == nullptr) {
            std::cout << "!!! Implement allocation handling in append_chunk, aborting." << std::endl;
            return;
        }
        memcpy(reinterpret_cast<char*>(data) + offset, remoteData, chunkSize);
        std::lock_guard<std::mutex> _lk_i(iteratorLock);
        readableOffset += chunkSize;
        iteratorCv.notify_all();
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