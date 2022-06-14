#pragma once

#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <variant>

enum col_data_t {
    gen_float = 0,
    gen_double = 1,
    gen_smallint = 2,
    gen_bigint = 3
};

struct col_network_info {
    size_t size_info;
    uint8_t type_info;

    col_network_info( size_t sz, col_data_t dt ) {
        size_info = sz;
        type_info = dt;
    }
};

enum catalog_communication_code : uint8_t {
    send_column_info = 0xf0,
    receive_column_info = 0xf1
};

struct col_t {
    void* data;
    col_data_t datatype;
    size_t size;

    ~col_t() {
        delete reinterpret_cast<char*>(data);
    }

    template <typename T>
    void allocate_aligned_internal(size_t size) {
        data = aligned_alloc(alignof(T), size * sizeof(T));
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
        return std::move(ss.str());
    }

   private:
    template <typename T>
    std::string print_data_head_typed() const {
        std::stringstream ss;
        ss << size << " elements of type ";
        auto tmp = static_cast<T>(data);
        for (size_t i = 0; i < size && i < 10; ++i) {
            if (datatype == gen_smallint) {
                ss << " " << (uint64_t)tmp[i];
            } else {
                ss << tmp[i];
            }
        }
        return std::move(ss.str());
    }
};

typedef std::unordered_map<std::string, col_t*> col_dict_t;

class DataCatalog {
   private:
    col_dict_t cols;
    DataCatalog();

   public:
    static DataCatalog& getInstance();

    DataCatalog(DataCatalog const&) = delete;
    void operator=(DataCatalog const&) = delete;
    ~DataCatalog();

    void clear();

    col_dict_t::iterator generate(std::string ident, col_data_t type, size_t elemCount);

    col_t* find(std::string ident) const;

    void print_column(std::string& ident) const;
    void print_all() const;
};