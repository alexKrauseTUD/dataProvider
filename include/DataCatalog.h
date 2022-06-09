#pragma once

#include <iostream>
#include <unordered_map>
#include <random>

typedef std::unordered_map< std::string, size_t* > col_dict_t;

class DataCatalog {
private:    
    col_dict_t cols;
    DataCatalog() {}

public:
    static DataCatalog &getInstance() {
        static DataCatalog instance;
        return instance;
    }

    DataCatalog(DataCatalog const &) = delete;
    void operator=(DataCatalog const &) = delete;
    ~DataCatalog() {
        clear();
    }

    void clear() {
        for ( auto it : cols ) {
            delete it.second;
        }
    }

    col_dict_t::iterator generate( std::string ident, size_t amount ) {
        auto it = cols.find( ident );
        
        if ( it != cols.end() ) { 
            std::cout << "Column width ident " << ident << " already present, returning old data." << std::endl;
            return it; 
        }

        std::default_random_engine generator;
        std::uniform_int_distribution<size_t> distribution(0,10000);

        size_t* col = (size_t*) aligned_alloc( alignof(size_t), amount * sizeof( size_t ) );
        for ( size_t i = 0; i < amount; ++i ) {
            col[ i ] = distribution( generator );
        }
        cols.insert( {ident, col} );
        return cols.find( ident );
    }

    size_t* find( std::string ident ) const {
        auto it = cols.find( ident );
        if ( it != cols.end() ) {
            return (*it).second;
        }
        return nullptr;
    }
};