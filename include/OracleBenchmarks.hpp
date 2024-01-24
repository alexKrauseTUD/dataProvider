#pragma once

#include <stdlib.h>

#include <vector>
#include <mutex>
#include <condition_variable>

class OracleBenchmarks {
   public:
    static OracleBenchmarks& getInstance() {
        static OracleBenchmarks instance;
        return instance;
    }
    ~OracleBenchmarks();

    void executeAllOracleBenchmarks();

   private:
    bool dataGenerationDone = false;
    mutable std::mutex dataGenerationLock;
    std::condition_variable dataGenerationCV;

    OracleBenchmarks();

    void execHashJoinBenchmark();

    void generateBenchmarkData(const uint64_t numberConnections, const uint64_t distinctLocalColumns, const uint64_t localColumnElements, const uint64_t remoteColumnElements);

    static const size_t OPTIMAL_BLOCK_SIZE = 65536;
    static const size_t NUMBER_OF_JOINS = 10;
    const std::vector<size_t> global_pins = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53};
};