#pragma once

#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "Worker.hpp"

extern std::chrono::duration<double> waitingTime;
extern std::chrono::duration<double> workingTime;

class Benchmarks {
   public:
    static Benchmarks& getInstance() {
        static Benchmarks instance;
        return instance;
    }
    ~Benchmarks();

    void executeAllBenchmarks();

    static const size_t OPTIMAL_BLOCK_SIZE = 65536;

   private:
    Benchmarks();

    void execLocalBenchmark(std::string& logName, std::string locality);
    void execRemoteBenchmark(std::string& logName, std::string locality);
    void execLocalBenchmarkMW(std::string& logName, std::string locality);
    void execRemoteBenchmarkMW(std::string& logName, std::string locality);

    template <bool filter>
    void execUPIBenchmark();

    void execRDMABenchmark();
    void execRDMAHashJoinBenchmark();
    void execRDMAHashJoinPGBenchmark();
    void execRDMAHashJoinStarBenchmark();

    void execChunkVsChunkStreamBenchmark();
    void execPaxVsPaxStreamBenchmark();

    static const size_t WORKER_NUMBER = 8;
    // Worker workers[WORKER_NUMBER];
};
