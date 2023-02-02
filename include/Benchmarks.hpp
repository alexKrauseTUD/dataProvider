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

    void executeAllBenchmarks(std::string& logName);

    static const size_t OPTIMAL_BLOCK_SIZE = 65536;

   private:
    Benchmarks();

    void execLocalBenchmark(std::string& logName, std::string locality);
    void execRemoteBenchmark(std::string& logName, std::string locality);

    static const size_t WORKER_NUMBER = 8;
    static const size_t TASKS_PER_WORKER = 4;
    Worker workers[WORKER_NUMBER];
};
