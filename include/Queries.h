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

void executeLocalBenchmarkingQueries(std::string& logName, std::string locality);
void executeRemoteBenchmarkingQueries(std::string& logName);

void executeFrontPageBenchmarkingQueries(std::string& logName);

void executeLocalMTBenchmarkingQueries(std::string& logName, std::string locality);
void executeRemoteMTBenchmarkingQueries(std::string& logName);
