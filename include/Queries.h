#ifndef QUERIES_H
#define QUERIES_H

#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <functional>

void executeAllBenchmarkingQueries(std::string& logName);
void executeNUMABenchmarkingQueries(std::string& logName);

#endif