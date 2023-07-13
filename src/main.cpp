#include <TaskManager.h>
#include <Utility.h>
#include <include/ConnectionManager.h>
#include <include/TaskManager.h>
#include <include/common.h>
#include <numa.h>
#include <numaif.h>
#include <omp.h>

#include <algorithm>
#include <iostream>

#include "Benchmarks.hpp"
#include "Column.h"
#include "DataCatalog.h"
#include "Worker.hpp"

int main() {
    struct bitmask *mask = numa_bitmask_alloc(numa_num_possible_nodes());
    numa_bitmask_setbit(mask, 0);
    numa_bind(mask);
    numa_bitmask_free(mask);

    // DataCatalog::getInstance().print_all();

    DataCatalog::getInstance();
    Benchmarks::getInstance();

    config_t config = {.deviceName = "",
                       .serverName = "",
                       .tcpPort = 20000,
                       .clientMode = false,
                       .infiniBandPort = 1,
                       .gidIndex = -1};

    bool abort = false;
    auto globalExit = [&]() -> void {
        {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(500ms);
        }
        ConnectionManager::getInstance().stop(true);
        abort = true;
    };

    TaskManager::getInstance().setGlobalAbortFunction(globalExit);
    ConnectionManager::getInstance();

    std::string content;
    std::string op;

    while (!abort) {
        op = "-1";
        TaskManager::getInstance().printAll();
        std::cout << "Type \"exit\" to terminate." << std::endl;
        // std::cin >> op;
        std::getline(std::cin, op, '\n');
        if (op == "-1") {
            globalExit();
            continue;
        }

        std::cout << "Chosen:" << op << std::endl;
        std::transform(op.begin(), op.end(), op.begin(), [](unsigned char c) { return std::tolower(c); });

        if (op == "exit") {
            globalExit();
        } else {
            std::size_t id;
            bool converted = false;
            try {
                id = stol(op);
                converted = true;
            } catch (...) {
                std::cout << "[Error] No number given." << std::endl;
                continue;
            }
            if (converted) {
                TaskManager::getInstance().executeById(id);
            }
        }
    }
}