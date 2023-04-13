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

    std::cout << "Generating Dummy Test Data" << std::endl;

    const size_t dataSize = 2000000;
    //     const size_t dataSize = 200000000;
    //     const size_t numberLocalColumns = 18;

    // #pragma omp parallel for schedule(static, 2) num_threads(12)
    //     for (size_t i = 0; i < numberLocalColumns * 2; ++i) {
    //         std::stringstream nameStream;
    //         nameStream << "col_" << i;
    //         if (i < numberLocalColumns) {
    //             DataCatalog::getInstance().generate(nameStream.str(), col_data_t::gen_bigint, dataSize, 0);
    //         } else {
    //             DataCatalog::getInstance().generate(nameStream.str(), col_data_t::gen_bigint, dataSize * 0.2, 0);
    //         }
    //     }

    //     for (size_t i = 0; i < numberLocalColumns * 2; ++i) {
    //         std::stringstream nameStream;
    //         nameStream << "col_" << i;
    //         int currnode;
    //         col_t* column = DataCatalog::getInstance().find_local(nameStream.str());
    //         get_mempolicy(&currnode, NULL, 0, (void *)column->data, MPOL_F_NODE | MPOL_F_ADDR);
    //         std::cout << nameStream.str() << " is placed on numa node " << currnode << std::endl;
    //     }

#pragma omp parallel for schedule(static, 2) num_threads(8)
    for (size_t i = 0; i < 16; ++i) {
        std::string name = "col_" + std::to_string(i);

        DataCatalog::getInstance().generate(name, col_data_t::gen_bigint, dataSize, 0);

        for (size_t j = 0; j < 10; ++j) {
            std::string sub_name = name + "_" + std::to_string(j);
            DataCatalog::getInstance().generate(sub_name, col_data_t::gen_bigint, dataSize * 0.05, 0);
        }
    }

    DataCatalog::getInstance()
        .print_all();

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