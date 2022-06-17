#include <algorithm>
#include <iostream>
#include <TaskManager.h>
#include <include/ConnectionManager.h>
#include <include/TaskManager.h>
#include <include/common.h>
#include <include/util.h>

#include "DataCatalog.h"

int main() {
    std::cout << "Works" << std::endl;

    auto worker_it = DataCatalog::getInstance().generate( "worker", col_data_t::gen_smallint, 20 );
    auto salary_it = DataCatalog::getInstance().generate( "salary", col_data_t::gen_float, 5 );

    DataCatalog::getInstance().print_all();


    config_t config = {.dev_name = "",
                    .server_name = "",
                    .tcp_port = 20000,
                    .client_mode = false,
                    .ib_port = 1,
                    .gid_idx = -1};

                        bool abort = false;
    auto globalExit = [&]() -> void {
        {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(500ms);
        }
        ConnectionManager::getInstance().stop();
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