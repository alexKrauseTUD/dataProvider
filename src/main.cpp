#include <TaskManager.h>
#include <include/ConnectionManager.h>
#include <include/TaskManager.h>
#include <include/common.h>
#include <include/util.h>

#include <algorithm>
#include <iostream>

#include "DataCatalog.h"

int main() {
    std::cout << "col_data_t size: " << sizeof(col_data_t) << std::endl;

    // auto worker_it = DataCatalog::getInstance().generate("worker", col_data_t::gen_smallint, 20);
    // auto salary_it = DataCatalog::getInstance().generate("salary", col_data_t::gen_float, 5);

    size_t lineorderSize = 2000000;

    auto lo_orderdate = DataCatalog::getInstance().generate("lo_orderdate", col_data_t::gen_bigint, lineorderSize);
    auto lo_discount = DataCatalog::getInstance().generate("lo_discount", col_data_t::gen_bigint, lineorderSize);
    auto lo_quantity = DataCatalog::getInstance().generate("lo_quantity", col_data_t::gen_bigint, lineorderSize);
    auto lo_extendedprice = DataCatalog::getInstance().generate("lo_extendedprice", col_data_t::gen_bigint, lineorderSize);

    size_t dateSize = 50000;

    auto d_datekey = DataCatalog::getInstance().generate("d_datekey", col_data_t::gen_bigint, dateSize);
    auto d_year = DataCatalog::getInstance().generate("d_year", col_data_t::gen_bigint, dateSize);

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