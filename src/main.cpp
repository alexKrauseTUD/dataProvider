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
#include "Logger.h"
#include "Worker.hpp"

void signal_handler(int signal) {
    switch (signal) {
        case SIGINT: {
            ConnectionManager::getInstance().stop(true);
            std::_Exit(EXIT_FAILURE);
        } break;
        case SIGUSR1: {
            ConnectionManager::getInstance().stop(false);
            std::_Exit(EXIT_SUCCESS);
        } break;
        default: {
            std::cerr << "Unexpected signal " << signal << " received!\n";
        } break;
    }
    std::_Exit(EXIT_FAILURE);
}

bool checkLinkUp() {
    std::array<char, 128> buffer;
    std::string result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen("ibstat", "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return (result.find("State: Active") != std::string::npos);
}

using namespace memordma;

int main(int argc, char *argv[]) {
    for (auto sig : {SIGINT, SIGUSR1}) {
        auto previous_handler = std::signal(sig, signal_handler);
        if (previous_handler == SIG_ERR) {
            std::cerr << "Setup of custom signal handler failed!\n";
            return EXIT_FAILURE;
        }
    }
    ConnectionManager::getInstance().configuration->add(argc, argv);
    Logger::LoadConfiguration();

    if (!checkLinkUp()) {
        LOG_FATAL("Could not find 'Active' state in ibstat, please check! Maybe you need to run \"sudo opensm -B\" on any server." << std::endl);
        exit(-2);
    }

    struct bitmask *mask = numa_bitmask_alloc(numa_num_possible_nodes());
    numa_bitmask_setbit(mask, 0);
    numa_bind(mask);
    numa_bitmask_free(mask);

    DataCatalog::getInstance();
    Benchmarks::getInstance();

    bool abort = false;
    auto globalExit = [&]() -> void {
        {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(500ms);
        }
        abort = true;
        ConnectionManager::getInstance().stop(true);
    };

    TaskManager::getInstance().setGlobalAbortFunction(globalExit);
    if (ConnectionManager::getInstance().configuration->get<bool>(MEMO_DEFAULT_CONNECTION_AUTO_LISTEN)) {
        std::thread([]() -> void { TaskManager::getInstance().executeByIdent("listenConnection"); }).detach();
    } else if (ConnectionManager::getInstance().configuration->get<bool>(MEMO_DEFAULT_CONNECTION_AUTO_INITIATE)) {
        std::thread([]() -> void { TaskManager::getInstance().executeByIdent("openConnection"); }).detach();
    }

    std::string content;
    std::string op;

    while (!abort) {
        op = "-1";
        TaskManager::getInstance().printAll();
        LOG_NOFORMAT("Type \"exit\" to terminate." << std::endl);

        std::getline(std::cin, op, '\n');
        if (op == "-1") {
            globalExit();
            continue;
        }

        LOG_DEBUG1("Chosen:" << op << std::endl);
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
                LOG_ERROR("No number given." << std::endl);
                continue;
            }
            if (converted) {
                TaskManager::getInstance().executeById(id);
            }
        }
    }

    return 0;
}