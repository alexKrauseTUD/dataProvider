#include <numa.h>
#include <numaif.h>
#include <omp.h>

#include <ConnectionManager.hpp>
#include <Logger.hpp>
#include <TaskManager.hpp>
#include <Utility.hpp>
#include <algorithm>
#include <common.hpp>
#include <iostream>

#include "ArgParser.hpp"
#include "Benchmarks.hpp"
#include "Column.hpp"
#include "DataCatalog.hpp"
#include "TCPClient.hpp"
#include "TCPServer.hpp"
#include "UnitDefinition.pb.h"
#include "ext/DisaggDataDisciple/include/Utility.hpp"
#include "WorkItem.pb.h"
#include "WorkResponse.pb.h"
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

using namespace memConnect;

int main(int argc, char* argv[]) {
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

    struct bitmask* mask = numa_bitmask_alloc(numa_num_possible_nodes());
    numa_bitmask_setbit(mask, 0);
    numa_bind(mask);
    numa_bitmask_free(mask);

    DataCatalog::getInstance();
    Benchmarks::getInstance();

    tuddbs::TCPClient client("141.76.47.94", 31337);
    client.start();

    auto work_cb = [&client](void* data, size_t len) -> void {
        std::cout << "[Work Callback] Invoked." << std::endl;
        WorkItem item;
        item.ParseFromArray(data, len);
        switch (item.op_data_case()) {
            case WorkItem::OpDataCase::kJoinData: {
                std::cout << "Item contains a Join Operator." << std::endl;
            } break;
            case WorkItem::OpDataCase::kScanData: {
                std::cout << "Item contains a Scan Operator." << std::endl;
            } break;
            default: {
                std::cout << "An unkown entity is packed in this WorkItem." << std::endl;
            }
        }

        WorkResponse response;
        response.set_itemid(item.itemid());
        response.set_info("Your intermediates are ready!");

        tuddbs::TCPMetaInfo info;
        info.package_type = tuddbs::TCPPackageType::TaskFinished;
        info.payload_size = response.ByteSizeLong();
        void* out_mem = malloc(sizeof(tuddbs::TCPMetaInfo) + info.payload_size);

        const size_t message_size = tuddbs::Utility::serializeItemToMemory(out_mem, response, info);

        client.notifyHost(out_mem, message_size);
        free(out_mem);
    };

    auto updateUnitInfo_cb = [&client](void* data, size_t len) -> void {
        std::cout << "[UpdateUnitInfo Callback] Invoked." << std::endl;
        UnitDefinition unit;
        unit.set_unit_type(static_cast<uint32_t>(tuddbs::UnitType::Worker));

        tuddbs::TCPMetaInfo info;
        info.package_type = tuddbs::TCPPackageType::UpdateUnitType;
        info.payload_size = unit.ByteSizeLong();
        void* out_mem = malloc(sizeof(tuddbs::TCPMetaInfo) + info.payload_size);

        const size_t message_size = tuddbs::Utility::serializeItemToMemory(out_mem, unit, info);

        client.notifyHost(out_mem, message_size);
        free(out_mem);
    };

    auto text_cb = [&client](void* data, size_t len) -> void {
        std::string str(reinterpret_cast<char*>(data), len);
        std::cout << "Text Received: " << str << std::endl;
    };

    client.addCallback(tuddbs::TCPPackageType::Work, work_cb);
    client.addCallback(tuddbs::TCPPackageType::UpdateUnitType, updateUnitInfo_cb);
    client.addCallback(tuddbs::TCPPackageType::Text, text_cb);

    bool abort = false;
    auto globalExit = [&]() -> void {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        abort = true;
        ConnectionManager::getInstance().stop(true);
        client.closeConnection();
    };

    //     LOG_DEBUG1("Creating Columns" << std::endl;)
    // #pragma omp parallel for schedule(static, 2) num_threads(8)
    //     for (size_t i = 0; i < 24; ++i) {
    //         std::string name = "col_" + std::to_string(i);

    //         DataCatalog::getInstance().generate(name, col_data_t::gen_bigint, 200000000, 0);
    //     }

    //     #pragma omp parallel for schedule(static, 2) num_threads(8)
    //     for (size_t i = 24; i < 48; ++i) {
    //         std::string name = "col_" + std::to_string(i);

    //         DataCatalog::getInstance().generate(name, col_data_t::gen_bigint, 200000000, 1);
    //     }

    TaskManager::getInstance().setGlobalAbortFunction(globalExit);
    if (ConnectionManager::getInstance().configuration->get<bool>(MEMCONNECT_DEFAULT_CONNECTION_AUTO_LISTEN)) {
        std::thread([]() -> void { TaskManager::getInstance().executeByIdent("listenConnection"); }).detach();
    } else if (ConnectionManager::getInstance().configuration->get<bool>(MEMCONNECT_DEFAULT_CONNECTION_AUTO_INITIATE)) {
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