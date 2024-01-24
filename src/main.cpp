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

#include "Benchmarks.hpp"
#include "OracleBenchmarks.hpp"
#include "Column.hpp"
#include "DataCatalog.hpp"
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
    OracleBenchmarks::getInstance();

    bool abort = false;
    auto globalExit = [&]() -> void {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        abort = true;
        ConnectionManager::getInstance().stop(true);
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
        std::thread([]() -> void {
            NetworkConfig config = {.deviceName = ConnectionManager::getInstance().configuration->getAsString(MEMCONNECT_DEFAULT_IB_DEVICE_NAME),
                                    .serverName = "172.16.7.205",
                                    .tcpPort = ConnectionManager::getInstance().configuration->get<uint32_t>(MEMCONNECT_DEFAULT_TCP_PORT),
                                    .clientMode = true,
                                    .infiniBandPort = ConnectionManager::getInstance().configuration->get<int32_t>(MEMCONNECT_DEFAULT_IB_PORT),
                                    .gidIndex = ConnectionManager::getInstance().configuration->get<int32_t>(MEMCONNECT_DEFAULT_IB_GLOBAL_INDEX)};

            BufferConfiguration bufferConfig;

            size_t connectionId = ConnectionManager::getInstance().registerConnection(config, bufferConfig, ConnectionType(ConnectionManager::getInstance().configuration->get<uint8_t>(MEMCONNECT_DEFAULT_CONNECTION_TYPE)));

            if (connectionId != 0) {
                LOG_SUCCESS("Connection " << connectionId << " opened for config: " << std::endl);
                ConnectionManager::getInstance().getConnectionById(connectionId)->printConnectionInfo();
            } else {
                LOG_ERROR("Something went wrong! The connection could not be opened for config: " << std::endl);
            }
        }).detach();
    } else if (ConnectionManager::getInstance().configuration->get<bool>(MEMCONNECT_DEFAULT_CONNECTION_AUTO_INITIATE)) {
        std::thread([]() -> void {
            uint8_t numOwnReceive = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMCONNECT_DEFAULT_OWN_RECEIVE_BUFFER_COUNT);
            uint32_t sizeOwnReceive = ConnectionManager::getInstance().configuration->get<uint32_t>(MEMCONNECT_DEFAULT_OWN_RECEIVE_BUFFER_SIZE);
            uint8_t numRemoteReceive = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMCONNECT_DEFAULT_REMOTE_RECEIVE_BUFFER_COUNT);
            uint32_t sizeRemoteReceive = ConnectionManager::getInstance().configuration->get<uint32_t>(MEMCONNECT_DEFAULT_REMOTE_RECEIVE_BUFFER_SIZE);
            uint64_t sizeOwnSend = ConnectionManager::getInstance().configuration->get<uint64_t>(MEMCONNECT_DEFAULT_OWN_SEND_BUFFER_SIZE);
            uint64_t sizeRemoteSend = ConnectionManager::getInstance().configuration->get<uint64_t>(MEMCONNECT_DEFAULT_REMOTE_SEND_BUFFER_SIZE);

            BufferConfiguration bufferConfig = {.num_own_send_threads = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMCONNECT_DEFAULT_OWN_SEND_THREADS),
                                                .num_own_receive_threads = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMCONNECT_DEFAULT_OWN_RECEIVE_THREADS),
                                                .num_remote_send_threads = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMCONNECT_DEFAULT_REMOTE_SEND_THREADS),
                                                .num_remote_receive_threads = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMCONNECT_DEFAULT_REMOTE_RECEIVE_THREADS),
                                                .num_own_receive = numOwnReceive,
                                                .size_own_receive = sizeOwnReceive,
                                                .num_remote_receive = numRemoteReceive,
                                                .size_remote_receive = sizeRemoteReceive,
                                                .num_own_send = numRemoteReceive,
                                                .size_own_send = sizeOwnSend,
                                                .num_remote_send = numOwnReceive,
                                                .size_remote_send = sizeRemoteSend,
                                                .meta_info_size = ConnectionManager::getInstance().configuration->get<uint8_t>(MEMCONNECT_DEFAULT_META_INFO_SIZE)};

            NetworkConfig config = {.deviceName = ConnectionManager::getInstance().configuration->getAsString(MEMCONNECT_DEFAULT_IB_DEVICE_NAME),
                                    .serverName = "0",
                                    .tcpPort = ConnectionManager::getInstance().configuration->get<uint32_t>(MEMCONNECT_DEFAULT_TCP_PORT),
                                    .clientMode = false,
                                    .infiniBandPort = ConnectionManager::getInstance().configuration->get<int32_t>(MEMCONNECT_DEFAULT_IB_PORT),
                                    .gidIndex = ConnectionManager::getInstance().configuration->get<int32_t>(MEMCONNECT_DEFAULT_IB_GLOBAL_INDEX)};

            for (std::string ip : {"172.16.6.59", "172.16.7.33"}) {
                config.serverName = ip;
                size_t connectionId = ConnectionManager::getInstance().registerConnection(config, bufferConfig, ConnectionType(ConnectionManager::getInstance().configuration->get<uint8_t>(MEMCONNECT_DEFAULT_CONNECTION_TYPE)));

                if (connectionId != 0) {
                    LOG_SUCCESS("Connection " << connectionId << " opened for config: " << std::endl);
                    ConnectionManager::getInstance().getConnectionById(connectionId)->printConnectionInfo();
                } else {
                    LOG_ERROR("Something went wrong! The connection could not be opened for config: " << std::endl);
                }
            }
        }).detach();
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