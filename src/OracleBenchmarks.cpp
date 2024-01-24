#include "OracleBenchmarks.hpp"

#include <omp.h>

#include <atomic>
#include <future>
#include <string>
#include <unordered_map>

#include "Column.hpp"
#include "ConnectionManager.hpp"
#include "TaskManager.hpp"

OracleBenchmarks::OracleBenchmarks() {
    auto benchmarkOracle = [this]() -> void {
        OracleBenchmarks::getInstance().executeAllOracleBenchmarks();
    };

    TaskManager::getInstance().registerTask(std::make_shared<Task>("oracleBenchmarksAll", "[DataCatalog] Execute All Oracle Benchmarks", benchmarkOracle));

    CallbackFunction cb_generateBenchmarkData = [this](const size_t conId, const ReceiveBuffer* rcv_buffer, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) -> void {
        uint64_t* data = rcv_buffer->getPayloadBasePtr<uint64_t>();

        uint64_t numEntries = data[0];

        LOG_DEBUG1("Creating Columns" << std::endl;)
#pragma omp parallel for schedule(static, 1) num_threads(8)
        for (size_t i = 1; i <= numEntries; ++i) {
            uint64_t entry = data[i];

            std::string name = "col_" + std::to_string(entry) + "_0";

            DataCatalog::getInstance().generate(name, col_data_t::gen_bigint, data[numEntries + 1], 0);
        }
        LOG_DEBUG1("Done Creating Columns!" << std::endl;)

        reset_buffer();

        DataCatalog::getInstance().print_all();

        ConnectionManager::getInstance().getConnectionById(conId)->sendOpcode(static_cast<uint8_t>(catalog_communication_code::ack_generate_oracle_benchmark_data));
    };

    CallbackFunction cb_ackGenerateBenchmarkData = [this](const size_t, const ReceiveBuffer*, const std::_Bind<ResetFunction(uint64_t)> reset_buffer) -> void {
        reset_buffer();
        std::lock_guard<std::mutex> lk(dataGenerationLock);
        dataGenerationDone = true;
        dataGenerationCV.notify_all();
    };

    DataCatalog::getInstance().registerCallback(static_cast<uint8_t>(catalog_communication_code::generate_oracle_benchmark_data), cb_generateBenchmarkData);
    DataCatalog::getInstance().registerCallback(static_cast<uint8_t>(catalog_communication_code::ack_generate_oracle_benchmark_data), cb_ackGenerateBenchmarkData);
}

OracleBenchmarks::~OracleBenchmarks() {}

double calculateMiBPerS(const size_t size_in_bytes, const size_t time_in_ns) {
    return (static_cast<double>(size_in_bytes) / 1024 / 1024)  // B-to-MiB
           /
           (static_cast<double>(time_in_ns) / 10e8);  // ns-to-s
}

inline void waitColDataReady(col_t* _col, char* _data, const size_t _bytes) {
    std::unique_lock<std::mutex> lk(_col->iteratorLock);
    if (!(_data + _bytes <= reinterpret_cast<char*>(_col->current_end))) {
        _col->iterator_data_available.wait(lk, [_col, _data, _bytes] { return reinterpret_cast<char*>(_data) + _bytes <= reinterpret_cast<char*>(_col->current_end); });
    }
}

void OracleBenchmarks::generateBenchmarkData(const uint64_t numberConnections, const uint64_t distinctLocalColumns, const uint64_t localColumnElements, const uint64_t remoteColumnElements) {
    LOG_DEBUG1("Generating Benchmark Data" << std::endl;)

    std::vector<std::vector<size_t>> cons(numberConnections);

    for (size_t i = 0; i < distinctLocalColumns; ++i) {
        cons[i % numberConnections].push_back(i);
    }

    for (size_t i = 1; i <= numberConnections; ++i) {
        uint64_t numEntries = cons[i - 1].size();
        std::unique_lock<std::mutex> lk(dataGenerationLock);
        dataGenerationDone = false;
        char* remInfos = reinterpret_cast<char*>(std::malloc(sizeof(uint64_t) + (sizeof(uint64_t) * numEntries) + sizeof(uint64_t)));
        char* tmp = remInfos;
        std::memcpy(reinterpret_cast<void*>(tmp), &numEntries, sizeof(uint64_t));
        tmp += sizeof(uint64_t);
        for (size_t entr : cons[i - 1]) {
            std::memcpy(reinterpret_cast<void*>(tmp), &entr, sizeof(uint64_t));
            tmp += sizeof(uint64_t);
        }
        std::memcpy(reinterpret_cast<void*>(tmp), &remoteColumnElements, sizeof(uint64_t));

        ConnectionManager::getInstance().sendData(i, remInfos, sizeof(uint64_t) + (sizeof(uint64_t) * numEntries) + sizeof(uint64_t), nullptr, 0, static_cast<uint8_t>(catalog_communication_code::generate_oracle_benchmark_data));
    }

    LOG_DEBUG1("Creating Columns" << std::endl;)
#pragma omp parallel for schedule(static, 2) num_threads(8)
    for (size_t i = 0; i < distinctLocalColumns; ++i) {
        std::string name = "col_" + std::to_string(i);

        DataCatalog::getInstance().generate(name, col_data_t::gen_bigint, localColumnElements, 0);
    }
    LOG_DEBUG1("Done Creating Columns!" << std::endl;)

    std::unique_lock<std::mutex> lk(dataGenerationLock);
    if (!dataGenerationDone) {
        dataGenerationCV.wait(lk, [this] { return dataGenerationDone; });
    }

    DataCatalog::getInstance().print_all();
}

void hashJoinKernel(std::shared_future<void>* ready_future, const size_t tid, const size_t local_worker_count, std::atomic<size_t>* ready_workers, std::atomic<size_t>* complete_workers, std::condition_variable* done_cv, std::mutex* done_cv_lock, bool* all_done, uint64_t* result_ptr, long* out_time, std::pair<std::string, std::string> idents) {
    col_t* column_0;
    col_t* column_1;
    size_t joinResult = 0;
    size_t chunkSize = DataCatalog::getInstance().dataCatalog_chunkMaxSize;

    ++(*ready_workers);
    ready_future->wait();
    auto start = std::chrono::high_resolution_clock::now();

    column_1 = DataCatalog::getInstance().find_remote(idents.second);
    column_1->request_data(false, true);

    column_0 = DataCatalog::getInstance().find_local(idents.first);

    size_t columnSize0 = column_0->size;
    size_t columnSize1 = column_1->size;

    uint64_t* data_0 = reinterpret_cast<uint64_t*>(column_0->data);
    uint64_t* data_1 = reinterpret_cast<uint64_t*>(column_1->data);

    std::unordered_map<uint64_t, std::vector<size_t>> hashMap;
    size_t currentBlockSize = chunkSize / sizeof(uint64_t);
    size_t baseOffset = 0;

    while (baseOffset < columnSize1) {
        const size_t elem_diff = columnSize1 - baseOffset;
        if (elem_diff < currentBlockSize) {
            currentBlockSize = elem_diff;
        }

        waitColDataReady(column_1, reinterpret_cast<char*>(data_1), currentBlockSize * sizeof(uint64_t));

        for (size_t i = 0; i < currentBlockSize; ++i) {
            hashMap[data_1[i]].push_back(i + baseOffset);
        }

        baseOffset += currentBlockSize;
        data_1 += currentBlockSize;
    }

    for (size_t i = 0; i < columnSize0; ++i) {
        auto it = hashMap.find(data_0[i]);
        if (it != hashMap.end()) {
            for (const auto& _ : it->second) {
                ++joinResult;
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    out_time[tid] = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    result_ptr[tid] = joinResult;
    if (++(*complete_workers) == local_worker_count) {
        *all_done = true;
        std::unique_lock<std::mutex> lk(*done_cv_lock);
        done_cv->notify_all();
    }
}

typedef std::function<void(std::shared_future<void>* ready_future, const size_t tid, const size_t local_worker_count, std::atomic<size_t>* ready_workers, std::atomic<size_t>* complete_workers, std::condition_variable* done_cv, std::mutex* done_cv_lock, bool* all_done, uint64_t* result_ptr, long* out_time, std::pair<std::string, std::string> idents)> BenchKernel;

void spawnThreads(BenchKernel kernel, std::vector<size_t>& pin_list, std::vector<std::thread>& pool, std::shared_future<void>* ready_future, const size_t local_worker_count, std::atomic<size_t>* ready_workers, std::atomic<size_t>* complete_workers, std::condition_variable* done_cv, std::mutex* done_cv_lock, bool* all_done, uint64_t* result_ptr, long* out_time, std::string prefix) {
    cpu_set_t cpuset;
    for (auto pin : pin_list) {
        std::string name = prefix + std::to_string(pin);
        std::string small_col = name + "_0";

        pool.emplace_back(std::thread(kernel, ready_future, pin, local_worker_count, ready_workers, complete_workers, done_cv, done_cv_lock, all_done, result_ptr, out_time, std::make_pair(name, small_col)));

        CPU_ZERO(&cpuset);
        CPU_SET(pin, &cpuset);
        int rc = pthread_setaffinity_np(pool.back().native_handle(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            LOG_ERROR("Error calling pthread_setaffinity_np in copy_pool assignment: " << rc << std::endl;)
            exit(-10);
        }
    }
}

void OracleBenchmarks::execHashJoinBenchmark() {
    size_t numberOfConnections = ConnectionManager::getInstance().getNumberOfConnections();

    const uint64_t localColumnElements = 160000000;
    const uint64_t remoteColumnElements = 0.1 * localColumnElements;
    const uint64_t maxRuns = 10;

    for (size_t connections = 1; connections <= numberOfConnections; ++connections) {
        LOG_INFO("Executing Oracle Benchmarks with " << connections << " connections" << std::endl;)

        auto in_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::stringstream logNameStream;
        logNameStream << "../logs/bench/" << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M-%S_") << "Oracle_" << connections << "_" << std::to_string(localColumnElements * sizeof(uint64_t)) << "_" << std::to_string(remoteColumnElements * sizeof(uint64_t)) << ".csv";
        std::string logName = logNameStream.str();

        LOG_INFO("[Task] Set name: " << logName << std::endl;)

        std::ofstream out;
        out.open(logName, std::ios_base::app);
        out << std::fixed << std::setprecision(7);

        out << "total_number_joins,time[ns],bwdh,result" << std::endl;

        DataCatalog::getInstance().clear(true);
        generateBenchmarkData(connections, NUMBER_OF_JOINS, localColumnElements, remoteColumnElements);

        for (size_t run = 0; run < maxRuns; ++run) {
            DataCatalog::getInstance().eraseAllRemoteColumns();
            DataCatalog::getInstance().fetchRemoteInfo();

            std::atomic<size_t> ready_workers = {0};
            std::atomic<size_t> complete_workers = {0};
            std::condition_variable done_cv;
            std::mutex done_cv_lock;
            bool all_done = false;

            std::vector<std::thread> worker_pool;

            std::promise<void> p;
            std::shared_future<void> ready_future(p.get_future());

            const size_t res_ptr_size = sizeof(uint64_t) * NUMBER_OF_JOINS;
            const size_t time_out_ptr_size = sizeof(long) * NUMBER_OF_JOINS;
            uint64_t* result_out_ptr = reinterpret_cast<uint64_t*>(numa_alloc_onnode(res_ptr_size, 0));
            long* time_out_ptr = reinterpret_cast<long*>(numa_alloc_onnode(time_out_ptr_size, 0));

            std::vector<size_t> local_pin_list;

            for (size_t i = 0; i < NUMBER_OF_JOINS; ++i) {
                local_pin_list.emplace_back(global_pins[i]);
            }

            spawnThreads(hashJoinKernel, local_pin_list, worker_pool, &ready_future, NUMBER_OF_JOINS, &ready_workers, &complete_workers, &done_cv, &done_cv_lock, &all_done, result_out_ptr, time_out_ptr, "col_");

            while (ready_workers != NUMBER_OF_JOINS) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            auto start = std::chrono::high_resolution_clock::now();
            p.set_value();

            {
                std::unique_lock<std::mutex> lk(done_cv_lock);
                done_cv.wait(lk, [&all_done] { return all_done; });
            }

            auto end = std::chrono::high_resolution_clock::now();
            const size_t duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            const double wallclock_bwdh = calculateMiBPerS((localColumnElements + remoteColumnElements) * sizeof(uint64_t) * NUMBER_OF_JOINS, duration);

            std::for_each(worker_pool.begin(), worker_pool.end(), [](std::thread& t) { t.join(); });
            worker_pool.clear();

            uint64_t result = 0;

            for (size_t i = 0; i < NUMBER_OF_JOINS; ++i) {
                result += result_out_ptr[i];
            }

            numa_free(result_out_ptr, res_ptr_size);
            numa_free(time_out_ptr, time_out_ptr_size);

            LOG_SUCCESS(std::fixed << std::setprecision(7) << "Wallclock bandwidth: " << wallclock_bwdh << "\tResult:" << result << std::endl;)
            // LOG_SUCCESS(std::fixed << std::setprecision(7) << "Waiting time total: " << std::chrono::duration_cast<std::chrono::nanoseconds>(waitingTime).count() << "\tWaiting per thread " << std::chrono::duration_cast<std::chrono::nanoseconds>(waitingTime).count() / local_buffer_cnt << std::endl;)
            out << std::to_string(NUMBER_OF_JOINS) << "," << std::to_string(duration) << "," << wallclock_bwdh << "," << std::to_string(result) << std::endl
                << std::flush;
        }
        out.close();
    }

    DataCatalog::getInstance().clear(true);

    LOG_NOFORMAT(std::endl;)
    LOG_INFO("Oracle Benchmarks (Hash Join) ended." << std::endl;)
}

void OracleBenchmarks::executeAllOracleBenchmarks() {
    execHashJoinBenchmark();
}