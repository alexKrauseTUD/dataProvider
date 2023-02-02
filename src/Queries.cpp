// #include <Column.h>
// #include <DataCatalog.h>
// #include <Queries.h>

// #include <future>

// #include "Benchmarks.hpp"
// #include "Operators.hpp"

// std::chrono::duration<double> waitingTime = std::chrono::duration<double>::zero();
// std::chrono::duration<double> workingTime = std::chrono::duration<double>::zero();

// inline void reset_timer() {
//     waitingTime = std::chrono::duration<double>::zero();
//     workingTime = std::chrono::duration<double>::zero();
// }

// inline void wait_col_data_ready(col_t* _col, char* _data) {
//     auto s_ts = std::chrono::high_resolution_clock::now();
//     std::unique_lock<std::mutex> lk(_col->iteratorLock);
//     if (!(_data < static_cast<char*>(_col->current_end))) {
//         _col->iterator_data_available.wait(lk, [_col, _data] { return reinterpret_cast<uint64_t*>(_data) < static_cast<uint64_t*>(_col->current_end); });
//     }
//     waitingTime += (std::chrono::high_resolution_clock::now() - s_ts);
// };

// template <bool remote, bool chunked, bool paxed, bool prefetching>
// inline void fetch_data(col_t* column, uint64_t* data, const bool reload) {
//     if (remote) {
//         if (reload) {
//             if (!prefetching && !paxed) {
//                 column->request_data(!chunked);
//             }
//         }
//         wait_col_data_ready(column, reinterpret_cast<char*>(data));
//         if (reload) {
//             if (prefetching && chunked && !paxed) {
//                 column->request_data(!chunked);
//             }
//         }
//     }
// }

// template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false, bool timings = false>
// inline std::vector<size_t> less_than(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
//     auto data = reinterpret_cast<uint64_t*>(column->data) + offset;

//     std::vector<std::size_t> out_vec;

//     fetch_data<remote, chunked, paxed, prefetching>(column, data, reload);

//     if (timings) {
//         auto s_ts = std::chrono::high_resolution_clock::now();
//         out_vec = Operators::less_than<isFirst>(data, predicate, blockSize, in_pos);
//         workingTime += (std::chrono::high_resolution_clock::now() - s_ts);
//     } else {
//         out_vec = Operators::less_than<isFirst>(data, predicate, blockSize, in_pos);
//     }

//     return out_vec;
// };

// template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false, bool timings = false>
// inline std::vector<size_t> less_equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
//     auto data = reinterpret_cast<uint64_t*>(column->data) + offset;

//     std::vector<std::size_t> out_vec;

//     fetch_data<remote, chunked, paxed, prefetching>(column, data, reload);

//     if (timings) {
//         auto s_ts = std::chrono::high_resolution_clock::now();
//         out_vec = Operators::less_equal<isFirst>(data, predicate, blockSize, in_pos);
//         workingTime += (std::chrono::high_resolution_clock::now() - s_ts);
//     } else {
//         out_vec = Operators::less_equal<isFirst>(data, predicate, blockSize, in_pos);
//     }

//     return out_vec;
// };

// template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false, bool timings = false>
// inline std::vector<size_t> greater_than(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
//     auto data = reinterpret_cast<uint64_t*>(column->data) + offset;

//     std::vector<std::size_t> out_vec;

//     fetch_data<remote, chunked, paxed, prefetching>(column, data, reload);

//     if (timings) {
//         auto s_ts = std::chrono::high_resolution_clock::now();
//         out_vec = Operators::greater_than<isFirst>(data, predicate, blockSize, in_pos);
//         workingTime += (std::chrono::high_resolution_clock::now() - s_ts);
//     } else {
//         out_vec = Operators::greater_than<isFirst>(data, predicate, blockSize, in_pos);
//     }

//     return out_vec;
// };

// template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false, bool timings = false>
// inline std::vector<size_t> greater_equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
//     auto data = reinterpret_cast<uint64_t*>(column->data) + offset;

//     std::vector<std::size_t> out_vec;

//     fetch_data<remote, chunked, paxed, prefetching>(column, data, reload);

//     if (timings) {
//         auto s_ts = std::chrono::high_resolution_clock::now();
//         out_vec = Operators::greater_equal<isFirst>(data, predicate, blockSize, in_pos);
//         workingTime += (std::chrono::high_resolution_clock::now() - s_ts);
//     } else {
//         out_vec = Operators::greater_equal<isFirst>(data, predicate, blockSize, in_pos);
//     }

//     return out_vec;
// };

// template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false, bool timings = false>
// inline std::vector<size_t> equal(col_t* column, const uint64_t predicate, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
//     auto data = reinterpret_cast<uint64_t*>(column->data) + offset;

//     std::vector<std::size_t> out_vec;

//     fetch_data<remote, chunked, paxed, prefetching>(column, data, reload);

//     if (timings) {
//         auto s_ts = std::chrono::high_resolution_clock::now();
//         out_vec = Operators::equal<isFirst>(data, predicate, blockSize, in_pos);
//         workingTime += (std::chrono::high_resolution_clock::now() - s_ts);
//     } else {
//         out_vec = Operators::equal<isFirst>(data, predicate, blockSize, in_pos);
//     }

//     return out_vec;
// };

// template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false, bool timings = false>
// inline std::vector<size_t> between_incl(col_t* column, const uint64_t predicate_1, const uint64_t predicate_2, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
//     auto data = reinterpret_cast<uint64_t*>(column->data) + offset;

//     std::vector<std::size_t> out_vec;

//     fetch_data<remote, chunked, paxed, prefetching>(column, data, reload);

//     if (timings) {
//         auto s_ts = std::chrono::high_resolution_clock::now();
//         out_vec = Operators::between_incl<isFirst>(data, predicate_1, predicate_2, blockSize, in_pos);
//         workingTime += (std::chrono::high_resolution_clock::now() - s_ts);
//     } else {
//         out_vec = Operators::between_incl<isFirst>(data, predicate_1, predicate_2, blockSize, in_pos);
//     }

//     return out_vec;
// };

// template <bool remote, bool chunked, bool paxed, bool prefetching, bool isFirst = false, bool timings = false>
// inline std::vector<size_t> between_excl(col_t* column, const uint64_t predicate_1, const uint64_t predicate_2, const uint64_t offset, const size_t blockSize, const std::vector<size_t> in_pos, const bool reload) {
//     auto data = reinterpret_cast<uint64_t*>(column->data) + offset;

//     std::vector<std::size_t> out_vec;

//     fetch_data<remote, chunked, paxed, prefetching>(column, data, reload);

//     if (timings) {
//         auto s_ts = std::chrono::high_resolution_clock::now();
//         out_vec = Operators::between_excl<isFirst>(data, predicate_1, predicate_2, blockSize, in_pos);
//         workingTime += (std::chrono::high_resolution_clock::now() - s_ts);
//     } else {
//         out_vec = Operators::between_excl<isFirst>(data, predicate_1, predicate_2, blockSize, in_pos);
//     }

//     return out_vec;
// };

// template <bool remote, bool chunked, bool paxed, bool prefetching>
// uint64_t pipe_1(const uint64_t predicate, const std::vector<std::string> idents) {
//     col_t* col_0;
//     col_t* col_1;
//     col_t* col_2;

//     if (idents.size() != 3) {
//         LOG_ERROR("The size of 'idents' was not equal to 3" << std::endl;)
//         return 0;
//     }

//     std::chrono::time_point<std::chrono::high_resolution_clock> s_ts;

//     if (remote) {
//         col_0 = DataCatalog::getInstance().find_remote(idents[0]);
//         if (prefetching && !paxed) col_0->request_data(!chunked);
//         col_1 = DataCatalog::getInstance().find_remote(idents[1]);
//         if (prefetching && !paxed) col_1->request_data(!chunked);
//         col_2 = DataCatalog::getInstance().find_remote(idents[2]);
//         if (prefetching && !paxed) col_2->request_data(!chunked);

//         // if (prefetching && paxed) DataCatalog::getInstance().fetchPseudoPax(1, idents);
//     } else {
//         col_0 = DataCatalog::getInstance().find_local(idents[0]);
//         col_1 = DataCatalog::getInstance().find_local(idents[1]);
//         col_2 = DataCatalog::getInstance().find_local(idents[2]);
//     }

//     size_t columnSize = col_0->size;

//     size_t max_elems_per_chunk = 0;
//     size_t currentBlockSize = max_elems_per_chunk;
//     // if (paxed) {
//     //     size_t total_id_len = 0;
//     //     for (auto& id : idents) {
//     //         total_id_len += id.size();
//     //     }

//     //     const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
//     //     const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(1)->maxBytesInPayload(appMetaSize);

//     //     max_elems_per_chunk = ((maximumPayloadSize / idents.size()) / (sizeof(uint64_t) * 4)) * 4;
//     //     currentBlockSize = max_elems_per_chunk;
//     // } else
//     if (!(remote && (chunked || paxed))) {
//         max_elems_per_chunk = columnSize;
//         currentBlockSize = Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
//     } else {
//         max_elems_per_chunk = DataCatalog::getInstance().dataCatalog_chunkMaxSize / sizeof(uint64_t);
//         if (max_elems_per_chunk <= Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t)) {
//             currentBlockSize = max_elems_per_chunk;
//         } else {
//             currentBlockSize = Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
//         }
//     }

//     uint64_t sum = 0;
//     size_t baseOffset = 0;
//     size_t currentChunkElementsProcessed = 0;

//     auto data_col_2 = reinterpret_cast<uint64_t*>(col_2->data);
//     auto data_col_0 = reinterpret_cast<uint64_t*>(col_0->data);

//     while (baseOffset < columnSize) {
//         // if (remote && paxed) {
//         //     if (!prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
//         //     wait_col_data_ready(col_2, reinterpret_cast<char*>(data_col_2));
//         //     if (prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
//         // }

//         const size_t elem_diff = columnSize - baseOffset;
//         if (elem_diff < currentBlockSize) {
//             currentBlockSize = elem_diff;
//         }

//         auto le_idx = greater_than<remote, chunked, paxed, prefetching>(col_2, 5, baseOffset, currentBlockSize,
//                                                                         less_than<remote, chunked, paxed, prefetching>(col_1, 25, baseOffset, currentBlockSize,
//                                                                                                                        between_incl<remote, chunked, paxed, prefetching, true>(col_0, 10, 30, baseOffset, currentBlockSize, {}, currentChunkElementsProcessed == 0), currentChunkElementsProcessed == 0),
//                                                                         currentChunkElementsProcessed == 0);

//         s_ts = std::chrono::high_resolution_clock::now();
//         for (auto idx : le_idx) {
//             sum += (data_col_0[idx] * data_col_2[idx]);
//             // ++sum;
//         }
//         workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

//         baseOffset += currentBlockSize;
//         data_col_0 += currentBlockSize;
//         data_col_2 += currentBlockSize;
//         currentChunkElementsProcessed = baseOffset % max_elems_per_chunk;
//     }

//     return sum;
// }

// template <bool remote, bool chunked, bool paxed, bool prefetching>
// uint64_t pipe_2(const uint64_t predicate, const std::vector<std::string> idents) {
//     col_t* col_0;
//     col_t* col_1;
//     col_t* col_2;

//     if (idents.size() != 3) {
//         LOG_ERROR("The size of 'idents' was not equal to 3" << std::endl;)
//         return 0;
//     }

//     std::chrono::time_point<std::chrono::high_resolution_clock> s_ts;

//     if (remote) {
//         col_1 = DataCatalog::getInstance().find_remote(idents[1]);
//         if (prefetching && !paxed) col_1->request_data(!chunked);
//         col_0 = DataCatalog::getInstance().find_remote(idents[0]);
//         if (prefetching && !paxed) col_0->request_data(!chunked);
//         col_2 = DataCatalog::getInstance().find_remote(idents[2]);
//         if (prefetching && !paxed) col_2->request_data(!chunked);

//         // if (prefetching && paxed) DataCatalog::getInstance().fetchPseudoPax(1, idents);
//     } else {
//         col_0 = DataCatalog::getInstance().find_local(idents[0]);
//         col_1 = DataCatalog::getInstance().find_local(idents[1]);
//         col_2 = DataCatalog::getInstance().find_local(idents[2]);
//     }

//     size_t columnSize = col_1->size;

//     size_t max_elems_per_chunk = 0;
//     size_t currentBlockSize = max_elems_per_chunk;
//     // if (paxed) {
//     //     size_t total_id_len = 0;
//     //     for (auto& id : idents) {
//     //         total_id_len += id.size();
//     //     }

//     //     const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
//     //     const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(1)->maxBytesInPayload(appMetaSize);

//     //     max_elems_per_chunk = ((maximumPayloadSize / idents.size()) / (sizeof(uint64_t) * 4)) * 4;
//     //     currentBlockSize = max_elems_per_chunk;
//     // } else
//     if (!(remote && (chunked || paxed))) {
//         max_elems_per_chunk = columnSize;
//         currentBlockSize = Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
//     } else {
//         max_elems_per_chunk = DataCatalog::getInstance().dataCatalog_chunkMaxSize / sizeof(uint64_t);
//         if (max_elems_per_chunk <= Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t)) {
//             currentBlockSize = max_elems_per_chunk;
//         } else {
//             currentBlockSize = Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
//         }
//     }

//     uint64_t sum = 0;
//     size_t baseOffset = 0;
//     size_t currentChunkElementsProcessed = 0;

//     auto data_col_2 = reinterpret_cast<uint64_t*>(col_2->data);
//     auto data_col_0 = reinterpret_cast<uint64_t*>(col_0->data);

//     while (baseOffset < columnSize) {
//         // if (remote && paxed) {
//         //     if (!prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
//         //     wait_col_data_ready(col_2, reinterpret_cast<char*>(data_col_2));
//         //     if (prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
//         // }

//         const size_t elem_diff = columnSize - baseOffset;
//         if (elem_diff < currentBlockSize) {
//             currentBlockSize = elem_diff;
//         }

//         auto le_idx = less_than<remote, chunked, paxed, prefetching, true>(col_1, predicate, baseOffset, currentBlockSize, {}, currentChunkElementsProcessed == 0);

//         if (remote && !paxed) {
//             if (currentChunkElementsProcessed == 0) {
//                 if (!prefetching && chunked) {
//                     col_2->request_data(!chunked);
//                     col_0->request_data(!chunked);
//                 }
//                 wait_col_data_ready(col_2, reinterpret_cast<char*>(data_col_2));
//                 wait_col_data_ready(col_0, reinterpret_cast<char*>(data_col_0));
//                 if (prefetching && chunked) {
//                     col_2->request_data(!chunked);
//                     col_0->request_data(!chunked);
//                 }
//             }
//         }

//         s_ts = std::chrono::high_resolution_clock::now();
//         for (auto idx : le_idx) {
//             sum += (data_col_0[idx] * data_col_2[idx]);
//             // ++sum;
//         }
//         workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

//         baseOffset += currentBlockSize;
//         data_col_0 += currentBlockSize;
//         data_col_2 += currentBlockSize;
//         currentChunkElementsProcessed = baseOffset % max_elems_per_chunk;
//     }

//     return sum;
// }

// template <bool remote, bool chunked, bool paxed, bool prefetching>
// uint64_t pipe_3(const uint64_t predicate, const std::vector<std::string> idents) {
//     col_t* column_0;
//     col_t* column_1;
//     col_t* column_2;
//     col_t* column_3;

//     if (idents.size() != 4) {
//         LOG_ERROR("The size of 'idents' was not equal to 4" << std::endl;)
//         return 0;
//     }

//     std::chrono::time_point<std::chrono::high_resolution_clock> s_ts;

//     if (remote) {
//         column_0 = DataCatalog::getInstance().find_remote(idents[0]);
//         if (prefetching && !paxed) column_0->request_data(!chunked);
//         column_1 = DataCatalog::getInstance().find_remote(idents[1]);
//         if (prefetching && !paxed) column_1->request_data(!chunked);
//         column_2 = DataCatalog::getInstance().find_remote(idents[2]);
//         if (prefetching && !paxed) column_2->request_data(!chunked);
//         column_3 = DataCatalog::getInstance().find_remote(idents[3]);
//         if (prefetching && !paxed) column_3->request_data(!chunked);

//         // if (prefetching && paxed) DataCatalog::getInstance().fetchPseudoPax(1, idents);
//     } else {
//         column_0 = DataCatalog::getInstance().find_local(idents[0]);
//         column_1 = DataCatalog::getInstance().find_local(idents[1]);
//         column_2 = DataCatalog::getInstance().find_local(idents[2]);
//         column_3 = DataCatalog::getInstance().find_local(idents[3]);
//     }

//     size_t columnSize = column_0->size;

//     size_t max_elems_per_chunk = 0;
//     size_t currentBlockSize = max_elems_per_chunk;
//     // if (paxed) {
//     //     size_t total_id_len = 0;
//     //     for (auto& id : idents) {
//     //         total_id_len += id.size();
//     //     }

//     //     const size_t appMetaSize = 3 * sizeof(size_t) + (sizeof(size_t) * idents.size()) + total_id_len;
//     //     const size_t maximumPayloadSize = ConnectionManager::getInstance().getConnectionById(1)->maxBytesInPayload(appMetaSize);

//     //     max_elems_per_chunk = ((maximumPayloadSize / idents.size()) / (sizeof(uint64_t) * 4)) * 4;
//     //     currentBlockSize = max_elems_per_chunk;
//     // } else
//     if (!(remote && (chunked || paxed))) {
//         max_elems_per_chunk = columnSize;
//         currentBlockSize = Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
//     } else {
//         max_elems_per_chunk = DataCatalog::getInstance().dataCatalog_chunkMaxSize / sizeof(uint64_t);
//         if (max_elems_per_chunk <= Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t)) {
//             currentBlockSize = max_elems_per_chunk;
//         } else {
//             currentBlockSize = Benchmarks::OPTIMAL_BLOCK_SIZE / sizeof(uint64_t);
//         }
//     }

//     uint64_t sum = 0;
//     size_t baseOffset = 0;
//     size_t currentChunkElementsProcessed = 0;

//     auto data_2 = reinterpret_cast<uint64_t*>(column_2->data);
//     auto data_3 = reinterpret_cast<uint64_t*>(column_3->data);

//     while (baseOffset < columnSize) {
//         // if (remote && paxed) {
//         //     if (!prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
//         //     wait_col_data_ready(column_3, reinterpret_cast<char*>(data_3));
//         //     if (prefetching) DataCatalog::getInstance().fetchPseudoPax(1, idents);
//         // }

//         const size_t elem_diff = columnSize - baseOffset;
//         if (elem_diff < currentBlockSize) {
//             currentBlockSize = elem_diff;
//         }

//         auto le_idx = equal<remote, chunked, paxed, prefetching>(column_3, 16, baseOffset, currentBlockSize,
//                                                                  greater_than<remote, chunked, paxed, prefetching>(column_2, 5, baseOffset, currentBlockSize,
//                                                                                                                    less_than<remote, chunked, paxed, prefetching>(column_1, 25, baseOffset, currentBlockSize,
//                                                                                                                                                                   between_incl<remote, chunked, paxed, prefetching, true>(column_0, 10, 30, baseOffset, currentBlockSize, {}, currentChunkElementsProcessed == 0), currentChunkElementsProcessed == 0),
//                                                                                                                    currentChunkElementsProcessed == 0),
//                                                                  currentChunkElementsProcessed == 0);

//         s_ts = std::chrono::high_resolution_clock::now();
//         for (auto idx : le_idx) {
//             sum += (data_2[idx] * data_3[idx]);
//             // ++sum;
//         }
//         workingTime += (std::chrono::high_resolution_clock::now() - s_ts);

//         baseOffset += currentBlockSize;
//         data_2 += currentBlockSize;
//         data_3 += currentBlockSize;
//         currentChunkElementsProcessed = baseOffset % max_elems_per_chunk;
//     }

//     return sum;
// }

// // template <typename Fn>
// // void doBenchmarkRemotes(Fn&& f1, Fn&& f2, Fn&& f3, Fn&& f4, Fn&& f5, std::ofstream& out, const uint64_t predicate) {
// //     uint64_t sum = 0;
// //     std::chrono::time_point<std::chrono::high_resolution_clock> s_ts;
// //     std::chrono::time_point<std::chrono::high_resolution_clock> e_ts;
// //     std::chrono::duration<double> secs;

// //     for (size_t i = 0; i < 5; ++i) {
// //         reset_timer();
// //         DataCatalog::getInstance().fetchRemoteInfo();
// //         s_ts = std::chrono::high_resolution_clock::now();
// //         sum = f1(predicate);
// //         e_ts = std::chrono::high_resolution_clock::now();

// //         secs = e_ts - s_ts;

// //         out << "Remote\tFull\tPipe\t" << Benchmarks::OPTIMAL_BLOCK_SIZE << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl
// //             << std::flush;
// //         std::cout << "Remote\tFull\tPipe\t" << Benchmarks::OPTIMAL_BLOCK_SIZE << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl;

// //         DataCatalog::getInstance().eraseAllRemoteColumns();

// //         for (uint64_t chunkSize = 1ull << 18; chunkSize <= 1ull << 27; chunkSize <<= 1) {
// //             DataCatalog::getInstance().reconfigureChunkSize(chunkSize, chunkSize);

// //             reset_timer();
// //             DataCatalog::getInstance().fetchRemoteInfo();
// //             s_ts = std::chrono::high_resolution_clock::now();
// //             sum = f2(predicate);
// //             e_ts = std::chrono::high_resolution_clock::now();

// //             secs = e_ts - s_ts;

// //             out << "Remote\tChunked\tOper\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl
// //                 << std::flush;
// //             std::cout << "Remote\tChunked\tOper\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl;

// //             DataCatalog::getInstance().eraseAllRemoteColumns();

// //             reset_timer();
// //             DataCatalog::getInstance().fetchRemoteInfo();
// //             s_ts = std::chrono::high_resolution_clock::now();
// //             sum = f3(predicate);
// //             e_ts = std::chrono::high_resolution_clock::now();

// //             secs = e_ts - s_ts;

// //             out << "Remote\tChunked\tPipe\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl
// //                 << std::flush;
// //             std::cout << "Remote\tChunked\tPipe\t" << +DataCatalog::getInstance().dataCatalog_chunkMaxSize << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl;

// //             DataCatalog::getInstance().eraseAllRemoteColumns();
// //         }

// //         reset_timer();
// //         DataCatalog::getInstance().fetchRemoteInfo();
// //         s_ts = std::chrono::high_resolution_clock::now();
// //         sum = f4(predicate);
// //         e_ts = std::chrono::high_resolution_clock::now();

// //         secs = e_ts - s_ts;

// //         out << "Remote\tPaxed\tOper\t000000\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl
// //             << std::flush;
// //         std::cout << "Remote\tPaxed\tOper\t000000\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl;

// //         DataCatalog::getInstance().eraseAllRemoteColumns();

// //         reset_timer();
// //         DataCatalog::getInstance().fetchRemoteInfo();
// //         s_ts = std::chrono::high_resolution_clock::now();
// //         sum = f5(predicate);
// //         e_ts = std::chrono::high_resolution_clock::now();

// //         secs = e_ts - s_ts;

// //         out << "Remote\tPaxed\tPipe\t000000\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl
// //             << std::flush;
// //         std::cout << "Remote\tPaxed\tPipe\t000000\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl;

// //         DataCatalog::getInstance().eraseAllRemoteColumns();
// //     }
// // }

// // template <bool remote, bool chunked, bool paxed, bool prefetching>
// // uint64_t frontPage_1() {
// //     uint64_t sum_1 = 0;
// //     uint64_t sum_2 = 0;

// //     DataCatalog::getInstance().fetchRemoteInfo();
// //     sum_1 = pipe_1<remote, chunked, paxed, prefetching>(0);
// //     DataCatalog::getInstance().eraseAllRemoteColumns();
// //     DataCatalog::getInstance().fetchRemoteInfo();
// //     sum_2 = pipe_3<remote, chunked, paxed, prefetching>(0);

// //     return sum_1 + sum_2;
// // }

// // template <bool remote, bool chunked, bool paxed, bool prefetching>
// // uint64_t frontPage_2() {
// //     uint64_t sum_1 = 0;
// //     uint64_t sum_2 = 0;

// //     DataCatalog::getInstance().fetchRemoteInfo();
// //     sum_1 = pipe_1<remote, chunked, paxed, prefetching>(0);
// //     sum_2 = pipe_3<remote, chunked, paxed, prefetching>(0);

// //     return sum_1 + sum_2;
// // }

// // template <bool remote, bool chunked, bool paxed, bool prefetching>
// // uint64_t frontPage_3() {
// //     uint64_t sum_1 = 0;
// //     uint64_t sum_2 = 0;

// //     DataCatalog::getInstance().fetchRemoteInfo();
// // #pragma omp parallel for schedule(static, 1) num_threads(2)
// //     for (size_t i = 0; i < 2; ++i) {
// //         if (i == 0) {
// //             sum_1 = pipe_1<remote, chunked, paxed, prefetching>(0);
// //         } else if (i == 1) {
// //             sum_2 = pipe_3<remote, chunked, paxed, prefetching>(0);
// //         }
// //     }

// //     return sum_1 + sum_2;
// // }

// // void doBenchmarkFrontPage(std::ofstream& out) {
// //     uint64_t sum = 0;
// //     std::chrono::time_point<std::chrono::high_resolution_clock> s_ts;
// //     std::chrono::time_point<std::chrono::high_resolution_clock> e_ts;
// //     std::chrono::duration<double> secs;
// //     uint64_t chunkSize = 1ull << 22;

// //     DataCatalog::getInstance().reconfigureChunkSize(chunkSize, chunkSize);

// //     for (size_t i = 0; i < 5; ++i) {
// //         reset_timer();
// //         s_ts = std::chrono::high_resolution_clock::now();
// //         sum = frontPage_1<true, true, false, false>();
// //         e_ts = std::chrono::high_resolution_clock::now();

// //         secs = e_ts - s_ts;

// //         out << +chunkSize << "\t1\t" << +sum << "\t" << waitingTime.count() << "\t" << secs.count() << std::endl
// //             << std::flush;
// //         std::cout << +chunkSize << "\t1\t" << +sum << "\t" << waitingTime.count() << "\t" << secs.count() << std::endl;

// //         DataCatalog::getInstance().eraseAllRemoteColumns();
// //         reset_timer();

// //         s_ts = std::chrono::high_resolution_clock::now();
// //         sum = frontPage_2<true, true, false, false>();
// //         e_ts = std::chrono::high_resolution_clock::now();

// //         secs = e_ts - s_ts;

// //         out << +chunkSize << "\t2\t" << +sum << "\t" << waitingTime.count() << "\t" << secs.count() << std::endl
// //             << std::flush;
// //         std::cout << +chunkSize << "\t2\t" << +sum << "\t" << waitingTime.count() << "\t" << secs.count() << std::endl;

// //         DataCatalog::getInstance().eraseAllRemoteColumns();
// //         reset_timer();

// //         s_ts = std::chrono::high_resolution_clock::now();
// //         sum = frontPage_2<true, true, false, true>();
// //         e_ts = std::chrono::high_resolution_clock::now();

// //         secs = e_ts - s_ts;

// //         out << +chunkSize << "\t3\t" << +sum << "\t" << waitingTime.count() << "\t" << secs.count() << std::endl
// //             << std::flush;
// //         std::cout << +chunkSize << "\t3\t" << +sum << "\t" << waitingTime.count() << "\t" << secs.count() << std::endl;

// //         DataCatalog::getInstance().eraseAllRemoteColumns();
// //         reset_timer();

// //         s_ts = std::chrono::high_resolution_clock::now();
// //         sum = frontPage_3<true, true, false, true>();
// //         e_ts = std::chrono::high_resolution_clock::now();

// //         secs = e_ts - s_ts;

// //         out << +chunkSize << "\t4\t" << +sum << "\t" << waitingTime.count() / 2 << "\t" << secs.count() << std::endl
// //             << std::flush;
// //         std::cout << +chunkSize << "\t4\t" << +sum << "\t" << waitingTime.count() / 2 << "\t" << secs.count() << std::endl;

// //         DataCatalog::getInstance().eraseAllRemoteColumns();
// //         reset_timer();
// //     }
// // }

// // void executeRemoteBenchmarkingQueries(std::string& logName) {
// //     std::ofstream out;
// //     out.open(logName, std::ios_base::app);
// //     out << std::fixed << std::setprecision(7) << std::endl;
// //     std::cout << std::fixed << std::setprecision(7) << std::endl;
// //     const std::array predicates{1, 25, 50, 75, 100};

// //     // <bool remote, bool chunked, bool paxed, bool prefetching>

// //     doBenchmarkRemotes(pipe_1<true, false, false, true>,  // Remote Full Pipe/Oper (difference due to block-wise evaluation not significant)
// //                        pipe_1<true, true, false, false>,  // Remote Chunked Oper
// //                        pipe_1<true, true, false, true>,   // Remote Chunked Pipe
// //                        pipe_1<true, false, true, false>,  // Remote Paxed Oper
// //                        pipe_1<true, false, true, true>,   // Remote Paxed Pipe
// //                        out, 0);

// //     for (const auto predicate : predicates) {
// //         doBenchmarkRemotes(pipe_2<true, false, false, true>,  // Remote Full Pipe/Oper (difference due to block-wise evaluation not significant)
// //                            pipe_2<true, true, false, false>,  // Remote Chunked Oper
// //                            pipe_2<true, true, false, true>,   // Remote Chunked Pipe
// //                            pipe_2<true, false, true, false>,  // Remote Paxed Oper
// //                            pipe_2<true, false, true, true>,   // Remote Paxed Pipe
// //                            out, predicate);
// //     }

// //     out.close();
// // }

// // void executeFrontPageBenchmarkingQueries(std::string& logName) {
// //     std::ofstream out;
// //     out.open(logName, std::ios_base::app);
// //     out << std::fixed << std::setprecision(7) << std::endl;
// //     std::cout << std::fixed << std::setprecision(7) << std::endl;

// //     // <bool remote, bool chunked, bool paxed, bool prefetching>

// //     doBenchmarkFrontPage(out);

// //     out.close();
// // }

// // void executeLocalBenchmarkingQueries(std::string& logName, std::string locality) {
// //     std::ofstream out;
// //     out.open(logName, std::ios_base::app);
// //     out << std::fixed << std::setprecision(7) << std::endl;
// //     std::cout << std::fixed << std::setprecision(7) << std::endl;
// //     const std::array predicates{0, 1, 25, 50, 75, 100};
// //     uint64_t sum = 0;
// //     std::chrono::_V2::system_clock::time_point s_ts;
// //     std::chrono::_V2::system_clock::time_point e_ts;

// //     for (const auto predicate : predicates) {
// //         for (size_t i = 0; i < 20; ++i) {
// //             reset_timer();

// //             if (predicate == 0) {
// //                 s_ts = std::chrono::high_resolution_clock::now();
// //                 sum = pipe_1<false, false, false, false>(predicate, {"col_0", "col_1", "col_2"});
// //                 e_ts = std::chrono::high_resolution_clock::now();
// //             } else {
// //                 s_ts = std::chrono::high_resolution_clock::now();
// //                 sum = pipe_2<false, false, false, false>(predicate, {"col_0", "col_1", "col_2"});
// //                 e_ts = std::chrono::high_resolution_clock::now();
// //             }

// //             std::chrono::duration<double> secs = e_ts - s_ts;
// //             auto additional_time = secs.count() - (workingTime.count() + waitingTime.count());

// //             out << locality << "\tFull\tPipe\t" << Benchmarks::OPTIMAL_BLOCK_SIZE << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << std::endl
// //                 << std::flush;
// //             std::cout << locality << "\tFull\tPipe\t" << Benchmarks::OPTIMAL_BLOCK_SIZE << "\t" << +predicate << "\t" << sum << "\t" << waitingTime.count() << "\t" << workingTime.count() << "\t" << secs.count() << "\t" << additional_time << std::endl;
// //         }
// //     }

// //     out.close();
// // }