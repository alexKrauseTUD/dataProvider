cmake_minimum_required(VERSION 3.10)

project(disaggDataProvider VERSION 0.1)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "-Wall -pedantic -Wextra -fopenmp")
set(OPTIMIZE_OPTIONS "-O3")
set(warnings "-Wall -Wextra -pedantic")

FIND_PACKAGE(OpenMP)

if(OPENMP_FOUND)
    message("OPENMP FOUND")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

include_directories(SYSTEM ${OpenMP_INCLUDE_PATH})

add_subdirectory("ext/memoRDMA")
include_directories(ext/memoRDMA)
include_directories(ext/memoRDMA/include)
include_directories(ext/memoRDMA/src)

# add_compile_options("-fsanitize=thread")
add_compile_options("-fopenmp")

set(SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(INC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

file(GLOB_RECURSE SOURCES ${SRC_DIR}/*.cpp)
file(GLOB_RECURSE HEADERS ${INC_DIR}/*.h)

file(GLOB_RECURSE SOURCES_RDMA ext/memoRDMA/src/*.cpp)
list(REMOVE_ITEM SOURCES_RDMA ${CMAKE_CURRENT_SOURCE_DIR}/ext/memoRDMA/src/memoRDMA_server.cpp)
file(GLOB_RECURSE HEADERS_RDMA ext/memoRDMA/include/*.h)
add_library(memoLib ${SOURCES_RDMA} ${HEADERS_RDMA})
target_link_libraries(memoLib ibverbs)

include_directories(${CMAKE_SOURCE_DIR}/ext/memoRDMA ${CMAKE_SOURCE_DIR}/ext/memoRDMA/include)

add_executable(disaggDataProvider ${SOURCES} ${HEADERS})
target_link_libraries(disaggDataProvider "pthread" "memoLib" "numa" ${OpenMP_CXX_LIBRARIES})