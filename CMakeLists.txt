cmake_minimum_required(VERSION 3.16)

project(disaggDataProvider VERSION 0.1)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(warnings "-Wall -Wextra -pedantic -Wcast-align -Wcast-qual -Wctor-dtor-privacy -Wdisabled-optimization -Wformat=2 -Winit-self -Wlogical-op -Wmissing-include-dirs -Wnoexcept -Wold-style-cast -Woverloaded-virtual -Wredundant-decls -Wshadow -Wsign-promo -Wstrict-null-sentinel -Wstrict-overflow=5 -Wundef -Wno-unused -Wno-variadic-macros -Wno-parentheses -fdiagnostics-show-option")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${warnings}")
set(OpenMP_CXX_FLAGS "${OpenMP_CXX_FLAGS} -fopenmp")
set(OPTIMIZE_OPTIONS "-O3")

FIND_PACKAGE(OpenMP)

if(OPENMP_FOUND)
    message("OPENMP FOUND")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

include_directories(SYSTEM ${OpenMP_INCLUDE_PATH})

if(NOT TARGET build-time-make-directory)
    add_custom_target(build-time-make-directory ALL
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_SOURCE_DIR}/logs
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_SOURCE_DIR}/logs/bench
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_SOURCE_DIR}/logs/tests)
endif()

add_subdirectory("ext/MemConnect")
include_directories(ext/MemConnect)
include_directories(ext/MemConnect/include)
include_directories(ext/MemConnect/src)

add_subdirectory("ext/DisaggDataDisciple")
include_directories(ext/DisaggDataDisciple)
include_directories(ext/DisaggDataDisciple/include)
include_directories(ext/DisaggDataDisciple/src)

# add_compile_options("-fsanitize=thread")
add_compile_options("-fopenmp")
add_compile_options("${OPTIMIZE_OPTIONS}")

set(SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(INC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

file(GLOB_RECURSE SOURCES ${SRC_DIR}/*.cpp)
file(GLOB_RECURSE HEADERS ${INC_DIR}/*.h)

file(GLOB_RECURSE SOURCES_RDMA ext/MemConnect/src/*.cpp)
list(REMOVE_ITEM SOURCES_RDMA ${CMAKE_CURRENT_SOURCE_DIR}/ext/DisaggDataDisciple/src/memConnect_server.cpp)
file(GLOB_RECURSE HEADERS_RDMA ext/MemConnect/include/*.h)
add_library(memoLib ${SOURCES_RDMA} ${HEADERS_RDMA})
target_link_libraries(memoLib ibverbs)

file(GLOB_RECURSE SOURCES_TCP ext/DisaggDataDisciple/src/*.cpp)
list(REMOVE_ITEM SOURCES_TCP ${CMAKE_CURRENT_SOURCE_DIR}/ext/DisaggDataDisciple/src/client/tcp_client_example.cpp)
list(REMOVE_ITEM SOURCES_TCP ${CMAKE_CURRENT_SOURCE_DIR}/ext/DisaggDataDisciple/src/server/tcp_server_example.cpp)
file(GLOB_RECURSE HEADERS_TCP ext/DisaggDataDisciple/include/*.h)
add_library(busLib ${SOURCES_TCP} ${HEADERS_TCP})

include_directories(${CMAKE_SOURCE_DIR}/ext/MemConnect ${CMAKE_SOURCE_DIR}/ext/MemConnect/include ${ProtobufIncludePath})

add_executable(disaggDataProvider ${SOURCES} ${HEADERS})
target_link_libraries(disaggDataProvider "pthread" "memoLib" "busLib" "numa" ddd-proto-objects ${OpenMP_CXX_LIBRARIES})