cmake_minimum_required(VERSION 3.22)
project(ArrowSample)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS -O3)

find_package(Arrow REQUIRED)
find_package(benchmark REQUIRED)

add_executable(arrow_sample src/main.cc)
target_link_libraries(arrow_sample PRIVATE Arrow::arrow_static)

add_executable(benchmark src/benchmark.cc)
target_link_libraries(benchmark PRIVATE Arrow::arrow_static benchmark::benchmark)
