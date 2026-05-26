cmake_minimum_required(VERSION 3.22)

project(telemetry_compute_accelerator LANGUAGES CXX)

option(ACCELERATOR_ENABLE_CUDA "Build CUDA placeholder target" OFF)
option(ACCELERATOR_ENABLE_TESTS "Build accelerator unit tests" ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(compute_accelerator_core
    cpp/accelerator_service.cpp
    grpc/server_skeleton.cpp
)

target_include_directories(compute_accelerator_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/grpc
)

add_executable(compute_accelerator cpp/service_main.cpp)
target_link_libraries(compute_accelerator PRIVATE compute_accelerator_core)

add_executable(accelerator_benchmark benchmarks/benchmark_placeholder.cpp)

if(ACCELERATOR_ENABLE_TESTS)
    enable_testing()
    add_executable(accelerator_service_test tests/accelerator_service_test.cpp)
    target_link_libraries(accelerator_service_test PRIVATE compute_accelerator_core)
    add_test(NAME accelerator_service_test COMMAND accelerator_service_test)
endif()

if(ACCELERATOR_ENABLE_CUDA)
    enable_language(CUDA)
    add_library(compute_accelerator_cuda cuda/kernels.cu)
    set_target_properties(compute_accelerator_cuda PROPERTIES CUDA_STANDARD 17)
endif()
