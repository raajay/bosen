cmake_minimum_required(VERSION 2.8.12)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "-O3 -std=c++11 -Wall -Wno-sign-compare -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -fno-omit-frame-pointer -Wno-deprecated-declarations -Wno-unused-variable")

include_directories(BEFORE "${CMAKE_SOURCE_DIR}/src" "${CMAKE_SOURCE_DIR}/third_party/include")
link_directories(BEFORE "${CMAKE_SOURCE_DIR}/third_party/lib")
set(third_party_libs rt nsl zmq boost_thread boost_system glog gflags
    tcmalloc config++ snappy yaml-cpp leveldb pthread)


file(GLOB_RECURSE petuum_ps_src "${CMAKE_SOURCE_DIR}/src/petuum_ps/*.[h|c]pp")
file(GLOB_RECURSE petuum_ps_common_src "${CMAKE_SOURCE_DIR}/src/petuum_ps_common/*.[h|c]pp")
file(GLOB_RECURSE ml_src "${CMAKE_SOURCE_DIR}/src/ml/*.[h|c]pp")
file(GLOB_RECURSE io_src "${CMAKE_SOURCE_DIR}/src/io/*.[h|c]pp")

file(MAKE_DIRECTORY "${CMAKE_SOURCE_DIR}/lib")
# petuum-ps
add_library(petuum-ps ${petuum_ps_src} ${petuum_ps_common_src})
set_target_properties(petuum-ps PROPERTIES ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)

# petuum-ml (misc files)
add_library(petuum-ml ${ml_src} ${io_src})
set_target_properties(petuum-ml PROPERTIES ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)

# dnn
add_subdirectory("./app/dnn")
