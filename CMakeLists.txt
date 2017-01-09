cmake_minimum_required(VERSION 2.8.12)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "-O3 -std=c++11 -Wall -Wno-sign-compare -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -fno-omit-frame-pointer -Wno-deprecated-declarations -Wno-unused-variable")

message(${CMAKE_SOURCE_DIR})

file(GLOB_RECURSE petuum_ps_src "${CMAKE_SOURCE_DIR}/src/petuum_ps/*.[h|c]pp")
file(GLOB_RECURSE petuum_ps_common_src "${CMAKE_SOURCE_DIR}/src/petuum_ps_common/*.[h|c]pp")
file(GLOB_RECURSE ml_src "${CMAKE_SOURCE_DIR}/src/ml/*.[h|c]pp")
file(GLOB_RECURSE io_src "${CMAKE_SOURCE_DIR}/src/io/*.[h|c]pp")

include_directories(BEFORE "${CMAKE_SOURCE_DIR}/src" "${CMAKE_SOURCE_DIR}/third_party/include")

#message("${petuum_ps_src}")

# petuum-ps
add_library(petuum-ps ${petuum_ps_src} ${petuum_ps_common_src})
set_target_properties(petuum-ps PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib2
    )

# petuum-ml (misc files)
add_library(petuum-ml ${ml_src} ${io_src})
set_target_properties(petuum-ml PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib2
    )

# dnn
set(DNN_APP_DIR "${CMAKE_SOURCE_DIR}/app/dnn")
file(GLOB_RECURSE dnn_src "${DNN_APP_DIR}/src/dnn/*.[h|c]pp" "${DNN_APP_DIR}/src/io/*.[c|h]pp")
file(GLOB_RECURSE dnn_pred_src "${DNN_APP_DIR}/src/dnn_predict/*.[h|c]pp")
file(GLOB_RECURSE gen_data_src "${DNN_APP_DIR}/src/tools/gen_data.cpp")
file(GLOB_RECURSE gen_data2_src "${DNN_APP_DIR}/src/tools/gen_data2.cpp")

add_executable(DNN ${dnn_src})
target_link_libraries(DNN petuum-ps petuum-ml)
set_target_properties(DNN PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${DNN_APP_DIR}/bin"
    )


add_executable(DNN_PRED ${dnn_pred_src})
target_link_libraries(DNN_PRED petuum-ps petuum-ml)
set_target_properties(DNN PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${DNN_APP_DIR}/bin"
    )

add_executable(gen_data ${gen_data_src})
set_target_properties(DNN PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${DNN_APP_DIR}/bin"
    )

add_executable(gen_data2 ${gen_data2_src})
set_target_properties(DNN PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${DNN_APP_DIR}/bin"
    )
