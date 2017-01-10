cmake_minimum_required(VERSION 2.8.12)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/bin")

set(DNN_APP_DIR "${CMAKE_SOURCE_DIR}/app/dnn")
file(GLOB_RECURSE dnn_src "${DNN_APP_DIR}/src/dnn/*.[h|c]pp" "${DNN_APP_DIR}/src/io/*.[c|h]pp")
file(GLOB_RECURSE dnn_pred_src "${DNN_APP_DIR}/src/dnn_predict/*.[h|c]pp")
file(GLOB_RECURSE gen_data_src "${DNN_APP_DIR}/src/tools/gen_data.cpp")
file(GLOB_RECURSE gen_data2_src "${DNN_APP_DIR}/src/tools/gen_data2.cpp")

add_executable(DNN ${dnn_src})
target_link_libraries(DNN petuum-ps petuum-ml ${third_party_libs})
set_target_properties(DNN PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${DNN_APP_DIR}/bin")

add_executable(DNN_PRED ${dnn_pred_src})
target_link_libraries(DNN_PRED petuum-ps petuum-ml ${third_party_libs})
set_target_properties(DNN_PRED PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${DNN_APP_DIR}/bin")

add_executable(gen_data ${gen_data_src})
set_target_properties(gen_data PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${DNN_APP_DIR}/bin")

add_executable(gen_data2 ${gen_data2_src})
set_target_properties(gen_data2 PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${DNN_APP_DIR}/bin")
