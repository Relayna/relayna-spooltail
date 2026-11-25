cmake_minimum_required(VERSION 3.18)
project(spooltail_replay_native LANGUAGES CXX)
find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)
find_package(Protobuf REQUIRED)
set(schema_dir "${CMAKE_CURRENT_SOURCE_DIR}/../../proto/spooltail/v1")
add_custom_command(
  OUTPUT spooltail.pb.cc spooltail.pb.h
  COMMAND protobuf::protoc "--proto_path=${schema_dir}" "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}"
          "${schema_dir}/spooltail.proto"
  DEPENDS "${schema_dir}/spooltail.proto"
  VERBATIM)
pybind11_add_module(spooltail_replay_native decoder.cc spooltail.pb.cc)
target_compile_features(spooltail_replay_native PRIVATE cxx_std_17)
target_include_directories(spooltail_replay_native PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(spooltail_replay_native PRIVATE protobuf::libprotobuf)
install(TARGETS spooltail_replay_native LIBRARY DESTINATION . RUNTIME DESTINATION .)
