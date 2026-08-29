cmake_minimum_required(VERSION 3.10)
project(ChatServer)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(spdlog REQUIRED)

add_executable(server server.cpp)
add_executable(client client.cpp)
add_executable(stress_test stress_test.cpp)

target_link_libraries(server pthread spdlog::spdlog)
target_link_libraries(client pthread)
target_link_libraries(stress_test pthread)
