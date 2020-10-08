cmake_minimum_required(VERSION 3.16)
project(asst2)

set(CMAKE_CXX_STANDARD 11)

include_directories(part_b common tests)

add_executable(asst2 tests/main.cpp part_b/tasksys.cpp)

target_link_libraries(asst2 PRIVATE pthread)