# Specify the minimum version of CMake required
cmake_minimum_required(VERSION 3.10)

# Define the project name
project(rtes-okx-raspi)

# Enable verbose output during the build process
# This is useful for debugging build issues
set(CMAKE_VERBOSE_MAKEFILE ON)

# Define the executable target
add_executable(rtes-okx-raspi rtes-okx.c)

# Optional: Add include directories if needed
# include_directories(include)

# Optional: Link libraries if needed
target_link_libraries(rtes-okx-raspi -lm -pthread -ljansson -lwebsockets)

# Optional: Set compile options if needed
# target_compile_options(cmake_hello_world_test PRIVATE -Wall -Wextra)
