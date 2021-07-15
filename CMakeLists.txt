cmake_minimum_required(VERSION 3.4)

project(mg_import VERSION 0.0.1)

# Find and set Clang compilers.
find_program(CLANG_FOUND clang)
find_program(CLANGXX_FOUND clang++)
if(CLANG_FOUND AND CLANGXX_FOUND)
  set(CMAKE_C_COMPILER "${CLANG_FOUND}")
  set(CMAKE_CXX_COMPILER "${CLANGXX_FOUND}")
else()
  message(FATAL_ERROR "Couldn't find clang and/or clang++!")
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)

# Set default build type to 'Debug'
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Debug")
endif()
message(STATUS "CMake build type: ${CMAKE_BUILD_TYPE}")

# ------------------------------------------------------------------------------
# --------------------------- EXTERNAL DEPENDENCIES ----------------------------
# ------------------------------------------------------------------------------

include(ExternalProject)

find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

# Setup PostgreSQL
find_package(PostgreSQL REQUIRED)

# Setup gflags
set(GFLAGS_ROOT ${PROJECT_BINARY_DIR}/gflags)
ExternalProject_Add(gflags-proj
  PREFIX            ${GFLAGS_ROOT}
  INSTALL_DIR       ${GFLAGS_ROOT}
  GIT_REPOSITORY    https://github.com/gflags/gflags.git
  GIT_TAG           v2.2.2
  CMAKE_ARGS        "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
                    "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
                    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")

set(GFLAGS_INCLUDE_DIR ${GFLAGS_ROOT}/include)
set(GFLAGS_LIBRARY_PATH ${GFLAGS_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gflags.a)
set(GFLAGS_DEBUG_LIBRARY_PATH ${GFLAGS_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gflags_debug.a)
add_library(gflags STATIC IMPORTED)
set_target_properties(gflags PROPERTIES
  IMPORTED_LOCATION ${GFLAGS_LIBRARY_PATH}
  IMPORTED_LOCATION_DEBUG ${GFLAGS_DEBUG_LIBRARY_PATH}
  INTERFACE_LINK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
add_dependencies(gflags gflags-proj)

# Setup glog
set(GLOG_ROOT ${PROJECT_BINARY_DIR}/glog)
ExternalProject_Add(glog-proj
  PREFIX            ${GLOG_ROOT}
  INSTALL_DIR       ${GLOG_ROOT}
  GIT_REPOSITORY    https://github.com/google/glog.git
  GIT_TAG           v0.4.0
  CMAKE_ARGS        "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
                    "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
                    "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
                    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  DEPENDS           gflags-proj)

set(GLOG_INCLUDE_DIR ${GLOG_ROOT}/include)
set(GLOG_LIBRARY_PATH ${GLOG_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}glog.a)
set(GLOG_DEBUG_LIBRARY_PATH ${GLOG_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}glogd.a)
add_library(glog STATIC IMPORTED)
set_target_properties(glog PROPERTIES
  IMPORTED_LOCATION ${GLOG_LIBRARY_PATH}
  IMPORTED_LOCATION_DEBUG ${GLOG_DEBUG_LIBRARY_PATH}
  INTERFACE_LINK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
add_dependencies(glog glog-proj)

# Setup mgclient
set(MG_CLIENT_ROOT ${PROJECT_BINARY_DIR}/mgclient)
ExternalProject_Add(mgclient-proj
  PREFIX            ${MG_CLIENT_ROOT}
  INSTALL_DIR       ${MG_CLIENT_ROOT}
  GIT_TAG           v1.2.1
  GIT_REPOSITORY    https://github.com/memgraph/mgclient.git
  CMAKE_ARGS        "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
                    "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
                    "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
                    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
                    "-DBUILD_TESTING=OFF")

set(MG_CLIENT_INCLUDE_DIR ${MG_CLIENT_ROOT}/include)
set(MG_CLIENT_LIBRARY_PATH ${MG_CLIENT_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}mgclient.a)
add_library(mgclient STATIC IMPORTED)
set_target_properties(mgclient PROPERTIES
  IMPORTED_LOCATION ${MG_CLIENT_LIBRARY_PATH}
  INTERFACE_LINK_LIBRARIES "${OPENSSL_LIBRARIES}")
add_dependencies(mgclient mgclient-proj)

# Setup pqxx
# Note: there is package libpqxx-dev, outdated and not compatible with C++17
set(PQXX_ROOT ${PROJECT_BINARY_DIR}/pqxx)
ExternalProject_Add(pqxx-proj
  PREFIX            ${PQXX_ROOT}
  INSTALL_DIR       ${PQXX_ROOT}
  GIT_REPOSITORY    https://github.com/jtv/libpqxx.git
  GIT_TAG           7.1.2
  CMAKE_ARGS        "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
  "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
  "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
  "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  "-DBUILD_TEST=OFF")

set(PQXX_INCLUDE_DIR ${PQXX_ROOT}/include)
set(PQXX_LIBRARY_PATH ${PQXX_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}pqxx.a)
add_library(pqxx STATIC IMPORTED)
set_target_properties(pqxx PROPERTIES
  IMPORTED_LOCATION ${PQXX_LIBRARY_PATH}
  INTERFACE_LINK_LIBRARIES "${OPENSSL_LIBRARIES}")
add_dependencies(pqxx pqxx-proj)

include_directories(${PQXX_INCLUDE_DIR})


# Setup MySQL Connector/C++
set(MYSQL_CONN_ROOT ${PROJECT_BINARY_DIR}/mysqlconnector)
ExternalProject_Add(mysqlconnector-proj
  PREFIX         ${MYSQL_CONN_ROOT}
  INSTALL_DIR    ${MYSQL_CONN_ROOT}
  GIT_REPOSITORY https://github.com/mysql/mysql-connector-cpp
  GIT_TAG        8.0.25
  CMAKE_ARGS     "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
  "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
  "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
  "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  "-DBUILD_STATIC=ON"
  "-DBUILD_TEST=OFF")

set(MYSQL_CONN_INCLUDE_DIR ${MYSQL_CONN_ROOT}/include)
set(MYSQL_CONN_LIBRARY_PATH ${MYSQL_CONN_ROOT}/lib64/${CMAKE_FIND_LIBRARY_PREFIXES}mysqlcppconn8-static.a)
set(MYSQL_CONN_DEBUG_LIBRARY_PATH ${MYSQL_CONN_ROOT}/lib64/debug/${CMAKE_FIND_LIBRARY_PREFIXES}mysqlcppconn8-static.a)
add_library(mysqlconnector STATIC IMPORTED)
set_target_properties(mysqlconnector PROPERTIES
  IMPORTED_LOCATION ${MYSQL_CONN_LIBRARY_PATH}
  IMPORTED_LOCATION_DEBUG ${MYSQL_CONN_DEBUG_LIBRARY_PATH}
  INTERFACE_LINK_LIBRARIES "${OPENSSL_LIBRARIES};${THREADS_LIBRARIES};resolv")
add_dependencies(mysqlconnector mysqlconnector-proj)

include_directories(${MYSQL_CONN_INCLUDE_DIR})


# Setup gtest
set(GTEST_ROOT ${PROJECT_BINARY_DIR}/gtest)
ExternalProject_Add(gtest-proj
  PREFIX            ${GTEST_ROOT}
  INSTALL_DIR       ${GTEST_ROOT}
  GIT_REPOSITORY    https://github.com/google/googletest.git
  GIT_TAG           v1.10.x
  CMAKE_ARGS        "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
                    "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
                    "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
                    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")

set(GTEST_INCLUDE_DIR ${GTEST_ROOT}/include)
set(GTEST_LIBRARY_PATH ${GTEST_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gtest.a)
set(GTEST_DEBUG_LIBRARY_PATH ${GTEST_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gtestd.a)
add_library(gtest STATIC IMPORTED)
set_target_properties(gtest PROPERTIES
  IMPORTED_LOCATION ${GTEST_LIBRARY_PATH}
  IMPORTED_LOCATION_DEBUG ${GTEST_DEBUG_LIBRARY_PATH}
  INTERFACE_LINK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
add_dependencies(gtest gtest-proj)

set(GTEST_MAIN_LIBRARY_PATH ${GTEST_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gtest_main.a)
set(GTEST_MAIN_DEBUG_LIBRARY_PATH ${GTEST_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gtest_maind.a)
add_library(gtest-main STATIC IMPORTED)
set_target_properties(gtest-main PROPERTIES
  IMPORTED_LOCATION ${GTEST_MAIN_LIBRARY_PATH}
  IMPORTED_LOCATION_DEBUG ${GTEST_MAIN_DEBUG_LIBRARY_PATH}
  INTERFACE_LINK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
add_dependencies(gtest-main gtest-proj)

# Setup gmock
set(GMOCK_LIBRARY_PATH ${GTEST_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gmock.a)
set(GMOCK_DEBUG_LIBRARY_PATH ${GTEST_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gmockd.a)
add_library(gmock STATIC IMPORTED)
set_target_properties(gmock PROPERTIES
  IMPORTED_LOCATION ${GMOCK_LIBRARY_PATH}
  IMPORTED_LOCATION_DEBUG ${GMOCK_DEBUG_LIBRARY_PATH}
  INTERFACE_LINK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
add_dependencies(gmock gtest-proj)

set(GMOCK_MAIN_LIBRARY_PATH ${GTEST_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gmock_main.a)
set(GMOCK_MAIN_DEBUG_LIBRARY_PATH ${GTEST_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gmock_maind.a)
add_library(gmock-main STATIC IMPORTED)
set_target_properties(gmock-main PROPERTIES
  IMPORTED_LOCATION ${GMOCK_MAIN_LIBRARY_PATH}
  IMPORTED_LOCATION_DEBUG ${GMOCK_MAIN_DEBUG_LIBRARY_PATH}
  INTERFACE_LINK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
add_dependencies(gmock-main gtest-proj)


# ------------------------------------------------------------------------------

add_subdirectory(src)
add_subdirectory(tests)
