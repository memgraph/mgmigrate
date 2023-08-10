cmake_minimum_required(VERSION 3.14)

project(mgmigrate VERSION 0.0.1)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake)
set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# Set default build type to 'Debug'
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Debug")
endif()
message(STATUS "CMake build type: ${CMAKE_BUILD_TYPE}")

if(WIN32)
  set(MGMIGRATE_ON_WINDOWS TRUE)
  set(MGMIGRATE_FIND_LIBRARY_PREFIXES "lib")
elseif(APPLE)
  set(MGMIGRATE_ON_OSX TRUE)
  set(MGMIGRATE_FIND_LIBRARY_PREFIXES "${CMAKE_FIND_LIBRARY_PREFIXES}")
elseif(UNIX)
  set(MGMIGRATE_ON_LINUX TRUE)
  set(MGMIGRATE_FIND_LIBRARY_PREFIXES "${CMAKE_FIND_LIBRARY_PREFIXES}")
endif()

# Set default instalation directory to '/usr'
if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    # '/usr' is a special case, for more details see:
    # https://cmake.org/cmake/help/latest/module/GNUInstallDirs.html#special-cases
    set(CMAKE_INSTALL_PREFIX "/usr" CACHE PATH "install dir" FORCE)
endif()

# ------------------------------------------------------------------------------
# --------------------------- EXTERNAL DEPENDENCIES ----------------------------
# ------------------------------------------------------------------------------
include(ExternalProject)

find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(PostgreSQL REQUIRED)

if(MGMIGRATE_ON_LINUX OR MGMIGRATE_ON_OSX)
  # We build static libpq.a manually so we have a lib with minimal dependencies
  set(POSTGRESQL_ROOT ${PROJECT_BINARY_DIR}/postgresql)
  ExternalProject_Add(postgresql-proj
    PREFIX            ${POSTGRESQL_ROOT}
    INSTALL_DIR       ${POSTGRESQL_ROOT}
    URL               https://ftp.postgresql.org/pub/source/v13.3/postgresql-13.3.tar.gz
    CONFIGURE_COMMAND ""
    BUILD_COMMAND     unset MAKEFLAGS && unset MFLAGS && unset MAKELEVEL && ${CMAKE_CURRENT_SOURCE_DIR}/scripts/build-libpq.py <SOURCE_DIR> <INSTALL_DIR>
    INSTALL_COMMAND   "")
  set(MGMIGRATE_POSTGRESQL_LIBRARIES ${POSTGRESQL_ROOT}/libpq.a ${POSTGRESQL_ROOT}/libpgcommon.a ${POSTGRESQL_ROOT}/libpgport.a)
endif()

if(MGMIGRATE_ON_WINDOWS)
  set(GFLAGS_LIB_SUFFIX "_static")
  set(CMAKE_CXX_STANDARD_LIBRARIES "-static-libgcc -static-libstdc++")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-Bstatic,--whole-archive -lwinpthread -Wl,--no-whole-archive,-Bdynamic")
elseif(MGMIGRATE_ON_LINUX)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
endif()

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
set(GFLAGS_LIBRARY_PATH ${GFLAGS_ROOT}/lib/${MGMIGRATE_FIND_LIBRARY_PREFIXES}gflags${GFLAGS_LIB_SUFFIX}.a)
set(GFLAGS_DEBUG_LIBRARY_PATH ${GFLAGS_ROOT}/lib/${MGMIGRATE_FIND_LIBRARY_PREFIXES}gflags${GFLAGS_LIB_SUFFIX}_debug.a)
add_library(gflags STATIC IMPORTED)
set_target_properties(gflags PROPERTIES
  IMPORTED_LOCATION "${GFLAGS_LIBRARY_PATH}"
  IMPORTED_LOCATION_DEBUG "${GFLAGS_DEBUG_LIBRARY_PATH}"
  INTERFACE_LINK_LIBRARIES "${CMAKE_THREAD_LIBS_INIT}")
add_dependencies(gflags gflags-proj)

# Setup glog
set(GLOG_ROOT ${PROJECT_BINARY_DIR}/glog)
ExternalProject_Add(glog-proj
  PREFIX            ${GLOG_ROOT}
  INSTALL_DIR       ${GLOG_ROOT}
  GIT_REPOSITORY    https://github.com/google/glog.git
  GIT_TAG           v0.5.0
  CMAKE_ARGS        "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
                    "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
                    "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
                    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
                    "-DBUILD_SHARED_LIBS=OFF"
  DEPENDS           gflags-proj)
set(GLOG_INCLUDE_DIR ${GLOG_ROOT}/include)
set(GLOG_LIBRARY_PATH ${GLOG_ROOT}/lib/${MGMIGRATE_FIND_LIBRARY_PREFIXES}glog.a)
set(GLOG_DEBUG_LIBRARY_PATH ${GLOG_ROOT}/lib/${MGMIGRATE_FIND_LIBRARY_PREFIXES}glogd.a)
add_library(glog STATIC IMPORTED)
set_target_properties(glog PROPERTIES
  IMPORTED_LOCATION "${GLOG_LIBRARY_PATH}"
  IMPORTED_LOCATION_DEBUG "${GLOG_DEBUG_LIBRARY_PATH}"
  INTERFACE_LINK_LIBRARIES "${CMAKE_THREAD_LIBS_INIT}")
add_dependencies(glog glog-proj)

# Setup mgclient
if(OPENSSL_ROOT_DIR)
  set(MGMIGRATE_MGCLIENT_OPENSSL_ROOTDIR_FLAG "-DOPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR}")
endif()
set(MG_CLIENT_ROOT ${PROJECT_BINARY_DIR}/mgclient)
ExternalProject_Add(mgclient-proj
  PREFIX            ${MG_CLIENT_ROOT}
  INSTALL_DIR       ${MG_CLIENT_ROOT}
  GIT_TAG           v1.2.1
  GIT_REPOSITORY    https://github.com/memgraph/mgclient.git
  CMAKE_ARGS        "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
                    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
                    "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
                    "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
                    ${MGMIGRATE_MGCLIENT_OPENSSL_ROOTDIR_FLAG}
                    "-DBUILD_TESTING=OFF")
set(MG_CLIENT_INCLUDE_DIR ${MG_CLIENT_ROOT}/include)
set(MG_CLIENT_LIBRARY_PATH ${MG_CLIENT_ROOT}/lib/${MGMIGRATE_FIND_LIBRARY_PREFIXES}mgclient.a)
add_library(mgclient STATIC IMPORTED)
set_target_properties(mgclient PROPERTIES
  IMPORTED_LOCATION "${MG_CLIENT_LIBRARY_PATH}"
  INTERFACE_LINK_LIBRARIES "${OPENSSL_LIBRARIES}")
add_dependencies(mgclient mgclient-proj)

# Setup pqxx
# Note: there is package libpqxx-dev, outdated and not compatible with C++17
set(PQXX_ROOT ${PROJECT_BINARY_DIR}/pqxx)
ExternalProject_Add(pqxx-proj
  PREFIX            ${PQXX_ROOT}
  INSTALL_DIR       ${PQXX_ROOT}
  GIT_REPOSITORY    https://github.com/jtv/libpqxx.git
  GIT_TAG           7.6.0
  CMAKE_ARGS        "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
  "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
  "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
  "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  "-DBUILD_SHARED_LIBS=OFF"
  "-DBUILD_TEST=OFF")
set(PQXX_INCLUDE_DIR ${PQXX_ROOT}/include)
set(PQXX_LIBRARY_PATH ${PQXX_ROOT}/lib/${MGMIGRATE_FIND_LIBRARY_PREFIXES}pqxx.a)
add_library(pqxx STATIC IMPORTED)
set_target_properties(pqxx PROPERTIES
  IMPORTED_LOCATION "${PQXX_LIBRARY_PATH}"
  INTERFACE_LINK_LIBRARIES "${OPENSSL_LIBRARIES}")
add_dependencies(pqxx pqxx-proj)
include_directories(${PQXX_INCLUDE_DIR})

# Setup MySQL Connector/C++
if(OPENSSL_ROOT_DIR)
  set(MGMIGRATE_MYSQL_CONN_OPENSSL_ROOTDIR_FLAG "-DWITH_SSL=${OPENSSL_ROOT_DIR}")
endif()
set(MYSQL_CONN_ROOT ${PROJECT_BINARY_DIR}/mysqlconnector)
ExternalProject_Add(mysqlconnector-proj
  PREFIX         ${MYSQL_CONN_ROOT}
  INSTALL_DIR    ${MYSQL_CONN_ROOT}
  GIT_REPOSITORY https://github.com/mysql/mysql-connector-cpp
  GIT_TAG        8.0.26
  PATCH_COMMAND  PATCH_COMMAND git reset --hard && git apply --reject --ignore-space-change --ignore-whitespace ${CMAKE_CURRENT_SOURCE_DIR}/mysqlconnector.patch
  CMAKE_ARGS     "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
  "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
  "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
  "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
  "-DBUILD_STATIC=ON"
  ${MGMIGRATE_MYSQL_CONN_OPENSSL_ROOTDIR_FLAG}
  "-DBUILD_TEST=OFF")
set(MYSQL_CONN_INCLUDE_DIR ${MYSQL_CONN_ROOT}/include)
set(MYSQL_CONN_LIBRARY_PATH ${MYSQL_CONN_ROOT}/lib64/${MGMIGRATE_FIND_LIBRARY_PREFIXES}mysqlcppconn8-static.a)
set(MYSQL_CONN_DEBUG_LIBRARY_PATH ${MYSQL_CONN_ROOT}/lib64/debug/${MGMIGRATE_FIND_LIBRARY_PREFIXES}mysqlcppconn8-static.a)
add_library(mysqlconnector STATIC IMPORTED)
set_target_properties(mysqlconnector PROPERTIES
  IMPORTED_LOCATION "${MYSQL_CONN_LIBRARY_PATH}"
  IMPORTED_LOCATION_DEBUG "${MYSQL_CONN_DEBUG_LIBRARY_PATH}"
  INTERFACE_LINK_LIBRARIES "${OPENSSL_LIBRARIES};${THREADS_LIBRARIES}")
add_dependencies(mysqlconnector mysqlconnector-proj)
include_directories(${MYSQL_CONN_INCLUDE_DIR})

# Setup gtest
# TODO(gitbuda): Replace all gtest External with Fetch
set(GTEST_ROOT ${PROJECT_BINARY_DIR}/gtest)
ExternalProject_Add(gtest-proj
  PREFIX            ${GTEST_ROOT}
  INSTALL_DIR       ${GTEST_ROOT}
  GIT_REPOSITORY    https://github.com/google/googletest.git
  GIT_TAG           release-1.11.0
  CMAKE_ARGS        "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
                    "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
                    "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
                    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")
set(GTEST_INCLUDE_DIR ${GTEST_ROOT}/include)
set(GTEST_LIBRARY_PATH ${GTEST_ROOT}/lib/${MGMIGRATE_FIND_LIBRARY_PREFIXES}gtest.a)
set(GTEST_DEBUG_LIBRARY_PATH ${GTEST_ROOT}/lib/${MGMIGRATE_FIND_LIBRARY_PREFIXES}gtestd.a)
add_library(gtest STATIC IMPORTED)
set_target_properties(gtest PROPERTIES
  IMPORTED_LOCATION "${GTEST_LIBRARY_PATH}"
  IMPORTED_LOCATION_DEBUG "${GTEST_DEBUG_LIBRARY_PATH}"
  INTERFACE_LINK_LIBRARIES "${CMAKE_THREAD_LIBS_INIT}")
add_dependencies(gtest gtest-proj)
set(GTEST_MAIN_LIBRARY_PATH ${GTEST_ROOT}/lib/${MGMIGRATE_FIND_LIBRARY_PREFIXES}gtest_main.a)
set(GTEST_MAIN_DEBUG_LIBRARY_PATH ${GTEST_ROOT}/lib/${MGMIGRATE_FIND_LIBRARY_PREFIXES}gtest_maind.a)
add_library(gtest-main STATIC IMPORTED)
set_target_properties(gtest-main PROPERTIES
  IMPORTED_LOCATION "${GTEST_MAIN_LIBRARY_PATH}"
  IMPORTED_LOCATION_DEBUG "${GTEST_MAIN_DEBUG_LIBRARY_PATH}"
  INTERFACE_LINK_LIBRARIES "${CMAKE_THREAD_LIBS_INIT}")
add_dependencies(gtest-main gtest-proj)
# Setup gmock
set(GMOCK_LIBRARY_PATH ${GTEST_ROOT}/lib/${MGMIGRATE_FIND_LIBRARY_PREFIXES}gmock.a)
set(GMOCK_DEBUG_LIBRARY_PATH ${GTEST_ROOT}/lib/${MGMIGRATE_FIND_LIBRARY_PREFIXES}gmockd.a)
add_library(gmock STATIC IMPORTED)
set_target_properties(gmock PROPERTIES
  IMPORTED_LOCATION "${GMOCK_LIBRARY_PATH}"
  IMPORTED_LOCATION_DEBUG "${GMOCK_DEBUG_LIBRARY_PATH}"
  INTERFACE_LINK_LIBRARIES "${CMAKE_THREAD_LIBS_INIT}")
add_dependencies(gmock gtest-proj)
set(GMOCK_MAIN_LIBRARY_PATH ${GTEST_ROOT}/lib/${MGMIGRATE_FIND_LIBRARY_PREFIXES}gmock_main.a)
set(GMOCK_MAIN_DEBUG_LIBRARY_PATH ${GTEST_ROOT}/lib/${MGMIGRATE_FIND_LIBRARY_PREFIXES}gmock_maind.a)
add_library(gmock-main STATIC IMPORTED)
set_target_properties(gmock-main PROPERTIES
  IMPORTED_LOCATION "${GMOCK_MAIN_LIBRARY_PATH}"
  IMPORTED_LOCATION_DEBUG "${GMOCK_MAIN_DEBUG_LIBRARY_PATH}"
  INTERFACE_LINK_LIBRARIES "${CMAKE_THREAD_LIBS_INIT}")
add_dependencies(gmock-main gtest-proj)

# ------------------------------------------------------------------------------
add_subdirectory(src)
add_subdirectory(tests)
