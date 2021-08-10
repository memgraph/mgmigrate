set(MG_MIGRATE_SOURCE_ROOT ${PROJECT_SOURCE_DIR}/src)

include_directories(
  ${GFLAGS_INCLUDE_DIR}
  ${GLOG_INCLUDE_DIR}
  ${MG_CLIENT_INCLUDE_DIR}
  ${MG_MIGRATE_SOURCE_ROOT})

set(MG_MIGRATE_LIB_SOURCES
  memgraph_destination.cpp
  source/memgraph.cpp
  source/postgresql.cpp
  source/mysql.cpp
  source/schema_info.cpp)

add_compile_options(-Wall -Wextra -Wredundant-move)

add_library(mgmigrate-lib ${MG_MIGRATE_LIB_SOURCES})
target_link_libraries(mgmigrate-lib mgclient pqxx pq mysqlconnector glog)
if(MGMIGRATE_ON_WINDOWS)
  target_link_libraries(mgmigrate-lib ws2_32 dnsapi)
else()
  target_link_libraries(mgmigrate-lib resolv)
endif()
target_compile_definitions(mgmigrate-lib PUBLIC MGCLIENT_STATIC_DEFINE)

add_executable(mgmigrate main.cpp)
target_link_libraries(mgmigrate mgmigrate-lib gflags glog)
if(MGMIGRATE_ON_WINDOWS)
  target_link_libraries(mgmigrate shlwapi)
endif()

install(TARGETS mgmigrate
        RUNTIME DESTINATION bin)

if(MGMIGRATE_ON_WINDOWS)
  install(FILES ${PostgreSQL_LIBRARY_DIRS}/libpq.dll DESTINATION bin)
endif()