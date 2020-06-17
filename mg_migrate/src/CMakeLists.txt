set(MG_MIGRATE_SOURCE_ROOT ${PROJECT_SOURCE_DIR}/src)

include_directories(
  ${GFLAGS_INCLUDE_DIR}
  ${GLOG_INCLUDE_DIR}
  ${MG_CLIENT_INCLUDE_DIR}
  ${MG_MIGRATE_SOURCE_ROOT})

set(MG_MIGRATE_LIB_SOURCES
  memgraph_destination.cpp
  memgraph_source.cpp)

add_compile_options(-Wall -Wextra -Wredundant-move)

add_subdirectory(mgclient)

add_library(mgmigrate-lib ${MG_MIGRATE_LIB_SOURCES})
target_link_libraries(mgmigrate-lib glog mgmigrate-mgclient)

add_executable(mg_migrate main.cpp)
target_link_libraries(mg_migrate gflags glog mgmigrate-lib)


