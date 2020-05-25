set(MG_MIGRATE_SOURCE_ROOT ${PROJECT_SOURCE_DIR}/src)

set(UNIT_TEST_PREFIX mg_migrate__unit__)

add_executable(${UNIT_TEST_PREFIX}mgclient_value mgclient_value.cpp)
target_link_libraries(${UNIT_TEST_PREFIX}mgclient_value gtest gtest-main mgmigrate-mgclient glog gflags)
target_include_directories(${UNIT_TEST_PREFIX}mgclient_value PRIVATE
  ${GTEST_INCLUDE_DIR}
  ${MG_CLIENT_INCLUDE_DIR}
  ${MG_MIGRATE_SOURCE_ROOT})
set_target_properties(${UNIT_TEST_PREFIX}mgclient_value PROPERTIES OUTPUT_NAME mgclient_value)

add_executable(${UNIT_TEST_PREFIX}memgraph_destination memgraph_destination.cpp)
target_link_libraries(${UNIT_TEST_PREFIX}memgraph_destination gtest gtest-main mgmigrate-lib glog gflags)
target_include_directories(${UNIT_TEST_PREFIX}memgraph_destination PRIVATE
  ${GTEST_INCLUDE_DIR}
  ${MG_CLIENT_INCLUDE_DIR}
  ${MG_MIGRATE_SOURCE_ROOT})
set_target_properties(${UNIT_TEST_PREFIX}memgraph_destination PROPERTIES OUTPUT_NAME memgraph_destination)
