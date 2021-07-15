set(MG_MIGRATE_SOURCE_ROOT ${PROJECT_SOURCE_DIR}/src)

set(UNIT_TEST_PREFIX mg_migrate__unit__)

add_executable(${UNIT_TEST_PREFIX}mgclient_value mgclient_value.cpp)
target_link_libraries(${UNIT_TEST_PREFIX}mgclient_value gtest gtest-main mgclient glog gflags)
target_include_directories(${UNIT_TEST_PREFIX}mgclient_value PRIVATE
  ${GTEST_INCLUDE_DIR}
  ${MG_CLIENT_INCLUDE_DIR}
  ${MG_MIGRATE_SOURCE_ROOT})
set_target_properties(${UNIT_TEST_PREFIX}mgclient_value PROPERTIES OUTPUT_NAME mgclient_value)
