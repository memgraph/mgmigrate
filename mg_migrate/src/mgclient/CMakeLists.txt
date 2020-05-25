set(MG_MIGRATE_MGCLIENT_SOURCES
  client.cpp
  value.cpp)

add_library(mgmigrate-mgclient STATIC ${MG_MIGRATE_MGCLIENT_SOURCES})
target_link_libraries(mgmigrate-mgclient PUBLIC glog mgclient)
