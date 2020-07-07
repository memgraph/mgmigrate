#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "memgraph_client.hpp"
#include "memgraph_destination.hpp"
#include "memgraph_source.hpp"

const char *kUsage =
    "A tool that imports data to the destination Memgraph from the given "
    "source database.";

DEFINE_string(source_host, "",
              "Server address of the source database. It can be a DNS "
              "resolvable hostname.");
DEFINE_int32(source_port, 0, "Server port of the source database.");
DEFINE_string(source_username, "", "Username for the source database");
DEFINE_string(source_password, "", "Password for the source database");
DEFINE_bool(source_use_ssl, true,
            "Use SSL when connecting to the source database.");

DEFINE_string(destination_host, "127.0.0.1",
              "Server address of the destination database. It can be a DNS "
              "resolvable hostname.");
DEFINE_int32(destination_port, 7687,
             "Server port of the destination database.");
DEFINE_string(destination_username, "",
              "Username for the destination database.");
DEFINE_string(destination_password, "",
              "Password for the destination database.");
DEFINE_bool(destination_use_ssl, true,
            "Use SSL when connecting to the destination database.");

/// Compares if two endpoints are the same.
// TODO(tsabolcec): We should check if endpoints matches only if the source
// database is Memgraph. That check should be added once multiple databases
// are supported. Also, we should check if the endpoints matches in a better
// way, e.g. the current way doesn't make difference between 'localhost' and
// '127.0.0.1', and even after DNS resolutions there could be differences
// (e.g. IPv4 '127.0.0.1' vs.  IPv6 '::1').
bool DoEndpointsMatch(const std::string_view &host1, uint16_t port1,
                      const std::string_view &host2, uint16_t port2) {
  return host1 == host2 && port1 == port2;
}

/// Migrates data from the `source` Memgraph database to the `destination`
/// Memgraph database.
void MigrateMemgraphDatabase(MemgraphSource *source,
                             MemgraphDestination *destination) {
  // Migrate nodes.
  source->ReadNodes(
      [&destination](const auto &node) { destination->CreateNode(node); });

  // Migrate relationships.
  source->ReadRelationships([&destination](const auto &rel) {
    destination->CreateRelationship(rel);
  });

  // Migrate indices.
  const auto &index_info = source->ReadIndices();
  for (const auto &label : index_info.label) {
    destination->CreateLabelIndex(label);
  }
  for (const auto &[label, property] : index_info.label_property) {
    destination->CreateLabelPropertyIndex(label, property);
  }

  // Migrate constraints.
  const auto &constraint_info = source->ReadConstraints();
  for (const auto &[label, property] : constraint_info.existence) {
    destination->CreateExistenceConstraint(label, property);
  }
  for (const auto &[label, properties] : constraint_info.unique) {
    destination->CreateUniqueConstraint(label, properties);
  }
}

int main(int argc, char **argv) {
  gflags::SetUsageMessage(kUsage);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // TODO(tsabolcec): Implement better validation for IP addresses.
  CHECK(FLAGS_source_host != "" && FLAGS_source_port != 0)
      << "Please specify a valid server address and port for the source "
         "database.";

  CHECK(!DoEndpointsMatch(FLAGS_source_host, FLAGS_source_port,
                          FLAGS_destination_host, FLAGS_destination_port))
      << "The source and destination endpoints match. Use two "
         "different endpoints.";

  // Create a connection to the source database.
  auto source_db = MemgraphClientConnection::Connect(
      {.host = FLAGS_source_host,
       .port = static_cast<uint16_t>(FLAGS_source_port),
       .username = FLAGS_source_username,
       .password = FLAGS_source_password,
       .use_ssl = FLAGS_source_use_ssl});

  CHECK(source_db) << "Couldn't connect to the source database.";

  // Create a connection to the destination database.
  auto destination_db = MemgraphClientConnection::Connect(
      {.host = FLAGS_destination_host,
       .port = static_cast<uint16_t>(FLAGS_destination_port),
       .username = FLAGS_destination_username,
       .password = FLAGS_destination_password,
       .use_ssl = FLAGS_destination_use_ssl});

  CHECK(destination_db)
      << "Couldn't connect to the destination Memgraph database.";

  MemgraphDestination destination(std::move(destination_db));
  MemgraphSource source(std::move(source_db));

  MigrateMemgraphDatabase(&source, &destination);

  return 0;
}
