#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "memgraph_client.hpp"
#include "memgraph_destination.hpp"
#include "memgraph_source.hpp"
#include "postgresql.hpp"

const char *kUsage =
    "A tool that imports data to the destination Memgraph from the given "
    "source database.";

DEFINE_string(source_kind, "memgraph",
              "The kind of the given server. Supported options are 'memgraph' "
              "and 'postgresql'.");
DEFINE_string(source_host, "127.0.0.1",
              "Server address of the source database. It can be a DNS "
              "resolvable hostname.");
DEFINE_int32(source_port, 0,
             "Server port of the source database. If set to 0, default port "
             "for the given source kind will be used, i.e. 7687 for Memgraph "
             "database and 5432 for PostgreSQL database.");
DEFINE_string(source_username, "", "Username for the source database");
DEFINE_string(source_password, "", "Password for the source database");
DEFINE_bool(source_use_ssl, true,
            "Use SSL when connecting to the source database.");
DEFINE_string(source_database, "",
              "Database name. Applicable to PostgreSQL source.");

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
  source->ReadNodes([&destination](const auto &node) {
    std::set<std::string> label_set;
    for (const auto &label : node.labels()) {
      label_set.emplace(label);
    }
    destination->CreateNode(mg::Value(node.id().AsInt()).AsConstValue(),
                            label_set, node.properties());
  });

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

void MigratePostgresqlDatabase(PostgresqlSource *source,
                               MemgraphDestination *destination) {
  // Get list of tables.
  auto tables = source->GetTables();

  // Counter used as unique identifier of migrated nodes.
  // TODO(tsabolcec): Come up with better approach for identifying migrated
  // nodes.
  int64_t next_node_id = 0;

  // Migrate rows of tables as nodes.
  for (const auto &table : tables) {
    source->ReadTable(table, [&destination, &next_node_id,
                              &table](const std::vector<mg::Value> &row) {
      // Row is converted to node by labeling a node by table name, and
      // constructing properties as list of (column name, column value) pairs.
      mg::Map properties(row.size());
      for (size_t i = 0; i < row.size(); ++i) {
        properties.InsertUnsafe(table.columns[i], row[i]);
      }
      destination->CreateNode(mg::Value(next_node_id++).AsConstValue(),
                              {table.name}, properties.AsConstMap());
    });
  }
}

uint16_t GetSourcePort(int port, const std::string &kind) {
  if (port == 0 && kind == "memgraph") {
    return 7687;
  }
  if (port == 0 && kind == "postgresql") {
    return 5432;
  }
  return port;
}

int main(int argc, char **argv) {
  gflags::SetUsageMessage(kUsage);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto source_port = GetSourcePort(FLAGS_source_port, FLAGS_source_kind);

  // TODO(tsabolcec): Implement better validation for IP addresses.
  CHECK(FLAGS_source_host != "" && source_port != 0)
      << "Please specify a valid server address and port for the source "
         "database.";

  CHECK(!DoEndpointsMatch(FLAGS_source_host, source_port,
                          FLAGS_destination_host, FLAGS_destination_port))
      << "The source and destination endpoints match. Use two "
         "different endpoints.";

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

  if (FLAGS_source_kind == "memgraph") {
    // Create a connection to the source database.
    auto source_db =
        MemgraphClientConnection::Connect({.host = FLAGS_source_host,
                                           .port = source_port,
                                           .username = FLAGS_source_username,
                                           .password = FLAGS_source_password,
                                           .use_ssl = FLAGS_source_use_ssl});

    CHECK(source_db) << "Couldn't connect to the source database.";

    MemgraphSource source(std::move(source_db));
    MigrateMemgraphDatabase(&source, &destination);
    return 0;
  }

  if (FLAGS_source_kind == "postgresql") {
    CHECK(FLAGS_source_database != "")
        << "Please specify a PostgreSQL database name!";

    auto source_db =
        PostgresqlClient::Connect({.host = FLAGS_source_host,
                                   .port = source_port,
                                   .username = FLAGS_source_username,
                                   .password = FLAGS_source_password,
                                   .database = FLAGS_source_database});

    CHECK(source_db) << "Couldn't connect to the source database.";

    PostgresqlSource source(std::move(source_db));
    MigratePostgresqlDatabase(&source, &destination);
  }

  std::cerr << "Unknown source kind '" << FLAGS_source_kind
            << "'. Please run 'mg_migrate --help' to see options.";

  return 0;
}
