#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "memgraph_client.hpp"
#include "memgraph_destination.hpp"
#include "source/memgraph.hpp"
#include "source/postgresql.hpp"
#include "source/mysql.hpp"
#include "utils/algorithm.hpp"

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
                             MemgraphClient *destination) {
  const char *internal_node_label = "__mg_vertex__";
  const char *internal_property_id = "__mg_id__";
  // Migrate nodes.
  source->ReadNodes([&destination, &internal_node_label,
                     &internal_property_id](const auto &node) {
    std::set<std::string> label_set;
    label_set.emplace(internal_node_label);
    for (const auto &label : node.labels()) {
      label_set.emplace(label);
    }
    mg::Map properties(node.properties().size() + 1);
    properties.InsertUnsafe(internal_property_id, mg::Value(node.id().AsInt()));
    for (const auto &[key, value] : node.properties()) {
      properties.InsertUnsafe(key, value);
    }
    CreateNode(destination, label_set, properties.AsConstMap());
  });

  // Create internal label+id index.
  CreateLabelPropertyIndex(destination, internal_node_label,
                           internal_property_id);

  // Migrate relationships.
  source->ReadRelationships([&destination, &internal_node_label,
                             &internal_property_id](const auto &rel) {
    mg::Map id1(1);
    mg::Map id2(1);
    id1.InsertUnsafe(internal_property_id, mg::Value(rel.from().AsInt()));
    id2.InsertUnsafe(internal_property_id, mg::Value(rel.to().AsInt()));
    CHECK(CreateRelationships(destination, internal_node_label,
                              id1.AsConstMap(), internal_node_label,
                              id2.AsConstMap(), rel.type(),
                              rel.properties()) == 1)
        << "Unexpected number of relationships created!";
  });

  // Migrate indices.
  const auto &index_info = source->ReadIndices();
  for (const auto &label : index_info.label) {
    CreateLabelIndex(destination, label);
  }
  for (const auto &[label, property] : index_info.label_property) {
    CreateLabelPropertyIndex(destination, label, property);
  }

  // Migrate constraints.
  const auto &constraint_info = source->ReadConstraints();
  for (const auto &[label, property] : constraint_info.existence) {
    CreateExistenceConstraint(destination, label, property);
  }
  for (const auto &[label, properties] : constraint_info.unique) {
    CreateUniqueConstraint(destination, label, properties);
  }

  // Remove internal labels, properties and indices.
  DropLabelPropertyIndex(destination, internal_node_label,
                         internal_property_id);
  RemoveLabelFromNodes(destination, internal_node_label);
  RemovePropertyFromNodes(destination, internal_property_id);
}

/// Helper function that, given the `table`, result `row` and list of
/// `positions`, returns a subset of result columns as a map.
mg::Map ExtractProperties(const SchemaInfo::Table &table,
                          const std::vector<mg::Value> &row,
                          const std::vector<size_t> &positions) {
  CHECK(table.columns.size() == row.size())
      << "Result size doesn't match column size of the table!";
  mg::Map properties(positions.size());
  for (const auto &pos : positions) {
    CHECK(pos < row.size())
        << "Couldn't access result for the given column (index out of bounds)!";
    properties.InsertUnsafe(table.columns[pos], row[pos]);
  }
  return properties;
}

/// Helper function that returns map of properties of foreign key columns that
/// can be used to match corresponding row of the parent table.
mg::Map ConstructForeignKeyMatcher(const SchemaInfo &schema,
                                   const SchemaInfo::ForeignKey &foreign_key,
                                   const std::vector<mg::Value> &row) {
  mg::Map properties(foreign_key.child_columns.size());
  for (size_t i = 0; i < foreign_key.child_columns.size(); ++i) {
    const auto child_pos = foreign_key.child_columns[i];
    const auto parent_pos = foreign_key.parent_columns[i];
    const auto &parent_table = schema.tables[foreign_key.parent_table];
    CHECK(child_pos < row.size());
    CHECK(parent_pos < parent_table.columns.size());
    properties.InsertUnsafe(parent_table.columns[parent_pos], row[child_pos]);
  }
  return properties;
}

/// Helper function that checks whether the `properties` corresponds to a well
/// defined foreign key (which doesn't contain any null values).
bool IsForeignKeyMatcherWellDefined(const mg::ConstMap &properties) {
  for (const auto &[_, value] : properties) {
    if (value.type() == mg::Value::Type::Null) {
      return false;
    }
  }
  return true;
}

/// A relationship table consists of exactly two foreign keys and there exists
/// a foreign key referencing the table's primary key.
bool IsTableRelationship(const SchemaInfo::Table &table) {
  return table.foreign_keys.size() == 2 && !table.primary_key_referenced;
}

/// Helper function that returns table name in the format which will be used for
/// label and edge type naming.
std::string GetTableName(const SchemaInfo::Table &table) {
  // Most used schema is 'public'. In that case, just return the table name.
  if (table.schema == "public") {
    return table.name;
  }
  return table.schema + "_" + table.name;
}

template<typename Source>
void MigrateSqlDatabase(Source* source,
                        MemgraphClient *destination) {
  // Get SQL schema info.
  auto schema = source->GetSchemaInfo();

  DLOG(INFO) << "Migrating rows";
  // Migrate rows of tables as nodes.
  for (const auto &table : schema.tables) {
    // If the table has exactly two foreign keys, it's better to represent it
    // as a relationship instead of a node.
    if (IsTableRelationship(table)) {
      continue;
    }
    source->ReadTable(table, [&destination,
                              &table](const std::vector<mg::Value> &row) {
      // Row is converted to node by labeling a node by table name, and
      // constructing properties as list of (column name, column value)
      // pairs.
      mg::Map properties(row.size());
      for (size_t i = 0; i < row.size(); ++i) {
        properties.InsertUnsafe(table.columns[i], row[i]);
      }
      CreateNode(destination, {GetTableName(table)}, properties.AsConstMap());
    });
    if (!table.primary_key.empty()) {
      // Create index for fast node matching. Memgraph doesn't support multiple
      // properties for a single index, so we'll create index over only one
      // primary index field.
      // TODO: If Memgraph supports this feature in the future, create index
      // over all primary key fields.
      CreateLabelPropertyIndex(destination, GetTableName(table),
                               table.columns[table.primary_key[0]]);
    } else {
      CreateLabelIndex(destination, GetTableName(table));
    }
  }

  DLOG(INFO) << "Migrating edges";
  // Migrate edges using foreign keys.
  for (const auto &table : schema.tables) {
    if (table.foreign_keys.empty()) {
      continue;
    }
    if (IsTableRelationship(table)) {
      source->ReadTable(table, [&destination, &schema,
                                &table](const auto &row) {
        const auto &foreign_key1 = schema.foreign_keys[table.foreign_keys[0]];
        const auto &foreign_key2 = schema.foreign_keys[table.foreign_keys[1]];
        const auto id1(
            std::move(ConstructForeignKeyMatcher(schema, foreign_key1, row)));
        const auto id2(
            std::move(ConstructForeignKeyMatcher(schema, foreign_key2, row)));
        if (IsForeignKeyMatcherWellDefined(id1.AsConstMap()) &&
            IsForeignKeyMatcherWellDefined(id2.AsConstMap())) {
          const auto &label1 =
              GetTableName(schema.tables[foreign_key1.parent_table]);
          const auto &label2 =
              GetTableName(schema.tables[foreign_key2.parent_table]);
          const auto &edge_type = GetTableName(table);
          mg::Map properties(row.size());
          for (size_t i = 0; i < row.size(); ++i) {
            if (!utils::Contains(foreign_key1.child_columns, i) &&
                !utils::Contains(foreign_key2.child_columns, i)) {
              properties.InsertUnsafe(table.columns[i], row[i]);
            }
          }
          CHECK(CreateRelationships(destination, label1, id1.AsConstMap(),
                                    label2, id2.AsConstMap(), edge_type,
                                    properties.AsConstMap()) == 1)
              << "Unexpected number of relationships created!";
        }
      });
    } else {
      source->ReadTable(
          table, [&destination, &schema, &table](const auto &row) {
            const auto &label1 = GetTableName(table);
            mg::Map id1(row.size());
            if (!table.primary_key.empty()) {
              for (const auto pos : table.primary_key) {
                id1.InsertUnsafe(table.columns[pos], row[pos]);
              }
            } else {
              // If there is no primary key, use all columns to match a node.
              for (size_t i = 0; i < row.size(); ++i) {
                id1.InsertUnsafe(table.columns[i], row[i]);
              }
            }
            for (const auto &fk_pos : table.foreign_keys) {
              const auto &foreign_key = schema.foreign_keys[fk_pos];
              const auto id2(std::move(
                  ConstructForeignKeyMatcher(schema, foreign_key, row)));
              if (IsForeignKeyMatcherWellDefined(id2.AsConstMap())) {
                const auto &label2 =
                    GetTableName(schema.tables[foreign_key.parent_table]);
                const std::string edge_type = label1 + "_to_" + label2;
                // If there is no primary key, use `MERGE` instead of `CREATE`
                // to prevent creating duplicate relationships.
                const bool use_merge = table.primary_key.empty();
                const auto rels_created = CreateRelationships(
                    destination, label1, id1.AsConstMap(), label2,
                    id2.AsConstMap(), edge_type,
                    mg::Map(static_cast<size_t>(0)).AsConstMap(), use_merge);
                if (!table.primary_key.empty()) {
                  CHECK(rels_created == 1)
                      << "Unexpected number of relationships created!";
                }
              }
            }
          });
    }
  }

  // Cleanup internally created indices.
  for (const auto &table : schema.tables) {
    if (!table.primary_key.empty()) {
      DropLabelPropertyIndex(destination, GetTableName(table),
                             table.columns[table.primary_key[0]]);
    } else {
      DropLabelIndex(destination, GetTableName(table));
    }
  }

  DLOG(INFO) << "Migrating existence constraints";
  // Migrate constraints.
  for (const auto &constraint : schema.existence_constraints) {
    const auto &table = schema.tables[constraint.first];
    if (IsTableRelationship(table)) {
      continue;
    }
    const auto &label = GetTableName(table);
    const auto &property = table.columns[constraint.second];
    CreateExistenceConstraint(destination, label, property);
  }
  DLOG(INFO) << "Migrating unique constraints";
  for (const auto &constraint : schema.unique_constraints) {
    const auto &table = schema.tables[constraint.first];
    if (IsTableRelationship(table)) {
      continue;
    }
    const auto &label = GetTableName(table);
    std::set<std::string> properties;
    for (const auto &column_pos : constraint.second) {
      properties.insert(table.columns[column_pos]);
    }
    CreateUniqueConstraint(destination, label, properties);
  }
}

uint16_t GetSourcePort(int port, const std::string &kind) {
  if (port == 0 ) {
    // Return default ports
    if (kind == "memgraph") {
      return 7687;
    }
    if (kind == "postgresql") {
      return 5432;
    }
    if (kind == "mysql") {
      return 3306;
    }
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
    MigrateMemgraphDatabase(&source, destination_db.get());
  } else if (FLAGS_source_kind == "postgresql") {
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
    MigrateSqlDatabase(&source, destination_db.get());
  } else if (FLAGS_source_kind == "mysql" ) {
    CHECK(FLAGS_source_database != "")
        << "Please specify a MySQL database name!";

    auto source_db =
        MysqlClient::Connect({.host = FLAGS_source_host,
                              .port = source_port,
                              .username = FLAGS_source_username,
                              .password = FLAGS_source_password,
                              .database = FLAGS_source_database});
    CHECK(source_db) << "Couldn't connect to the source database.";

    MysqlSource source(std::move(source_db));
    MigrateSqlDatabase(&source, destination_db.get());
  } else {
    std::cerr << "Unknown source kind '" << FLAGS_source_kind
              << "'. Please run 'mg_migrate --help' to see options.";
  }

  return 0;
}
