#include "postgresql.hpp"

#include <memory>
#include <optional>
#include <sstream>
#include <string_view>
#include <vector>

#include <glog/logging.h>

#include "mgclient/value.hpp"
#include "utils/algorithm.hpp"

namespace {

// TODO(tsabolcec): Make this configurable from command line.
const char *kDefaultScheme = "public";

/// Converts pqxx::field to mg::Value. Types of pqxx::values are represented as
/// integers internally known to postgres server. Correct mapping can be found
/// at: https://godoc.org/github.com/lib/pq/oid. The same list can be obtained
/// by running `SELECT typname, oid FROM pg_type`.
mg::Value ConvertField(const pqxx::field &field) {
  switch (field.type()) {
    case PostgresqlOidType::kBool:
      return mg::Value(field.as<bool>());
    case PostgresqlOidType::kByte:
    case PostgresqlOidType::kInt8:
    case PostgresqlOidType::kInt2:
    case PostgresqlOidType::kInt4:
      return mg::Value(field.as<int64_t>());
    case PostgresqlOidType::kChar:
    case PostgresqlOidType::kText:
    case PostgresqlOidType::kVarchar:
      return mg::Value(field.as<std::string>());
    case PostgresqlOidType::kFloat4:
    case PostgresqlOidType::kFloat8:
    case PostgresqlOidType::kNumeric:
      return mg::Value(field.as<double>());
      // TODO(tsabolcec): Implement conversion of lists and maps (JSON) as well.
  }
  // Most values are readable in string format:
  return mg::Value(field.as<std::string>());
}

std::vector<mg::Value> ConvertRow(const pqxx::row &row) {
  std::vector<mg::Value> values;
  values.reserve(row.size());
  for (const auto &field : row) {
    values.push_back(ConvertField(field));
  }
  return values;
}

std::vector<std::string> ListAllTables(PostgresqlClient *client) {
  const std::string statement =
      "SELECT table_name FROM information_schema.tables "
      "WHERE table_type = 'BASE TABLE' AND table_schema='" +
      client->Escape(kDefaultScheme) + "';";
  CHECK(client->Execute(statement)) << "Unable to list all tables!";
  std::vector<std::string> tables;
  std::optional<std::vector<mg::Value>> result;
  while ((result = client->FetchOne()) != std::nullopt) {
    CHECK(result->size() == 1)
        << "Received unexpected result while listing tables in schema '"
        << kDefaultScheme << "'!";
    const auto &value = (*result)[0];
    CHECK(value.type() == mg::Value::Type::String)
        << "Received unexpected result while listing tables in schema '"
        << kDefaultScheme << "'!";
    tables.emplace_back(value.ValueString());
  }
  return tables;
}

size_t GetTableIndex(const std::vector<SchemaInfo::Table> &tables,
                     const std::string_view &table_name) {
  for (size_t i = 0; i < tables.size(); ++i) {
    if (tables[i].name == table_name) {
      return i;
    }
  }
  CHECK(false) << "Couldn't find table name '" << table_name << "'!";
  return 0;
}

size_t GetColumnIndex(const std::vector<std::string> &columns,
                      const std::string_view &column_name) {
  auto it = std::find(columns.begin(), columns.end(), column_name);
  CHECK(it != columns.end())
      << "Couldn't find column name '" << column_name << "'!";
  return it - columns.begin();
}

std::vector<std::string> ListColumnsForTable(PostgresqlClient *client,
                                             const std::string_view &table) {
  const std::string statement =
      "SELECT column_name FROM information_schema.columns WHERE "
      "table_schema='" +
      client->Escape(kDefaultScheme) + "' AND table_name='" +
      client->Escape(table) + "';";
  CHECK(client->Execute(statement))
      << "Unable to list columns of table '" << table << "'!";
  std::vector<std::string> columns;
  std::optional<std::vector<mg::Value>> result;
  while ((result = client->FetchOne()) != std::nullopt) {
    CHECK(result->size() == 1)
        << "Received unexpected result while listing columns of table '"
        << table << "'!";
    const auto &value = (*result)[0];
    CHECK(value.type() == mg::Value::Type::String)
        << "Received unexpected result while listing columns of table '"
        << table << "'!";
    columns.emplace_back(value.ValueString());
  }
  return columns;
}

std::vector<std::string> GetPrimaryKeyForTable(PostgresqlClient *client,
                                               const std::string_view &table) {
  const std::string statement =
      "SELECT usage.column_name FROM "
      "  information_schema.table_constraints AS constraints"
      "  JOIN information_schema.constraint_column_usage AS usage"
      "    USING (constraint_schema, constraint_name)"
      "WHERE"
      "  constraint_type = 'PRIMARY KEY'"
      "  AND constraint_schema = '" +
      client->Escape(kDefaultScheme) + "'" +
      "  AND constraints.table_name = '" + client->Escape(table) + "';";
  CHECK(client->Execute(statement))
      << "Unable to get primary key of table '" << table << "'!";
  std::optional<std::vector<mg::Value>> result;
  std::vector<std::string> primary_key;
  while ((result = client->FetchOne()) != std::nullopt) {
    CHECK(result->size() == 1 && (*result)[0].type() == mg::Value::Type::String)
        << "Received unexpected result while trying to "
           "get primary key of table '"
        << table << "'!";
    primary_key.emplace_back((*result)[0].ValueString());
  }
  return primary_key;
}

std::vector<SchemaInfo::ForeignKey> ListAllForeignKeys(
    PostgresqlClient *client, const std::vector<SchemaInfo::Table> &tables) {
  const std::string statement =
      "SELECT"
      "  constraints.constraint_name,"
      "  child.table_name,"
      "  child.column_name,"
      "  parent.table_name,"
      "  parent.column_name "
      "FROM"
      "  information_schema.referential_constraints AS constraints"
      "  JOIN information_schema.key_column_usage AS child"
      "    USING (constraint_schema, constraint_name)"
      "  JOIN information_schema.key_column_usage AS parent"
      "    ON parent.ordinal_position = child.position_in_unique_constraint"
      "   AND parent.table_schema = child.table_schema"
      "   AND parent.constraint_name = constraints.unique_constraint_name "
      "WHERE constraints.constraint_schema = '" +
      client->Escape(kDefaultScheme) +
      "' "
      "ORDER BY constraints.constraint_name, child.ordinal_position;";
  CHECK(client->Execute(statement)) << "Unable to list foreign keys!";
  std::optional<std::vector<mg::Value>> result;
  std::vector<SchemaInfo::ForeignKey> foreign_keys;
  SchemaInfo::ForeignKey current_foreign_key;
  while ((result = client->FetchOne()) != std::nullopt) {
    CHECK(result->size() == 5)
        << "Received unexpected result while listing foreign keys!";
    for (const auto &value : *result) {
      CHECK(value.type() == mg::Value::Type::String)
          << "Received unexpected result while listing foreign keys!";
    }
    const auto &foreign_key_name = (*result)[0].ValueString();
    auto child_table = GetTableIndex(tables, (*result)[1].ValueString());
    auto child_column =
        GetColumnIndex(tables[child_table].columns, (*result)[2].ValueString());
    auto parent_table = GetTableIndex(tables, (*result)[3].ValueString());
    auto parent_column = GetColumnIndex(tables[parent_table].columns,
                                        (*result)[4].ValueString());
    if (foreign_key_name != current_foreign_key.name) {
      if (!current_foreign_key.child_columns.empty()) {
        foreign_keys.push_back(current_foreign_key);
        current_foreign_key.child_columns.clear();
        current_foreign_key.parent_columns.clear();
      }
      current_foreign_key.name = foreign_key_name;
      current_foreign_key.child_table = child_table;
      current_foreign_key.parent_table = parent_table;
    }
    current_foreign_key.child_columns.emplace_back(child_column);
    current_foreign_key.parent_columns.emplace_back(parent_column);
  }
  if (!current_foreign_key.child_columns.empty()) {
    foreign_keys.push_back(current_foreign_key);
  }
  return foreign_keys;
}

}  // namespace

bool PostgresqlClient::Execute(const std::string &statement) {
  /// If there's an active execution going on, stop.
  if (cursor_) {
    return false;
  }
  try {
    work_.emplace(*connection_);
    cursor_.emplace(*work_, statement, "cursor_mg_migrate");
  } catch (const pqxx::internal_error &e) {
    LOG(ERROR) << "Unable to execute PostgreSQL query '" << statement
               << "': " << e.what();
    return false;
  } catch (const pqxx::usage_error &e) {
    LOG(ERROR) << "Unable to execute PostgreSQL query '" << statement
               << "': " << e.what();
    return false;
  }
  return true;
}

std::optional<std::vector<mg::Value>> PostgresqlClient::FetchOne() {
  if (!cursor_) {
    return std::nullopt;
  }
  try {
    pqxx::result result;
    *cursor_ >> result;
    CHECK(result.size() <= 1) << "Unexpected number of rows received!";
    if (result.size() == 1) {
      return ConvertRow(result[0]);
    } else {
      // The end of result is reached.
      cursor_ = std::nullopt;
      work_ = std::nullopt;
      return std::nullopt;
    }
  } catch (const pqxx::sql_error &e) {
    CHECK(false) << "Unable to fetch PostgreSQL result: " << e.what();
  }
}

std::unique_ptr<PostgresqlClient> PostgresqlClient::Connect(
    const PostgresqlClient::Params &params) {
  std::ostringstream stream;
  stream << "postgresql://" << params.username << ":" << params.password << "@"
         << params.host << ":" << params.port << "/" << params.database;
  try {
    auto connection = std::make_unique<pqxx::connection>(stream.str());
    if (!connection->is_open()) {
      return nullptr;
    }
    return std::unique_ptr<PostgresqlClient>(
        new PostgresqlClient(std::move(connection)));
  } catch (const pqxx::broken_connection &e) {
    LOG(ERROR) << "Unable to connect to PostgreSQL server: " << e.what();
  }
  return nullptr;
}

PostgresqlSource::PostgresqlSource(std::unique_ptr<PostgresqlClient> client)
    : client_(std::move(client)) {}

PostgresqlSource::~PostgresqlSource() {}

SchemaInfo PostgresqlSource::GetSchemaInfo() {
  std::vector<std::string> table_names = ListAllTables(client_.get());
  std::vector<SchemaInfo::Table> tables;
  tables.reserve(table_names.size());
  for (const auto &table_name : table_names) {
    auto columns = ListColumnsForTable(client_.get(), table_name);
    const auto &primary_key_columns =
        GetPrimaryKeyForTable(client_.get(), table_name);
    std::vector<size_t> primary_key;
    primary_key.reserve(primary_key_columns.size());
    for (const auto &column_name : primary_key_columns) {
      auto it = std::find(columns.begin(), columns.end(), column_name);
      CHECK(it != columns.end())
          << "Couldn't find a primary key field '" << column_name
          << "' in the table '" << table_name << "'!";
      primary_key.push_back(it - columns.begin());
    }
    // List of foreign keys of the current table is currently left empty.
    tables.push_back({table_name, std::move(columns), std::move(primary_key),
                      std::vector<size_t>(), false});
  }

  auto foreign_keys = ListAllForeignKeys(client_.get(), tables);
  for (size_t i = 0; i < foreign_keys.size(); ++i) {
    tables[foreign_keys[i].child_table].foreign_keys.push_back(i);
    tables[foreign_keys[i].parent_table].primary_key_referenced = true;
  }

  return {std::move(tables), std::move(foreign_keys)};
}

void PostgresqlSource::ReadTable(
    const SchemaInfo::Table &table,
    std::function<void(const std::vector<mg::Value> &)> callback) {
  std::ostringstream statement;
  statement << "SELECT ";
  utils::PrintIterable(statement, table.columns, ", ",
                       [this](auto &os, const auto &column) {
                         os << client_->EscapeName(column);
                       });
  statement << " FROM " << client_->EscapeName(kDefaultScheme) << "."
            << client_->EscapeName(table.name) << ";";
  CHECK(client_->Execute(statement.str()))
      << "Unable to read table '" << table.name << "'!";
  std::optional<std::vector<mg::Value>> result;
  while ((result = client_->FetchOne()) != std::nullopt) {
    CHECK(result->size() == table.columns.size())
        << "Received unexpected result while reading table '" << table.name
        << "'!";
    callback(*result);
  }
}
