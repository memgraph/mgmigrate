#include "source/postgresql.hpp"

#include <memory>
#include <optional>
#include <sstream>
#include <stack>
#include <string_view>
#include <vector>

#include <glog/logging.h>

#include "utils/algorithm.hpp"

namespace {

/// SQL list of schema names that shouldn't be migrated.
const std::string kSchemaBlacklist = "('information_schema', 'pg_catalog')";

/// A helper function which parses SQL string array (possibly multidimensional)
/// into a single `mg::Value` type. `conversion` lambda is used to convert a
/// single string element of an array into a `mg::Value` type.
mg::Value ParseArray(
    const pqxx::field &field,
    std::function<mg::Value(const std::string &element)> conversion) {
  // Array can be multidimensional, so we use stack to parse it. The first
  // `std::vector` in the stack will be used to store the final result.
  std::stack<std::vector<mg::Value>> stack;
  stack.push(std::vector<mg::Value>());

  auto parser = field.as_array();
  for (auto item = parser.get_next();
       item.first != pqxx::array_parser::juncture::done;
       item = parser.get_next()) {
    switch (item.first) {
      case pqxx::array_parser::juncture::row_start: {
        stack.push(std::vector<mg::Value>());
        break;
      }
      case pqxx::array_parser::juncture::row_end: {
        CHECK(stack.size() >= 2) << "Unexpected row end encountered while "
                                    "parsing a PostgreSQL array!";
        mg::Value array_value(mg::List(std::move(stack.top())));
        stack.pop();
        stack.top().push_back(std::move(array_value));
        break;
      }
      case pqxx::array_parser::juncture::string_value: {
        CHECK(!stack.empty()) << "Unexpected string value encountered while "
                                 "parsing a PostgreSQL array!";
        stack.top().push_back(conversion(item.second));
        break;
      }
      case pqxx::array_parser::juncture::null_value: {
        CHECK(!stack.empty()) << "Unexpected null value encountered while "
                                 "parsing a PostgreSQL array!";
        stack.top().push_back(mg::Value());
        break;
      }
      case pqxx::array_parser::juncture::done:
        break;
    }
  }

  // At the end there should be only one `std::vector` left in the stack,
  // containing a single element.
  CHECK(stack.size() == 1 && stack.top().size() == 1)
      << "Got unexpected result while parsing a PostgreSQL array!";
  return stack.top()[0];
}

/// Converts pqxx::field to mg::Value. Types of pqxx::values are represented as
/// integers internally known to postgres server. Correct mapping can be found
/// at: https://godoc.org/github.com/lib/pq/oid. The same list can be obtained
/// by running `SELECT typname, oid FROM pg_type`.
mg::Value ConvertField(const pqxx::field &field) {
  if (field.is_null()) {
    return mg::Value();
  }
  switch (field.type()) {
    case PostgresqlOidType::kBool:
      return mg::Value(field.as<bool>());
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
    case PostgresqlOidType::kBoolArray:
      return ParseArray(field, [](const auto &el) {
        bool el_bool;
        pqxx::from_string(el, el_bool);
        return mg::Value(el_bool);
      });
    case PostgresqlOidType::kInt8Array:
    case PostgresqlOidType::kInt2Array:
    case PostgresqlOidType::kInt4Array:
      return ParseArray(field, [](const auto &el) {
        int64_t el_int;
        pqxx::from_string(el, el_int);
        return mg::Value(el_int);
      });
    case PostgresqlOidType::kFloat4Array:
    case PostgresqlOidType::kFloat8Array:
    case PostgresqlOidType::kNumericArray:
      return ParseArray(field, [](const auto &el) {
        double el_double;
        pqxx::from_string(el, el_double);
        return mg::Value(el_double);
      });
    case PostgresqlOidType::kCharArray:
    case PostgresqlOidType::kBlankPaddedCharArray:
    case PostgresqlOidType::kVarcharArray:
    case PostgresqlOidType::kTextArray:
      return ParseArray(field, [](const auto &el) { return mg::Value(el); });
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

/// Returns list of pairs, where the first element in the pair corresponds to a
/// table schema and the second to a table name.
std::vector<std::pair<std::string, std::string>> ListAllTables(
    PostgresqlClient *client) {
  const std::string statement =
      "SELECT table_schema, table_name "
      "FROM information_schema.tables "
      "WHERE table_type = 'BASE TABLE'"
      "  AND table_schema NOT IN " +
      kSchemaBlacklist;
  CHECK(client->Execute(statement)) << "Unable to list all tables!";
  std::vector<std::pair<std::string, std::string>> tables;
  std::optional<std::vector<mg::Value>> result;
  while ((result = client->FetchOne()) != std::nullopt) {
    CHECK(result->size() == 2)
        << "Received unexpected result while listing tables!";
    for (const auto &value : *result) {
      CHECK(value.type() == mg::Value::Type::String)
          << "Received unexpected result while listing tables!";
    }
    const auto &table_schema = (*result)[0].ValueString();
    const auto &table_name = (*result)[1].ValueString();
    tables.emplace_back(std::move(table_schema), std::move(table_name));
  }
  return tables;
}

std::vector<std::string> ListColumnsForTable(
    PostgresqlClient *client, const std::string_view &table_schema,
    const std::string_view &table_name) {
  const std::string statement =
      "SELECT column_name FROM information_schema.columns WHERE "
      "table_schema='" +
      client->Escape(table_schema) + "' AND table_name='" +
      client->Escape(table_name) + "';";
  CHECK(client->Execute(statement))
      << "Unable to list columns of table '" << table_name << "' in schema '"
      << table_schema << "'!";
  std::vector<std::string> columns;
  std::optional<std::vector<mg::Value>> result;
  while ((result = client->FetchOne()) != std::nullopt) {
    CHECK(result->size() == 1)
        << "Received unexpected result while listing columns of table '"
        << table_name << "' in schema '" << table_schema << "'!";
    const auto &value = (*result)[0];
    CHECK(value.type() == mg::Value::Type::String)
        << "Received unexpected result while listing columns of table '"
        << table_name << "' in schema '" << table_schema << "'!";
    columns.emplace_back(value.ValueString());
  }
  return columns;
}

std::vector<std::string> GetPrimaryKeyForTable(
    PostgresqlClient *client, const std::string_view &table_schema,
    const std::string_view &table_name) {
  const std::string statement =
      "SELECT usage.column_name FROM "
      "  information_schema.table_constraints AS constraints"
      "  JOIN information_schema.constraint_column_usage AS usage"
      "    USING (constraint_schema, constraint_name)"
      "WHERE"
      "  constraint_type = 'PRIMARY KEY'"
      "  AND constraints.table_schema = '" +
      client->Escape(table_schema) + "' AND constraints.table_name = '" +
      client->Escape(table_name) + "';";
  CHECK(client->Execute(statement))
      << "Unable to get primary key of table '" << table_name << "' in schema '"
      << table_schema << "'!";
  std::optional<std::vector<mg::Value>> result;
  std::vector<std::string> primary_key;
  while ((result = client->FetchOne()) != std::nullopt) {
    CHECK(result->size() == 1 && (*result)[0].type() == mg::Value::Type::String)
        << "Received unexpected result while trying to "
           "get primary key of table '"
        << table_name << "' in schema '" << table_schema << "'!";
    primary_key.emplace_back((*result)[0].ValueString());
  }
  return primary_key;
}

std::vector<SchemaInfo::ForeignKey> ListAllForeignKeys(
    PostgresqlClient *client, const std::vector<SchemaInfo::Table> &tables) {
  const std::string statement =
      "SELECT"
      "  constraints.constraint_name,"
      "  child.table_schema,"
      "  child.table_name,"
      "  child.column_name,"
      "  parent.table_schema,"
      "  parent.table_name,"
      "  parent.column_name "
      "FROM"
      "  information_schema.referential_constraints AS constraints"
      "  JOIN information_schema.key_column_usage AS child"
      "    USING (constraint_schema, constraint_name)"
      "  JOIN information_schema.key_column_usage AS parent"
      "    ON parent.ordinal_position = child.position_in_unique_constraint"
      "   AND parent.constraint_name = constraints.unique_constraint_name "
      "WHERE constraints.constraint_schema NOT IN " +
      kSchemaBlacklist + "  AND child.table_schema NOT IN " + kSchemaBlacklist +
      "  AND parent.table_schema NOT IN " + kSchemaBlacklist +
      " ORDER BY constraints.constraint_name, child.ordinal_position;";
  CHECK(client->Execute(statement)) << "Unable to list foreign keys!";
  std::optional<std::vector<mg::Value>> result;
  std::vector<SchemaInfo::ForeignKey> foreign_keys;
  SchemaInfo::ForeignKey current_foreign_key;
  std::string prev_foreign_key_name;
  while ((result = client->FetchOne()) != std::nullopt) {
    CHECK(result->size() == 7)
        << "Received unexpected result while listing foreign keys!";
    for (const auto &value : *result) {
      CHECK(value.type() == mg::Value::Type::String)
          << "Received unexpected result while listing foreign keys!";
    }
    const auto &foreign_key_name = (*result)[0].ValueString();
    auto child_table = GetTableIndex(tables, (*result)[1].ValueString(),
                                     (*result)[2].ValueString());
    auto child_column =
        GetColumnIndex(tables[child_table].columns, (*result)[3].ValueString());
    auto parent_table = GetTableIndex(tables, (*result)[4].ValueString(),
                                      (*result)[5].ValueString());
    auto parent_column = GetColumnIndex(tables[parent_table].columns,
                                        (*result)[6].ValueString());
    if (foreign_key_name != prev_foreign_key_name) {
      if (!current_foreign_key.child_columns.empty()) {
        foreign_keys.push_back(current_foreign_key);
        current_foreign_key.child_columns.clear();
        current_foreign_key.parent_columns.clear();
      }
      current_foreign_key.child_table = child_table;
      current_foreign_key.parent_table = parent_table;
    }
    current_foreign_key.child_columns.emplace_back(child_column);
    current_foreign_key.parent_columns.emplace_back(parent_column);
    prev_foreign_key_name = foreign_key_name;
  }
  if (!current_foreign_key.child_columns.empty()) {
    foreign_keys.push_back(current_foreign_key);
  }
  return foreign_keys;
}

std::vector<SchemaInfo::ExistenceConstraint> ListAllExistenceConstraints(
    PostgresqlClient *client, const std::vector<SchemaInfo::Table> &tables) {
  std::string statement =
      "SELECT table_schema, table_name, column_name "
      "FROM information_schema.columns "
      "WHERE is_nullable = 'NO' AND table_schema NOT IN " +
      kSchemaBlacklist + ";";
  CHECK(client->Execute(statement)) << "Unable to list existence constraints!";
  std::optional<std::vector<mg::Value>> result;
  std::vector<SchemaInfo::ExistenceConstraint> existence_constraints;
  while ((result = client->FetchOne()) != std::nullopt) {
    CHECK(result->size() == 3)
        << "Received unexpected result while listing existence constraints!";
    for (const auto &value : *result) {
      CHECK(value.type() == mg::Value::Type::String)
          << "Received unexpected result while listing existence constraints!";
    }
    const auto table = GetTableIndex(tables, (*result)[0].ValueString(),
                                     (*result)[1].ValueString());
    const auto column =
        GetColumnIndex(tables[table].columns, (*result)[2].ValueString());
    existence_constraints.emplace_back(table, column);
  }
  return existence_constraints;
}

std::vector<SchemaInfo::UniqueConstraint> ListAllUniqueConstraints(
    PostgresqlClient *client, std::vector<SchemaInfo::Table> &tables) {
  std::string statement =
      "SELECT"
      "  tc.constraint_name,"
      "  tc.table_schema,"
      "  tc.table_name,"
      "  ccu.column_name "
      "FROM"
      "  information_schema.table_constraints AS tc"
      "  JOIN information_schema.constraint_column_usage AS ccu"
      "    USING (constraint_name, table_schema) "
      "WHERE tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY') "
      "ORDER BY tc.constraint_name";
  CHECK(client->Execute(statement)) << "Unable to list unique constraints!";
  std::optional<std::vector<mg::Value>> result;
  std::vector<SchemaInfo::UniqueConstraint> constraints;
  SchemaInfo::UniqueConstraint current_constraint;
  std::string prev_constraint_name;
  while ((result = client->FetchOne()) != std::nullopt) {
    CHECK(result->size() == 4)
        << "Received unexpected result while listing unique constraints!";
    for (const auto &value : *result) {
      CHECK(value.type() == mg::Value::Type::String)
          << "Received unexpected result while listing unique constraints!";
    }
    const auto &constraint_name = (*result)[0].ValueString();
    auto table = GetTableIndex(tables, (*result)[1].ValueString(),
                               (*result)[2].ValueString());
    auto column =
        GetColumnIndex(tables[table].columns, (*result)[3].ValueString());
    if (prev_constraint_name != constraint_name) {
      if (!current_constraint.second.empty()) {
        constraints.push_back(current_constraint);
        current_constraint.second.clear();
      }
      current_constraint.first = table;
    }
    current_constraint.second.push_back(column);
    prev_constraint_name = constraint_name;
  }
  if (!current_constraint.second.empty()) {
    constraints.push_back(current_constraint);
  }
  return constraints;
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
    LOG(FATAL) << "Unable to fetch PostgreSQL result: " << e.what();
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
  std::vector<std::pair<std::string, std::string>> table_names =
      ListAllTables(client_.get());
  std::vector<SchemaInfo::Table> tables;
  tables.reserve(table_names.size());
  for (const auto &[table_schema, table_name] : table_names) {
    auto columns = ListColumnsForTable(client_.get(), table_schema, table_name);
    const auto &primary_key_columns =
        GetPrimaryKeyForTable(client_.get(), table_schema, table_name);
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
    tables.push_back({table_schema, table_name, std::move(columns),
                      std::move(primary_key), std::vector<size_t>(), false});
  }

  auto foreign_keys = ListAllForeignKeys(client_.get(), tables);
  for (size_t i = 0; i < foreign_keys.size(); ++i) {
    tables[foreign_keys[i].child_table].foreign_keys.push_back(i);
    tables[foreign_keys[i].parent_table].primary_key_referenced = true;
  }

  auto existence_constraints =
      ListAllExistenceConstraints(client_.get(), tables);
  auto unique_constraints = ListAllUniqueConstraints(client_.get(), tables);

  return {std::move(tables), std::move(foreign_keys),
          std::move(unique_constraints), std::move(existence_constraints)};
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
  statement << " FROM " << client_->EscapeName(table.schema) << "."
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
