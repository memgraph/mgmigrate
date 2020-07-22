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
      "SELECT table_name FROM information_schema.tables WHERE table_schema='" +
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

std::vector<PostgresqlSource::Table> PostgresqlSource::GetTables() {
  std::vector<std::string> table_names = ListAllTables(client_.get());
  std::vector<Table> tables;
  tables.reserve(table_names.size());
  for (const auto &table_name : table_names) {
    tables.push_back(
        {table_name, ListColumnsForTable(client_.get(), table_name)});
  }
  return tables;
}

void PostgresqlSource::ReadTable(
    const PostgresqlSource::Table &table,
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
