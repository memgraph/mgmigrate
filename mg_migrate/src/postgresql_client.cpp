#include "postgresql_client.hpp"

#include <memory>
#include <optional>
#include <sstream>
#include <vector>

#include <glog/logging.h>

#include "mgclient/value.hpp"

namespace {

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
