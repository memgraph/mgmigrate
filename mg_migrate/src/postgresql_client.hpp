#pragma once

#include <memory>
#include <optional>
#include <vector>

#include <pqxx/pqxx>

#include "mgclient/value.hpp"

/// Name mapping for PostgreSQL object identifier types (OID). These values are
/// internally used by PostgreSQL server and the same list can be obtained by
/// running `SELECT oid, typname FROM pg_type`.
/// Note that the list contains only relevant types and it should be expanded on
/// introduction of new OID types.
// TODO(tsabolcec): This list can be found in the 'server/catalog/pg_type.h'
// file of `postgresql-server-dev-all` package. Due to difficulties encountered
// while integrating the library into this project, it was decided to use this
// enum instead, but we should clean this up once we start releasing the tool.
// Using `enum` instead of `enum class` for implicit conversion to int.
enum PostgresqlOidType {
  kBool = 16,
  kByte = 17,
  kChar = 18,
  kInt8 = 20,
  kInt2 = 21,
  kInt4 = 23,
  kText = 25,
  kFloat4 = 700,
  kFloat8 = 701,
  kVarchar = 1043,
  kNumeric = 1700,
};

/// Client which executes queries on PostgreSQL server.
class PostgresqlClient {
 public:
  struct Params {
    std::string host;
    uint16_t port;
    std::string username;
    std::string password;
    std::string database;
  };

  PostgresqlClient(const PostgresqlClient &) = delete;
  PostgresqlClient(PostgresqlClient &&) = delete;
  PostgresqlClient &operator=(const PostgresqlClient &) = delete;
  PostgresqlClient &operator=(PostgresqlClient &&) = delete;
  ~PostgresqlClient() {}

  /// Executes the given PostgreSQL `statement`.
  /// Returns true when the statement is successfully executed, false otherwise.
  /// After executing the statement, the method is blocked until all incoming
  /// data (execution results) are handled, i.e. until the `FetchOne` method
  /// returns `std::nullopt`.
  // TODO(tsabolcec): Implement an additional method that takes list of
  // parameters. Currently, there's an issue with the `libpqxx` library which
  // isn't able to asynchronously fetch results for queries supplied with
  // parameters. Instead, a user should carefully escape specific query parts
  // using `pqxx::connection::esc` and its variant methods. This should resolved
  // once a better client wrapper is introduced.
  bool Execute(const std::string &statement);

  /// Fetches the next (single) row of the result from the input stream.
  /// If there is nothing to fetch, `std::nullopt` is returned instead.
  /// All PostgreSQL value types are converted to `mg::Value` in this step.
  std::optional<std::vector<mg::Value>> FetchOne();

  /// Static method that creates a PostgreSQL client instance.
  /// If the connection couldn't be established, it returns a `nullptr`.
  static std::unique_ptr<PostgresqlClient> Connect(const Params &params);

 private:
  explicit PostgresqlClient(std::unique_ptr<pqxx::connection> connection)
      : connection_(std::move(connection)) {}

  std::unique_ptr<pqxx::connection> connection_;

  // Execution context:
  std::optional<pqxx::work> work_;
  std::optional<pqxx::icursorstream> cursor_;
};
