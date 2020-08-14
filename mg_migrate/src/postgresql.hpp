#pragma once

#include <memory>
#include <optional>
#include <string>
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
  kChar = 18,
  kInt8 = 20,
  kInt2 = 21,
  kInt4 = 23,
  kText = 25,
  kFloat4 = 700,
  kFloat8 = 701,
  kBoolArray = 1000,
  kCharArray = 1002,
  kInt2Array = 1005,
  kInt4Array = 1007,
  kTextArray = 1009,
  kBlankPaddedCharArray = 1014,  // bpchar
  kVarcharArray = 1015,
  kInt8Array = 1016,
  kFloat4Array = 1021,
  kFloat8Array = 1022,
  kVarchar = 1043,
  kNumericArray = 1231,
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

  /// Escapes string for use as SQL string literal.
  std::string Escape(const std::string_view &text) const {
    return connection_->esc(text);
  }

  /// Escapes string for use as SQL identifier.
  std::string EscapeName(const std::string_view &text) const {
    return connection_->quote_name(text);
  }

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

struct SchemaInfo {
  struct Table {
    std::string schema;
    std::string name;
    std::vector<std::string> columns;
    std::vector<size_t> primary_key;
    std::vector<size_t> foreign_keys;

    /// Indicates whether there's a foreign key referencing this table's primary
    /// key.
    bool primary_key_referenced;
  };

  struct ForeignKey {
    size_t child_table;
    size_t parent_table;
    std::vector<size_t> child_columns;
    std::vector<size_t> parent_columns;
  };

  /// Pair of table and list of its columns.
  using UniqueConstraint = std::pair<size_t, std::vector<size_t>>;

  /// Pair of table and its column.
  using ExistenceConstraint = std::pair<size_t, size_t>;

  std::vector<Table> tables;
  std::vector<ForeignKey> foreign_keys;
  std::vector<UniqueConstraint> unique_constraints;
  std::vector<ExistenceConstraint> existence_constraints;
};

/// Class that reads from the PostgreSQL database.
class PostgresqlSource {
 public:
  explicit PostgresqlSource(std::unique_ptr<PostgresqlClient> client);

  PostgresqlSource(const PostgresqlSource &) = delete;
  PostgresqlSource(PostgresqlSource &&) = default;
  PostgresqlSource &operator=(const PostgresqlSource &) = delete;
  PostgresqlSource &operator=(PostgresqlSource &&) = delete;

  ~PostgresqlSource();

  /// Returns structure of the 'public' schema.
  SchemaInfo GetSchemaInfo();

  /// Reads the given `table` row by row. Order of returned values corresponds
  /// to the order of columns listed in the `table`. If `distinct` is set to
  /// `true`, duplicates will be skipped.
  void ReadTable(const SchemaInfo::Table &table,
                 std::function<void(const std::vector<mg::Value> &)> callback);

 private:
  std::unique_ptr<PostgresqlClient> client_;
};
