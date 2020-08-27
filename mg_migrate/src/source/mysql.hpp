#pragma once

#include "mgclient/value.hpp"
#include "schema_info.hpp"

#include <mysqlx/xdevapi.h>

class MysqlClient {
public:
  struct Params {
    std::string host;
    std::uint16_t port;
    std::string username;
    std::string password;
    std::string database;
  };

  MysqlClient(const MysqlClient &) = delete;
  MysqlClient(MysqlClient &&) = delete;
  MysqlClient &operator=(const MysqlClient &) = delete;
  MysqlClient &operator=(MysqlClient &&) = delete;
  ~MysqlClient() {}

  static std::unique_ptr<MysqlClient> Connect(const Params &params);

  auto const &session() const {
    return session_;
  }
private:
  explicit MysqlClient(std::unique_ptr<mysqlx::Session> session)
    : session_(std::move(session))
  {}

  std::unique_ptr<mysqlx::Session> session_;
};

class MysqlSource {
public:
  explicit MysqlSource(std::unique_ptr<MysqlClient> client)
    : client_(std::move(client))
  {}

  MysqlSource(const MysqlSource &) = delete;
  MysqlSource(MysqlSource &&) = delete;
  MysqlSource &operator=(const MysqlSource &) = delete;
  MysqlSource &operator=(MysqlSource &&) = delete;

  SchemaInfo GetSchemaInfo();

  void ReadTable(const SchemaInfo::Table &table,
                 std::function<void(const std::vector<mg::Value> &)> callback);
private:
  std::unique_ptr<MysqlClient> client_;
};
