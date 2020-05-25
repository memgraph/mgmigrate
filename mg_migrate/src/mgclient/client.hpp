#pragma once

#include <memory>
#include <optional>

#include <mgclient.h>

#include "mgclient/value.hpp"

namespace mg {

/// An interface for a Memgraph client that can execute queries and fetch
/// results.
class Client {
 public:
  struct Params {
    std::string host;
    uint16_t port;
    std::string username;
    std::string password;
    bool use_ssl;
    std::string client_name;
  };

  Client(const Client &) = delete;
  Client(Client &&) = default;
  Client &operator=(const Client &) = delete;
  Client &operator=(Client &&) = delete;
  ~Client();

  /// Executes the given Cypher `statement`.
  /// Returns true when the statement is successfully executed, false otherwise.
  /// After executing the statement, the method is blocked until all incoming
  /// data (execution results) are handled, i.e. until `FetchOne` method returns
  /// `std::nullopt`.
  bool Execute(const std::string &statement,
               const std::map<std::string, Value> &params = {});

  /// Fetches the next result from the input stream.
  /// If there is nothing to fetch, `std::nullopt` is returned.
  std::optional<std::vector<Value>> FetchOne();

  /// Static method that creates a Memgraph client instance.
  /// If the connection couldn't be established given the `params`, it returns
  /// a `nullptr`.
  static std::unique_ptr<Client> Connect(const Params &params);

 private:
  explicit Client(mg_session *session);

  mg_session *session_;
};

}  // namespace mg
