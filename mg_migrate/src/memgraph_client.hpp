#pragma once

#include "mgclient/client.hpp"

/// Interface for Memgraph client.
class MemgraphClient {
 public:
  virtual ~MemgraphClient() {}

  /// Executes the `statement` supplied with `params`. Returns true on success
  /// and false otherwise, e.g. if there is another ongoing execution.
  virtual bool Execute(const std::string &statement,
                       const std::map<std::string, mg::Value> &params = {}) = 0;

  /// Fetches one row from the input stream. Returns `std::nullopt` if there's
  /// nothing to fetch. This method should be called as long as `std::nullopt`
  /// not returned after the execution.
  virtual std::optional<std::vector<mg::Value>> FetchOne() = 0;
};

/// A concrete implementation of MemgraphClient interface, that is a very thin
/// wrapper for mg::Client.
class MemgraphClientConnection : public MemgraphClient {
 public:
  MemgraphClientConnection(const MemgraphClientConnection &) = delete;
  MemgraphClientConnection(MemgraphClientConnection &&) = default;
  MemgraphClientConnection &operator=(const MemgraphClientConnection &) =
      delete;
  MemgraphClientConnection &operator=(MemgraphClientConnection &&) = delete;
  ~MemgraphClientConnection() override {}

  bool Execute(const std::string &statement,
               const std::map<std::string, mg::Value> &params = {}) override {
    return client_->Execute(statement, params);
  }

  std::optional<std::vector<mg::Value>> FetchOne() override {
    return client_->FetchOne();
  }

  /// Constructs a new client.
  static std::unique_ptr<MemgraphClient> Connect(
      const mg::Client::Params &params) {
    auto client = mg::Client::Connect(params);
    if (!client) {
      return nullptr;
    }
    return std::unique_ptr<MemgraphClient>(
        new MemgraphClientConnection(std::move(client)));
  }

 private:
  explicit MemgraphClientConnection(std::unique_ptr<mg::Client> client)
      : client_(std::move(client)) {}

  std::unique_ptr<mg::Client> client_;
};
