#pragma once

#include <memory>
#include <set>
#include <string>
#include <string_view>

#include "memgraph_client.hpp"

/// Class that writes to the destination Memgraph database.
class MemgraphDestination {
 public:
  explicit MemgraphDestination(std::unique_ptr<MemgraphClient> client);

  MemgraphDestination(const MemgraphDestination &) = delete;
  MemgraphDestination(MemgraphDestination &&) = default;
  MemgraphDestination &operator=(const MemgraphDestination &) = delete;
  MemgraphDestination &operator=(MemgraphDestination &&) = delete;

  /// Executes queries to remove internal index and properties on vertices.
  /// If the `CreateVertex` method was never called, it does nothing.
  ~MemgraphDestination();

  /// Creates vertex. On the first call, additional query is executed to create
  /// an internal index for matching nodes by the `vertex`'s id property.
  void CreateVertex(const mg::Vertex &vertex);

  void CreateEdge(const mg::Edge &edge);

  void CreateLabelIndex(const std::string_view &label);

  void CreateLabelPropertyIndex(const std::string_view &label,
                                const std::string_view &property);

  void CreateExistenceConstraint(const std::string_view &label,
                                 const std::string_view &property);

  void CreateUniqueConstraint(const std::string_view &label,
                              const std::set<std::string> &properties);

 private:
  std::unique_ptr<MemgraphClient> client_;

  bool created_internal_index_{false};
};
