#pragma once

#include <functional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "memgraph_client.hpp"

/// Class that reads from the Memgraph database.
class MemgraphSource {
 public:
  struct IndexInfo {
    std::vector<std::string> label;
    std::vector<std::pair<std::string, std::string>> label_property;
  };

  struct ConstraintInfo {
    std::vector<std::pair<std::string, std::string>> existence;
    std::vector<std::pair<std::string, std::set<std::string>>> unique;
  };

  explicit MemgraphSource(std::unique_ptr<MemgraphClient> client);

  MemgraphSource(const MemgraphSource &) = delete;
  MemgraphSource(MemgraphSource &&) = default;
  MemgraphSource &operator=(const MemgraphSource &) = delete;
  MemgraphSource &operator=(MemgraphSource &&) = delete;

  ~MemgraphSource();

  void ReadNodes(std::function<void(const mg::ConstNode &node)> callback);

  void ReadRelationships(
      std::function<void(const mg::ConstRelationship &rel)> callback);

  IndexInfo ReadIndices();

  ConstraintInfo ReadConstraints();

 private:
  std::unique_ptr<MemgraphClient> client_;
};
