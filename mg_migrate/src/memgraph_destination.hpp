#pragma once

#include <memory>
#include <set>
#include <string>
#include <string_view>

#include "memgraph_client.hpp"

void CreateNode(MemgraphClient *client, const std::set<std::string> &labels,
                const mg::ConstMap &properties);

// Creates relationships between nodes that are matched by label and property
// set (id). If `use_merge` is set to true, it won't create already existing
// relationships between nodes. It returns a number of created/merged
// relationships.
size_t CreateRelationships(
    MemgraphClient *client, const std::string_view &label1,
    const mg::ConstMap &id1, const std::string_view &label2,
    const mg::ConstMap &id2, const std::string_view &edge_type,
    const mg::ConstMap &properties, bool use_merge = false);

void CreateLabelIndex(MemgraphClient *client, const std::string_view &label);

void CreateLabelPropertyIndex(MemgraphClient *client,
                              const std::string_view &label,
                              const std::string_view &property);

void CreateExistenceConstraint(MemgraphClient *client,
                               const std::string_view &label,
                               const std::string_view &property);

void CreateUniqueConstraint(MemgraphClient *client,
                            const std::string_view &label,
                            const std::set<std::string> &properties);

void DropLabelIndex(MemgraphClient *client, const std::string_view &label);

void DropLabelPropertyIndex(MemgraphClient *client,
                            const std::string_view &label,
                            const std::string_view &property);

void RemoveLabelFromNodes(MemgraphClient *client,
                          const std::string_view &label);

void RemovePropertyFromNodes(MemgraphClient *client,
                             const std::string_view &property);
