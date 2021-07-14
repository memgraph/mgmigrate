#include "memgraph_destination.hpp"

#include <map>
#include <sstream>

#include <glog/logging.h>

#include "utils/algorithm.hpp"

namespace {

const char *kParamPrefix = "param";

/// A helper class for easier query parameter management.
class ParamsBuilder {
 public:
  /// Assign a new parameter name to the given `value` and returns '$'-prefixed
  /// parameter name.
  std::string Create(const mg::ConstValue &value) {
    std::string key = kParamPrefix + std::to_string(counter_++);
    auto [_, emplaced] = params_.emplace(key, mg::Value(value));
    CHECK(emplaced) << "Param should be successfully inserted!";
    return "$" + key;
  }

  /// Returns map of all assigned parameters. It should be called only once.
  mg::Map GetParams() {
    mg::Map map(params_.size());
    for (auto &[key, value] : params_) {
      map.InsertUnsafe(key, std::move(value));
    }
    params_.clear();
    return map;
  }

 private:
  int counter_{0};
  std::map<std::string, mg::Value> params_;
};

/// A helper function that escapes label, edge type and property names.
std::string EscapeName(const std::string_view &src) {
  std::string out;
  out.reserve(src.size() + 2);
  out.append(1, '`');
  for (auto c : src) {
    if (c == '`') {
      out.append("``");
    } else {
      out.append(1, c);
    }
  }
  out.append(1, '`');
  return out;
}

void WriteProperties(std::ostream *stream, ParamsBuilder *params,
                     const mg::ConstMap &properties) {
  *stream << "{";
  utils::PrintIterable(
      *stream, properties, ", ", [&params](auto &os, const auto &item) {
        os << EscapeName(item.first) << ": " << params->Create(item.second);
      });
  *stream << "}";
}

void WriteIdMatcher(std::ostream *stream, ParamsBuilder *params,
                    const std::string &node,
                    const mg::ConstMap &id_properties) {
  utils::PrintIterable(*stream, id_properties, " AND ",
                       [&params, &node](auto &os, const auto &item) {
                         os << node << "." << EscapeName(item.first) << " = "
                            << params->Create(item.second);
                       });
}

}  // namespace

void CreateNode(MemgraphClient *client, const std::set<std::string> &labels,
                const mg::ConstMap &properties) {
  ParamsBuilder params;
  std::ostringstream stream;
  stream << "CREATE (u";
  for (const auto &label : labels) {
    stream << ":" << EscapeName(label);
  }
  stream << " ";
  WriteProperties(&stream, &params, properties);
  stream << ");";

  CHECK(client->Execute(stream.str(), params.GetParams().AsConstMap()))
      << "Couldn't create a vertex!";
  CHECK(!client->FetchOne())
      << "Unexpected data received while creating a vertex!";
}

size_t CreateRelationships(MemgraphClient *client,
                           const std::string_view &label1,
                           const mg::ConstMap &id1,
                           const std::string_view &label2,
                           const mg::ConstMap &id2,
                           const std::string_view &edge_type,
                           const mg::ConstMap &properties, bool use_merge) {
  ParamsBuilder params;
  std::ostringstream stream;
  stream << "MATCH ";
  stream << "(u:" << EscapeName(label1) << "), ";
  stream << "(v:" << EscapeName(label2) << ")";
  stream << " WHERE ";
  WriteIdMatcher(&stream, &params, "u", id1);
  stream << " AND ";
  WriteIdMatcher(&stream, &params, "v", id2);
  stream << (use_merge ? " MERGE " : " CREATE ");
  stream << "(u)-[:" << EscapeName(edge_type);
  if (!properties.empty()) {
    stream << " ";
    WriteProperties(&stream, &params, properties);
  }
  stream << "]->(v) RETURN COUNT(u);";

  // Execute query and expect a single result returned.
  CHECK(client->Execute(stream.str(), params.GetParams().AsConstMap()))
      << "Couldn't create an edge!";
  auto result = client->FetchOne();
  CHECK(result) << "Couldn't create a relationship!";
  CHECK(!client->FetchOne())
      << "Unexpected data received while creating a relationship!";
  CHECK(result->size() == 1 && (*result)[0].type() == mg::Value::Type::Int)
      << "Unexpected data received while creating a relationship!";
  return static_cast<size_t>((*result)[0].ValueInt());
}

void CreateLabelIndex(MemgraphClient *client, const std::string_view &label) {
  std::ostringstream stream;
  stream << "CREATE INDEX ON :" << EscapeName(label) << ";";

  CHECK(client->Execute(stream.str())) << "Couldn't create a label index!";
  CHECK(!client->FetchOne())
      << "Unexpected data received while creating a label index!";
}

void CreateLabelPropertyIndex(MemgraphClient *client,
                              const std::string_view &label,
                              const std::string_view &property) {
  std::ostringstream stream;
  stream << "CREATE INDEX ON :" << EscapeName(label) << "("
         << EscapeName(property) << ");";

  CHECK(client->Execute(stream.str()))
      << "Couldn't create a label-property index!";
  CHECK(!client->FetchOne())
      << "Unexpected data received while creating a label-property index!";
}

void CreateExistenceConstraint(MemgraphClient *client,
                               const std::string_view &label,
                               const std::string_view &property) {
  std::ostringstream stream;
  stream << "CREATE CONSTRAINT ON (u:" << EscapeName(label)
         << ") ASSERT EXISTS (u." << EscapeName(property) << ");";

  CHECK(client->Execute(stream.str()))
      << "Couldn't create an existence constraint!";
  CHECK(!client->FetchOne())
      << "Unexpected data received while creating an existence constraint!";
}

void CreateUniqueConstraint(MemgraphClient *client,
                            const std::string_view &label,
                            const std::set<std::string> &properties) {
  std::ostringstream stream;
  stream << "CREATE CONSTRAINT ON (u:" << EscapeName(label) << ") ASSERT ";
  utils::PrintIterable(stream, properties, ", ",
                       [](auto &stream, const auto &property) {
                         stream << "u." << EscapeName(property);
                       });
  stream << " IS UNIQUE;";

  CHECK(client->Execute(stream.str()))
      << "Couldn't create a unique constraint!";
  CHECK(!client->FetchOne())
      << "Unexpected data received while creating a unique constraint!";
}

void DropLabelIndex(MemgraphClient *client, const std::string_view &label) {
  std::ostringstream stream;
  stream << "DROP INDEX ON :" << EscapeName(label) << ";";

  CHECK(client->Execute(stream.str())) << "Couldn't drop a label index!";
  CHECK(!client->FetchOne())
      << "Unexpected data received while dropping a label index!";
}

void DropLabelPropertyIndex(MemgraphClient *client,
                            const std::string_view &label,
                            const std::string_view &property) {
  std::ostringstream stream;
  stream << "DROP INDEX ON :" << EscapeName(label) << "("
         << EscapeName(property) << ");";

  CHECK(client->Execute(stream.str()))
      << "Couldn't drop a label-property index!";
  CHECK(!client->FetchOne())
      << "Unexpected data received while dropping a label-property index!";
}

void RemoveLabelFromNodes(MemgraphClient *client,
                          const std::string_view &label) {
  const std::string query = "MATCH (u) REMOVE u:" + EscapeName(label) + ";";
  CHECK(client->Execute(query)) << "Couldn't remove a label from nodes!";
  CHECK(!client->Execute(query))
      << "Unexpected data received while removing a label from nodes!";
}

void RemovePropertyFromNodes(MemgraphClient *client,
                             const std::string_view &property) {
  const std::string query = "MATCH (u) REMOVE u." + EscapeName(property) + ";";
  CHECK(client->Execute(query)) << "Couldn't remove a property from nodes!";
  CHECK(!client->Execute(query))
      << "Unexpected data received while removing a property from nodes!";
}
