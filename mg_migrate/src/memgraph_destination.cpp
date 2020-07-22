#include "memgraph_destination.hpp"

#include <map>
#include <sstream>

#include <glog/logging.h>

#include "utils/algorithm.hpp"

namespace {

const char *kInternalPropertyId = "__mg_id__";
const char *kInternalVertexLabel = "__mg_vertex__";
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

void WritePropertiesWithId(std::ostream *stream, ParamsBuilder *params,
                           const mg::ConstMap &properties,
                           const mg::ConstValue &property_id) {
  *stream << "{";
  *stream << kInternalPropertyId << ": " << params->Create(property_id);
  if (!properties.empty()) {
    *stream << ", ";
  }
  utils::PrintIterable(
      *stream, properties, ", ", [&params](auto &os, const auto &item) {
        os << EscapeName(item.first) << ": " << params->Create(item.second);
      });
  *stream << "}";
}

}  // namespace

MemgraphDestination::MemgraphDestination(std::unique_ptr<MemgraphClient> client)
    : client_(std::move(client)) {}

MemgraphDestination::~MemgraphDestination() {
  if (created_internal_index_) {
    {
      std::ostringstream stream;
      stream << "DROP INDEX ON :" << kInternalVertexLabel << "("
             << kInternalPropertyId << ");";
      CHECK(client_->Execute(stream.str()));
      CHECK(!client_->FetchOne());
    }
    {
      std::ostringstream stream;
      stream << "MATCH (u) REMOVE u:" << kInternalVertexLabel << ", u."
             << kInternalPropertyId << ";";
      CHECK(client_->Execute(stream.str()));
      CHECK(!client_->FetchOne());
    }
  }
}

void MemgraphDestination::CreateNode(const mg::ConstValue &id,
                                     const std::set<std::string> &labels,
                                     const mg::ConstMap &properties) {
  if (!created_internal_index_) {
    CreateLabelPropertyIndex(kInternalVertexLabel, kInternalPropertyId);
    created_internal_index_ = true;
  }

  ParamsBuilder params;
  std::ostringstream stream;
  stream << "CREATE (u:__mg_vertex__";
  for (const auto &label : labels) {
    stream << ":" << EscapeName(label);
  }
  stream << " ";
  WritePropertiesWithId(&stream, &params, properties, id);
  stream << ");";

  CHECK(client_->Execute(stream.str(), params.GetParams().AsConstMap()))
      << "Couldn't create a vertex!";
  CHECK(!client_->FetchOne())
      << "Unexpected data received while creating a vertex!";
}

void MemgraphDestination::CreateRelationship(const mg::ConstRelationship &rel) {
  CHECK(created_internal_index_)
      << "Can't create an edge when the database doesn't have any vertices!";

  ParamsBuilder params;
  std::ostringstream stream;
  stream << "MATCH ";
  stream << "(u:" << kInternalVertexLabel << "), ";
  stream << "(v:" << kInternalVertexLabel << ")";
  stream << " WHERE ";
  stream << "u." << kInternalPropertyId << " = " << rel.from().AsInt();
  stream << " AND ";
  stream << "v." << kInternalPropertyId << " = " << rel.to().AsInt();
  stream << " CREATE (u)-[:" << EscapeName(rel.type());
  const auto &properties = rel.properties();
  if (!properties.empty()) {
    stream << " ";
    WriteProperties(&stream, &params, properties);
  }
  stream << "]->(v) RETURN 1;";

  // Execute and make sure that exactly one edge is created.
  CHECK(client_->Execute(stream.str(), params.GetParams().AsConstMap()))
      << "Couldn't create an edge!";
  CHECK(client_->FetchOne()) << "Couldn't create an edge!";
  CHECK(!client_->FetchOne())
      << "Unexpected data received while creating an edge!";
}

void MemgraphDestination::CreateLabelIndex(const std::string_view &label) {
  std::ostringstream stream;
  stream << "CREATE INDEX ON :" << EscapeName(label) << ";";

  CHECK(client_->Execute(stream.str())) << "Couldn't create a label index!";
  CHECK(!client_->FetchOne())
      << "Unexpected data received while creating a label index!";
}

void MemgraphDestination::CreateLabelPropertyIndex(
    const std::string_view &label, const std::string_view &property) {
  std::ostringstream stream;
  stream << "CREATE INDEX ON :" << EscapeName(label) << "("
         << EscapeName(property) << ");";

  CHECK(client_->Execute(stream.str()))
      << "Couldn't create a label-property index!";
  CHECK(!client_->FetchOne())
      << "Unexpected data received while creating a label-property index!";
}

void MemgraphDestination::CreateExistenceConstraint(
    const std::string_view &label, const std::string_view &property) {
  std::ostringstream stream;
  stream << "CREATE CONSTRAINT ON (u:" << EscapeName(label)
         << ") ASSERT EXISTS (u." << EscapeName(property) << ");";

  CHECK(client_->Execute(stream.str()))
      << "Couldn't create an existence constraint!";
  CHECK(!client_->FetchOne())
      << "Unexpected data received while creating an existence constraint!";
}

void MemgraphDestination::CreateUniqueConstraint(
    const std::string_view &label, const std::set<std::string> &properties) {
  std::ostringstream stream;
  stream << "CREATE CONSTRAINT ON (u:" << EscapeName(label) << ") ASSERT ";
  utils::PrintIterable(stream, properties, ", ",
                       [](auto &stream, const auto &property) {
                         stream << "u." << EscapeName(property);
                       });
  stream << " IS UNIQUE;";

  CHECK(client_->Execute(stream.str()))
      << "Couldn't create a unique constraint!";
  CHECK(!client_->FetchOne())
      << "Unexpected data received while creating a unique constraint!";
}
