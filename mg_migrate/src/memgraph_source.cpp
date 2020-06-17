#include "memgraph_source.hpp"

#include <glog/logging.h>

MemgraphSource::MemgraphSource(std::unique_ptr<MemgraphClient> client)
    : client_(std::move(client)) {}

MemgraphSource::~MemgraphSource() {}

void MemgraphSource::ReadVertices(
    std::function<void(const mg::Vertex &vertex)> callback) {
  CHECK(client_->Execute("MATCH (u) RETURN u;")) << "Can't read vertices!";
  std::optional<std::vector<mg::Value>> row;
  while ((row = client_->FetchOne()) != std::nullopt) {
    CHECK(row->size() == 1 && (*row)[0].type() == mg::Value::Type::Vertex)
        << "Received unexpected result while reading vertices!";
    callback((*row)[0].ValueVertex());
  }
}

void MemgraphSource::ReadEdges(
    std::function<void(const mg::Edge &edge)> callback) {
  CHECK(client_->Execute("MATCH (u)-[e]->(v) RETURN e;"))
      << "Can't read edges!";
  std::optional<std::vector<mg::Value>> row;
  while ((row = client_->FetchOne()) != std::nullopt) {
    CHECK(row->size() == 1 && (*row)[0].type() == mg::Value::Type::Edge)
        << "Received unexpected result while reading edges!";
    callback((*row)[0].ValueEdge());
  }
}

MemgraphSource::IndexInfo MemgraphSource::ReadIndices() {
  IndexInfo info;
  CHECK(client_->Execute("SHOW INDEX INFO;")) << "Can't read indices!";
  std::optional<std::vector<mg::Value>> row;
  while ((row = client_->FetchOne()) != std::nullopt) {
    CHECK(row->size() == 3 && (*row)[0].type() == mg::Value::Type::String &&
          (*row)[1].type() == mg::Value::Type::String)
        << "Received unexpected result while reading indices!";
    const auto &type = (*row)[0].ValueString();
    const auto &label = (*row)[1].ValueString();
    if (type == "label") {
      info.label.push_back(label);
    } else if (type == "label+property") {
      CHECK((*row)[2].type() == mg::Value::Type::String);
      const auto &property = (*row)[2].ValueString();
      info.label_property.emplace_back(label, property);
    } else {
      CHECK(false) << "Received unsupported index type '" << type << "'!";
    }
  }
  return info;
}

MemgraphSource::ConstraintInfo MemgraphSource::ReadConstraints() {
  ConstraintInfo info;
  CHECK(client_->Execute("SHOW CONSTRAINT INFO;")) << "Can't read constraints";
  std::optional<std::vector<mg::Value>> row;
  while ((row = client_->FetchOne()) != std::nullopt) {
    CHECK(row->size() == 3 && (*row)[0].type() == mg::Value::Type::String &&
          (*row)[1].type() == mg::Value::Type::String)
        << "Received unexpected result while reading constraints!";
    const auto &type = (*row)[0].ValueString();
    const auto &label = (*row)[1].ValueString();
    if (type == "existence") {
      CHECK((*row)[2].type() == mg::Value::Type::String)
          << "Received unexpected result while reading constraints!";
      const auto &property = (*row)[2].ValueString();
      info.existence.emplace_back(label, property);
    } else if (type == "unique") {
      CHECK((*row)[2].type() == mg::Value::Type::List)
          << "Received unexpected result while reading constraints!";
      const auto &list = (*row)[2].ValueList();
      std::set<std::string> properties;
      for (const auto &value : list) {
        CHECK(value.type() == mg::Value::Type::String)
            << "Received unexpected result while reading constraints!";
        properties.insert(value.ValueString());
      }
      info.unique.emplace_back(label, std::move(properties));
    } else {
      CHECK(false) << "Received unsupported constraint type '" << type << "'!";
    }
  }
  return info;
}
