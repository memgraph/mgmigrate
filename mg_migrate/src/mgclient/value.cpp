#include "mgclient/value.hpp"

#include <glog/logging.h>

namespace mg {

namespace {

std::string ToString(const mg_string *string_value) {
  CHECK(string_value != nullptr) << "Expected non-null value!";
  return std::string(mg_string_data(string_value),
                     mg_string_size(string_value));
}

std::vector<Value> ToList(const mg_list *list) {
  CHECK(list != nullptr) << "Expected non-null value!";
  std::vector<Value> value_list;
  const size_t list_length = mg_list_size(list);
  value_list.reserve(list_length);
  for (size_t i = 0; i < list_length; ++i) {
    value_list.push_back(Value(mg_list_at(list, i)));
  }
  return value_list;
}

std::map<std::string, Value> ToMap(const mg_map *map) {
  CHECK(map != nullptr) << "Expected non-null value!";
  std::map<std::string, Value> value_map;
  size_t index = 0;
  const mg_string *key_value;
  while ((key_value = mg_map_key_at(map, index)) != nullptr) {
    std::string key = ToString(key_value);
    value_map.emplace(key, Value(mg_map_at(map, key.c_str())));
    index++;
  }
  return value_map;
}

Vertex ToVertex(const mg_node *node) {
  CHECK(node != nullptr) << "Expected non-null value!";
  const Id id = Id::FromInt(mg_node_id(node));
  const size_t label_count = mg_node_label_count(node);
  std::vector<std::string> labels;
  labels.reserve(label_count);
  for (size_t i = 0; i < label_count; ++i) {
    labels.push_back(ToString(mg_node_label_at(node, i)));
  }
  const std::map<std::string, Value> properties =
      ToMap(mg_node_properties(node));
  return Vertex{id, labels, properties};
}

UnboundedEdge ToUnboundedEdge(const mg_unbound_relationship *relationship) {
  CHECK(relationship != nullptr) << "Expected non-null value!";
  const Id id = Id::FromInt(mg_unbound_relationship_id(relationship));
  const std::string type = ToString(mg_unbound_relationship_type(relationship));
  const std::map<std::string, Value> properties =
      ToMap(mg_unbound_relationship_properties(relationship));
  return UnboundedEdge{id, type, properties};
}

}  // namespace

Value::Value(const mg_value *value) {
  CHECK(value != nullptr) << "Expected non-null value!";
  switch (mg_value_get_type(value)) {
    case MG_VALUE_TYPE_NULL: {
      type_ = Type::Null;
      return;
    }
    case MG_VALUE_TYPE_BOOL: {
      type_ = Type::Bool;
      bool_v = mg_value_bool(value);
      return;
    }
    case MG_VALUE_TYPE_INTEGER: {
      type_ = Type::Int;
      int_v = mg_value_integer(value);
      return;
    }
    case MG_VALUE_TYPE_FLOAT: {
      type_ = Type::Double;
      double_v = mg_value_float(value);
      return;
    }
    case MG_VALUE_TYPE_STRING: {
      type_ = Type::String;
      new (&string_v) std::string(ToString(mg_value_string(value)));
      return;
    }
    case MG_VALUE_TYPE_LIST: {
      type_ = Type::List;
      new (&list_v) std::vector<Value>(ToList(mg_value_list(value)));
      return;
    }
    case MG_VALUE_TYPE_MAP: {
      type_ = Type::Map;
      new (&map_v) std::map<std::string, Value>(ToMap(mg_value_map(value)));
      return;
    }
    case MG_VALUE_TYPE_NODE: {
      type_ = Type::Vertex;
      new (&vertex_v) Vertex(ToVertex(mg_value_node(value)));
      return;
    }
    case MG_VALUE_TYPE_RELATIONSHIP: {
      const mg_relationship *relationship = mg_value_relationship(value);
      Id id = Id::FromInt(mg_relationship_id(relationship));
      Id from = Id::FromInt(mg_relationship_start_id(relationship));
      Id to = Id::FromInt(mg_relationship_end_id(relationship));
      std::string type = ToString(mg_relationship_type(relationship));
      std::map<std::string, Value> properties =
          ToMap(mg_relationship_properties(relationship));

      type_ = Type::Edge;
      new (&edge_v) Edge({id, from, to, type, properties});
      return;
    }
    case MG_VALUE_TYPE_UNBOUND_RELATIONSHIP: {
      type_ = Type::UnboundedEdge;
      new (&unbounded_edge_v)
          UnboundedEdge(ToUnboundedEdge(mg_value_unbound_relationship(value)));
      return;
    }
    case MG_VALUE_TYPE_PATH: {
      const mg_path *path = mg_value_path(value);
      const size_t path_length = mg_path_length(path);
      std::vector<Vertex> vertices;
      vertices.reserve(path_length + 1);
      for (size_t i = 0; i <= path_length; ++i) {
        vertices.push_back(ToVertex(mg_path_node_at(path, i)));
      }
      std::vector<Edge> edges;
      edges.reserve(path_length);
      for (size_t i = 0; i < path_length; ++i) {
        UnboundedEdge unbounded_edge =
            ToUnboundedEdge(mg_path_relationship_at(path, i));
        int reversed = mg_path_relationship_reversed_at(path, i);
        CHECK(reversed == 0 || reversed == 1);
        const auto &u = vertices[i];
        const auto &v = vertices[i + 1];
        if (reversed) {
          edges.push_back(Edge{unbounded_edge.id, u.id, v.id,
                               unbounded_edge.type, unbounded_edge.properties});
        } else {
          edges.push_back(Edge{unbounded_edge.id, v.id, u.id,
                               unbounded_edge.type, unbounded_edge.properties});
        }
      }

      type_ = Type::Int;
      new (&path_v) Path(vertices, edges);
      return;
    }
    case MG_VALUE_TYPE_UNKNOWN: {
      type_ = Type::Null;
      CHECK(false) << "Unknown value type!";
      return;
    }
  }
}

#define DEF_GETTER_BY_VAL(type, value_type, field)   \
  value_type &Value::Value##type() {                 \
    if (type_ != Type::type) throw ValueException(); \
    return field;                                    \
  }                                                  \
  value_type Value::Value##type() const {            \
    if (type_ != Type::type) throw ValueException(); \
    return field;                                    \
  }

DEF_GETTER_BY_VAL(Bool, bool, bool_v)
DEF_GETTER_BY_VAL(Int, int64_t, int_v)
DEF_GETTER_BY_VAL(Double, double, double_v)

#undef DEF_GETTER_BY_VAL

#define DEF_GETTER_BY_REF(type, value_type, field)   \
  value_type &Value::Value##type() {                 \
    if (type_ != Type::type) throw ValueException(); \
    return field;                                    \
  }                                                  \
  const value_type &Value::Value##type() const {     \
    if (type_ != Type::type) throw ValueException(); \
    return field;                                    \
  }

DEF_GETTER_BY_REF(String, std::string, string_v)
DEF_GETTER_BY_REF(List, std::vector<Value>, list_v)
using map_t = std::map<std::string, Value>;
DEF_GETTER_BY_REF(Map, map_t, map_v)
DEF_GETTER_BY_REF(Vertex, Vertex, vertex_v)
DEF_GETTER_BY_REF(Edge, Edge, edge_v)
DEF_GETTER_BY_REF(UnboundedEdge, UnboundedEdge, unbounded_edge_v)
DEF_GETTER_BY_REF(Path, Path, path_v)

#undef DEF_GETTER_BY_REF

Value::Value(const Value &other) : type_(other.type_) {
  switch (other.type_) {
    case Type::Null:
      return;
    case Type::Bool:
      this->bool_v = other.bool_v;
      return;
    case Type::Int:
      this->int_v = other.int_v;
      return;
    case Type::Double:
      this->double_v = other.double_v;
      return;
    case Type::String:
      new (&string_v) std::string(other.string_v);
      return;
    case Type::List:
      new (&list_v) std::vector<Value>(other.list_v);
      return;
    case Type::Map:
      new (&map_v) std::map<std::string, Value>(other.map_v);
      return;
    case Type::Vertex:
      new (&vertex_v) Vertex(other.vertex_v);
      return;
    case Type::Edge:
      new (&edge_v) Edge(other.edge_v);
      return;
    case Type::UnboundedEdge:
      new (&unbounded_edge_v) UnboundedEdge(other.unbounded_edge_v);
      return;
    case Type::Path:
      new (&path_v) Path(other.path_v);
      return;
  }
}

Value &Value::operator=(const Value &other) {
  if (this != &other) {
    this->~Value();
    // set the type of this
    type_ = other.type_;

    switch (other.type_) {
      case Type::Null:
        return *this;
      case Type::Bool:
        this->bool_v = other.bool_v;
        return *this;
      case Type::Int:
        this->int_v = other.int_v;
        return *this;
      case Type::Double:
        this->double_v = other.double_v;
        return *this;
      case Type::String:
        new (&string_v) std::string(other.string_v);
        return *this;
      case Type::List:
        new (&list_v) std::vector<Value>(other.list_v);
        return *this;
      case Type::Map:
        new (&map_v) std::map<std::string, Value>(other.map_v);
        return *this;
      case Type::Vertex:
        new (&vertex_v) Vertex(other.vertex_v);
        return *this;
      case Type::Edge:
        new (&edge_v) Edge(other.edge_v);
        return *this;
      case Type::UnboundedEdge:
        new (&unbounded_edge_v) UnboundedEdge(other.unbounded_edge_v);
        return *this;
      case Type::Path:
        new (&path_v) Path(other.path_v);
        return *this;
    }
  }
  return *this;
}

Value::Value(Value &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::Null:
      break;
    case Type::Bool:
      this->bool_v = other.bool_v;
      break;
    case Type::Int:
      this->int_v = other.int_v;
      break;
    case Type::Double:
      this->double_v = other.double_v;
      break;
    case Type::String:
      new (&string_v) std::string(std::move(other.string_v));
      break;
    case Type::List:
      new (&list_v) std::vector<Value>(std::move(other.list_v));
      break;
    case Type::Map:
      new (&map_v) std::map<std::string, Value>(std::move(other.map_v));
      break;
    case Type::Vertex:
      new (&vertex_v) Vertex(std::move(other.vertex_v));
      break;
    case Type::Edge:
      new (&edge_v) Edge(std::move(other.edge_v));
      break;
    case Type::UnboundedEdge:
      new (&unbounded_edge_v) UnboundedEdge(std::move(other.unbounded_edge_v));
      break;
    case Type::Path:
      new (&path_v) Path(std::move(other.path_v));
      break;
  }

  // reset the type of other
  other.~Value();
  other.type_ = Type::Null;
}

Value &Value::operator=(Value &&other) noexcept {
  if (this != &other) {
    this->~Value();
    // set the type of this
    type_ = other.type_;

    switch (other.type_) {
      case Type::Null:
        break;
      case Type::Bool:
        this->bool_v = other.bool_v;
        break;
      case Type::Int:
        this->int_v = other.int_v;
        break;
      case Type::Double:
        this->double_v = other.double_v;
        break;
      case Type::String:
        new (&string_v) std::string(std::move(other.string_v));
        break;
      case Type::List:
        new (&list_v) std::vector<Value>(std::move(other.list_v));
        break;
      case Type::Map:
        new (&map_v) std::map<std::string, Value>(std::move(other.map_v));
        break;
      case Type::Vertex:
        new (&vertex_v) Vertex(std::move(other.vertex_v));
        break;
      case Type::Edge:
        new (&edge_v) Edge(std::move(other.edge_v));
        break;
      case Type::UnboundedEdge:
        new (&unbounded_edge_v)
            UnboundedEdge(std::move(other.unbounded_edge_v));
        break;
      case Type::Path:
        new (&path_v) Path(std::move(other.path_v));
        break;
    }

    // reset the type of other
    other.~Value();
    other.type_ = Type::Null;
  }
  return *this;
}

Value::~Value() {
  switch (type_) {
    // destructor for primitive types does nothing
    case Type::Null:
    case Type::Bool:
    case Type::Int:
    case Type::Double:
      return;

    // we need to call destructors for non primitive types since we used
    // placement new
    case Type::String:
      // Clang fails to compile ~std::string. It seems it is a bug in some
      // versions of clang. using namespace std statement solves the issue.
      using namespace std;
      string_v.~string();
      return;
    case Type::List:
      using namespace std;
      list_v.~vector<Value>();
      return;
    case Type::Map:
      using namespace std;
      map_v.~map<std::string, Value>();
      return;
    case Type::Vertex:
      vertex_v.~Vertex();
      return;
    case Type::Edge:
      edge_v.~Edge();
      return;
    case Type::UnboundedEdge:
      unbounded_edge_v.~UnboundedEdge();
      return;
    case Type::Path:
      path_v.~Path();
      return;
  }
}

bool operator==(const Value &lhs, const Value &rhs) {
  if (lhs.type() != rhs.type()) {
    return false;
  }
  switch (lhs.type()) {
    case Value::Type::Null:
      return true;
    case Value::Type::Bool:
      return lhs.ValueBool() == rhs.ValueBool();
    case Value::Type::Int:
      return lhs.ValueInt() == rhs.ValueInt();
    case Value::Type::Double:
      return lhs.ValueDouble() == rhs.ValueDouble();
    case Value::Type::String:
      return lhs.ValueString() == rhs.ValueString();
    case Value::Type::List:
      return lhs.ValueList() == rhs.ValueList();
    case Value::Type::Map:
      return lhs.ValueMap() == rhs.ValueMap();
    case Value::Type::Vertex:
      return lhs.ValueVertex() == rhs.ValueVertex();
    case Value::Type::Edge:
      return lhs.ValueEdge() == rhs.ValueEdge();
    case Value::Type::UnboundedEdge:
      return lhs.ValueUnboundedEdge() == rhs.ValueUnboundedEdge();
    case Value::Type::Path:
      return lhs.ValuePath() == rhs.ValuePath();
  }
}

std::ostream &operator<<(std::ostream &os, const Value::Type type) {
  switch (type) {
    case Value::Type::Null:
      return os << "null";
    case Value::Type::Bool:
      return os << "bool";
    case Value::Type::Int:
      return os << "int";
    case Value::Type::Double:
      return os << "double";
    case Value::Type::String:
      return os << "string";
    case Value::Type::List:
      return os << "list";
    case Value::Type::Map:
      return os << "map";
    case Value::Type::Vertex:
      return os << "vertex";
    case Value::Type::Edge:
      return os << "edge";
    case Value::Type::UnboundedEdge:
      return os << "unbounded_edge";
    case Value::Type::Path:
      return os << "path";
  }
}

}  // namespace mg
