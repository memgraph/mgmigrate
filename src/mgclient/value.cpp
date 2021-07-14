#include "mgclient/value.hpp"

#include <set>

#include <glog/logging.h>

namespace mg {
namespace {

std::string_view ConvertString(const mg_string *str) {
  return std::string_view(mg_string_data(str), mg_string_size(str));
}

Value::Type ConvertType(mg_value_type type) {
  switch (type) {
    case MG_VALUE_TYPE_NULL:
      return Value::Type::Null;
    case MG_VALUE_TYPE_BOOL:
      return Value::Type::Bool;
    case MG_VALUE_TYPE_INTEGER:
      return Value::Type::Int;
    case MG_VALUE_TYPE_FLOAT:
      return Value::Type::Double;
    case MG_VALUE_TYPE_STRING:
      return Value::Type::String;
    case MG_VALUE_TYPE_LIST:
      return Value::Type::List;
    case MG_VALUE_TYPE_MAP:
      return Value::Type::Map;
    case MG_VALUE_TYPE_NODE:
      return Value::Type::Node;
    case MG_VALUE_TYPE_RELATIONSHIP:
      return Value::Type::Relationship;
    case MG_VALUE_TYPE_UNBOUND_RELATIONSHIP:
      return Value::Type::UnboundRelationship;
    case MG_VALUE_TYPE_PATH:
      return Value::Type::Path;
    case MG_VALUE_TYPE_UNKNOWN:
      LOG(FATAL) << "Unknown value type!";
      return Value::Type::Null;
  }
}

bool AreValuesEqual(const mg_value *value1, const mg_value *value2);

bool AreListsEqual(const mg_list *list1, const mg_list *list2) {
  if (list1 == list2) {
    return true;
  }
  if (mg_list_size(list1) != mg_list_size(list2)) {
    return false;
  }
  const size_t len = mg_list_size(list1);
  for (size_t i = 0; i < len; ++i) {
    if (!AreValuesEqual(mg_list_at(list1, i), mg_list_at(list2, i))) {
      return false;
    }
  }
  return true;
}

bool AreMapsEqual(const mg_map *map1, const mg_map *map2) {
  if (map1 == map2) {
    return true;
  }
  if (mg_map_size(map1) != mg_map_size(map2)) {
    return false;
  }
  const size_t len = mg_map_size(map1);
  for (size_t i = 0; i < len; ++i) {
    const mg_string *key = mg_map_key_at(map1, i);
    const mg_value *value1 = mg_map_value_at(map1, i);
    const mg_value *value2 =
        mg_map_at2(map2, mg_string_size(key), mg_string_data(key));
    if (value2 == nullptr) {
      return false;
    }
    if (!AreValuesEqual(value1, value2)) {
      return false;
    }
  }
  return true;
}

bool AreNodesEqual(const mg_node *node1, const mg_node *node2) {
  if (node1 == node2) {
    return true;
  }
  if (mg_node_id(node1) != mg_node_id(node2)) {
    return false;
  }
  if (mg_node_label_count(node1) != mg_node_label_count(node2)) {
    return false;
  }
  std::set<std::string_view> labels1;
  std::set<std::string_view> labels2;
  const size_t label_count = mg_node_label_count(node1);
  for (size_t i = 0; i < label_count; ++i) {
    labels1.insert(ConvertString(mg_node_label_at(node1, i)));
    labels2.insert(ConvertString(mg_node_label_at(node2, i)));
  }
  if (labels1 != labels2) {
    return false;
  }
  return AreMapsEqual(mg_node_properties(node1), mg_node_properties(node2));
}

bool AreRelationshipsEqual(const mg_relationship *rel1,
                           const mg_relationship *rel2) {
  if (rel1 == rel2) {
    return true;
  }
  if (mg_relationship_id(rel1) != mg_relationship_id(rel2)) {
    return false;
  }
  if (mg_relationship_start_id(rel1) != mg_relationship_start_id(rel2)) {
    return false;
  }
  if (mg_relationship_end_id(rel1) != mg_relationship_end_id(rel2)) {
    return false;
  }
  if (ConvertString(mg_relationship_type(rel1)) !=
      ConvertString(mg_relationship_type(rel2))) {
    return false;
  }
  return AreMapsEqual(mg_relationship_properties(rel1),
                      mg_relationship_properties(rel2));
}

bool AreUnboundRelationshipsEqual(const mg_unbound_relationship *rel1,
                                  const mg_unbound_relationship *rel2) {
  if (rel1 == rel2) {
    return true;
  }
  if (mg_unbound_relationship_id(rel1) != mg_unbound_relationship_id(rel2)) {
    return false;
  }
  if (ConvertString(mg_unbound_relationship_type(rel1)) !=
      ConvertString(mg_unbound_relationship_type(rel2))) {
    return false;
  }
  return AreMapsEqual(mg_unbound_relationship_properties(rel1),
                      mg_unbound_relationship_properties(rel2));
}

bool ArePathsEqual(const mg_path *path1, const mg_path *path2) {
  if (path1 == path2) {
    return true;
  }
  if (mg_path_length(path1) != mg_path_length(path2)) {
    return false;
  }
  const size_t len = mg_path_length(path1);
  for (size_t i = 0; i < len; ++i) {
    if (!AreNodesEqual(mg_path_node_at(path1, i), mg_path_node_at(path2, i))) {
      return false;
    }
    if (!AreUnboundRelationshipsEqual(mg_path_relationship_at(path1, i),
                                      mg_path_relationship_at(path2, i))) {
      return false;
    }
    if (mg_path_relationship_reversed_at(path1, i) !=
        mg_path_relationship_reversed_at(path2, i)) {
      return false;
    }
  }
  return AreNodesEqual(mg_path_node_at(path1, len),
                       mg_path_node_at(path2, len));
}

bool AreValuesEqual(const mg_value *value1, const mg_value *value2) {
  if (value1 == value2) {
    return true;
  }
  if (mg_value_get_type(value1) != mg_value_get_type(value2)) {
    return false;
  }
  switch (mg_value_get_type(value1)) {
    case MG_VALUE_TYPE_NULL:
      return true;
    case MG_VALUE_TYPE_BOOL:
      return mg_value_bool(value1) == mg_value_bool(value2);
    case MG_VALUE_TYPE_INTEGER:
      return mg_value_integer(value1) == mg_value_integer(value2);
    case MG_VALUE_TYPE_FLOAT:
      return mg_value_float(value1) == mg_value_float(value2);
    case MG_VALUE_TYPE_STRING:
      return ConvertString(mg_value_string(value1)) ==
             ConvertString(mg_value_string(value2));
    case MG_VALUE_TYPE_LIST:
      return AreListsEqual(mg_value_list(value1), mg_value_list(value2));
    case MG_VALUE_TYPE_MAP:
      return AreMapsEqual(mg_value_map(value1), mg_value_map(value2));
    case MG_VALUE_TYPE_NODE:
      return AreNodesEqual(mg_value_node(value1), mg_value_node(value2));
    case MG_VALUE_TYPE_RELATIONSHIP:
      return AreRelationshipsEqual(mg_value_relationship(value1),
                                   mg_value_relationship(value2));
    case MG_VALUE_TYPE_UNBOUND_RELATIONSHIP:
      return AreUnboundRelationshipsEqual(
          mg_value_unbound_relationship(value1),
          mg_value_unbound_relationship(value2));
    case MG_VALUE_TYPE_PATH:
      return ArePathsEqual(mg_value_path(value1), mg_value_path(value2));
    case MG_VALUE_TYPE_UNKNOWN:
      LOG(FATAL) << "Unknown value type!";
      return false;
  }
}

}  // namespace

////////////////////////////////////////////////////////////////////////////////
// List:

ConstValue List::Iterator::operator*() const { return (*iterable_)[index_]; }

List::List(const List &other) : ptr_(mg_list_copy(other.ptr_)) {}

List::List(List &&other) : ptr_(other.ptr_) { other.ptr_ = nullptr; }

List::~List() {
  if (ptr_ != nullptr) {
    mg_list_destroy(ptr_);
  }
}

List::List(const ConstList &list) : ptr_(mg_list_copy(list.ptr())) {}

List::List(const std::vector<mg::Value> &values) : List(values.size()) {
  for (const auto &value : values) {
    Append(value);
  }
}

List::List(std::vector<mg::Value> &&values) : List(values.size()) {
  for (auto &value : values) {
    Append(std::move(value));
  }
}

List::List(std::initializer_list<Value> values) : List(values.size()) {
  for (const auto &value : values) {
    Append(value);
  }
}

const ConstValue List::operator[](size_t index) const {
  return ConstValue(mg_list_at(ptr_, index));
}

bool List::Append(const Value &value) {
  return mg_list_append(ptr_, mg_value_copy(value.ptr())) == 0;
}

bool List::Append(const ConstValue &value) {
  return mg_list_append(ptr_, mg_value_copy(value.ptr())) == 0;
}

bool List::Append(Value &&value) {
  bool result = mg_list_append(ptr_, value.ptr_) == 0;
  value.ptr_ = nullptr;
  return result;
}

const ConstList List::AsConstList() const { return ConstList(ptr_); }

bool List::operator==(const List &other) const {
  return AreListsEqual(ptr_, other.ptr_);
}

bool List::operator==(const ConstList &other) const {
  return AreListsEqual(ptr_, other.ptr());
}

ConstValue ConstList::Iterator::operator*() const {
  return (*iterable_)[index_];
}

const ConstValue ConstList::operator[](size_t index) const {
  return ConstValue(mg_list_at(const_ptr_, index));
}

bool ConstList::operator==(const ConstList &other) const {
  return AreListsEqual(const_ptr_, other.const_ptr_);
}

bool ConstList::operator==(const List &other) const {
  return AreListsEqual(const_ptr_, other.ptr());
}

////////////////////////////////////////////////////////////////////////////////
// Map:

std::pair<std::string_view, ConstValue> Map::Iterator::operator*() const {
  return std::make_pair(ConvertString(mg_map_key_at(iterable_->ptr(), index_)),
                        ConstValue(mg_map_value_at(iterable_->ptr(), index_)));
}

Map::Map(const Map &other) : Map(mg_map_copy(other.ptr_)) {}

Map::Map(Map &&other) : Map(other.ptr_) { other.ptr_ = nullptr; }

Map::Map(const ConstMap &map) : ptr_(mg_map_copy(map.ptr())) {}

Map::~Map() {
  if (ptr_ != nullptr) {
    mg_map_destroy(ptr_);
  }
}

Map::Map(std::initializer_list<std::pair<std::string, Value>> list)
    : Map(list.size()) {
  for (const auto &[key, value] : list) {
    Insert(key, value.AsConstValue());
  }
}

ConstValue Map::operator[](const std::string_view &key) const {
  return ConstValue(mg_map_at2(ptr_, key.size(), key.data()));
}

Map::Iterator Map::find(const std::string_view &key) const {
  for (size_t i = 0; i < size(); ++i) {
    if (key == ConvertString(mg_map_key_at(ptr_, i))) {
      return Iterator(this, i);
    }
  }
  return end();
}

bool Map::Insert(const std::string_view &key, const Value &value) {
  return mg_map_insert2(ptr_, mg_string_make2(key.size(), key.data()),
                        mg_value_copy(value.ptr())) == 0;
}

bool Map::Insert(const std::string_view &key, const ConstValue &value) {
  return mg_map_insert2(ptr_, mg_string_make2(key.size(), key.data()),
                        mg_value_copy(value.ptr())) == 0;
}

bool Map::Insert(const std::string_view &key, Value &&value) {
  bool result = mg_map_insert2(ptr_, mg_string_make2(key.size(), key.data()),
                               value.ptr_) == 0;
  value.ptr_ = nullptr;
  return result;
}

bool Map::InsertUnsafe(const std::string_view &key, const Value &value) {
  return mg_map_insert_unsafe2(ptr_, mg_string_make2(key.size(), key.data()),
                               mg_value_copy(value.ptr())) == 0;
}

bool Map::InsertUnsafe(const std::string_view &key, const ConstValue &value) {
  return mg_map_insert_unsafe2(ptr_, mg_string_make2(key.size(), key.data()),
                               mg_value_copy(value.ptr())) == 0;
}

bool Map::InsertUnsafe(const std::string_view &key, Value &&value) {
  bool result =
      mg_map_insert_unsafe2(ptr_, mg_string_make2(key.size(), key.data()),
                            value.ptr_) == 0;
  value.ptr_ = nullptr;
  return result;
}

const ConstMap Map::AsConstMap() const { return ConstMap(ptr_); }

bool Map::operator==(const Map &other) const {
  return AreMapsEqual(ptr_, other.ptr_);
}

bool Map::operator==(const ConstMap &other) const {
  return AreMapsEqual(ptr_, other.ptr());
}

std::pair<std::string_view, ConstValue> ConstMap::Iterator::operator*() const {
  return std::make_pair(ConvertString(mg_map_key_at(iterable_->ptr(), index_)),
                        ConstValue(mg_map_value_at(iterable_->ptr(), index_)));
}

ConstValue ConstMap::operator[](const std::string_view &key) const {
  return ConstValue(mg_map_at2(const_ptr_, key.size(), key.data()));
}

ConstMap::Iterator ConstMap::find(const std::string_view &key) const {
  for (size_t i = 0; i < size(); ++i) {
    if (key == ConvertString(mg_map_key_at(const_ptr_, i))) {
      return Iterator(this, i);
    }
  }
  return end();
}

bool ConstMap::operator==(const ConstMap &other) const {
  return AreMapsEqual(const_ptr_, other.const_ptr_);
}

bool ConstMap::operator==(const Map &other) const {
  return AreMapsEqual(const_ptr_, other.ptr());
}

////////////////////////////////////////////////////////////////////////////////
// Node:

std::string_view Node::Labels::Iterator::operator*() const {
  return (*iterable_)[index_];
}

std::string_view Node::Labels::operator[](size_t index) const {
  return ConvertString(mg_node_label_at(node_, index));
}

Node::Node(const Node &other) : Node(mg_node_copy(other.ptr_)) {}

Node::Node(Node &&other) : ptr_(other.ptr_) { other.ptr_ = nullptr; }

Node::~Node() {
  if (ptr_ != nullptr) {
    mg_node_destroy(ptr_);
  }
}

Node::Node(const ConstNode &node) : ptr_(mg_node_copy(node.ptr())) {}

bool Node::operator==(const Node &other) const {
  return AreNodesEqual(ptr_, other.ptr_);
}

bool Node::operator==(const ConstNode &other) const {
  return AreNodesEqual(ptr_, other.ptr());
}

ConstNode Node::AsConstNode() const { return ConstNode(ptr_); }

bool ConstNode::operator==(const ConstNode &other) const {
  return AreNodesEqual(const_ptr_, other.const_ptr_);
}

bool ConstNode::operator==(const Node &other) const {
  return AreNodesEqual(const_ptr_, other.ptr());
}

////////////////////////////////////////////////////////////////////////////////
// Relationship:

Relationship::Relationship(const Relationship &other)
    : Relationship(mg_relationship_copy(other.ptr_)) {}

Relationship::Relationship(Relationship &&other) : Relationship(other.ptr_) {
  other.ptr_ = nullptr;
}

Relationship::~Relationship() {
  if (ptr_ != nullptr) {
    mg_relationship_destroy(ptr_);
  }
}

Relationship::Relationship(const ConstRelationship &rel)
    : ptr_(mg_relationship_copy(rel.ptr())) {}

std::string_view Relationship::type() const {
  return ConvertString(mg_relationship_type(ptr_));
}

ConstRelationship Relationship::AsConstRelationship() const {
  return ConstRelationship(ptr_);
}

bool Relationship::operator==(const Relationship &other) const {
  return AreRelationshipsEqual(ptr_, other.ptr_);
}

bool Relationship::operator==(const ConstRelationship &other) const {
  return AreRelationshipsEqual(ptr_, other.ptr());
}

std::string_view ConstRelationship::type() const {
  return ConvertString(mg_relationship_type(const_ptr_));
}

bool ConstRelationship::operator==(const ConstRelationship &other) const {
  return AreRelationshipsEqual(const_ptr_, other.const_ptr_);
}

bool ConstRelationship::operator==(const Relationship &other) const {
  return AreRelationshipsEqual(const_ptr_, other.ptr());
}

////////////////////////////////////////////////////////////////////////////////
// UnboundRelationship:

UnboundRelationship::UnboundRelationship(const UnboundRelationship &other)
    : ptr_(mg_unbound_relationship_copy(other.ptr_)) {}

UnboundRelationship::UnboundRelationship(UnboundRelationship &&other)
    : ptr_(other.ptr_) {
  other.ptr_ = nullptr;
}

UnboundRelationship::~UnboundRelationship() {
  if (ptr_ != nullptr) {
    mg_unbound_relationship_destroy(ptr_);
  }
}

UnboundRelationship::UnboundRelationship(const ConstUnboundRelationship &rel)
    : ptr_(mg_unbound_relationship_copy(rel.ptr())) {}

std::string_view UnboundRelationship::type() const {
  return ConvertString(mg_unbound_relationship_type(ptr_));
}

ConstUnboundRelationship UnboundRelationship::AsConstUnboundRelationship()
    const {
  return ConstUnboundRelationship(ptr_);
}

bool UnboundRelationship::operator==(const UnboundRelationship &other) const {
  return AreUnboundRelationshipsEqual(ptr_, other.ptr_);
}

bool UnboundRelationship::operator==(
    const ConstUnboundRelationship &other) const {
  return AreUnboundRelationshipsEqual(ptr_, other.ptr());
}

std::string_view ConstUnboundRelationship::type() const {
  return ConvertString(mg_unbound_relationship_type(const_ptr_));
}

bool ConstUnboundRelationship::operator==(
    const ConstUnboundRelationship &other) const {
  return AreUnboundRelationshipsEqual(const_ptr_, other.const_ptr_);
}

bool ConstUnboundRelationship::operator==(
    const UnboundRelationship &other) const {
  return AreUnboundRelationshipsEqual(const_ptr_, other.ptr());
}

////////////////////////////////////////////////////////////////////////////////
// Path:

Path::Path(const Path &other) : ptr_(mg_path_copy(other.ptr_)) {}

Path::Path(Path &&other) : ptr_(other.ptr_) { other.ptr_ = nullptr; }

Path::~Path() {
  if (ptr_ != nullptr) {
    mg_path_destroy(ptr_);
  }
}

Path::Path(const ConstPath &path) : ptr_(mg_path_copy(path.ptr())) {}

ConstNode Path::GetNodeAt(size_t index) const {
  auto vertex_ptr = mg_path_node_at(ptr_, index);
  CHECK(vertex_ptr != nullptr) << "Unable to access the vertex of a path!";
  return ConstNode(vertex_ptr);
}

ConstUnboundRelationship Path::GetRelationshipAt(size_t index) const {
  auto edge_ptr = mg_path_relationship_at(ptr_, index);
  CHECK(edge_ptr != nullptr) << "Unable to access the edge of a path!";
  return ConstUnboundRelationship(edge_ptr);
}

bool Path::IsReversedRelationshipAt(size_t index) const {
  auto is_reversed = mg_path_relationship_reversed_at(ptr_, index);
  CHECK(is_reversed != -1)
      << "Unable to access the edge orientation of a path!";
  return is_reversed == 1;
}

ConstPath Path::AsConstPath() const { return ConstPath(ptr_); }

bool Path::operator==(const Path &other) const {
  return ArePathsEqual(ptr_, other.ptr_);
}

bool Path::operator==(const ConstPath &other) const {
  return ArePathsEqual(ptr_, other.ptr());
}

ConstNode ConstPath::GetNodeAt(size_t index) const {
  auto vertex_ptr = mg_path_node_at(const_ptr_, index);
  CHECK(vertex_ptr != nullptr) << "Unable to access the vertex of a path!";
  return ConstNode(vertex_ptr);
}

ConstUnboundRelationship ConstPath::GetRelationshipAt(size_t index) const {
  auto edge_ptr = mg_path_relationship_at(const_ptr_, index);
  CHECK(edge_ptr != nullptr) << "Unable to access the edge of a path!";
  return ConstUnboundRelationship(edge_ptr);
}

bool ConstPath::IsReversedRelationshipAt(size_t index) const {
  auto is_reversed = mg_path_relationship_reversed_at(const_ptr_, index);
  CHECK(is_reversed != -1)
      << "Unable to access the edge orientation of a path!";
  return is_reversed == 1;
}

bool ConstPath::operator==(const ConstPath &other) const {
  return ArePathsEqual(const_ptr_, other.const_ptr_);
}

bool ConstPath::operator==(const Path &other) const {
  return ArePathsEqual(const_ptr_, other.ptr());
}

////////////////////////////////////////////////////////////////////////////////
// Value:

Value::Value(const Value &other) : Value(mg_value_copy(other.ptr_)) {}

Value::Value(Value &&other) : ptr_(other.ptr_) { other.ptr_ = nullptr; }

Value::~Value() {
  if (ptr_ != nullptr) {
    mg_value_destroy(ptr_);
  }
}

Value::Value(const ConstValue &value) : ptr_(mg_value_copy(value.ptr())) {}

Value::Value(const std::string_view &value)
    : Value(
          mg_value_make_string2(mg_string_make2(value.size(), value.data()))) {}

Value::Value(const char *value) : Value(mg_value_make_string(value)) {}

Value::Value(List &&list) : Value(mg_value_make_list(list.ptr_)) {
  list.ptr_ = nullptr;
}

Value::Value(Map &&map) : Value(mg_value_make_map(map.ptr_)) {
  map.ptr_ = nullptr;
}

Value::Value(Node &&vertex) : Value(mg_value_make_node(vertex.ptr_)) {
  vertex.ptr_ = nullptr;
}

Value::Value(Relationship &&edge)
    : Value(mg_value_make_relationship(edge.ptr_)) {
  edge.ptr_ = nullptr;
}

Value::Value(UnboundRelationship &&edge)
    : Value(mg_value_make_unbound_relationship(edge.ptr_)) {
  edge.ptr_ = nullptr;
}

Value::Value(Path &&path) : Value(mg_value_make_path(path.ptr_)) {
  path.ptr_ = nullptr;
}

bool Value::ValueBool() const {
  CHECK(type() == Type::Bool);
  return static_cast<bool>(mg_value_bool(ptr_));
}

int64_t Value::ValueInt() const {
  CHECK(type() == Type::Int);
  return mg_value_integer(ptr_);
}

double Value::ValueDouble() const {
  CHECK(type() == Type::Double);
  return mg_value_float(ptr_);
}

std::string_view Value::ValueString() const {
  CHECK(type() == Type::String);
  return ConvertString(mg_value_string(ptr_));
}

const ConstList Value::ValueList() const {
  CHECK(type() == Type::List);
  return ConstList(mg_value_list(ptr_));
}

const ConstMap Value::ValueMap() const {
  CHECK(type() == Type::Map);
  return ConstMap(mg_value_map(ptr_));
}

const ConstNode Value::ValueNode() const {
  CHECK(type() == Type::Node);
  return ConstNode(mg_value_node(ptr_));
}

const ConstRelationship Value::ValueRelationship() const {
  CHECK(type() == Type::Relationship);
  return ConstRelationship(mg_value_relationship(ptr_));
}

const ConstUnboundRelationship Value::ValueUnboundRelationship() const {
  CHECK(type() == Type::UnboundRelationship);
  return ConstUnboundRelationship(mg_value_unbound_relationship(ptr_));
}

const ConstPath Value::ValuePath() const {
  CHECK(type() == Type::Path);
  return ConstPath(mg_value_path(ptr_));
}

Value::Type Value::type() const { return ConvertType(mg_value_get_type(ptr_)); }

ConstValue Value::AsConstValue() const { return ConstValue(ptr_); }

bool Value::operator==(const Value &other) const {
  return AreValuesEqual(ptr_, other.ptr_);
}

bool Value::operator==(const ConstValue &other) const {
  return AreValuesEqual(ptr_, other.ptr());
}

bool ConstValue::ValueBool() const {
  CHECK(type() == Value::Type::Bool);
  return static_cast<bool>(mg_value_bool(const_ptr_));
}

int64_t ConstValue::ValueInt() const {
  CHECK(type() == Value::Type::Int);
  return mg_value_integer(const_ptr_);
}

double ConstValue::ValueDouble() const {
  CHECK(type() == Value::Type::Double);
  return mg_value_float(const_ptr_);
}

std::string_view ConstValue::ValueString() const {
  CHECK(type() == Value::Type::String);
  return ConvertString(mg_value_string(const_ptr_));
}

const ConstList ConstValue::ValueList() const {
  CHECK(type() == Value::Type::List);
  return ConstList(mg_value_list(const_ptr_));
}

const ConstMap ConstValue::ValueMap() const {
  CHECK(type() == Value::Type::List);
  return ConstMap(mg_value_map(const_ptr_));
}

const ConstNode ConstValue::ValueNode() const {
  CHECK(type() == Value::Type::Node);
  return ConstNode(mg_value_node(const_ptr_));
}

const ConstRelationship ConstValue::ValueRelationship() const {
  CHECK(type() == Value::Type::Relationship);
  return ConstRelationship(mg_value_relationship(const_ptr_));
}

const ConstUnboundRelationship ConstValue::ValueUnboundRelationship() const {
  CHECK(type() == Value::Type::UnboundRelationship);
  return ConstUnboundRelationship(mg_value_unbound_relationship(const_ptr_));
}

const ConstPath ConstValue::ValuePath() const {
  CHECK(type() == Value::Type::Path);
  return ConstPath(mg_value_path(const_ptr_));
}
Value::Type ConstValue::type() const {
  return ConvertType(mg_value_get_type(const_ptr_));
}

bool ConstValue::operator==(const ConstValue &other) const {
  return AreValuesEqual(const_ptr_, other.const_ptr_);
}

bool ConstValue::operator==(const Value &other) const {
  return AreValuesEqual(const_ptr_, other.ptr());
}

}  // namespace mg
