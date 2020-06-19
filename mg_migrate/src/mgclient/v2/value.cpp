#include "mgclient/v2/value.hpp"

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
      return Value::Type::Vertex;
    case MG_VALUE_TYPE_RELATIONSHIP:
      return Value::Type::Edge;
    case MG_VALUE_TYPE_UNBOUND_RELATIONSHIP:
      return Value::Type::UnboundedEdge;
    case MG_VALUE_TYPE_PATH:
      return Value::Type::Path;
    case MG_VALUE_TYPE_UNKNOWN:
      CHECK(false) << "Unknown value type!";
      return Value::Type::Null;
  }
}

bool AreValuesEqual(const mg_value *value1, const mg_value *value2);

bool AreListsEqual(const mg_list *list1, const mg_list *list2) {
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

bool AreValuesEqual(const mg_value *value1, const mg_value *value2) {
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
    case MG_VALUE_TYPE_RELATIONSHIP:
    case MG_VALUE_TYPE_UNBOUND_RELATIONSHIP:
    case MG_VALUE_TYPE_PATH:
      // TODO(tsabolcec): Implement other types.
      return false;
    case MG_VALUE_TYPE_UNKNOWN:
      CHECK(false) << "Unknown value type!";
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

const ConstValue List::operator[](size_t index) const {
  return ConstValue(mg_list_at(ptr_, index));
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

const ConstMap Map::AsConstMap() const { return ConstMap(ptr_); }

bool Map::operator==(const Map &other) const {
  return AreMapsEqual(ptr_, other.ptr_);
}

bool Map::operator==(const ConstMap &other) const {
  return AreMapsEqual(ptr_, other.ptr());
}

std::pair<std::string_view, ConstValue> ConstMap::Iterator::operator*() const {
  return std::make_pair(
      ConvertString(mg_map_key_at(iterable_->ptr(), index_)),
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
// Value:

Value::Value(const Value &other) : Value(mg_value_copy(other.ptr_)) {}

Value::Value(Value &&other) : ptr_(other.ptr_) { other.ptr_ = nullptr; }

Value::~Value() {
  if (ptr_ != nullptr) {
    mg_value_destroy(ptr_);
  }
}

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
