#pragma once

#include <string>
#include <utility>
#include <vector>

#include <mgclient.h>

namespace mg {

// Forward declarations:
class ConstList;
class ConstMap;
class ConstValue;
class Value;

#define CREATE_ITERATOR(container, element)                              \
  class Iterator {                                                       \
   private:                                                              \
    friend class container;                                              \
                                                                         \
   public:                                                               \
    bool operator==(const Iterator &other) {                             \
      return iterable_ == other.iterable_ && index_ == other.index_;     \
    }                                                                    \
                                                                         \
    bool operator!=(const Iterator &other) { return !(*this == other); } \
                                                                         \
    Iterator &operator++() {                                             \
      index_++;                                                          \
      return *this;                                                      \
    }                                                                    \
                                                                         \
    element operator*() const;                                           \
                                                                         \
   private:                                                              \
    Iterator(const container *iterable, size_t index)                    \
        : iterable_(iterable), index_(index) {}                          \
                                                                         \
    const container *iterable_;                                          \
    size_t index_;                                                       \
  }

////////////////////////////////////////////////////////////////////////////////
/// List:

class List final {
 private:
  friend class Value;

 public:
  CREATE_ITERATOR(List, ConstValue);

  explicit List(mg_list *ptr) : ptr_(ptr) {}

  List(const List &other);
  List(List &&other);
  List &operator=(const List &other) = delete;
  List &operator=(List &&other) = delete;

  ~List();

  /// Copies the given list.
  explicit List(const ConstList &list);

  explicit List(size_t capacity) : List(mg_list_make_empty(capacity)) {}

  size_t size() const { return mg_list_size(ptr_); }

  /// Returns the value at the given `index`.
  const ConstValue operator[](size_t index) const;

  Iterator begin() const { return Iterator(this, 0); }
  Iterator end() const { return Iterator(this, size()); }

  /// Appends the given `value` to the list. It copies the `value`.
  bool Append(const ConstValue &value);

  /// Appends the given `value` to the list. It takes the ownership of the
  /// `value` by moving it.
  /// Behaviour of accessing the `value` after performing this operation is
  /// considered undefined.
  bool Append(Value &&value);

  const ConstList AsConstList() const;

  bool operator==(const List &other) const;
  bool operator==(const ConstList &other) const;
  bool operator!=(const List &other) const { return !(*this == other); }
  bool operator!=(const ConstList &other) const { return !(*this == other); }

  const mg_list *ptr() const { return ptr_; }

 private:
  mg_list *ptr_;
};

class ConstList final {
 public:
  CREATE_ITERATOR(ConstList, ConstValue);

  explicit ConstList(const mg_list *const_ptr) : const_ptr_(const_ptr) {}

  size_t size() const { return mg_list_size(const_ptr_); }
  const ConstValue operator[](size_t index) const;

  Iterator begin() const { return Iterator(this, 0); }
  Iterator end() const { return Iterator(this, size()); }

  bool operator==(const ConstList &other) const;
  bool operator==(const List &other) const;
  bool operator!=(const ConstList &other) const { return !(*this == other); }
  bool operator!=(const List &other) const { return !(*this == other); }

  const mg_list *ptr() const { return const_ptr_; }

 private:
  const mg_list *const_ptr_;
};

////////////////////////////////////////////////////////////////////////////////
// Map:

class Map final {
 private:
  friend class Value;
  using KeyValuePair = std::pair<std::string_view, ConstValue>;

 public:
  CREATE_ITERATOR(Map, KeyValuePair);

  explicit Map(mg_map *ptr) : ptr_(ptr) {}

  Map(const Map &other);
  Map(Map &&other);
  Map &operator=(const Map &other) = delete;
  Map &operator=(Map &&other) = delete;
  ~Map();

  /// Copies content of the given `map`.
  explicit Map(const ConstMap &map);

  /// Constructs an empty map of the given `capacity`.
  explicit Map(size_t capacity) : Map(mg_map_make_empty(capacity)) {}

  size_t size() const { return mg_map_size(ptr_); }

  /// Returns the value associated with the given `key`.
  /// Behaves undefined if there is no such a value. Note that each key-value
  /// pair has to be checked, resulting with O(n) time complexity.
  ConstValue operator[](const std::string_view &key) const;

  Iterator begin() const { return Iterator(this, 0); }
  Iterator end() const { return Iterator(this, size()); }

  /// Returns the key-value iterator for the given `key`.
  /// In the case there is no such pair, end iterator is returned. Note that
  /// each key-value pair has to be checked, resulting with O(n) time
  /// complexity.
  Iterator find(const std::string_view &key) const;

  /// Inserts the given `key`-`value` pair into the map.
  /// Copies both the `key` and the `value`.
  bool Insert(const std::string_view &key, const ConstValue &value);

  /// Inserts the given `key`-`value` pair into the map.
  /// Copies the `key` and takes the ownership of `value` by moving it.
  /// Behaviour of accessing the `value` after performing this operation is
  /// considered undefined.
  bool Insert(const std::string_view &key, Value &&value);

  const ConstMap AsConstMap() const;

  bool operator==(const Map &other) const;
  bool operator==(const ConstMap &other) const;
  bool operator!=(const Map &other) const { return !(*this == other); }
  bool operator!=(const ConstMap &other) const { return !(*this == other); }

  const mg_map *ptr() const { return ptr_; }

 private:
  mg_map *ptr_;
};

class ConstMap final {
 private:
  using KeyValuePair = std::pair<std::string_view, ConstValue>;

 public:
  CREATE_ITERATOR(ConstMap, KeyValuePair);

  explicit ConstMap(const mg_map *const_ptr) : const_ptr_(const_ptr) {}

  size_t size() const { return mg_map_size(const_ptr_); }

  /// Returns the value associated with the given `key`.
  /// Behaves undefined if there is no such a value. Note that each key-value
  /// pair has to be checked, resulting with O(n) time complexity.
  ConstValue operator[](const std::string_view &key) const;

  Iterator begin() const { return Iterator(this, 0); }
  Iterator end() const { return Iterator(this, size()); }

  /// Returns the key-value iterator for the given `key`.
  /// In the case there is no such pair, end iterator is returned. Note that
  /// each key-value pair has to be checked, resulting with O(n) time
  /// complexity.
  Iterator find(const std::string_view &key) const;

  bool operator==(const ConstMap &other) const;
  bool operator==(const Map &other) const;
  bool operator!=(const ConstMap &other) const { return !(*this == other); }
  bool operator!=(const Map &other) const { return !(*this == other); }

  const mg_map *ptr() const { return const_ptr_; }

 private:
  const mg_map *const_ptr_;
};

////////////////////////////////////////////////////////////////////////////////
// Value:

class Value final {
 private:
  friend class List;
  friend class Map;

 public:
  /// Types that can be stored in a `Value`.
  enum class Type {
    Null,
    Bool,
    Int,
    Double,
    String,
    List,
    Map,
    Vertex,
    Edge,
    UnboundedEdge,
    Path
  };

  /// Constructs an object that becomes the owner of the given `value`, i.e.
  /// `value` is destroyed when a `Value` object is destroyed.
  explicit Value(mg_value *ptr) : ptr_(ptr) {}

  Value(const Value &other);
  Value(Value &&other);
  Value &operator=(const Value &other) = delete;
  Value &operator=(Value &&other) = delete;
  ~Value();

  /// Empty constructor, creates Null value.
  Value() : Value(mg_value_make_null()) {}

  // Constructors for primitive types:
  explicit Value(bool value) : Value(mg_value_make_bool(value)) {}
  explicit Value(int value) : Value(mg_value_make_integer(value)) {}
  explicit Value(int64_t value) : Value(mg_value_make_integer(value)) {}
  explicit Value(double value) : Value(mg_value_make_float(value)) {}

  // Constructors for string:
  explicit Value(const std::string_view &value);
  explicit Value(const char *value);

  /// Constructs a list value and takes the ownership of the `list`.
  /// Behaviour of accessing the `list` after performing this operation is
  /// considered undefined.
  explicit Value(List &&list);

  /// Constructs a map value and takes the ownership of the `map`.
  /// Behaviour of accessing the `list` after performing this operation is
  /// considered undefined.
  explicit Value(Map &&map);

  bool ValueBool() const;
  int64_t ValueInt() const;
  double ValueDouble() const;
  std::string_view ValueString() const;
  const ConstList ValueList() const;
  const ConstMap ValueMap() const;

  Type type() const;

  ConstValue AsConstValue() const;

  bool operator==(const Value &other) const;
  bool operator==(const ConstValue &other) const;
  bool operator!=(const Value &other) const { return !(*this == other); }
  bool operator!=(const ConstValue &other) const { return !(*this == other); }

  const mg_value *ptr() const { return ptr_; }

 private:
  mg_value *ptr_;
};

class ConstValue final {
 public:
  explicit ConstValue(const mg_value *const_ptr) : const_ptr_(const_ptr) {}

  bool ValueBool() const;
  int64_t ValueInt() const;
  double ValueDouble() const;
  std::string_view ValueString() const;
  const ConstList ValueList() const;
  const ConstMap ValueMap() const;

  Value::Type type() const;

  bool operator==(const ConstValue &other) const;
  bool operator==(const Value &other) const;
  bool operator!=(const ConstValue &other) const { return !(*this == other); }
  bool operator!=(const Value &other) const { return !(*this == other); }

  const mg_value *ptr() const { return const_ptr_; }

 private:
  const mg_value *const_ptr_;
};

#undef CREATE_ITERATOR

}  // namespace mg
