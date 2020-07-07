#pragma once

#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

#include <mgclient.h>

#include "utils/cast.hpp"

namespace mg {

// Forward declarations:
class ConstList;
class ConstMap;
class ConstNode;
class ConstRelationship;
class ConstUnboundRelationship;
class ConstPath;
class ConstValue;
class Value;

#define CREATE_ITERATOR(container, element)                                    \
  class Iterator {                                                             \
   private:                                                                    \
    friend class container;                                                    \
                                                                               \
   public:                                                                     \
    bool operator==(const Iterator &other) const {                             \
      return iterable_ == other.iterable_ && index_ == other.index_;           \
    }                                                                          \
                                                                               \
    bool operator!=(const Iterator &other) const { return !(*this == other); } \
                                                                               \
    Iterator &operator++() {                                                   \
      index_++;                                                                \
      return *this;                                                            \
    }                                                                          \
                                                                               \
    element operator*() const;                                                 \
                                                                               \
   private:                                                                    \
    Iterator(const container *iterable, size_t index)                          \
        : iterable_(iterable), index_(index) {}                                \
                                                                               \
    const container *iterable_;                                                \
    size_t index_;                                                             \
  }

/// Wraps int64_t to prevent dangerous implicit conversions.
class Id {
 public:
  Id() = default;

  /// Construct Id from uint64_t
  static Id FromUint(uint64_t id) { return Id(utils::MemcpyCast<int64_t>(id)); }

  /// Construct Id from int64_t
  static Id FromInt(int64_t id) { return Id(id); }

  int64_t AsInt() const { return id_; }
  uint64_t AsUint() const { return utils::MemcpyCast<uint64_t>(id_); }

 private:
  explicit Id(int64_t id) : id_(id) {}

  int64_t id_;
};

inline bool operator==(const Id &id1, const Id &id2) {
  return id1.AsInt() == id2.AsInt();
}

inline bool operator!=(const Id &id1, const Id &id2) { return !(id1 == id2); }

////////////////////////////////////////////////////////////////////////////////
/// List:

class List final {
 private:
  friend class Value;

 public:
  CREATE_ITERATOR(List, ConstValue);

  explicit List(mg_list *ptr) : ptr_(ptr) {}

  explicit List(const mg_list *const_ptr) : List(mg_list_copy(const_ptr)) {}

  List(const List &other);
  List(List &&other);
  List &operator=(const List &other) = delete;
  List &operator=(List &&other) = delete;

  ~List();

  /// Copies the given list.
  explicit List(const ConstList &list);

  explicit List(size_t capacity) : List(mg_list_make_empty(capacity)) {}

  List(std::initializer_list<Value> list);

  size_t size() const { return mg_list_size(ptr_); }

  bool empty() const { return size() == 0; }

  /// Returns the value at the given `index`.
  const ConstValue operator[](size_t index) const;

  Iterator begin() const { return Iterator(this, 0); }
  Iterator end() const { return Iterator(this, size()); }

  /// Appends the given `value` to the list. It copies the `value`.
  bool Append(const Value &value);

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
  bool empty() const { return size() == 0; }
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

  explicit Map(const mg_map *const_ptr) : Map(mg_map_copy(const_ptr)) {}

  Map(const Map &other);
  Map(Map &&other);
  Map &operator=(const Map &other) = delete;
  Map &operator=(Map &&other) = delete;
  ~Map();

  /// Copies content of the given `map`.
  explicit Map(const ConstMap &map);

  /// Constructs an empty map of the given `capacity`.
  explicit Map(size_t capacity) : Map(mg_map_make_empty(capacity)) {}

  /// Constructs an map from the list of key-value pairs. Values are copied.
  Map(std::initializer_list<std::pair<std::string, Value>> list);

  size_t size() const { return mg_map_size(ptr_); }

  bool empty() const { return size() == 0; }

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
  /// Checks if the given `key` already exists by iterating over all entries.
  /// Copies both the `key` and the `value`.
  bool Insert(const std::string_view &key, const Value &value);

  /// Inserts the given `key`-`value` pair into the map.
  /// Checks if the given `key` already exists by iterating over all entries.
  /// Copies both the `key` and the `value`.
  bool Insert(const std::string_view &key, const ConstValue &value);

  /// Inserts the given `key`-`value` pair into the map.
  /// Checks if the given `key` already exists by iterating over all entries.
  /// Copies the `key` and takes the ownership of `value` by moving it.
  /// Behaviour of accessing the `value` after performing this operation is
  /// considered undefined.
  bool Insert(const std::string_view &key, Value &&value);

  /// Inserts the given `key`-`value` pair into the map. It doesn't check if the
  /// given `key` already exists in the map. Copies both the `key` and the
  /// `value`.
  bool InsertUnsafe(const std::string_view &key, const Value &value);

  /// Inserts the given `key`-`value` pair into the map. It doesn't check if the
  /// given `key` already exists in the map. Copies both the `key` and the
  /// `value`.
  bool InsertUnsafe(const std::string_view &key, const ConstValue &value);

  /// Inserts the given `key`-`value` pair into the map. It doesn't check if the
  /// given `key` already exists in the map. Copies the `key` and takes the
  /// ownership of `value` by moving it. Behaviour of accessing the `value`
  /// after performing this operation is considered undefined.
  bool InsertUnsafe(const std::string_view &key, Value &&value);

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

  bool empty() const { return size() == 0; }

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
// Node:

class Node final {
 private:
  friend class Value;

 public:
  class Labels final {
   public:
    CREATE_ITERATOR(Labels, std::string_view);

    explicit Labels(const mg_node *node) : node_(node) {}

    size_t size() const { return mg_node_label_count(node_); }

    std::string_view operator[](size_t index) const;

    Iterator begin() { return Iterator(this, 0); }
    Iterator end() { return Iterator(this, size()); }

   private:
    const mg_node *node_;
  };

  explicit Node(mg_node *ptr) : ptr_(ptr) {}

  explicit Node(const mg_node *const_ptr) : Node(mg_node_copy(const_ptr)) {}

  Node(const Node &other);
  Node(Node &&other);
  Node &operator=(const Node &other) = delete;
  Node &operator=(Node &&other) = delete;
  ~Node();

  explicit Node(const ConstNode &node);

  Id id() const { return Id::FromInt(mg_node_id(ptr_)); }

  Labels labels() const { return Labels(ptr_); }

  ConstMap properties() const { return ConstMap(mg_node_properties(ptr_)); }

  ConstNode AsConstNode() const;

  bool operator==(const Node &other) const;
  bool operator==(const ConstNode &other) const;
  bool operator!=(const Node &other) const { return !(*this == other); }
  bool operator!=(const ConstNode &other) const { return !(*this == other); }

  const mg_node *ptr() const { return ptr_; }

 private:
  mg_node *ptr_;
};

class ConstNode final {
 public:
  explicit ConstNode(const mg_node *const_ptr) : const_ptr_(const_ptr) {}

  Id id() const { return Id::FromInt(mg_node_id(const_ptr_)); }

  Node::Labels labels() const { return Node::Labels(const_ptr_); }

  ConstMap properties() const {
    return ConstMap(mg_node_properties(const_ptr_));
  }

  bool operator==(const ConstNode &other) const;
  bool operator==(const Node &other) const;
  bool operator!=(const ConstNode &other) const { return !(*this == other); }
  bool operator!=(const Node &other) const { return !(*this == other); }

  const mg_node *ptr() const { return const_ptr_; }

 private:
  const mg_node *const_ptr_;
};

////////////////////////////////////////////////////////////////////////////////
// Relationship:

class Relationship final {
 private:
  friend class Value;

 public:
  explicit Relationship(mg_relationship *ptr) : ptr_(ptr) {}

  explicit Relationship(const mg_relationship *const_ptr)
      : Relationship(mg_relationship_copy(const_ptr)) {}

  Relationship(const Relationship &other);
  Relationship(Relationship &&other);
  Relationship &operator=(const Relationship &other) = delete;
  Relationship &operator=(Relationship &&other) = delete;
  ~Relationship();

  explicit Relationship(const ConstRelationship &rel);

  Id id() const { return Id::FromInt(mg_relationship_id(ptr_)); }

  Id from() const { return Id::FromInt(mg_relationship_start_id(ptr_)); }

  Id to() const { return Id::FromInt(mg_relationship_end_id(ptr_)); }

  std::string_view type() const;

  ConstMap properties() const {
    return ConstMap(mg_relationship_properties(ptr_));
  }

  ConstRelationship AsConstRelationship() const;

  bool operator==(const Relationship &other) const;
  bool operator==(const ConstRelationship &other) const;
  bool operator!=(const Relationship &other) const { return !(*this == other); }
  bool operator!=(const ConstRelationship &other) const {
    return !(*this == other);
  }

  const mg_relationship *ptr() const { return ptr_; }

 private:
  mg_relationship *ptr_;
};

class ConstRelationship final {
 public:
  explicit ConstRelationship(const mg_relationship *const_ptr)
      : const_ptr_(const_ptr) {}

  Id id() const { return Id::FromInt(mg_relationship_id(const_ptr_)); }

  Id from() const { return Id::FromInt(mg_relationship_start_id(const_ptr_)); }

  Id to() const { return Id::FromInt(mg_relationship_end_id(const_ptr_)); }

  std::string_view type() const;

  ConstMap properties() const {
    return ConstMap(mg_relationship_properties(const_ptr_));
  }

  bool operator==(const ConstRelationship &other) const;
  bool operator==(const Relationship &other) const;
  bool operator!=(const ConstRelationship &other) const {
    return !(*this == other);
  }
  bool operator!=(const Relationship &other) const { return !(*this == other); }

  const mg_relationship *ptr() const { return const_ptr_; }

 private:
  const mg_relationship *const_ptr_;
};

////////////////////////////////////////////////////////////////////////////////
// UnboundRelationship:

class UnboundRelationship final {
 private:
  friend class Value;

 public:
  explicit UnboundRelationship(mg_unbound_relationship *ptr) : ptr_(ptr) {}

  explicit UnboundRelationship(const mg_unbound_relationship *const_ptr)
      : UnboundRelationship(mg_unbound_relationship_copy(const_ptr)) {}

  UnboundRelationship(const UnboundRelationship &other);
  UnboundRelationship(UnboundRelationship &&other);
  UnboundRelationship &operator=(const UnboundRelationship &other) = delete;
  UnboundRelationship &operator=(UnboundRelationship &&other) = delete;
  ~UnboundRelationship();

  explicit UnboundRelationship(const ConstUnboundRelationship &rel);

  Id id() const { return Id::FromInt(mg_unbound_relationship_id(ptr_)); }

  std::string_view type() const;

  ConstMap properties() const {
    return ConstMap(mg_unbound_relationship_properties(ptr_));
  }

  ConstUnboundRelationship AsConstUnboundRelationship() const;

  bool operator==(const UnboundRelationship &other) const;
  bool operator==(const ConstUnboundRelationship &other) const;
  bool operator!=(const UnboundRelationship &other) const {
    return !(*this == other);
  }
  bool operator!=(const ConstUnboundRelationship &other) const {
    return !(*this == other);
  }

  const mg_unbound_relationship *ptr() const { return ptr_; }

 private:
  mg_unbound_relationship *ptr_;
};

class ConstUnboundRelationship final {
 public:
  explicit ConstUnboundRelationship(const mg_unbound_relationship *const_ptr)
      : const_ptr_(const_ptr) {}

  Id id() const { return Id::FromInt(mg_unbound_relationship_id(const_ptr_)); }

  std::string_view type() const;

  ConstMap properties() const {
    return ConstMap(mg_unbound_relationship_properties(const_ptr_));
  }

  bool operator==(const ConstUnboundRelationship &other) const;
  bool operator==(const UnboundRelationship &other) const;
  bool operator!=(const ConstUnboundRelationship &other) const {
    return !(*this == other);
  }
  bool operator!=(const UnboundRelationship &other) const {
    return !(*this == other);
  }

  const mg_unbound_relationship *ptr() const { return const_ptr_; }

 private:
  const mg_unbound_relationship *const_ptr_;
};

////////////////////////////////////////////////////////////////////////////////
// Path:

class Path final {
 private:
  friend class Value;

 public:
  explicit Path(mg_path *ptr) : ptr_(ptr) {}

  explicit Path(const mg_path *const_ptr) : Path(mg_path_copy(const_ptr)) {}

  Path(const Path &other);
  Path(Path &&other);
  Path &operator=(const Path &other);
  Path &operator=(Path &&other);
  ~Path();

  explicit Path(const ConstPath &path);

  /// Length of the path in number of edges.
  size_t length() const { return mg_path_length(ptr_); }

  /// Returns the vertex at the given `index`, which should be less than or
  /// equal to length of the path.
  ConstNode GetNodeAt(size_t index) const;

  /// Returns the edge at the given `index`, which should be less than length of
  /// the path.
  ConstUnboundRelationship GetRelationshipAt(size_t index) const;

  /// Returns the orientation of the edge at the given `index`, which should be
  /// less than length of the path. Returns true if the edge is reversed, false
  /// otherwise.
  bool IsReversedRelationshipAt(size_t index) const;

  ConstPath AsConstPath() const;

  bool operator==(const Path &other) const;
  bool operator==(const ConstPath &other) const;
  bool operator!=(const Path &other) const { return !(*this == other); }
  bool operator!=(const ConstPath &other) const { return !(*this == other); }

  const mg_path *ptr() const { return ptr_; }

 private:
  mg_path *ptr_;
};

class ConstPath final {
 public:
  explicit ConstPath(const mg_path *const_ptr) : const_ptr_(const_ptr) {}

  /// Length of the path in number of edges.
  size_t length() const { return mg_path_length(const_ptr_); }

  /// Returns the vertex at the given `index`, which should be less than or
  /// equal to length of the path.
  ConstNode GetNodeAt(size_t index) const;

  /// Returns the edge at the given `index`, which should be less than length of
  /// the path.
  ConstUnboundRelationship GetRelationshipAt(size_t index) const;

  /// Returns the orientation of the edge at the given `index`, which should be
  /// less than length of the path. Returns true if the edge is reversed, false
  /// otherwise.
  bool IsReversedRelationshipAt(size_t index) const;

  bool operator==(const ConstPath &other) const;
  bool operator==(const Path &other) const;
  bool operator!=(const ConstPath &other) const { return !(*this == other); }
  bool operator!=(const Path &other) const { return !(*this == other); }

  const mg_path *ptr() const { return const_ptr_; }

 private:
  const mg_path *const_ptr_;
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
    Node,
    Relationship,
    UnboundRelationship,
    Path
  };

  /// Constructs an object that becomes the owner of the given `value`, i.e.
  /// `value` is destroyed when a `Value` object is destroyed.
  explicit Value(mg_value *ptr) : ptr_(ptr) {}

  /// Constructor that copies the given value.
  explicit Value(const mg_value *const_ptr) : Value(mg_value_copy(const_ptr)) {}

  Value(const Value &other);
  Value(Value &&other);
  Value &operator=(const Value &other) = delete;
  Value &operator=(Value &&other) = delete;
  ~Value();

  explicit Value(const ConstValue &value);

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
  /// Behaviour of accessing the `map` after performing this operation is
  /// considered undefined.
  explicit Value(Map &&map);

  /// Constructs a vertex value and takes the ownership of the given `vertex`.
  /// Behaviour of accessing the `vertex` after performing this operation is
  /// considered undefined.
  explicit Value(Node &&vertex);

  /// Constructs an edge value and takes the ownership of the given `edge`.
  /// Behaviour of accessing the `edge` after performing this operation is
  /// considered undefined.
  explicit Value(Relationship &&edge);

  /// Constructs an unbounded edge value and takes the ownership of the given
  /// `edge`. Behaviour of accessing the `edge` after performing this operation
  /// is considered undefined.
  explicit Value(UnboundRelationship &&edge);

  /// Constructs a path value and takes the ownership of the given `path`.
  /// Behaviour of accessing the `path` after performing this operation is
  /// considered undefined.
  explicit Value(Path &&path);

  bool ValueBool() const;
  int64_t ValueInt() const;
  double ValueDouble() const;
  std::string_view ValueString() const;
  const ConstList ValueList() const;
  const ConstMap ValueMap() const;
  const ConstNode ValueNode() const;
  const ConstRelationship ValueRelationship() const;
  const ConstUnboundRelationship ValueUnboundRelationship() const;
  const ConstPath ValuePath() const;

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
  const ConstNode ValueNode() const;
  const ConstRelationship ValueRelationship() const;
  const ConstUnboundRelationship ValueUnboundRelationship() const;
  const ConstPath ValuePath() const;

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
