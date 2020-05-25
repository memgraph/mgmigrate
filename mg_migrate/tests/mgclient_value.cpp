#include <gtest/gtest.h>

#include <malloc.h>

#include "mgclient/value.hpp"

namespace mg {

TEST(MgValueTest, NullConversion) {
  mg_value *null_value = mg_value_make_null();
  Value value(null_value);
  mg_value_destroy(null_value);
  ASSERT_EQ(value.type(), Value::Type::Null);
}

TEST(MgValueTest, BoolConversion) {
  for (const auto &x : {false, true}) {
    mg_value *bool_value = mg_value_make_bool(x);
    Value value(bool_value);
    mg_value_destroy(bool_value);
    ASSERT_EQ(value.type(), Value::Type::Bool);
    ASSERT_EQ(value.ValueBool(), x);
  }
}

TEST(MgValueTest, IntConversion) {
  mg_value *int_value = mg_value_make_integer(13);
  Value value(int_value);
  mg_value_destroy(int_value);
  ASSERT_EQ(value.type(), Value::Type::Int);
  ASSERT_EQ(value.ValueInt(), 13);
}

TEST(MgValueTest, DoubleConversion) {
  mg_value *float_value = mg_value_make_float(3.14);
  Value value(float_value);
  mg_value_destroy(float_value);
  ASSERT_EQ(value.type(), Value::Type::Double);
  ASSERT_DOUBLE_EQ(value.ValueDouble(), 3.14);
}

TEST(MgValueTest, StringConversion) {
  for (const std::string &str : {"", "Some 'value'"}) {
    mg_value *string_value = mg_value_make_string(str.c_str());
    Value value(string_value);
    mg_value_destroy(string_value);
    ASSERT_EQ(value.type(), Value::Type::String);
    ASSERT_EQ(value.ValueString(), str);
  }
}

TEST(MgValueTest, ListConversion) {
  mg_list *list = mg_list_make_empty(3);
  mg_list_append(list, mg_value_make_null());
  mg_list_append(list, mg_value_make_integer(2));
  mg_list_append(list, mg_value_make_string("hello"));
  mg_value *list_value = mg_value_make_list(list);
  Value value(list_value);
  mg_value_destroy(list_value);

  ASSERT_EQ(value.type(), Value::Type::List);
  const auto &value_list = value.ValueList();
  ASSERT_EQ(value_list.size(), 3U);
  ASSERT_EQ(value_list[0].type(), Value::Type::Null);
  ASSERT_EQ(value_list[1].ValueInt(), 2);
  ASSERT_EQ(value_list[2].ValueString(), "hello");
}

TEST(MgValueTest, MapConversion) {
  mg_map *map = mg_map_make_empty(3);
  mg_map_insert(map, "name", mg_value_make_string("Bosko"));
  mg_map_insert(map, "age", mg_value_make_integer(25));
  mg_map_insert(map, "height", mg_value_make_float(1.79));
  mg_value *map_value = mg_value_make_map(map);
  Value value(map_value);
  mg_value_destroy(map_value);

  ASSERT_EQ(value.type(), Value::Type::Map);
  const auto &value_map = value.ValueMap();
  ASSERT_EQ(value_map.size(), 3U);

  auto assert_entry_existence = [&value_map](const std::string &key,
                                             const Value &value) {
    auto it = value_map.find(key);
    ASSERT_NE(it, value_map.end());
    ASSERT_EQ(it->second, value);
  };

  assert_entry_existence("name", Value("Bosko"));
  assert_entry_existence("age", Value(25));
  assert_entry_existence("height", Value(1.79));
}

}  // namespace mg
