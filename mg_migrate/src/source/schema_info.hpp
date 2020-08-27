#pragma once

#include <string>
#include <vector>
#include <optional>

struct SchemaInfo {
  struct Table {
    std::string schema;
    std::string name;
    std::vector<std::string> columns;
    std::vector<size_t> primary_key;
    std::vector<size_t> foreign_keys;

    /// Indicates whether there's a foreign key referencing this table's primary
    /// key.
    bool primary_key_referenced;
  };

  struct ForeignKey {
    size_t child_table;
    size_t parent_table;
    std::vector<size_t> child_columns;
    std::vector<size_t> parent_columns;
  };

  /// Pair of table and list of its columns.
  using UniqueConstraint = std::pair<size_t, std::vector<size_t>>;

  /// Pair of table and its column.
  using ExistenceConstraint = std::pair<size_t, size_t>;

  std::vector<Table> tables;
  std::vector<ForeignKey> foreign_keys;
  std::vector<UniqueConstraint> unique_constraints;
  std::vector<ExistenceConstraint> existence_constraints;
};

std::size_t GetTableIndex(const std::vector<SchemaInfo::Table> &tables,
                          const std::string_view table_schema,
                          const std::string_view table_name);

std::size_t GetColumnIndex(const std::vector<std::string> &columns,
                           const std::string_view column_name);
