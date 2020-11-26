#include "source/schema_info.hpp"

#include <algorithm>

#include <glog/logging.h>

size_t GetTableIndex(const std::vector<SchemaInfo::Table> &tables,
                     const std::string_view table_schema,
                     const std::string_view table_name) {
  for (size_t i = 0; i < tables.size(); ++i) {
    if (tables[i].schema == table_schema && tables[i].name == table_name) {
      return i;
    }
  }

  LOG(FATAL) << "Couldn't find table '" << table_name << "' in schema '"
             << table_schema << "'!";
  return 0;
}

size_t GetColumnIndex(const std::vector<std::string> &columns,
                      const std::string_view column_name) {
  auto const it = std::find(columns.begin(), columns.end(), column_name);
  CHECK(it != columns.end())
      << "Couldn't find column name '" << column_name << "'!";

  return it - columns.begin();
}
