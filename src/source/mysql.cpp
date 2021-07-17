#include "source/mysql.hpp"

#include <glog/logging.h>
#include <mysqlx/devapi/common.h>
#include <mysqlx/devapi/document.h>
#include <mysqlx/devapi/result.h>
#include <mysqlx/xdevapi.h>

#include <algorithm>
#include <cctype>
#include <mgclient-value.hpp>

#include "source/schema_info.hpp"

namespace {
const std::string kSchemaBlacklist =
    "('information_schema', 'sys', 'mysql', 'performance_schema')";

std::vector<std::pair<std::string, std::string>> ListAllTables(
    const MysqlClient &client) {
  std::vector<std::pair<std::string, std::string>> table_names;
  try {
    auto rows =
        client.session()
            ->getSchema("information_schema")
            .getTable("tables")
            .select("table_schema", "table_name")
            .where("table_type = 'BASE TABLE' AND table_schema NOT IN " +
                   kSchemaBlacklist)
            .execute();

    CHECK(rows.count() > 0) << "No tables found in the database!";
    table_names.reserve(rows.count());
    for (auto row = rows.fetchOne(); !row.isNull(); row = rows.fetchOne()) {
      CHECK(row.colCount() == 2)
          << "Recieved wrong number of columns while listing tables!";

      const auto &table_schema{row.get(0)};
      const auto &table_name{row.get(0)};
      CHECK(table_schema.getType() == mysqlx::Value::Type::STRING &&
            table_name.getType() == mysqlx::Value::Type::STRING)
          << "Received unexpected results while listing tables!";

      table_names.emplace_back(row.get(0).get<std::string>(),
                               row.get(1).get<std::string>());
      DLOG(INFO) << "Found a table '" << table_names.back().second
                 << "' in schema '" << table_names.back().first << "'";
    }
  } catch (const mysqlx::Error &e) {
    LOG(FATAL) << "Failed to list all tables: " << e.what();
  }

  return table_names;
}

std::vector<std::string> ListColumnsForTable(const MysqlClient &client,
                                             const std::string &table_schema,
                                             const std::string &table_name) {
  DLOG(INFO) << "Listing columns for table '" << table_name << "' in schema '"
             << table_schema << "'";

  std::vector<std::string> columns;
  try {
    auto rows = client.session()
                    ->getSchema("information_schema")
                    .getTable("columns")
                    .select("column_name")
                    .where(
                        "table_schema=:table_schema"
                        " AND table_name=:table_name")
                    .bind("table_schema", table_schema)
                    .bind("table_name", table_name)
                    .execute();
    CHECK(rows.count() > 0)
        << "Failed to fetch columns for table '" << table_name
        << "' in schema '" << table_schema << "'!";

    columns.reserve(rows.count());
    for (auto row = rows.fetchOne(); !row.isNull(); row = rows.fetchOne()) {
      CHECK(row.colCount() == 1)
          << "Received wrong number of columns while listing columns of table '"
          << table_name << "' in schema '" << table_schema << "'!";

      const auto &column_name{row.get(0)};
      CHECK(column_name.getType() == mysqlx::Value::Type::STRING)
          << "Received unexpected result while listing columns of table '"
          << table_name << "' in schema '" << table_schema << "'!";
      columns.emplace_back(column_name.get<std::string>());

      DLOG(INFO) << "Found column '" << columns.back() << "'";
    }
  } catch (const mysqlx::Error &e) {
    LOG(FATAL) << "Failed to list columns for table '" << table_name
               << "' in schema '" << table_schema << "'!";
  }
  return columns;
}

std::vector<std::string> ListPrimaryKeyColumnsForTable(
    const MysqlClient &client, const std::string &table_schema,
    const std::string &table_name) {
  DLOG(INFO) << "Listing primary key columns for table '" << table_name
             << "' in schema '" << table_schema << "'";
  std::vector<std::string> primary_keys;
  try {
    auto rows = client.session()
                    ->sql("SHOW KEYS FROM " + table_schema + "." + table_name +
                          " WHERE Key_name='PRIMARY'")
                    .execute();
    if (rows.count() == 0) {
      LOG(WARNING) << "No primary keys found for '" << table_name
                   << "' in schema '" << table_schema << "'!";
      return {};
    }

    const auto &columns = rows.getColumns();
    const auto to_upper = [](std::string input) {
      std::transform(input.begin(), input.end(), input.begin(),
                     [](unsigned char c) { return std::toupper(c); });
      return input;
    };
    const auto column_name_it =
        std::find_if(columns.begin(), columns.end(), [&](const auto &column) {
          return to_upper(column.getColumnName()) == "COLUMN_NAME";
        });
    if (column_name_it == columns.end()) {
      LOG(FATAL) << "Failed to retrieve primary keys!";
    }
    const size_t col_index = column_name_it - columns.begin();

    primary_keys.reserve(rows.count());
    for (auto row = rows.fetchOne(); !row.isNull(); row = rows.fetchOne()) {
      CHECK(row.colCount() >= col_index &&
            row.get(col_index).getType() == mysqlx::Value::Type::STRING)
          << "Received unexpected result while trying to list primary keys of "
             "table '"
          << table_name << "' in schema '" << table_schema << "'!";

      primary_keys.emplace_back(row.get(col_index).get<std::string>());
      DLOG(INFO) << "'" << primary_keys.back()
                 << "' is part of the primary key";
    }
  } catch (const mysqlx::Error &e) {
    LOG(FATAL) << "Failed to fetch primary keys for table '" << table_name
               << "' in schema '" << table_schema << "':" << e.what();
  }

  return primary_keys;
}

std::vector<SchemaInfo::ForeignKey> ListAllForeignKeys(
    const MysqlClient &client, const std::vector<SchemaInfo::Table> &tables) {
  DLOG(INFO) << "Listing all foreign keys";
  std::vector<SchemaInfo::ForeignKey> foreign_keys;
  try {
    auto rows =
        client.session()
            ->sql(
                "SELECT"
                "  constraints.constraint_name,"
                "  child.table_schema,"
                "  child.table_name,"
                "  child.column_name,"
                "  child.referenced_table_schema,"
                "  child.referenced_table_name,"
                "  child.referenced_column_name "
                "FROM"
                "  information_schema.referential_constraints AS constraints"
                "  JOIN information_schema.key_column_usage AS child"
                "    USING (constraint_schema, constraint_name)"
                "  JOIN information_schema.key_column_usage AS parent"
                "    ON parent.ordinal_position = "
                "child.position_in_unique_constraint"
                "   AND parent.constraint_name = constraints.constraint_name "
                "WHERE constraints.constraint_schema NOT IN " +
                kSchemaBlacklist + "  AND child.table_schema NOT IN " +
                kSchemaBlacklist + "  AND parent.table_schema NOT IN " +
                kSchemaBlacklist +
                " ORDER BY constraints.constraint_name, child.ordinal_position")
            .execute();
    if (rows.count() == 0) {
      LOG(WARNING) << "No foreign keys found!";
      return {};
    }
    foreign_keys.reserve(rows.count());
    SchemaInfo::ForeignKey current_foreign_key;
    std::string prev_foreign_key_name;
    for (auto row = rows.fetchOne(); !row.isNull(); row = rows.fetchOne()) {
      CHECK(row.colCount() == 7)
          << "Received unexpected result while listing foreign keys!";

      for (mysqlx::col_count_t i = 0; i < row.colCount(); ++i) {
        CHECK(row.get(i).getType() == mysqlx::Value::Type::STRING)
            << "Received unexpected result while listing foreign keys!";
      }

      const auto &foreign_key_name = row.get(0).get<std::string>();
      const auto child_table = GetTableIndex(
          tables, row.get(1).get<std::string>(), row.get(2).get<std::string>());
      const auto child_column = GetColumnIndex(tables[child_table].columns,
                                               row.get(3).get<std::string>());
      const auto parent_table = GetTableIndex(
          tables, row.get(4).get<std::string>(), row.get(5).get<std::string>());
      const auto parent_column = GetColumnIndex(tables[parent_table].columns,
                                                row.get(6).get<std::string>());

      if (foreign_key_name != prev_foreign_key_name) {
        DLOG(INFO) << "Found foreign key '" << foreign_key_name << "'";
        if (!current_foreign_key.child_columns.empty()) {
          foreign_keys.push_back(current_foreign_key);
          current_foreign_key.child_columns.clear();
          current_foreign_key.parent_columns.clear();
        }
        current_foreign_key.child_table = child_table;
        current_foreign_key.parent_table = parent_table;
      }

      current_foreign_key.child_columns.emplace_back(child_column);
      current_foreign_key.parent_columns.emplace_back(parent_column);
      prev_foreign_key_name = foreign_key_name;
    }
    if (!current_foreign_key.child_columns.empty()) {
      foreign_keys.push_back(current_foreign_key);
    }
  } catch (const mysqlx::Error &e) {
    LOG(FATAL) << "Failed to fetch foreign keys: " << e.what();
  }

  return foreign_keys;
}

std::vector<SchemaInfo::ExistenceConstraint> ListAllExistenceConstraints(
    const MysqlClient &client, const std::vector<SchemaInfo::Table> &tables) {
  DLOG(INFO) << "Listing all existence constraints!";
  std::vector<SchemaInfo::ExistenceConstraint> existence_constraints;
  try {
    auto rows = client.session()
                    ->getSchema("information_schema")
                    .getTable("columns")
                    .select("table_schema", "table_name", "column_name")
                    .where("is_nullable = 'NO' AND table_schema NOT IN " +
                           kSchemaBlacklist)
                    .execute();

    if (rows.count() == 0) {
      LOG(WARNING) << "No existence constraints were found!";
      return {};
    }
    existence_constraints.reserve(rows.count());
    for (auto row = rows.fetchOne(); !row.isNull(); row = rows.fetchOne()) {
      CHECK(row.colCount() == 3)
          << "Received unexpected result while listing existence constraints!";
      for (mysqlx::col_count_t i = 0; i < row.colCount(); ++i) {
        CHECK(row.get(i).getType() == mysqlx::Value::Type::STRING)
            << "Recevied unexpected result while listing existence "
               "constraints!";
      }
      const auto table = GetTableIndex(tables, row.get(0).get<std::string>(),
                                       row.get(1).get<std::string>());
      const auto column =
          GetColumnIndex(tables[table].columns, row.get(2).get<std::string>());
      existence_constraints.emplace_back(table, column);
    }

  } catch (const mysqlx::Error &e) {
    LOG(FATAL) << "Failed to list existence constraints: " << e.what();
  }

  return existence_constraints;
}

std::vector<SchemaInfo::UniqueConstraint> ListAllUniqueConstraints(
    const MysqlClient &client, const std::vector<SchemaInfo::Table> &tables) {
  DLOG(INFO) << "Listing all unique constraints";
  std::vector<SchemaInfo::UniqueConstraint> unique_constraints;
  try {
    auto rows = client.session()
                    ->sql(
                        "SELECT"
                        " tc.constraint_name,"
                        " tc.table_schema,"
                        " tc.table_name,"
                        " kcu.column_name "
                        "FROM"
                        " information_schema.table_constraints AS tc"
                        " JOIN information_schema.key_column_usage as kcu"
                        "   USING (constraint_name, table_schema, table_name) "
                        "WHERE tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY') "
                        "AND tc.table_schema NOT IN " +
                        kSchemaBlacklist + "ORDER BY tc.constraint_name")
                    .execute();
    if (rows.count() == 0) {
      LOG(WARNING) << "No unique constraints found!";
      return {};
    }
    unique_constraints.reserve(rows.count());
    SchemaInfo::UniqueConstraint current_constraint;
    // MySQL names every primary key constraint PRIMARY
    // so unique constraint is defined by constraint name,
    // table name and schema name
    std::optional<size_t> prev_table;
    std::string prev_constraint_name;
    for (auto row = rows.fetchOne(); !row.isNull(); row = rows.fetchOne()) {
      CHECK(row.colCount() == 4)
          << "Received unexpected result while listing unique constraints!";
      for (mysqlx::col_count_t i = 0; i < row.colCount(); ++i) {
        CHECK(row.get(i).getType() == mysqlx::Value::Type::STRING)
            << "Received unexpected result while listing unique constraints!";
      }

      const auto &constraint_name = row.get(0).get<std::string>();
      const auto table = GetTableIndex(tables, row.get(1).get<std::string>(),
                                       row.get(2).get<std::string>());
      const auto column =
          GetColumnIndex(tables[table].columns, row.get(3).get<std::string>());
      if (prev_constraint_name != constraint_name ||
          (prev_table && prev_table != table)) {
        if (!current_constraint.second.empty()) {
          unique_constraints.push_back(current_constraint);
          current_constraint.second.clear();
        }
        current_constraint.first = table;
      }
      current_constraint.second.push_back(column);
      prev_constraint_name = constraint_name;
      prev_table = table;
    }
    if (!current_constraint.second.empty()) {
      unique_constraints.push_back(current_constraint);
    }
  } catch (const mysqlx::Error &e) {
    LOG(FATAL) << "Failed to list all unique constraints: " << e.what();
  }
  return unique_constraints;
}

mg::Value ConvertField(const mysqlx::Value &value);

mg::List ConvertToList(const mysqlx::Value &value) {
  std::vector<mg::Value> list_values;
  list_values.reserve(value.elementCount());
  std::transform(value.begin(), value.end(), std::back_inserter(list_values),
                 ConvertField);
  return mg::List(std::move(list_values));
}

mg::Value ConvertField(const mysqlx::Value &value) {
  using namespace mysqlx;
  switch (value.getType()) {
    case Value::Type::UINT64:
      LOG(WARNING) << "Converting unsigned integer to signed integer";
    case Value::Type::INT64:
      return mg::Value(value.get<int64_t>());
    case Value::Type::FLOAT:
    case Value::Type::DOUBLE:
      return mg::Value(value.get<double>());
    case Value::Type::BOOL:
      return mg::Value(value.get<bool>());
    case Value::Type::STRING:
      return mg::Value(value.get<std::string>());
    case Value::Type::ARRAY:
      return mg::Value(ConvertToList(value));
    case Value::Type::DOCUMENT:
    case Value::Type::RAW:
      LOG(FATAL) << "Document and raw bytes are currently not supported";
    case Value::Type::VNULL:
      return mg::Value();
  }

  // if there was some unknown type, the safest thing is to return null Value
  return mg::Value();
}

std::vector<mg::Value> ConvertRow(mysqlx::Row &row) {
  std::vector<mg::Value> values;
  values.reserve(row.colCount());
  for (mysqlx::col_count_t i = 0; i < row.colCount(); ++i) {
    values.push_back(ConvertField(row.get(i)));
  }
  return values;
}
}  // namespace

std::unique_ptr<MysqlClient> MysqlClient::Connect(
    const MysqlClient::Params &params) {
  try {
    auto session = std::make_unique<mysqlx::Session>(
        mysqlx::SessionOption::HOST, params.host, mysqlx::SessionOption::PORT,
        params.port, mysqlx::SessionOption::AUTH, mysqlx::AuthMethod::PLAIN,
        mysqlx::SessionOption::USER, params.username,
        mysqlx::SessionOption::PWD, params.password, mysqlx::SessionOption::DB,
        params.database, mysqlx::SessionOption::SSL_MODE,
        mysqlx::SSLMode::REQUIRED);

    return std::unique_ptr<MysqlClient>(new MysqlClient(std::move(session)));
  } catch (const mysqlx::Error &e) {
    LOG(ERROR) << "Unable to connect to MySQL server: " << e.what();
    return nullptr;
  }
}

SchemaInfo MysqlSource::GetSchemaInfo() {
  const auto table_names = ListAllTables(*client_);

  std::vector<SchemaInfo::Table> tables;
  tables.reserve(table_names.size());

  for (const auto &[table_schema, table_name] : table_names) {
    const auto columns =
        ListColumnsForTable(*client_, table_schema, table_name);
    const auto primary_key_columns =
        ListPrimaryKeyColumnsForTable(*client_, table_schema, table_name);

    std::vector<size_t> primary_key;
    primary_key.reserve(primary_key_columns.size());

    DLOG(INFO) << "Finding indices of the primary key fields";
    for (const auto &column_name : primary_key_columns) {
      const auto it{std::find(columns.begin(), columns.end(), column_name)};
      CHECK(it != columns.end())
          << "Coudn't find a primary key field '" << column_name
          << "' in the table '" << table_name << "'!";
      primary_key.push_back(it - columns.begin());
    }

    tables.push_back({table_schema, table_name, std::move(columns),
                      std::move(primary_key), std::vector<size_t>(), false});
  }

  auto foreign_keys = ListAllForeignKeys(*client_, tables);
  for (size_t i = 0; i < foreign_keys.size(); ++i) {
    tables[foreign_keys[i].child_table].foreign_keys.push_back(i);
    tables[foreign_keys[i].parent_table].primary_key_referenced = true;
  }

  auto existence_constraints = ListAllExistenceConstraints(*client_, tables);
  auto unique_constriants = ListAllUniqueConstraints(*client_, tables);

  return {std::move(tables), std::move(foreign_keys),
          std::move(unique_constriants), std::move(existence_constraints)};
}

void MysqlSource::ReadTable(
    const SchemaInfo::Table &table,
    std::function<void(const std::vector<mg::Value> &)> callback) {
  DLOG(INFO) << "Reading data from table '" << table.name << "' in schema '"
             << table.schema << "'";

  try {
    auto rows = client_->session()
                    ->getSchema(table.schema)
                    .getTable(table.name)
                    .select(table.columns)
                    .execute();
    if (rows.count() == 0) {
      LOG(WARNING) << "Table '" << table.name << "' in schema '" << table.schema
                   << "' is empty!";
      return;
    }
    for (auto row = rows.fetchOne(); !row.isNull(); row = rows.fetchOne()) {
      CHECK(row.colCount() == table.columns.size())
          << "Received unexpected results from table '" << table.name
          << "' in schema '" << table.schema << "'!";
      std::vector<mg::Value> mg_row = ConvertRow(row);
      callback(mg_row);
    }
  } catch (mysqlx::Error &e) {
    LOG(FATAL) << "Failed to read table '" << table.name << "' in schema '"
               << table.schema << "':" << e.what();
  }
}
