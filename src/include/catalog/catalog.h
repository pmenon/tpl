#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "ast/identifier.h"
#include "catalog/catalog_defs.h"
#include "sql/data_types.h"
#include "sql/schema.h"
#include "storage/sql_table.h"
#include "util/common.h"

namespace terrier::catalog {

constexpr u32 test_table_size = 200000;

/// A catalog of all the databases and tables in the system. Only one exists
/// per TPL process, and its instance can be acquired through
/// \ref Catalog::instance(). At this time, the catalog is read-only after
/// startup. If you want to add a new table, modify the \ref TABLES macro which
/// lists the IDs of all tables, and configure your table in the Catalog
/// initialization function.
class Catalog {
 public:
  class TableInfo {
   public:
    TableInfo(std::unique_ptr<storage::SqlTable> &&table,
              Schema &&storage_schema, tpl::sql::Schema &&sql_schema,
              table_oid_t oid)
        : table_(std::move(table)),
          storage_schema_(std::move(storage_schema)),
          sql_schema_(std::move(sql_schema)),
          oid_(oid) {}

    storage::SqlTable *GetTable() { return table_.get(); }

    const Schema *GetStorageSchema() { return &storage_schema_; }

    const tpl::sql::Schema *GetSqlSchema() { return &sql_schema_; }

    const table_oid_t GetOid() { return oid_; }

   private:
    std::unique_ptr<storage::SqlTable> table_;
    Schema storage_schema_;
    tpl::sql::Schema sql_schema_;
    table_oid_t oid_;
  };

  /// Lookup a table in this catalog by name
  /// \param name The name of the target table
  /// \return A pointer to the table, or NULL if the table doesn't exist.
  TableInfo *LookupTableByName(const std::string &name);

  /// Lookup a table in this catalog by name, using an identifier
  /// \param name The name of the target table
  /// \return A pointer to the table, or NULL if the table doesn't exist.
  TableInfo *LookupTableByName(tpl::ast::Identifier name);

  /// Lookup a table in this catalog by ID
  /// \param table_id The ID of the target table
  /// \return A pointer to the table, or NULL if the table doesn't exist.
  TableInfo *LookupTableById(table_oid_t table_id);

  bool CreateTable(const std::string &table_name, Schema &&storage_schema,
                   tpl::sql::Schema &&sql_schema);

  table_oid_t GetNewTableOid() {
    curr_id_++;
    return table_oid_t(curr_id_);
  }

  col_oid_t GetNewColOid() {
    curr_id_++;
    return col_oid_t(curr_id_);
  }

  Schema::Column MakeStorageColumn(const std::string &name,
                                   const tpl::sql::Type &type);
  tpl::sql::Schema::ColumnInfo MakeSqlColumn(const std::string &name,
                                             const tpl::sql::Type &sql_type);

  void CreateTestTables();

 private:
  friend class ExecutionStructures;

  std::unordered_map<table_oid_t, std::unique_ptr<TableInfo>> table_catalog_;
  std::unordered_map<std::string, table_oid_t> name_ids;
  u32 curr_id_ = 0;  //
};

// Test Tables
enum TableId { EmptyTable = 1, Test1 = 2 };

}  // namespace terrier::catalog
