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

namespace tpl::catalog {

constexpr u32 test_table_size = 2000000;

/**
 * Temporary placeholder for Terrier's actual catalog. It will be remove once
 * the catalog PR is in. Catalog::TableInfo should contain all the information
 * needed to interact with a table. Currently this makes a difference between
 * the schema needed by the storage layer, and the schema needed by the
 * execution layer. Eventually, I think we need to unify these schemas.
 */
class Catalog {
 public:
  /**
   * Contains all the information that should be needed for scans, expressions,
   * plan nodes, ... Feel free to extend it to add more stuff.
   */
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

  /**
   * Creates a table and adds it to the list of tables.
   * @param table_name the name of the table
   * @param storage_schema the schema needed by the storage layer.
   * @param sql_schema the schema needed by the execution layer.
   * @return true if the creation was successful.
   */
  bool CreateTable(const std::string &table_name, Schema &&storage_schema,
                   tpl::sql::Schema &&sql_schema);

  /**
   * Increments the oid counter and returns it.
   * @return the new oid.
   */
  table_oid_t GetNewTableOid() {
    curr_id_++;
    return table_oid_t(curr_id_);
  }

  /**
   * Increments the oid counter and returns it.
   * @return the new oid.
   */
  col_oid_t GetNewColOid() {
    curr_id_++;
    return col_oid_t(curr_id_);
  }

  /**
   * Helper method to create storage column metadata.
   * @param name of the column.
   * @param type sql::Type of the column.
   * @return the new storage column.
   */
  Schema::Column MakeStorageColumn(const std::string &name,
                                   const tpl::sql::Type &type);

  /**
   * Helper method to create sql column.
   * @param name of the column.
   * @param type sql::Type of the column.
   * @return the new storage sql column.
   */
  tpl::sql::Schema::ColumnInfo MakeSqlColumn(const std::string &name,
                                             const tpl::sql::Type &sql_type);

  /**
   * Used by tests to create test tables.
   */
  void CreateTestTables();

 private:
  friend class ExecutionStructures;

  std::unordered_map<table_oid_t, std::unique_ptr<TableInfo>> table_catalog_;
  std::unordered_map<std::string, table_oid_t> name_ids;
  u32 curr_id_ = 0;  // Global counter.
};
}  // namespace tpl::catalog
