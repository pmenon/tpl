#pragma once

#include <string>
#include <vector>

#include "util/common.h"

namespace tpl::runtime {

// Supported SQL data types
enum class SqlTypeId : u8 { SmallInt, Integer, BigInt, Decimal };

// A physical schema for a table
class Schema {
 public:
  struct ColInfo {
    std::string name;
    SqlTypeId type_id;
  };

  Schema(std::vector<ColInfo> cols) : cols_(std::move(cols)) {}
  Schema(std::initializer_list<ColInfo> cols) : cols_(cols) {}

  const std::vector<ColInfo> columns() const { return cols_; }

 private:
  std::vector<ColInfo> cols_;
};

// A SQL table
class SqlTable {
 public:
  explicit SqlTable(Schema &&schema) : schema_(std::move(schema)) {}

  const Schema &schema() const { return schema_; }

 private:
  struct Block {
    std::vector<const byte *> columns_;
  };

 private:
  Schema schema_;
  std::vector<Block> blocks_;
};

// A SQL table iterator
class SqlTableIterator {
 public:
  explicit SqlTableIterator(SqlTable *table) : block(0), pos(0) {}

  bool Next();

 private:
  u32 block;
  u32 pos;
  std::vector<const byte *> cols;
};

// Static catalog functions

#define TEST_TABLES(V) V(Test1, "test_1")

enum class TestTableId : u8 {
#define DECLARE(Name, ...) Name
  TEST_TABLES(DECLARE)
#undef DECLARE
};

void InitTables();

SqlTable *LookupTableById(TestTableId table_id);
SqlTable *LookupTableByName(const std::string &name);

}  // namespace tpl::runtime
