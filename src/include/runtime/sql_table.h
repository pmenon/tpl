#pragma once

#include <string>
#include <vector>

#include "util/common.h"

namespace tpl::runtime {

// Supported SQL data types
enum class SqlTypeId : u8 { SmallInt, Integer, BigInt, Decimal };

// A physical schema for a table
struct Schema {
  struct ColInfo {
    std::string name;
    SqlTypeId type_id;
  };

  std::vector<ColInfo> cols;
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
struct SqlTableIterator {
  u32 block;
  u32 pos;
  std::vector<const byte *> cols;

  explicit SqlTableIterator(SqlTable *table) : block(0), pos(0) {}
};

// Static catalog functions

void InitTables();

SqlTable *LookupTable(const std::string &name);

}  // namespace tpl::runtime
