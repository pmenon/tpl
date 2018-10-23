#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "ast/identifier.h"
#include "util/common.h"

namespace tpl::sql {

class Table;

#define TABLES(V) V(Test1, "test_1")

enum class TableId : u16 {
#define ENTRY(Id, ...) Id
  TABLES(ENTRY)
#undef ENTRY
};

/**
 * The main catalog
 */
class Catalog {
 public:
  static Catalog *instance() {
    static Catalog kInstance;
    return &kInstance;
  }

  Table *LookupTableByName(const std::string &name);
  Table *LookupTableByName(ast::Identifier name);
  Table *LookupTableById(TableId table_id);

 private:
  Catalog();
  ~Catalog();

 private:
  std::unordered_map<TableId, std::unique_ptr<Table>> table_catalog_;
};

}  // namespace tpl::sql