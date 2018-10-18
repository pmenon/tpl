#include "runtime/sql_table.h"

#include <memory>
#include <unordered_map>

namespace tpl::runtime {

// The catalog of all test tables
static std::unordered_map<TestTableId, std::unique_ptr<SqlTable>> kTableCatalog;

// A map from table name to its ID
static std::unordered_map<std::string, TestTableId> kTableNameMap = {
#define ENTRY(Name, Str, ...) {Str, TestTableId::Name},
    TEST_TABLES(ENTRY)
#undef ENTRY
};

// Lookup a table by its ID
SqlTable *LookupTableById(TestTableId table_id) {
  auto iter = kTableCatalog.find(table_id);
  return (iter == kTableCatalog.end() ? nullptr : iter->second.get());
}

// Lookup a table by its name
SqlTable *LookupTableByName(const std::string &name) {
  auto iter = kTableNameMap.find(name);
  if (iter == kTableNameMap.end()) {
    return nullptr;
  }

  return LookupTableById(iter->second);
}

void InitTables() {
  // Table 1
  {
    Schema schema_1 = {
        Schema::ColInfo{.name = "col_A", .type_id = SqlTypeId::Integer},
        Schema::ColInfo{.name = "col_B", .type_id = SqlTypeId::Integer},
        Schema::ColInfo{.name = "col_C", .type_id = SqlTypeId::Integer},
        Schema::ColInfo{.name = "col_D", .type_id = SqlTypeId::Integer},
    };

    // Insert into catalog
    kTableCatalog.emplace(TestTableId::Test1,
                          std::make_unique<SqlTable>(std::move(schema_1)));
  }
}

bool SqlTableIterator::Next() { return false; }

}  // namespace tpl::runtime