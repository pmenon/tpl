#include "runtime/sql_table.h"

#include <unordered_map>

namespace tpl::runtime {

static std::unordered_map<std::string, SqlTable> kTableCatalog;

SqlTable *LookupTable(const std::string &name) {
  auto iter = kTableCatalog.find(name);
  return (iter == kTableCatalog.end() ? nullptr : &iter->second);
}

void InitTables() {
  // Table 1
  {
    Schema schema_1 = {.cols = {
                           {"col_A", SqlTypeId::Integer},
                           {"col_B", SqlTypeId::Integer},
                           {"col_C", SqlTypeId::Integer},
                           {"col_D", SqlTypeId::Integer},
                       }};
    kTableCatalog.insert(
        std::make_pair("test_1", SqlTable(std::move(schema_1))));
  }
}

}  // namespace tpl::runtime