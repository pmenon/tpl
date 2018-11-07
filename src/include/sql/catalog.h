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
  /**
   * Access singleton Catalog object. Singletons are bad blah blah blah ...
   * @return The singleton Catalog object
   */
  static Catalog *instance() {
    static Catalog kInstance;
    return &kInstance;
  }

  /**
   * Lookup a table in this catalog by name
   * @param name The name of the target table
   * @return A pointer to the table, or NULL if the table doesn't exist.
   */
  Table *LookupTableByName(const std::string &name);

  /**
   * Lookup a table in this catalog by name, using an identifier
   * @param name The name of the target table
   * @return A pointer to the table, or NULL if the table doesn't exist.
   */
  Table *LookupTableByName(ast::Identifier name);

  /**
   * Lookup a table in this catalog by ID
   * @param table_id The ID of the target table
   * @return A pointer to the table, or NULL if the table doesn't exist.
   */
  Table *LookupTableById(TableId table_id);

 private:
  Catalog();
  ~Catalog();

 private:
  std::unordered_map<TableId, std::unique_ptr<Table>> table_catalog_;
};

}  // namespace tpl::sql