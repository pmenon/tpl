#pragma once

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "sql/catalog.h"
#include "sql/schema.h"
#include "sql/table.h"
#include "sql/tablegen/schema_reader.h"

namespace tpl::sql::tablegen {

/**
 * This class reads table from files
 */
class TableLoader {
 public:
  /**
   * Construct a loader configured to interact with the given catalog instance.
   * @param catalog The catalog to use.
   */
  explicit TableLoader(Catalog *catalog) : catalog_{catalog} {}

  /**
   * Load the table with a predefined schema and pre-generated data file.
   * @param schema_file The file containing the schema of the table.
   * @param data_file The file containing CSV data for the table.
   * @return The number of tuples in the table.
   */
  uint32_t LoadTable(const std::string &schema_file, const std::string &data_file);

 private:
  // Create table in the catalog with the given table information
  Table *CreateTable(TableInfo *info);

 private:
  Catalog *catalog_;
};

}  // namespace tpl::sql::tablegen
