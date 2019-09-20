#pragma once

#include <string>

#include "sql/catalog.h"

namespace tpl::sql::tablegen {

/**
 * Helper class to generate test tables and their indexes.
 */
class TableGenerator {
 public:
  static void GenerateTableFromFile(sql::Catalog *catalog, const std::string &schema_file,
                                    const std::string &data_file);

  static void GenerateTPCHTables(sql::Catalog *catalog, const std::string &dir_name);
};

}  // namespace tpl::sql::tablegen
