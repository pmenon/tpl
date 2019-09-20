#pragma once

#include <string>

#include "sql/catalog.h"

namespace tpl::sql::tablegen {

/**
 * Helper class to generate test tables and their indexes.
 */
class TableGenerator {
 public:
  static void GenerateTPCHTables(sql::Catalog *catalog, const std::string &data_dir);
};

}  // namespace tpl::sql::tablegen
