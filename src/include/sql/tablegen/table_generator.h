#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "sql/catalog.h"
#include "sql/planner/plannodes/output_schema.h"
#include "sql/schema.h"

namespace tpl::sql::tablegen {

/**
 * Helper class to generate test tables and their indexes.
 */
class TableGenerator {
 public:
  /**
   * Generate all TPC-H tables.
   * @param catalog The catalog instance to insert tables into.
   * @param data_dir The directory containing table data.
   * @param compress Should table data be compressed after loading?
   */
  static void GenerateTPCHTables(sql::Catalog *catalog, const std::string &data_dir,
                                 bool compress = false);

  /**
   * Generate all Star-Schema Benchmark tables.
   * @param catalog The catalog instance to insert tables into.
   * @param data_dir The directory containing table data.
   */
  static void GenerateSSBMTables(sql::Catalog *catalog, const std::string &data_dir);
};

}  // namespace tpl::sql::tablegen
