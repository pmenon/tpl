#include "sql/tablegen/table_generator.h"

#include "logging/logger.h"
#include "sql/tablegen/table_loader.h"

namespace tpl::sql::tablegen {

void TableGenerator::GenerateTableFromFile(Catalog *catalog, const std::string &schema_file,
                                           const std::string &data_file) {
  TableLoader table_reader(catalog);
  uint32_t num_rows = table_reader.LoadTable(schema_file, data_file);
  LOG_INFO("Wrote {} rows from file {}.", num_rows, data_file);
}

void TableGenerator::GenerateTPCHTables(Catalog *catalog, const std::string &dir_name) {
  static const std::string kTpchTableNames[] = {
      "customer",  // Customer
      "lineitem",  // Line Item
      "nation",    // Nation
      "orders",    // Orders
      "part",      // Part
      "partsupp",  // Part-Supplier
      "region",    // Region
      "supplier",  // Supplier
  };

  TableLoader table_reader(catalog);

  std::string dir = dir_name.back() == '/' ? dir_name : dir_name + "/";

  for (const auto &table_name : kTpchTableNames) {
    LOG_INFO("Generating TPCH table table '{}'", table_name);
    const std::string schema_file = dir + table_name + ".schema";
    const std::string data_file = dir + table_name + ".data";
    uint32_t num_rows = table_reader.LoadTable(schema_file, data_file);
    LOG_INFO("Created TPCH table '{}' with {} rows", table_name, num_rows);
  }
}

}  // namespace tpl::sql::tablegen
