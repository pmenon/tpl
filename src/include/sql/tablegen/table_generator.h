#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "sql/catalog.h"
#include "sql/schema.h"

namespace tpl::sql::tablegen {

/**
 * Helper class to generate test tables and their indexes.
 */
class TableGenerator {
 public:
  static void GenerateTPCHTables(sql::Catalog *catalog, const std::string &data_dir);
};

/**
 * All TPCH output schemas
 */
class TPCHOutputSchemas {
 public:
  TPCHOutputSchemas() { InitTestOutput(); }

  /**
   * Initialize test output schemas
   */
  void InitTestOutput() { InitTPCHOutput(); }

  /**
   * @param name name of schema
   * @return the schema if it exists; an exception otherwise
   */
  const Schema *GetSchema(const std::string &name) {
    auto iter = schemas_by_name_.find(name);
    return iter == schemas_by_name_.end() ? nullptr : owned_schemas_[iter->second].get();
  }

 private:
  void AddSchema(std::unique_ptr<Schema> schema, const std::string &table_name) {
    owned_schemas_.emplace_back(std::move(schema));
    schemas_by_name_.emplace(table_name, owned_schemas_.size() - 1);
    schemas_by_name_.emplace(table_name + "_par", owned_schemas_.size() - 1);
  }

  void InitTPCHOutput() {
    const auto &int_type = IntegerType::Instance(false);
    const auto &real_type = RealType::Instance(false);
    const auto &date_type = DateType::Instance(false);
    const auto &string_type = VarcharType::Instance(false, 100);
    Schema::ColumnInfo int_col{"dummy", int_type};
    Schema::ColumnInfo real_col{"dummy", real_type};
    Schema::ColumnInfo date_col{"dummy", date_type};
    Schema::ColumnInfo string_col{"dummy", string_type};

    // Q1 (two strings, 7 reals, 1 int)
    {
      std::vector<Schema::ColumnInfo> cols = {string_col, string_col, real_col, real_col, real_col,
                                              real_col,   real_col,   real_col, real_col, int_col};
      AddSchema(std::make_unique<Schema>(std::move(cols)), "q1");
    }

    // Q4 (one string, one int)
    {
      std::vector<Schema::ColumnInfo> cols{string_col, int_col};
      AddSchema(std::make_unique<Schema>(std::move(cols)), "q4");
    }

    // Q5 (one string, one real)
    {
      std::vector<Schema::ColumnInfo> cols{string_col, real_col};
      AddSchema(std::make_unique<Schema>(std::move(cols)), "q5");
    }

    // Q6 (one real)
    {
      std::vector<Schema::ColumnInfo> cols{real_col};
      AddSchema(std::make_unique<Schema>(std::move(cols)), "q6");
    }

    // Q7 (two strings, one int, one real)
    {
      std::vector<Schema::ColumnInfo> cols{string_col, string_col, int_col, real_col};
      AddSchema(std::make_unique<Schema>(std::move(cols)), "q7");
    }

    // Q11 (one int, one real)
    {
      std::vector<Schema::ColumnInfo> cols{int_col, real_col};
      AddSchema(std::make_unique<Schema>(std::move(cols)), "q11");
    }

    // Q16 (two strings, two ints)
    {
      std::vector<Schema::ColumnInfo> cols{string_col, string_col, int_col, int_col};
      AddSchema(std::make_unique<Schema>(std::move(cols)), "q16");
    }

    // Q18 (one string, two integers, one date, two reals)
    {
      std::vector<Schema::ColumnInfo> cols{string_col, int_col,  int_col,
                                           date_col,   real_col, real_col};
      AddSchema(std::make_unique<Schema>(std::move(cols)), "q18");
    }

    // Q19 (one real)
    {
      std::vector<Schema::ColumnInfo> cols{real_col};
      AddSchema(std::make_unique<Schema>(std::move(cols)), "q19");
    }
  }

  std::unordered_map<std::string, std::size_t> schemas_by_name_;
  std::vector<std::unique_ptr<Schema>> owned_schemas_;
};

}  // namespace tpl::sql::tablegen
