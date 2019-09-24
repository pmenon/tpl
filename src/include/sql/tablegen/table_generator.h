#pragma once

#include <string>

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
    auto iter = schemas_.find(name);
    return iter == schemas_.end() ? nullptr : iter->second.get();
  }

 private:
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
      std::vector<Schema::ColumnInfo> cols;
      for (uint32_t i = 0; i < uint32_t(2); i++) {
        cols.emplace_back(string_col);
      }
      for (uint32_t i = 0; i < uint32_t(7); i++) {
        cols.emplace_back(real_col);
      }
      cols.emplace_back(int_col);
      schemas_.emplace("tpch_q1", std::make_unique<Schema>(std::move(cols)));
    }

    // Q6 (one real)
    {
      std::vector<Schema::ColumnInfo> cols{real_col};
      schemas_.emplace("tpch_q6", std::make_unique<Schema>(std::move(cols)));
    }
    // Q4 (one string, one int)
    {
      std::vector<Schema::ColumnInfo> cols{string_col, int_col};
      schemas_.emplace("tpch_q4", std::make_unique<Schema>(std::move(cols)));
    }

    // Q5 (one string, one real)
    {
      std::vector<Schema::ColumnInfo> cols{string_col, real_col};
      schemas_.emplace("tpch_q5", std::make_unique<Schema>(std::move(cols)));
    }

    // Q7 (two strings, one int, one real)
    {
      std::vector<Schema::ColumnInfo> cols{string_col, string_col, int_col, real_col};
      schemas_.emplace("tpch_q7", std::make_unique<Schema>(std::move(cols)));
    }

    // Q11 (one int, one real)
    {
      std::vector<Schema::ColumnInfo> cols{int_col, real_col};
      schemas_.emplace("tpch_q11", std::make_unique<Schema>(std::move(cols)));
    }

    // Q16 (two strings, two ints)
    {
      std::vector<Schema::ColumnInfo> cols{string_col, string_col, int_col, int_col};
      schemas_.emplace("tpch_q16", std::make_unique<Schema>(std::move(cols)));
    }

    // Q18 (one string, two integers, one date, two reals)
    {
      std::vector<Schema::ColumnInfo> cols{string_col, int_col,  int_col,
                                           date_col,   real_col, real_col};
      schemas_.emplace("tpch_q18", std::make_unique<Schema>(std::move(cols)));
    }

    // Q19 (one real)
    {
      std::vector<Schema::ColumnInfo> cols{real_col};
      schemas_.emplace("tpch_q19", std::make_unique<Schema>(std::move(cols)));
    }
  }

  std::unordered_map<std::string, std::unique_ptr<Schema>> schemas_;
};

}  // namespace tpl::sql::tablegen
