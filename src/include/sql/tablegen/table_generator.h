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
   */
  static void GenerateTPCHTables(sql::Catalog *catalog, const std::string &data_dir);

  /**
   * Generate all Star-Schema Benchmark tables.
   * @param catalog The catalog instance to insert tables into.
   * @param data_dir The directory containing table data.
   */
  static void GenerateSSBMTables(sql::Catalog *catalog, const std::string &data_dir);
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
  void InitTestOutput() {
    // TPC-H schema.
    InitTPCHOutput();

    // SSBM schema.
    InitSSBMOutput();
  }

  /**
   * @param name name of schema
   * @return the schema if it exists; an exception otherwise
   */
  const sql::planner::OutputSchema *GetSchema(const std::string &name) {
    auto iter = schemas_by_name_.find(name);
    return iter == schemas_by_name_.end() ? owned_schemas_[0].get()
                                          : owned_schemas_[iter->second].get();
  }

 private:
  void AddSchema(std::unique_ptr<sql::planner::OutputSchema> schema,
                 const std::string &table_name) {
    owned_schemas_.emplace_back(std::move(schema));
    schemas_by_name_.emplace(table_name, owned_schemas_.size() - 1);
    schemas_by_name_.emplace(table_name + "_par", owned_schemas_.size() - 1);
  }

  void InitTPCHOutput() {
    sql::planner::OutputSchema::Column int_col{sql::TypeId::Integer, false, nullptr};
    sql::planner::OutputSchema::Column real_col{sql::TypeId::Double, false, nullptr};
    sql::planner::OutputSchema::Column date_col{sql::TypeId::Date, false, nullptr};
    sql::planner::OutputSchema::Column string_col{sql::TypeId::Varchar, false, nullptr};

    // Q1 (two strings, 7 reals, 1 int)
    {
      std::vector<sql::planner::OutputSchema::Column> cols = {
          string_col, string_col, real_col, real_col, real_col,
          real_col,   real_col,   real_col, real_col, int_col};
      AddSchema(std::make_unique<sql::planner::OutputSchema>(std::move(cols)), "q1");
    }

    // Q4 (one string, one int)
    {
      std::vector<sql::planner::OutputSchema::Column> cols{string_col, int_col};
      AddSchema(std::make_unique<sql::planner::OutputSchema>(std::move(cols)), "q4");
    }

    // Q5 (one string, one real)
    {
      std::vector<sql::planner::OutputSchema::Column> cols{string_col, real_col};
      AddSchema(std::make_unique<sql::planner::OutputSchema>(std::move(cols)), "q5");
    }

    // Q6 (one real)
    {
      std::vector<sql::planner::OutputSchema::Column> cols{real_col};
      AddSchema(std::make_unique<sql::planner::OutputSchema>(std::move(cols)), "q6");
    }

    // Q7 (two strings, one int, one real)
    {
      std::vector<sql::planner::OutputSchema::Column> cols{string_col, string_col, int_col,
                                                           real_col};
      AddSchema(std::make_unique<sql::planner::OutputSchema>(std::move(cols)), "q7");
    }

    // Q11 (one int, one real)
    {
      std::vector<sql::planner::OutputSchema::Column> cols{int_col, real_col};
      AddSchema(std::make_unique<sql::planner::OutputSchema>(std::move(cols)), "q11");
    }

    // Q16 (two strings, two ints)
    {
      std::vector<sql::planner::OutputSchema::Column> cols{string_col, string_col, int_col,
                                                           int_col};
      AddSchema(std::make_unique<sql::planner::OutputSchema>(std::move(cols)), "q16");
    }

    // Q18 (one string, two integers, one date, two reals)
    {
      std::vector<sql::planner::OutputSchema::Column> cols{string_col, int_col,  int_col,
                                                           date_col,   real_col, real_col};
      AddSchema(std::make_unique<sql::planner::OutputSchema>(std::move(cols)), "q18");
    }

    // Q19 (one real)
    {
      std::vector<sql::planner::OutputSchema::Column> cols{real_col};
      AddSchema(std::make_unique<sql::planner::OutputSchema>(std::move(cols)), "q19");
    }
  }

  void InitSSBMOutput() {
    sql::planner::OutputSchema::Column big_int{sql::TypeId::BigInt, false, nullptr};

    // Q1.1 (1 int)
    {
      std::vector<sql::planner::OutputSchema::Column> cols = {big_int};
      AddSchema(std::make_unique<sql::planner::OutputSchema>(std::move(cols)), "q1.1");
    }

    // Q1.2 (1 int)
    {
      std::vector<sql::planner::OutputSchema::Column> cols = {big_int};
      AddSchema(std::make_unique<sql::planner::OutputSchema>(std::move(cols)), "q1.2");
    }

    // Q1.3 (1 int)
    {
      std::vector<sql::planner::OutputSchema::Column> cols = {big_int};
      AddSchema(std::make_unique<sql::planner::OutputSchema>(std::move(cols)), "q1.3");
    }
  }

 private:
  std::unordered_map<std::string, std::size_t> schemas_by_name_;
  std::vector<std::unique_ptr<sql::planner::OutputSchema>> owned_schemas_;
};
}  // namespace tpl::sql::tablegen
