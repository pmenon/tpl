#pragma once

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "logging/logger.h"
#include "sql/schema.h"

namespace tpl::sql::tablegen {

struct TableInfo {
  // Name
  std::string table_name;
  // Schema
  std::vector<Schema::ColumnInfo> cols;
};

/**
 * Reads .schema file
 * File format:
 * table_name num_cols
 * col_name1(string), type1(string), nullable1(0 or 1)
 * ...
 * col_nameN(string), typeN(string), nullableN(0 or 1), varchar_size if type == varchar
 */
class SchemaReader {
 public:
  /**
   * Create a schema reader.
   */
  SchemaReader()
      : type_names_{{"tinyint", SqlTypeId::TinyInt}, {"smallint", SqlTypeId::SmallInt},
                    {"int", SqlTypeId::Integer},     {"bigint", SqlTypeId::BigInt},
                    {"bool", SqlTypeId::Boolean},    {"real", SqlTypeId::Real},
                    {"decimal", SqlTypeId::Real},    {"varchar", SqlTypeId::Varchar},
                    {"varlen", SqlTypeId::Varchar},  {"date", SqlTypeId::Date}} {}

  /**
   * Reads table metadata.
   *
   * @throws std::runtime_error if the schema filename cannot be read
   *
   * @param filename name of the file containing the metadata
   * @return the struct containing information about the table
   */
  std::unique_ptr<TableInfo> ReadTableInfo(const std::string &filename) {
    // Allocate table information
    auto table_info = std::make_unique<TableInfo>();

    // Open file to read
    std::ifstream schema_file;
    schema_file.open(filename);
    if (!schema_file) {
      throw std::runtime_error("Cannot open file " + std::string(filename));
    }

    // Read Table name and num_cols
    uint32_t num_cols;
    schema_file >> table_info->table_name >> num_cols;
    LOG_INFO("Reading table {} with {} columns", table_info->table_name, num_cols);

    // Read columns and create table schema
    table_info->cols = ReadColumns(&schema_file, num_cols);
    return table_info;
  }

 private:
  // Read columns
  std::vector<Schema::ColumnInfo> ReadColumns(std::ifstream *in, uint32_t num_cols) {
    std::vector<Schema::ColumnInfo> cols;

    // Read each column
    std::string col_name;
    std::string col_type_str;
    SqlTypeId col_type;
    uint32_t varchar_size{0};
    bool nullable;

    for (uint32_t i = 0; i < num_cols; i++) {
      *in >> col_name >> col_type_str >> nullable;
      col_type = type_names_.at(col_type_str);
      if (col_type == SqlTypeId::Varchar) {
        *in >> varchar_size;
        cols.emplace_back(col_name, GetSqlTypeFromId(col_type, nullable, varchar_size));
      } else {
        cols.emplace_back(col_name, GetSqlTypeFromId(col_type, nullable));
      }
    }

    return cols;
  }

  static const SqlType &GetSqlTypeFromId(SqlTypeId type_id, bool nullable, uint32_t max_size = 0) {
    switch (type_id) {
      case SqlTypeId::TinyInt:
        return TinyIntType::Instance(nullable);
      case SqlTypeId::SmallInt:
        return SmallIntType::Instance(nullable);
      case SqlTypeId::Integer:
        return IntegerType::Instance(nullable);
      case SqlTypeId::BigInt:
        return BigIntType::Instance(nullable);
      case SqlTypeId::Double:
        return DoubleType::Instance(nullable);
      case SqlTypeId::Real:
        return RealType::Instance(nullable);
      case SqlTypeId::Date:
        return DateType::Instance(nullable);
      case SqlTypeId::Varchar:
        return VarcharType::Instance(nullable, max_size);
      default:
        UNREACHABLE("Unsupported type!");
    }
  }

 private:
  // Supported types
  const std::unordered_map<std::string, SqlTypeId> type_names_;
};

}  // namespace tpl::sql::tablegen
