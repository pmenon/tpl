#pragma once

#include <string>
#include <utility>
#include <vector>

#include "sql/data_types.h"
#include "sql/sql.h"

namespace tpl::sql {

/// A class to capture the physical schema layout
class Schema {
 public:
  struct ColumnInfo {
    std::string name;
    const SqlType &sql_type;
    ColumnEncoding encoding;

    ColumnInfo(std::string name, const SqlType &sql_type,
               ColumnEncoding encoding = ColumnEncoding::None)
        : name(std::move(name)), sql_type(sql_type), encoding(encoding) {}

    std::size_t GetStorageSize() const {
      TPL_ASSERT(encoding == ColumnEncoding::None, "Only supports uncompressed encodings");
      const auto prim_type = sql_type.GetPrimitiveTypeId();
      return GetTypeIdSize(prim_type);
    }
  };

  explicit Schema(std::vector<ColumnInfo> &&cols) : cols_(std::move(cols)) {}

  const ColumnInfo *GetColumnInfo(uint32_t col_idx) const { return &cols_[col_idx]; }

  uint32_t num_columns() const { return static_cast<uint32_t>(columns().size()); }

  const std::vector<ColumnInfo> &columns() const { return cols_; }

  std::string ToString() const {
    std::string result = "cols=[";
    bool first = true;
    for (const auto &col : cols_) {
      if (!first) result += ",";
      first = false;
      result += col.sql_type.GetName();
    }
    result += "]";
    return result;
  }

 private:
  // The metadata for each column. This is immutable after construction.
  const std::vector<ColumnInfo> cols_;
};

}  // namespace tpl::sql
