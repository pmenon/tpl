#pragma once

#include <string>
#include <utility>
#include <vector>

#include "sql/data_types.h"
#include "sql/sql.h"

namespace tpl::sql {

/**
 * A Schema captures a physical layout of data in a tabular form.
 */
class Schema {
 public:
  /**
   * Metadata about a column.
   */
  struct ColumnInfo {
    std::string name;
    const SqlType &sql_type;
    ColumnEncoding encoding;
    uint16_t oid{0};

    ColumnInfo(std::string name, const SqlType &sql_type,
               ColumnEncoding encoding = ColumnEncoding::None)
        : name(std::move(name)), sql_type(sql_type), encoding(encoding) {}

    std::size_t GetStorageAlignment() const {
      TPL_ASSERT(encoding == ColumnEncoding::None, "Only supports uncompressed encodings");
      const auto prim_type = sql_type.GetPrimitiveTypeId();
      return GetTypeIdAlignment(prim_type);
    }

    std::size_t GetStorageSize() const {
      TPL_ASSERT(encoding == ColumnEncoding::None, "Only supports uncompressed encodings");
      const auto prim_type = sql_type.GetPrimitiveTypeId();
      return GetTypeIdSize(prim_type);
    }

    void SetOid(uint16_t oid) { this->oid = oid; }
  };

  /**
   * Create a schema with the given columns.
   * @param cols All the columns in the schema and their metadata.
   */
  explicit Schema(std::vector<ColumnInfo> &&cols) : cols_(std::move(cols)) {
    uint16_t oid = 0;
    for (auto &col : cols_) {
      col.SetOid(oid);
      oid++;
    }
  }

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(Schema);

  /**
   * Get the metadata for the column at the index @em col_index in the schema.
   * @param col_idx The index of the column in the schema.
   * @return The metadata for the column.
   */
  const ColumnInfo *GetColumnInfo(uint32_t col_idx) const {
    TPL_ASSERT(col_idx < cols_.size(), "Out-of-bounds column access");
    return &cols_[col_idx];
  }

  const ColumnInfo &GetColumnInfo(const std::string &name) const {
    for (const auto &c : cols_) {
      if (c.name == name) {
        return c;
      }
    }
    throw std::out_of_range("Column name doesn't exist");
  }

  /**
   * @return Return the number of columns in the schema.
   */
  uint32_t GetColumnCount() const { return static_cast<uint32_t>(GetColumns().size()); }

  /**
   * @return A const-view of the column metadata.
   */
  const std::vector<ColumnInfo> &GetColumns() const { return cols_; }

  /**
   * @return A string representation of this schema.
   */
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
  std::vector<ColumnInfo> cols_;
};

}  // namespace tpl::sql
