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

    // TODO(pmenon): Fix me to change based on encoding
    u32 StorageSize() const {
      switch (sql_type.id()) {
        case SqlTypeId::Boolean:
        case SqlTypeId::TinyInt: {
          return sizeof(i8);
        }
        case SqlTypeId::SmallInt: {
          return sizeof(i16);
        }
        case SqlTypeId::Date:
        case SqlTypeId::Integer: {
          return sizeof(i32);
        }
        case SqlTypeId::BigInt: {
          return sizeof(i64);
        }
        case SqlTypeId::Decimal: {
          return sizeof(i128);
        }
        case SqlTypeId::Char: {
          auto *char_type = sql_type.As<CharType>();
          return char_type->length() * sizeof(i8);
        }
        case SqlTypeId::Varchar: {
          return 16;
        }
        default: {
          TPL_UNLIKELY("Impossible type");
          return 0;
        }
      }
    }
  };

  explicit Schema(std::vector<ColumnInfo> &&cols) : cols_(std::move(cols)) {}

  const ColumnInfo *GetColumnInfo(u32 col_idx) const { return &cols_[col_idx]; }

  u32 num_columns() const { return static_cast<u32>(columns().size()); }

  const std::vector<ColumnInfo> &columns() const { return cols_; }

 private:
  // The metadata for each column. This is immutable after construction.
  const std::vector<ColumnInfo> cols_;
};

}  // namespace tpl::sql
