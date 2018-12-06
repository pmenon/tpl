#include "sql/table.h"

#include <iostream>

// TODO(siva): Hack! Fix me!
i32 current_partition = -1;

namespace tpl::sql {

void Table::Insert(Block &&block) {
  num_rows_ += block.num_rows();
  blocks_.emplace_back(std::move(block));
}

void DumpColValue(std::ostream &os, const Type &type, const ColumnVector &col,
                  u32 row_idx) {
  switch (type.type_id()) {
    case TypeId::Boolean: {
      break;
    }
    case TypeId::SmallInt: {
      if (type.nullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << col.TypedAccessAt<i16>(row_idx);
      }
      break;
    }
    case TypeId::Integer: {
      if (type.nullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << col.TypedAccessAt<i32>(row_idx);
      }
      break;
    }
    case TypeId::BigInt: {
      if (type.nullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << col.TypedAccessAt<i64>(row_idx);
      }
      break;
    }
    case TypeId::Decimal:
    case TypeId::Date:
    case TypeId::Char:
    case TypeId::Varchar: {
      break;
    }
  }
}

void Table::Dump(std::ostream &os) const {
  const auto &cols_meta = schema().columns();
  for (const auto &block : blocks_) {
    for (u32 row_idx = 0; row_idx < block.num_rows(); row_idx++) {
      for (u32 col_idx = 0; col_idx < cols_meta.size(); col_idx++) {
        if (col_idx != 0) os << ", ";
        const auto *col_vector = block.GetColumnData(col_idx);
        DumpColValue(os, cols_meta[col_idx].type, *col_vector, row_idx);
      }
      os << "\n";
    }
  }
}

}  // namespace tpl::sql