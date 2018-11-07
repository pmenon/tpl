#include "sql/table.h"

#include <iostream>

namespace tpl::sql {

void Table::BulkInsert(std::vector<Table::ColumnVector> &&data, u32 num_rows) {
  blocks_.emplace_back(std::move(data), num_rows);
}

bool Table::Scan(TableIterator *iter) const {
  if (iter->block_ == blocks_.size()) {
    return false;
  }

  // The columns in the new block
  const auto &block = blocks_[iter->block_];
  const auto &cols = block.columns;

  iter->cols_.resize(cols.size());
  for (u32 i = 0; i < cols.size(); i++) {
    iter->cols_[i] = &cols[i];
  }
  iter->block_ += 1;
  iter->pos_ = 0;
  iter->bound_ = block.num_rows;
  return true;
}

void DumpColValue(std::ostream &os, Type type, const Table::ColumnVector &col,
                  u32 row_idx) {
  switch (type.type_id()) {
    case TypeId::Boolean: {
      break;
    }
    case TypeId::SmallInt: {
      if (type.nullable() && col.null_bitmap[row_idx]) {
        os << "NULL";
      } else {
        os << reinterpret_cast<const i16 *>(col.data)[row_idx];
      }
      break;
    }
    case TypeId::Integer: {
      if (type.nullable() && col.null_bitmap[row_idx]) {
        os << "NULL";
      } else {
        os << reinterpret_cast<const i32 *>(col.data)[row_idx];
      }
      break;
    }
    case TypeId::BigInt: {
      if (type.nullable() && col.null_bitmap[row_idx]) {
        os << "NULL";
      } else {
        os << reinterpret_cast<const i64 *>(col.data)[row_idx];
      }
      break;
    }
    case TypeId::Decimal: {
      break;
    }
    case TypeId::Varchar: {
      break;
    }
  }
}

void Table::Dump(std::ostream &os) const {
  const auto &cols_meta = schema().columns();
  for (const auto &block : blocks_) {
    for (u32 row_idx = 0; row_idx < block.num_rows; row_idx++) {
      for (u32 col_idx = 0; col_idx < cols_meta.size(); col_idx++) {
        if (col_idx != 0) os << ", ";
        const auto &col_vector = block.columns[col_idx];
        DumpColValue(os, cols_meta[col_idx].type, col_vector, row_idx);
      }
      os << "\n";
    }
  }
}

bool TableIterator::Next() {
  if (++pos_ >= bound_) {
    return table_->Scan(this);
  }
  return true;
}

}  // namespace tpl::sql