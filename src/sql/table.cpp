#include "sql/table.h"

#include <iostream>

// TODO(siva): Hack! Fix me!
i32 current_partition = -1;

namespace tpl::sql {

void Table::Insert(Block &&block) {
  blocks_.emplace_back(std::move(block));
}

bool Table::Scan(TableIterator *iter) const {
  if ((current_partition == -1 && iter->block_ == blocks_.size()) ||
      (current_partition != -1 &&
       iter->block_ != static_cast<u32>(current_partition))) {
    return false;
  }

  // The columns in the new block
  const auto &block = blocks_[iter->block_];
  //  const auto &cols = block.columns;

  iter->cols_.resize(block.num_cols());
  for (u32 i = 0; i < block.num_cols(); i++) {
    iter->cols_[i] = &block.GetColumnData(i);
  }
  iter->block_ += 1;
  iter->pos_ = 0;
  iter->bound_ = block.num_rows();
  return true;
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
        os << col.GetAt<i16>(row_idx);
      }
      break;
    }
    case TypeId::Integer: {
      if (type.nullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << col.GetAt<i32>(row_idx);
      }
      break;
    }
    case TypeId::BigInt: {
      if (type.nullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << col.GetAt<i64>(row_idx);
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
        const auto &col_vector = block.GetColumnData(col_idx);
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