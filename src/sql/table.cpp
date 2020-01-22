#include "sql/table.h"

#include <iostream>
#include <limits>
#include <string>
#include <utility>

#include "sql/catalog.h"

// TODO(siva): Hack! Fix me!
int32_t current_partition = -1;

namespace tpl::sql {

//===----------------------------------------------------------------------===//
//
// Table
//
//===----------------------------------------------------------------------===//

void Table::Insert(Block &&block) {
#ifndef NDEBUG
  // Sanity check
  TPL_ASSERT(block.num_cols() == schema_->GetColumnCount(), "Column count mismatch");
  for (uint32_t i = 0; i < schema_->GetColumnCount(); i++) {
    const auto &block_col_type = block.GetColumnData(i)->GetSqlType();
    const auto &schema_col_type = GetSchema().GetColumnInfo(i)->sql_type;
    TPL_ASSERT(schema_col_type.Equals(block_col_type), "Column type mismatch");
  }
#endif

  num_tuples_ += block.num_tuples();
  blocks_.emplace_back(std::move(block));
}

namespace {

void DumpColValue(std::ostream &os, const SqlType &sql_type, const ColumnSegment &col,
                  uint32_t row_idx) {
  switch (sql_type.GetId()) {
    case SqlTypeId::Boolean: {
      break;
    }
    case SqlTypeId::TinyInt: {
      if (sql_type.IsNullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << col.TypedAccessAt<int8_t>(row_idx);
      }
      break;
    }
    case SqlTypeId::SmallInt: {
      if (sql_type.IsNullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << col.TypedAccessAt<int16_t>(row_idx);
      }
      break;
    }
    case SqlTypeId::Integer: {
      if (sql_type.IsNullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << col.TypedAccessAt<int32_t>(row_idx);
      }
      break;
    }
    case SqlTypeId::BigInt: {
      if (sql_type.IsNullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << col.TypedAccessAt<int64_t>(row_idx);
      }
      break;
    }
    case SqlTypeId::Real: {
      if (sql_type.IsNullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << col.TypedAccessAt<float>(row_idx);
      }
      break;
    }
    case SqlTypeId::Double: {
      if (sql_type.IsNullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << col.TypedAccessAt<double>(row_idx);
      }
      break;
    }
    case SqlTypeId::Char:
    case SqlTypeId::Varchar: {
      if (sql_type.IsNullable() && col.IsNullAt(row_idx)) {
        os << "NULL";
      } else {
        os << std::string(col.TypedAccessAt<const char *>(row_idx));
      }
      break;
    }
    case SqlTypeId::Decimal:
    case SqlTypeId::Date: {
      break;
    }
  }
}

}  // namespace

void Table::Dump(std::ostream &stream) const {
  const auto &cols_meta = GetSchema().GetColumns();
  for (const auto &block : blocks_) {
    for (uint32_t row_idx = 0; row_idx < block.num_tuples(); row_idx++) {
      for (uint32_t col_idx = 0; col_idx < cols_meta.size(); col_idx++) {
        if (col_idx != 0) {
          stream << ", ";
        }
        const auto *col_vector = block.GetColumnData(col_idx);
        DumpColValue(stream, cols_meta[col_idx].sql_type, *col_vector, row_idx);
      }
      stream << "\n";
    }
  }
}

//===----------------------------------------------------------------------===//
//
// Table Block Iterator
//
//===----------------------------------------------------------------------===//

TableBlockIterator::TableBlockIterator(uint16_t table_id)
    : TableBlockIterator(table_id, 0, std::numeric_limits<uint32_t>::max()) {}

TableBlockIterator::TableBlockIterator(uint16_t table_id, uint32_t start_block_idx,
                                       uint32_t end_block_idx)
    : table_id_(table_id),
      start_block_idx_(start_block_idx),
      end_block_idx_(end_block_idx),
      table_(nullptr),
      curr_block_(nullptr) {}

bool TableBlockIterator::Init() {
  // Lookup the table
  const Catalog *catalog = Catalog::Instance();
  table_ = catalog->LookupTableById(table_id_);

  // If the table wasn't found, we didn't initialize
  if (table_ == nullptr) {
    return false;
  }

  // Check block range
  if (start_block_idx_ > table_->GetBlockCount()) {
    return false;
  }

  if (end_block_idx_ != std::numeric_limits<uint32_t>::max()) {
    if (end_block_idx_ > table_->GetBlockCount()) {
      return false;
    }
  }

  if (start_block_idx_ >= end_block_idx_) {
    return false;
  }

  // Setup the block position boundaries
  curr_block_ = nullptr;
  pos_ = table_->begin() + start_block_idx_;
  end_ =
      (end_block_idx_ == std::numeric_limits<uint32_t>::max() ? table_->end()
                                                              : table_->begin() + end_block_idx_);
  return true;
}

bool TableBlockIterator::Advance() {
  if (pos_ == end_) {
    return false;
  }

  curr_block_ = &*pos_;
  ++pos_;
  return true;
}

}  // namespace tpl::sql
