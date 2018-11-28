#pragma once

#include <vector>

#include "sql/column.h"
#include "util/common.h"
#include "util/macros.h"

namespace tpl::sql {

class Block {
 public:
  Block(std::vector<ColumnVector> &&data, u32 num_rows)
      : data_(std::move(data)), num_rows_(num_rows) {}

  u32 num_cols() const { return static_cast<u32>(data_.size()); }
  u32 num_rows() const { return num_rows_; }

  const ColumnVector &ColumnData(u32 col_idx) const {
    TPL_ASSERT(col_idx < num_cols(), "Invalid column index!");
    return data_[col_idx];
  }

 private:
  std::vector<ColumnVector> data_;
  u32 num_rows_;
};

class BlockIterator {
 public:
  virtual ~BlockIterator() = default;

  Block Next();
};


}  // namespace tpl::sql