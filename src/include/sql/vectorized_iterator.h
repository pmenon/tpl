#pragma once

#include "sql/column.h"
#include "sql/table.h"

namespace tpl::sql {

/// An iterator that iterates over a table in vectorized fashion
class VectorizedIterator {
 public:
  /// A collection of projected columns that forms a batch of rows. A row batch
  /// is mutable in the sense that columns can be added and removed as need be
  /// during query processing. After a column is added to a batch, the column is
  /// immutable.
  ///
  /// An invariant is that all columns in the batch have the same number of rows
  class RowBatch {
   public:
    explicit RowBatch(u32 size) : selection_vector_(nullptr), num_rows_(0) {}

    void AddColumn(const ColumnVector *col) {
      TPL_ASSERT(col->num_rows() == num_rows(),
                 "Adding column with different number of rows than row batch");
      cols_.push_back(col);
    }

    const ColumnVector *GetColumn(u32 col_idx) const { return cols_[col_idx]; }

    u32 num_rows() const { return num_rows_; }

   private:
    friend class VectorizedIterator;

    void Reset(u32 num_rows) {
      num_rows_ = num_rows;
      cols_.clear();
    }

   private:
    std::vector<const ColumnVector *> cols_;
    std::unique_ptr<ColumnVector> selection_vector_;
    u32 num_rows_;
  };

  /// Constructor for a vectorized iterator
  /// \param table
  explicit VectorizedIterator(const Table &table) noexcept
      : block_iterator_(table.Iterate()), row_batch_(2048) {}

  bool Next() noexcept {
    if (!block_iterator()->Next()) {
      return false;
    }

    SetupRowBatch(block_iterator()->current_block());
    return true;
  }

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  const RowBatch *row_batch() const { return &row_batch_; }
  RowBatch *row_batch() { return &row_batch_; }

 private:
  void SetupRowBatch(const Table::Block *block) {
    row_batch()->Reset(block->num_rows());

    for (u32 i = 0; i < block->num_cols(); i++) {
      row_batch()->AddColumn(block->GetColumnData(i));
    }
  }

  Table::BlockIterator *block_iterator() { return &block_iterator_; }

  const Table::BlockIterator *block_iterator() const {
    return &block_iterator_;
  }

 private:
  Table::BlockIterator block_iterator_;
  RowBatch row_batch_;
};

}  // namespace tpl::sql