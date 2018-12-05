#pragma once

#include "sql/vectorized_iterator.h"

namespace tpl::sql {

/**
 * A row-at-a-time iterator over SQL tables. This iterator is only used during
 * JIT when generating row-at-a-time iterations in tight loops. Or, it's used
 * for debugging purposes.
 */
class TableIterator {
 public:
  explicit TableIterator(const Table &table)
      : iterator_(table), pos_(-1), limit_(0) {}

  bool Next() noexcept {
    if (++pos_ < limit()) {
      return true;
    }

    if (!iterator()->Next()) {
      return false;
    }

    Setup();
    return true;
  }

  /**
   * Read an integer column value from the current iterator position
   *
   * @tparam type_id The SQL type of the column
   * @tparam nullable Whether the column is NULLable
   * @param col_idx The ID (offset) of the column to read from
   * @param out The output value to populate
   */
  template <TypeId type_id, bool nullable>
  void ReadIntegerColumn(u32 col_idx, Integer *out) const {
    const auto *col = row_batch()->GetColumn(col_idx);

    // Set null (if column is nullable)
    if constexpr (nullable) {
      out->null = col->IsNullAt(pos());
    }

    // Set appropriate value
    if constexpr (type_id == TypeId::SmallInt) {
      out->val.smallint = col->TypedAccessAt<i16>(pos());
    } else if constexpr (type_id == TypeId::Integer) {
      out->val.integer = col->TypedAccessAt<i32>(pos());
    } else if constexpr (type_id == TypeId::BigInt) {
      out->val.bigint = col->TypedAccessAt<i64>(pos());
    }
  }

  template <bool nullable>
  void ReadDecimalColumn(u32 col_idx, Decimal *out) const {}

 private:
  void Setup() {
    pos_ = 0;
    limit_ = row_batch()->num_rows();
  }

  //////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////

  VectorizedIterator *iterator() { return &iterator_; }
  const VectorizedIterator *iterator() const { return &iterator_; }

  const VectorizedIterator::RowBatch *row_batch() const {
    return iterator()->row_batch();
  }

  i32 pos() const { return pos_; }

  i32 limit() const { return limit_; }

 private:
  VectorizedIterator iterator_;
  i32 pos_;
  i32 limit_;
};

}  // namespace tpl::sql