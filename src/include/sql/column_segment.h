#pragma once

#include "sql/data_types.h"
#include "util/bit_util.h"
#include "util/common.h"
#include "util/macros.h"

namespace tpl::sql {

/**
 * A column segment represents a compact array of column values along with a
 * dense positionally aligned bitmap indicating whether the column value is
 * NULL.
 */
class ColumnSegment {
 public:
  ColumnSegment(const SqlType &sql_type, byte *data, u32 *null_bitmap, u32 num_tuples) noexcept
      : sql_type_(sql_type), data_(data), null_bitmap_(null_bitmap), num_tuples_(num_tuples) {}

  ColumnSegment(ColumnSegment &&other) noexcept
      : sql_type_(other.sql_type_),
        data_(other.data_),
        null_bitmap_(other.null_bitmap_),
        num_tuples_(other.num_tuples_) {
    other.data_ = nullptr;
    other.null_bitmap_ = nullptr;
  }

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY(ColumnSegment);

  /**
   * Destructor. Free's data if allocated.
   */
  ~ColumnSegment() {
    if (data_ != nullptr) {
      std::free(data_);
      std::free(null_bitmap_);
    }
  }

  /**
   * Read the value of type @em T at the given index within the column's data
   * @tparam T The type of the value to read. We make no assumptions on copy
   * @param idx
   * \return A reference to the value at index @em index
   */
  template <typename T>
  const T &TypedAccessAt(u32 idx) const {
    TPL_ASSERT(idx < num_tuples(), "Invalid row index!");
    const T *typed_data = reinterpret_cast<const T *>(data_);
    return typed_data[idx];
  }

  /**
   * Is the value at the given index NULL
   * @param idx The index to check
   * @return True if the value is null; false otherwise
   */
  bool IsNullAt(u32 idx) const { return util::BitUtil::Test(null_bitmap_, idx); }

  /**
   * Return the SQL type of the column.
   */
  const SqlType &sql_type() const { return sql_type_; }

  /**
   * Return the number of values in the column
   */
  u32 num_tuples() const { return num_tuples_; }

 private:
  friend class ColumnVectorIterator;

  auto *AccessRaw(u32 idx) const { return &data_[idx]; }

  auto *AccessRawNullBitmap(u32 idx) const { return &null_bitmap_[idx]; }

 private:
  const SqlType &sql_type_;
  byte *data_;
  u32 *null_bitmap_;
  u32 num_tuples_;
};

}  // namespace tpl::sql
