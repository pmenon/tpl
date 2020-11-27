#pragma once

#include "common/common.h"
#include "common/macros.h"
#include "sql/type.h"
#include "util/bit_util.h"

namespace tpl::sql {

/**
 * A column segment represents a compact array of column values along with a dense positionally
 * aligned bitmap indicating whether the column value is NULL.
 */
class ColumnSegment {
 public:
  /**
   * Construct a column segment with the given SQL type @em sql_type, underlying data @em data, NULL
   * bitmap @em null_bitmap, and size @em num_tuples.
   * @param sql_type The SQL type of the column.
   * @param data The underlying data for the column.
   * @param null_bitmap The NULL bitmap for the column's values.
   * @param num_tuples The number of tuples in this segment.
   */
  ColumnSegment(Type type, byte *data, uint32_t *null_bitmap,
                uint32_t num_tuples) noexcept
      : type_(type), data_(data), null_bitmap_(null_bitmap), num_tuples_(num_tuples) {}

  /**
   * Move constructor.
   * @param other The segment to move into this instance.
   */
  ColumnSegment(ColumnSegment &&other) noexcept
      : type_(other.type_),
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
  const T &TypedAccessAt(uint32_t idx) const {
    TPL_ASSERT(idx < GetTupleCount(), "Invalid row index!");
    const T *typed_data = reinterpret_cast<const T *>(data_);
    return typed_data[idx];
  }

  /**
   * Is the value at the given index NULL
   * @param idx The index to check
   * @return True if the value is null; false otherwise
   */
  bool IsNullAt(uint32_t idx) const { return util::BitUtil::Test(null_bitmap_, idx); }

  /**
   * @return The SQL type of the column.
   */
  const Type &GetSqlType() const { return type_; }

  /**
   * @return The number of tuples in the column.
   */
  uint32_t GetTupleCount() const { return num_tuples_; }

 private:
  friend class ColumnVectorIterator;

  auto *AccessRaw(uint32_t idx) const { return &data_[idx]; }

  auto *AccessRawNullBitmap(uint32_t idx) const { return &null_bitmap_[idx]; }

 private:
  // The SQL type
  Type type_;

  // The underlying data
  byte *data_;

  // The NULL bitmap
  uint32_t *null_bitmap_;

  // The number of tuples
  uint32_t num_tuples_;
};

}  // namespace tpl::sql
