#pragma once

#include "sql/data_types.h"
#include "util/common.h"
#include "util/macros.h"

namespace tpl::sql {

/**
 * A column vector represents a compact array of column values along with a
 * compact, positionally aligned bitmap indicating whether a column value is
 * NULL.
 */
class ColumnVector {
 public:
  ColumnVector(const Type &type, const byte *data, const bool *null_bitmap,
               u32 num_rows) noexcept
      : type_(type),
        data_(data),
        null_bitmap_(null_bitmap),
        num_rows_(num_rows) {}

  ColumnVector(ColumnVector &&other) noexcept
      : type_(other.type_),
        data_(other.data_),
        null_bitmap_(other.null_bitmap_),
        num_rows_(other.num_rows_) {
    other.data_ = nullptr;
    other.null_bitmap_ = nullptr;
  }

  DISALLOW_COPY(ColumnVector);

  ~ColumnVector() {
    if (data_ != nullptr) {
      std::free((void *)data_);
      std::free((void *)null_bitmap_);
    }
  }

  template <typename T>
  const T &TypedAccessAt(u32 index) const {
    TPL_ASSERT(index < num_rows(), "Invalid row index!");
    return Raw<T>()[index];
  }

  bool IsNullAt(u32 index) const { return null_bitmap_[index]; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  const Type &type() const { return type_; }

  u32 num_rows() const { return num_rows_; }

 private:
  template <typename T>
  const T *Raw() const {
    return reinterpret_cast<const T *>(data_);
  }

 private:
  const Type &type_;
  const byte *data_;
  const bool *null_bitmap_;
  u32 num_rows_;
};

}  // namespace tpl::sql