#pragma once

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
  ColumnVector() noexcept : data_(nullptr), null_bitmap_(nullptr) {}
  ColumnVector(const byte *data, const bool *null_bitmap) noexcept
      : data_(data), null_bitmap_(null_bitmap) {}

  ColumnVector(ColumnVector &&other) noexcept
      : data_(other.data_), null_bitmap_(other.null_bitmap_) {
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
  const T &GetAt(u32 index) const {
    return reinterpret_cast<const T *>(data_)[index];
  }

  bool IsNullAt(u32 index) const { return null_bitmap_[index]; }

 private:
  const byte *data_;
  const bool *null_bitmap_;
};

}  // namespace tpl::sql