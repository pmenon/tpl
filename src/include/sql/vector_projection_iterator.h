#pragma once

#include <limits>
#include <type_traits>

#include "common/common.h"
#include "common/macros.h"
#include "sql/vector_projection.h"
#include "util/bit_util.h"

namespace tpl::sql {

/**
 * A tuple-at-a-time iterator over VectorProjections. The iterator gives the <i>view</i> of
 * individual tuple access, but does not physically materialize full tuples in memory. Tuples can
 * also be filtered out of the underlying projection (again without moving or copying any data)
 * through VectorProjectionIterator::Match() which considers the tuple that the iterator is
 * positioned at.
 *
 * A different iteration API exists depending on whether the underlying vector projection has been
 * filtered or not. Users must query filtration status before iteration through
 * VectorProjectionIterator::IsFiltered().
 *
 * @code
 * VectorProjectionIterator iter = ...
 * if (iter.IsFiltered()) {
 *   for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
 *     // do work
 *   }
 * } else {
 *   for (; iter.HasNext(); iter.Advance()) {
 *     // do work
 *   }
 * }
 * @endcode
 *
 * The above template exists only for TPL programs, since lambdas don't exist there. For you regular
 * C++ folks, use VectorProjectionIterator::ForEach().
 */
class VectorProjectionIterator {
 public:
  /**
   * Create an empty iterator over an empty projection.
   */
  VectorProjectionIterator()
      : vector_projection_(nullptr),
        curr_idx_(0),
        sel_vector_(nullptr),
        sel_vector_read_idx_(0),
        sel_vector_write_idx_(0) {}

  /**
   * Create an iterator over the given projection @em vp.
   * @param vp The projection to iterator over.
   */
  explicit VectorProjectionIterator(VectorProjection *vp) : VectorProjectionIterator() {
    Reset(vp);
  }

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(VectorProjectionIterator);

  /**
   * Has this vector projection been filtered? Does it have a selection vector?
   * @return True if filtered; false otherwise.
   */
  bool IsFiltered() const { return vector_projection_->IsFiltered(); }

  /**
   * Reset this iterator to begin iteration over the given projection @em vp.
   * @param vp The vector projection to iterate over.
   */
  void Reset(VectorProjection *vp) {
    vector_projection_ = vp;
    curr_idx_ = vp->IsFiltered() ? vp->sel_vector_[0] : 0;
    sel_vector_ = vp->sel_vector_;
    sel_vector_read_idx_ = 0;
    sel_vector_write_idx_ = 0;
  }

  /**
   * Return the vector projection this iterator is iterating over.
   * @return The vector projection that's being iterated over.
   */
  VectorProjection *GetVectorProjection() const { return vector_projection_; }

  /**
   * Get a pointer to the value in the column at index @em col_idx.
   * @tparam T The desired data type stored in the vector projection.
   * @tparam nullable Whether the column is NULL-able.
   * @param col_idx The index of the column to read from.
   * @param[out] null null Whether the given column is null.
   * @return The typed value at the current iterator position in the column.
   */
  template <typename T, bool Nullable>
  const T *GetValue(uint32_t col_idx, bool *null) const;

  /**
   * Set the value of the column at index @em col_idx for the tuple the iterator is currently
   * positioned at to @em val. If the column is NULL-able, the NULL bit is also set to the provided
   * NULL value @em null.
   * @tparam T The desired primitive data type of the column.
   * @tparam Nullable Whether the column is NULL-able.
   * @param col_idx The index of the column to write to.
   * @param val The value to write.
   * @param null Whether the value is NULL.
   */
  template <typename T, bool Nullable>
  void SetValue(uint32_t col_idx, T val, bool null);

  /**
   * Set the current iterator position.
   * @tparam IsFiltered Is this VPI filtered?
   * @param idx The index the iteration should jump to
   */
  template <bool IsFiltered>
  void SetPosition(uint32_t idx);

  /**
   * Advance the iterator by one tuple.
   */
  void Advance();

  /**
   * Advance the iterator by one to the next valid tuple in the filtered projection.
   */
  void AdvanceFiltered();

  /**
   * Mark the tuple this iterator is currently positioned at as valid or invalid.
   * @param matched True if the current tuple is valid; false otherwise
   */
  void Match(bool matched);

  /**
   * Does the iterator have another tuple?
   * @return True if there is more input tuples; false otherwise
   */
  bool HasNext() const;

  /**
   * Does the iterator have another tuple after the filter has been applied?
   * @return True if there is more input tuples; false otherwise
   */
  bool HasNextFiltered() const;

  /**
   * Reset iteration to the beginning of the vector projection.
   */
  void Reset();

  /**
   * Reset iteration to the beginning of the filtered vector projection.
   */
  void ResetFiltered();

  /**
   * Run a function over each active tuple in the vector projection. This is a read-only function
   * (despite it being non-const), meaning the callback must not modify the state of the iterator,
   * but should only query it using const functions!
   * @tparam F The function type
   * @param fn A callback function
   */
  template <typename F>
  void ForEach(const F &fn);

  /**
   * Run a generic tuple-at-a-time filter over all active tuples in the vector projection.
   * @tparam F The generic type of the filter function. This can be any functor-like type including
   *           raw function pointer, functor or std::function.
   * @param filter A function that accepts a const version of this VPI and returns true if the tuple
   *               pointed to by the VPI is valid (i.e., passes the filter) or false otherwise.
   */
  template <typename F>
  void RunFilter(const F &filter);

  /**
   * Return the number of selected tuples after any filters have been applied.
   */
  uint32_t GetTupleCount() const { return vector_projection_->GetSelectedTupleCount(); }

 private:
  // The vector projection we're iterating over
  VectorProjection *vector_projection_;

  // The current raw position in the vector projection we're pointing to
  uint32_t curr_idx_;

  // The selection vector used to filter the vector projection
  sel_t *sel_vector_;

  // The next slot in the selection vector to read from
  uint32_t sel_vector_read_idx_;

  // The next slot in the selection vector to write into
  uint32_t sel_vector_write_idx_;
};

// ---------------------------------------------------------
//
// Implementation below
//
// ---------------------------------------------------------

// The below methods are inlined in the header on purpose for performance. Please do not move them.

// Note: The getting and setter functions operate on the underlying vector's raw data rather than
// going through Vector::GetValue() or Vector::SetValue(). This implies that the user is aware of
// the underlying vector's type and its nullability property. We take advantage of that here.

template <typename T, bool Nullable>
inline const T *VectorProjectionIterator::GetValue(uint32_t col_idx, bool *null) const {
  // The vector we'll read from
  const Vector *col_vector = vector_projection_->GetColumn(col_idx);

  if constexpr (Nullable) {
    TPL_ASSERT(null != nullptr, "Missing output variable for NULL indicator");
    *null = col_vector->null_mask_[curr_idx_];
  }

  return reinterpret_cast<const T *>(col_vector->data_) + curr_idx_;
}

template <typename T, bool Nullable>
inline void VectorProjectionIterator::SetValue(uint32_t col_idx, const T val, bool null) {
  // The vector we'll write into
  Vector *col_vector = vector_projection_->GetColumn(col_idx);

  // If the column is NULL-able, we check the NULL indication flag before writing into the columns's
  // underlying data array. If the column isn't NULL-able, we can skip the NULL check and directly
  // write into the column data array.

  if constexpr (Nullable) {
    col_vector->null_mask_[curr_idx_] = null;
    if (!null) {
      reinterpret_cast<T *>(col_vector->data_)[curr_idx_] = val;
    }
  } else {
    reinterpret_cast<T *>(col_vector->data_)[curr_idx_] = val;
  }
}

template <bool Filtered>
inline void VectorProjectionIterator::SetPosition(uint32_t idx) {
  TPL_ASSERT(idx < GetTupleCount(), "Out of bounds access");
  if constexpr (Filtered) {
    TPL_ASSERT(IsFiltered(), "Attempting to set position in unfiltered VPI");
    sel_vector_read_idx_ = idx;
    curr_idx_ = sel_vector_[sel_vector_read_idx_];
  } else {
    TPL_ASSERT(!IsFiltered(), "Attempting to set position in filtered VPI");
    curr_idx_ = idx;
  }
}

inline void VectorProjectionIterator::Advance() { curr_idx_++; }

inline void VectorProjectionIterator::AdvanceFiltered() {
  curr_idx_ = sel_vector_[++sel_vector_read_idx_];
}

inline void VectorProjectionIterator::Match(bool matched) {
  sel_vector_[sel_vector_write_idx_] = curr_idx_;
  sel_vector_write_idx_ += static_cast<uint32_t>(matched);
}

inline bool VectorProjectionIterator::HasNext() const {
  return curr_idx_ < vector_projection_->GetSelectedTupleCount();
}

inline bool VectorProjectionIterator::HasNextFiltered() const {
  return sel_vector_read_idx_ < vector_projection_->GetSelectedTupleCount();
}

inline void VectorProjectionIterator::Reset() {
  curr_idx_ = IsFiltered() ? sel_vector_[0] : 0;
  sel_vector_read_idx_ = 0;
  sel_vector_write_idx_ = 0;
}

inline void VectorProjectionIterator::ResetFiltered() {
  // Update the projection counts
  for (uint32_t i = 0; i < vector_projection_->GetNumColumns(); i++) {
    vector_projection_->GetColumn(i)->SetSelectionVector(sel_vector_, sel_vector_write_idx_);
  }

  // Reset
  curr_idx_ = sel_vector_[0];
  sel_vector_read_idx_ = 0;
  sel_vector_write_idx_ = 0;
}

template <typename F>
inline void VectorProjectionIterator::ForEach(const F &fn) {
  // Ensure function conforms to expected form
  static_assert(std::is_invocable_r_v<void, F>,
                "Iteration function must be a no-arg void-return function");

  if (IsFiltered()) {
    for (; HasNextFiltered(); AdvanceFiltered()) {
      fn();
    }
  } else {
    for (; HasNext(); Advance()) {
      fn();
    }
  }

  Reset();
}

template <typename F>
inline void VectorProjectionIterator::RunFilter(const F &filter) {
  // Ensure filter function conforms to expected form
  static_assert(std::is_invocable_r_v<bool, F>,
                "Filter function must be a no-arg function returning a bool");

  if (IsFiltered()) {
    for (; HasNextFiltered(); AdvanceFiltered()) {
      bool valid = filter();
      Match(valid);
    }
  } else {
    for (; HasNext(); Advance()) {
      bool valid = filter();
      Match(valid);
    }
  }

  ResetFiltered();
}

}  // namespace tpl::sql
