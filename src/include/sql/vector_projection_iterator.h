#pragma once

#include <limits>
#include <type_traits>

#include "sql/vector_projection.h"
#include "util/bit_util.h"
#include "util/common.h"
#include "util/macros.h"
#include "util/vector_util.h"

namespace tpl::sql {

/// An iterator over vector projections. A VectorProjectionIterator allows both
/// tuple-at-a-time iteration over a vector projection and vector-at-a-time
/// processing. There are two separate APIs for each and interleaving is
/// supported only to a certain degree. This class exists so that we can iterate
/// over a vector projection multiples times and ensure processing always only
/// on filtered items.
class VectorProjectionIterator {
  static constexpr const u32 kInvalidPos = std::numeric_limits<u32>::max();

 public:
  VectorProjectionIterator();

  explicit VectorProjectionIterator(VectorProjection *vp);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(VectorProjectionIterator);

  /// Has this vector projection been filtered?
  /// \return True if filtered; false otherwise
  bool IsFiltered() const { return selection_vector_[0] != kInvalidPos; }

  /// Set the vector projection to iterate over
  /// \param vp The vector projection
  void SetVectorProjection(VectorProjection *vp) {
    vp_ = vp;
    num_selected_ = vp->TotalTupleCount();
  }

  // -------------------------------------------------------
  // Tuple-at-a-time API
  // -------------------------------------------------------

  /// Get a pointer to the value in the column at index \ref col_idx
  /// \tparam T The desired data type stored in the vector projection
  /// \tparam nullable Whether the column is NULLable
  /// \param col_idx The index of the column to read from
  /// \param[out] null Whether the given column is null
  /// \return The typed value at the current iterator position in the column
  template <typename T, bool nullable>
  const T *Get(u32 col_idx, bool *null) const;

  /// Advance the iterator by a single row
  void Advance();

  /// Advance the iterator by a single entry to the next valid tuple in the
  /// filtered vector projection
  void AdvanceFiltered();

  /// Mark the current tuple as matched/valid (or unmatched/invalid)
  /// \param matched True if the current tuple is matched; false otherwise
  void Match(bool matched);

  /// Does the iterator have another tuple?
  /// \return True if there is more input tuples; false otherwise
  bool HasNext() const;

  /// Does the iterator have another tuple after the filter has been applied
  /// \return True if there is more input tuples; false otherwise
  bool HasNextFiltered() const;

  /// Reset iteration to the beginning of the vector projection
  void Reset();

  /// Reset iteration to the beginning of the filtered vector projection
  void ResetFiltered();

  /// Fun a function over each active tuple in the vector projection. This is a
  /// read-only function (despite it being non-const), meaning the callback must
  /// not modify the state of the iterator, but should only query it using const
  /// functions!
  /// \param fn A callback function
  template <typename F>
  void ForEach(const F &fn);

  /// Run a generic tuple-at-a-time filter over all active tuples in the
  /// vector projection
  /// \tparam F The generic type of the filter function. This can be any
  /// functor-like type including raw function pointer, functor or std::function
  /// \param filter A function that accepts a const version of this VPI and
  /// returns true if the tuple pointed to by the VPI is valid (i.e., passes the
  /// filter) or false otherwise
  template <typename F>
  void RunFilter(const F &filter);

  // -------------------------------------------------------
  // Vectorized API
  // -------------------------------------------------------

  /// Filter the given column by the given value
  /// \tparam Compare The comparison function
  /// \param col_idx The index of the column in the vector projection to filter
  /// \param val The value to filter on
  /// \return The number of selected elements
  template <typename T, template <typename> typename Op, bool nullable>
  u32 FilterColByVal(u32 col_idx, T val);

  /// Return the number of selected tuples after any filters have been applied
  /// \return The number of selected tuples
  u32 num_selected() const { return num_selected_; }

 private:
  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  VectorProjection *vector_projection() { return vp_; }
  const VectorProjection *vector_projection() const { return vp_; }

  u32 current_position() const { return curr_pos_; }

  u32 *selection_vector() { return selection_vector_; }

  u32 selection_vector_read_pos() const { return selection_vector_read_pos_; }

  u32 selection_vector_write_pos() const { return selection_vector_write_pos_; }

 private:
  // The vector projection we're iterating over
  VectorProjection *vp_;

  // The current raw position in the vector projection we're pointing to
  u32 curr_pos_;

  // The number of tuples from the projection that have been selected (filtered)
  u32 num_selected_;

  // The selection vector used to filter the vector projection
  u32 selection_vector_[kDefaultVectorSize];

  // The next slot in the selection vector to read from
  u32 selection_vector_read_pos_;

  // The next slot in the selection vector to write into
  u32 selection_vector_write_pos_;
};

// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

// Retrieve a single column value (and potentially its NULL indicator) from the
// desired column's input data
template <typename T, bool nullable>
inline const T *VectorProjectionIterator::Get(u32 col_idx, bool *null) const {
  if constexpr (nullable) {
    TPL_ASSERT(null != nullptr, "Missing output variable for NULL indicator");
    const u32 *col_null_bitmap = vector_projection()->GetNullBitmap(col_idx);
    *null = util::BitUtil::Test(col_null_bitmap, current_position());
  }

  const T *col_data = vector_projection()->GetVectorAs<T>(col_idx);
  return &col_data[current_position()];
}

inline void VectorProjectionIterator::Advance() { curr_pos_++; }

inline void VectorProjectionIterator::AdvanceFiltered() {
  curr_pos_ = selection_vector()[selection_vector_read_pos()];
  selection_vector_read_pos_++;
}

inline void VectorProjectionIterator::Match(bool matched) {
  selection_vector_[selection_vector_write_pos()] = current_position();
  selection_vector_write_pos_ += matched;
}

inline bool VectorProjectionIterator::HasNext() const {
  return current_position() < vector_projection()->TotalTupleCount();
}

inline bool VectorProjectionIterator::HasNextFiltered() const {
  return selection_vector_read_pos() < num_selected();
}

inline void VectorProjectionIterator::Reset() { curr_pos_ = 0; }

inline void VectorProjectionIterator::ResetFiltered() {
  curr_pos_ = selection_vector()[0];
  num_selected_ = selection_vector_write_pos();
  selection_vector_read_pos_ = 0;
  selection_vector_write_pos_ = 0;
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
    selection_vector_write_pos_ = num_selected();
    ResetFiltered();
  } else {
    for (; HasNext(); Advance()) {
      fn();
    }
    Reset();
  }
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

  // After the filter has been run on the entire vector projection, we need to
  // ensure that we reset it so that clients can query the updated state of the
  // VPI, and subsequent filters operate only on valid tuples potentially
  // filtered out in this filter.
  ResetFiltered();
}

// Filter an entire column's data by the provided constant value
template <typename T, template <typename> typename Op, bool nullable>
inline u32 VectorProjectionIterator::FilterColByVal(u32 col_idx, T val) {
  const T *input = vector_projection()->GetVectorAs<T>(col_idx);

  if (IsFiltered()) {
    selection_vector_write_pos_ = util::VectorUtil::FilterVectorByVal<T, Op>(
        input, num_selected(), val, selection_vector(), selection_vector());
  } else {
    u32 num_tuples = vector_projection()->TotalTupleCount();
    selection_vector_write_pos_ = util::VectorUtil::FilterVectorByVal<T, Op>(
        input, num_tuples, val, selection_vector(), nullptr);
  }

  // After the filter has been run on the entire vector projection, we need to
  // ensure that we reset it so that clients can query the updated state of the
  // VPI, and subsequent filters operate only on valid tuples potentially
  // filtered out in this filter.
  ResetFiltered();

  // After the call to ResetFiltered(), num_selected_ should indicate the number
  // of valid tuples in the filter.
  return num_selected();
}

}  // namespace tpl::sql
