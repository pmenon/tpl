#pragma once

#include <functional>
#include <vector>

#include "common/common.h"

namespace tpl::sql {

class GenericValue;
class Vector;
class VectorProjection;
class VectorProjectionIterator;

struct Val;

/**
 * This is a helper class to execute conjunctive filters over an input vector projection. Filters
 * can be applied between a vectors and scalar constants, between two vectors, or between multiple
 * vectors within a projection. While the most common filter functions are specialized here, a
 * generic filtering interface is also provided through @em SelectGeneric() to support user-provided
 * filter implementations.
 *
 * Filtering operations are applied incrementally, meaning they will only be applied to the valid
 * set of the tuples generated from a previous invocation of a selection, using the input vector
 * projection's own selection vector on the first filtering operation.
 *
 * To materialize the results of the filter into the projection, the filter must be finalized
 * through a call to @em Finish(). After this call, the projection will only contain those tuples
 * that passed all filtering operations.
 */
class VectorFilterExecutor {
 public:
  /**
   * Create a filter runner using the provided vector projection as input.
   * @param vector_projection The input projection the filter operates on.
   */
  explicit VectorFilterExecutor(VectorProjection *vector_projection);

  /**
   * Create a filter runner using the projection contained within provided iterator.
   * @param vector_projection_iterator A vector projection iterator to filter.
   */
  explicit VectorFilterExecutor(VectorProjectionIterator *vector_projection_iterator);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are equal to the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  void SelectEqVal(uint32_t col_idx, const GenericValue &val);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are equal to the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  void SelectEqVal(uint32_t col_idx, const Val &val);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are greater than or equal to the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  void SelectGeVal(uint32_t col_idx, const GenericValue &val);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are greater than or equal to the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  void SelectGeVal(uint32_t col_idx, const Val &val);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are strictly greater than the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  void SelectGtVal(uint32_t col_idx, const GenericValue &val);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are strictly greater than the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  void SelectGtVal(uint32_t col_idx, const Val &val);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are less than or equal to the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  void SelectLeVal(uint32_t col_idx, const GenericValue &val);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are less than or equal to the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  void SelectLeVal(uint32_t col_idx, const Val &val);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are strictly less than the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  void SelectLtVal(uint32_t col_idx, const GenericValue &val);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are strictly less than the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  void SelectLtVal(uint32_t col_idx, const Val &val);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are not equal to the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  void SelectNeVal(uint32_t col_idx, const GenericValue &val);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are not equal to the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  void SelectNeVal(uint32_t col_idx, const Val &val);

  /**
   * Select tuples whose values in the left (first) column are equal to the values in the right
   * (second) column.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   */
  void SelectEq(uint32_t left_col_idx, uint32_t right_col_idx);

  /**
   * Select tuples whose values in the left (first) column are greater than or equal to the values
   * in the right (second) column.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   */
  void SelectGe(uint32_t left_col_idx, uint32_t right_col_idx);

  /**
   * Select tuples whose values in the left (first) column are greater than the values in the right
   * (second) column.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   */
  void SelectGt(uint32_t left_col_idx, uint32_t right_col_idx);

  /**
   * Select tuples whose values in the left (first) column are less than or equal to the values in
   * the right (second) column.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   */
  void SelectLe(uint32_t left_col_idx, uint32_t right_col_idx);

  /**
   * Select tuples whose values in the left (first) column are less than the values in the right
   * (second) column.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   */
  void SelectLt(uint32_t left_col_idx, uint32_t right_col_idx);

  /**
   * Select tuples whose values in the left (first) column are not equal to the values in the right
   * (second) column.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   */
  void SelectNe(uint32_t col_idx, uint32_t right_col_idx);

  using VectorFilterFn = std::function<uint32_t(const Vector *[], sel_t[])>;

  /**
   * Apply a generic selection filter using the vectors at indexes stored in @em col_indexes as
   * input vectors.
   * @param col_indexes The indexes of the columns to operate on.
   * @param filter The filtering function that writes valid selection indexes
   *               into a provided output selection vector.
   */
  void SelectGeneric(const std::vector<uint32_t> &col_indexes, const VectorFilterFn &filter);

  /**
   * Invert the current active selection, i.e., select all currently unselected tuples.
   */
  void InvertSelection();

  /**
   * Materialize the results of the filter in the input vector projection.
   */
  void Finish();

 private:
  // Helper to invoke a given filter function over a collection of input vectors
  template <typename F>
  void SelectInternal(const uint32_t col_indexes[], uint32_t num_cols, F &&filter);

 private:
  // The vector projection we're filtering.
  VectorProjection *vector_projection_;
  // The currently active selection vector. In general, we read using this
  // vector and write using the owned/output selection vector.
  sel_t *sel_vector_;
  // Where we collect the results of a filter.
  sel_t owned_sel_vector_[kDefaultVectorSize];
  // The number of elements in the selection vector.
  uint32_t count_;
};

}  // namespace tpl::sql
