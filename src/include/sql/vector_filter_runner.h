#pragma once

#include <functional>
#include <vector>

#include "util/common.h"

namespace tpl::sql {

class GenericValue;
class Vector;
class VectorProjection;
class VectorProjectionIterator;

/**
 * This is a helper class to execute conjunctive filters over an input vector
 * projection. Filters can be applied between a vectors and scalar constants,
 * between two vectors, or between multiple vectors within a projection. While
 * the most common filter functions are specialized here, a generic filtering
 * interface is also provided through @em SelectGeneric() to support
 * user-provided filter implementations.
 *
 * Filtering operations are applied incrementally, meaning they will only be
 * applied to the valid set of the tuples generated from a previous invocation
 * of a selection, using the input vector projection's own selection vector on
 * the first filtering operation.
 *
 * To materialize the results of the filter into the projection, the filter must
 * be finalized through a call to @em Finish(). After this call, the projection
 * will only contain those tuples that passed all filtering operations.
 */
class VectorFilterRunner {
 public:
  /**
   * Create a filter runner using the provided vector projection as input.
   * @param vector_projection The input projection the filter operates on.
   */
  explicit VectorFilterRunner(VectorProjection *vector_projection);

  /**
   * Create a filter runner using the projection contained within provided
   * iterator.
   * @param vector_projection_iterator A vector projection iterator storing the
   *                                   projection to filter.
   */
  explicit VectorFilterRunner(
      VectorProjectionIterator *vector_projection_iterator);

  void SelectEqVal(u32 col_idx, const GenericValue &val);
  void SelectGeVal(u32 col_idx, const GenericValue &val);
  void SelectGtVal(u32 col_idx, const GenericValue &val);
  void SelectLeVal(u32 col_idx, const GenericValue &val);
  void SelectLtVal(u32 col_idx, const GenericValue &val);
  void SelectNeVal(u32 col_idx, const GenericValue &val);

  void SelectEq(u32 left_col_idx, u32 right_col_idx);
  void SelectGe(u32 left_col_idx, u32 right_col_idx);
  void SelectGt(u32 left_col_idx, u32 right_col_idx);
  void SelectLe(u32 left_col_idx, u32 right_col_idx);
  void SelectLt(u32 left_col_idx, u32 right_col_idx);
  void SelectNe(u32 col_idx, u32 right_col_idx);

  using VectorFilterFn = std::function<u32(const Vector *[], sel_t[])>;

  /**
   * Apply a generic selection filter using the vectors at indexes stored in
   * @em col_indexes as input vectors.
   * @param col_indexes The indexes of the columns to operate on.
   * @param filter The filtering function that writes valid selection indexes
   *               into a provided output selection vector.
   */
  void SelectGeneric(const std::vector<u32> &col_indexes,
                     const VectorFilterFn &filter);

  /**
   * Invert the current active selection, i.e., select all currently unselected
   * tuples.
   */
  void InvertSelection();

  /**
   * Materialize the results of the filter in the input vector projection.
   */
  void Finish();

 private:
  // The vector projection we're filtering.
  VectorProjection *vector_projection_;
  // The currently active selection vector. In general, we read using this
  // vector and write using the owned/output selection vector.
  sel_t *sel_vector_;
  // Where we collect the results of a filter.
  sel_t owned_sel_vector_[kDefaultVectorSize];
  // The number of elements in the selection vector.
  u32 count_;
};

}  // namespace tpl::sql
