#pragma once

#include <functional>
#include <vector>

#include "common/common.h"
#include "sql/sql.h"
#include "sql/tuple_id_list.h"

namespace tpl::sql {

class GenericValue;
class Vector;
class VectorProjection;
class VectorProjectionIterator;

struct Val;

/**
 * This is a helper class to execute filters over vector projections. Filters can be applied between
 * vectors and scalars, or between two vectors.
 *
 * Vector filters main a list of TIDs in the input projection that are active. Filtering operations
 * are applied incrementally to only those tuples that are active. To materialize the results of the
 * filter into the projection, the filter must be finalized through a call to
 * VectorFilterExecutor::Finish(). This call atomically makes all valid TIDs active in the input
 * projection.
 *
 * Filters work on column indexes as they appear in the vector projections they operate on. It is
 * the responsibility of the user to be aware of this ordering.
 *
 * @code
 * VectorFilterExecutor filter = ...
 * GenericValue ten = ...
 * filter.SelectEqVal(1, ten);
 * filter.Finalize();
 * @endcode
 *
 * The above code
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

  /**
   * Materialize the results of the filter in the input vector projection.
   */
  void Finish();

 private:
  // The (optional) iterator over the projection that's being filtered.
  VectorProjectionIterator *vector_projection_iterator_;

  // The vector projection we're filtering
  VectorProjection *vector_projection_;

  // The list where we collect the result of the filter
  TupleIdList tid_list_;
};

}  // namespace tpl::sql
