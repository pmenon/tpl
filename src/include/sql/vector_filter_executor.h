#pragma once

#include "common/common.h"

namespace tpl::sql {

class VectorProjection;
class TupleIdList;
struct Val;

/**
 * This is a helper class to execute filters over vector projections. These are used exclusively
 * from generated code since we don't want to worry about constant vector construction.
 */
class VectorFilterExecutor : public AllStatic {
 public:
  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are equal to the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  static void SelectEqualVal(VectorProjection *vector_projection, uint32_t col_idx, const Val &val,
                             TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are greater than or equal to the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  static void SelectGreaterThanEqualVal(VectorProjection *vector_projection, uint32_t col_idx,
                                        const Val &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are strictly greater than the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  static void SelectGreaterThanVal(VectorProjection *vector_projection, uint32_t col_idx,
                                   const Val &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are less than or equal to the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  static void SelectLessThanEqualVal(VectorProjection *vector_projection, uint32_t col_idx,
                                     const Val &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are strictly less than the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  static void SelectLessThanVal(VectorProjection *vector_projection, uint32_t col_idx,
                                const Val &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are not equal to the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  static void SelectNotEqualVal(VectorProjection *vector_projection, uint32_t col_idx,
                                const Val &val, TupleIdList *tid_list);

  /**
   * Select tuples whose values in the left (first) column are equal to the values in the right
   * (second) column.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   */
  static void SelectEqual(VectorProjection *vector_projection, uint32_t left_col_idx,
                          uint32_t right_col_idx, TupleIdList *tid_list);

  /**
   * Select tuples whose values in the left (first) column are greater than or equal to the values
   * in the right (second) column.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   */
  static void SelectGreaterThanEqual(VectorProjection *vector_projection, uint32_t left_col_idx,
                                     uint32_t right_col_idx, TupleIdList *tid_list);

  /**
   * Select tuples whose values in the left (first) column are greater than the values in the right
   * (second) column.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   */
  static void SelectGreaterThan(VectorProjection *vector_projection, uint32_t left_col_idx,
                                uint32_t right_col_idx, TupleIdList *tid_list);

  /**
   * Select tuples whose values in the left (first) column are less than or equal to the values in
   * the right (second) column.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   */
  static void SelectLessThanEqual(VectorProjection *vector_projection, uint32_t left_col_idx,
                                  uint32_t right_col_idx, TupleIdList *tid_list);

  /**
   * Select tuples whose values in the left (first) column are less than the values in the right
   * (second) column.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   */
  static void SelectLessThan(VectorProjection *vector_projection, uint32_t left_col_idx,
                             uint32_t right_col_idx, TupleIdList *tid_list);

  /**
   * Select tuples whose values in the left (first) column are not equal to the values in the right
   * (second) column.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   */
  static void SelectNotEqual(VectorProjection *vector_projection, uint32_t col_idx,
                             uint32_t right_col_idx, TupleIdList *tid_list);
};

}  // namespace tpl::sql
