#pragma once

#include <functional>
#include <vector>

#include "common/common.h"
#include "sql/constant_vector.h"
#include "sql/generic_value.h"
#include "sql/sql.h"
#include "sql/tuple_id_list.h"
#include "sql/vector_operations/vector_operators.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"

namespace tpl::sql {

/**
 * This is a helper class to execute filters over vector projections.
 */
class VectorFilterExecutor : public AllStatic {
 public:
  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are equal to the provided constant value (@em val).
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   */
  static void SelectEqualVal(VectorProjection *vector_projection, uint32_t col_idx,
                             const GenericValue &val, TupleIdList *tid_list);

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
                                        const GenericValue &val, TupleIdList *tid_list);

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
                                   const GenericValue &val, TupleIdList *tid_list);

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
                                     const GenericValue &val, TupleIdList *tid_list);

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
                                const GenericValue &val, TupleIdList *tid_list);

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
                                const GenericValue &val, TupleIdList *tid_list);

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

// ---------------------------------------------------------
//
// Implementation
//
// ---------------------------------------------------------

#define GEN_FILTER_VECTOR_GENERIC_VAL(OpName)                                                    \
  inline void VectorFilterExecutor::OpName##Val(VectorProjection *vector_projection,             \
                                                const uint32_t col_idx, const GenericValue &val, \
                                                TupleIdList *tid_list) {                         \
    const auto *left_vector = vector_projection->GetColumn(col_idx);                             \
    VectorOps::OpName(*left_vector, ConstantVector(val), tid_list);                              \
  }

#define GEN_FILTER_VECTOR_VAL(OpName)                                                          \
  inline void VectorFilterExecutor::OpName##Val(VectorProjection *vector_projection,           \
                                                const uint32_t col_idx, const Val &val,        \
                                                TupleIdList *tid_list) {                       \
    const auto *left_vector = vector_projection->GetColumn(col_idx);                           \
    const auto constant = GenericValue::CreateFromRuntimeValue(left_vector->GetTypeId(), val); \
    VectorOps::OpName(*left_vector, ConstantVector(constant), tid_list);                       \
  }

#define GEN_FILTER_VECTOR_VECTOR(OpName)                                                          \
  inline void VectorFilterExecutor::OpName(VectorProjection *vector_projection,                   \
                                           const uint32_t left_col_idx,                           \
                                           const uint32_t right_col_idx, TupleIdList *tid_list) { \
    const auto *left_vector = vector_projection->GetColumn(left_col_idx);                         \
    const auto *right_vector = vector_projection->GetColumn(right_col_idx);                       \
    VectorOps::OpName(*left_vector, *right_vector, tid_list);                                     \
  }

#define GEN_FILTER(OpName)              \
  GEN_FILTER_VECTOR_GENERIC_VAL(OpName) \
  GEN_FILTER_VECTOR_VAL(OpName)         \
  GEN_FILTER_VECTOR_VECTOR(OpName)

GEN_FILTER(SelectEqual);
GEN_FILTER(SelectGreaterThan);
GEN_FILTER(SelectGreaterThanEqual);
GEN_FILTER(SelectLessThan);
GEN_FILTER(SelectLessThanEqual);
GEN_FILTER(SelectNotEqual);

#undef GEN_FILTER
#undef GEN_FILTER_VECTOR_VECTOR
#undef GEN_FILTER_VECTOR_VAL
#undef GEN_FILTER_VECTOR_GENERIC_VAL

}  // namespace tpl::sql
