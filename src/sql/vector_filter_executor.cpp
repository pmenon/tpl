#include "sql/vector_filter_executor.h"

#include "sql/constant_vector.h"
#include "sql/generic_value.h"
#include "sql/tuple_id_list.h"
#include "sql/vector_operations/vector_operations.h"
#include "sql/vector_projection.h"
#include "sql/vector_projection_iterator.h"

namespace tpl::sql {

#define GEN_FILTER(OpName)                                                                     \
  /* Select vector-with-constant runtime value. */                                             \
  void VectorFilterExecutor::OpName##Val(VectorProjection *vector_projection,                  \
                                         const uint32_t col_idx, const Val &val,               \
                                         TupleIdList *tid_list) {                              \
    const auto *left_vector = vector_projection->GetColumn(col_idx);                           \
    const auto constant = GenericValue::CreateFromRuntimeValue(left_vector->GetTypeId(), val); \
    VectorOps::OpName(*left_vector, ConstantVector(constant), tid_list);                       \
  }                                                                                            \
  /* Select vector-with-vector. */                                                             \
  void VectorFilterExecutor::OpName(VectorProjection *vector_projection,                       \
                                    const uint32_t left_col_idx, const uint32_t right_col_idx, \
                                    TupleIdList *tid_list) {                                   \
    const auto *left_vector = vector_projection->GetColumn(left_col_idx);                      \
    const auto *right_vector = vector_projection->GetColumn(right_col_idx);                    \
    VectorOps::OpName(*left_vector, *right_vector, tid_list);                                  \
  }

GEN_FILTER(SelectEqual);
GEN_FILTER(SelectGreaterThan);
GEN_FILTER(SelectGreaterThanEqual);
GEN_FILTER(SelectLessThan);
GEN_FILTER(SelectLessThanEqual);
GEN_FILTER(SelectNotEqual);

}  // namespace tpl::sql
