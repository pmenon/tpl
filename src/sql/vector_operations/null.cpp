#include "sql/vector_operations/vector_operators.h"

#include "common/exception.h"
#include "sql/tuple_id_list.h"

namespace tpl::sql {

void VectorOps::IsNull(const Vector &input, TupleIdList *tid_list) {
  TPL_ASSERT(input.num_elements() == tid_list->GetCapacity(),
             "Input vector and TID list have mismatched sizes");
  tid_list->GetMutableBits()->Intersect(input.null_mask());
}

void VectorOps::IsNotNull(const Vector &input, TupleIdList *tid_list) {
  TPL_ASSERT(input.num_elements() == tid_list->GetCapacity(),
             "Input vector and TID list have mismatched sizes");
  tid_list->GetMutableBits()->Difference(input.null_mask());
}

}  // namespace tpl::sql
