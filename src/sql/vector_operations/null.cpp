#include "sql/vector_operations/vector_operations.h"

#include "sql/tuple_id_list.h"

namespace tpl::sql {

void VectorOps::IsNull(const Vector &input, TupleIdList *tid_list) {
  TPL_ASSERT(input.GetSize() == tid_list->GetCapacity(), "Input vector size != TID list size");
  tid_list->GetMutableBits()->Intersect(input.GetNullMask());
}

void VectorOps::IsNotNull(const Vector &input, TupleIdList *tid_list) {
  TPL_ASSERT(input.GetSize() == tid_list->GetCapacity(), "Input vector size != TID list size");
  tid_list->GetMutableBits()->Difference(input.GetNullMask());
}

}  // namespace tpl::sql
