#include "sql/vector_operations/vector_operations.h"

#include "common/exception.h"
#include "common/macros.h"
#include "sql/operators/string_operators.h"
#include "sql/tuple_id_list.h"

namespace tpl::sql {

namespace {

template <typename Op>
void TemplatedLikeOperation_Vector_Constant(const Vector &a, const Vector &b,
                                            TupleIdList *tid_list) {
  if (b.IsNull(0)) {
    tid_list->Clear();
    return;
  }

  const auto *RESTRICT a_data = reinterpret_cast<const VarlenEntry *>(a.GetData());
  const auto *RESTRICT b_data = reinterpret_cast<const VarlenEntry *>(b.GetData());

  // Remove NULL entries from the left input
  tid_list->GetMutableBits()->Difference(a.GetNullMask());

  // Lift-off
  tid_list->Filter([&](const uint64_t i) { return Op{}(a_data[i], b_data[0]); });
}

template <typename Op>
void TemplatedLikeOperation_Vector_Vector(const Vector &a, const Vector &b, TupleIdList *tid_list) {
  TPL_ASSERT(a.GetSize() == tid_list->GetCapacity() && b.GetSize() == tid_list->GetCapacity(),
             "Input/output TID list not large enough to store all TIDS from inputs to LIKE()");

  const auto *RESTRICT a_data = reinterpret_cast<const VarlenEntry *>(a.GetData());
  const auto *RESTRICT b_data = reinterpret_cast<const VarlenEntry *>(b.GetData());

  // Remove NULL entries in both left and right inputs (cheap)
  tid_list->GetMutableBits()->Difference(a.GetNullMask()).Difference(b.GetNullMask());

  // Lift-off
  tid_list->Filter([&](const uint64_t i) { return Op{}(a_data[i], b_data[i]); });
}

template <typename Op>
void TemplatedLikeOperation(const Vector &a, const Vector &b, TupleIdList *tid_list) {
  if (a.GetTypeId() != TypeId::Varchar || b.GetTypeId() != TypeId::Varchar) {
    throw InvalidTypeException(a.GetTypeId(), "Inputs to (NOT) LIKE must be VARCHAR");
  }
  if (a.IsConstant()) {
    throw Exception(ExceptionType::Execution, "First input to LIKE cannot be constant");
  }

  if (b.IsConstant()) {
    TemplatedLikeOperation_Vector_Constant<Op>(a, b, tid_list);
  } else {
    TemplatedLikeOperation_Vector_Vector<Op>(a, b, tid_list);
  }
}

}  // namespace

void VectorOps::Like(const Vector &a, const Vector &b, TupleIdList *tid_list) {
  TemplatedLikeOperation<sql::Like>(a, b, tid_list);
}

void VectorOps::NotLike(const Vector &a, const Vector &b, TupleIdList *tid_list) {
  TemplatedLikeOperation<sql::NotLike>(a, b, tid_list);
}

}  // namespace tpl::sql
