#include "sql/vector_operations/vector_operators.h"

#include "common/exception.h"
#include "common/macros.h"
#include "sql/operations/like_operators.h"
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

  const auto *RESTRICT a_data = reinterpret_cast<const char **>(a.data());
  const auto *RESTRICT b_data = reinterpret_cast<const char **>(b.data());

  // Remove NULL entries from the left input
  tid_list->GetMutableBits()->Difference(a.null_mask());

  // Lift-off
  tid_list->Filter([&](const uint64_t i) { return Op::Apply(a_data[i], b_data[0]); });
}

template <typename Op>
void TemplatedLikeOperation_Vector_Vector(const Vector &a, const Vector &b, TupleIdList *tid_list) {
  TPL_ASSERT(
      a.num_elements() == tid_list->GetCapacity() && b.num_elements() == tid_list->GetCapacity(),
      "Input/output TID list not large enough to store all TIDS from inputs to LIKE()");

  const auto *RESTRICT a_data = reinterpret_cast<const char **>(a.data());
  const auto *RESTRICT b_data = reinterpret_cast<const char **>(b.data());

  // Remove NULL entries in both left and right inputs (cheap)
  tid_list->GetMutableBits()->Difference(a.null_mask()).Difference(b.null_mask());

  // Lift-off
  tid_list->Filter([&](const uint64_t i) { return Op::Apply(a_data[i], b_data[i]); });
}

template <typename Op>
void TemplatedLikeOperation(const Vector &a, const Vector &b, TupleIdList *tid_list) {
  if (a.type_id() != TypeId::Varchar) {
    throw InvalidTypeException(a.type_id(), "Left input to (NOT) LIKE must be VARCHAR");
  }
  if (b.type_id() != TypeId::Varchar) {
    throw InvalidTypeException(a.type_id(), "Right input to (NOT) LIKE must be VARCHAR");
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
