#include "sql/vector_operations/vector_operators.h"

#include "common/exception.h"

namespace tpl::sql {

namespace {

template <bool FindNull>
void NullCheckOperation(const Vector &input, Vector *result) {
  if (result->type_id() != TypeId::Boolean) {
    throw InvalidTypeException(result->type_id(),
                               "Result of IS_NULL() or IS_NOT_NULL() must be boolean");
  }

  const Vector::NullMask &null_mask = input.null_mask();
  auto result_data = reinterpret_cast<bool *>(result->data());
  VectorOps::Exec(input, [&](uint64_t i, uint64_t k) {
    result_data[i] = (FindNull ? null_mask[i] : !null_mask[i]);
  });
  result->SetSelectionVector(input.selection_vector(), input.count());
}

}  // namespace

void VectorOps::IsNull(const Vector &input, Vector *result) {
  NullCheckOperation<true>(input, result);
}

void VectorOps::IsNotNull(const Vector &input, Vector *result) {
  NullCheckOperation<false>(input, result);
}

}  // namespace tpl::sql
