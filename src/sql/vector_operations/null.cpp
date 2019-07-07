#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

namespace {

template <bool FindNull>
void NullCheckLoop(const Vector &input, Vector *result) {
  TPL_ASSERT(result->type_id() == TypeId::Boolean, "Result must be boolean");
  const auto &null_mask = input.null_mask();
  auto result_data = reinterpret_cast<bool *>(result->data());
  VectorOps::Exec(input, [&](u64 i, u64 k) {
    result_data[i] = (FindNull ? null_mask[i] : !null_mask[i]);
  });
  result->SetSelectionVector(input.selection_vector(), input.count());
}

}  // namespace

void VectorOps::IsNull(const Vector &input, Vector *result) {
  NullCheckLoop<true>(input, result);
}

void VectorOps::IsNotNull(const Vector &input, Vector *result) {
  NullCheckLoop<false>(input, result);
}

}  // namespace tpl::sql
