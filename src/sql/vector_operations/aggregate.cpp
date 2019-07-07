#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

bool VectorOps::AllTrue(const Vector &input) {
  TPL_ASSERT(input.type_id() == TypeId::Boolean,
             "Input vector to AllTrue() must be boolean");
  if (input.count() == 0) {
    return false;
  }
  bool result = true;
  ExecTyped<bool>(input, [&](bool val, u64 i, u64 k) {
    result = result && (!input.null_mask_[i] && val);
  });
  return result;
}

bool VectorOps::AnyTrue(const Vector &input) {
  TPL_ASSERT(input.type_id() == TypeId::Boolean,
             "Input vector to AnyTrue() must be boolean");
  if (input.count() == 0) {
    return false;
  }
  bool result = false;
  ExecTyped<bool>(input, [&](bool val, u64 i, u64 k) {
    result = result || (!input.null_mask_[i] && val);
  });
  return result;
}

}  // namespace tpl::sql
