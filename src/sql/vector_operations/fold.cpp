#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

// This file contains functions that fold vectors into a single return value.

bool VectorOps::AllTrue(const Vector &input) {
  TPL_ASSERT(input.type_id() == TypeId::Boolean, "Input vector to AllTrue() must be boolean");
  if (input.count() == 0) {
    return false;
  }

  bool result = true;
  if (input.null_mask().Any()) {
    // Slow-path: Input has NULLs we need to check
    ExecTyped<bool>(
        input, [&](bool val, u64 i, u64 k) { result = result && (!input.null_mask_[i] && val); });
  } else {
    // Fast-path: No NULL check needed
    ExecTyped<bool>(input, [&](bool val, u64 i, u64 k) { result = result && val; });
  }
  return result;
}

bool VectorOps::AnyTrue(const Vector &input) {
  TPL_ASSERT(input.type_id() == TypeId::Boolean, "Input vector to AnyTrue() must be boolean");
  if (input.count() == 0) {
    return false;
  }

  bool result = false;
  if (input.null_mask().Any()) {
    // Slow-path: Input has NULLs we need to check
    ExecTyped<bool>(
        input, [&](bool val, u64 i, u64 k) { result = result || (!input.null_mask_[i] && val); });
  } else {
    // Fast-path: No NULL check needed
    ExecTyped<bool>(input, [&](bool val, u64 i, u64 k) { result = result || val; });
  }
  return result;
}

}  // namespace tpl::sql
