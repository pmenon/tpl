#include "sql/vector_operations/vector_operators.h"

#include "sql/operations/boolean_operators.h"
#include "sql/vector_operations/unary_op_helpers.h"

namespace tpl::sql {

namespace {

template <typename Op, typename OpNull>
void BooleanLogicOperation(const Vector &left, const Vector &right,
                           Vector *result) {
  TPL_ASSERT(
      left.type_id() == TypeId::Boolean && right.type_id() == TypeId::Boolean,
      "Inputs to boolean logic op must be boolean");

  const auto *left_data = reinterpret_cast<const bool *>(left.data());
  const auto *right_data = reinterpret_cast<const bool *>(right.data());
  const auto &left_null_mask = left.null_mask();
  const auto &right_null_mask = right.null_mask();

  auto *result_data = reinterpret_cast<bool *>(result->data());
  Vector::NullMask result_mask;

  if (left.IsConstant()) {
    VectorOps::Exec(right, [&](u64 i, u64 k) {
      result_data[i] = Op::Apply(left_data[0], right_data[i]);
      result_mask[i] = OpNull::Apply(left_data[0], right_data[i],
                                     left_null_mask[0], right_null_mask[i]);
    });
  } else if (right.IsConstant()) {
    BooleanLogicOperation<Op, OpNull>(right, left, result);
    return;
  } else {
    TPL_ASSERT(left.selection_vector() == right.selection_vector(),
               "Mismatched selection vectors");
    TPL_ASSERT(left.count() == right.count(), "Mismatched counts");
    VectorOps::Exec(left, [&](u64 i, u64 k) {
      result_data[i] = Op::Apply(left_data[i], right_data[i]);
      result_mask[i] = OpNull::Apply(left_data[i], right_data[i],
                                     left_null_mask[i], right_null_mask[i]);
    });
  }

  result->set_null_mask(result_mask);
  result->SetSelectionVector(right.selection_vector(), right.count());
}

}  // namespace

void VectorOps::And(const Vector &left, const Vector &right, Vector *result) {
  BooleanLogicOperation<tpl::sql::And, tpl::sql::AndNullable>(left, right,
                                                              result);
}

void VectorOps::Or(const Vector &left, const Vector &right, Vector *result) {
  BooleanLogicOperation<tpl::sql::Or, tpl::sql::OrNullable>(left, right,
                                                            result);
}

void VectorOps::Not(const Vector &input, Vector *result) {
  TemplatedUnaryOperation<bool, bool, tpl::sql::Not>(input, result);
}

}  // namespace tpl::sql
