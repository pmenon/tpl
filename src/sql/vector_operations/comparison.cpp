#include "sql/vector_operations/vector_operators.h"

#include "sql/operations/comparison_operators.h"
#include "sql/vector_operations/binary_op_helpers.h"
#include "util/common.h"

namespace tpl::sql {

namespace {

template <typename Op>
void ComparisonOperation(const Vector &left, const Vector &right,
                         Vector *result) {
  switch (left.type_id()) {
    case TypeId::Boolean: {
      TemplatedBinaryOperation<bool, bool, bool, Op>(left, right, result);
      break;
    }
    case TypeId::TinyInt: {
      TemplatedBinaryOperation<i8, i8, bool, Op>(left, right, result);
      break;
    }
    case TypeId::SmallInt: {
      TemplatedBinaryOperation<i16, i16, bool, Op>(left, right, result);
      break;
    }
    case TypeId::Integer: {
      TemplatedBinaryOperation<i32, i32, bool, Op>(left, right, result);
      break;
    }
    case TypeId::BigInt: {
      TemplatedBinaryOperation<i64, i64, bool, Op>(left, right, result);
      break;
    }
    case TypeId::Hash: {
      TemplatedBinaryOperation<hash_t, hash_t, bool, Op>(left, right, result);
      break;
    }
    case TypeId::Pointer: {
      TemplatedBinaryOperation<uintptr_t, uintptr_t, bool, Op>(left, right,
                                                               result);
      break;
    }
    case TypeId::Float: {
      TemplatedBinaryOperation<f32, f32, bool, Op>(left, right, result);
      break;
    }
    case TypeId::Double: {
      TemplatedBinaryOperation<f64, f64, bool, Op>(left, right, result);
      break;
    }
    case TypeId::Varchar: {
      TemplatedBinaryOperation<const char *, const char *, bool, Op, true>(
          left, right, result);
      break;
    }
    default: { TPL_ASSERT(false, "Type not supported for comparison"); }
  }
}

}  // namespace

void VectorOps::Equal(const Vector &left, const Vector &right, Vector *result) {
  ComparisonOperation<tpl::sql::Equal>(left, right, result);
}

void VectorOps::GreaterThan(const Vector &left, const Vector &right,
                            Vector *result) {
  ComparisonOperation<tpl::sql::GreaterThan>(left, right, result);
}

void VectorOps::GreaterThanEqual(const Vector &left, const Vector &right,
                                 Vector *result) {
  ComparisonOperation<tpl::sql::GreaterThanEqual>(left, right, result);
}

void VectorOps::LessThan(const Vector &left, const Vector &right,
                         Vector *result) {
  ComparisonOperation<tpl::sql::LessThan>(left, right, result);
}

void VectorOps::LessThanEqual(const Vector &left, const Vector &right,
                              Vector *result) {
  ComparisonOperation<tpl::sql::LessThanEqual>(left, right, result);
}

void VectorOps::NotEqual(const Vector &left, const Vector &right,
                         Vector *result) {
  ComparisonOperation<tpl::sql::NotEqual>(left, right, result);
}

}  // namespace tpl::sql
