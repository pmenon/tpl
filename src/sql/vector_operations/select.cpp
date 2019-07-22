#include "sql/vector_operations/vector_operators.h"

#include "sql/operations/comparison_operators.h"

namespace tpl::sql {

namespace {

template <typename T, typename Op, bool IgnoreNull = false>
u32 TemplatedSelectOperation(const Vector &left, const Vector &right,
                             sel_t *RESTRICT out_sel_vector) {
  auto *left_data = reinterpret_cast<const T *>(left.data());
  auto *right_data = reinterpret_cast<const T *>(right.data());

  sel_t out_idx = 0;

  if (right.IsConstant() && !right.IsNull(0)) {
    // Right is a non-null constant, need to do some work.
    if (IgnoreNull || left.null_mask().any()) {
      // Slow-path: left vector has NULLs that need to be checked.
      VectorOps::Exec(left, [&](u64 i, u64 k) {
        if (!left.null_mask()[i]) {
          out_sel_vector[out_idx] = i;
          out_idx += static_cast<u32>(Op::Apply(left_data[i], right_data[0]));
        }
      });
    } else {
      // Fast-path: left vector has no NULLs.
      VectorOps::Exec(left, [&](u64 i, u64 k) {
        out_sel_vector[out_idx] = i;
        out_idx += static_cast<u32>(Op::Apply(left_data[i], right_data[0]));
      });
    }
  } else if (left.IsConstant()) {
    return TemplatedSelectOperation<T, typename Op::SymmetricOp,  // NOLINT
                                    IgnoreNull>(right, left, out_sel_vector);
  } else {
    // Vector-vector selection. The selection vector has to be a subset of the
    // selections in both vectors.
    TPL_ASSERT(left.selection_vector() == right.selection_vector(),
               "Vectors must have the same selection vector, or none at all");
    TPL_ASSERT(left.count() == right.count(),
               "Count must be less than input vector size");
    if (IgnoreNull || left.null_mask().any() || right.null_mask().any()) {
      // Left or right inputs have some NULLs, need to check.
      Vector::NullMask result_mask = left.null_mask() | right.null_mask();
      VectorOps::Exec(left, [&](u64 i, u64 k) {
        if (!result_mask[i]) {
          out_sel_vector[out_idx] = i;
          out_idx += static_cast<u32>(Op::Apply(left_data[i], right_data[i]));
        }
      });
    } else {
      // Fast-path: neither input has NULLs.
      VectorOps::Exec(left, [&](u64 i, u64 k) {
        out_sel_vector[out_idx] = i;
        out_idx += static_cast<u32>(Op::Apply(left_data[i], right_data[i]));
      });
    }
  }

  return out_idx;
}

template <typename Op>
u32 SelectOperation(const Vector &left, const Vector &right,
                    sel_t out_sel_vector[]) {
  switch (left.type_id()) {
    case TypeId::Boolean:
      return TemplatedSelectOperation<bool, Op>(left, right, out_sel_vector);
    case TypeId::TinyInt:
      return TemplatedSelectOperation<i8, Op>(left, right, out_sel_vector);
    case TypeId::SmallInt:
      return TemplatedSelectOperation<i16, Op>(left, right, out_sel_vector);
    case TypeId::Integer:
      return TemplatedSelectOperation<i32, Op>(left, right, out_sel_vector);
    case TypeId::BigInt:
      return TemplatedSelectOperation<i64, Op>(left, right, out_sel_vector);
    case TypeId::Hash:
      return TemplatedSelectOperation<hash_t, Op>(left, right, out_sel_vector);
    case TypeId::Pointer:
      return TemplatedSelectOperation<uintptr_t, Op>(left, right,
                                                     out_sel_vector);
    case TypeId::Float:
      return TemplatedSelectOperation<f32, Op>(left, right, out_sel_vector);
    case TypeId::Double:
      return TemplatedSelectOperation<f64, Op>(left, right, out_sel_vector);
    case TypeId::Varchar:
      return TemplatedSelectOperation<const char *, Op, true>(left, right,
                                                              out_sel_vector);
    default:
      throw std::runtime_error("Type not supported for comparison");
  }
}

}  // namespace

u32 VectorOps::SelectEqual(const Vector &left, const Vector &right,
                           sel_t out_sel_vector[]) {
  return SelectOperation<tpl::sql::Equal>(left, right, out_sel_vector);
}

u32 VectorOps::SelectGreaterThan(const Vector &left, const Vector &right,
                                 sel_t out_sel_vector[]) {
  return SelectOperation<tpl::sql::GreaterThan>(left, right, out_sel_vector);
}

u32 VectorOps::SelectGreaterThanEqual(const Vector &left, const Vector &right,
                                      sel_t out_sel_vector[]) {
  return SelectOperation<tpl::sql::GreaterThanEqual>(left, right,
                                                     out_sel_vector);
}

u32 VectorOps::SelectLessThan(const Vector &left, const Vector &right,
                              sel_t out_sel_vector[]) {
  return SelectOperation<tpl::sql::LessThan>(left, right, out_sel_vector);
}

u32 VectorOps::SelectLessThanEqual(const Vector &left, const Vector &right,
                                   sel_t out_sel_vector[]) {
  return SelectOperation<tpl::sql::LessThanEqual>(left, right, out_sel_vector);
}

u32 VectorOps::SelectNotEqual(const Vector &left, const Vector &right,
                              sel_t out_sel_vector[]) {
  return SelectOperation<tpl::sql::NotEqual>(left, right, out_sel_vector);
}

}  // namespace tpl::sql
