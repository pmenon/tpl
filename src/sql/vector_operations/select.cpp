#include "sql/vector_operations/vector_operators.h"

#include "sql/operations/comparison_operators.h"

namespace tpl::sql {

namespace {

template <typename T, typename Op, bool IgnoreNull>
uint32_t TemplatedSelectOperation_Vector_Constant(const Vector &left, const Vector &right,
                                                  sel_t out_sel_vector[]) {
  auto *left_data = reinterpret_cast<const T *>(left.data());
  auto *right_data = reinterpret_cast<const T *>(right.data());

  sel_t out_idx = 0;

  const auto &left_nulls = left.null_mask();
  const auto left_has_nulls = left_nulls.Any();
  if (IgnoreNull && left_has_nulls) {
    // Slow-path: manually skip NULLs
    VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
      if (!left_nulls[i]) {
        out_sel_vector[out_idx] = i;
        out_idx += Op::Apply(left_data[i], right_data[0]);
      }
    });
  } else if (left_has_nulls) {
    // Slow-path: evaluate NULLs and condition
    VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
      out_sel_vector[out_idx] = i;
      out_idx += !left_nulls[i] && Op::Apply(left_data[i], right_data[0]);
    });
  } else {
    // Fast-path: no NULLs
    VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
      out_sel_vector[out_idx] = i;
      out_idx += Op::Apply(left_data[i], right_data[0]);
    });
  }

  return out_idx;
}

template <typename T, typename Op, bool IgnoreNull>
uint32_t TemplatedSelectOperation_Vector_Vector(const Vector &left, const Vector &right,
                                                sel_t out_sel_vector[]) {
  TPL_ASSERT(left.selection_vector() == right.selection_vector(),
             "Vectors must have the same selection vector, or none at all");
  TPL_ASSERT(left.count() == right.count(), "Count must be less than input vector size");

  auto *left_data = reinterpret_cast<const T *>(left.data());
  auto *right_data = reinterpret_cast<const T *>(right.data());

  sel_t out_idx = 0;

  const Vector::NullMask result_mask = left.null_mask() | right.null_mask();
  const bool has_nulls = result_mask.Any();

  if (IgnoreNull && has_nulls) {
    // Slow-path: manually skip NULLs
    VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
      if (!result_mask[i]) {
        out_sel_vector[out_idx] = i;
        out_idx += Op::Apply(left_data[i], right_data[i]);
      }
    });
  } else if (has_nulls) {
    // Slow-path: evaluate NULLs and condition
    VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
      out_sel_vector[out_idx] = i;
      out_idx += !result_mask[i] && Op::Apply(left_data[i], right_data[i]);
    });
  } else {
    // Fast-path: no NULLs
    VectorOps::Exec(left, [&](uint64_t i, uint64_t k) {
      out_sel_vector[out_idx] = i;
      out_idx += Op::Apply(left_data[i], right_data[i]);
    });
  }

  return out_idx;
}

template <typename T, typename Op, bool IgnoreNull = false>
uint32_t TemplatedSelectOperation(const Vector &left, const Vector &right, sel_t out_sel_vector[]) {
  if (right.IsConstant() && !right.IsNull(0)) {
    return TemplatedSelectOperation_Vector_Constant<T, Op, IgnoreNull>(left, right, out_sel_vector);
  } else if (left.IsConstant() && !left.IsNull(0)) {
    // NOLINTNEXTLINE
    return TemplatedSelectOperation<T, typename Op::SymmetricOp, IgnoreNull>(right, left,
                                                                             out_sel_vector);
  } else {
    return TemplatedSelectOperation_Vector_Vector<T, Op, IgnoreNull>(left, right, out_sel_vector);
  }
}

template <typename Op>
uint32_t SelectOperation(const Vector &left, const Vector &right, sel_t out_sel_vector[]) {
  TPL_ASSERT(left.type_id() == right.type_id(), "Mismatched vector inputs to selection");
  switch (left.type_id()) {
    case TypeId::Boolean:
      return TemplatedSelectOperation<bool, Op>(left, right, out_sel_vector);
    case TypeId::TinyInt:
      return TemplatedSelectOperation<int8_t, Op>(left, right, out_sel_vector);
    case TypeId::SmallInt:
      return TemplatedSelectOperation<int16_t, Op>(left, right, out_sel_vector);
    case TypeId::Integer:
      return TemplatedSelectOperation<int32_t, Op>(left, right, out_sel_vector);
    case TypeId::BigInt:
      return TemplatedSelectOperation<int64_t, Op>(left, right, out_sel_vector);
    case TypeId::Float:
      return TemplatedSelectOperation<float, Op>(left, right, out_sel_vector);
    case TypeId::Double:
      return TemplatedSelectOperation<double, Op>(left, right, out_sel_vector);
    case TypeId::Varchar:
      return TemplatedSelectOperation<const char *, Op, true>(left, right, out_sel_vector);
    default: { throw std::runtime_error("Type not supported for selection"); }
  }
}

}  // namespace

uint32_t VectorOps::SelectEqual(const Vector &left, const Vector &right, sel_t out_sel_vector[]) {
  return SelectOperation<tpl::sql::Equal>(left, right, out_sel_vector);
}

uint32_t VectorOps::SelectGreaterThan(const Vector &left, const Vector &right,
                                      sel_t out_sel_vector[]) {
  return SelectOperation<tpl::sql::GreaterThan>(left, right, out_sel_vector);
}

uint32_t VectorOps::SelectGreaterThanEqual(const Vector &left, const Vector &right,
                                           sel_t out_sel_vector[]) {
  return SelectOperation<tpl::sql::GreaterThanEqual>(left, right, out_sel_vector);
}

uint32_t VectorOps::SelectLessThan(const Vector &left, const Vector &right,
                                   sel_t out_sel_vector[]) {
  return SelectOperation<tpl::sql::LessThan>(left, right, out_sel_vector);
}

uint32_t VectorOps::SelectLessThanEqual(const Vector &left, const Vector &right,
                                        sel_t out_sel_vector[]) {
  return SelectOperation<tpl::sql::LessThanEqual>(left, right, out_sel_vector);
}

uint32_t VectorOps::SelectNotEqual(const Vector &left, const Vector &right,
                                   sel_t out_sel_vector[]) {
  return SelectOperation<tpl::sql::NotEqual>(left, right, out_sel_vector);
}

}  // namespace tpl::sql
