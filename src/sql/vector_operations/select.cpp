#include "sql/vector_operations/vector_operators.h"

#include "sql/operations/comparison_operators.h"

namespace tpl::sql {

namespace {

template <typename T, typename Op, bool IgnoreNull>
void TemplatedSelectOperation_Vector_Constant(const Vector &left,
                                              const Vector &right,
                                              sel_t *out_sel_vector,
                                              u32 *count) {
  auto *left_data = reinterpret_cast<const T *>(left.data());
  auto *right_data = reinterpret_cast<const T *>(right.data());

  sel_t out_idx = 0;

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

  *count = out_idx;
}

template <typename T, typename Op, bool IgnoreNull>
void TemplatedSelectOperation_Vector_Vector(const Vector &left,
                                            const Vector &right,
                                            sel_t *out_sel_vector, u32 *count) {
  TPL_ASSERT(left.selection_vector() == right.selection_vector(),
             "Vectors must have the same selection vector, or none at all");
  TPL_ASSERT(left.count() == right.count(),
             "Count must be less than input vector size");

  auto *left_data = reinterpret_cast<const T *>(left.data());
  auto *right_data = reinterpret_cast<const T *>(right.data());

  sel_t out_idx = 0;

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

  *count = out_idx;
}

template <typename T, typename Op, bool IgnoreNull = false>
void TemplatedSelectOperation(const Vector &left, const Vector &right,
                              sel_t *out_sel_vector, u32 *count) {
  if (right.IsConstant() && !right.IsNull(0)) {
    TemplatedSelectOperation_Vector_Constant<T, Op, IgnoreNull>(
        left, right, out_sel_vector, count);
  } else if (left.IsConstant() && !left.IsNull(0)) {
    // NOLINTNEXTLINE
    TemplatedSelectOperation<T, typename Op::SymmetricOp, IgnoreNull>(
        right, left, out_sel_vector, count);
  } else {
    TemplatedSelectOperation_Vector_Vector<T, Op, IgnoreNull>(
        left, right, out_sel_vector, count);
  }
}

template <typename Op>
void SelectOperation(const Vector &left, const Vector &right,
                     sel_t *out_sel_vector, u32 *count) {
  switch (left.type_id()) {
    case TypeId::Boolean: {
      TemplatedSelectOperation<bool, Op>(left, right, out_sel_vector, count);
      break;
    }
    case TypeId::TinyInt: {
      TemplatedSelectOperation<i8, Op>(left, right, out_sel_vector, count);
      break;
    }
    case TypeId::SmallInt: {
      TemplatedSelectOperation<i16, Op>(left, right, out_sel_vector, count);
      break;
    }
    case TypeId::Integer: {
      TemplatedSelectOperation<i32, Op>(left, right, out_sel_vector, count);
      break;
    }
    case TypeId::BigInt: {
      TemplatedSelectOperation<i64, Op>(left, right, out_sel_vector, count);
      break;
    }
    case TypeId::Float: {
      TemplatedSelectOperation<f32, Op>(left, right, out_sel_vector, count);
      break;
    }
    case TypeId::Double: {
      TemplatedSelectOperation<f64, Op>(left, right, out_sel_vector, count);
      break;
    }
    case TypeId::Varchar: {
      TemplatedSelectOperation<const char *, Op, true>(left, right,
                                                       out_sel_vector, count);
      break;
    }
    default: { throw std::runtime_error("Type not supported for comparison"); }
  }
}

}  // namespace

void VectorOps::SelectEqual(const Vector &left, const Vector &right,
                            sel_t *out_sel_vector, u32 *out_count) {
  SelectOperation<tpl::sql::Equal>(left, right, out_sel_vector, out_count);
}

void VectorOps::SelectGreaterThan(const Vector &left, const Vector &right,
                                  sel_t *out_sel_vector, u32 *out_count) {
  SelectOperation<tpl::sql::GreaterThan>(left, right, out_sel_vector,
                                         out_count);
}

void VectorOps::SelectGreaterThanEqual(const Vector &left, const Vector &right,
                                       sel_t *out_sel_vector, u32 *out_count) {
  SelectOperation<tpl::sql::GreaterThanEqual>(left, right, out_sel_vector,
                                              out_count);
}

void VectorOps::SelectLessThan(const Vector &left, const Vector &right,
                               sel_t *out_sel_vector, u32 *out_count) {
  SelectOperation<tpl::sql::LessThan>(left, right, out_sel_vector, out_count);
}

void VectorOps::SelectLessThanEqual(const Vector &left, const Vector &right,
                                    sel_t *out_sel_vector, u32 *out_count) {
  SelectOperation<tpl::sql::LessThanEqual>(left, right, out_sel_vector,
                                           out_count);
}

void VectorOps::SelectNotEqual(const Vector &left, const Vector &right,
                               sel_t *out_sel_vector, u32 *out_count) {
  SelectOperation<tpl::sql::NotEqual>(left, right, out_sel_vector, out_count);
}

}  // namespace tpl::sql
