#include "sql/vector_operations/vector_operators.h"

#include "common/exception.h"
#include "common/settings.h"
#include "sql/operations/comparison_operators.h"
#include "sql/tuple_id_list.h"

namespace tpl::sql {

namespace {

void CheckSelection(const Vector &left, const Vector &right, TupleIdList *result) {
  if (left.type_id() != right.type_id()) {
    throw TypeMismatchException(left.type_id(), right.type_id(),
                                "input vector types must match for selections");
  }
  if (!left.IsConstant() && !right.IsConstant()) {
    if (left.num_elements() != right.num_elements()) {
      throw Exception(ExceptionType::Execution,
                      "left and right vectors to comparison have different sizes");
    }
    if (left.count() != right.count()) {
      throw Exception(ExceptionType::Execution,
                      "left and right vectors to comparison have different counts");
    }
    if (result->GetCapacity() != left.num_elements()) {
      throw Exception(ExceptionType::Execution,
                      "result list not large enough to store all TIDs in input vector");
    }
  }
}

template <typename T, typename Op, bool IgnoreNull>
void TemplatedSelectOperation_Vector_Constant(const Vector &left, const Vector &right,
                                              TupleIdList *tid_list) {
  // If the scalar constant is NULL, all comparisons are NULL.
  if (right.IsNull(0)) {
    tid_list->Clear();
    return;
  }

  auto *left_data = reinterpret_cast<const T *>(left.data());
  auto constant = *reinterpret_cast<const T *>(right.data());

  if constexpr (std::is_fundamental_v<T>) {
    // We're comparing a vector of primitive values to a constant. We COULD just iterate the TIDs in
    // the input TupleIdList, apply the predicate and be done with it, but we take advantage of the
    // fact that we can operate on non-selected data and potentially use SIMD to accelerate the
    // total performance. But, this optimization only makes sense beyond a certain selectivity of
    // the vector. Let's make the decision now.

    auto full_compute_threshold =
        Settings::Instance()->GetDouble(Settings::Name::SelectOptThreshold);

    if (full_compute_threshold && *full_compute_threshold <= tid_list->ComputeSelectivity()) {
      TupleIdList::BitVectorType *bit_vector = tid_list->GetMutableBits();
      bit_vector->UpdateFull([&](uint64_t i) { return Op::Apply(left_data[i], constant); });
      bit_vector->Difference(left.null_mask());
      return;
    }
  }

  // Remove all NULL entries from left input. Right constant is guaranteed non-NULL by this point.
  tid_list->GetMutableBits()->Difference(left.null_mask());

  // Filter
  tid_list->Filter([&](uint64_t i) { return Op::Apply(left_data[i], constant); });
}

template <typename T, typename Op, bool IgnoreNull>
void TemplatedSelectOperation_Vector_Vector(const Vector &left, const Vector &right,
                                            TupleIdList *tid_list) {
  auto *left_data = reinterpret_cast<const T *>(left.data());
  auto *right_data = reinterpret_cast<const T *>(right.data());

  if constexpr (std::is_fundamental_v<T>) {
    // We're comparing a vector of primitive values to a constant. We COULD just iterate the TIDs in
    // the input TupleIdList, apply the predicate and be done with it, but we take advantage of the
    // fact that we can operate on non-selected data and potentially use SIMD to accelerate the
    // total performance. But, this optimization only makes sense beyond a certain selectivity of
    // the vector. Let's make the decision now.

    auto full_compute_threshold =
        Settings::Instance()->GetDouble(Settings::Name::SelectOptThreshold);

    if (full_compute_threshold && *full_compute_threshold <= tid_list->ComputeSelectivity()) {
      TupleIdList::BitVectorType *bit_vector = tid_list->GetMutableBits();
      bit_vector->UpdateFull([&](uint64_t i) { return Op::Apply(left_data[i], right_data[i]); });
      bit_vector->Difference(left.null_mask()).Difference(right.null_mask());
      return;
    }
  }

  // Remove all NULL entries in either vector
  tid_list->GetMutableBits()->Difference(left.null_mask()).Difference(right.null_mask());

  // Filter
  tid_list->Filter([&](uint64_t i) { return Op::Apply(left_data[i], right_data[i]); });
}

template <typename T, typename Op, bool IgnoreNull = false>
void TemplatedSelectOperation(const Vector &left, const Vector &right, TupleIdList *tid_list) {
  if (right.IsConstant()) {
    TemplatedSelectOperation_Vector_Constant<T, Op, IgnoreNull>(left, right, tid_list);
  } else if (left.IsConstant()) {
    // NOLINTNEXTLINE
    TemplatedSelectOperation<T, typename Op::SymmetricOp, IgnoreNull>(right, left, tid_list);
  } else {
    TemplatedSelectOperation_Vector_Vector<T, Op, IgnoreNull>(left, right, tid_list);
  }
}

template <typename Op>
void SelectOperation(const Vector &left, const Vector &right, TupleIdList *tid_list) {
  // Sanity check
  CheckSelection(left, right, tid_list);

  // Lift-off
  switch (left.type_id()) {
    case TypeId::Boolean:
      TemplatedSelectOperation<bool, Op>(left, right, tid_list);
      break;
    case TypeId::TinyInt:
      TemplatedSelectOperation<int8_t, Op>(left, right, tid_list);
      break;
    case TypeId::SmallInt:
      TemplatedSelectOperation<int16_t, Op>(left, right, tid_list);
      break;
    case TypeId::Integer:
      TemplatedSelectOperation<int32_t, Op>(left, right, tid_list);
      break;
    case TypeId::BigInt:
      TemplatedSelectOperation<int64_t, Op>(left, right, tid_list);
      break;
    case TypeId::Float:
      TemplatedSelectOperation<float, Op>(left, right, tid_list);
      break;
    case TypeId::Double:
      TemplatedSelectOperation<double, Op>(left, right, tid_list);
      break;
    case TypeId::Varchar:
      TemplatedSelectOperation<const char *, Op, true>(left, right, tid_list);
      break;
    default:
      throw NotImplementedException("selections on vector type '{}' not supported",
                                    TypeIdToString(left.type_id()));
  }
}

}  // namespace

void VectorOps::SelectEqual(const Vector &left, const Vector &right, TupleIdList *tid_list) {
  SelectOperation<tpl::sql::Equal>(left, right, tid_list);
}

void VectorOps::SelectGreaterThan(const Vector &left, const Vector &right, TupleIdList *tid_list) {
  SelectOperation<tpl::sql::GreaterThan>(left, right, tid_list);
}

void VectorOps::SelectGreaterThanEqual(const Vector &left, const Vector &right,
                                       TupleIdList *tid_list) {
  SelectOperation<tpl::sql::GreaterThanEqual>(left, right, tid_list);
}

void VectorOps::SelectLessThan(const Vector &left, const Vector &right, TupleIdList *tid_list) {
  SelectOperation<tpl::sql::LessThan>(left, right, tid_list);
}

void VectorOps::SelectLessThanEqual(const Vector &left, const Vector &right,
                                    TupleIdList *tid_list) {
  SelectOperation<tpl::sql::LessThanEqual>(left, right, tid_list);
}

void VectorOps::SelectNotEqual(const Vector &left, const Vector &right, TupleIdList *tid_list) {
  SelectOperation<tpl::sql::NotEqual>(left, right, tid_list);
}

}  // namespace tpl::sql
