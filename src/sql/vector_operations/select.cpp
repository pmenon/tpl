#include "sql/vector_operations/vector_operators.h"

#include "common/exception.h"
#include "common/settings.h"
#include "sql/operations/comparison_operators.h"
#include "sql/runtime_types.h"
#include "sql/tuple_id_list.h"

namespace tpl::sql {

namespace {

// Filter optimization:
// --------------------
// When comparison two vectors (or a vector with a constant) of primitive values we __COULD__ just
// iterate the input TID list, apply the predicate, update the list, and be done with it. But, we
// take advantage of the fact that we can operate on unselected data and potentially use SIMD to
// accelerate the total performance. But, this optimization only makes sense beyond a certain
// selectivity of the vector. Let's make the decision now.
//
// To determine scenarios when we can apply such an optimization, we leverage the below
// is_safe_for_full_compute trait to find types for which we can use SIMD for. Right now, all
// fundamental types along with Dates and Timestamps can use this technique.

template <typename T, typename Enable = void>
struct is_safe_for_full_compute {
  static constexpr bool value = false;
};

template <typename T>
struct is_safe_for_full_compute<
    T, std::enable_if_t<std::is_fundamental_v<T> || std::is_same_v<T, Date>>> {
  static constexpr bool value = true;
};

// When performing a selection between two vectors, we need to make sure of a few things:
// 1. The types of the two vectors are the same
// 2. If both input vectors are not constants
//   2a. The size of the vectors are the same
//   2b. The selection counts (i.e., the number of "active" or "visible" elements is equal)
// 3. The output TID list is sufficiently large to represents all TIDs in both left and right inputs
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

template <typename T, typename Op>
void TemplatedSelectOperation_Vector_Constant(const Vector &left, const Vector &right,
                                              TupleIdList *tid_list) {
  // If the scalar constant is NULL, all comparisons are NULL.
  if (right.IsNull(0)) {
    tid_list->Clear();
    return;
  }

  auto *left_data = reinterpret_cast<const T *>(left.data());
  auto &constant = *reinterpret_cast<const T *>(right.data());

  // Safe full-compute. Refer to comment at start of file for explanation.
  if constexpr (is_safe_for_full_compute<T>::value) {
    const auto full_compute_threshold =
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

template <typename T, typename Op>
void TemplatedSelectOperation_Vector_Vector(const Vector &left, const Vector &right,
                                            TupleIdList *tid_list) {
  auto *left_data = reinterpret_cast<const T *>(left.data());
  auto *right_data = reinterpret_cast<const T *>(right.data());

  // Safe full-compute. Refer to comment at start of file for explanation.
  if constexpr (is_safe_for_full_compute<T>::value) {
    const auto full_compute_threshold =
        Settings::Instance()->GetDouble(Settings::Name::SelectOptThreshold);

    // Only perform the full compute if the TID selectivity is larger than the threshold
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

template <typename T, template <typename> typename Op>
void TemplatedSelectOperation(const Vector &left, const Vector &right, TupleIdList *tid_list) {
  if (right.IsConstant()) {
    TemplatedSelectOperation_Vector_Constant<T, Op<T>>(left, right, tid_list);
  } else if (left.IsConstant()) {
    // NOLINTNEXTLINE re-arrange arguments
    TemplatedSelectOperation_Vector_Constant<T, typename Op<T>::SymmetricOp>(right, left, tid_list);
  } else {
    TemplatedSelectOperation_Vector_Vector<T, Op<T>>(left, right, tid_list);
  }
}

template <template <typename> typename Op>
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
    case TypeId::Date:
      TemplatedSelectOperation<Date, Op>(left, right, tid_list);
      break;
    case TypeId::Varchar:
      TemplatedSelectOperation<VarlenEntry, Op>(left, right, tid_list);
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
