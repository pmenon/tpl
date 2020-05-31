#include "sql/vector_operations/vector_operations.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "common/settings.h"
#include "sql/operators/comparison_operators.h"
#include "sql/runtime_types.h"
#include "sql/tuple_id_list.h"
#include "sql/vector_operations/binary_operation_executor.h"
#include "sql/vector_operations/traits.h"

namespace tpl::sql {

namespace traits {

// Filter optimization:
// --------------------
// When perform a comparison between two vectors, we take advantage of the fact
// that, for some data types, we can operate on unselected data and potentially
// leverage SIMD to accelerate performance. This only works for simple types
// like integers because unselected data can safely participate in comparisons
// and get masked out later. This is NOT true for complex types like strings
// which may have NULLs or other garbage. This "full-compute" optimization is
// only beneficial for a range of selectivities that depend on the input
// vector's data type.
//
// Full-compute is enabled through the traits::ShouldPerformFullCompute trait.

template <template <typename> typename Op, typename T>
struct IsComparisonOp {
  static constexpr bool value = std::is_same_v<Op<T>, tpl::sql::Equal<T>> ||
                                std::is_same_v<Op<T>, tpl::sql::GreaterThan<T>> ||
                                std::is_same_v<Op<T>, tpl::sql::GreaterThanEqual<T>> ||
                                std::is_same_v<Op<T>, tpl::sql::LessThan<T>> ||
                                std::is_same_v<Op<T>, tpl::sql::LessThanEqual<T>> ||
                                std::is_same_v<Op<T>, tpl::sql::NotEqual<T>>;
};

template <typename T>
struct IsNumeric {
  static constexpr bool value = std::is_fundamental_v<T> || std::is_same_v<T, Date> ||
                                std::is_same_v<T, Timestamp> || std::is_same_v<T, Decimal32> ||
                                std::is_same_v<T, Decimal64> || std::is_same_v<T, Decimal128>;
};

template <template <typename> typename Op, typename T>
constexpr bool IsComparisonOpV = IsComparisonOp<Op, T>::value;

template <typename T>
constexpr bool IsNumericV = IsNumeric<T>::value;

// Specialized struct to enable full-computation.
template <template <typename> typename Op, typename T>
struct ShouldPerformFullCompute<Op<T>, std::enable_if_t<IsComparisonOpV<Op, T> && IsNumericV<T>>> {
  bool operator()(const TupleIdList *tid_list) const {
    auto settings = Settings::Instance();
    auto full_compute_threshold = settings->GetDouble(Settings::Name::SelectOptThreshold);
    return tid_list == nullptr || full_compute_threshold <= tid_list->ComputeSelectivity();
  }
};

}  // namespace traits

namespace {

// When performing a selection between two vectors, we need to make sure of a few things:
// 1. The types of the two vectors are the same
// 2. If both input vectors are not constants
//   2a. The size of the vectors are the same
//   2b. The selection counts (i.e., the number of "active" or "visible" elements is equal)
// 3. The output TID list is sufficiently large to represents all TIDs in both left and right inputs
void CheckSelection(const Vector &left, const Vector &right, TupleIdList *result) {
  if (left.GetTypeId() != right.GetTypeId()) {
    throw TypeMismatchException(left.GetTypeId(), right.GetTypeId(),
                                "input vector types must match for selections");
  }
  if (!left.IsConstant() && !right.IsConstant()) {
    if (left.GetSize() != right.GetSize()) {
      throw Exception(ExceptionType::Execution,
                      "left and right vectors to comparison have different sizes");
    }
    if (left.GetCount() != right.GetCount()) {
      throw Exception(ExceptionType::Execution,
                      "left and right vectors to comparison have different counts");
    }
    if (result->GetCapacity() != left.GetSize()) {
      throw Exception(ExceptionType::Execution,
                      "result list not large enough to store all TIDs in input vector");
    }
  }
}

template <typename T, template <typename> typename Op>
void TemplatedSelectOperation(const Vector &left, const Vector &right, TupleIdList *tid_list) {
  BinaryOperationExecutor::Select<T, T, Op<T>>(left, right, tid_list);
}

template <template <typename> typename Op>
void SelectOperation(const Vector &left, const Vector &right, TupleIdList *tid_list) {
  // Sanity check
  CheckSelection(left, right, tid_list);

  // Lift-off
  switch (left.GetTypeId()) {
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
    case TypeId::Hash:
      TemplatedSelectOperation<hash_t, Op>(left, right, tid_list);
      break;
    case TypeId::Pointer:
      TemplatedSelectOperation<uintptr_t, Op>(left, right, tid_list);
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
    case TypeId::Timestamp:
      TemplatedSelectOperation<Timestamp, Op>(left, right, tid_list);
      break;
    case TypeId::Varchar:
      TemplatedSelectOperation<VarlenEntry, Op>(left, right, tid_list);
      break;
    default:
      throw NotImplementedException(fmt::format("selections on vector type '{}' not supported",
                                                TypeIdToString(left.GetTypeId())));
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
