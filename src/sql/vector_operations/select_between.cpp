#include "sql/vector_operations/vector_operations.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "common/settings.h"
#include "sql/operators/comparison_operators.h"
#include "sql/runtime_types.h"
#include "sql/tuple_id_list.h"
#include "sql/vector_operations/ternary_operation_executor.h"
#include "sql/vector_operations/traits.h"

namespace tpl::sql {

namespace traits {

template <template <typename> typename Op, typename T>
struct IsBetweenOp {
  static constexpr bool value = std::is_same_v<Op<T>, tpl::sql::InclusiveBetweenOperator<T>> ||
                                std::is_same_v<Op<T>, tpl::sql::LowerInclusiveBetweenOperator<T>> ||
                                std::is_same_v<Op<T>, tpl::sql::UpperInclusiveBetweenOperator<T>> ||
                                std::is_same_v<Op<T>, tpl::sql::ExclusiveBetweenOperator<T>>;
};

template <typename T>
struct IsNumeric {
  static constexpr bool value = std::is_fundamental_v<T> || std::is_same_v<T, Date> ||
                                std::is_same_v<T, Timestamp> || std::is_same_v<T, Decimal32> ||
                                std::is_same_v<T, Decimal64> || std::is_same_v<T, Decimal128>;
};

template <template <typename> typename Op, typename T>
constexpr bool IsBetweenOpV = IsBetweenOp<Op, T>::value;

template <typename T>
constexpr bool IsNumericV = IsNumeric<T>::value;

// Specialized struct to enable full-computation.
template <template <typename> typename Op, typename T>
struct ShouldPerformFullCompute<Op<T>, std::enable_if_t<IsBetweenOpV<Op, T> && IsNumericV<T>>> {
  bool operator()(const TupleIdList *tid_list) const {
    auto settings = Settings::Instance();
    auto full_compute_threshold = settings->GetDouble(Settings::Name::FullSelectOptThreshold);
    return tid_list == nullptr || full_compute_threshold <= tid_list->ComputeSelectivity();
  }
};

}  // namespace traits

namespace {

void CheckSelection(const Vector &input, const Vector &lower, const Vector &upper) {
  if (input.GetTypeId() != lower.GetTypeId()) {
    throw TypeMismatchException(input.GetTypeId(), lower.GetTypeId(),
                                "input vector types must match for selections");
  }
  if (input.GetTypeId() != upper.GetTypeId()) {
    throw TypeMismatchException(input.GetTypeId(), upper.GetTypeId(),
                                "input vector types must match for selections");
  }
}

template <typename T, template <typename> typename Op>
void TemplatedSelectBetweenOperation(const Vector &input, const Vector &lower, const Vector &upper,
                                     TupleIdList *tid_list) {
  TernaryOperationExecutor::Select<T, T, T, Op<T>>(input, lower, upper, tid_list);
}

template <template <typename> typename Op>
void SelectBetweenOperation(const Vector &input, const Vector &lower, const Vector &upper,
                            TupleIdList *tid_list) {
  // Sanity check
  CheckSelection(input, lower, upper);

  // Lift-off
  switch (input.GetTypeId()) {
    case TypeId::Boolean:
      TemplatedSelectBetweenOperation<bool, Op>(input, lower, upper, tid_list);
      break;
    case TypeId::TinyInt:
      TemplatedSelectBetweenOperation<int8_t, Op>(input, lower, upper, tid_list);
      break;
    case TypeId::SmallInt:
      TemplatedSelectBetweenOperation<int16_t, Op>(input, lower, upper, tid_list);
      break;
    case TypeId::Integer:
      TemplatedSelectBetweenOperation<int32_t, Op>(input, lower, upper, tid_list);
      break;
    case TypeId::BigInt:
      TemplatedSelectBetweenOperation<int64_t, Op>(input, lower, upper, tid_list);
      break;
    case TypeId::Hash:
      TemplatedSelectBetweenOperation<hash_t, Op>(input, lower, upper, tid_list);
      break;
    case TypeId::Pointer:
      TemplatedSelectBetweenOperation<uintptr_t, Op>(input, lower, upper, tid_list);
      break;
    case TypeId::Float:
      TemplatedSelectBetweenOperation<float, Op>(input, lower, upper, tid_list);
      break;
    case TypeId::Double:
      TemplatedSelectBetweenOperation<double, Op>(input, lower, upper, tid_list);
      break;
    case TypeId::Date:
      TemplatedSelectBetweenOperation<Date, Op>(input, lower, upper, tid_list);
      break;
    case TypeId::Timestamp:
      TemplatedSelectBetweenOperation<Timestamp, Op>(input, lower, upper, tid_list);
      break;
    case TypeId::Varchar:
      TemplatedSelectBetweenOperation<VarlenEntry, Op>(input, lower, upper, tid_list);
      break;
    default:
      throw NotImplementedException(
          fmt::format("between-selections on vector type '{}' not supported",
                      TypeIdToString(input.GetTypeId())));
  }
}

}  // namespace

void VectorOps::SelectBetween(const Vector &input, const Vector &lower, const Vector &upper,
                              bool lower_inclusive, bool upper_inclusive, TupleIdList *tid_list) {
  if (lower_inclusive && upper_inclusive) {
    SelectBetweenOperation<tpl::sql::InclusiveBetweenOperator>(input, lower, upper, tid_list);
  } else if (lower_inclusive) {
    SelectBetweenOperation<tpl::sql::LowerInclusiveBetweenOperator>(input, lower, upper, tid_list);
  } else if (upper_inclusive) {
    SelectBetweenOperation<tpl::sql::UpperInclusiveBetweenOperator>(input, lower, upper, tid_list);
  } else {
    SelectBetweenOperation<tpl::sql::ExclusiveBetweenOperator>(input, lower, upper, tid_list);
  }
}

}  // namespace tpl::sql
