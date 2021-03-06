#include "sql/vector_operations/vector_operations.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "common/settings.h"
#include "sql/operators/hash_operators.h"
#include "sql/vector_operations/traits.h"

namespace tpl::sql {

// TODO(pmenon): Hash operations should re-use the UnaryOperationExecutor.

namespace traits {

template <typename T>
struct IsNumeric {
  static constexpr bool value = std::is_fundamental_v<T> || std::is_same_v<T, Date> ||
                                std::is_same_v<T, Timestamp> || std::is_same_v<T, Decimal32> ||
                                std::is_same_v<T, Decimal64> || std::is_same_v<T, Decimal128>;
};

template <template <typename> typename Op, typename T>
struct IsHashOp {
  static constexpr bool value =
      // Either a vanilla hashing operation.
      std::is_same_v<Op<T>, tpl::sql::Hash<T>> ||
      // Or, a hash-with-seed (i.e., hash combine) operation.
      std::is_same_v<Op<T>, tpl::sql::HashCombine<T>>;
};

template <template <typename> typename Op, typename T>
constexpr bool IsHashOpV = IsHashOp<Op, T>::value;

template <typename T>
constexpr bool IsNumericV = IsNumeric<T>::value;

// Specialize trait struct to enable full hash computation.
template <template <typename> typename Op, typename T>
struct ShouldPerformFullCompute<Op<T>, std::enable_if_t<IsHashOpV<Op, T> && IsNumericV<T>>> {
  bool operator()(const TupleIdList *tid_list) const {
    auto settings = Settings::Instance();
    const auto full_compute_threshold = settings->GetDouble(Settings::Name::FullHashOptThreshold);
    return tid_list == nullptr || full_compute_threshold <= tid_list->ComputeSelectivity();
  }
};

}  // namespace traits

namespace {

void CheckHashArguments(const Vector &input, Vector *result) {
  if (result->GetTypeId() != TypeId::Hash) {
    throw InvalidTypeException(result->GetTypeId(), "Output of Hash() operation must be hash");
  }
}

template <typename InputType>
void TemplatedHashOperation(const Vector &input, Vector *result) {
  // The hashing functor.
  using HashOp = tpl::sql::Hash<InputType>;

  auto *RESTRICT input_data = reinterpret_cast<InputType *>(input.GetData());
  auto *RESTRICT result_data = reinterpret_cast<hash_t *>(result->GetData());

  result->Resize(input.GetSize());
  result->GetMutableNullMask()->Reset();
  result->SetFilteredTupleIdList(input.GetFilteredTupleIdList(), input.GetCount());

  if (const auto &null_mask = input.GetNullMask(); null_mask.Any()) {
    VectorOps::Exec(input, [&](uint64_t i, uint64_t k) {
      result_data[i] = HashOp{}(input_data[i], null_mask[i]);
    });
  } else {
    if (traits::ShouldPerformFullCompute<HashOp>{}(input.GetFilteredTupleIdList())) {
      VectorOps::ExecIgnoreFilter(
          input, [&](uint64_t i, uint64_t k) { result_data[i] = HashOp{}(input_data[i], false); });
    } else {
      VectorOps::Exec(
          input, [&](uint64_t i, uint64_t k) { result_data[i] = HashOp{}(input_data[i], false); });
    }
  }
}

template <typename InputType>
void TemplatedHashCombineOperation(const Vector &input, Vector *result) {
  auto *RESTRICT input_data = reinterpret_cast<InputType *>(input.GetData());
  auto *RESTRICT result_data = reinterpret_cast<hash_t *>(result->GetData());

  result->Resize(input.GetSize());
  result->GetMutableNullMask()->Reset();
  result->SetFilteredTupleIdList(input.GetFilteredTupleIdList(), input.GetCount());

  if (const auto &null_mask = input.GetNullMask(); null_mask.Any()) {
    VectorOps::Exec(input, [&](uint64_t i, uint64_t k) {
      result_data[i] =
          tpl::sql::HashCombine<InputType>{}(input_data[i], null_mask[i], result_data[i]);
    });
  } else {
    VectorOps::Exec(input, [&](uint64_t i, uint64_t k) {
      result_data[i] = tpl::sql::HashCombine<InputType>{}(input_data[i], false, result_data[i]);
    });
  }
}

}  // namespace

void VectorOps::Hash(const Vector &input, Vector *result) {
  // Sanity check
  CheckHashArguments(input, result);

  // Lift-off
  switch (input.GetTypeId()) {
    case TypeId::Boolean:
      TemplatedHashOperation<bool>(input, result);
      break;
    case TypeId::TinyInt:
      TemplatedHashOperation<int8_t>(input, result);
      break;
    case TypeId::SmallInt:
      TemplatedHashOperation<int16_t>(input, result);
      break;
    case TypeId::Integer:
      TemplatedHashOperation<int32_t>(input, result);
      break;
    case TypeId::BigInt:
      TemplatedHashOperation<int64_t>(input, result);
      break;
    case TypeId::Float:
      TemplatedHashOperation<float>(input, result);
      break;
    case TypeId::Double:
      TemplatedHashOperation<double>(input, result);
      break;
    case TypeId::Date:
      TemplatedHashOperation<Date>(input, result);
      break;
    case TypeId::Timestamp:
      TemplatedHashOperation<Timestamp>(input, result);
      break;
    case TypeId::Varchar:
      TemplatedHashOperation<VarlenEntry>(input, result);
      break;
    default:
      throw NotImplementedException(
          fmt::format("hashing vector type '{}'", TypeIdToString(input.GetTypeId())));
  }
}

void VectorOps::HashCombine(const Vector &input, Vector *result) {
  // Sanity check
  CheckHashArguments(input, result);

  // Lift-off
  switch (input.GetTypeId()) {
    case TypeId::Boolean:
      TemplatedHashCombineOperation<bool>(input, result);
      break;
    case TypeId::TinyInt:
      TemplatedHashCombineOperation<int8_t>(input, result);
      break;
    case TypeId::SmallInt:
      TemplatedHashCombineOperation<int16_t>(input, result);
      break;
    case TypeId::Integer:
      TemplatedHashCombineOperation<int32_t>(input, result);
      break;
    case TypeId::BigInt:
      TemplatedHashCombineOperation<int64_t>(input, result);
      break;
    case TypeId::Float:
      TemplatedHashCombineOperation<float>(input, result);
      break;
    case TypeId::Double:
      TemplatedHashCombineOperation<double>(input, result);
      break;
    case TypeId::Date:
      TemplatedHashCombineOperation<Date>(input, result);
      break;
    case TypeId::Timestamp:
      TemplatedHashCombineOperation<Timestamp>(input, result);
      break;
    case TypeId::Varchar:
      TemplatedHashCombineOperation<VarlenEntry>(input, result);
      break;
    default:
      throw NotImplementedException(
          fmt::format("hashing vector type '{}'", TypeIdToString(input.GetTypeId())));
  }
}

}  // namespace tpl::sql
