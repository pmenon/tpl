#include "sql/vector_operations/vector_operations.h"

#include "ips4o/ips4o.hpp"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"
#include "sql/operators/comparison_operators.h"
#include "util/bit_vector.h"

namespace tpl::sql {

namespace {

template <typename T>
void TemplatedSort(const Vector &input, const TupleIdList &non_null_selections, sel_t result[]) {
  const auto *data = reinterpret_cast<const T *>(input.GetData());
  const auto size = non_null_selections.ToSelectionVector(result);
  ips4o::sort(result, result + size,
              [&](auto idx1, auto idx2) { return LessThanEqual<T>{}(data[idx1], data[idx2]); });
}

}  // namespace

void VectorOps::Sort(const Vector &input, sel_t result[]) {
  TupleIdList non_nulls(kDefaultVectorSize), nulls(kDefaultVectorSize);
  input.GetNonNullSelections(&non_nulls, &nulls);

  // Write NULLs indexes first
  auto num_nulls = nulls.ToSelectionVector(result);

  // Sort non-NULL elements now
  auto non_null_result = result + num_nulls;
  switch (input.GetTypeId()) {
    case TypeId::Boolean:
      TemplatedSort<bool>(input, non_nulls, non_null_result);
      break;
    case TypeId::TinyInt:
      TemplatedSort<int8_t>(input, non_nulls, non_null_result);
      break;
    case TypeId::SmallInt:
      TemplatedSort<int16_t>(input, non_nulls, non_null_result);
      break;
    case TypeId::Integer:
      TemplatedSort<int32_t>(input, non_nulls, non_null_result);
      break;
    case TypeId::BigInt:
      TemplatedSort<int64_t>(input, non_nulls, non_null_result);
      break;
    case TypeId::Hash:
      TemplatedSort<hash_t>(input, non_nulls, non_null_result);
      break;
    case TypeId::Pointer:
      TemplatedSort<uintptr_t>(input, non_nulls, non_null_result);
      break;
    case TypeId::Float:
      TemplatedSort<float>(input, non_nulls, non_null_result);
      break;
    case TypeId::Double:
      TemplatedSort<double>(input, non_nulls, non_null_result);
      break;
    case TypeId::Date:
      TemplatedSort<Date>(input, non_nulls, non_null_result);
      break;
    case TypeId::Timestamp:
      TemplatedSort<Timestamp>(input, non_nulls, non_null_result);
      break;
    case TypeId::Varchar:
      TemplatedSort<VarlenEntry>(input, non_nulls, non_null_result);
      break;
    default:
      throw NotImplementedException(
          fmt::format("cannot sort vector of type {}", TypeIdToString(input.GetTypeId())));
  }
}

}  // namespace tpl::sql
