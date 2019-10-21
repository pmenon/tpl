#include "sql/vector_operations/vector_operators.h"

#include "common/exception.h"

namespace tpl::sql {

namespace {

void CheckGatherArguments(const Vector &pointers, UNUSED Vector *result) {
  if (pointers.GetTypeId() != TypeId::Pointer) {
    throw InvalidTypeException(pointers.GetTypeId(), "Gather only works on pointer inputs");
  }
}

template <typename T>
void GatherOperation(const Vector &pointers, Vector *result, const std::size_t offset) {
  T *result_data = reinterpret_cast<T *>(result->GetData());
  if (const auto &null_mask = pointers.GetNullMask(); null_mask.Any()) {
    VectorOps::ExecTyped<byte *>(pointers, [&](byte *ptr, uint64_t i, uint64_t k) {
      result_data[i] = null_mask[i] ? T{} : *reinterpret_cast<T *>(ptr + offset);
    });
  } else {
    VectorOps::ExecTyped<byte *>(pointers, [&](byte *ptr, uint64_t i, uint64_t k) {
      result_data[i] = *reinterpret_cast<T *>(ptr + offset);
    });
  }
}

}  // namespace

void VectorOps::Gather(const Vector &pointers, Vector *result, const std::size_t offset) {
  // Sanity check
  CheckGatherArguments(pointers, result);

  // Lift-off
  switch (result->GetTypeId()) {
    case TypeId::Boolean:
      GatherOperation<bool>(pointers, result, offset);
      break;
    case TypeId::TinyInt:
      GatherOperation<int8_t>(pointers, result, offset);
      break;
    case TypeId::SmallInt:
      GatherOperation<int16_t>(pointers, result, offset);
      break;
    case TypeId::Integer:
      GatherOperation<int32_t>(pointers, result, offset);
      break;
    case TypeId::BigInt:
      GatherOperation<int64_t>(pointers, result, offset);
      break;
    case TypeId::Float:
      GatherOperation<float>(pointers, result, offset);
      break;
    case TypeId::Double:
      GatherOperation<double>(pointers, result, offset);
      break;
    case TypeId::Date:
      GatherOperation<Date>(pointers, result, offset);
      break;
    case TypeId::Varchar:
      GatherOperation<VarlenEntry>(pointers, result, offset);
      break;
    default:
      throw NotImplementedException("Gathering '{}' types not supported",
                                    TypeIdToString(result->GetTypeId()));
  }
}

}  // namespace tpl::sql
