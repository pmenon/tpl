#include "sql/vector_operations/vector_operations.h"

#include "spdlog/fmt/fmt.h"

#include "common/exception.h"

namespace tpl::sql {

namespace {

template <typename T>
void TemplatedCopyOperation(const Vector &source, Vector *target) {
  auto *RESTRICT src_data = reinterpret_cast<T *>(source.GetData());
  auto *RESTRICT target_data = reinterpret_cast<T *>(target->GetData());
  VectorOps::Exec(source, [&](uint64_t i, uint64_t k) { target_data[k] = src_data[i]; });
}

void StringCopyOperation(const Vector &source, Vector *target) {
  TPL_ASSERT(source.GetTypeId() == TypeId::Varchar, "Expected string vector type for COPY()");
  TPL_ASSERT(target->GetTypeId() == TypeId::Varchar, "Expected string vector type for COPY()");

  auto *RESTRICT src_data = reinterpret_cast<const VarlenEntry *>(source.GetData());
  auto *RESTRICT target_data = reinterpret_cast<VarlenEntry *>(target->GetData());
  VectorOps::Exec(source, [&](uint64_t i, uint64_t k) {
    if (!source.GetNullMask()[i]) {
      target_data[k] = target->GetMutableStringHeap()->AddVarlen(src_data[i]);
    }
  });
}

}  // namespace

void VectorOps::Copy(const Vector &source, Vector *target) {
  // If nothing is selected, there is nothing to copy.
  if (source.GetCount() == 0) return;

  // Resize the target to the count of the source.
  target->Resize(source.GetCount());
  // Copy NULLs.
  Exec(source, [&](uint64_t i, uint64_t k) { target->null_mask_[k] = source.null_mask_[i]; });
  // Copy data.
  switch (source.GetTypeId()) {
    case TypeId::Boolean:
      TemplatedCopyOperation<bool>(source, target);
      break;
    case TypeId::TinyInt:
      TemplatedCopyOperation<int8_t>(source, target);
      break;
    case TypeId::SmallInt:
      TemplatedCopyOperation<int16_t>(source, target);
      break;
    case TypeId::Integer:
      TemplatedCopyOperation<int32_t>(source, target);
      break;
    case TypeId::BigInt:
      TemplatedCopyOperation<int64_t>(source, target);
      break;
    case TypeId::Hash:
      TemplatedCopyOperation<hash_t>(source, target);
      break;
    case TypeId::Pointer:
      TemplatedCopyOperation<uintptr_t>(source, target);
      break;
    case TypeId::Float:
      TemplatedCopyOperation<float>(source, target);
      break;
    case TypeId::Double:
      TemplatedCopyOperation<double>(source, target);
      break;
    case TypeId::Date:
      TemplatedCopyOperation<Date>(source, target);
      break;
    case TypeId::Timestamp:
      TemplatedCopyOperation<Timestamp>(source, target);
      break;
    case TypeId::Varchar:
      StringCopyOperation(source, target);
      break;
    default:
      throw NotImplementedException(fmt::format("copying vector of type '{}' not supported",
                                                TypeIdToString(source.GetTypeId())));
  }
}  // namespace tpl::sql

}  // namespace tpl::sql
