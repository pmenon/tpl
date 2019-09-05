#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

namespace {

template <typename T>
void CopyLoop(const Vector &source, void *target, u64 offset, u64 count) {
  auto src_data = reinterpret_cast<T *>(source.data());
  auto target_data = reinterpret_cast<T *>(target);
  VectorOps::Exec(source, [&](u64 i, u64 k) { target_data[k - offset] = src_data[i]; }, offset,
                  count);
}

void GenericCopyLoop(const Vector &source, void *target, u64 offset, u64 element_count) {
  if (source.count() == 0) {
    return;
  }

  switch (source.type_id()) {
    case TypeId::Boolean: {
      CopyLoop<bool>(source, target, offset, element_count);
      break;
    }
    case TypeId::TinyInt: {
      CopyLoop<i8>(source, target, offset, element_count);
      break;
    }
    case TypeId::SmallInt: {
      CopyLoop<i16>(source, target, offset, element_count);
      break;
    }
    case TypeId::Integer: {
      CopyLoop<i32>(source, target, offset, element_count);
      break;
    }
    case TypeId::BigInt: {
      CopyLoop<i64>(source, target, offset, element_count);
      break;
    }
    case TypeId::Hash: {
      CopyLoop<hash_t>(source, target, offset, element_count);
      break;
    }
    case TypeId::Pointer: {
      CopyLoop<uintptr_t>(source, target, offset, element_count);
      break;
    }
    case TypeId::Float: {
      CopyLoop<f32>(source, target, offset, element_count);
      break;
    }
    case TypeId::Double: {
      CopyLoop<f64>(source, target, offset, element_count);
      break;
    }
    default: { throw std::logic_error("Don't use Copy for varlen types"); }
  }
}

}  // namespace

void VectorOps::Copy(const Vector &source, void *target, u64 offset, u64 element_count) {
  TPL_ASSERT(IsTypeFixedSize(source.type_), "Copy should only be used for fixed-length types");
  GenericCopyLoop(source, target, offset, element_count);
}

void VectorOps::Copy(const Vector &source, Vector *target, u64 offset) {
  TPL_ASSERT(offset < source.count_, "Out-of-bounds offset");
  target->count_ = source.count_ - offset;
  Exec(source, [&](u64 i, u64 k) { target->null_mask_[k - offset] = source.null_mask_[i]; },
       offset);
  Copy(source, target->data_, offset, target->count_);
}

}  // namespace tpl::sql
