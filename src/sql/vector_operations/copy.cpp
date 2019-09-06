#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

namespace {

template <typename T>
void TemplatedCopyOperation(const Vector &source, void *target, u64 offset, u64 count) {
  auto *src_data = reinterpret_cast<T *>(source.data());
  auto *target_data = reinterpret_cast<T *>(target);
  VectorOps::Exec(source, [&](u64 i, u64 k) { target_data[k - offset] = src_data[i]; }, offset,
                  count);
}

void GenericCopyOperation(const Vector &source, void *target, u64 offset, u64 element_count) {
  if (source.count() == 0) {
    return;
  }

  switch (source.type_id()) {
    case TypeId::Boolean: {
      TemplatedCopyOperation<bool>(source, target, offset, element_count);
      break;
    }
    case TypeId::TinyInt: {
      TemplatedCopyOperation<i8>(source, target, offset, element_count);
      break;
    }
    case TypeId::SmallInt: {
      TemplatedCopyOperation<i16>(source, target, offset, element_count);
      break;
    }
    case TypeId::Integer: {
      TemplatedCopyOperation<i32>(source, target, offset, element_count);
      break;
    }
    case TypeId::BigInt: {
      TemplatedCopyOperation<i64>(source, target, offset, element_count);
      break;
    }
    case TypeId::Hash: {
      TemplatedCopyOperation<hash_t>(source, target, offset, element_count);
      break;
    }
    case TypeId::Pointer: {
      TemplatedCopyOperation<uintptr_t>(source, target, offset, element_count);
      break;
    }
    case TypeId::Float: {
      TemplatedCopyOperation<f32>(source, target, offset, element_count);
      break;
    }
    case TypeId::Double: {
      TemplatedCopyOperation<f64>(source, target, offset, element_count);
      break;
    }
    default: { throw std::logic_error("Don't use Copy for varlen types"); }
  }
}

}  // namespace

void VectorOps::Copy(const Vector &source, void *target, u64 offset, u64 element_count) {
  TPL_ASSERT(IsTypeFixedSize(source.type_), "Copy should only be used for fixed-length types");
  GenericCopyOperation(source, target, offset, element_count);
}

void VectorOps::Copy(const Vector &source, Vector *target, u64 offset) {
  TPL_ASSERT(offset < source.count_, "Out-of-bounds offset");
  target->count_ = source.count_ - offset;
  Exec(source,
       [&](u64 i, u64 k) { target->null_mask_.SetTo(k - offset, source.null_mask_.Test(i)); },
       offset);
  Copy(source, target->data_, offset, target->count_);
}

}  // namespace tpl::sql
