#include "sql/vector_operations/vector_operators.h"

namespace tpl::sql {

namespace {

template <typename T>
void TemplatedCopyOperation(const Vector &source, void *target, uint64_t offset, uint64_t count) {
  auto *src_data = reinterpret_cast<T *>(source.data());
  auto *target_data = reinterpret_cast<T *>(target);
  VectorOps::Exec(source, [&](uint64_t i, uint64_t k) { target_data[k - offset] = src_data[i]; },
                  offset, count);
}

void GenericCopyOperation(const Vector &source, void *target, uint64_t offset,
                          uint64_t element_count) {
  if (source.count() == 0) {
    return;
  }

  switch (source.type_id()) {
    case TypeId::Boolean: {
      TemplatedCopyOperation<bool>(source, target, offset, element_count);
      break;
    }
    case TypeId::TinyInt: {
      TemplatedCopyOperation<int8_t>(source, target, offset, element_count);
      break;
    }
    case TypeId::SmallInt: {
      TemplatedCopyOperation<int16_t>(source, target, offset, element_count);
      break;
    }
    case TypeId::Integer: {
      TemplatedCopyOperation<int32_t>(source, target, offset, element_count);
      break;
    }
    case TypeId::BigInt: {
      TemplatedCopyOperation<int64_t>(source, target, offset, element_count);
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
      TemplatedCopyOperation<float>(source, target, offset, element_count);
      break;
    }
    case TypeId::Double: {
      TemplatedCopyOperation<double>(source, target, offset, element_count);
      break;
    }
    default: { throw std::logic_error("Don't use Copy for varlen types"); }
  }
}

}  // namespace

void VectorOps::Copy(const Vector &source, void *target, uint64_t offset, uint64_t element_count) {
  TPL_ASSERT(IsTypeFixedSize(source.type_), "Copy should only be used for fixed-length types");
  GenericCopyOperation(source, target, offset, element_count);
}

void VectorOps::Copy(const Vector &source, Vector *target, uint64_t offset) {
  TPL_ASSERT(offset < source.count_, "Out-of-bounds offset");
  target->count_ = source.count_ - offset;
  Exec(source,
       [&](uint64_t i, uint64_t k) { target->null_mask_[k - offset] = source.null_mask_[i]; },
       offset);
  Copy(source, target->data_, offset, target->count_);
}

}  // namespace tpl::sql
