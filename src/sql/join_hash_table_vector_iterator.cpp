#include "sql/join_hash_table.h"

#include "sql/vector_projection.h"

namespace tpl::sql {

JoinHashTableVectorIterator::JoinHashTableVectorIterator(const JoinHashTable &table)
    : iter_(table), data_(std::make_unique<VectorProjection>()) {
  // Create projection.
  data_->Initialize({TypeId::Pointer});
  // Attempt to fill projection.
  FillProjection();
}

// Needed because we forward-declared VectorProjection.
JoinHashTableVectorIterator::~JoinHashTableVectorIterator() = default;

bool JoinHashTableVectorIterator::HasNext() const noexcept { return !data_->IsEmpty(); }

void JoinHashTableVectorIterator::Next() noexcept { FillProjection(); }

void JoinHashTableVectorIterator::FillProjection() {
  std::size_t idx = 0;
  const std::size_t capacity = data_->GetTupleCapacity();
  auto entries = reinterpret_cast<const byte **>(data_->GetColumn(0)->GetData());
  for (; iter_.HasNext() && idx < capacity; iter_.Next()) {
    entries[idx++] = iter_.GetCurrentRow();
  }
  data_->Reset(idx);
}

Vector *JoinHashTableVectorIterator::GetEntries() const { return data_->GetColumn(0); }

}  // namespace tpl::sql
