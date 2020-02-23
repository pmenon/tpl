#include "sql/join_hash_table_vector_probe.h"

#include "common/exception.h"
#include "sql/constant_vector.h"
#include "sql/generic_value.h"
#include "sql/join_hash_table.h"
#include "sql/static_vector.h"
#include "sql/vector_operations/vector_operators.h"
#include "sql/vector_projection.h"

namespace tpl::sql {

JoinHashTableVectorProbe::JoinHashTableVectorProbe(const JoinHashTable &table,
                                                   planner::LogicalJoinType join_type,
                                                   std::vector<uint32_t> join_key_indexes)
    : table_(table),
      join_type_(join_type),
      join_key_indexes_(std::move(join_key_indexes)),
      key_matches_(kDefaultVectorSize),
      semi_anti_key_matches_(kDefaultVectorSize),
      non_null_entries_(kDefaultVectorSize),
      matches_(TypeId::Pointer, true, true),
      first_(true) {}

void JoinHashTableVectorProbe::Init(VectorProjection *input) {
  // Resize keys, if need be.
  if (const auto size = input->GetTotalTupleCount(); TPL_UNLIKELY(matches_.GetSize() != size)) {
    matches_.Resize(size);
    key_matches_.Resize(size);
    semi_anti_key_matches_.Resize(size);
    non_null_entries_.Resize(size);
  }

  // First, hash the keys.
  StaticVector<hash_t> hashes;
  input->Hash(join_key_indexes_, &hashes);

  // Perform the initial lookup.
  table_.LookupBatch(hashes, &matches_);

  // Assume for simplicity that all probe keys found join partners from
  // the previous lookup. We'll verify and validate this assumption when we
  // filter the matches vector for non-null entries below.
  input->CopySelectionsTo(&non_null_entries_);

  // Filter out non-null entries, storing the result in the non-null TID list.
  ConstantVector null_ptr(GenericValue::CreatePointer<HashTableEntry>(nullptr));
  VectorOps::SelectNotEqual(matches_, null_ptr, &non_null_entries_);

  // Initial matches.
  key_matches_.AssignFrom(non_null_entries_);
}

namespace {

#define FUSE_GATHER_SELECT 0

template <typename T>
void TemplatedCompareKey(Vector *probe_keys, Vector *entries, const std::size_t key_offset,
                         TupleIdList *key_equal_tids) {
#if FUSE_GATHER_SELECT == 0
  // The build keys we'll gather.
  StaticVector<T> build_keys;
  build_keys.Resize(key_equal_tids->GetCapacity());

  // Temporarily scope.
  const uint64_t size = key_equal_tids->GetTupleCount();
  Vector::TempFilterScope probe_scope(probe_keys, key_equal_tids, size);
  Vector::TempFilterScope build_scope(&build_keys, key_equal_tids, size);

  // Gather and select.
  VectorOps::Gather(*entries, &build_keys, key_offset);
  VectorOps::SelectEqual(*probe_keys, build_keys, key_equal_tids);
#else
  auto *RESTRICT raw_probe_keys = reinterpret_cast<const T *>(probe_keys->GetData());
  auto *RESTRICT raw_entries = reinterpret_cast<const HashTableEntry **>(entries->GetData());
  key_equal_tids->Filter([&](uint64_t i) {
    auto *RESTRICT table_key = reinterpret_cast<const T *>(raw_entries[i]->payload + key_offset);
    return raw_probe_keys[i] == *table_key;
  });
#endif
}

void CompareKey(Vector *probe_keys, Vector *entries, const std::size_t key_offset,
                TupleIdList *key_equal_tids) {
  switch (probe_keys->GetTypeId()) {
    case TypeId::Boolean:
      TemplatedCompareKey<bool>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::TinyInt:
      TemplatedCompareKey<int8_t>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::SmallInt:
      TemplatedCompareKey<int16_t>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::Integer:
      TemplatedCompareKey<int32_t>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::BigInt:
      TemplatedCompareKey<int64_t>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::Float:
      TemplatedCompareKey<float>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::Double:
      TemplatedCompareKey<double>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::Date:
      TemplatedCompareKey<Date>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::Varchar:
      TemplatedCompareKey<VarlenEntry>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    case TypeId::Varbinary:
      TemplatedCompareKey<Blob>(probe_keys, entries, key_offset, key_equal_tids);
      break;
    default:
      throw NotImplementedException("key comparison on type {} not supported",
                                    TypeIdToString(probe_keys->GetTypeId()));
  }
}

}  // namespace

void JoinHashTableVectorProbe::CheckKeyEquality(VectorProjection *input) {
  // Filter matches in preparation for the key check.
  matches_.SetFilteredTupleIdList(&key_matches_, key_matches_.GetTupleCount());

  // Check each key component.
  std::size_t key_offset = sizeof(HashTableEntry);
  for (const auto key_index : join_key_indexes_) {
    auto probe_keys = input->GetColumn(key_index);
    CompareKey(probe_keys, &matches_, key_offset, &key_matches_);
    if (key_matches_.IsEmpty()) break;
    key_offset += GetTypeIdSize(probe_keys->GetTypeId());
  }
}

// Advance all non-null entries in the matches vector to their next element.
void JoinHashTableVectorProbe::FollowChainNext() {
  // We decompose this advancement into two steps to
  auto *RESTRICT raw_entries = reinterpret_cast<const HashTableEntry **>(matches_.GetData());
  non_null_entries_.ForEach([&](const uint64_t i) { raw_entries[i] = raw_entries[i]->next; });
  non_null_entries_.GetMutableBits()->UpdateFull(
      [&](const uint64_t i) { return raw_entries[i] != nullptr; });
}

bool JoinHashTableVectorProbe::NextInnerJoin(VectorProjection *input) {
  while (!non_null_entries_.IsEmpty()) {
    if (!first_) {
      FollowChainNext();
    }
    first_ = false;

    // Check the input keys against the current set of matches.
    key_matches_.AssignFrom(non_null_entries_);
    if (const auto *input_filter = input->GetFilteredTupleIdList(); input_filter != nullptr) {
      key_matches_.IntersectWith(*input_filter);
    }

    // Check the keys.
    CheckKeyEquality(input);

    // If there are any matches, we exit the loop. Otherwise, if there are still
    // valid non-NULL entries, we'll follow the chain.
    if (!key_matches_.IsEmpty()) {
      return true;
    }
  }

  // No more matches.
  return false;
}

template <bool Match>
bool JoinHashTableVectorProbe::NextSemiOrAntiJoin(VectorProjection *input) {
  // SEMI and ANTI joins are different that INNER joins since there can only be
  // at most ONE match for each input tuple. Thus, we handle the entire chunk in
  // one call to Next(). For every pointer, we chase bucket chain pointers doing
  // comparisons, stopping either when we find the first match (for SEMI), or
  // exhaust the chain (for ANTI).
  const auto *input_filter = input->GetFilteredTupleIdList();

  semi_anti_key_matches_.Clear();
  while (!non_null_entries_.IsEmpty()) {
    if (!first_) {
      FollowChainNext();
    }
    first_ = false;

    // Check the input keys against the current set of matches.
    key_matches_.AssignFrom(non_null_entries_);
    if (input_filter != nullptr) {
      key_matches_.IntersectWith(*input_filter);
    }
    key_matches_.UnsetFrom(semi_anti_key_matches_);

    // Check the keys.
    CheckKeyEquality(input);

    // Add the found matches to the running list.
    semi_anti_key_matches_.UnionWith(key_matches_);
  }

  key_matches_.AddAll();
  if (Match) {
    key_matches_.IntersectWith(semi_anti_key_matches_);
  } else {
    key_matches_.UnsetFrom(semi_anti_key_matches_);
  }

  return !key_matches_.IsEmpty();
}

bool JoinHashTableVectorProbe::NextSemiJoin(VectorProjection *input) {
  return NextSemiOrAntiJoin<true>(input);
}

bool JoinHashTableVectorProbe::NextAntiJoin(VectorProjection *input) {
  return NextSemiOrAntiJoin<false>(input);
}

bool JoinHashTableVectorProbe::NextRightJoin(VectorProjection *input) {
  throw NotImplementedException("Vectorized right outer joins");
}

bool JoinHashTableVectorProbe::Next(VectorProjection *input) {
  switch (join_type_) {
    case planner::LogicalJoinType::INNER:
      return NextInnerJoin(input);
    case planner::LogicalJoinType::SEMI:
      return NextSemiJoin(input);
    case planner::LogicalJoinType::ANTI:
      return NextAntiJoin(input);
    case planner::LogicalJoinType::RIGHT:
      return NextRightJoin(input);
    default:
      throw NotImplementedException("Join type []", planner::JoinTypeToString(join_type_));
  }
}

const Vector *JoinHashTableVectorProbe::GetMatches() {
  // Setup the matches vector with the correct filter since only a subset of
  // matches have found matching keys.
  matches_.SetFilteredTupleIdList(&key_matches_, key_matches_.GetTupleCount());
  // Done.
  return &matches_;
}

void JoinHashTableVectorProbe::Reset() {}

}  // namespace tpl::sql
