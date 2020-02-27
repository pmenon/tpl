#include "sql/join_hash_table_vector_probe.h"

#include "common/cpu_info.h"
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
      initial_match_list_(kDefaultVectorSize),
      initial_matches_(TypeId::Pointer, true, true),
      non_null_entries_(kDefaultVectorSize),
      key_matches_(kDefaultVectorSize),
      semi_anti_key_matches_(kDefaultVectorSize),
      curr_matches_(TypeId::Pointer, true, true),
      first_(true) {}

void JoinHashTableVectorProbe::Init(VectorProjection *input) {
  // Resize keys, if need be.
  if (const auto size = input->GetTotalTupleCount();
      TPL_UNLIKELY(initial_matches_.GetSize() != size)) {
    initial_match_list_.Resize(size);
    initial_matches_.Resize(size);
    non_null_entries_.Resize(size);
    key_matches_.Resize(size);
    semi_anti_key_matches_.Resize(size);
    curr_matches_.Resize(size);
  }

  // First probe.
  first_ = true;

  // First, hash the keys.
  StaticVector<hash_t> hashes;
  input->Hash(join_key_indexes_, &hashes);

  // Perform the initial lookup.
  table_.LookupBatch(hashes, &initial_matches_);

  // Assume for simplicity that all probe keys found join partners from the
  // previous lookup. We'll verify and validate this assumption when we filter
  // the matches vector for non-null entries below.
  input->CopySelectionsTo(&initial_match_list_);

  // Filter out non-null entries, storing the result in the non-null TID list.
  ConstantVector null_ptr(GenericValue::CreatePointer<HashTableEntry>(nullptr));
  VectorOps::SelectNotEqual(initial_matches_, null_ptr, &initial_match_list_);

  // At this point, initial-matches contains a list of pointers to bucket chains
  // in the hash table, and the initial-matches-list contains only the TIDS of
  // non-null entries. We'll copy this into the current-matches and non-null
  // entries list so that the call to Next() is primed and ready to go.
  non_null_entries_.AssignFrom(initial_match_list_);
  key_matches_.AssignFrom(initial_match_list_);
  initial_matches_.Clone(&curr_matches_);
}

namespace {

// TODO(pmenon): Prefetch?
template <typename T>
void TemplatedCompareKey(Vector *probe_keys, Vector *entries, const std::size_t key_offset,
                         TupleIdList *key_equal_tids) {
  auto *RESTRICT raw_probe_keys = reinterpret_cast<const T *>(probe_keys->GetData());
  auto *RESTRICT raw_entries = reinterpret_cast<const HashTableEntry **>(entries->GetData());
  key_equal_tids->Filter([&](uint64_t i) {
    auto *RESTRICT table_key = reinterpret_cast<const T *>(raw_entries[i]->payload + key_offset);
    return raw_probe_keys[i] == *table_key;
  });
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
  curr_matches_.SetFilteredTupleIdList(&key_matches_, key_matches_.GetTupleCount());

  // Check each key component.
  std::size_t key_offset = 0;
  for (const auto key_index : join_key_indexes_) {
    auto probe_keys = input->GetColumn(key_index);
    CompareKey(probe_keys, &curr_matches_, key_offset, &key_matches_);
    if (key_matches_.IsEmpty()) break;
    key_offset += GetTypeIdSize(probe_keys->GetTypeId());
  }
}

// Advance all non-null entries in the matches vector to their next element.
void JoinHashTableVectorProbe::FollowNext() {
  auto *RESTRICT entries = reinterpret_cast<const HashTableEntry **>(curr_matches_.GetData());
  non_null_entries_.Filter([&](uint64_t i) { return (entries[i] = entries[i]->next) != nullptr; });
}

bool JoinHashTableVectorProbe::NextInnerJoin(VectorProjection *input) {
  const auto *input_filter = input->GetFilteredTupleIdList();

  if (input_filter != nullptr) {
    non_null_entries_.IntersectWith(*input_filter);
  }

  while (!non_null_entries_.IsEmpty()) {
    if (!first_) {
      FollowNext();
    }
    first_ = false;

    // Check the input keys against the current set of matches.
    key_matches_.AssignFrom(non_null_entries_);
    if (key_matches_.IsEmpty()) {
      return false;
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

  // Filter out TIDs from the non-null entries list. This can happen if the
  // input batch was filtered through another process after we were
  // initialized.
  if (input_filter != nullptr) {
    non_null_entries_.IntersectWith(*input_filter);
  }

  semi_anti_key_matches_.Clear();
  while (!non_null_entries_.IsEmpty()) {
    if (!first_) {
      FollowNext();
    }
    first_ = false;

    // The keys to check are all the non-null entries minus the entries that
    // have already found a match. If this set is empty, we're done.
    key_matches_.AssignFrom(non_null_entries_);
    key_matches_.UnsetFrom(semi_anti_key_matches_);
    if (key_matches_.IsEmpty()) {
      break;
    }

    // Check the keys.
    CheckKeyEquality(input);

    // Add the found matches to the running list.
    semi_anti_key_matches_.UnionWith(key_matches_);
  }

  key_matches_.AddAll();
  if constexpr (Match) {
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
  bool has_next;
  switch (join_type_) {
    case planner::LogicalJoinType::INNER:
      has_next = NextInnerJoin(input);
      break;
    case planner::LogicalJoinType::SEMI:
      has_next = NextSemiJoin(input);
      break;
    case planner::LogicalJoinType::ANTI:
      has_next = NextAntiJoin(input);
      break;
    case planner::LogicalJoinType::RIGHT:
      has_next = NextRightJoin(input);
      break;
    default:
      throw NotImplementedException("Join type []", planner::JoinTypeToString(join_type_));
  }

  // Filter the match vector now so GetMatches() returns the filtered list.
  curr_matches_.SetFilteredTupleIdList(&key_matches_, key_matches_.GetTupleCount());

  // Done.
  return has_next;
}

void JoinHashTableVectorProbe::Reset() {
  non_null_entries_.AssignFrom(initial_match_list_);
  key_matches_.AssignFrom(initial_match_list_);
  initial_matches_.Clone(&curr_matches_);
  first_ = true;
}

}  // namespace tpl::sql
