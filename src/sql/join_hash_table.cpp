#include "sql/join_hash_table.h"

#include "sql/vector_projection_iterator.h"

namespace tpl::sql {

JoinHashTable::JoinHashTable(util::Region *region, u32 tuple_size,
                             bool use_concise_ht) noexcept
    : region_(region),
      tuple_size_(tuple_size),
      num_elems_(0),
      built_(false),
      use_concise_ht_(use_concise_ht) {
  head()->next = nullptr;
}

void JoinHashTable::BuildGenericHashTable() {
  // TODO(pmenon): Use tagged insertions/probes if no bloom filter exists

  generic_hash_table()->SetSize(num_elems());

  for (HashTableEntry *entry = head()->next; entry != nullptr;) {
    HashTableEntry *next = entry->next;
    generic_hash_table()->Insert<false>(entry, entry->hash);
    entry = next;
  }
}

void JoinHashTable::ReorderEntries() {
  //
  // We need to walk over all materialized entries and reorder them according to
  // their order in the concise hash table. This order is stored in the
  // 'cht_slot' field in each HashTableEntry.
  //
  // This requires linear time, but only constant space. We do this by walking
  // the array and consecutively swapping elements into a temporary buffer.
  //
}

void JoinHashTable::BuildConciseHashTable() {
  concise_hash_table()->SetSize(num_elems());

  for (HashTableEntry *entry = head()->next; entry != nullptr;) {
    HashTableEntry *next = entry->next;
    entry->cht_slot = concise_hash_table()->Insert(entry->hash);
    entry = next;
  }

  // Insertions complete, build it
  concise_hash_table()->Build();

  // Re-order
  ReorderEntries();
}

void JoinHashTable::Build() {
  if (is_table_built()) {
    return;
  }

  // TODO(pmenon): Use HLL++ sketches to better estimate size

  if (use_concise_hash_table()) {
    BuildConciseHashTable();
  } else {
    BuildGenericHashTable();
  }

  // The table has been built. Set the flag now so we don't redo it
  built_ = true;
}

void JoinHashTable::LookupBatchInGenericHashTable(
    u32 num_tuples, hash_t hashes[], HashTableEntry *results[]) const {
  // TODO(pmenon): Use tagged insertions/probes if no bloom filter exists

  // Initial lookup
  for (u32 i = 0; i < num_tuples; i++) {
    results[i] = generic_hash_table()->FindChainHead(hashes[i]);
  }

  // Ensure find match on hash
  for (u32 i = 0; i < num_tuples; i++) {
    auto *entry = results[i];
    while (entry != nullptr && entry->hash != hashes[i]) {
      entry = entry->next;
    }
    results[i] = entry;
  }
}

void JoinHashTable::LookupBatchInConciseHashTable(
    u32 num_tuples, hash_t hashes[], HashTableEntry *results[]) const {}

void JoinHashTable::LookupBatch(u32 num_tuples, hash_t hashes[],
                                HashTableEntry *results[]) const {
  TPL_ASSERT(is_table_built(), "Cannot perform lookup before table is built!");
  if (use_concise_hash_table()) {
    LookupBatchInConciseHashTable(num_tuples, hashes, results);
  } else {
    LookupBatchInGenericHashTable(num_tuples, hashes, results);
  }
}

}  // namespace tpl::sql
