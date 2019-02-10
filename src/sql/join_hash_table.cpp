#include "sql/join_hash_table.h"

#include "sql/vector_projection_iterator.h"

namespace tpl::sql {

JoinHashTable::JoinHashTable(util::Region *region, u32 tuple_size)
    : region_(region), tuple_size_(tuple_size), num_elems_(0), built_(false) {
  head()->next = nullptr;
}

void JoinHashTable::Build() {
  if (is_table_built()) {
    return;
  }

  // TODO(pmenon): Use HLL++ sketches to better estimate size
  // TODO(pmenon): Select between generic tables and concise tables
  // TODO(pmenon): Use tagged insertions/probes if no bloom filter exists

  generic_hash_table()->SetSize(num_elems());

  for (auto *entry = head()->next; entry != nullptr;) {
    auto *const next = entry->next;
    generic_hash_table()->Insert<false>(entry, entry->hash);
    entry = next;
  }

  // The table has been built. Set the flag now so we don't redo it
  built_ = true;
}

void JoinHashTable::LookupBatch(JoinHashTable::VectorLookup *lookup) const {
  // TODO(pmenon): Select between generic tables and concise tables
  // TODO(pmenon): Use tagged insertions/probes if no bloom filter exists

  auto *hashes = lookup->hashes();
  auto *entries = lookup->entries();

  // Initial lookup
  for (u32 i = 0; i < lookup->NumTuples(); i++) {
    entries[i] = generic_hash_table()->FindChainHead(hashes[i]);
  }

  // Ensure find match on hash
  for (u32 i = 0; i < lookup->NumTuples(); i++) {
    auto *entry = entries[i];
    while (entry != nullptr && entry->hash != hashes[i]) {
      entry = entry->next;
    }
    entries[i] = entry;
  }
}

// ---------------------------------------------------------
// JoinHashTable's VectorLookup
// ---------------------------------------------------------

JoinHashTable::VectorLookup::VectorLookup(const JoinHashTable &table,
                                          VectorProjectionIterator *vpi)
    : table_(table), vpi_(vpi) {}

u32 JoinHashTable::VectorLookup::NumTuples() { return vpi()->num_selected(); }

}  // namespace tpl::sql