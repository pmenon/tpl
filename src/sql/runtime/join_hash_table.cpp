#include "sql/runtime/join_hash_table.h"

namespace tpl::sql::runtime {

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

}  // namespace tpl::sql::runtime