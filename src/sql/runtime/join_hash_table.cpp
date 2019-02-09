#include "sql/runtime/join_hash_table.h"

namespace tpl::sql::runtime {

JoinHashTable::JoinHashTable(util::Region *region)
    : region_(region), num_elems_(0), built_(false) {
  head()->next = nullptr;
}

void JoinHashTable::Build() {
  TPL_ASSERT(!is_table_built(), "Calling Build() on an already built table");

  // TODO(pmenon): Use HLL++ sketches to better estimate size

  generic_hash_table()->SetSize(num_elems());

  for (auto *entry_header = head()->next; entry_header != nullptr;) {
    auto *const RESTRICT next = entry_header->next;
    generic_hash_table()->Insert<false>(entry_header, entry_header->hash);
    entry_header = next;
  }

  // The table has been built. Set the flag now so we don't redo it
  built_ = true;
}

}  // namespace tpl::sql::runtime