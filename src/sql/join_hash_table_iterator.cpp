#include "sql/join_hash_table.h"

namespace tpl::sql {

JoinHashTableIterator::JoinHashTableIterator(const JoinHashTable &table) {
  TPL_ASSERT(table.IsBuilt(), "Cannot iterate a JoinHashTable that hasn't been yet!");
  chunk_iter_ = table.owned_.begin();
  chunk_end_ = table.owned_.end();
  if (table.owned_.empty()) {
    entry_iter_ = table.entries_.begin();
    entry_end_ = table.entries_.end();
  } else {
    entry_iter_ = chunk_iter_->begin();
    entry_end_ = chunk_iter_->end();
    ++chunk_iter_;
  }
}

}  // namespace tpl::sql
