#include "sql/generic_hash_table.h"

#include "util/math_util.h"
#include "util/memory.h"

namespace tpl::sql {

GenericHashTable::GenericHashTable(float load_factor)
    : entries_(nullptr),
      mask_(0),
      capacity_(0),
      num_elems_(0),
      load_factor_(load_factor) {}

GenericHashTable::~GenericHashTable() {
  if (entries() != nullptr) {
    util::mem::FreeHugeArray(entries(), capacity());
  }
}

void GenericHashTable::SetSize(u64 new_size) {
  TPL_ASSERT(new_size > 0, "New size cannot be zero!");
  if (entries() != nullptr) {
    util::mem::FreeHugeArray(entries(), capacity());
  }

  u64 next_size = util::MathUtil::PowerOf2Ceil(new_size);
  if (next_size < new_size / load_factor()) {
    next_size *= 2;
  }

  capacity_ = next_size;
  mask_ = capacity_ - 1;
  entries_ =
      util::mem::MallocHugeArray<std::atomic<HashTableEntry *>>(capacity_);
}

}  // namespace tpl::sql