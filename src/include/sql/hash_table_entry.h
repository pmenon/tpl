#pragma once

#include "util/common.h"

namespace tpl::sql {

using ConciseHashTableSlot = u64;

/// A generic structure used to represent an entry in either a generic hash
/// table or a concise hash table. An entry is a variably-sized chunk of
/// memory where the keys, attributes, aggregates are stored in the \a payload
/// field. This structure is used for both joins and aggregations.
struct HashTableEntry {
  union {
    // Next is used to chain together entries falling to the same bucket
    HashTableEntry *next;

    // This slot is used to record the slot this entry occupies in the CHT
    ConciseHashTableSlot cht_slot;

    // Used during reordering over overflow entries when constructing a CHT
    u64 overflow_count;
  };

  hash_t hash;
  byte payload[0];
};

}  // namespace tpl::sql
