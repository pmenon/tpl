#pragma once

#include <limits>

#include "util/bitfield.h"
#include "util/common.h"

namespace tpl::sql {

/// Compact structure representing a position in the concise hash table. CHT
/// slots are 32-bit values with the following encoding:
///
/// +------------------+-----------------+
/// | Overflow (1 bit) | Index (31 bits) |
/// +------------------+-----------------+
///
/// We use the least-significant 31-bits to store the index of the entry in the
/// concise hash array; the most-significant bit is used to indicate whether the
/// slot points to the overflow table.
class ConciseHashTableSlot {
  friend class ConciseHashTable;

 private:
  ConciseHashTableSlot(bool overflow, u32 index)
      : bitfield_(OverflowField::Encode(overflow) | IndexField::Encode(index)) {
  }

 public:
  ConciseHashTableSlot() : bitfield_(std::numeric_limits<u32>::max()) {}

  // -------------------------------------------------------
  // Static factories
  // -------------------------------------------------------

  static ConciseHashTableSlot Make(u32 index) {
    return ConciseHashTableSlot(false, index);
  }

  static ConciseHashTableSlot MakeOverflow() {
    return ConciseHashTableSlot(true, 0);
  }

  // -------------------------------------------------------
  // Query
  // -------------------------------------------------------

  /// Does this slow point to the overflow table?
  bool IsOverflow() const { return OverflowField::Decode(bitfield_); }

  /// Assuming this slot isn't an overflow slot, return the index this slot
  /// represents in the concise hash table
  u32 GetIndex() const { return IndexField::Decode(bitfield_); }

  // -------------------------------------------------------
  // Equality operations
  // -------------------------------------------------------

  bool Equal(const ConciseHashTableSlot &that) const {
    return IsOverflow() == that.IsOverflow() && GetIndex() == that.GetIndex();
  }

  bool operator==(const ConciseHashTableSlot &that) const {
    return Equal(that);
  }

  bool operator!=(const ConciseHashTableSlot &that) const {
    return !(*this == that);
  }

 private:
  // clang-format off
  class IndexField : public util::BitField32<u32, 0, 31> {};
  class OverflowField : public util::BitField32<bool, IndexField::kNextBit, 1> {};
  // clang-format on

 private:
  // The bitfield we use to encode the overflow and index bits
  u32 bitfield_;
};

/// A generic structure used to represent an entry in either a generic hash
/// table or a concise hash table. An entry is a variably-sized chunk of
/// memory where the keys, attributes, aggregates are stored in the \a payload
/// field. This structure is used for both joins and aggregations.
struct HashTableEntry {
  static_assert(sizeof(ConciseHashTableSlot) <= sizeof(HashTableEntry *),
                "CHT slots should be smaller than 64-bits");

  union {
    HashTableEntry *next;
    ConciseHashTableSlot cht_slot{};
  };
  hash_t hash;
  byte payload[0];
};

}  // namespace tpl::sql
