#pragma once

#include "util/bitfield.h"
#include "util/common.h"

namespace tpl::sql::runtime {

/// Compact structure representing a position in the concise hash table.
class CHTSlot {
 public:
  CHTSlot() = default;

  static CHTSlot Make(u32 index) { return CHTSlot(false, index); }

  static CHTSlot MakeOverflow() { return CHTSlot(true, 0); }

  bool IsOverflow() const { return Overflow::Decode(bitfield()); }

  u32 GetIndex() const { return Index::Decode(bitfield()); }

 private:
  u32 bitfield() const { return bitfield_; }

  class Overflow : public util::BitField32<bool, 0, 1> {};
  class Index : public util::BitField32<u32, Overflow::kNextBit, 31> {};

 private:
  CHTSlot(bool overflow, u32 index)
      : bitfield_(Overflow::Encode(overflow) | Index::Encode(index)) {}

 private:
  u32 bitfield_;
};

/// A generic structure used to represent an entry in either a generic hash
/// table or a concise hash table. An entry is a variably-sized chunk of
/// memory where the keys, attributes, aggregates are stored in the \p payload
/// field. This structure is used for both joins and aggregations.
struct HashTableEntry {
  static_assert(sizeof(CHTSlot) == sizeof(u32), "CHT slots should be 4-bytes");

  union {
    HashTableEntry *next;
    CHTSlot cht_slot;
  };
  hash_t hash;
  byte payload[0];

  HashTableEntry() = default;
};

}  // namespace tpl::sql::runtime