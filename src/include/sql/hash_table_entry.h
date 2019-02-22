#pragma once

#include <limits>

#include "util/bitfield.h"
#include "util/common.h"

namespace tpl::sql {

/// Compact structure representing a position in the concise hash table. CHT
/// slots are 64-bit values with the following encoding:
///
/// 63         62         61         60           0
/// +-----------+----------+----------+-----------+
/// | Processed | Buffered | Overflow |   Index   |
/// |  (1-bit)  |  (1-bit) | (1-bit)  | (61-bits) |
/// +-----------+----------+----------+-----------+
///
/// These flags are modified during the construction of the concise hash table
class ConciseHashTableSlot {
  friend class ConciseHashTable;

 private:
  ConciseHashTableSlot(bool overflow, u64 index) noexcept
      : bitfield_(ProcessedField::Encode(false) | BufferedField::Encode(false) |
                  OverflowField::Encode(overflow) | IndexField::Encode(index)) {
  }

 public:
  ConciseHashTableSlot() noexcept
      : bitfield_(std::numeric_limits<u64>::max()) {}

  // -------------------------------------------------------
  // Static factories
  // -------------------------------------------------------

  static ConciseHashTableSlot Make(u64 index) {
    return ConciseHashTableSlot(false, index);
  }

  static ConciseHashTableSlot MakeOverflow() {
    return ConciseHashTableSlot(true, 0);
  }

  // -------------------------------------------------------
  // Query/Update
  // -------------------------------------------------------

  bool IsBuffered() const noexcept { return BufferedField::Decode(bitfield_); }

  void SetBuffered(bool buffered) noexcept {
    bitfield_ = BufferedField::Update(bitfield_, buffered);
  }

  bool IsProcessed() const noexcept {
    return ProcessedField::Decode(bitfield_);
  }

  void SetProcessed(bool processed) noexcept {
    bitfield_ = ProcessedField::Update(bitfield_, processed);
  }

  bool IsOverflow() const noexcept { return OverflowField::Decode(bitfield_); }

  u64 GetSlotIndex() const noexcept { return IndexField::Decode(bitfield_); }

  // -------------------------------------------------------
  // Equality operations
  // -------------------------------------------------------

  bool Equal(const ConciseHashTableSlot &that) const noexcept {
    return IsOverflow() == that.IsOverflow() &&
           GetSlotIndex() == that.GetSlotIndex();
  }

  bool operator==(const ConciseHashTableSlot &that) const noexcept {
    return Equal(that);
  }

  bool operator!=(const ConciseHashTableSlot &that) const noexcept {
    return !(*this == that);
  }

 private:
  // clang-format off
  class IndexField : public util::BitField64<u64, 0, 61> {};
  class OverflowField : public util::BitField64<bool, IndexField::kNextBit, 1> {};
  class BufferedField : public util::BitField64<bool, OverflowField::kNextBit, 1> {};
  class ProcessedField : public util::BitField64<bool, BufferedField::kNextBit, 1> {};
  // clang-format on

 private:
  // The bitfield we use to encode the overflow and index bits
  u64 bitfield_;
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
