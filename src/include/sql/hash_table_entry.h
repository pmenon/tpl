#pragma once

#include <limits>

#include "util/bitfield.h"
#include "util/common.h"

namespace tpl::sql {

/// Compact structure representing a position in the concise hash table. CHT
/// slots are 64-bit values with the following encoding:
///
/// 63         62         61           0
/// +-----------+----------+-----------+
/// | Processed | Buffered |   Index   |
/// |  (1-bit)  |  (1-bit) | (62-bits) |
/// +-----------+----------+-----------+
///
/// These flags are modified during the construction of the concise hash table
class ConciseHashTableSlot {
 public:
  ConciseHashTableSlot() noexcept
      : ConciseHashTableSlot(std::numeric_limits<u64>::max()) {}

  explicit ConciseHashTableSlot(u64 index) noexcept
      : bitfield_(ProcessedField::Encode(false) | BufferedField::Encode(false) |
                  IndexField::Encode(index)) {}

  bool IsProcessed() const noexcept {
    return ProcessedField::Decode(bitfield_);
  }
  void SetProcessed(bool processed) noexcept {
    bitfield_ = ProcessedField::Update(bitfield_, processed);
  }

  bool IsBuffered() const noexcept { return BufferedField::Decode(bitfield_); }

  void SetBuffered(bool buffered) noexcept {
    bitfield_ = BufferedField::Update(bitfield_, buffered);
  }

  u64 GetSlotIndex() const noexcept { return IndexField::Decode(bitfield_); }

  bool Equal(const ConciseHashTableSlot &that) const noexcept {
    return IsProcessed() == that.IsProcessed() &&
           IsBuffered() == that.IsBuffered() &&
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
  class IndexField : public util::BitField64<u64, 0, 62> {};
  class BufferedField : public util::BitField64<bool, IndexField::kNextBit, 1> {};
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
  static_assert(sizeof(ConciseHashTableSlot) == sizeof(HashTableEntry *),
                "CHT slots should be exactly 8 bytes");

  union {
    HashTableEntry *next;
    ConciseHashTableSlot cht_slot{};
    u64 overflow_count;
  };

  hash_t hash;
  byte payload[0];
};

}  // namespace tpl::sql
