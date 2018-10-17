#pragma once

#include <cstdint>

#include "common.h"

namespace tpl::util {

// Bitfield for encoding/decoding values of type T into a storage space of type
// S. The values are located at the bit 'position' and have a size of 'size'
template <typename S, typename T, unsigned position, unsigned size>
class BitFieldBase {
 public:
  // Some checks
  static_assert((sizeof(S) * kBitsPerByte) >= (position + size),
                "The size of the provided storage type is not large enough to "
                "encode the value");

  static_assert((sizeof(T) * kBitsPerByte) <= size,
                "The provided size of the bitfield is smaller than the type "
                "you want to encode");

  static constexpr const S kOne = static_cast<S>(1U);

  static constexpr const S kNextBit = position + size;

  static constexpr const S kMask = (kOne << size) - 1;

  static constexpr const S kMaskInPosition = kMask << position;

  static constexpr S mask() { return kMask; }

  static constexpr S mask_in_position() { return kMaskInPosition; }

  static S Encode(T val) { return static_cast<S>(val) << position; }

  static T Decode(S storage) {
    return (static_cast<T>(storage >> position) & mask());
  }

  static S Update(S curr_storage, T update) {
    return (curr_storage & ~mask_in_position()) | Encode(update);
  }
};

template <typename T, unsigned position, unsigned size>
class BitField8 : public BitFieldBase<u8, T, position, size> {};

template <typename T, unsigned position, unsigned size>
class BitField16 : public BitFieldBase<u16, T, position, size> {};

template <typename T, unsigned position, unsigned size>
class BitField32 : public BitFieldBase<u32, T, position, size> {};

template <typename T, unsigned position, unsigned size>
class BitField64 : public BitFieldBase<u64, T, position, size> {};

}  // namespace tpl::util