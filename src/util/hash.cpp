#include "util/hash.h"

#include <x86intrin.h>

namespace tpl::util {

uint32_t Hasher::Hash(const char *buf, uint64_t len, HashMethod method) {
  switch (method) {
    case HashMethod::Fnv1:
      return HashFnv1(buf, len);
    case HashMethod::Murmur3:
      return HashMurmur(buf, len);
    default:
      return HashCrc(buf, len);
  }
}

uint32_t Hasher::HashFnv1(const char *buf, uint64_t len) {
  uint32_t hash = 2166136261u;

  for (uint64_t i = 0; i < len; i++) {
    hash ^= buf[i];
    hash *= 16777619;
  }

  return hash;
}

uint32_t Hasher::HashMurmur(const char *buf, uint64_t len) { return 0; }

#define CRC32(op, crc, type, buf, len)                   \
  do {                                                   \
    for (; (len) >= sizeof(type);                        \
         (len) -= sizeof(type), (buf) += sizeof(type)) { \
      (crc) = op((crc), *(type *)buf);                   \
    }                                                    \
  } while (0)

uint32_t Hasher::HashCrc(const char *buf, uint64_t len) {
  static constexpr const auto kAlignSize = 0x08ul;
  static constexpr const auto kAlignMask = (kAlignSize - 1);
  static constexpr const uint32_t kSeed = 0x741b8cd7;

  uint32_t crc = kSeed;

  // Align the input to the word boundary
  for (; (len > 0) && ((size_t)buf & kAlignMask); len--, buf++) {
    crc = _mm_crc32_u8(crc, *(uint8_t *)buf);
  }

  // Now greedily process 8-byte, 4-byte, and 2-byte chunks of the input,
  // exhausting the tail byte-at-a-time
#if defined(__x86_64__) || defined(_M_X64)
  CRC32(_mm_crc32_u64, crc, uint64_t, buf, len);
#endif
  CRC32(_mm_crc32_u32, crc, uint32_t, buf, len);
  CRC32(_mm_crc32_u16, crc, uint16_t, buf, len);
  CRC32(_mm_crc32_u8, crc, uint8_t, buf, len);

  return crc;
}

#undef CRC32

}  // namespace tpl::util