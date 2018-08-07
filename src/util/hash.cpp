#include "util/hash.h"

#include <x86intrin.h>

namespace tpl::util {

uint32_t Hasher::Hash(const char *data, uint64_t len, HashMethod method) {
  switch (method) {
    case HashMethod::Fnv1:
      return HashFnv1(data, len);
    case HashMethod::Murmur3:
      return HashMurmur(data, len);
    default:
      return HashCrc(data, len);
  }
}

uint32_t Hasher::HashFnv1(const char *data, uint64_t len) {
  uint32_t hash = 2166136261u;

  for (uint64_t i = 0; i < len; i++) {
    hash ^= data[i];
    hash *= 16777619;
  }

  return hash;
}

uint32_t Hasher::HashMurmur(const char *data, uint64_t len) { return 0; }

#define CRC32(op, crc, type, buf, len)                   \
  do {                                                   \
    for (; (len) >= sizeof(type);                        \
         (len) -= sizeof(type), (buf) += sizeof(type)) { \
      (crc) = op((crc), *(type *)buf);                   \
    }                                                    \
  } while (0)

uint32_t Hasher::HashCrc(const char *data, uint64_t len) {
  uint32_t crc = 0x741B8CD7;

  // If the string is empty, return the CRC calculated so far
  if (len == 0) {
    return crc;
  }

#if defined(__x86_64__) || defined(_M_X64)
  // Try to each up as many 8-byte values as possible
  CRC32(_mm_crc32_u64, crc, uint64_t, data, len);
#endif
  // Now we perform CRC in 4-byte, then 2-byte chunks.  Finally, we process
  // what remains in byte chunks.
  CRC32(_mm_crc32_u32, crc, uint32_t, data, len);
  CRC32(_mm_crc32_u16, crc, uint16_t, data, len);
  CRC32(_mm_crc32_u8, crc, uint8_t, data, len);
  // Done
  return crc;
}

#undef CRC32

}  // namespace tpl::util