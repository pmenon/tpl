#pragma once

#include <cstdint>

#include "util/common.h"

namespace tpl::util {

/**
 * Utility class for hashing
 */
class Hasher {
 public:
  enum class HashMethod { Fnv1, Murmur3, Crc };

  static hash_t Hash(const u8 *buf, u64 len,
                     HashMethod method = HashMethod::Crc);

 private:
  static hash_t HashFnv1(const u8 *buf, u64 len);

  static hash_t HashMurmur3(const u8 *buf, u64 len);

  static hash_t HashCrc32(const u8 *buf, u64 len);
};

}  // namespace tpl::util