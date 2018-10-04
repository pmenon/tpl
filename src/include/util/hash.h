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

  static u32 Hash(const char *buf, u64 len,
                       HashMethod method = HashMethod::Crc);

 private:
  static u32 HashFnv1(const char *buf, u64 len);

  static u32 HashMurmur(const char *buf, u64 len);

  static u32 HashCrc(const char *buf, u64 len);
};

}  // namespace tpl::util