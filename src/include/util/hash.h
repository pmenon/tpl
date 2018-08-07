#pragma once

#include <cstdint>

namespace tpl::util {

class Hasher {
 public:
  enum class HashMethod { Fnv1, Murmur3, Crc };

  static uint32_t Hash(const char *data, uint64_t len,
                       HashMethod method = HashMethod::Crc);

 private:
  static uint32_t HashFnv1(const char *data, uint64_t len);

  static uint32_t HashMurmur(const char *data, uint64_t len);

  static uint32_t HashCrc(const char *data, uint64_t len);
};

}  // namespace tpl::util